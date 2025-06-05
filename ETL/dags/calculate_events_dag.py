# Standard library
import os
import json
from datetime import datetime, timedelta
import importlib
from dotenv import load_dotenv, find_dotenv

# Third-party
from airflow import DAG
from airflow.operators.python import PythonOperator
from pytz import timezone
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert  
from sqlalchemy.exc import SQLAlchemyError  
import logging

# Project-specific
from utils.help import load_json, get_now_brazil, get_now_brazil_full
from utils.database import update_last_datetime, create_engine_postgres
from delta_calculators.delta_base import DeltaBase
from event_calculators.base_event import BaseEventCalculator


load_dotenv(find_dotenv())

default_args = {
    'owner': 'malu',
    'start_date': datetime(2025, 5, 1),
    'retries': 0,
    
}

dag = DAG(
    dag_id='calculate_events',
    default_args=default_args,
    catchup=False,
    tags=['transform'],
    schedule_interval=None
)

calculation_tasks = []


logger = logging.getLogger(__name__)
TABLE_NAME = "events"
RISK_CLASS_TABLE = "last_risk_class"

CALCULATOR_CLASSES =  load_json("config/event_calculators.json")

def get_event_calculator(event_name, base):
    if event_name not in CALCULATOR_CLASSES:
        raise ValueError(f"Evento '{event_name}' não reconhecido.")
    module_path, class_name = CALCULATOR_CLASSES[event_name].rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(base)

def make_callable(event_name):
    def _call(ti, **kwargs):
        execution_date = kwargs.get("execution_date")
        now_brazil = get_now_brazil(execution_date)
        base = BaseEventCalculator(source_schema="source", current_hour=now_brazil)

        calculator = get_event_calculator(event_name, base)
        df = calculator.calculate()
        logger.info(f"[{event_name}] calculado com {len(df)} registros para hora {now_brazil}.")

        # Conecta ao banco uma vez só
        engine = create_engine_postgres()
        metadata = MetaData()

        # Persistência principal
        if df.empty:
            logger.info(f"[{event_name}] Nenhum dado a inserir.")
            return 
    
        table = Table(TABLE_NAME, metadata, autoload_with=engine)
        records = df.to_dict(orient="records")

        with engine.begin() as connection:
            stmt = pg_insert(table).values(records).on_conflict_do_nothing()
            result = connection.execute(stmt)
        
        update_last_datetime(engine=engine, table_name=TABLE_NAME, report_time=now_brazil)

        inserted_count = result.rowcount if result.rowcount is not None else 0
        skipped_count = len(records) - inserted_count
        logger.info(f"[{event_name}] Inseridos: {inserted_count} | Ignorados por duplicata: {skipped_count}")

        # Se for fim da classificação, gerar e persistir last_risk_class
        if event_name == 'fim_classificacao':
            df_risk = calculator.get_class()

            if df_risk.empty:
                logger.info(f"[{event_name}] get_class retornou DataFrame vazio.")
                return

            risk_table = Table(RISK_CLASS_TABLE, metadata, autoload_with=engine)
            risk_records = df_risk.to_dict(orient="records")

            with engine.begin() as connection:
                stmt = pg_insert(risk_table).values(risk_records).on_conflict_do_nothing()
                connection.execute(stmt)

            logger.info(f"[{event_name}] get_class: {len(risk_records)} registros inseridos em '{RISK_CLASS_TABLE}'.")

    return _call

for event_name in CALCULATOR_CLASSES:
    task = PythonOperator(
        task_id=f'process_{event_name}',
        python_callable=make_callable(event_name),
        dag=dag
    )
