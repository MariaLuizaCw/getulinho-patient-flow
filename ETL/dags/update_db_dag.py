from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
from utils.help import load_json, get_now_brazil
import os
import sys
import json
from ingest_module import APIFetcher  # importa a classe
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule


load_dotenv(find_dotenv())

apis =  load_json("config/api_routes.json")
# Wrapper para uso no PythonOperator
def run_fetch(url, table_name, params):
    fetcher = APIFetcher()
    fetcher.fetch_and_insert_data(route=url, table_name=table_name, params=params)

# Configurações do DAG
default_args = {
    'owner': 'malu',
    'start_date': datetime(2025, 5, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_tables_db',
    default_args=default_args,
    schedule_interval='@hourly',
    tags=["ingestion"],
    catchup=False,
)

ingestion_tasks = []

# Cria uma task por API
for api in apis:
    op_kwargs = {
        "url": api["route"],
        "table_name": api["table_name"],
        "params": api["params"]
    }

    task = PythonOperator(
        task_id=f"update_{api['table_name']}",
        python_callable=run_fetch,
        op_kwargs=op_kwargs,
        dag=dag,
        execution_timeout=timedelta(minutes=40)  
    )

    ingestion_tasks.append(task)

# Task que dispara a próxima DAG após todas as ingestões
trigger_next_dag = TriggerDagRunOperator(
    task_id="trigger_calculate_events",
    trigger_dag_id="calculate_events",  # nome da DAG a ser disparada
    trigger_rule=TriggerRule.ALL_DONE,  
    conf={"execution_date": "{{ ts }}"},
    dag=dag,
)

# Define que o trigger só será executado após todas as tasks de ingestão
for task in ingestion_tasks:
    task >> trigger_next_dag