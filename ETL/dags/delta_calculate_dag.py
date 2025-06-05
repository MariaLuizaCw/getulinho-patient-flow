# Standard library
import os
import json
from datetime import datetime, timedelta
import importlib

# Third-party
from airflow import DAG
from airflow.operators.python import PythonOperator
from pytz import timezone

# Project-specific
from utils.help import load_json, get_now_brazil, get_now_brazil_full
from utils.database import update_last_datetime, create_engine_postgres
from delta_calculators.delta_base import DeltaBase


# Carregar o JSON de delta_calculators

DELTA_CALCULATORS = load_json("config/delta_calculators.json")
SEQUENCES = load_json("common_settings/delta_sequences.json")

# Parâmetros da DAG
default_args = {
    'owner': 'malu',
    'start_date': datetime(2025, 5, 15),
    'retries': 0,
}

# Função que calcula os deltas e insere os resultados
def calculate_metrics_for_sequence(delta_base, delta_calculator, sequence, function_params):
    class_name = delta_calculator["class_name"]
    
    module_path, class_name = class_name.rsplit('.', 1) 
    module = importlib.import_module(module_path)  
    delta_calculator_class = getattr(module, class_name)  

    delta_calculator_instance = delta_calculator_class(delta_base)

    results, start_interval = delta_calculator_instance.calculate(sequence=sequence, **function_params) 
   
  
    delta_base.insert_results_into_db(delta_calculator["table_name"], results)

    update_last_datetime(
        engine=create_engine_postgres(), 
        table_name=delta_calculator["table_name"], 
        report_time=start_interval
    )

    return

# Instanciar a classe DeltaBase (classe base)
delta_base = DeltaBase(
    current_hour=get_now_brazil(),
    source_schema='public', 
    source_table_name='events'
)

# Criar uma DAG para cada entrada no JSON
for delta_calculator in DELTA_CALCULATORS:
    dag_id = f"calculate_{delta_calculator['table_name']}"  # Nome único da DAG baseado no nome da tabela

    with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=delta_calculator['cron'],  # A DAG será executada a cada hora
        catchup=False,
        tags=['transform'],
    ) as dag:

        # Criar uma tarefa para cada sequência no delta_calculator
        tasks = []
        for sequence in SEQUENCES:  # Aqui, SEQUENCES contém as sequências que você forneceu
            # Obter os parâmetros necessários para a função
            function_params = delta_calculator.get('function_params', {})

            task = PythonOperator(
                task_id=f'calculate_and_insert_{sequence["delta_name"]}_{delta_calculator["table_name"]}',
                python_callable=calculate_metrics_for_sequence,
                op_args=[delta_base, delta_calculator, sequence, function_params],  # Passando o delta_base para a função de cálculo
                provide_context=True,
            )
            tasks.append(task)