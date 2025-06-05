from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from utils.help import load_json, get_now_brazil
import json
import os

# Caminho para o JSON com os diretórios e retenções

AIRFLOW_BASE_DIR = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
rules = load_json("config/retention_rules.json")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 1),
    'retries': 0
}

dag = DAG(
    dag_id='cleanup_directories',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['maintenance']
)

for path, days in rules.items():
    safe_name = path.strip('/').replace('/', '_')
    full_path = os.path.join(AIRFLOW_BASE_DIR, path.strip("/"))

    delete_files = BashOperator(
        task_id=f"cleanup_files_{safe_name}",
        bash_command=f"find {full_path} -type f -mtime +{int(days)} -delete",
        dag=dag
    )

    delete_empty_dirs = BashOperator(
        task_id=f"cleanup_empty_dirs_{safe_name}",
        bash_command=f"find {full_path} -mindepth 1 -type d -empty -delete",
        dag=dag
    )


    delete_files >> delete_empty_dirs