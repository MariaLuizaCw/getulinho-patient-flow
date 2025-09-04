from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0
}

with DAG('airflow_db_cleanup', schedule_interval='@daily', default_args=default_args, catchup=False, tags=['maintenance']) as dag:
    clean_db = BashOperator(
        task_id='clean_airflow_db',
        bash_command='airflow db clean --clean-before-timestamp $(date -d "90 days ago" +%Y-%m-%dT%H:%M:%S) --yes'
    )
