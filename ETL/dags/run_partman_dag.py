from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'malu',
    'start_date': datetime(2025, 5, 1, 0, 30),
    'retries': 3, 
    'retry_delay': timedelta(minutes=5), 
}

dag = DAG(
    'run_partman_maintenance',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags=["maintenance"],
    description='Executa o partman.run_maintenance_proc uma vez por hora',
)

def run_partman():
    hook = PostgresHook(postgres_conn_id='postgres_master')
    sql = "CALL partman.run_maintenance_proc();"

    conn = hook.get_conn()
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()


run_task = PythonOperator(
    task_id='execute_partman_maintenance',
    python_callable=run_partman,
    dag=dag
)
