from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from utils.help import load_json, get_now_brazil
import pendulum

default_args = {
    'owner': 'malu',
    'depends_on_past': False,
    'retries': 0,
}

def generate_execution_dates(**kwargs):
    params = kwargs['params']
    
    start_str = params.get('start_hour_br')
    end_str = params.get('end_hour_br')

    if not start_str or not end_str:
        raise ValueError("Parâmetros 'start_hour_br' e 'end_hour_br' são obrigatórios (formato: YYYY-MM-DD HH:MM).")
        
    tz = pendulum.timezone("America/Sao_Paulo")
    start = tz.convert(datetime.strptime(start_str, "%Y-%m-%d %H:%M"))
    end = tz.convert(datetime.strptime(end_str, "%Y-%m-%d %H:%M"))


    execution_dates = []
    while start <= end:
        execution_dates.append(start)
        start += timedelta(hours=1)

    kwargs['ti'].xcom_push(key='execution_dates', value=execution_dates)

with DAG(
    dag_id='reprocess_calculate_events',
    default_args=default_args,
    description='Reprocessa a DAG calculate_events para um range de horas',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['reprocess', 'events'],
    params={
        "start_hour_br": "2025-05-13 00:00",
        "end_hour_br": "2025-05-13 05:00"
    }
) as dag:

    extract_dates = PythonOperator(
        task_id='generate_execution_dates',
        python_callable=generate_execution_dates
    )

    def trigger_all(**kwargs):
        ti = kwargs['ti']
        execution_dates = ti.xcom_pull(task_ids='generate_execution_dates', key='execution_dates')

        for dt in execution_dates:
            TriggerDagRunOperator(
                task_id=f"trigger_{dt.strftime('%Y%m%dT%H')}",
                trigger_dag_id='calculate_events',
                execution_date=dt,
                reset_dag_run=True,
                wait_for_completion=False
            ).execute(context=kwargs)

    trigger = PythonOperator(
        task_id='trigger_all_runs',
        python_callable=trigger_all
    )

    extract_dates >> trigger
