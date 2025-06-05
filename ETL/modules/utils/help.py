
import os
import json
from pytz import timezone
from datetime import datetime



def load_json(file_name):
    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    full_path = os.path.join(airflow_home, file_name)
    with open(full_path, "r") as f:
        return json.load(f)


def get_now_brazil(execution_date=None):
    br_tz = timezone("America/Sao_Paulo")
    if execution_date:
        return execution_date.in_timezone(br_tz).replace(minute=0, second=0, microsecond=0)
    return datetime.now(br_tz).replace(minute=0, second=0, microsecond=0)

def get_now_brazil_full(execution_date=None):
    br_tz = timezone("America/Sao_Paulo")
    if execution_date:
        return execution_date.astimezone(br_tz)
    return datetime.now(br_tz)
