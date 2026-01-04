import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator

# path = os.path.expanduser('~/airflow_hw')
# # Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
# os.environ['PROJECT_PATH'] = path
# # Добавим путь к коду проекта в $PATH, чтобы импортировать функции
# sys.path.insert(0, path)

# from modules.pipeline import pipeline
# <YOUR_IMPORTS>

import sys
sys.path.insert(0, '/opt/airflow')

from modules.test import test  # если файл: modules/test.py, функция: def test()

def pipeline():
    print('pipeline')

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='test',
        schedule_interval="0 15 * * *",
        default_args=args,
) as dag:
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
    )
    test = PythonOperator(
        task_id='test',
        python_callable=test,
    )
    pipeline >> test 

