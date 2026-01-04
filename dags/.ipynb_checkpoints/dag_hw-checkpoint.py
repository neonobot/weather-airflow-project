import datetime as dt
import os
import sys
sys.path.insert(0, '/opt/airflow')

from airflow.models import DAG
from airflow.operators.python import PythonOperator

# from modules.test import test 
from modules.pipeline import pipeline 
from modules.predict import predict 


args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="0 15 * * *",
        default_args=args,
) as dag:
    run_pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
    )
    run_predict = PythonOperator(
        task_id='predict',
        python_callable=predict,
    )
    run_pipeline >> run_predict 

