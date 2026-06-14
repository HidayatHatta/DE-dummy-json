import sys
import os
from datetime import datetime

sys.path.append(os.path.abspath(os.environ.get('AIRFLOW_HOME', '/opt/airflow')))

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.extract import extract
from scripts.transform import transform
from scripts.validate import validate
from scripts.load import load


def extract_task(**context):
    return extract()

def transform_task(**context):
    file_path = context['ti'].xcom_pull(task_ids='extract')
    return transform(file_path)

def validate_task(**context):
    file_path = context['ti'].xcom_pull(task_ids='transform')
    return validate(file_path)

def load_task(**context):
    file_path = context['ti'].xcom_pull(task_ids='transform')
    load(file_path)

with DAG(
    dag_id='advanced_data_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_op = PythonOperator(task_id='extract', python_callable=extract_task)
    transform_op = PythonOperator(task_id='transform', python_callable=transform_task)
    validate_op = PythonOperator(task_id='validate', python_callable=validate_task)
    load_op = PythonOperator(task_id='load', python_callable=load_task)

    extract_op >> transform_op >> validate_op >> load_op