import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/pipeline/scraper')
from insert_records import main

default_args = {
    'description': 'A DAG to orchestrate data',
    'start_date': datetime(2024, 1, 21),
    'catchup': False,
}

dag = DAG(
    dag_id='scraping-orchestrator',
    default_args=default_args,
    schedule=timedelta(minutes=5)
)

with dag:
    task1 = PythonOperator(
        task_id='ingest_data_task',
        python_callable=main
    )