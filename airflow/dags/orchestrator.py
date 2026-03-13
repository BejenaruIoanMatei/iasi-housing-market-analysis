import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def safe_ingest_callable():
    import sys
    sys.path.insert(0, '/opt/airflow/pipeline/scraper')
    from insert_records import main
    return main()

def safe_transform_callable():
    import sys
    sys.path.insert(0, '/opt/airflow/pipeline/transformer')
    from transform_records import main
    return main()


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
    task_ingest = PythonOperator(
        task_id='ingest_data_task',
        python_callable=safe_ingest_callable
    )
    
    task_transform = PythonOperator(
        task_id='transform_data_task',
        python_callable=safe_transform_callable
    )
    
    task_ingest >> task_transform