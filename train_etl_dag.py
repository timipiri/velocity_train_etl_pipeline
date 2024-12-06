from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

# Import functions from the script
from train_etl_pipeline import (
    data_ingestion_layer,
    data_transformation_layer,
    data_validation_layer,
    data_duplication_layer
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': 'your_email',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'train_etl_pipeline',
    default_args=default_args,
    description='Train ETL pipeline with XCom data transfer',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 10, 25),
    catchup=False,
)

ingestion_task = PythonOperator(
    task_id='data_ingestion',
    python_callable=data_ingestion_layer,
    provide_context=True,
    dag=dag,
)

transformation_task = PythonOperator(
    task_id='data_transformation',
    python_callable=data_transformation_layer,
    provide_context=True,
    dag=dag,
)

validation_task = PythonOperator(
    task_id='data_validation',
    python_callable=data_validation_layer,
    provide_context=True,
    dag=dag,
)

duplication_task = PythonOperator(
    task_id='data_duplication',
    python_callable=data_duplication_layer,
    provide_context=True,
    dag=dag,
)

ingestion_task >> transformation_task >> validation_task >> duplication_task