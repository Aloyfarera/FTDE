from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dag_file_path = os.path.dirname(os.path.abspath(__file__))

from etls.postgres_etl import  load_to_postgres,extract_csv

# Define default args
default_args = {
    'owner': 'Query-Query Ninja',
    'start_date': datetime(2024, 6, 18)
}


# Define the DAG
dag = DAG(
    'csv_to_postgres',
    default_args=default_args,
    description='Extract CSVs from GitHub and insert into PostgreSQL',
    schedule_interval='@daily',
)

# Create the tasks
extract_csv_task = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv,
    dag=dag,
)

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

# Set the task dependencies
extract_csv_task >> load_to_postgres_task