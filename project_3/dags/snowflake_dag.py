from datetime import datetime, timedelta
from airflow import DAG
import os
import sys
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dag_file_path = os.path.dirname(os.path.abspath(__file__))

from etls.snowflake_etl import  load_to_snowflake,transform_dm_top_category_sales,transform_dm_top_employee_revenue,transform_dm_supplier_revenue

default_args = {
    'owner': 'Query-Query Ninja',
    'start_date': datetime(2024, 6, 18)
}

dag = DAG(
    'postgres_to_snowflake',
    default_args=default_args,
    schedule_interval='@daily', 
)

transform_dm_supplier_revenue = PythonOperator(
    task_id='transform_dm_supplier_revenue',
    python_callable=transform_dm_supplier_revenue,
    dag=dag,
)
transform_dm_top_category_sales = PythonOperator(
    task_id='transform_dm_top_category_sales',
    python_callable=transform_dm_top_category_sales,
    dag=dag,
)

transform_dm_top_employee_revenue = PythonOperator(
    task_id='transform_dm_top_employee_revenue',
    python_callable=transform_dm_top_employee_revenue,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag,
)

transform_dm_supplier_revenue >> transform_dm_top_category_sales >> transform_dm_top_employee_revenue >> load_task