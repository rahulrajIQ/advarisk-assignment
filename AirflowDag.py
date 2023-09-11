from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import extract_sales
import transform_sales
import load_sales
import requests
import json
import os


# Define the default dag arguments.
default_args = {
    'owner': 'Rahul Raj',
    'depends_on_past': False,
    'email': ['raazx2j@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


# Define the dag, the start date and how frequently it runs.
dag = DAG(
    dag_id='SalesDag',
    default_args=default_args,
    start_date=datetime(2023, 9, 10),
    schedule_interval='0 0 * * *' #Run once a day at midnight
)

# First task is to query get the sales data
task1 = PythonOperator(
    task_id='get_sales',
    provide_context=True,
    python_callable=get_sales,
    dag=dag)


# Second task is to transform the data
task2 = PythonOperator(
    task_id='transform_sales',
    provide_context=True,
    python_callable=transform_sales,
    dag=dag)

# Third task is to load data into the database.
task3 = PythonOperator(
    task_id='load_sales',
    provide_context=True,
    python_callable=load_salesS,
    dag=dag)

# Set task1 "upstream" of task2
# task1 must be completed before task2 can be started
task1 >> task2 >> task3
