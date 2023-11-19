from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pipeline_utils import extract, transform, load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ev-stock-etl-dags',
    default_args=default_args,
    description='A EV stock ETL pipeline DAG',
    schedule_interval=timedelta(days=1),
)

def extract_data(**kwargs):
    raw_stock_data = extract(["TSLA", "RIVN", "AEHR", "ON", "VFS"])
    kwargs["ti"].xcom_push(key="raw_stock_data", value=raw_stock_data)

def transform_data(**kwargs):
    raw_data = kwargs["ti"].xcom_pull(key="raw_stock_data", task_ids="extract_data")
    transformed_data = transform(raw_data)
    kwargs["ti"].xcom_push(key="transformed_data", value=transformed_data)
    return transformed_data

def load_data(**kwargs):
    transformed_data = kwargs["ti"].xcom_pull(key="transformed_data", task_ids="transform_data")
    load(transformed_data, "transformed_stock_data")

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# set task dependencies
extract_task >> transform_task >> load_task

# example of running a bash command
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# set task dependencies
t1 >> extract_task
