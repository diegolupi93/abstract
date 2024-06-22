from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
import pendulum
from task.melbourne import extract_data, transform_data, load_data


# Define default arguments for the DAG
local_tz = pendulum.timezone("UTC")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='melbourne_dag',
    default_args=default_args,
    catchup=False,
    description='A simple DAG to extract, transform, and load data from a CSV to a SQLite database',
    schedule_interval=timedelta(days=1),
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        
    )

    chain(
        extract_task,
        transform_task,
        load_task,
    )