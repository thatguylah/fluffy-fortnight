from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def transform_and_load_to_silver():
    # Code to perform transformations and load into PROCESSED_DATASET
    pass


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    "transform_to_silver_dag", default_args=default_args, schedule_interval=None
) as dag:
    transform_to_silver = PythonOperator(
        task_id="transform_and_load_to_silver",
        python_callable=transform_and_load_to_silver,
    )
