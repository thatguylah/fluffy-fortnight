from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    "main_dag", default_args=default_args, schedule_interval="@daily", catchup=False
) as dag:
    start = DummyOperator(task_id="start")

    trigger_load_bronze = TriggerDagRunOperator(
        task_id="trigger_load_bronze", trigger_dag_id="load_bronze_dag"
    )

    trigger_transform_to_silver = TriggerDagRunOperator(
        task_id="trigger_transform_to_silver", trigger_dag_id="transform_to_silver_dag"
    )

    trigger_handle_exceptions = TriggerDagRunOperator(
        task_id="trigger_handle_exceptions", trigger_dag_id="handle_exceptions_dag"
    )

    end = DummyOperator(task_id="end")

    (
        start
        >> trigger_load_bronze
        >> trigger_transform_to_silver
        >> trigger_handle_exceptions
        >> end
    )
