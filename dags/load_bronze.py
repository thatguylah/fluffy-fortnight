import os
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

PYTHON_REQUIREMENTS = [
    "pandas==2.2.2",
    "openpyxl==3.1.5",
    "sqlalchemy==2.0.31",
    "psycopg2-binary==2.9.9",
]


# Define file paths
dataset1_path = f'{os.getenv("AIRFLOW_HOME")}/data/20240723/window1/dataset1.xlsx'
dataset2_path = f'{os.getenv("AIRFLOW_HOME")}/data/20240723/window1/dataset2.json'


@task.virtualenv(
    task_id="load_raw_dataset_1",
    requirements=PYTHON_REQUIREMENTS,
    system_site_packages=False,
)
def load_raw_dataset_1():
    from sqlalchemy import create_engine
    import pandas as pd
    import os

    dataset1_path = f'{os.getenv("AIRFLOW_HOME")}/data/20240723/window1/dataset1.xlsx'

    engine = create_engine(
        "postgresql://warehouse:warehouse@postgres_warehouse:5432/warehouse"
    )

    def load_dataframe_to_postgres(df, table_name):
        df.to_sql(
            table_name, con=engine, if_exists="append", index=False, method="multi"
        )

    df_dataset1 = pd.read_excel(dataset1_path, sheet_name="DATA")
    print(df_dataset1.shape[0])
    load_dataframe_to_postgres(df_dataset1, "RAW_DATASET_1")


@task.virtualenv(
    task_id="load_raw_dataset_2",
    requirements=PYTHON_REQUIREMENTS,
    system_site_packages=False,
)
def load_raw_dataset_2():
    from sqlalchemy import create_engine
    import pandas as pd
    import os

    dataset2_path = f'{os.getenv("AIRFLOW_HOME")}/data/20240723/window1/dataset2.json'

    engine = create_engine(
        "postgresql://warehouse:warehouse@postgres_warehouse:5433/warehouse"
    )

    def truncate_table(table_name):
        with engine.connect() as connection:
            connection.execute(f"TRUNCATE TABLE {table_name}")

    def load_dataframe_to_postgres(df, table_name):
        truncate_table(table_name)
        df.to_sql(
            table_name, con=engine, if_exists="append", index=False, method="multi"
        )

    df_dataset2 = pd.read_json(dataset2_path)
    load_dataframe_to_postgres(df_dataset2, "RAW_DATASET_2")


@task.virtualenv(
    task_id="load_raw_mapping",
    requirements=PYTHON_REQUIREMENTS,
    system_site_packages=False,
)
def load_raw_mapping():
    from sqlalchemy import create_engine
    import pandas as pd
    import os

    dataset1_path = f'{os.getenv("AIRFLOW_HOME")}/data/20240723/window1/dataset1.xlsx'

    engine = create_engine(
        "postgresql://warehouse:warehouse@postgres_warehouse:5433/warehouse"
    )

    def truncate_table(table_name):
        with engine.connect() as connection:
            connection.execute(f"TRUNCATE TABLE {table_name}")

    def load_dataframe_to_postgres(df, table_name):
        truncate_table(table_name)
        df.to_sql(
            table_name, con=engine, if_exists="append", index=False, method="multi"
        )

    df_mapping = pd.read_excel(dataset1_path, sheet_name="CITY_DISTRICT_MAP")
    load_dataframe_to_postgres(df_mapping, "RAW_MAPPING")


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

# Define the DAG
with DAG("load_bronze_dag", default_args=default_args, schedule_interval=None) as dag:
    task_1 = load_raw_dataset_1()
    task_2 = load_raw_dataset_2()
    task_3 = load_raw_mapping()

    task_1 >> task_2 >> task_3
