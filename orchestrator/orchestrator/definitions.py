import os

from dagster import Definitions, in_process_executor
from dagster_dbt import DbtCliResource

from .assets import (
    raw_dataset_1,
    raw_dataset_2,
    raw_mapping,
    dbt_assets,
    translations_city_mapping,
    translations_district_mapping,
    currency_code_mapping,
    curated_city_cluster_results,
)
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[
        raw_dataset_1,
        raw_dataset_2,
        raw_mapping,
        dbt_assets,
        translations_city_mapping,
        translations_district_mapping,
        currency_code_mapping,
        curated_city_cluster_results,
    ],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    executor=in_process_executor,
)
