import os
from pathlib import Path

from dagster_dbt import DbtCliResource

dbt_project_dir = Path(__file__).joinpath("..", "..", "..").resolve()
dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

DUCKDB_FILE_PATH = (
    Path(__file__)
    .joinpath("..", "..", "..", "data", "output", "datawarehouse.duckdb")
    .resolve()
)

INPUT_EXCEL_PATH = (
    Path(__file__)
    .joinpath("..", "..", "..", "data", "input", "20240723", "window1", "dataset1.xlsx")
    .resolve()
)

INPUT_JSON_PATH = (
    Path(__file__)
    .joinpath("..", "..", "..", "data", "input", "20240723", "window1", "dataset2.json")
    .resolve()
)

CITY_TRANSLATIONS_FILE_PATH = (
    Path(__file__)
    .joinpath("..", "..", "..", "data", "static", "mappings", "city_translations.json")
    .resolve()
)

DISTRICTS_TRANSLATIONS_FILE_PATH = (
    Path(__file__)
    .joinpath(
        "..", "..", "..", "data", "static", "mappings", "districts_translations.json"
    )
    .resolve()
)

CITY_CLUSTER_RESULTS_FILE_PATH = (
    Path(__file__)
    .joinpath("..", "..", "..", "data", "static", "cluster", "cluster_results.csv")
    .resolve()
)

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at run time.
# Otherwise, we expect a manifest to be present in the project's target directory.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")
