import os
import duckdb
import re
import json
import pandas as pd
from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets, get_asset_key_for_model

from .constants import (
    dbt_manifest_path,
    DUCKDB_FILE_PATH,
    INPUT_EXCEL_PATH,
    INPUT_JSON_PATH,
    CITY_TRANSLATIONS_FILE_PATH,
    DISTRICTS_TRANSLATIONS_FILE_PATH,
    CITY_CLUSTER_RESULTS_FILE_PATH,
)


def extract_per_capita(per_capita_str):
    """
    Extracts per capita value from a metadata JSON scraped from wikipedia.
    Source data comes with both RMB and USD GDP/capita.
    Args:
        per_capita_str (str): The string containing per capita value.

    Returns:
        str: Extracted per capita value without commas.
    """
    match = re.search(r"US\$ ([\d,]+)", per_capita_str)
    if match:
        return match.group(1).replace(",", "")
    return None


def extract_total_gdp(total_gdp_str):
    """
    Extracts total GDP value from a metadata JSON scraped from wikipedia.
    Source data comes with both RMB and USD GDP.
    Args:
        total_gdp_str (str): The string containing total GDP value.

    Returns:
        int: Extracted total GDP value as an integer.
    """
    match_billion = re.search(r"US\$ ([\d\.]+) billion", total_gdp_str)
    if match_billion:
        return int(float(match_billion.group(1)) * 1e9)
    match_simple = re.search(r"US\$ ([\d,]+)", total_gdp_str)
    if match_simple:
        return int(match_simple.group(1).replace(",", ""))
    return None


def execute_upsert_query(con, table_name, df, create_table_query, upsert_query):
    """
    Executes the upsert query for a given DataFrame and table.
    Important for idempotent pipelines otherwise we would have duplicates.

    Args:
        con (duckdb.DuckDBPyConnection): The DuckDB connection.
        table_name (str): The name of the table.
        df (pd.DataFrame): The DataFrame to upsert.
        create_table_query (str): The SQL query to create the table.
        upsert_query (str): The SQL query to upsert data into the table.
    """
    con.execute(create_table_query)
    con.register(f"df_{table_name}", df)
    con.execute(upsert_query)
    print(f"DataFrame loaded into {table_name}:")
    print(df.head())


def load_json_data(file_path):
    """
    Loads JSON data from a file.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: The loaded JSON data.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


@dbt_assets(manifest=dbt_manifest_path)
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Runs dbt build command and streams the output.

    Args:
        context (AssetExecutionContext): The execution context.
        dbt (DbtCliResource): The dbt CLI resource.
    """
    yield from dbt.cli(["build"], context=context).stream()


@asset(compute_kind="python", description="Extract and Load raw Excel dataset 1")
def raw_dataset_1(context: AssetExecutionContext) -> None:
    """
    Extracts and loads raw dataset 1 from an Excel file into DuckDB.
    Given source data contains both "DATA" and "CITY_DISTRICT_MAPPING" sheets.
    Args:
        context (AssetExecutionContext): The execution context.
    """
    # Load data from the Excel file into a DataFrame
    df = pd.read_excel(INPUT_EXCEL_PATH, sheet_name="DATA")
    df.rename(columns={"ORDER_TIME  (PST)": "ORDER_TIME_PST"}, inplace=True)

    # Connect to DuckDB and set the pandas analyze sample parameter
    with duckdb.connect(os.fspath(DUCKDB_FILE_PATH)) as con:
        con.execute(
            "SET GLOBAL pandas_analyze_sample=100000000"
        )  # We need to tell duckdb to automatically convert some cols as VARCHAR first otherwise it will fail loading.

        # SQL query to create the table if it does not exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS RAW_DATASET_1 (
            ORDER_ID VARCHAR PRIMARY KEY,
            ORDER_TIME_PST VARCHAR,
            CITY_DISTRICT_ID INT,
            RPTG_AMT DECIMAL(18,2),
            CURRENCY_CD VARCHAR,
            ORDER_QTY VARCHAR
        );
        """

        # SQL query to upsert data into the table
        upsert_query = """
        INSERT INTO RAW_DATASET_1
        SELECT * FROM df_raw_dataset_1
        ON CONFLICT(ORDER_ID) DO UPDATE SET
            ORDER_TIME_PST = EXCLUDED.ORDER_TIME_PST,
            CITY_DISTRICT_ID = EXCLUDED.CITY_DISTRICT_ID,
            RPTG_AMT = EXCLUDED.RPTG_AMT,
            CURRENCY_CD = EXCLUDED.CURRENCY_CD,
            ORDER_QTY = EXCLUDED.ORDER_QTY;
        """

        # Execute the upsert query
        execute_upsert_query(con, "raw_dataset_1", df, create_table_query, upsert_query)

    # Log metadata about the table we just wrote. It will show up in the UI.
    context.add_output_metadata({"num_rows": df.shape[0]})


@asset(compute_kind="python", description="Extract and Load raw JSON dataset 2")
def raw_dataset_2(context: AssetExecutionContext) -> None:
    """
    Extracts and loads raw dataset 2 from a JSON file into DuckDB.
    Args:
        context (AssetExecutionContext): The execution context.
    """
    df = pd.read_json(INPUT_JSON_PATH)
    with duckdb.connect(os.fspath(DUCKDB_FILE_PATH)) as con:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS RAW_DATASET_2 (
            ORDER_ID VARCHAR PRIMARY KEY,
            ORDER_TIME_PST BIGINT,
            SHIP_TO_DISTRICT_NAME VARCHAR,
            SHIP_TO_CITY_CD VARCHAR,
            RPTG_AMT DECIMAL(18,2),
            CURRENCY_CD VARCHAR,
            ORDER_QTY INT
        );
        """
        upsert_query = """
        INSERT INTO RAW_DATASET_2
        SELECT * FROM df_raw_dataset_2
        ON CONFLICT(ORDER_ID) DO UPDATE SET
            ORDER_TIME_PST = EXCLUDED.ORDER_TIME_PST,
            SHIP_TO_CITY_CD = EXCLUDED.SHIP_TO_CITY_CD,
            SHIP_TO_DISTRICT_NAME = EXCLUDED.SHIP_TO_DISTRICT_NAME,
            RPTG_AMT = EXCLUDED.RPTG_AMT,
            CURRENCY_CD = EXCLUDED.CURRENCY_CD,
            ORDER_QTY = EXCLUDED.ORDER_QTY;
        """
        execute_upsert_query(con, "raw_dataset_2", df, create_table_query, upsert_query)
    context.add_output_metadata({"num_rows": df.shape[0]})


@asset(compute_kind="python", description="Extract and Load City-District Mapping")
def raw_mapping(context: AssetExecutionContext) -> None:
    """
    Extracts and loads raw mapping from an Excel file into DuckDB.
    Given source data contains both "DATA" and "CITY_DISTRICT_MAPPING" sheets.

    Args:
        context (AssetExecutionContext): The execution context.
    """
    df = pd.read_excel(INPUT_EXCEL_PATH, sheet_name="CITY_DISTRICT_MAP")
    with duckdb.connect(os.fspath(DUCKDB_FILE_PATH)) as con:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS RAW_MAPPING (
            CITY_DISTRICT_ID INT PRIMARY KEY,
            SHIP_TO_CITY_CD VARCHAR,
            SHIP_TO_DISTRICT_NAME VARCHAR
        );
        """
        upsert_query = """
        INSERT INTO RAW_MAPPING
        SELECT * FROM df_raw_mapping
        ON CONFLICT(CITY_DISTRICT_ID) DO UPDATE SET
            SHIP_TO_CITY_CD = EXCLUDED.SHIP_TO_CITY_CD,
            SHIP_TO_DISTRICT_NAME = EXCLUDED.SHIP_TO_DISTRICT_NAME;
        """
        execute_upsert_query(con, "raw_mapping", df, create_table_query, upsert_query)
    context.add_output_metadata({"num_rows": df.shape[0]})


@asset(
    compute_kind="python",
    description="Load Translated Cities and Metadata",
    deps=get_asset_key_for_model([dbt_assets], "processed_dataset"),
)
def translations_city_mapping(context: AssetExecutionContext) -> None:
    """
    Loads translated cities and metadata from a JSON file into DuckDB.
    Source Data is webscrapped from China's wikipedia using a separate script.
    This materialization assumes the source files are in /data/static/mapping/
    Args:
        context (AssetExecutionContext): The execution context.
    """
    json_data = load_json_data(CITY_TRANSLATIONS_FILE_PATH)
    with duckdb.connect(os.fspath(DUCKDB_FILE_PATH)) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS TRANSLATIONS_CITY_MAPPING (
                SHIP_TO_CITY_CD VARCHAR PRIMARY KEY,
                SHIP_TO_CITY_CD_ENG VARCHAR,
                METADATA JSON,
                PROVINCE VARCHAR,
                PER_CAPITA_USD VARCHAR,
                TOTAL_GDP_USD VARCHAR
            );
            """
        )
        for item in json_data:
            metadata = json.dumps(item["metadata"])
            if item["SHIP_TO_CITY_CD_ENG"] in [
                "Shanghai",
                "Beijing",
                "Tianjin",
                "Chongqing",  # 4 municipalities
            ]:
                province = item["SHIP_TO_CITY_CD_ENG"]
            else:
                province = item["metadata"].get("Province", "").replace(
                    '"', ""
                ) or item["metadata"].get("Autonomous region", "").replace('"', "")
                if not province:
                    province = None

            per_capita_str = item["metadata"].get("Per capita", "")
            per_capita_usd = extract_per_capita(per_capita_str)

            total_gdp_str = item["metadata"].get("Total", "")
            total_gdp_usd = extract_total_gdp(total_gdp_str)

            con.execute(
                """
                INSERT INTO TRANSLATIONS_CITY_MAPPING (SHIP_TO_CITY_CD, SHIP_TO_CITY_CD_ENG, metadata, PROVINCE, PER_CAPITA_USD, TOTAL_GDP_USD)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(SHIP_TO_CITY_CD) DO UPDATE SET
                    SHIP_TO_CITY_CD_ENG = EXCLUDED.SHIP_TO_CITY_CD_ENG,
                    metadata = EXCLUDED.metadata,
                    PROVINCE = EXCLUDED.PROVINCE,
                    PER_CAPITA_USD = EXCLUDED.PER_CAPITA_USD,
                    TOTAL_GDP_USD = EXCLUDED.TOTAL_GDP_USD
                """,
                (
                    item["SHIP_TO_CITY_CD"],
                    item["SHIP_TO_CITY_CD_ENG"],
                    metadata,
                    province,
                    per_capita_usd,
                    total_gdp_usd,
                ),
            )


@asset(
    compute_kind="python",
    description="Load Translated Districts and Metadata",
    deps=get_asset_key_for_model([dbt_assets], "processed_dataset"),
)
def translations_district_mapping(context: AssetExecutionContext) -> None:
    """
    Loads translated cities and metadata from a JSON file into DuckDB.
    Source Data is webscrapped from China's wikipedia using a separate script.
    This materialization assumes the source files are in /data/static/mapping/

    Args:
        context (AssetExecutionContext): The execution context.
    """
    json_data = load_json_data(DISTRICTS_TRANSLATIONS_FILE_PATH)
    with duckdb.connect(os.fspath(DUCKDB_FILE_PATH)) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS TRANSLATIONS_DISTRICT_MAPPING (
                SHIP_TO_DISTRICT_NAME VARCHAR PRIMARY KEY,
                SHIP_TO_DISTRICT_NAME_ENG VARCHAR,
                METADATA JSON
            );
            """
        )
        for item in json_data:
            con.execute(
                """
                INSERT INTO TRANSLATIONS_DISTRICT_MAPPING (SHIP_TO_DISTRICT_NAME, SHIP_TO_DISTRICT_NAME_ENG, metadata)
                VALUES (?, ?, ?)
                ON CONFLICT(SHIP_TO_DISTRICT_NAME) DO UPDATE SET
                    SHIP_TO_DISTRICT_NAME_ENG = EXCLUDED.SHIP_TO_DISTRICT_NAME_ENG,
                    metadata = EXCLUDED.metadata
                """,
                (
                    item["SHIP_TO_DISTRICT_NAME"],
                    item["SHIP_TO_DISTRICT_NAME_ENG"],
                    json.dumps(item["metadata"]),
                ),
            )


@asset(
    compute_kind="python",
    description="Load Currency Rate Conversion",
    deps=get_asset_key_for_model([dbt_assets], "processed_dataset"),
)
def currency_code_mapping(context: AssetExecutionContext) -> None:
    """
    Loads currency rate conversion data into DuckDB.
    Just assume static mapping. Could also be done with dbt seed.
    Args:
        context (AssetExecutionContext): The execution context.
    """
    with duckdb.connect(os.fspath(DUCKDB_FILE_PATH)) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS CURRENCY_CODE_MAPPING (
                CURRENCY_CD VARCHAR PRIMARY KEY,
                MULTIPLIER FLOAT,
                DATE_RECORDED DATE
            );
            """
        )
        con.execute(
            """
            INSERT INTO CURRENCY_CODE_MAPPING (CURRENCY_CD, MULTIPLIER, DATE_RECORDED)
            VALUES 
                ('RMB', 1, CURRENT_DATE),
                ('USD', 7.28, CURRENT_DATE)
            ON CONFLICT (CURRENCY_CD) DO UPDATE SET
                MULTIPLIER = EXCLUDED.MULTIPLIER,
                DATE_RECORDED = EXCLUDED.DATE_RECORDED;
            """
        )


@asset(
    compute_kind="python",
    description="Load K-Means City Clustering Results",
    deps=get_asset_key_for_model([dbt_assets], "curated_dataset"),
)
def curated_city_cluster_results(context: AssetExecutionContext) -> None:
    """
    Loads K-Means city clustering results from a CSV file into DuckDB.
    Source Data is trained using scikit-learn in a separate script and
    then dumped to csv in /data/static/cluster.

    Args:
        context (AssetExecutionContext): The execution context.
    """
    with duckdb.connect(os.fspath(DUCKDB_FILE_PATH)) as con:
        con.execute("DROP TABLE IF EXISTS CURATED_CITY_CLUSTER_RESULTS")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS CURATED_CITY_CLUSTER_RESULTS (
                SHIP_TO_CITY_CD VARCHAR,
                RMB_DOLLARS DOUBLE,
                SHIP_TO_CITY_CD_ENG VARCHAR,
                PROVINCE VARCHAR,
                PER_CAPITA_USD VARCHAR,
                normalized_sales DOUBLE,
                cluster INTEGER
            );
            """
        )
        con.execute(
            f"COPY CURATED_CITY_CLUSTER_RESULTS FROM '{CITY_CLUSTER_RESULTS_FILE_PATH}' (HEADER, DELIMITER ',');"
        )
