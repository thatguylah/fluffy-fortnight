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
)


def extract_per_capita(per_capita_str):
    match = re.search(r"US\$ ([\d,]+)", per_capita_str)
    if match:
        return match.group(1).replace(",", "")
    return None


def extract_total_gdp(total_gdp_str):
    match_billion = re.search(r"US\$ ([\d\.]+) billion", total_gdp_str)
    if match_billion:
        return int(float(match_billion.group(1)) * 1e9)
    match_simple = re.search(r"US\$ ([\d,]+)", total_gdp_str)
    if match_simple:
        return int(match_simple.group(1).replace(",", ""))
    return None


@dbt_assets(manifest=dbt_manifest_path)
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset(compute_kind="python", description="Extract and Loading raw Excel dataset")
def raw_dataset_1(context: AssetExecutionContext) -> None:
    # Create a DuckDB connection to a persistent database file
    con = duckdb.connect(os.fspath(DUCKDB_FILE_PATH))
    con.execute("SET GLOBAL pandas_analyze_sample=100000000")

    # Read the Excel file into a Pandas DataFrame for RAW_DATASET_1
    df_dataset1 = pd.read_excel(
        INPUT_EXCEL_PATH,
        sheet_name="DATA",
    )
    # Rename the column
    df_dataset1.rename(columns={"ORDER_TIME  (PST)": "ORDER_TIME_PST"}, inplace=True)

    # Display the first few rows of the DataFrame to verify the contents
    print("DataFrame loaded from Excel file (RAW_DATASET_1):")
    print(df_dataset1.head())

    # Register the DataFrame as a DuckDB table
    con.register("df_dataset1", df_dataset1)

    # Perform the upsert operation for RAW_DATASET_1
    upsert_query = """
    CREATE TABLE IF NOT EXISTS RAW_DATASET_1 (
    ORDER_ID VARCHAR PRIMARY KEY,
    ORDER_TIME_PST VARCHAR,
    CITY_DISTRICT_ID INT,
    RPTG_AMT DECIMAL(18,2),
    CURRENCY_CD VARCHAR,
    ORDER_QTY VARCHAR
    );
    INSERT INTO RAW_DATASET_1
    SELECT * FROM df_dataset1
    ON CONFLICT(ORDER_ID) DO UPDATE SET
        ORDER_TIME_PST = EXCLUDED.ORDER_TIME_PST,
        CITY_DISTRICT_ID = EXCLUDED.CITY_DISTRICT_ID,
        RPTG_AMT = EXCLUDED.RPTG_AMT,
        CURRENCY_CD = EXCLUDED.CURRENCY_CD,
        ORDER_QTY = EXCLUDED.ORDER_QTY;
    """

    con.execute(upsert_query)
    # Log some metadata about the table we just wrote. It will show up in the UI.
    context.add_output_metadata({"num_rows": df_dataset1.shape[0]})


@asset(compute_kind="python", description="Extract and Loading raw Excel dataset")
def raw_dataset_2(context: AssetExecutionContext) -> None:
    con = duckdb.connect(os.fspath(DUCKDB_FILE_PATH))
    con.execute("SET GLOBAL pandas_analyze_sample=100000000")

    # Read the JSON file into a Pandas DataFrame for RAW_DATASET_2
    df_dataset2 = pd.read_json(INPUT_JSON_PATH)

    # Display the first few rows of the DataFrame to verify the contents
    print("DataFrame loaded from JSON file (RAW_DATASET_2):")
    print(df_dataset2.head())

    # Register the DataFrame as a DuckDB table
    con.register("df_dataset2", df_dataset2)

    # Perform the upsert operation for RAW_DATASET_2
    upsert_query = """
    CREATE TABLE IF NOT EXISTS RAW_DATASET_2 (
    ORDER_ID VARCHAR PRIMARY KEY,
    ORDER_TIME_PST BIGINT,
    SHIP_TO_DISTRICT_NAME VARCHAR,
    SHIP_TO_CITY_CD VARCHAR,
    RPTG_AMT DECIMAL(18,2),
    CURRENCY_CD VARCHAR,
    ORDER_QTY INT
    );
    INSERT INTO RAW_DATASET_2
    SELECT * FROM df_dataset2
    ON CONFLICT(ORDER_ID) DO UPDATE SET
        ORDER_TIME_PST = EXCLUDED.ORDER_TIME_PST,
        SHIP_TO_CITY_CD = EXCLUDED.SHIP_TO_CITY_CD,
        SHIP_TO_DISTRICT_NAME = EXCLUDED.SHIP_TO_DISTRICT_NAME,
        RPTG_AMT = EXCLUDED.RPTG_AMT,
        CURRENCY_CD = EXCLUDED.CURRENCY_CD,
        ORDER_QTY = EXCLUDED.ORDER_QTY;
    """

    con.execute(upsert_query)
    # Log some metadata about the table we just wrote. It will show up in the UI.
    context.add_output_metadata({"num_rows": df_dataset2.shape[0]})


@asset(compute_kind="python", description="Extract and Loading raw Excel dataset")
def raw_mapping(context: AssetExecutionContext) -> None:
    con = duckdb.connect(os.fspath(DUCKDB_FILE_PATH))
    con.execute("SET GLOBAL pandas_analyze_sample=100000000")

    # Read the JSON file into a Pandas DataFrame for RAW_DATASET_2
    df_mapping = pd.read_excel(
        INPUT_EXCEL_PATH,
        sheet_name="CITY_DISTRICT_MAP",
    )

    # Display the first few rows of the DataFrame to verify the contents
    print("DataFrame loaded from Excel file (RAW_MAPPING):")
    print(df_mapping.head())

    # Register the DataFrame as a DuckDB table
    con.register("df_mapping", df_mapping)

    # Perform the upsert operation for RAW_MAPPING
    upsert_query = """
    CREATE TABLE IF NOT EXISTS RAW_MAPPING (
    CITY_DISTRICT_ID INT PRIMARY KEY,
    SHIP_TO_CITY_CD VARCHAR,
    SHIP_TO_DISTRICT_NAME VARCHAR
    );
    INSERT INTO RAW_MAPPING
    SELECT * FROM df_mapping
    ON CONFLICT(CITY_DISTRICT_ID) DO UPDATE SET
        SHIP_TO_CITY_CD = EXCLUDED.SHIP_TO_CITY_CD,
        SHIP_TO_DISTRICT_NAME = EXCLUDED.SHIP_TO_DISTRICT_NAME;
    """

    con.execute(upsert_query)
    # Log some metadata about the table we just wrote. It will show up in the UI.
    context.add_output_metadata({"num_rows": df_mapping.shape[0]})


@asset(
    compute_kind="python",
    description="Translated Cities and Metadata",
    deps=get_asset_key_for_model([dbt_assets], "processed_dataset"),
)
def translations_city_mapping(context: AssetExecutionContext) -> None:
    # Connect to DuckDB
    con = duckdb.connect(os.fspath(DUCKDB_FILE_PATH))
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
    # Read the mapping city JSON file
    with open(CITY_TRANSLATIONS_FILE_PATH, "r", encoding="utf-8") as file:
        json_data = json.load(file)

    # Insert or update the JSON data into the table
    for item in json_data:
        metadata = json.dumps(item["metadata"])
        province = item["metadata"].get("Province", "").replace('"', "")
        if not province:
            province = item["SHIP_TO_CITY_CD_ENG"]

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
    # Close the DuckDB connection
    con.close()


@asset(
    compute_kind="python",
    description="Translated Districts and Metadata",
    deps=get_asset_key_for_model([dbt_assets], "processed_dataset"),
)
def translations_district_mapping(context: AssetExecutionContext) -> None:
    # Connect to DuckDB
    con = duckdb.connect(os.fspath(DUCKDB_FILE_PATH))
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS TRANSLATIONS_DISTRICT_MAPPING (
        SHIP_TO_DISTRICT_NAME VARCHAR PRIMARY KEY,
        SHIP_TO_DISTRICT_NAME_ENG VARCHAR,
        METADATA JSON
    );
    """
    )
    # Read the mapping district JSON file
    with open(DISTRICTS_TRANSLATIONS_FILE_PATH, "r", encoding="utf-8") as file:
        json_data = json.load(file)

    # Insert or update the JSON data into the table
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
    # Close the DuckDB connection
    con.close()


@asset(
    compute_kind="python",
    description="Currency Rate Conversion",
    deps=get_asset_key_for_model([dbt_assets], "processed_dataset"),
)
def currency_code_mapping(context: AssetExecutionContext) -> None:
    # Connect to DuckDB
    con = duckdb.connect(os.fspath(DUCKDB_FILE_PATH))
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
    # Close the DuckDB connection
    con.close()
