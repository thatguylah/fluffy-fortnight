import duckdb
import pandas as pd
from pydantic_models.RawDatasets import (
    RawDatasetExcelModel,
    validate_and_replace,
    RawDatasetJSONModel,
    validate_only,
)
from constants import DUCKDB_FILE_PATH


# Create a DuckDB connection to a persistent database file
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=False)
con.execute("SET GLOBAL pandas_analyze_sample=100000000")

# Fetch
df = con.execute("SELECT * FROM RAW_DATASET_1").fetchdf()
df_json = con.execute("SELECT * FROM RAW_DATASET_2").fetchdf()

# Fetch CITY_DISTRICT_IDs from RAW_MAPPING table
raw_mapping_df = con.execute(
    "SELECT CITY_DISTRICT_ID, SHIP_TO_CITY_CD, SHIP_TO_DISTRICT_NAME FROM RAW_MAPPING"
).fetchdf()

RawDatasetExcelModel.raw_mapping_ids = set(raw_mapping_df["CITY_DISTRICT_ID"].tolist())
RawDatasetJSONModel.raw_mapping_city_ids = set(
    raw_mapping_df["SHIP_TO_CITY_CD"].tolist()
)
RawDatasetJSONModel.raw_mapping_district_names = set(
    raw_mapping_df["SHIP_TO_DISTRICT_NAME"].tolist()
)
# Validate the data and replace invalid values with NaN
df_cleaned, validation_errors = validate_and_replace(df)

df_json, validation_errors_2 = validate_only(df_json)

# Perform the left join with raw_mapping_df to add SHIP_TO_CITY_CD and SHIP_TO_DISTRICT_NAME columns
df_merged = pd.merge(df_cleaned, raw_mapping_df, on="CITY_DISTRICT_ID", how="left")

# Drop the CITY_DISTRICT_ID column from df_merged
df_merged.drop(columns=["CITY_DISTRICT_ID"], inplace=True)

# Register the cleaned and merged DataFrame as a DuckDB table
con.register("df_merged", df_merged)

# Create and insert data into the PROCESSED_DATASET table with upsert
# Insert data into the table
con.execute(
    """
    INSERT INTO PROCESSED_DATASET (
        ORDER_ID, ORDER_TIME_PST, RPTG_AMT, CURRENCY_CD, ORDER_QTY, SHIP_TO_CITY_CD, SHIP_TO_DISTRICT_NAME
    )
    SELECT ORDER_ID, ORDER_TIME_PST, RPTG_AMT, CURRENCY_CD, ORDER_QTY, SHIP_TO_CITY_CD, SHIP_TO_DISTRICT_NAME
    FROM (
        SELECT ORDER_ID, ORDER_TIME_PST, RPTG_AMT, CURRENCY_CD, ORDER_QTY, SHIP_TO_CITY_CD, SHIP_TO_DISTRICT_NAME FROM df_merged
        UNION ALL
        SELECT ORDER_ID, ORDER_TIME_PST, RPTG_AMT, CURRENCY_CD, ORDER_QTY, SHIP_TO_CITY_CD, SHIP_TO_DISTRICT_NAME FROM df_json
    )
    ON CONFLICT(ORDER_ID) DO UPDATE SET
        ORDER_TIME_PST = EXCLUDED.ORDER_TIME_PST,
        RPTG_AMT = EXCLUDED.RPTG_AMT,
        CURRENCY_CD = EXCLUDED.CURRENCY_CD,
        ORDER_QTY = EXCLUDED.ORDER_QTY,
        SHIP_TO_CITY_CD = EXCLUDED.SHIP_TO_CITY_CD,
        SHIP_TO_DISTRICT_NAME = EXCLUDED.SHIP_TO_DISTRICT_NAME
    """
)

# Convert validation errors to a DataFrame
errors_df = pd.concat(
    [pd.DataFrame(validation_errors), pd.DataFrame(validation_errors_2)]
)

# Load errors into EXCEPTIONS_DATASET table
con.execute("DROP TABLE IF EXISTS EXCEPTIONS_DATASET")
con.execute(
    """
CREATE TABLE EXCEPTIONS_DATASET (
    ORDER_ID VARCHAR,
    ERROR_MESSAGE VARCHAR
)
"""
)
con.register("errors_df", errors_df)
con.execute(
    """
INSERT INTO EXCEPTIONS_DATASET
SELECT ORDER_ID, json_group_array(errors) AS ERROR_MESSAGE
FROM errors_df
GROUP BY ORDER_ID
"""
)

# Verify by running a SQL query on the PROCESSED_DATASET table
result_df = con.execute("SELECT * FROM PROCESSED_DATASET LIMIT 5").fetchdf()
errors_df = con.execute("SELECT * FROM EXCEPTIONS_DATASET LIMIT 5").fetchdf()

# Print the result
print("Cleaned data from PROCESSED_DATASET table in DuckDB:")
print(result_df)
print("Error data from EXCEPTIONS_DATASET table in DuckDB:")
print(errors_df)

# Close the DuckDB connection
con.close()
