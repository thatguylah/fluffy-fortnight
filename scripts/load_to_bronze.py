import duckdb
import pandas as pd
from constants import EXCEL_FILE_PATH, JSON_FILE_PATH, DUCKDB_FILE_PATH

# Create a DuckDB connection to a persistent database file
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=False)
con.execute("SET GLOBAL pandas_analyze_sample=100000000")

# Read the Excel file into a Pandas DataFrame for RAW_DATASET_1
df_dataset1 = pd.read_excel(EXCEL_FILE_PATH, sheet_name="DATA")
# Rename the column
df_dataset1.rename(columns={"ORDER_TIME  (PST)": "ORDER_TIME_PST"}, inplace=True)

# Display the first few rows of the DataFrame to verify the contents
print("DataFrame loaded from Excel file (RAW_DATASET_1):")
print(df_dataset1.head())

# Register the DataFrame as a DuckDB table
con.register("df_dataset1", df_dataset1)

# Perform the upsert operation for RAW_DATASET_1
upsert_query = """
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

# Read the Excel file into a Pandas DataFrame for RAW_MAPPING
df_mapping = pd.read_excel(EXCEL_FILE_PATH, sheet_name="CITY_DISTRICT_MAP")

# Display the first few rows of the DataFrame to verify the contents
print("DataFrame loaded from Excel file (RAW_MAPPING):")
print(df_mapping.head())

# Register the DataFrame as a DuckDB table
con.register("df_mapping", df_mapping)

# Perform the upsert operation for RAW_MAPPING
upsert_query = """
INSERT INTO RAW_MAPPING
SELECT * FROM df_mapping
ON CONFLICT(CITY_DISTRICT_ID) DO UPDATE SET
    SHIP_TO_CITY_CD = EXCLUDED.SHIP_TO_CITY_CD,
    SHIP_TO_DISTRICT_NAME = EXCLUDED.SHIP_TO_DISTRICT_NAME;
"""

con.execute(upsert_query)

# Read the JSON file into a Pandas DataFrame for RAW_DATASET_2
df_dataset2 = pd.read_json(JSON_FILE_PATH)

# Display the first few rows of the DataFrame to verify the contents
print("DataFrame loaded from JSON file (RAW_DATASET_2):")
print(df_dataset2.head())

# Register the DataFrame as a DuckDB table
con.register("df_dataset2", df_dataset2)

# Perform the upsert operation for RAW_DATASET_2
upsert_query = """
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

# Verify by running a SQL query on the DuckDB tables
result_df1 = con.execute("SELECT * FROM RAW_DATASET_1 LIMIT 5").fetchdf()
result_df2 = con.execute("SELECT * FROM RAW_MAPPING LIMIT 5").fetchdf()
result_df3 = con.execute("SELECT * FROM RAW_DATASET_2 LIMIT 5").fetchdf()

# Print the results
print("Data from RAW_DATASET_1 table in DuckDB:")
print(result_df1)
print("Data from RAW_MAPPING table in DuckDB:")
print(result_df2)
print("Data from RAW_DATASET_2 table in DuckDB:")
print(result_df3)

# Close the DuckDB connection
con.close()
