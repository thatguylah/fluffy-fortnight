import duckdb
from constants import DUCKDB_FILE_PATH

# Connect to DuckDB
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=False)

# Load necessary data from Silver Layer
processed_df = con.execute("SELECT * FROM PROCESSED_DATASET").fetchdf()
currency_code_df = con.execute("SELECT * FROM CURRENCY_CODE_MAPPING").fetchdf()
translations_city_df = con.execute("SELECT * FROM TRANSLATIONS_CITY_MAPPING").fetchdf()
translations_district_df = con.execute(
    "SELECT * FROM TRANSLATIONS_DISTRICT_MAPPING"
).fetchdf()

# Perform the required transformations with suffixes to avoid clashes
merged_df = processed_df.merge(
    translations_city_df, how="left", on="SHIP_TO_CITY_CD", suffixes=("", "_city")
)
merged_df = merged_df.merge(
    translations_district_df,
    how="left",
    on="SHIP_TO_DISTRICT_NAME",
    suffixes=("", "_district"),
)
merged_df = merged_df.merge(
    currency_code_df[["CURRENCY_CD", "MULTIPLIER"]], how="left", on="CURRENCY_CD"
)

# Calculate RMB_DOLLARS
merged_df["RMB_DOLLARS"] = merged_df["RPTG_AMT"] * merged_df["MULTIPLIER"]

# Select required columns for the Gold Layer
curated_df = merged_df[
    [
        "ORDER_ID",
        "ORDER_TIME_PST",
        "SHIP_TO_CITY_CD",
        "SHIP_TO_DISTRICT_NAME",
        "SHIP_TO_DISTRICT_NAME_ENG",
        "SHIP_TO_CITY_CD_ENG",
        "RMB_DOLLARS",
        "ORDER_QTY",
    ]
]

# Load the data into the Gold Layer table
con.register("curated_df", curated_df)
con.execute(
    """
INSERT INTO CURATED_DATASET
SELECT * FROM curated_df
ON CONFLICT(ORDER_ID) DO UPDATE SET
    ORDER_TIME_PST = EXCLUDED.ORDER_TIME_PST,
    SHIP_TO_CITY_CD = EXCLUDED.SHIP_TO_CITY_CD,
    SHIP_TO_CITY_CD_ENG = EXCLUDED.SHIP_TO_CITY_CD_ENG,
    SHIP_TO_DISTRICT_NAME = EXCLUDED.SHIP_TO_DISTRICT_NAME,
    SHIP_TO_DISTRICT_NAME_ENG = EXCLUDED.SHIP_TO_DISTRICT_NAME_ENG,
    RMB_DOLLARS = EXCLUDED.RMB_DOLLARS,
    ORDER_QTY = EXCLUDED.ORDER_QTY
"""
)

# Verify by running a SQL query on the DuckDB table
result_df = con.execute("SELECT * FROM CURATED_DATASET LIMIT 5").fetchdf()
print(result_df)

# Close the DuckDB connection
con.close()
