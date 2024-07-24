import duckdb
from constants import DUCKDB_FILE_PATH

# Create a DuckDB connection to a persistent database file
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=True)

result_df = con.execute(
    """
    SELECT DISTINCT SHIP_TO_DISTRICT_NAME FROM (
            SELECT SHIP_TO_DISTRICT_NAME FROM RAW_DATASET_2
            UNION ALL
            SELECT SHIP_TO_DISTRICT_NAME FROM RAW_MAPPING
    )
"""
).fetchdf()

# Print the result
print(result_df)
