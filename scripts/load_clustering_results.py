import duckdb
from constants import DUCKDB_FILE_PATH, CITY_CLUSTER_RESULTS_FILE_PATH

# Connect to DuckDB
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=False)

# Define the CSV file path
csv_file_path = CITY_CLUSTER_RESULTS_FILE_PATH

con.execute("DROP TABLE IF EXISTS CURATED_CITY_CLUSTER_RESULTS")

# Create the table structure before loading the data
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
    )
"""
)

# Load the CSV file into the table with specified parser options
con.execute(
    f"COPY CURATED_CITY_CLUSTER_RESULTS FROM '{CITY_CLUSTER_RESULTS_FILE_PATH}' (HEADER, DELIMITER ',');"
)


# Verify the data
result = con.execute("SELECT * FROM CURATED_CITY_CLUSTER_RESULTS LIMIT 5").fetchdf()
print(result)

# Close the DuckDB connection
con.close()
