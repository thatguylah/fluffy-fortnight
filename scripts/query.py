import duckdb
from constants import DUCKDB_FILE_PATH

# Create a DuckDB connection to a persistent database file
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=True)

result_df = con.execute(
    """
    SELECT * FROM EXCEPTIONS_DATASET;
"""
).fetchdf()

# Print the result
print(result_df)
