import duckdb
from constants import DUCKDB_FILE_PATH

# Connect to DuckDB
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=False)

# List all indexes
indexes_query = """
SELECT *
FROM duckdb_indexes;
"""
indexes = con.execute(indexes_query).fetchdf()

# Display indexes
print("Indexes in the database:")
print(indexes)

# Drop all indexes
for index_name in indexes["index_name"]:
    drop_index_query = f"DROP INDEX {index_name};"
    con.execute(drop_index_query)
    print(f"Dropped index: {index_name}")

# Close the connection
con.close()
