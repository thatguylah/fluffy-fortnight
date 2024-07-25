import duckdb
from constants import DUCKDB_FILE_PATH

ddl_statements = """
-- Create indexes after data insertion
CREATE INDEX idx_curated_order_time ON CURATED_DATASET (ORDER_TIME_PST);
CREATE INDEX idx_curated_ship_to_city ON CURATED_DATASET (SHIP_TO_CITY_CD);
CREATE INDEX idx_curated_ship_to_district ON CURATED_DATASET (SHIP_TO_DISTRICT_NAME);
CREATE INDEX idx_curated_rmb_dollars ON CURATED_DATASET (RMB_DOLLARS);
"""
# Create a DuckDB connection to a persistent database file
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=False)

# Execute the DDL statements
con.execute(ddl_statements)

# Query to list all indexes
indexes = con.execute("SELECT * FROM duckdb_indexes;").fetchall()
print("Indexes in the database:", indexes)

# Close the DuckDB connection
con.close()
