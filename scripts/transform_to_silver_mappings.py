import duckdb
import json
from constants import (
    DUCKDB_FILE_PATH,
    CITY_TRANSLATIONS_FILE_PATH,
    DISTRICTS_TRANSLATIONS_FILE_PATH,
)


# Connect to DuckDB
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=False)

# Read the mapping city JSON file
with open(CITY_TRANSLATIONS_FILE_PATH, "r", encoding="utf-8") as file:
    json_data = json.load(file)


# Insert or update the JSON data into the table
for item in json_data:
    con.execute(
        """
    INSERT INTO TRANSLATIONS_CITY_MAPPING (SHIP_TO_CITY_CD, SHIP_TO_CITY_CD_ENG, metadata)
    VALUES (?, ?, ?)
    ON CONFLICT(SHIP_TO_CITY_CD) DO UPDATE SET
        SHIP_TO_CITY_CD_ENG = EXCLUDED.SHIP_TO_CITY_CD_ENG,
        metadata = EXCLUDED.metadata
    """,
        (
            item["SHIP_TO_CITY_CD"],
            item["SHIP_TO_CITY_CD_ENG"],
            json.dumps(item["metadata"]),
        ),
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
# Verify by running a SQL query on the DuckDB table
result_df = con.execute("SELECT * FROM TRANSLATIONS_CITY_MAPPING LIMIT 5").fetchdf()
print(result_df)
result_df = con.execute("SELECT * FROM TRANSLATIONS_DISTRICT_MAPPING LIMIT 5").fetchdf()
print(result_df)
result_df = con.execute("SELECT * FROM CURRENCY_CODE_MAPPING LIMIT 5").fetchdf()
print(result_df)

# Close the DuckDB connection
con.close()
