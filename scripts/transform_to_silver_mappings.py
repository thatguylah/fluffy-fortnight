import duckdb
import json
import re
from constants import (
    DUCKDB_FILE_PATH,
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


# Connect to DuckDB
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=False)

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
