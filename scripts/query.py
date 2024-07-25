import duckdb
from constants import DUCKDB_FILE_PATH

# Create a DuckDB connection to a persistent database file
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=True)

# Query to find the city with the highest sales for each hour
highest_sales_per_hour = con.execute(
    """
    WITH HourlySales AS (
    SELECT
        c.SHIP_TO_CITY_CD,
        t.SHIP_TO_CITY_CD_ENG,
        ROUND(c.ORDER_TIME_PST / 10000) AS ORDER_HOUR_PST,
        SUM(c.RMB_DOLLARS) AS total_sales,
        ROW_NUMBER() OVER (PARTITION BY ROUND(c.ORDER_TIME_PST / 10000) ORDER BY SUM(c.RMB_DOLLARS) DESC) AS rank
    FROM
        CURATED_DATASET c
    LEFT JOIN
        TRANSLATIONS_CITY_MAPPING t
    ON
        c.SHIP_TO_CITY_CD = t.SHIP_TO_CITY_CD
    GROUP BY
        c.SHIP_TO_CITY_CD,
        t.SHIP_TO_CITY_CD_ENG,
        ROUND(c.ORDER_TIME_PST / 10000)
)
SELECT
    SHIP_TO_CITY_CD,
    SHIP_TO_CITY_CD_ENG,
    ORDER_HOUR_PST,
    total_sales
FROM
    HourlySales
WHERE
    rank = 1
ORDER BY
    ORDER_HOUR_PST;

    """
).fetchall()
print("City with the highest per-hour sales:", highest_sales_per_hour)

# Query to find the city and hour with the highest sales
highest_per_hour_sales = con.execute(
    """
    SELECT
        c.SHIP_TO_CITY_CD,
        t.SHIP_TO_CITY_CD_ENG,
        ROUND(c.ORDER_TIME_PST / 10000) AS ORDER_HOUR_PST,
        SUM(c.RMB_DOLLARS) AS total_sales
    FROM
        CURATED_DATASET c
    LEFT JOIN
        TRANSLATIONS_CITY_MAPPING t
    ON
        c.SHIP_TO_CITY_CD = t.SHIP_TO_CITY_CD
    GROUP BY
        c.SHIP_TO_CITY_CD,
        t.SHIP_TO_CITY_CD_ENG,
        ORDER_HOUR_PST
    ORDER BY
        total_sales DESC
    LIMIT 5;

"""
).fetchall()

print("City with the highest per-hour sales:", highest_per_hour_sales)

# Query to find the city with the highest average sales by district
highest_avg_sales_by_district = con.execute(
    """
    SELECT * FROM CURATED_CITY_CLUSTER_RESULTS;
"""
).fetchdf()

print("City with the highest average sales by district:", highest_avg_sales_by_district)

# Close the DuckDB connection
con.close()
