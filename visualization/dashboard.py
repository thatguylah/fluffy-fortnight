import streamlit as st
import duckdb
import plotly.express as px
import json

# Connect to DuckDB
con = duckdb.connect(database="data/output/datawarehouse.duckdb", read_only=True)


# Query to get the data
@st.cache_data
def load_data():
    query = """
    SELECT * FROM CURATED_DATASET
    """
    df = con.execute(query).fetchdf()
    return df


@st.cache_data
def load_cluster_data():
    query = """
    SELECT * FROM CURATED_CITY_CLUSTER_RESULTS
    """
    df = con.execute(query).fetchdf()
    return df


# Load data
df = load_data()
cluster_df = load_cluster_data()

# Streamlit App
st.title("Sales Performance Dashboard")
# Load China geojson data
with open("data/static/geojson/province_geojson.json") as response:
    china_geojson = json.loads(response.read())
# Query to get province-wise spending data
query = """
SELECT 
    PROVINCE,
    SUM(RMB_DOLLARS) AS TOTAL_SPENDING,
    COUNT(DISTINCT c.SHIP_TO_CITY_CD) AS TOTAL_COUNT_OF_CITIES,
    COUNT(DISTINCT d.SHIP_TO_DISTRICT_NAME) AS TOTAL_COUNT_OF_DISTRICTS
FROM 
    TRANSLATIONS_CITY_MAPPING t
JOIN
    CURATED_DATASET c ON t.SHIP_TO_CITY_CD = c.SHIP_TO_CITY_CD
LEFT JOIN
    TRANSLATIONS_DISTRICT_MAPPING d ON c.SHIP_TO_DISTRICT_NAME = d.SHIP_TO_DISTRICT_NAME
GROUP BY 
    PROVINCE
ORDER BY 
    TOTAL_SPENDING DESC;
"""
# Execute the query and load data into a DataFrame
df = con.execute(query).fetchdf()

# Create a choropleth map
fig = px.choropleth(
    df,
    geojson=china_geojson,
    locations="PROVINCE",
    featureidkey="properties.NAME_1",
    color="TOTAL_SPENDING",
    hover_name="PROVINCE",
    hover_data={
        "TOTAL_SPENDING": ":,.2f",
        "TOTAL_COUNT_OF_CITIES": True,
        "TOTAL_COUNT_OF_DISTRICTS": True,
    },
    color_continuous_scale="Viridis",
    labels={
        "TOTAL_SPENDING": "Total Spending",
        "TOTAL_COUNT_OF_CITIES": "Total Count of Cities",
        "TOTAL_COUNT_OF_DISTRICTS": "Total Count of Districts",
    },
)


fig.update_geos(
    fitbounds="locations",
    visible=True,
    showsubunits=True,
    showcoastlines=True,
    coastlinecolor="Black",
    showocean=True,
    oceancolor="LightBlue",
)
fig.update_layout(title_text="Total Spending by Province in China")

# Display the map in Streamlit
st.plotly_chart(fig)

# Add explanatory text
st.write(
    "This map shows the total sales in different regions of China. The darker the color, the higher the total sales."
)

#####################################
st.header("City Level Metadata")
query = """
-- Exploding METADATA JSON column into separate columns
SELECT *
FROM 
    TRANSLATIONS_CITY_MAPPING
LIMIT 10;
"""
city_metadata_df = con.execute(query).fetchdf()
st.write(city_metadata_df)

# Query to get top 10 provinces by sales
top_provinces_query = """
WITH CityProvinceMapping AS (
    SELECT
        SHIP_TO_CITY_CD,
        PROVINCE
    FROM
        TRANSLATIONS_CITY_MAPPING
),
CitySales AS (
    SELECT
        d.SHIP_TO_CITY_CD,
        p.PROVINCE,
        SUM(d.RMB_DOLLARS) AS total_sales
    FROM
        CURATED_DATASET d
    JOIN
        CityProvinceMapping p ON d.SHIP_TO_CITY_CD = p.SHIP_TO_CITY_CD
    GROUP BY
        d.SHIP_TO_CITY_CD,
        p.PROVINCE
)
SELECT
    PROVINCE,
    SUM(total_sales) AS province_total_sales
FROM
    CitySales
GROUP BY
    PROVINCE
ORDER BY
    province_total_sales DESC
LIMIT 10;
"""
top_provinces_df = con.execute(top_provinces_query).fetchdf()
st.header("Top 10 Provinces in Sales")
st.write(top_provinces_df)
fig = px.bar(
    top_provinces_df,
    x="PROVINCE",
    y="province_total_sales",
    title="Top 10 Provinces in Sales",
)
st.plotly_chart(fig)

st.title("Percentage of Cities with Valid Translation")
query = """
WITH unique_cities AS (
    SELECT DISTINCT SHIP_TO_CITY_CD
    FROM (
        SELECT SHIP_TO_CITY_CD FROM RAW_DATASET_2
        UNION ALL
        SELECT SHIP_TO_CITY_CD FROM RAW_MAPPING
    )
),
translated_cities AS (
    SELECT COUNT(*) AS translated_count
    FROM TRANSLATIONS_CITY_MAPPING
),
total_unique_cities AS (
    SELECT COUNT(*) AS unique_count
    FROM unique_cities
)
SELECT 
    translated_count,
    unique_count,
    (translated_count::FLOAT / unique_count::FLOAT) * 100 AS percentage
FROM
    translated_cities, total_unique_cities;
"""
result_df = con.execute(query).fetchdf()
st.write(result_df)

query = """
WITH unique_districts AS (
    SELECT DISTINCT SHIP_TO_DISTRICT_NAME
    FROM (
        SELECT SHIP_TO_DISTRICT_NAME FROM RAW_DATASET_2
        UNION ALL
        SELECT SHIP_TO_DISTRICT_NAME FROM RAW_MAPPING
    )
),
translated_districts AS (
    SELECT COUNT(*) AS translated_count
    FROM TRANSLATIONS_DISTRICT_MAPPING
),
total_unique_districts AS (
    SELECT COUNT(*) AS unique_count
    FROM unique_districts
)
SELECT 
    translated_count,
    unique_count,
    (translated_count::FLOAT / unique_count::FLOAT) * 100 AS percentage
FROM
    translated_districts, total_unique_districts;
"""
result_df = con.execute(query).fetchdf()
st.title("Percentage of Districts with Valid Translation")
st.write(result_df)

st.header("Top 10 Cities In Sales")
top_10_cities_query = """
SELECT SHIP_TO_CITY_CD, SHIP_TO_CITY_CD_ENG, SUM(RMB_DOLLARS) as total_sales
FROM CURATED_DATASET
GROUP BY SHIP_TO_CITY_CD, SHIP_TO_CITY_CD_ENG
ORDER BY total_sales DESC
LIMIT 10
"""
top_10_cities_df = con.execute(top_10_cities_query).fetchdf()
st.write(top_10_cities_df)

st.header("Top 10 Cities in Transaction Count")
top_10_cities_transactions_query = """
SELECT
    SHIP_TO_CITY_CD,
    SHIP_TO_CITY_CD_ENG,
    COUNT(*) AS order_count
FROM
    CURATED_DATASET
GROUP BY
    SHIP_TO_CITY_CD, SHIP_TO_CITY_CD_ENG,
ORDER BY
    order_count DESC
LIMIT 10;
"""
top_10_cities_count_df = con.execute(top_10_cities_transactions_query).fetchdf()
st.write(top_10_cities_count_df)

st.header("Top 10 Transactions By Amount")
top_10_transactions_query = """
SELECT
    *
FROM
    CURATED_DATASET
ORDER BY
    RMB_DOLLARS DESC
LIMIT 10;

"""
top_10_transactions_df = con.execute(top_10_transactions_query).fetchdf()
st.write(top_10_transactions_df)

st.markdown("## Find the city with the highest per-hour sales")
st.markdown(
    "Analysis: This question looks like it can be interpreted in 2 ways. Either 1) For each hour, find the city with the highest spending or 2) Find the city-hour pair with the highest spending. Why not both? The interesting analysis is that while Shanghai tops the charts in sales across all times of day, at certain peak periods, other cities can do better in sales than Shanghai at off-peak periods. Refer to the next two figures."
)
# City with the highest per-hour sales
st.header("City with the Highest Per-Hour Sales")
hourly_sales_query = """
WITH HourlySales AS (
    SELECT
        SHIP_TO_CITY_CD,
        ROUND(ORDER_TIME_PST / 10000) AS ORDER_HOUR_PST,
        SUM(RMB_DOLLARS) AS total_sales,
        ROW_NUMBER() OVER (PARTITION BY ROUND(ORDER_TIME_PST / 10000) ORDER BY SUM(RMB_DOLLARS) DESC) AS rank
    FROM
        CURATED_DATASET
    GROUP BY
        SHIP_TO_CITY_CD,
        ROUND(ORDER_TIME_PST / 10000)
)
SELECT
    SHIP_TO_CITY_CD,
    ORDER_HOUR_PST,
    total_sales
FROM
    HourlySales
WHERE
    rank = 1
ORDER BY
    ORDER_HOUR_PST;
"""
hourly_sales_df = con.execute(hourly_sales_query).fetchdf()
st.write(hourly_sales_df)

# City pair with the highest spendings
st.header("Top 10 City-Hour Pair with the Highest Sales")
city_hour_pair_query = """
SELECT 
    SHIP_TO_CITY_CD,
    ROUND(ORDER_TIME_PST/10000) AS ORDER_HOUR_PST,
    SUM(RMB_DOLLARS) AS total_sales
FROM 
    CURATED_DATASET
GROUP BY 
    SHIP_TO_CITY_CD,
    ORDER_HOUR_PST
ORDER BY 
    total_sales DESC
LIMIT 10;
"""
city_hour_pair_sales_df = con.execute(city_hour_pair_query).fetchdf()
st.write(city_hour_pair_sales_df)
# City with the highest average sales by district
st.header("City with the Highest Average Sales by District")
average_sales_query = """
WITH city_district_sales AS (
    SELECT
        SHIP_TO_CITY_CD,
        SHIP_TO_DISTRICT_NAME,
        SUM(RMB_DOLLARS) AS total_sales,
        COUNT(*) AS transaction_count
    FROM
        CURATED_DATASET
    GROUP BY
        SHIP_TO_CITY_CD, SHIP_TO_DISTRICT_NAME
),
average_sales_per_district AS (
    SELECT
        SHIP_TO_CITY_CD,
        SHIP_TO_DISTRICT_NAME,
        total_sales / transaction_count AS avg_sales
    FROM
        city_district_sales
),
city_avg_sales AS (
    SELECT
        SHIP_TO_CITY_CD,
        AVG(avg_sales) AS city_avg_sales
    FROM
        average_sales_per_district
    GROUP BY
        SHIP_TO_CITY_CD
)
SELECT
    SHIP_TO_CITY_CD,
    city_avg_sales
FROM
    city_avg_sales
ORDER BY
    city_avg_sales DESC
LIMIT 10;

"""
average_sales_df = con.execute(average_sales_query).fetchdf()
st.write(average_sales_df)

# Curated Dataset
st.header("Curated Dataset")
curated_dataset_query = """
SELECT * FROM CURATED_DATASET LIMIT 5;
"""
st.write(con.execute(curated_dataset_query).fetchdf())
# Visualizations
st.header("Visualizations")

fig = px.scatter(
    cluster_df,
    x="SHIP_TO_CITY_CD",
    y="RMB_DOLLARS",
    color="cluster",
    title="City Clusters Based on Sales",
)
st.plotly_chart(fig)

# # Total Sales by City
# fig = px.bar(df, x="SHIP_TO_CITY_CD", y="RMB_DOLLARS", title="Total Sales by City")
# st.plotly_chart(fig)

# # Total Sales by City
# fig = px.bar(
#     df,
#     x="SHIP_TO_CITY_CD_ENG",
#     y="RMB_DOLLARS",
#     title="Total Sales by City with English Names",
# )
# st.plotly_chart(fig)


# # Total Sales by Hour
# fig = px.bar(
#     hourly_sales_df, x="ORDER_HOUR_PST", y="total_sales", title="Total Sales by Hour"
# )
# st.plotly_chart(fig)

# Close the DuckDB connection
con.close()
