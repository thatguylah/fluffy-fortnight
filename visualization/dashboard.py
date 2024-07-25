import streamlit as st
import duckdb
import plotly.express as px

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

# Query data from the CURATED_DATASET table
query = """
SELECT SHIP_TO_CITY_CD, SHIP_TO_CITY_CD_ENG, SUM(RMB_DOLLARS) AS total_sales
FROM CURATED_DATASET
GROUP BY SHIP_TO_CITY_CD, SHIP_TO_CITY_CD_ENG
ORDER BY total_sales DESC
"""
df = con.execute(query).fetchdf()

# Ensure the dataframe contains the total_sales column
if "total_sales" not in df.columns:
    st.error("Column 'total_sales' not found in the dataframe.")
else:
    # Create a choropleth map
    fig = px.choropleth(
        df,
        geojson="https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json",
        locations="SHIP_TO_CITY_CD",
        featureidkey="properties.name",
        color="total_sales",
        hover_name="SHIP_TO_CITY_CD_ENG",
        color_continuous_scale="Viridis",
        range_color=(0, df["total_sales"].max()),
        scope="asia",
        labels={"total_sales": "Total Sales (RMB)"},
    )

    fig.update_geos(
        scope="asia",
        projection_type="mercator",
        showland=True,
        landcolor="rgb(217, 217, 217)",
        showocean=True,
        oceancolor="rgb(204, 204, 255)",
    )

    # Streamlit app layout
    st.plotly_chart(fig)

    # Add explanatory text
    st.write(
        "This map shows the total sales in different regions of China. The darker the color, the higher the total sales."
    )

#####################################
st.header("City Level Metadata")
query = """
-- Exploding METADATA JSON column into separate columns
SELECT 
    SHIP_TO_CITY_CD,
    SHIP_TO_CITY_CD_ENG,
    json_extract(METADATA, '$.Province')::VARCHAR AS PROVINCE,
    json_extract(METADATA, '$."Per capita"')::VARCHAR AS PER_CAPITA,
    json_extract(METADATA, '$."Party Secretary"') AS PARTY_SECRETARY
FROM 
    TRANSLATIONS_CITY_MAPPING
LIMIT 10;
"""
city_metadata_df = con.execute(query).fetchdf()
st.write(city_metadata_df)

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
