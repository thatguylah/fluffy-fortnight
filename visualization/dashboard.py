import streamlit as st
import duckdb
import plotly.express as px
import json
from queries import (
    AGG_PROVINCE_SPENDING,
    AGG_TOP_10_CITIES_SPENDING,
    AGG_TOP_10_CITIES_TRANSACTION_COUNT,
    AGG_TOP_10_PROVINCE_SPENDING,
    ALL_CITY_MAPPING,
    ALL_TOP_10_TRANSACTIONS,
    CORR_TOTAL_SPEND_GDP_PER_CAPITA,
    RANKED_TOP_10_CITIES_HIGHEST_DISTRICT_AVG,
    RANKED_TOP_10_CITY_HOUR_PAIR,
    RANKED_TOP_CITY_PER_HOUR,
    PERCENTAGE_OF_VALID_CITY_TRANSLATIONS,
    PERCENTAGE_OF_VALID_DISTRICTS_TRANSLATIONS,
    AGG_TOTAL_SPEND_PER_HOUR,
)
from constants import DUCKDB_FILE_PATH, GEOJSON_FILE_PATH

# Connect to DuckDB
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=True)

st.set_page_config(layout="wide")


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
with open(GEOJSON_FILE_PATH) as response:
    china_geojson = json.loads(response.read())

# Execute the query and load data into a DataFrame
df = con.execute(AGG_PROVINCE_SPENDING).fetchdf()

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
        "TOTAL_SPENDING": "Total Spending(RMB)",
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
    "This map shows the total sales in different regions of China. The lighter the color, the higher the total sales."
)
st.write(
    "It has been studied that in China, coastal cities have a higher GDP per capita than inner regions."
)
st.write(
    "source: https://typeset.io/questions/why-does-coastal-regions-in-china-have-a-higher-gdp-per-5gt586emod"
)

# Execute the query and fetch the data
df = con.execute(CORR_TOTAL_SPEND_GDP_PER_CAPITA).fetchdf()

# Calculate the correlation
correlation = df["total_spend"].corr(df["PER_CAPITA_USD"])

# Display the correlation
st.title("Correlation between Total Spend and Per Capita USD")
st.write(f"Correlation coefficient: {correlation:.2f}")

# Plot the data using Plotly
fig = px.scatter(
    df,
    x="PER_CAPITA_USD",
    y="total_spend",
    title="Total Spend vs. Per Capita USD",
    color="PROVINCE",  # Change color according to PROVINCE
    labels={"PER_CAPITA_USD": "Per Capita USD", "total_spend": "Total Spend"},
    hover_data={"SHIP_TO_CITY_CD_ENG": True, "PROVINCE": True},
)

# Show the plot in Streamlit
st.plotly_chart(fig)
#####################################
st.header("City Level Metadata")
city_metadata_df = con.execute(ALL_CITY_MAPPING).fetchdf()
st.write(city_metadata_df)

st.header("Top 10 Provinces in Sales")
top_provinces_df = con.execute(AGG_TOP_10_PROVINCE_SPENDING).fetchdf()
fig = px.bar(
    top_provinces_df,
    x="PROVINCE",
    y="province_total_sales",
    title="Top 10 Provinces in Sales",
)
st.plotly_chart(fig)

st.title("Percentage of Cities with Valid Translation")
result_df = con.execute(PERCENTAGE_OF_VALID_CITY_TRANSLATIONS).fetchdf()
st.write(result_df)

st.title("Percentage of Districts with Valid Translation")
result_df = con.execute(PERCENTAGE_OF_VALID_DISTRICTS_TRANSLATIONS).fetchdf()
st.write(result_df)

st.header("Top 10 Cities In Sales")
top_10_cities_df = con.execute(AGG_TOP_10_CITIES_SPENDING).fetchdf()
fig = px.bar(
    top_10_cities_df,
    x="SHIP_TO_CITY_CD_ENG",
    y="total_sales",
    title="Top 10 Cities in Sales",
)
st.plotly_chart(fig)

# st.header("Top 10 Cities in Transaction Count")
# top_10_cities_count_df = con.execute(AGG_TOP_10_CITIES_TRANSACTION_COUNT).fetchdf()
# st.write(top_10_cities_count_df)

st.header("Top 10 Transactions By Amount")
top_10_transactions_df = con.execute(ALL_TOP_10_TRANSACTIONS).fetchdf()
st.write(top_10_transactions_df)

st.markdown("## Q1. Find the city with the highest per-hour sales")
st.markdown(
    "Analysis: This question looks like it can be interpreted in 2 ways. Either 1) For each hour, find the city with the highest spending or 2) Find the city-hour pair with the highest spending. Why not both? The interesting analysis is that while Shanghai tops the charts in sales across all times of day, at certain peak periods, other cities can do better in sales than Shanghai at off-peak periods. Refer to the next two figures."
)
# City with the highest per-hour sales
st.markdown("Q1a. City with the Highest Sales Per Hour")
hourly_sales_df = con.execute(RANKED_TOP_CITY_PER_HOUR).fetchdf()
st.write(hourly_sales_df)

# City pair with the highest spendings
st.markdown("Q1b. Top 10 City-Hour Pair with the Highest Sales")
city_hour_pair_sales_df = con.execute(RANKED_TOP_10_CITY_HOUR_PAIR).fetchdf()
st.write(city_hour_pair_sales_df)

# City with the highest average sales by district
st.markdown("## Q2. Find the city with the highest average sales by district")
st.markdown(
    "For each city, find the district with the highest average sales. Then return top 1 or top n cities."
)
average_sales_df = con.execute(RANKED_TOP_10_CITIES_HIGHEST_DISTRICT_AVG).fetchdf()
st.write(average_sales_df)

# Visualizations
st.header(
    "Q3. Discuss and show how to cluster cities into n-number of tiers based on sales (e.g. lowest spending to highest spending)."
)
fig = px.scatter(
    cluster_df,
    x="SHIP_TO_CITY_CD",
    y="RMB_DOLLARS",
    color="cluster",
    title="City Clusters Based on Sales using K-Means clustering.",
)
st.plotly_chart(fig)

hourly_sales_df = con.execute(AGG_TOTAL_SPEND_PER_HOUR).fetchdf()
# Total Sales by Hour
fig = px.bar(
    hourly_sales_df,
    x="rounded_order_hour",
    y="total_sales",
    title="Total Sales by Hour",
)
st.plotly_chart(fig)

# Close the DuckDB connection
con.close()
