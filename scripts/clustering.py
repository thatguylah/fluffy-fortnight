import duckdb
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from constants import DUCKDB_FILE_PATH, CITY_CLUSTER_RESULTS_FILE_PATH

# Extract from OLAP and aggregate
# Connect to DuckDB and retrieve the data
con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=False)
df = con.execute("SELECT SHIP_TO_CITY_CD, RMB_DOLLARS FROM CURATED_DATASET").fetchdf()

# Aggregate total sales by city
city_sales = df.groupby("SHIP_TO_CITY_CD").sum().reset_index()

# Optionally, merge with English city names if available
city_names = con.execute(
    "SELECT SHIP_TO_CITY_CD, SHIP_TO_CITY_CD_ENG, PROVINCE, PER_CAPITA_USD  FROM TRANSLATIONS_CITY_MAPPING"
).fetchdf()
city_sales = city_sales.merge(city_names, on="SHIP_TO_CITY_CD", how="left")

# Data Preprocessing
# Normalize the sales data
scaler = StandardScaler()
city_sales["normalized_sales"] = scaler.fit_transform(city_sales[["RMB_DOLLARS"]])

# Determine optimal number of clusters
# Elbow method to find the optimal number of clusters
sse = []
for k in range(1, 10):
    kmeans = KMeans(n_clusters=k)
    kmeans.fit(city_sales[["normalized_sales"]])
    sse.append(kmeans.inertia_)

# Perform clustering
# Assuming the optimal number of clusters is 3
kmeans = KMeans(n_clusters=3)
city_sales["cluster"] = kmeans.fit_predict(city_sales[["normalized_sales"]])


# Insert the clustering results
con.register("city_sales", city_sales)
con.execute(
    f"COPY (SELECT * FROM city_sales) TO '{CITY_CLUSTER_RESULTS_FILE_PATH}' (HEADER, DELIMITER ',');"
)

# Close the DuckDB connection
con.close()
