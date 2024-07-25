# Define the path to your Excel file and the DuckDB database file
EXCEL_FILE_PATH = "data/input/20240723/window1/dataset1.xlsx"
JSON_FILE_PATH = "data/input/20240723/window1/dataset2.json"
DUCKDB_FILE_PATH = "data/output/datawarehouse.duckdb"
CITY_TRANSLATIONS_FILE_PATH = "data/static/mappings/city_translations.json"
DISTRICTS_TRANSLATIONS_FILE_PATH = "data/static/mappings/districts_translations.json"
ERROR_TRANSLATIONS_FILE_PATH = "data/static/mappings/error_translations.json"
ERROR_DISTRICTS_TRANSLATIONS_FILE_PATH = (
    "data/static/mappings/error_districts_translations.json"
)
CITY_CLUSTER_RESULTS_FILE_PATH = "data/static/cluster/cluster_results.csv"
GEOJSON_FILE_PATH = "data/static/geojson/province_geojson.json"
