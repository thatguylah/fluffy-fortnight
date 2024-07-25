import duckdb
import requests
from bs4 import BeautifulSoup
import json
import re
import os
import glob

from concurrent.futures import ThreadPoolExecutor, as_completed
from constants import (
    DUCKDB_FILE_PATH,
    ERROR_TRANSLATIONS_FILE_PATH,
    CITY_TRANSLATIONS_FILE_PATH,
    DISTRICTS_TRANSLATIONS_FILE_PATH,
    ERROR_DISTRICTS_TRANSLATIONS_FILE_PATH,
)


# Function to clean up the data
def clean_text(text):
    if isinstance(text, str):
        text = re.sub(
            r"\xa0", " ", text
        )  # Replace non-breaking space with normal space
        text = re.sub(r"\s+", " ", text)  # Replace multiple spaces with a single space
        return text.strip()
    return text


# Function to parse the HTML content
def parse_html(identifier, html_content, identifier_type):
    soup = BeautifulSoup(html_content, "html.parser")
    english_name = soup.title.string.replace("- Wikipedia", "").strip()
    demographics = {}
    tables = soup.find_all("table", class_="infobox")

    for table in tables:
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                key = th.text.strip()
                value = td.text.strip()
                demographics[key] = value

    cleaned_demographics = {
        re.sub(r"^[-•]\s*", "", key): clean_text(value)
        for key, value in demographics.items()
    }

    return {
        f"{identifier_type}_ENG": english_name,
        identifier_type: identifier,
        "metadata": cleaned_demographics,
    }


# Function to initialize the JSON file
def initialize_json_file(filename):
    with open(filename, "w", encoding="utf-8") as json_file:
        json_file.write("[\n")


# Function to finalize the JSON file
def finalize_json_file(filename):
    # Remove the last comma and add closing bracket
    with open(filename, "rb+") as json_file:
        json_file.seek(-2, os.SEEK_END)
        json_file.truncate()
        json_file.write(b"\n]")


# Function to log and save results incrementally
def save_result_incrementally(result, filename):
    with open(filename, "a", encoding="utf-8") as json_file:
        json.dump(result, json_file, ensure_ascii=False, indent=4)
        json_file.write(",\n")


# Function to log errors incrementally
def log_error_incrementally(identifier, error_message, filename):
    error_log = {identifier: error_message}
    with open(filename, "a", encoding="utf-8") as json_file:
        json.dump(error_log, json_file, ensure_ascii=False, indent=4)
        json_file.write(",\n")


# Function to fetch and process HTML content for a single identifier
def fetch_and_process(identifier, identifier_type, output_filename, error_filename):
    # url = f"https://en.wikipedia.org/wiki/{identifier}"  # Scrape english version of wikipedia.
    url = f"https://zh.wikipedia.org/wiki/{identifier}"  # Scrape chinese version of wikipedia.

    # ZH version has more hits but no native way to translate without using google translate.
    headers = {"Accept-Language": "en-US,en;q=0.5"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        result = parse_html(identifier, response.content, identifier_type)
        # Save the result incrementally
        save_result_incrementally(result, output_filename)
    else:
        error_message = f"HTTP Response: {response.status_code}"
        log_error_incrementally(identifier, error_message, error_filename)

    # Log the current identifier, HTTP response status, and metadata object
    print(f"{identifier_type}: {identifier}, HTTP Response: {response.status_code}")


# Function to process identifiers
def process_identifiers(
    identifier_type, output_filename, error_filename, max_workers=10
):
    # Connect to DuckDB and retrieve unique identifiers
    con = duckdb.connect(database=DUCKDB_FILE_PATH, read_only=False)
    df_identifiers = con.execute(
        f"""
        SELECT DISTINCT {identifier_type} FROM (
            SELECT {identifier_type} FROM RAW_DATASET_2
            UNION ALL
            SELECT {identifier_type} FROM RAW_MAPPING
        )
        """
    ).fetchdf()
    con.close()

    total_identifiers = len(df_identifiers)

    # we use thread safe multithreading to improve performance
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                fetch_and_process,
                row[identifier_type],
                identifier_type,
                output_filename,
                error_filename,
            )
            for index, row in df_identifiers.iterrows()
        ]

        for i, future in enumerate(as_completed(futures)):
            future.result()  # Ensure any exceptions are raised
            # Calculate and print progress
            progress = (i + 1) / total_identifiers * 100
            print(f"Progress: {progress:.2f}%")


# Main function to process both city and district names
def main():

    # Use glob to list all files in the directory
    files = glob.glob(os.path.join("data/static/", "*"))

    # Loop through the files and remove each one

    for file in files:
        print(
            f"{file} exists! Removing before this pipeline runs to ensure idempotency."
        )
        try:
            os.remove(file)
            print(f"Successfully deleted: {file}")
        except Exception as e:
            print(f"Error deleting {file}: {e}")

    initialize_json_file(CITY_TRANSLATIONS_FILE_PATH)
    initialize_json_file(ERROR_TRANSLATIONS_FILE_PATH)
    initialize_json_file(DISTRICTS_TRANSLATIONS_FILE_PATH)
    initialize_json_file(ERROR_DISTRICTS_TRANSLATIONS_FILE_PATH)

    process_identifiers(
        "SHIP_TO_CITY_CD", CITY_TRANSLATIONS_FILE_PATH, ERROR_TRANSLATIONS_FILE_PATH
    )
    finalize_json_file(CITY_TRANSLATIONS_FILE_PATH)
    finalize_json_file(ERROR_TRANSLATIONS_FILE_PATH)

    process_identifiers(
        "SHIP_TO_DISTRICT_NAME",
        DISTRICTS_TRANSLATIONS_FILE_PATH,
        ERROR_DISTRICTS_TRANSLATIONS_FILE_PATH,
    )
    finalize_json_file(DISTRICTS_TRANSLATIONS_FILE_PATH)
    finalize_json_file(ERROR_DISTRICTS_TRANSLATIONS_FILE_PATH)


if __name__ == "__main__":
    main()
