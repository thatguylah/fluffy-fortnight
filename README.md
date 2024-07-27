
Scenario :
The Sales team would like to analyse on store sales performance across different locations, and obtain actionable insights for driving strategic marketing initiatives.

For example, ability to
- identify which cities are performing better, and any correlation to location or timing?
- plan on areas of investment (marketing campaigns, opening of stores etc)

You are provided a sample data set of orders for a single day between 5am and 12pm. The order timestamp is in format HHMMSS.


Perform the following data engineering tasks (Python, Scala, Java etc.)
Setup a database locally. For example, you can use mysql or sqlite3.
Process and load the data from both “dataset1.xlsx” and “dataset2.json”, performing any data cleansing and mapping as required
Additional bonus - perform translation of districts and cities to English
Create the necessary tables to answer the questions below

Build a simple web app (Flask, Django, NodeJS etc.) which provides actionable insights to sales analysts on sales performance across different locations.
Plot some charts to allow users to answer the following questions :
Find the city with the highest per-hour sales
Find the city with the highest average sales by district
Discuss and show how to cluster cities into n-number of tiers based on sales (e.g. lowest spending to highest spending).
Discuss (and if possible, show) what other types of metadata we can include to get deeper insights and analysis.
Analyse the dataset yourself and share/showcase any interesting insights you find
In the presentation interview session : 
Do an intro & presentation on this project (includes demo of the app). Highlight any issues found in the data, or challenges with the process. (1 hour)
Q&A and code review of data preparation and the app (30 mins) 

Initial thoughts:
- What is the relationship if any between dataset 1 and dataset 2? Or does the assessment require me to derive their rs on my own?
  - Datasets 1 and 2 cannot be joined bc no overlapping ORDER_ID 
  - They are likely the same datasets just different source files
- RPTG_AMT stands for REPORTING_AMOUNT in full?
- Is there a data dictionary/data catalogue i can refer to or i should build one using the datasets on hand?
- For null values, or otherwise invalid values how should i proceed or do i make my own assumptions and carry on?
- Could i propose multiple solutions or is that considered too much for the assessment and i should focus on just one solution and the visuals and presentation materials?

# Section 1 - Exploratory Data Analysis Using Jupyter Notebooks (eda/)
Initial EDA and findings:
- Adding on initial thoughts:
  - Quite obvious these datasets represent transactions of goods and services within China
  - Manual eyeballing and profilling: ORDER_QTY and RPTG_AMT reveals this is likely the case
  - Lack of SKU or any product key means each order_id is a rolled up aggregate or they represent the same goods and services, most likely the former. 
  - Prompting a LLM like ChatGPT reveals that the cities and districts are actual locations in China + Wikipedia and google searching 
  
- It seems like dataset1 and dataset2 are likely the same dataset just different source file formats
  - Cannot be joined due to no overlapping ORDER_ID, hence there cannot be duplicates across files
  - Current hypothesis: Perhaps each dataset could logically represent a type of store (physical,online) or are grouped by geographical locations?
  - Contain same schema just that one is denormalized and the other is normalized
- Summary statistics:
  - Dataset 1
  - RangeIndex: 159750 entries, 0 to 159749
Data columns (total 6 columns):
 #   Column             Non-Null Count   Dtype  
---  ------             --------------   -----  
 0   ORDER_ID           159750 non-null  object 
 1   ORDER_TIME  (PST)  159750 non-null  object 
 2   CITY_DISTRICT_ID   159750 non-null  int64  
 3   RPTG_AMT           159750 non-null  float64
 4   CURRENCY_CD        159750 non-null  object 
 5   ORDER_QTY          159712 non-null  float64
   - Dataset 2
   - RangeIndex: 40236 entries, 0 to 40235
Data columns (total 7 columns):
 #   Column                 Non-Null Count  Dtype  
---  ------                 --------------  -----  
 0   ORDER_ID               40236 non-null  object 
 1   ORDER_TIME_PST         40236 non-null  int64  
 2   SHIP_TO_DISTRICT_NAME  40236 non-null  object 
 3   SHIP_TO_CITY_CD        40236 non-null  object 
 4   RPTG_AMT               40236 non-null  float64
 5   CURRENCY_CD            40236 non-null  object 
 6   ORDER_QTY              40236 non-null  int64  
dtypes: float64(1), int64(2), object(4)
- Data Profiling Tools:
  - AutoViz
  - Lux
- Data Quality Checks
  - ORDER_QTY (INTEGER type, cannot be float):
    - Dataset 1: 38 rows failed
    - Dataset 2: 0 rows failed 
    - [x] Data validtaion: Can be casted as Integer
    - [x] Data validation: Greater than 0 
    - [ ] Data validation: Upper bound for number of order qty, outliers
  - CURRENCY_CD (ENUM type, "USD" or "RMB")
    - Dataset 1: 37 rows where currency is USD
    - Dataset 2: 0 rows where currency is USD, 100% RMB
    - [x] Data validation: Check all unique values across both datasets
    - [x] Data validation: Row distributions and skew
  - RPTG_AMT (FLOAT type, used to denote currency)
    - Dataset 1: All rows are floats with two dp
    - Dataset 2: All rows are floats with two dp
    - [ ] Data validation: Box plot to see outliers of RPTG_AMT
    - [x] Data validation: Check that value cannot be less than 0
    - [ ] Data validation: Upper bound of practical value
  - CITY_DISTRICT_ID (INTEGER type, categorical ordinal, used in junction with mapping table)
    - Mapping Table: 362 unique cities, 2545 unique districts, 2582 unique pairings of city and district
    - Dataset 1: 37 rows where CITY_DISTRICT_ID = 999999
    - ❌ Data validation: Check if all IDs in dataset1 are present in mapping table 
  - SHIP_TO_DISTRICT_NAME and SHIP_TO_CITY_CD (STRING and STRING, used to denote district name and city name(province), both in chinese characters)
    - Dataset 2: 119 rows where district not in mapping table, 2 rows where  city not in mapping table, 2 rows where both are no in mapping table
    - ❌ Data validation: Check if all composite key (CITY_DISTRICT) combination is present in mapping table
  - ORDER_TIME PST (LONG/BIGINT, used to represent HHMMSS)
    - Dataset 1:replace string time values with null 
    - [ ] Data validation: check lower bound of 5am, 50000 (in excel leading 0s are dropped)
    - [ ] Data validation: check upper bound of 12pm, 120000
  - ORDER_ID (STRING type, used as PK)
    - Dataset 1: Each order_id is unique. 
    - Dataset 2: Each order_id is unique. 
    - Dataset 1 & 2: Each order_id is unique. (Cannot be joined on order_id, meaning no duplicates across datasets)
    - [x] Data validation:: Check for uniqueness across dataset 1
    - [x] Data validation: Check for uniqueness across dataset 2
    - [x] Data validation: Check for uniqueness across dataset 1 and 2
  
# Section 1.5 Data Preparation (Cleaning of null/invalid values)
- Stand up EXCEPTIONS_ table to load all erroneous values
- Two EXCEPTIONS_ table, one for excel source, another for json source due to differing schema that cannot be reconciled
- ORDER_QTY: 
  - dataset1: load into exceptions table with descriptive status msg then replace all rows that failed check with NaN 
- CURRENCY_CD:
  - Check either USD or RMB, load both
- RPTG_AMT:
  - load into exceptions table then replace all rows that failed with 0
- CITY_DISTRICT_ID:
  - dataset 1 only: load into exceptions table w descriptive status msg, then replace all rows that failed check with NaN
- SHIP_TO_DISTRICT_NAME and SHIP_TO_CITY_CD:
  - dataset 2 only: load into exceptions table, continue loading into silver as per normal
  
# Section 2 - Data Engineering Infra + ELT + Data Quality Checks
- Stand up postgres container using Dockerfile
- Aft postgres container is stood up, run DDL on initialization, make sure to create indexes properly
  - Unexpectedly huge challenge here, somehow standing up a docker container for postgres and using pandas + sqlalchemy + psycopg2 as ORM and db driver were harder than expected with non-ideal performance. In the interest of time, i havent deep dived into why performance was so slow, was taking ~8 mins to bulk load 150k rows into postgres. 
  - Cause of that, i switched to duckdb with much better performance and minimal bloat, it has no other dependencies and can standalone, and plays nice with excel,json and pandas. 
- Basic: Docker container to run ELT scripts using pandas to hit the data/ folders and load them into pg
  - Data flow: 
  - (Docker) Postgres container <-> (Docker) Pandas script <-> Source Data 
- Advanced: Self contained Dockerized Airflow with Celery executors to run dags
  - New stack: Modern Data Stack (MDS): Dagster orchestrator, dbt core transformation workflow and scaffolds, duckdb OLAP warehouse, streamlit visualization app 
  - Note to self: Dagster has diff project templates to choose from, look through them at `dagster project list-examples`
  - (Docker-compose) Airflow Orchestrator/Webserver -> (Docker-compose) Executors DAGS <- Source Data 
  - Executors DAGS -> (Docker) Postgres
- Idempotent pipelines: Philosophy of pipelines should be to upsert where possible, update if data exists, insert if do not, otherwise each rerun will have multiple duplicate data rows. 
- Why idempotent pipelines are important? I just realized my mistake in bronze layer that the columns CITY and DISTRICT in dataset 2 were swapped, with idempotent pipeline, i just had to change the position of columns in my DDL script, and run everything from the top to get back to a correct state. 
- Data Quality Checks: Pydantic validators vs Great Expectations suite vs Bare bones pytest
- ERD
- Medallion architecture: Bronze, Silver, Gold tables
- Stack used: (Basic) Pandas, Postgres, Docker, Pydantic
  - Updated Basic Stack: Pandas, DuckDB, Pydantic, BeautifulSoup, Requests
- (Advanced) Airflow/Dagster, Spark?, Pandas?, Postgres, Docker, Pydantic/GE
  - Infra problems so far, Airflow doesnt play nice with versions of SQLAlchemy and Psycopg2
  - Dependencies have alot to be managed
- (Stretch) Full cloud deployment on aws 
- 
# Section 2.5 Translating Chinese districts and cities to English
- Method 1: Reading from Wikipedia
  - Pros and cons: pros, relatively static, reliable, although quite a few misses, but could work in a pinch
  - Also i just realized there is a https://zh.wikipedia.org/wiki/ for mainly chinese market
- Method 2: Using Translate API
  - For some reason, all API clients using google or even bing seem to time out and is not very reliable. 
- Method 3: LLM translation (locally hosted or api?)
  - Open source LLM models on hugging face like the Helsinki-NLP/opus-mt-en-fr were also laughable in some of the translations with quite small rate limiting 
  - locally hosted models could work but were too expensive and resource intensive to host
- Method 4: Manual Translation by copying into ChatGPT:
  - Once off manual effort to generate key value pairs
- Conclusion: Ended up i went with Wikpedia solution as it was way easier to develop and i could also have a bonus of querying other metadata that comes along with scrapinga wiki page.
- There were two wiki scraping methods, one is to query a wikipedia table with most of the cities but no further metadata, i went the other option to put each city/district as a slug to pull data from each wiki page. 
  
# Section 3 - Analysis and Findings
- First 2 queries can be solved in SQL and charts
- Clustering into N-tier cities 
- Basic: Flask/Streamlit app running Dash or Plotly visuals, probably can only do static visuals? 
- Advanced: Data Viz tool like Tableau/Metabase/Looker over ODBC connection to postgres container, using SQL queries to generate visuals 

# Section 3.5 - Deeper analysis and findings
- Other metadata, demographics, population, income
  
# Section 4 - Future improvements
- Data security/privacy
- Data governance