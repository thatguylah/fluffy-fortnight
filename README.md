
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
- It seems like dataset1 and dataset2 are likely the same dataset just different source file formats
  - Cannot be joined due to no overlapping ORDER_ID
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
  - 

# Section 2 - Data Engineering Infra + ELT + Data Quality Checks
- Stand up postgres container using Dockerfile
- Aft postgres container is stood up, run DDL on initialization
- Basic: Docker container to run ELT scripts using pandas to hit the data/ folders and load them into pg
- Advanced: Self contained Dockerized Airflow with Celery executors to run dags
- Data Quality Checks: Pydantic validators vs Great Expectations suite vs Bare bones pytest
- Stack used: (Basic) Pandas, Postgres, Docker, Pydantic
- (Advanced) Airflow/Dagster, Spark?, Pandas?, Postgres, Docker, Pydantic
- (Stretch) Full cloud deployment on aws 
- 
# Section 3 - Analysis and Findings