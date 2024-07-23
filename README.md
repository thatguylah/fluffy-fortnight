
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