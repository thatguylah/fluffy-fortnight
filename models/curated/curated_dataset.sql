-- Load necessary data from Silver Layer
WITH processed_df AS (
    SELECT * FROM {{ ref('processed_dataset') }}
),
currency_code_df AS (
    SELECT * FROM {{ source('main', 'currency_code_mapping') }}
),
translations_city_df AS (
    SELECT * FROM {{ source('main', 'translations_city_mapping') }}
),
translations_district_df AS (
    SELECT * FROM {{ source('main', 'translations_district_mapping') }}
),

-- Perform the required transformations
merged_data AS (
    SELECT 
        p.ORDER_ID,
        CAST(p.ORDER_TIME_PST AS INTEGER) AS ORDER_TIME_PST,
        p.SHIP_TO_CITY_CD,
        p.SHIP_TO_DISTRICT_NAME,
        p.RPTG_AMT,
        p.CURRENCY_CD,
        CAST(p.ORDER_QTY AS INTEGER) AS ORDER_QTY,
        tc.SHIP_TO_CITY_CD_ENG,
        td.SHIP_TO_DISTRICT_NAME_ENG,
        cc.MULTIPLIER
    FROM 
        processed_df p
    LEFT JOIN 
        translations_city_df tc 
    ON 
        p.SHIP_TO_CITY_CD = tc.SHIP_TO_CITY_CD
    LEFT JOIN 
        translations_district_df td 
    ON 
        p.SHIP_TO_DISTRICT_NAME = td.SHIP_TO_DISTRICT_NAME
    LEFT JOIN 
        currency_code_df cc 
    ON 
        p.CURRENCY_CD = cc.CURRENCY_CD
)

SELECT
    ORDER_ID,
    CAST(ORDER_TIME_PST AS INTEGER) AS ORDER_TIME_PST,
    SHIP_TO_CITY_CD,
    SHIP_TO_DISTRICT_NAME,
    SHIP_TO_DISTRICT_NAME_ENG,
    SHIP_TO_CITY_CD_ENG,
    CAST(RPTG_AMT * MULTIPLIER AS DECIMAL(18,2)) AS RMB_DOLLARS,
    CAST(ORDER_QTY AS INTEGER) AS ORDER_QTY,
FROM 
    merged_data
