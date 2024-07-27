WITH mapping_source AS (
    SELECT * FROM {{ source('main', 'raw_mapping') }}
),
fused_dataset_1 AS (
    SELECT 
        qd1.ORDER_ID, 
        qd1.ORDER_TIME_PST, 
        mapping_source.SHIP_TO_CITY_CD, 
        mapping_source.SHIP_TO_DISTRICT_NAME, 
        qd1.RPTG_AMT, 
        qd1.CURRENCY_CD, 
        qd1.ORDER_QTY
    FROM 
        {{ ref('qualified_dataset_1') }} qd1
    LEFT JOIN
        mapping_source
    ON 
        qd1.CITY_DISTRICT_ID = mapping_source.CITY_DISTRICT_ID
),
fused_dataset_combined AS (
    SELECT 
        ORDER_ID, 
        ORDER_TIME_PST, 
        RPTG_AMT, 
        CURRENCY_CD, 
        ORDER_QTY, 
        SHIP_TO_CITY_CD, 
        SHIP_TO_DISTRICT_NAME 
    FROM 
        fused_dataset_1
    UNION ALL
    SELECT 
        ORDER_ID, 
        ORDER_TIME_PST, 
        RPTG_AMT, 
        CURRENCY_CD, 
        ORDER_QTY, 
        SHIP_TO_CITY_CD, 
        SHIP_TO_DISTRICT_NAME 
    FROM 
        {{ ref('qualified_dataset_2') }}
)

SELECT * FROM fused_dataset_combined
