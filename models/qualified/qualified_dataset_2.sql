WITH source AS (
    -- Use source instead of seed:
    SELECT * FROM {{ source('main', 'raw_dataset_2') }}
),
valid_raw_dataset_json AS (
    SELECT
        ORDER_ID,
        {{ validate_field('ORDER_TIME_PST', "ORDER_TIME_PST BETWEEN 50000 AND 120000") }} AS ORDER_TIME_PST,
        SHIP_TO_DISTRICT_NAME,
        SHIP_TO_CITY_CD,
        {{ validate_field('RPTG_AMT', "RPTG_AMT >= 0") }} AS RPTG_AMT,
        {{ validate_field('CURRENCY_CD', "CURRENCY_CD IN ('USD', 'RMB')") }} AS CURRENCY_CD,
        {{ validate_field('ORDER_QTY', "CAST(ORDER_QTY AS INTEGER) > 0") }} AS ORDER_QTY
    FROM
        source
)
SELECT * FROM valid_raw_dataset_json
