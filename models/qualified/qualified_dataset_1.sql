WITH source AS (
    -- Use source instead of seed:
    SELECT * FROM {{ source('main', 'raw_dataset_1') }}
),
mapping_source AS (
    SELECT * FROM {{ source('main', 'raw_mapping') }}
),
valid_raw_dataset_excel AS (
    SELECT
        ORDER_ID,
        {{ validate_field('ORDER_TIME_PST', "ORDER_TIME_PST ~ '^\\d+$'") }} AS ORDER_TIME_PST,
        {{ validate_field('CITY_DISTRICT_ID', "CITY_DISTRICT_ID IN (SELECT CITY_DISTRICT_ID FROM mapping_source)") }} AS CITY_DISTRICT_ID,
        {{ validate_field('RPTG_AMT', "RPTG_AMT >= 0") }} AS RPTG_AMT,
        {{ validate_field('CURRENCY_CD', "CURRENCY_CD IN ('USD', 'RMB')") }} AS CURRENCY_CD,
        {{ validate_field('ORDER_QTY', "CAST(ORDER_QTY AS INTEGER) > 0") }} AS ORDER_QTY
    FROM
        source
)
SELECT * FROM valid_raw_dataset_excel
