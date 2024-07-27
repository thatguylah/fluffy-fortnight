{% set validations = [
    ("ORDER_TIME_PST", "ORDER_TIME_PST ~ '^\\d+$'", "Invalid ORDER_TIME_PST"),
    ("CITY_DISTRICT_ID", "CITY_DISTRICT_ID IN (SELECT CITY_DISTRICT_ID FROM mapping_source)", "Invalid CITY_DISTRICT_ID"),
    ("RPTG_AMT", "RPTG_AMT >= 0", "Invalid RPTG_AMT"),
    ("CURRENCY_CD", "CURRENCY_CD IN ('USD', 'RMB')", "Invalid CURRENCY_CD"),
    ("ORDER_QTY", "CAST(ORDER_QTY AS INTEGER) > 0", "Invalid ORDER_QTY")
] %}

{{ validation_errors(
    source('main', 'raw_dataset_1'),
    source('main', 'raw_mapping'),
    validations
) }}
