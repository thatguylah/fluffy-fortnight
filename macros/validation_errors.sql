{% macro validation_errors(source_table, mapping_table, validations) %}
WITH source AS (
    SELECT * FROM {{ source_table }}
),
mapping_source AS (
    SELECT * FROM {{ mapping_table }}
),
failed_validations AS (
    SELECT
        ORDER_ID,
        {% for field, condition, error_message in validations %}
        CASE 
            WHEN NOT ({{ condition }}) THEN '{{ error_message }}'
            ELSE NULL 
        END AS {{ field }}_ERROR{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM
        source
)
SELECT 
    ORDER_ID,
    COALESCE({% for field, condition, error_message in validations %}{{ field }}_ERROR{% if not loop.last %}, {% endif %}{% endfor %}) AS validation_error
FROM 
    failed_validations
WHERE 
    {% for field, condition, error_message in validations %}
    {{ field }}_ERROR IS NOT NULL{% if not loop.last %} OR {% endif %}
    {% endfor %}
{% endmacro %}
