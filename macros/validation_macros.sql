{% macro validate_field(field, condition) %}
    CASE 
        WHEN {{ condition }} THEN {{ field }}
        ELSE NULL 
    END
{% endmacro %}
