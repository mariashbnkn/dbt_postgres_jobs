{% test unique_job_link_date(model, column_name, column_name_2) %}
SELECT
    {{ column_name }},
    {{ column_name_2 }},
    COUNT(*) AS duplicates
FROM {{ model }}
GROUP BY {{ column_name }}, {{ column_name_2 }}
HAVING COUNT(*) > 1
{% endtest %}