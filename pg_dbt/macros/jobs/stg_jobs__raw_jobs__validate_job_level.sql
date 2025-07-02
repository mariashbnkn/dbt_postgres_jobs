{% test valid_job_level(model, column_name) %}

WITH valid_levels AS (
    SELECT * FROM (VALUES ('Associate'), ('Mid senior'), ('Senior'), ('Executive')) AS t(level)
)

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} NOT IN (SELECT level FROM valid_levels)

{% endtest %}