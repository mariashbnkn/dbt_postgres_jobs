{% macro track_job_changes(date1, date2) %}
    {% set changes_query %}
        SELECT COUNT(1) AS changes
        FROM {{ ref('job_snapshots') }}
        WHERE dbt_valid_from BETWEEN '{{ date1 }}'::date AND '{{ date2 }}'::date
    {% endset %}
    
    {% set results = run_query(changes_query) %}
    
    {% if execute %}
        {% set changes_count = results.columns['changes'].values()[0] %}
        {{ log("Найдено изменений: " ~ changes_count, info=True) }}
        SELECT 
            '{{ date1 }}' AS start_date,
            '{{ date2 }}' AS end_date,
            {{ changes_count }} AS changes_count
    {% endif %}
{% endmacro %}