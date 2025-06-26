
{% set positions_query %} 
SELECT DISTINCT
    search_position
FROM {{ ref('stg_jobs__raw_jobs') }} limit 10
{% endset %}

{% set positions_query_result = run_query(positions_query) %}
{% if execute %}
    {% set important_positions = positions_query_result.columns[0].values() %} 
{% else %}
    {% set important_positions = [] %}
{% endif %}

select distinct
    {% for position in important_positions %}
    sum(case when search_position = '{{ position }}' then 1 else 0 end) as position_{{ position|replace(" ", "_") }}
        {%- if not loop.last %}, {% endif %}
        -- {{ loop.nextitem }}
    {% endfor %}
from {{ ref('stg_jobs__raw_jobs') }}
