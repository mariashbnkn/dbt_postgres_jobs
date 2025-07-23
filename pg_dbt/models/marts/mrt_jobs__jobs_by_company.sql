--количество вакансий по компаниям

{{ 
    config(
        materialized = 'table',
        transient = false
    )
}}

SELECT
    company,
    first_seen,
    COUNT(1) AS total_jobs
FROM {{ ref('stg_jobs__raw_jobs') }}
    where '{{ var('date_range')['start_date'] }}' <= first_seen 
		and 
	first_seen < '{{ var('date_range')['end_date'] }}'
GROUP BY company, first_seen
ORDER BY total_jobs DESC