
{{ 
    config(
        materialized = 'table',
        transient = false, 
        indexes=[
            {
                "columns": ["first_seen_date"],
                "unique": false,
                "type": "btree"
            }
        ]
    )
}}
SELECT
    job_link,
    job_title,
    company,
    job_city,
    job_state,
    search_city,
    search_country,
    job_level,
    job_type,
    first_seen_date
FROM {{ ref('stg_jobs__raw_jobs') }} 
    where '{{ var('mart_date_range')['start_date'] }}' <= first_seen_date 
		and 
	first_seen_date < '{{ var('mart_date_range')['end_date'] }}'