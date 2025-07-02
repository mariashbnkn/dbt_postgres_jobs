--Разделение локации на город/штат
{{ 
    config(
        materialized = "materialized_view",
        on_configuration_change = "continue",
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