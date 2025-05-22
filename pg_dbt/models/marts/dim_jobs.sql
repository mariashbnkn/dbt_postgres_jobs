--Разделение локации на город/штат

{{ config(materialized='table') }}

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