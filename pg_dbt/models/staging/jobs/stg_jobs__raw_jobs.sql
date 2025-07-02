{{
  config(
    materialized='table'
  )
}}

SELECT
    job_link, 
    job_title,
    company,
    job_location,
    first_seen,
    search_city,
    search_country,
    search_position,
    job_level,
    job_type,
    SPLIT_PART(job_location, ', ', 1) AS job_city,
    SPLIT_PART(job_location, ', ', 2) AS job_state,
    DATE(first_seen) AS first_seen_date
FROM {{ source('src', 'raw_jobs') }}