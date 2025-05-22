{{
  config(
    materialized='table'
  )
}}

SELECT
    job_link, 
    job_summary
FROM {{ source('src', 'job_summaries') }}