{{
  config(
    materialized='table'
  )
}}

SELECT
    job_link, 
    job_skills
FROM {{ source('src', 'job_skills') }}