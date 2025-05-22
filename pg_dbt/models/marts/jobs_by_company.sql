--количество вакансий по компаниям

{{ config(materialized='table') }}

SELECT
    company,
    COUNT(1) AS total_jobs
FROM {{ ref('stg_jobs__raw_jobs') }}
GROUP BY company
ORDER BY total_jobs DESC