--Инкрементальная модель (только новые вакансии)

{{ config(
    materialized='incremental',
    unique_key='job_link'
) }}

SELECT * FROM {{ ref('stg_jobs__raw_jobs') }}

{% if is_incremental() %}
    WHERE first_seen_date > (SELECT MAX(first_seen_date) FROM {{ this }})
{% endif %}