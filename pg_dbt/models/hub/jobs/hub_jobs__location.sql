{{
  config(
    materialized = 'incremental',
    unique_key = 'location_hash_key',
    dist = 'location_hash_key',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_hub_location_name ON {{ this }} (job_location)"
    ]
  )
}}

SELECT distinct
  '{{ get_hashkey('job_location') }}' AS location_hash_key,
  lower(job_location) as job_location,
  CURRENT_TIMESTAMP AS load_date,
  'LinkedIn Scraper' AS record_source
FROM {{ ref('stg_jobs__raw_jobs') }}

{% if is_incremental() %}
  WHERE '{{ get_hashkey('job_location') }}' NOT IN (
    SELECT location_hash_key FROM {{ this }}
  )
{% endif %}