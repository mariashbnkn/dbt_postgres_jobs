{{
  config(
    materialized = 'incremental',
    unique_key = 'job_listing_hash_key',
    dist = 'job_listing_hash_key',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_hub_job_link ON {{ this }} (job_link)"
    ]
  )
}}

SELECT distinct
  '{{ get_hashkey('job_link') }}' AS job_listing_hash_key,
  job_link,
  CURRENT_TIMESTAMP AS load_date,
  'LinkedIn Scraper' AS record_source
FROM {{ ref('stg_jobs__raw_jobs') }}

{% if is_incremental() %}
  WHERE '{{ get_hashkey('job_link') }}' NOT IN (
    SELECT job_listing_hash_key FROM {{ this }}
  )
{% endif %}