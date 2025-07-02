{{
  config(
    materialized = 'incremental',
    unique_key = 'company_hash_key',
    dist = 'company_hash_key',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_hub_company_name ON {{ this }} (company)"
    ]
  )
}}

SELECT distinct
  '{{ get_hashkey('company') }}' AS company_hash_key,
  lower(company) as company,
  CURRENT_TIMESTAMP AS load_date,
  'LinkedIn Scraper' AS record_source
FROM {{ ref('stg_jobs__raw_jobs') }}

{% if is_incremental() %}
  WHERE '{{ get_hashkey('company') }}' NOT IN (
    SELECT company_hash_key FROM {{ this }}
  )
{% endif %}