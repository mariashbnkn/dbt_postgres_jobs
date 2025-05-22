{% snapshot job_snapshots %}

{{
    config(
      target_schema='snapshots',
      unique_key='job_link',
      strategy='timestamp',
      updated_at='last_processed_time',
      invalidate_hard_deletes=True,
      post_hook = [
        "CREATE INDEX IF NOT EXISTS idx_job_snapshots_time ON {{ this }} (last_processed_time)",
        "DELETE FROM {{ this }} WHERE last_processed_time < NOW() - INTERVAL '6 months'",
        "ANALYZE VERBOSE {{ this }}"
      ]
    )
}}

SELECT
    job_link,
    last_processed_time,
    got_summary,
    got_ner,
    is_being_worked,
    job_title,
    company,
    job_location,
    first_seen as first_seen,
    search_city,
    search_country,
    search_position,
    job_level,
    job_type
FROM {{ source('src', 'raw_jobs') }}

{% endsnapshot %}