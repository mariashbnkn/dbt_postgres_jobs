{{ config(materialized='table') }}

{{ track_job_changes('2024-01-19', '2024-01-20') }}