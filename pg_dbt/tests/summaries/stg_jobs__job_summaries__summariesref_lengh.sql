{{
    config(
        severity = 'error',
        error_if = '>100',
        warn_if = '>100'
    )
}}

select 
    s.job_summary
from 
    {{ ref('stg_jobs__job_summaries') }} s 
    join {{ ref('stg_jobs__raw_jobs' )}} j on s.job_link = j.job_link 
where length(s.job_summary) > 10000 and lower(j.job_title) like '%developer%'