select 
    count(1)
from 
    {{ ref('stg_jobs__job_skills') }}
where job_skills like '%management%'
