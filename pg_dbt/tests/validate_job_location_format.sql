-- Проверка, что job_location содержит город и штат

SELECT
    job_link,
    job_location
FROM {{ ref('stg_jobs__raw_jobs') }}
WHERE
    job_location NOT LIKE '%, %'  -- Проверяем формат "Город, Штат"
    OR SPLIT_PART(job_location, ', ', 2) = ''  -- Проверяем, что есть штат