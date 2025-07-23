CREATE DATABASE dbt
ENGINE = Atomic;

DROP TABLE IF EXISTS dbt.mrt_jobs__dim_jobs;

DROP TABLE IF EXISTS dbt.jobs__dim_jobs_hst;

DROP TABLE IF EXISTS dbt.mrt_jobs__jobs_by_company;

DROP TABLE IF EXISTS dbt.jobs__jobs_by_company_hst;
