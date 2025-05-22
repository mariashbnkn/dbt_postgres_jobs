CREATE DATABASE test_dbt
WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    TEMPLATE = template0
    CONNECTION LIMIT = -1;

--db test_dbt
CREATE SCHEMA dbt AUTHORIZATION postgres;

CREATE DATABASE src_dbt
WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    TEMPLATE = template0
    CONNECTION LIMIT = -1;

--db src_dbt
CREATE SCHEMA src_linkedin_jobs AUTHORIZATION postgres;

--db test_dbt
create extension postgres_fdw;

drop server if exists pg_dbt cascade;
create server pg_dbt foreign data wrapper postgres_fdw options (
	host 'localhost',
	dbname 'src_dbt',
	port '5432'
);

create user mapping for postgres server pg_dbt options (
	user 'postgres',
	password 'pass'
);

drop schema if exists src;
create schema src authorization postgres;
import foreign schema src_linkedin_jobs from server pg_dbt into src;