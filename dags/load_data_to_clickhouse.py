
from db_create_client_ch import get_client
import os
from dotenv import load_dotenv

load_dotenv()

db = os.getenv('CLICK_DB')


def load_jobs__dim_jobs_to_clickhouse():
    
    client = get_client()

    create_source = f"""
    CREATE TABLE IF NOT EXISTS {db}.mrt_jobs__dim_jobs
    (
        job_link text NULL,
        job_title text NULL,
        company text NULL,
        job_city text NULL,
        job_state text NULL,
        search_city text NULL,
        search_country text NULL,
        job_level text NULL,
        job_type text NULL,
        first_seen_date date NULL
    )
    ENGINE = PostgreSQL(
        '{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}',
        '{os.getenv("POSTGRES_DB")}',
        'mrt_jobs__dim_jobs',
        '{os.getenv("POSTGRES_USER")}',
        '{os.getenv("POSTGRES_PASSWORD")}',
        '{os.getenv("POSTGRES_SCHEMA")}'
    )
    SETTINGS external_table_functions_use_nulls = 1;
    """

    create_target = f"""
    CREATE TABLE IF NOT EXISTS {db}.jobs__dim_jobs_hst
    (
        job_link String,
        job_title String,
        company String,
        job_city String,
        job_state String,
        search_city String,
        search_country String,
        job_level String,
        job_type String,
        first_seen_date Date,
        load_date DateTime DEFAULT now(),
        sign Int8 DEFAULT 1
    )
    ENGINE = CollapsingMergeTree(sign)
    PARTITION BY toYYYYMM(load_date)
    ORDER BY (job_link, first_seen_date)
    SETTINGS index_granularity = 8192;
    """

    client.command(create_source)
    client.command(create_target)

    insert_query = f"""
    INSERT INTO {db}.jobs__dim_jobs_hst
    SELECT 
        job_link, job_title, company, 
        job_city, job_state, search_city, 
        search_country, job_level, job_type, 
        first_seen_date, now() as load_date, 1 as sign
    FROM dbt.mrt_jobs__dim_jobs;
    """
    optimize_query = f"OPTIMIZE TABLE {db}.jobs__dim_jobs_hst FINAL;"

    client.command(insert_query)
    client.command(optimize_query)


def load_jobs__jobs_by_company_to_clickhouse():
    
    client = get_client()

    create_source = f"""
    CREATE TABLE IF NOT EXISTS {db}.mrt_jobs__jobs_by_company
    (
        company text not NULL,
        first_seen timestamp not NULL,
        total_jobs int not NULL
    )
    ENGINE = PostgreSQL(
        '{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}',
        '{os.getenv("POSTGRES_DB")}',
        'mrt_jobs__jobs_by_company',
        '{os.getenv("POSTGRES_USER")}',
        '{os.getenv("POSTGRES_PASSWORD")}',
        '{os.getenv("POSTGRES_SCHEMA")}'
    )
    SETTINGS external_table_functions_use_nulls = 1;
    """

    create_target = f"""
    CREATE TABLE IF NOT EXISTS {db}.jobs__jobs_by_company_hst
    (
        company text not NULL,
        first_seen timestamp not NULL,
        total_jobs int not NULL,
        load_date DateTime DEFAULT now(),
        sign Int8 DEFAULT 1
    )
    ENGINE = CollapsingMergeTree(sign)
    PARTITION BY toYYYYMM(load_date)
    ORDER BY (company, first_seen)
    SETTINGS index_granularity = 8192;
    """

    client.command(create_source)
    client.command(create_target)

    insert_query = f"""
    INSERT INTO {db}.jobs__jobs_by_company_hst
    SELECT 
        company,
        first_seen,
        total_jobs, 
        now() as load_date, 
        1 as sign
    FROM dbt.mrt_jobs__jobs_by_company;
    """
    optimize_query = f"OPTIMIZE TABLE {db}.jobs__jobs_by_company_hst FINAL;"

    client.command(insert_query)
    client.command(optimize_query)    
