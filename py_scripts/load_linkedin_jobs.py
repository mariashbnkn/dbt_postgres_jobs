
import os
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import kaggle

load_dotenv()
DB_HOST = os.getenv('POSTGRES_HOST')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASS = os.getenv('POSTGRES_PASSWORD')
DB_NAME = 'src_dbt'
DB_SCHEMA = 'src_linkedin_jobs'

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}')
conn = engine.connect()

# dataset = 'asaniczka/1-3m-linkedin-jobs-and-skills-2024'
# kaggle.api.dataset_download_files(dataset, path='./data', unzip=True)

# Создание таблицы для описаний вакансий
inspector = inspect(engine)
table_exists = inspector.has_table('row_jobs', schema=DB_SCHEMA)

if not table_exists:
    create_table_sql = text(f"""
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.raw_jobs (
        job_link text NULL,
        last_processed_time timestamp NULL,
        got_summary bool NULL DEFAULT false,
        got_ner bool NULL DEFAULT false,
        is_being_worked bool NULL DEFAULT false,
        job_title text NULL,
        company text NULL,
        job_location text NULL,
        first_seen timestamp NULL,
        search_city text NULL,
        search_country text NULL,
        search_position text NULL,
        job_level text NULL,
        job_type text NULL
    );
    """)

    conn.execute(create_table_sql)
    conn.commit()

linkedin_job_postings_csv = './data/linkedin_job_postings.csv'

with conn.connection.cursor() as cursor:
    with open(linkedin_job_postings_csv, 'r') as f:
        next(f)
        cursor.copy_expert(
            f"""
            COPY {DB_SCHEMA}.raw_jobs
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER ',', QUOTE '"', NULL '')
            """,
            f
        )
    conn.commit()

index_sql = f"""
CREATE INDEX IF NOT EXISTS idx_raw_jobs_job_link ON {DB_SCHEMA}.raw_jobs (job_link);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_company ON {DB_SCHEMA}.raw_jobs (company);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_company ON {DB_SCHEMA}.raw_jobs (first_seen);
ANALYZE VERBOSE {DB_SCHEMA}.raw_jobs; 
"""
conn.execute(text(index_sql))
conn.commit()

conn.close()
