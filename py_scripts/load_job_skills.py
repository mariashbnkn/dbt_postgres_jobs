
import os
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

load_dotenv()
DB_HOST = os.getenv('POSTGRES_HOST')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASS = os.getenv('POSTGRES_PASSWORD')
DB_NAME = 'src_dbt'
DB_SCHEMA = 'src_linkedin_jobs'

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}')
conn = engine.connect()

# Создание таблицы для описаний вакансий
inspector = inspect(engine)
table_exists = inspector.has_table('job_skills', schema=DB_SCHEMA)

if not table_exists:
    create_summary_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.job_skills (
        job_link TEXT PRIMARY KEY,
        job_skills TEXT
    );
    """
    conn.execute(text(create_summary_table_sql))
    conn.commit()

load_csv = './data/job_skills.csv'

with conn.connection.cursor() as cursor:
    with open(load_csv, 'r') as f:
        next(f)
        cursor.copy_expert(
            f"""
            COPY {DB_SCHEMA}.job_skills
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER ',', NULL '')
            """,
            f
        )

sql = f"""
ANALYZE VERBOSE {DB_SCHEMA}.job_skills; 
"""
conn.execute(text(sql))
conn.commit()

conn.close()