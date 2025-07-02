from sqlalchemy import text, inspect
from db_create_engine import get_engine
import kaggle


DB_SCHEMA = 'src_linkedin_jobs'


def load_kaggle_data():
    dataset = 'asaniczka/1-3m-linkedin-jobs-and-skills-2024'
    kaggle.api.dataset_download_files(dataset, path='/opt/airflow/data', unzip=True)


def load_linkedin_jobs_data():
    engine = get_engine()
    conn = engine.connect()

    DB_TABLE = 'raw_jobs'

    inspector = inspect(engine)
    table_exists = inspector.has_table(DB_TABLE, schema=DB_SCHEMA)

    if not table_exists:
        create_table_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{DB_TABLE} (
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

    count_result = conn.execute(text(f"SELECT COUNT(1) FROM {DB_SCHEMA}.{DB_TABLE} limit 1")).scalar()

    if count_result > 0:
        conn.close()
        return

    linkedin_job_postings_csv = './data/linkedin_job_postings.csv'

    with conn.connection.cursor() as cursor:
        with open(linkedin_job_postings_csv, 'r') as f:
            next(f)
            cursor.copy_expert(
                f"""
                COPY {DB_SCHEMA}.{DB_TABLE}
                FROM STDIN
                WITH (FORMAT CSV, DELIMITER ',', QUOTE '"', NULL '')
                """,
                f
            )

    index_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_raw_jobs_job_link ON {DB_SCHEMA}.{DB_TABLE} (job_link);
    CREATE INDEX IF NOT EXISTS idx_raw_jobs_company ON {DB_SCHEMA}.{DB_TABLE} (company);
    CREATE INDEX IF NOT EXISTS idx_raw_jobs_company ON {DB_SCHEMA}.{DB_TABLE} (first_seen);
    ANALYZE VERBOSE {DB_SCHEMA}.{DB_TABLE}; 
    """
    conn.execute(text(index_sql))

    conn.close()


def load_job_skills_data():
    engine = get_engine()
    conn = engine.connect()
    inspector = inspect(engine)

    DB_TABLE = 'job_skills' 

    table_exists = inspector.has_table(DB_TABLE, schema=DB_SCHEMA)

    if not table_exists:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{DB_TABLE} (
            job_link TEXT PRIMARY KEY,
            job_skills TEXT
        );
        """
        conn.execute(text(create_table_sql))

    count_result = conn.execute(text(f"SELECT COUNT(1) FROM {DB_SCHEMA}.{DB_TABLE} limit 1")).scalar()

    if count_result > 0:
        conn.close()
        return

    load_csv = '/opt/airflow/data/job_skills.csv'

    with conn.connection.cursor() as cursor:
        with open(load_csv, 'r') as f:
            next(f)
            cursor.copy_expert(
                f"""
                COPY {DB_SCHEMA}.{DB_TABLE}
                FROM STDIN
                WITH (FORMAT CSV, DELIMITER ',', NULL '')
                """,
                f
            )

    conn.execute(text(f"ANALYZE VERBOSE {DB_SCHEMA}.{DB_TABLE};"))
    conn.close()


def load_job_summaries_data():
    engine = get_engine()
    conn = engine.connect()
    inspector = inspect(engine)

    DB_TABLE = 'job_summaries'

    table_exists = inspector.has_table(DB_TABLE, schema=DB_SCHEMA)

    if not table_exists:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{DB_TABLE} (
            job_link TEXT PRIMARY KEY,
            job_summary TEXT
        );
        """
        conn.execute(text(create_table_sql))

    count_result = conn.execute(text(f"SELECT COUNT(1) FROM {DB_SCHEMA}.{DB_TABLE} limit 1")).scalar()

    if count_result > 0:
        conn.close()
        return

    load_csv = '/opt/airflow/data/job_summary.csv'

    with conn.connection.cursor() as cursor:
        with open(load_csv, 'r') as f:
            next(f)
            cursor.copy_expert(
                f"""
                COPY {DB_SCHEMA}.{DB_TABLE}
                FROM STDIN
                WITH (FORMAT CSV, DELIMITER ',', QUOTE '"', NULL '')
                """,
                f
            )

    conn.execute(text(f"ANALYZE VERBOSE {DB_SCHEMA}.job_summaries;"))
    conn.close()