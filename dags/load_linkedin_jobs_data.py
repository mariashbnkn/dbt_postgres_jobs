from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from load_from_kaggle_in_postgres import load_kaggle_data, load_linkedin_jobs_data, load_job_skills_data, load_job_summaries_data
from load_data_to_clickhouse import load_jobs__dim_jobs_to_clickhouse, load_jobs__jobs_by_company_to_clickhouse


with DAG(
    dag_id='load_linkedin_jobs_data',
    start_date=datetime(2023, 1, 1),
    # запуск по расписанию
    schedule_interval=None,
    # запуск с текущего момента, не догонять предыдущие даты
    catchup=False,
    tags=['linkedin', 'postgres'],
) as dag:
    
    load_kaggle_data = PythonOperator(
        task_id='load_kaggle_data',
        python_callable=load_kaggle_data
    )
    
    load_linkedin_jobs_in_pg = PythonOperator(
        task_id='load_linkedin_jobs_in_pg',
        python_callable=load_linkedin_jobs_data
    )

    load_skills_in_pg = PythonOperator(
        task_id='load_skills_in_pg',
        python_callable=load_job_skills_data
    )

    load_summaries_in_pg = PythonOperator(
        task_id='load_summaries_in_pg',
        python_callable=load_job_summaries_data
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir /opt/airflow/pg_dbt --profiles-dir /opt/airflow/.dbt'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --project-dir /opt/airflow/pg_dbt --profiles-dir /opt/airflow/.dbt'
    )

    load_jobs__dim_jobs_to_ch = PythonOperator(
        task_id='load_jobs__dim_jobs_to_ch',
        python_callable=load_jobs__dim_jobs_to_clickhouse
    )

    load_jobs__jobs_by_company_to_ch = PythonOperator(
        task_id='load_jobs__jobs_by_company_to_ch',
        python_callable=load_jobs__jobs_by_company_to_clickhouse
    )


    load_kaggle_data >> [load_linkedin_jobs_in_pg, load_skills_in_pg, load_summaries_in_pg] >> dbt_run >> dbt_test
    dbt_test >> [load_jobs__dim_jobs_to_ch, load_jobs__jobs_by_company_to_ch] 