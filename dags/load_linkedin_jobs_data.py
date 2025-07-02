from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from load_from_kaggle_in_postgres import load_kaggle_data, load_linkedin_jobs_data, load_job_skills_data, load_job_summaries_data


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
    
    load_linkedin_jobs = PythonOperator(
        task_id='load_linkedin_jobs',
        python_callable=load_linkedin_jobs_data
    )

    load_skills = PythonOperator(
        task_id='load_job_skills',
        python_callable=load_job_skills_data
    )

    load_summaries = PythonOperator(
        task_id='load_job_summaries',
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


    load_kaggle_data >> [load_linkedin_jobs, load_skills, load_summaries] >> dbt_run >> dbt_test
