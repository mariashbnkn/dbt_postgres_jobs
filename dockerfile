FROM apache/airflow:2.7.1-python3.11

# переключаемся на пользователя airflow
USER airflow 

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
RUN mkdir -p /opt/airflow/data
RUN chmod -R 777 /opt/airflow/data
