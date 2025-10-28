FROM apache/airflow:3.1.0

ENV PYTHONPATH=/opt/airflow:/opt/airflow/dags:/opt/airflow/scripts

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
