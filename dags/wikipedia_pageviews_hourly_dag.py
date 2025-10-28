# dags/wikipedia_pageviews_hourly_dag.py
from pendulum import datetime
import os
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# import logic from include/
from include.download import download_pageviews
from include.extract import extract_for_dag
from include.load import init_table_if_needed, load_for_dag
from include.notify import analyze_and_notify



DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": 300,  # seconds
}

with DAG(
    dag_id="wikipedia_pageviews_hourly",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    default_args=DEFAULT_ARGS,
    catchup=False,
    params={
        "year": os.getenv("WIKI_YEAR"),
        "month": os.getenv("WIKI_MONTH"),
        "day": os.getenv("WIKI_DAY"),
        "hour": os.getenv("WIKI_HOUR"),
    },
    tags=["coresentiment"],
) as dag:

    download = PythonOperator(
        task_id="download_pageviews",
        python_callable=download_pageviews,
        op_kwargs={
            "year": "{{ params.year }}",
            "month": "{{ params.month }}",
            "day": "{{ params.day }}",
            "hour": "{{ params.hour }}",
            "output_dir": os.getenv("OUTPUT_PATH", "/opt/airflow/output")
        },
    )

    extract = PythonOperator(
        task_id="extract_companies",
        python_callable=extract_for_dag,
        op_kwargs={"gz_path": "{{ ti.xcom_pull(task_ids='download_pageviews') }}"},
    )

    # 3. ensure table exists
    init_db = PythonOperator(
        task_id="init_table",
        python_callable=init_table_if_needed,
    )


    load = PythonOperator(
        task_id="load_to_supabase",
        python_callable=load_for_dag,
    )


    notify = PythonOperator(
        task_id="notify",
        python_callable=analyze_and_notify,
    )

    download >> extract >> init_db >> load >> notify
