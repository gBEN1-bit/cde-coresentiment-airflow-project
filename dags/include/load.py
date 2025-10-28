from .db import create_table_if_not_exists, insert_records, truncate_table

def init_table_if_needed():
    create_table_if_not_exists()
    return True

def load_records_to_db(records):
    if not records:
        return 0
    # Truncate table first
    truncate_table()
    inserted = insert_records(records)
    return inserted

def load_for_dag(**context):
    records = context['ti'].xcom_pull(task_ids='extract_companies')
    return load_records_to_db(records)
