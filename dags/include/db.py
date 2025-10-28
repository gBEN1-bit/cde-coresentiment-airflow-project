import os
import psycopg2
from psycopg2.extras import execute_values

def get_connection():
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=os.getenv("SUPABASE_PORT", 5432),
        dbname=os.getenv("SUPABASE_DB", "postgres"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        sslmode="require"  # supabase typically requires TLS
    )
    return conn

def create_table_if_not_exists():
    sql = """
    CREATE TABLE IF NOT EXISTS public.wikipedia_pageviews (
        id SERIAL PRIMARY KEY,
        domain VARCHAR,
        page_title VARCHAR,
        view_count INTEGER,
        response_size INTEGER,
        hour_timestamp TIMESTAMP,
        created_at TIMESTAMP DEFAULT now()
    );
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    finally:
        conn.close()

def truncate_table():
    """Truncate the wikipedia_pageviews table before loading new data."""
    sql = "TRUNCATE TABLE public.wikipedia_pageviews;"
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    finally:
        conn.close()


def insert_records(records: list):

    if not records:
        return 0
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                values = [
                    (r["domain"], r["page_title"], r["view_count"], r["response_size"], r["hour_timestamp"])
                    for r in records
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO public.wikipedia_pageviews (domain, page_title, view_count, response_size, hour_timestamp)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                    """,
                    values
                )
                return cur.rowcount
    finally:
        conn.close()
