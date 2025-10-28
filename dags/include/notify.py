from datetime import datetime
from .utils import send_slack_message, send_email_alert
import psycopg2, os


def analyze_and_notify(**context):
    records = context['ti'].xcom_pull(task_ids='extract_companies')
    run_id = context['run_id']
    hour_ts = records[0]['hour_timestamp'] if records else None

    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=os.getenv("SUPABASE_PORT", 5432),
        dbname=os.getenv("SUPABASE_DB", "postgres"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        sslmode="require"
    )

    slack_lines = []
    summary_html = ""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT page_title, SUM(view_count) as total_views
                FROM public.wikipedia_pageviews
                WHERE hour_timestamp = %s
                AND page_title IN %s
                GROUP BY page_title
                ORDER BY total_views DESC;
            """, (hour_ts, tuple([r['page_title'] for r in records])))
            results = cur.fetchall()
            if results:
                slack_lines.append("Company Pageviews (Hourly):\n```")
                for title, views in results:
                    slack_lines.append(f"{title:<10} {views:>5}")
                slack_lines.append("```")
                top_company, top_views = results[0]
                slack_lines.append(f"*Highest pageviews:* {top_company} ({top_views} views)")

                summary_html += "<h3>Company Pageviews (Hourly)</h3><table border='1'><tr><th>Company</th><th>Pageviews</th></tr>"
                for title, views in results:
                    summary_html += f"<tr><td>{title}</td><td>{views}</td></tr>"
                summary_html += f"</table><p><b>Highest pageviews:</b> {top_company} ({top_views} views)</p>"
    finally:
        conn.close()

    hour_str = hour_ts.strftime("%Y-%m-%d %H:00 UTC") if hour_ts else "N/A"
    plain_body = f"""
Wiki Pipeline Notification {datetime.utcnow().strftime('%Y-%m-%d')}

Pipeline Run Summary
--------------------
DAG: wikipedia_pageviews_hourly
Run ID: {run_id}
Date/Hour Processed: {hour_str}
Status: Success

{'\n'.join(slack_lines)}

Check the database for full details.
"""
    html_body = f"""
<h2>Wiki Pipeline Notification {datetime.utcnow().strftime('%Y-%m-%d')}</h2>
<p><b>DAG:</b> wikipedia_pageviews_hourly<br>
<b>Run ID:</b> {run_id}<br>
<b>Date/Hour Processed:</b> {hour_str}<br>
<b>Status:</b> Success</p>
{summary_html}
<p>Check the database for full details.</p>
"""
    send_slack_message(plain_body)
    send_email_alert(
        subject=f"Wiki Pipeline Notification {datetime.utcnow().strftime('%Y-%m-%d')}",
        body=plain_body,
        html_body=html_body
    )
