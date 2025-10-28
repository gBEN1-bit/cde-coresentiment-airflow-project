import os
import smtplib
from email.message import EmailMessage
import requests


def send_slack_message(text: str):
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        print("SLACK_WEBHOOK_URL not set; skipping slack alert")
        return
    payload = {"text": text}
    try:
        requests.post(webhook, json=payload, timeout=10).raise_for_status()
    except Exception as e:
        print("Slack notify failed:", e)

def send_email_alert(subject: str, body: str, html_body: str = None, to_addr: str = None):

    to_addr = to_addr or os.getenv("ALERT_EMAIL")
    if not to_addr:
        print("ALERT_EMAIL not set; skipping email")
        return

    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", 465))  # 465 for SSL
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASSWORD")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_addr
    msg.set_content(body)

    if html_body:
        msg.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as s:
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)
        print(f"Email sent to {to_addr}")
    except Exception as e:
        print("Email notify failed:", e)
