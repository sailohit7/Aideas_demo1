# email_notifier.py
"""
Email notifier utility using SMTP.
Environment variables:
- NOTIFY_EMAIL = true/false
- EMAIL_SMTP_HOST
- EMAIL_SMTP_PORT
- EMAIL_SENDER
- EMAIL_PASSWORD
- EMAIL_RECIPIENT
"""

import os
import smtplib
from email.message import EmailMessage

ENABLE = os.environ.get("NOTIFY_EMAIL", "false").lower() == "true"
SMTP_HOST = os.environ.get("EMAIL_SMTP_HOST")
SMTP_PORT = int(os.environ.get("EMAIL_SMTP_PORT") or 587)
SENDER = os.environ.get("EMAIL_SENDER")
PASSWORD = os.environ.get("EMAIL_PASSWORD")
RECIPIENT = os.environ.get("EMAIL_RECIPIENT")

def send_email_alert(subject: str, body: str):
    """Send email alert if enabled and config present."""
    if not ENABLE:
        return False
    if not (SMTP_HOST and SENDER and PASSWORD and RECIPIENT):
        print("⚠ Email alert disabled — missing SMTP configuration.")
        return False

    try:
        msg = EmailMessage()
        msg["From"] = SENDER
        msg["To"] = RECIPIENT
        msg["Subject"] = subject
        msg.set_content(body)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as smtp:
            smtp.starttls()
            smtp.login(SENDER, PASSWORD)
            smtp.send_message(msg)

        return True
    except Exception as e:
        print("⚠ Email send failed:", e)
        return False
