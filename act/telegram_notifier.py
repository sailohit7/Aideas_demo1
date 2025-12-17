# telegram_notifier.py
"""
Telegram alert utility for critical errors
- Uses environment variables:
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
  NOTIFY_TELEGRAM=true to enable
"""

import os
import requests

BOT_TOKEN = os.environ.get("8238241182:AAHz6KXHTJk-tCw-YWFvm9--aRsHeDVtR6E")
CHAT_ID = os.environ.get("5056158338")
ENABLE = os.environ.get("NOTIFY_TELEGRAM", "false").lower() == "true"

def send_telegram_alert(message: str):
    """Send Telegram alert if enabled and configured."""
    if not ENABLE:
        return False
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠ Telegram alert disabled — missing token/chat id.")
        return False
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": message}
        resp = requests.post(url, data=payload, timeout=8)
        if resp.status_code != 200:
            print("⚠ Telegram response:", resp.status_code, resp.text)
            return False
        return True
    except Exception as e:
        print(f"⚠ Telegram error: {e}")
        return False
