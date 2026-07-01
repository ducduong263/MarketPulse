"""
Telegram bot helper — sends alert messages with retry.
"""

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
GRAFANA_URL: str = os.getenv("GRAFANA_URL", "http://localhost:3000")

_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
_MAX_RETRIES = 3
_RETRY_DELAY = 5  # seconds


def send(text: str, retries: int = _MAX_RETRIES) -> bool:
    """Send a Telegram message (Markdown). Returns True if successful."""
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(_API_URL, json=payload, timeout=10)
            if resp.status_code == 200:
                return True
            logger.warning(
                "Telegram send failed (attempt %d/%d): HTTP %d — %s",
                attempt, retries, resp.status_code, resp.text[:200],
            )
        except requests.RequestException as exc:
            logger.warning(
                "Telegram send error (attempt %d/%d): %s", attempt, retries, exc
            )
        if attempt < retries:
            time.sleep(_RETRY_DELAY)
    return False


def build_alert_message(title: str, items: list[str]) -> str:
    """Format alert message with title and bullet list."""
    from datetime import datetime, timezone, timedelta

    ict = datetime.now(timezone(timedelta(hours=7)))
    ts = ict.strftime("%H:%M:%S ICT | %Y-%m-%d")
    lines = [f"*{title}*", "─" * 22]
    lines.extend(items)
    lines += ["", f"⏰ `{ts}`", f"🔗 {GRAFANA_URL}/d/mp-data-quality"]
    return "\n".join(lines)
