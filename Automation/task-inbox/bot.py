import requests

TELEGRAM_API_BASE = "https://api.telegram.org"


def send_telegram_message(bot_token, chat_id, text):
    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"
    response = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
    response.raise_for_status()
    return response.json()
