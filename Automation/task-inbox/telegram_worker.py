from datetime import datetime, timedelta, timezone


def backfill_cutoff(days):
    return datetime.now(timezone.utc) - timedelta(days=days)


def parse_telegram_message(message, chat_id, sender_name):
    timestamp = message.date
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    numeric_id = str(chat_id).lstrip("-")
    if numeric_id.startswith("100"):
        numeric_id = numeric_id[3:]
    link = f"https://t.me/c/{numeric_id}/{message.id}"
    return {
        "source": "telegram",
        "external_id": str(message.id),
        "chat_id": str(chat_id),
        "sender": sender_name,
        "is_from_user": bool(message.out),
        "text": message.text or "",
        "link": link,
        "timestamp": timestamp.isoformat(),
    }
