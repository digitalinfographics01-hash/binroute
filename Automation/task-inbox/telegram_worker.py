from datetime import datetime, timedelta, timezone
from telethon import TelegramClient, events

import db


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


def build_client(session_name, api_id, api_hash):
    return TelegramClient(session_name, api_id, api_hash)


async def run_backfill(client, conn, days):
    cutoff = backfill_cutoff(days)
    async for dialog in client.iter_dialogs():
        chat_id = dialog.id
        async for message in client.iter_messages(dialog, offset_date=cutoff, reverse=True):
            message_date = message.date if message.date.tzinfo else message.date.replace(tzinfo=timezone.utc)
            if message_date < cutoff:
                continue
            sender_name = "me" if message.out else (dialog.name or str(chat_id))
            fields = parse_telegram_message(message, chat_id, sender_name)
            db.insert_message(conn, **fields)
            if fields["is_from_user"]:
                db.update_activity_state(conn, last_outbound_activity_at=fields["timestamp"])


def register_live_listener(client, conn):
    @client.on(events.NewMessage())
    async def handler(event):
        chat = await event.get_chat()
        sender_name = "me" if event.message.out else (
            getattr(chat, "title", None) or getattr(chat, "first_name", None) or str(event.chat_id)
        )
        fields = parse_telegram_message(event.message, event.chat_id, sender_name)
        db.insert_message(conn, **fields)
        if fields["is_from_user"]:
            db.update_activity_state(conn, last_outbound_activity_at=fields["timestamp"])

    return handler
