import asyncio
import os

from anthropic import Anthropic
from dotenv import load_dotenv

import bot
import classifier
import db
import gmail_worker
import reminder
import resolver
import telegram_worker

load_dotenv()

DB_PATH = os.environ["DB_PATH"]
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "30"))
POLL_SECONDS = 180


def classify_pending(conn, anthropic_client):
    for message_row in db.get_unclassified_messages(conn):
        classifier.classify_and_store(anthropic_client, conn, message_row)


def send_reminder(text):
    bot.send_telegram_message(
        os.environ["TELEGRAM_BOT_TOKEN"], os.environ["TELEGRAM_BOT_CHAT_ID"], text
    )


def main():
    conn = db.get_connection(DB_PATH)
    db.init_db(conn)

    anthropic_client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    gmail_service = gmail_worker.build_service(
        os.environ["GMAIL_CREDENTIALS_FILE"], os.environ["GMAIL_TOKEN_FILE"]
    )

    telegram_client = telegram_worker.build_client(
        os.environ.get("TELEGRAM_SESSION_NAME", "task_inbox_user"),
        int(os.environ["TELEGRAM_API_ID"]),
        os.environ["TELEGRAM_API_HASH"],
    )
    telegram_client.start()

    if db.get_sync_cursor(conn, "telegram") is None:
        print("worker_loop: starting Telegram backfill...")
        telegram_client.loop.run_until_complete(
            telegram_worker.run_backfill(telegram_client, conn, BACKFILL_DAYS)
        )
        db.set_sync_cursor(conn, "telegram", "done")
        print("worker_loop: Telegram backfill complete")

    if db.get_sync_cursor(conn, "gmail") is None:
        print("worker_loop: starting Gmail backfill...")
        gmail_worker.run_backfill(gmail_service, conn, os.environ["GMAIL_ACCOUNT"], BACKFILL_DAYS)
        print("worker_loop: Gmail backfill complete")

    classify_pending(conn, anthropic_client)
    resolver.resolve_open_tasks(conn)

    telegram_worker.register_live_listener(telegram_client, conn)

    async def periodic_cycle():
        while True:
            try:
                gmail_worker.run_poll(gmail_service, conn, os.environ["GMAIL_ACCOUNT"])
                classify_pending(conn, anthropic_client)
                resolver.resolve_open_tasks(conn)
                reminder.check_and_send_reminders(conn, send_reminder)
                print("worker_loop: cycle complete")
            except Exception as error:
                print(f"worker_loop: cycle failed, will retry next interval: {error}")
            await asyncio.sleep(POLL_SECONDS)

    telegram_client.loop.create_task(periodic_cycle())
    telegram_client.run_until_disconnected()


if __name__ == "__main__":
    main()
