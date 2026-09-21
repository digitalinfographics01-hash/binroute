import types
from datetime import datetime, timezone

import telegram_worker


def _fake_message(msg_id, date, out, text):
    return types.SimpleNamespace(id=msg_id, date=date, out=out, text=text)


def test_parse_telegram_message_incoming():
    message = _fake_message(42, datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc), False, "Can you confirm?")
    result = telegram_worker.parse_telegram_message(message, chat_id=-1001234567890, sender_name="Alice")
    assert result["source"] == "telegram"
    assert result["external_id"] == "42"
    assert result["chat_id"] == "-1001234567890"
    assert result["sender"] == "Alice"
    assert result["is_from_user"] is False
    assert result["text"] == "Can you confirm?"
    assert result["timestamp"] == "2026-09-20T09:00:00+00:00"


def test_parse_telegram_message_outgoing():
    message = _fake_message(43, datetime(2026, 9, 20, 9, 5, tzinfo=timezone.utc), True, "Confirmed!")
    result = telegram_worker.parse_telegram_message(message, chat_id=-1001234567890, sender_name="Me")
    assert result["is_from_user"] is True


def test_parse_telegram_message_handles_naive_datetime():
    message = _fake_message(44, datetime(2026, 9, 20, 9, 10), False, "hey")
    result = telegram_worker.parse_telegram_message(message, chat_id=555, sender_name="Bob")
    assert result["timestamp"] == "2026-09-20T09:10:00+00:00"


def test_backfill_cutoff_is_timezone_aware_and_in_the_past():
    cutoff = telegram_worker.backfill_cutoff(30)
    assert cutoff.tzinfo is not None
    assert cutoff < datetime.now(timezone.utc)
