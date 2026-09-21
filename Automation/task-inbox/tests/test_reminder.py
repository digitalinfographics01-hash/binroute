from datetime import datetime
from zoneinfo import ZoneInfo

import db
import reminder

PST = ZoneInfo("America/Los_Angeles")


def _conn_with_open_task(created_at_iso):
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    msg_id, _ = db.insert_message(
        conn, "gmail", "ext-1", "thread-1", "bob@example.com", False,
        "Can you confirm?", "link1", created_at_iso,
    )
    db.insert_task(conn, msg_id, "waiting_on_reply", "Confirm", created_at=created_at_iso)
    return conn


def test_no_reminder_outside_work_hours():
    conn = _conn_with_open_task(datetime(2026, 9, 20, 6, 0, tzinfo=PST).isoformat())
    sent = []
    now = datetime(2026, 9, 20, 7, 0, tzinfo=PST)
    result = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result is None
    assert sent == []


def test_no_reminder_at_or_after_noon():
    conn = _conn_with_open_task(datetime(2026, 9, 20, 6, 0, tzinfo=PST).isoformat())
    sent = []
    now = datetime(2026, 9, 20, 12, 0, tzinfo=PST)
    result = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result is None
    assert sent == []


def test_930_digest_sent_once_per_day():
    conn = _conn_with_open_task(datetime(2026, 9, 20, 6, 0, tzinfo=PST).isoformat())
    sent = []
    now = datetime(2026, 9, 20, 9, 30, tzinfo=PST)
    result = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result == "digest_930"
    assert len(sent) == 1

    result2 = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result2 is None
    assert len(sent) == 1


def test_1130_digest_sent_once_per_day():
    conn = _conn_with_open_task(datetime(2026, 9, 20, 6, 0, tzinfo=PST).isoformat())
    sent = []
    now = datetime(2026, 9, 20, 11, 30, tzinfo=PST)
    result = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result == "digest_1130"
    assert len(sent) == 1

    result2 = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result2 is None
    assert len(sent) == 1


def test_idle_nudge_fires_after_60_quiet_minutes():
    conn = _conn_with_open_task(datetime(2026, 9, 20, 8, 0, tzinfo=PST).isoformat())
    db.update_activity_state(
        conn, last_outbound_activity_at=datetime(2026, 9, 20, 9, 0, tzinfo=PST).isoformat()
    )
    now = datetime(2026, 9, 20, 10, 5, tzinfo=PST)
    sent = []
    result = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result == "idle_nudge"
    assert len(sent) == 1


def test_idle_nudge_does_not_fire_before_60_minutes():
    conn = _conn_with_open_task(datetime(2026, 9, 20, 8, 0, tzinfo=PST).isoformat())
    db.update_activity_state(
        conn, last_outbound_activity_at=datetime(2026, 9, 20, 9, 35, tzinfo=PST).isoformat()
    )
    now = datetime(2026, 9, 20, 10, 0, tzinfo=PST)
    sent = []
    result = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result is None
    assert sent == []


def test_idle_nudge_does_not_fire_with_no_open_tasks():
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    db.update_activity_state(
        conn, last_outbound_activity_at=datetime(2026, 9, 20, 9, 0, tzinfo=PST).isoformat()
    )
    now = datetime(2026, 9, 20, 10, 5, tzinfo=PST)
    sent = []
    result = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result is None
    assert sent == []


def test_idle_nudge_falls_back_to_most_recent_open_task_when_no_activity_recorded():
    conn = _conn_with_open_task(datetime(2026, 9, 20, 8, 0, tzinfo=PST).isoformat())
    now = datetime(2026, 9, 20, 9, 5, tzinfo=PST)
    sent = []
    result = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result == "idle_nudge"
    assert len(sent) == 1
