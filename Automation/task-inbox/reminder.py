from datetime import datetime
from zoneinfo import ZoneInfo

import db

PST = ZoneInfo("America/Los_Angeles")
WORK_START_HOUR = 8
WORK_END_HOUR = 12
DIGEST_930 = (9, 30)
DIGEST_1130 = (11, 30)
IDLE_THRESHOLD_MINUTES = 60


def _is_within_work_hours(now_pst):
    return WORK_START_HOUR <= now_pst.hour < WORK_END_HOUR


def _is_digest_time(now_pst, hour, minute):
    return now_pst.hour == hour and now_pst.minute == minute


def format_digest(open_tasks):
    if not open_tasks:
        return "Nothing outstanding right now."
    lines = ["Outstanding items:"]
    for task in open_tasks:
        line = f"- [{task['category']}] {task['task_text']}"
        if task['link']:
            line += f" ({task['link']})"
        lines.append(line)
    return "\n".join(lines)


def check_and_send_reminders(conn, send_fn, now=None):
    # Caller-supplied now must be timezone-aware; naive datetimes are misinterpreted as local system time
    now_pst = (now or datetime.now(PST)).astimezone(PST)

    if not _is_within_work_hours(now_pst):
        return None

    state = db.get_activity_state(conn)
    today = now_pst.date().isoformat()
    open_tasks = db.get_open_tasks(conn)

    if _is_digest_time(now_pst, *DIGEST_930) and state["last_930_digest_sent_date"] != today:
        send_fn(format_digest(open_tasks))
        db.update_activity_state(
            conn, last_930_digest_sent_date=today, last_reminder_sent_at=now_pst.isoformat()
        )
        return "digest_930"

    if _is_digest_time(now_pst, *DIGEST_1130) and state["last_1130_digest_sent_date"] != today:
        send_fn(format_digest(open_tasks))
        db.update_activity_state(
            conn, last_1130_digest_sent_date=today, last_reminder_sent_at=now_pst.isoformat()
        )
        return "digest_1130"

    if not open_tasks:
        return None

    candidates = []
    if state["last_outbound_activity_at"]:
        candidates.append(datetime.fromisoformat(state["last_outbound_activity_at"]))
    if state["last_reminder_sent_at"]:
        candidates.append(datetime.fromisoformat(state["last_reminder_sent_at"]))
    if not candidates:
        candidates = [datetime.fromisoformat(t["created_at"]) for t in open_tasks]

    reference_dt = max(candidates).astimezone(PST)
    idle_minutes = (now_pst - reference_dt).total_seconds() / 60
    if idle_minutes >= IDLE_THRESHOLD_MINUTES:
        send_fn(format_digest(open_tasks))
        db.update_activity_state(conn, last_reminder_sent_at=now_pst.isoformat())
        return "idle_nudge"

    return None
