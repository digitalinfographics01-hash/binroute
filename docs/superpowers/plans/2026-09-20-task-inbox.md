# Task Inbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `Automation/task-inbox/`, a PM2-managed Python service that logs every Telegram message and every email in `muhammad.zain@amalacademy.org`, classifies each into "waiting on my reply" / "asked to do something" via Claude, auto-resolves items once the user replies, sends Telegram-bot reminders on a fixed 8am-12pm PST schedule, and serves a 3-tab local dashboard.

**Architecture:** Two ingestion workers (Telethon for Telegram, Gmail API for email) write into a shared SQLite database. A classifier tags each new row via the Anthropic API. A resolver clears tasks once the user replies in the original thread. A reminder module enforces the fixed digest/idle-nudge schedule and sends via a dedicated Telegram bot. A Flask app reads the database for the dashboard. Everything runs as two PM2 processes: `task-inbox-worker` (ingestion + classify + resolve + remind, looped) and `task-inbox-dashboard` (Flask).

**Tech Stack:** Python 3.11+, Telethon, google-api-python-client, anthropic SDK, Flask, SQLite (stdlib), requests, pytest, PM2.

**Spec:** `docs/superpowers/specs/2026-09-20-task-inbox-design.md`

---

## Task 0: Project scaffold

**Files:**
- Create: `Automation/task-inbox/requirements.txt`
- Create: `Automation/task-inbox/.env.example`
- Create: `Automation/task-inbox/.gitignore`
- Create: `Automation/task-inbox/pytest.ini`
- Create: `Automation/task-inbox/tests/` (directory, via first test file in Task 1)

- [ ] **Step 1: Create `requirements.txt`**

```
telethon==1.36.0
google-api-python-client==2.149.0
google-auth==2.35.0
anthropic==0.39.0
flask==3.0.3
requests==2.32.3
python-dotenv==1.0.1
pytest==8.3.3
```

- [ ] **Step 2: Create `.env.example`**

```
# Anthropic API (console.anthropic.com) - used for message classification
ANTHROPIC_API_KEY=

# Telegram user session (my.telegram.org) - used to read all chats/DMs
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
TELEGRAM_SESSION_NAME=task_inbox_user

# Telegram bot (BotFather) - used ONLY to send reminder notifications,
# separate from the user session above and from merchant-automation's bot
TELEGRAM_BOT_TOKEN=
TELEGRAM_BOT_CHAT_ID=

# Gmail - reuses the existing OAuth client/token already authorized
# against muhammad.zain@amalacademy.org (see repo root)
GMAIL_CREDENTIALS_FILE=../../gmail-credentials.json
GMAIL_TOKEN_FILE=../../gmail-token.json
GMAIL_ACCOUNT=muhammad.zain@amalacademy.org

# Backfill window in days, applied once on first run per source
BACKFILL_DAYS=30

# SQLite database path
DB_PATH=task_inbox.db
```

- [ ] **Step 3: Create `.gitignore`**

```
.env
*.db
*.session
*.session-journal
__pycache__/
```

- [ ] **Step 4: Create `pytest.ini`**

```ini
[pytest]
pythonpath = .
```

- [ ] **Step 5: Commit**

```bash
git add Automation/task-inbox/requirements.txt Automation/task-inbox/.env.example Automation/task-inbox/.gitignore Automation/task-inbox/pytest.ini
git commit -m "chore: scaffold task-inbox project"
```

---

## Task 1: SQLite schema and core DB helpers

**Files:**
- Create: `Automation/task-inbox/db.py`
- Test: `Automation/task-inbox/tests/test_db.py`

- [ ] **Step 1: Write the failing tests**

`Automation/task-inbox/tests/test_db.py`:

```python
import pytest
import db


@pytest.fixture
def conn():
    connection = db.get_connection(":memory:")
    db.init_db(connection)
    yield connection
    connection.close()


def test_init_db_creates_tables(conn):
    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert {"messages", "tasks", "sync_state", "activity_state"} <= tables


def test_insert_message_returns_id_and_created_flag(conn):
    msg_id, created = db.insert_message(
        conn, "telegram", "ext-1", "chat-1", "alice", False,
        "hello", "link1", "2026-09-20T10:00:00+00:00",
    )
    assert created is True
    assert msg_id is not None


def test_insert_message_dedupes_on_source_and_external_id(conn):
    first_id, _ = db.insert_message(
        conn, "telegram", "ext-1", "chat-1", "alice", False,
        "hello", "link1", "2026-09-20T10:00:00+00:00",
    )
    second_id, created = db.insert_message(
        conn, "telegram", "ext-1", "chat-1", "alice", False,
        "hello again", "link1", "2026-09-20T10:00:01+00:00",
    )
    assert created is False
    assert second_id == first_id


def test_insert_task_and_get_open_tasks(conn):
    msg_id, _ = db.insert_message(
        conn, "gmail", "ext-2", "thread-1", "bob@example.com", False,
        "can you send the report", "link2", "2026-09-20T09:00:00+00:00",
    )
    db.insert_task(conn, msg_id, "asked_of_me", "Send the report")
    open_tasks = db.get_open_tasks(conn, category="asked_of_me")
    assert len(open_tasks) == 1
    assert open_tasks[0]["task_text"] == "Send the report"
    assert open_tasks[0]["status"] == "open"


def test_resolve_task_marks_resolved(conn):
    msg_id, _ = db.insert_message(
        conn, "gmail", "ext-3", "thread-2", "bob@example.com", False,
        "waiting on you", "link3", "2026-09-20T09:00:00+00:00",
    )
    db.insert_task(conn, msg_id, "waiting_on_reply", "Reply to bob")
    task = db.get_open_tasks(conn)[0]
    db.resolve_task(conn, task["id"])
    assert db.get_open_tasks(conn) == []


def test_sync_cursor_roundtrip(conn):
    assert db.get_sync_cursor(conn, "gmail") is None
    db.set_sync_cursor(conn, "gmail", "12345")
    assert db.get_sync_cursor(conn, "gmail") == "12345"
    db.set_sync_cursor(conn, "gmail", "67890")
    assert db.get_sync_cursor(conn, "gmail") == "67890"


def test_activity_state_update(conn):
    state = db.get_activity_state(conn)
    assert state["last_outbound_activity_at"] is None
    db.update_activity_state(conn, last_outbound_activity_at="2026-09-20T08:15:00+00:00")
    state = db.get_activity_state(conn)
    assert state["last_outbound_activity_at"] == "2026-09-20T08:15:00+00:00"


def test_mark_classified_and_get_unclassified(conn):
    msg_id, _ = db.insert_message(
        conn, "telegram", "ext-4", "chat-2", "carol", False,
        "hey", "link4", "2026-09-20T08:00:00+00:00",
    )
    assert len(db.get_unclassified_messages(conn)) == 1
    db.mark_classified(conn, msg_id)
    assert db.get_unclassified_messages(conn) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd Automation/task-inbox && python -m pytest tests/test_db.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'db'`

- [ ] **Step 3: Write `db.py`**

```python
import sqlite3
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL CHECK(source IN ('telegram', 'gmail')),
    external_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    sender TEXT,
    is_from_user INTEGER NOT NULL DEFAULT 0,
    text TEXT,
    link TEXT,
    timestamp TEXT NOT NULL,
    classified INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source, external_id)
);

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL REFERENCES messages(id),
    category TEXT NOT NULL CHECK(category IN ('waiting_on_reply', 'asked_of_me')),
    task_text TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open', 'resolved')),
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    UNIQUE(message_id, category)
);

CREATE TABLE IF NOT EXISTS sync_state (
    source TEXT PRIMARY KEY,
    cursor TEXT
);

CREATE TABLE IF NOT EXISTS activity_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_outbound_activity_at TEXT,
    last_reminder_sent_at TEXT,
    last_930_digest_sent_date TEXT,
    last_1130_digest_sent_date TEXT
);
"""


def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn):
    conn.executescript(SCHEMA)
    conn.execute("INSERT OR IGNORE INTO activity_state (id) VALUES (1)")
    conn.commit()


def insert_message(conn, source, external_id, chat_id, sender, is_from_user, text, link, timestamp):
    cur = conn.execute(
        """INSERT OR IGNORE INTO messages
           (source, external_id, chat_id, sender, is_from_user, text, link, timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (source, external_id, chat_id, sender, int(is_from_user), text, link, timestamp),
    )
    conn.commit()
    if cur.rowcount == 0:
        row = conn.execute(
            "SELECT id FROM messages WHERE source = ? AND external_id = ?",
            (source, external_id),
        ).fetchone()
        return row["id"], False
    return cur.lastrowid, True


def mark_classified(conn, message_id):
    conn.execute("UPDATE messages SET classified = 1 WHERE id = ?", (message_id,))
    conn.commit()


def get_unclassified_messages(conn):
    return conn.execute(
        "SELECT * FROM messages WHERE classified = 0 ORDER BY timestamp ASC"
    ).fetchall()


def insert_task(conn, message_id, category, task_text):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT OR IGNORE INTO tasks (message_id, category, task_text, status, created_at)
           VALUES (?, ?, ?, 'open', ?)""",
        (message_id, category, task_text, now),
    )
    conn.commit()


def get_open_tasks(conn, category=None):
    if category:
        return conn.execute(
            """SELECT tasks.*, messages.chat_id, messages.link, messages.sender, messages.source
               FROM tasks JOIN messages ON tasks.message_id = messages.id
               WHERE tasks.status = 'open' AND tasks.category = ?
               ORDER BY tasks.created_at ASC""",
            (category,),
        ).fetchall()
    return conn.execute(
        """SELECT tasks.*, messages.chat_id, messages.link, messages.sender, messages.source
           FROM tasks JOIN messages ON tasks.message_id = messages.id
           WHERE tasks.status = 'open'
           ORDER BY tasks.created_at ASC"""
    ).fetchall()


def resolve_task(conn, task_id):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE tasks SET status = 'resolved', resolved_at = ? WHERE id = ?",
        (now, task_id),
    )
    conn.commit()


def get_sync_cursor(conn, source):
    row = conn.execute("SELECT cursor FROM sync_state WHERE source = ?", (source,)).fetchone()
    return row["cursor"] if row else None


def set_sync_cursor(conn, source, cursor):
    conn.execute(
        """INSERT INTO sync_state (source, cursor) VALUES (?, ?)
           ON CONFLICT(source) DO UPDATE SET cursor = excluded.cursor""",
        (source, cursor),
    )
    conn.commit()


def get_activity_state(conn):
    return conn.execute("SELECT * FROM activity_state WHERE id = 1").fetchone()


def update_activity_state(conn, **fields):
    if not fields:
        return
    columns = ", ".join(f"{key} = ?" for key in fields)
    values = list(fields.values())
    conn.execute(f"UPDATE activity_state SET {columns} WHERE id = 1", values)
    conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd Automation/task-inbox && python -m pytest tests/test_db.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add Automation/task-inbox/db.py Automation/task-inbox/tests/test_db.py
git commit -m "feat(task-inbox): add SQLite schema and core DB helpers"
```

---

## Task 2: Message classifier (Claude)

**Files:**
- Create: `Automation/task-inbox/classifier.py`
- Test: `Automation/task-inbox/tests/test_classifier.py`

- [ ] **Step 1: Write the failing tests**

`Automation/task-inbox/tests/test_classifier.py`:

```python
import json
import types

import db
import classifier


class FakeResponse:
    def __init__(self, payload):
        self.content = [types.SimpleNamespace(text=json.dumps(payload))]


class FakeMessages:
    def __init__(self, payload):
        self._payload = payload
        self.last_call = None

    def create(self, **kwargs):
        self.last_call = kwargs
        return FakeResponse(self._payload)


class FakeClient:
    def __init__(self, payload):
        self.messages = FakeMessages(payload)


def test_classify_message_parses_both_true():
    client = FakeClient(
        {"waiting_on_reply": True, "asked_of_me": True, "task_text": "Send the report by Friday"}
    )
    result = classifier.classify_message(client, "Can you send me the report by Friday? Let me know.")
    assert result == {
        "waiting_on_reply": True,
        "asked_of_me": True,
        "task_text": "Send the report by Friday",
    }


def test_classify_message_parses_neither():
    client = FakeClient({"waiting_on_reply": False, "asked_of_me": False, "task_text": ""})
    result = classifier.classify_message(client, "FYI the invoice was paid.")
    assert result["waiting_on_reply"] is False
    assert result["asked_of_me"] is False


def test_classify_and_store_inserts_both_categories():
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    msg_id, _ = db.insert_message(
        conn, "gmail", "ext-1", "thread-1", "bob@example.com", False,
        "Can you send me the report by Friday?", "link1", "2026-09-20T09:00:00+00:00",
    )
    message_row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
    client = FakeClient(
        {"waiting_on_reply": True, "asked_of_me": True, "task_text": "Send the report by Friday"}
    )
    classifier.classify_and_store(client, conn, message_row)
    categories = {t["category"] for t in db.get_open_tasks(conn)}
    assert categories == {"waiting_on_reply", "asked_of_me"}
    assert db.get_unclassified_messages(conn) == []


def test_classify_and_store_inserts_nothing_when_neither():
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    msg_id, _ = db.insert_message(
        conn, "gmail", "ext-2", "thread-2", "bob@example.com", False,
        "FYI the invoice was paid.", "link2", "2026-09-20T09:00:00+00:00",
    )
    message_row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
    client = FakeClient({"waiting_on_reply": False, "asked_of_me": False, "task_text": ""})
    classifier.classify_and_store(client, conn, message_row)
    assert db.get_open_tasks(conn) == []
    assert db.get_unclassified_messages(conn) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd Automation/task-inbox && python -m pytest tests/test_classifier.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'classifier'`

- [ ] **Step 3: Write `classifier.py`**

```python
import json

import db

MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """You are triaging one message (a Telegram message or an \
email) sent to Muhammad, who runs client projects. Decide two independent \
things about this message:

1. waiting_on_reply: true if the sender appears to expect a reply, \
acknowledgment, or answer from Muhammad (a question, a check-in, "let me \
know", "thoughts?", anything that reads as unanswered).
2. asked_of_me: true if the sender is requesting Muhammad take an action \
or produce a deliverable (send something, fix something, do something, \
whether or not a deadline is stated).

A message can be both, either, or neither (e.g. a pure FYI/notification \
with no ask is neither).

Respond with ONLY a JSON object, no other text, in this exact shape:
{"waiting_on_reply": true or false, "asked_of_me": true or false, \
"task_text": "one short sentence describing the ask, or empty string if both are false"}
"""


def classify_message(client, text):
    response = client.messages.create(
        model=MODEL,
        max_tokens=200,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": text}],
    )
    raw = response.content[0].text.strip()
    result = json.loads(raw)
    return {
        "waiting_on_reply": bool(result.get("waiting_on_reply", False)),
        "asked_of_me": bool(result.get("asked_of_me", False)),
        "task_text": result.get("task_text", "") or "",
    }


def classify_and_store(client, conn, message_row):
    result = classify_message(client, message_row["text"] or "")
    if result["waiting_on_reply"]:
        db.insert_task(conn, message_row["id"], "waiting_on_reply", result["task_text"])
    if result["asked_of_me"]:
        db.insert_task(conn, message_row["id"], "asked_of_me", result["task_text"])
    db.mark_classified(conn, message_row["id"])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd Automation/task-inbox && python -m pytest tests/test_classifier.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add Automation/task-inbox/classifier.py Automation/task-inbox/tests/test_classifier.py
git commit -m "feat(task-inbox): add Claude-based message classifier"
```

---

## Task 3: Telegram bot notifier

**Files:**
- Create: `Automation/task-inbox/bot.py`
- Test: `Automation/task-inbox/tests/test_bot.py`

- [ ] **Step 1: Write the failing tests**

`Automation/task-inbox/tests/test_bot.py`:

```python
import bot


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def test_send_telegram_message_posts_expected_payload(monkeypatch):
    captured = {}

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return FakeResponse({"ok": True})

    monkeypatch.setattr(bot.requests, "post", fake_post)
    result = bot.send_telegram_message("BOT_TOKEN", "12345", "Hello Muhammad")

    assert captured["url"] == "https://api.telegram.org/botBOT_TOKEN/sendMessage"
    assert captured["json"] == {"chat_id": "12345", "text": "Hello Muhammad"}
    assert result == {"ok": True}


def test_send_telegram_message_raises_on_http_error(monkeypatch):
    def fake_post(url, json, timeout):
        return FakeResponse({"ok": False}, status_code=400)

    monkeypatch.setattr(bot.requests, "post", fake_post)
    raised = False
    try:
        bot.send_telegram_message("BOT_TOKEN", "12345", "Hello")
    except RuntimeError:
        raised = True
    assert raised
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd Automation/task-inbox && python -m pytest tests/test_bot.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'bot'`

- [ ] **Step 3: Write `bot.py`**

```python
import requests

TELEGRAM_API_BASE = "https://api.telegram.org"


def send_telegram_message(bot_token, chat_id, text):
    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"
    response = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
    response.raise_for_status()
    return response.json()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd Automation/task-inbox && python -m pytest tests/test_bot.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add Automation/task-inbox/bot.py Automation/task-inbox/tests/test_bot.py
git commit -m "feat(task-inbox): add Telegram bot notifier"
```

---

## Task 4: Resolver

**Files:**
- Create: `Automation/task-inbox/resolver.py`
- Test: `Automation/task-inbox/tests/test_resolver.py`

- [ ] **Step 1: Write the failing tests**

`Automation/task-inbox/tests/test_resolver.py`:

```python
import db
import resolver


def test_resolve_open_tasks_resolves_when_user_replied_after():
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    incoming_id, _ = db.insert_message(
        conn, "telegram", "ext-1", "chat-1", "client", False,
        "Can you confirm this?", "link1", "2026-09-20T09:00:00+00:00",
    )
    db.insert_task(conn, incoming_id, "waiting_on_reply", "Confirm this")
    db.insert_message(
        conn, "telegram", "ext-2", "chat-1", "me", True,
        "Confirmed!", "link2", "2026-09-20T09:05:00+00:00",
    )
    resolved = resolver.resolve_open_tasks(conn)
    assert resolved == 1
    assert db.get_open_tasks(conn) == []


def test_resolve_open_tasks_leaves_open_when_no_reply_yet():
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    incoming_id, _ = db.insert_message(
        conn, "telegram", "ext-1", "chat-1", "client", False,
        "Can you confirm this?", "link1", "2026-09-20T09:00:00+00:00",
    )
    db.insert_task(conn, incoming_id, "waiting_on_reply", "Confirm this")
    resolved = resolver.resolve_open_tasks(conn)
    assert resolved == 0
    assert len(db.get_open_tasks(conn)) == 1


def test_resolve_open_tasks_ignores_reply_in_different_chat():
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    incoming_id, _ = db.insert_message(
        conn, "telegram", "ext-1", "chat-1", "client", False,
        "Can you confirm this?", "link1", "2026-09-20T09:00:00+00:00",
    )
    db.insert_task(conn, incoming_id, "waiting_on_reply", "Confirm this")
    db.insert_message(
        conn, "telegram", "ext-2", "chat-2", "me", True,
        "Unrelated reply elsewhere", "link2", "2026-09-20T09:05:00+00:00",
    )
    resolved = resolver.resolve_open_tasks(conn)
    assert resolved == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd Automation/task-inbox && python -m pytest tests/test_resolver.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'resolver'`

- [ ] **Step 3: Write `resolver.py`**

```python
import db


def resolve_open_tasks(conn):
    resolved_count = 0
    for task in db.get_open_tasks(conn):
        newer_own_message = conn.execute(
            """SELECT 1 FROM messages
               WHERE chat_id = ? AND source = ? AND is_from_user = 1
                 AND timestamp > (SELECT timestamp FROM messages WHERE id = ?)
               LIMIT 1""",
            (task["chat_id"], task["source"], task["message_id"]),
        ).fetchone()
        if newer_own_message:
            db.resolve_task(conn, task["id"])
            resolved_count += 1
    return resolved_count
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd Automation/task-inbox && python -m pytest tests/test_resolver.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add Automation/task-inbox/resolver.py Automation/task-inbox/tests/test_resolver.py
git commit -m "feat(task-inbox): auto-resolve tasks when user replies"
```

---

## Task 5: Reminder scheduling logic

**Files:**
- Create: `Automation/task-inbox/reminder.py`
- Test: `Automation/task-inbox/tests/test_reminder.py`

- [ ] **Step 1: Write the failing tests**

`Automation/task-inbox/tests/test_reminder.py`:

```python
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
    db.insert_task(conn, msg_id, "waiting_on_reply", "Confirm")
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


def test_idle_nudge_falls_back_to_oldest_open_task_when_no_activity_recorded():
    conn = _conn_with_open_task(datetime(2026, 9, 20, 8, 0, tzinfo=PST).isoformat())
    now = datetime(2026, 9, 20, 9, 5, tzinfo=PST)
    sent = []
    result = reminder.check_and_send_reminders(conn, sent.append, now=now)
    assert result == "idle_nudge"
    assert len(sent) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd Automation/task-inbox && python -m pytest tests/test_reminder.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'reminder'`

- [ ] **Step 3: Write `reminder.py`**

```python
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
        lines.append(f"- [{task['category']}] {task['task_text']} ({task['link']})")
    return "\n".join(lines)


def check_and_send_reminders(conn, send_fn, now=None):
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd Automation/task-inbox && python -m pytest tests/test_reminder.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add Automation/task-inbox/reminder.py Automation/task-inbox/tests/test_reminder.py
git commit -m "feat(task-inbox): add fixed working-hours reminder schedule"
```

---

## Task 6: Gmail message parsing

**Files:**
- Create: `Automation/task-inbox/gmail_worker.py`
- Test: `Automation/task-inbox/tests/test_gmail_worker.py`

- [ ] **Step 1: Write the failing tests**

`Automation/task-inbox/tests/test_gmail_worker.py`:

```python
import base64

import gmail_worker


def _b64(text):
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii")


def test_parse_gmail_message_incoming():
    resource = {
        "id": "msg-1",
        "threadId": "thread-1",
        "internalDate": "1758360000000",
        "payload": {
            "headers": [
                {"name": "From", "value": "Bob <bob@example.com>"},
                {"name": "To", "value": "muhammad.zain@amalacademy.org"},
            ],
            "mimeType": "text/plain",
            "body": {"data": _b64("Can you send the report by Friday?")},
        },
    }
    result = gmail_worker.parse_gmail_message(resource, "muhammad.zain@amalacademy.org")
    assert result["source"] == "gmail"
    assert result["external_id"] == "msg-1"
    assert result["chat_id"] == "thread-1"
    assert result["sender"] == "Bob <bob@example.com>"
    assert result["is_from_user"] is False
    assert result["text"] == "Can you send the report by Friday?"
    assert result["link"] == "https://mail.google.com/mail/u/0/#inbox/thread-1"


def test_parse_gmail_message_outgoing():
    resource = {
        "id": "msg-2",
        "threadId": "thread-1",
        "internalDate": "1758360600000",
        "payload": {
            "headers": [{"name": "From", "value": "Muhammad Zain <muhammad.zain@amalacademy.org>"}],
            "mimeType": "text/plain",
            "body": {"data": _b64("Sending it over now.")},
        },
    }
    result = gmail_worker.parse_gmail_message(resource, "muhammad.zain@amalacademy.org")
    assert result["is_from_user"] is True


def test_parse_gmail_message_multipart_extracts_plain_text():
    resource = {
        "id": "msg-3",
        "threadId": "thread-2",
        "internalDate": "1758361200000",
        "payload": {
            "headers": [{"name": "From", "value": "bob@example.com"}],
            "mimeType": "multipart/alternative",
            "parts": [
                {"mimeType": "text/html", "body": {"data": _b64("<p>Hi</p>")}},
                {"mimeType": "text/plain", "body": {"data": _b64("Plain text body")}},
            ],
        },
    }
    result = gmail_worker.parse_gmail_message(resource, "muhammad.zain@amalacademy.org")
    assert result["text"] == "Plain text body"


def test_backfill_query_uses_days_window():
    query = gmail_worker.backfill_query(30)
    assert query.startswith("after:")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd Automation/task-inbox && python -m pytest tests/test_gmail_worker.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gmail_worker'`

- [ ] **Step 3: Write `gmail_worker.py`** (parsing functions only — I/O functions added in Task 7)

```python
import base64
from datetime import datetime, timedelta, timezone


def backfill_query(days):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return f"after:{cutoff.strftime('%Y/%m/%d')}"


def _get_header(headers, name):
    for header in headers:
        if header["name"].lower() == name.lower():
            return header["value"]
    return ""


def _extract_plain_text(payload):
    if payload.get("mimeType") == "text/plain" and "data" in payload.get("body", {}):
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")
    for part in payload.get("parts", []) or []:
        text = _extract_plain_text(part)
        if text:
            return text
    return ""


def parse_gmail_message(msg_resource, account_email):
    headers = msg_resource["payload"]["headers"]
    sender = _get_header(headers, "From")
    is_from_user = account_email.lower() in sender.lower()
    timestamp = datetime.fromtimestamp(
        int(msg_resource["internalDate"]) / 1000, tz=timezone.utc
    ).isoformat()
    text = _extract_plain_text(msg_resource["payload"])
    thread_id = msg_resource["threadId"]
    link = f"https://mail.google.com/mail/u/0/#inbox/{thread_id}"
    return {
        "source": "gmail",
        "external_id": msg_resource["id"],
        "chat_id": thread_id,
        "sender": sender,
        "is_from_user": is_from_user,
        "text": text,
        "link": link,
        "timestamp": timestamp,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd Automation/task-inbox && python -m pytest tests/test_gmail_worker.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add Automation/task-inbox/gmail_worker.py Automation/task-inbox/tests/test_gmail_worker.py
git commit -m "feat(task-inbox): parse Gmail API message resources"
```

---

## Task 7: Gmail backfill and poll (live API glue)

**Files:**
- Modify: `Automation/task-inbox/gmail_worker.py`

No automated test here — these functions call the real Gmail API and are covered by manual verification in Task 12. They're built on the already-tested `parse_gmail_message`/`backfill_query` from Task 6.

- [ ] **Step 1: Append credential loading and I/O functions to `gmail_worker.py`**

Add to the bottom of `Automation/task-inbox/gmail_worker.py`:

```python
import json

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

import db

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def load_credentials(credentials_file, token_file):
    with open(credentials_file) as f:
        installed = json.load(f)["installed"]
    with open(token_file) as f:
        token_data = json.load(f)
    return Credentials(
        token=token_data["access_token"],
        refresh_token=token_data.get("refresh_token"),
        token_uri=installed["token_uri"],
        client_id=installed["client_id"],
        client_secret=installed["client_secret"],
        scopes=GMAIL_SCOPES,
    )


def build_service(credentials_file, token_file):
    creds = load_credentials(credentials_file, token_file)
    return build("gmail", "v1", credentials=creds)


def _ingest_message(service, conn, message_id, account_email):
    full = service.users().messages().get(userId="me", id=message_id, format="full").execute()
    fields = parse_gmail_message(full, account_email)
    db.insert_message(conn, **fields)
    if fields["is_from_user"]:
        db.update_activity_state(conn, last_outbound_activity_at=fields["timestamp"])


def run_backfill(service, conn, account_email, days):
    query = backfill_query(days)
    request = service.users().messages().list(userId="me", q=query)
    while request is not None:
        response = request.execute()
        for item in response.get("messages", []):
            _ingest_message(service, conn, item["id"], account_email)
        request = service.users().messages().list_next(request, response)
    profile = service.users().getProfile(userId="me").execute()
    db.set_sync_cursor(conn, "gmail", profile["historyId"])


def run_poll(service, conn, account_email):
    cursor = db.get_sync_cursor(conn, "gmail")
    if cursor is None:
        run_backfill(service, conn, account_email, days=30)
        return
    response = service.users().history().list(userId="me", startHistoryId=cursor).execute()
    for record in response.get("history", []):
        for added in record.get("messagesAdded", []):
            _ingest_message(service, conn, added["message"]["id"], account_email)
    new_history_id = response.get("historyId")
    if new_history_id:
        db.set_sync_cursor(conn, "gmail", new_history_id)
```

- [ ] **Step 2: Run the existing Gmail test file to confirm nothing broke**

Run: `cd Automation/task-inbox && python -m pytest tests/test_gmail_worker.py -v`
Expected: PASS (4 tests, unchanged — new functions aren't unit tested here, only manually in Task 12)

- [ ] **Step 3: Commit**

```bash
git add Automation/task-inbox/gmail_worker.py
git commit -m "feat(task-inbox): add Gmail backfill and incremental poll"
```

---

## Task 8: Telegram message parsing

**Files:**
- Create: `Automation/task-inbox/telegram_worker.py`
- Test: `Automation/task-inbox/tests/test_telegram_worker.py`

- [ ] **Step 1: Write the failing tests**

`Automation/task-inbox/tests/test_telegram_worker.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd Automation/task-inbox && python -m pytest tests/test_telegram_worker.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'telegram_worker'`

- [ ] **Step 3: Write `telegram_worker.py`** (parsing functions only — I/O functions added in Task 9)

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd Automation/task-inbox && python -m pytest tests/test_telegram_worker.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add Automation/task-inbox/telegram_worker.py Automation/task-inbox/tests/test_telegram_worker.py
git commit -m "feat(task-inbox): parse Telethon message objects"
```

---

## Task 9: Telegram backfill and live listener (live API glue)

**Files:**
- Modify: `Automation/task-inbox/telegram_worker.py`

No automated test here — these functions require a real, logged-in Telethon session and are covered by manual verification in Task 12. Built on the already-tested `parse_telegram_message`/`backfill_cutoff` from Task 8.

**Note (open item from the spec):** the `link` format built in `parse_telegram_message` is a best-effort guess at Telegram's private-channel deep-link shape. Confirm it actually opens the right message once real data flows through in Task 12, and fix `parse_telegram_message` if not.

- [ ] **Step 1: Append client/listener functions to `telegram_worker.py`**

Add to the bottom of `Automation/task-inbox/telegram_worker.py`:

```python
from datetime import timezone

from telethon import TelegramClient, events

import db


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
```

- [ ] **Step 2: Run the existing Telegram test file to confirm nothing broke**

Run: `cd Automation/task-inbox && python -m pytest tests/test_telegram_worker.py -v`
Expected: PASS (4 tests, unchanged)

- [ ] **Step 3: Commit**

```bash
git add Automation/task-inbox/telegram_worker.py
git commit -m "feat(task-inbox): add Telegram backfill and live listener"
```

---

## Task 10: Dashboard

**Files:**
- Create: `Automation/task-inbox/dashboard.py`
- Create: `Automation/task-inbox/templates/dashboard.html`
- Test: `Automation/task-inbox/tests/test_dashboard.py`

- [ ] **Step 1: Write the failing tests**

`Automation/task-inbox/tests/test_dashboard.py`:

```python
import os
import tempfile

import db
import dashboard


def _make_client_with_data():
    tmp_path = tempfile.mktemp(suffix=".db")
    dashboard.DB_PATH = tmp_path
    conn = db.get_connection(tmp_path)
    db.init_db(conn)
    msg_id, _ = db.insert_message(
        conn, "gmail", "ext-1", "thread-1", "bob@example.com", False,
        "Can you send the report?", "https://mail.example/thread-1", "2026-09-20T09:00:00+00:00",
    )
    db.insert_task(conn, msg_id, "asked_of_me", "Send the report")
    conn.close()
    return dashboard.app.test_client(), tmp_path


def test_waiting_tab_excludes_asked_of_me_task():
    client, tmp_path = _make_client_with_data()
    try:
        response = client.get("/waiting")
        assert response.status_code == 200
        assert b"Send the report" not in response.data
    finally:
        os.remove(tmp_path)


def test_todo_tab_shows_asked_of_me_task():
    client, tmp_path = _make_client_with_data()
    try:
        response = client.get("/todo")
        assert response.status_code == 200
        assert b"Send the report" in response.data
    finally:
        os.remove(tmp_path)


def test_messages_tab_shows_raw_message():
    client, tmp_path = _make_client_with_data()
    try:
        response = client.get("/messages")
        assert response.status_code == 200
        assert b"Can you send the report?" in response.data
    finally:
        os.remove(tmp_path)


def test_index_redirects_to_waiting_tab():
    client, tmp_path = _make_client_with_data()
    try:
        response = client.get("/")
        assert response.status_code == 200
        assert b"Waiting on my reply" in response.data
    finally:
        os.remove(tmp_path)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd Automation/task-inbox && python -m pytest tests/test_dashboard.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dashboard'`

- [ ] **Step 3: Write `templates/dashboard.html`**

`Automation/task-inbox/templates/dashboard.html`:

```html
<!doctype html>
<html>
<head>
  <title>Task Inbox</title>
  <style>
    body { font-family: sans-serif; margin: 2rem; }
    nav a { margin-right: 1rem; }
    nav a.active { font-weight: bold; }
    .item { border-bottom: 1px solid #ddd; padding: 0.5rem 0; }
    .meta { color: #666; font-size: 0.85rem; }
  </style>
</head>
<body>
  <nav>
    <a href="/messages" class="{{ 'active' if view == 'messages' else '' }}">All Messages</a>
    <a href="/waiting" class="{{ 'active' if view == 'waiting' else '' }}">Waiting on my reply</a>
    <a href="/todo" class="{{ 'active' if view == 'todo' else '' }}">Asked to do something</a>
  </nav>
  {% if items|length == 0 %}
    <p>Nothing here.</p>
  {% endif %}
  {% for item in items %}
    <div class="item">
      {% if view == 'messages' %}
        <div class="meta">{{ item['source'] }} &middot; {{ item['timestamp'] }} &middot; {{ item['sender'] }}</div>
        <div>{{ item['text'] }}</div>
      {% else %}
        <div class="meta">{{ item['source'] }} &middot; {{ item['created_at'] }} &middot; {{ item['sender'] }}</div>
        <div>{{ item['task_text'] }}</div>
        <a href="{{ item['link'] }}" target="_blank">Open original</a>
      {% endif %}
    </div>
  {% endfor %}
</body>
</html>
```

- [ ] **Step 4: Write `dashboard.py`**

```python
from flask import Flask, render_template

import db

app = Flask(__name__)
DB_PATH = "task_inbox.db"


def get_conn():
    conn = db.get_connection(DB_PATH)
    db.init_db(conn)
    return conn


@app.route("/")
def index():
    return waiting()


@app.route("/messages")
def all_messages():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM messages ORDER BY timestamp DESC LIMIT 200").fetchall()
    return render_template("dashboard.html", view="messages", items=rows)


@app.route("/waiting")
def waiting():
    conn = get_conn()
    rows = db.get_open_tasks(conn, category="waiting_on_reply")
    return render_template("dashboard.html", view="waiting", items=rows)


@app.route("/todo")
def todo():
    conn = get_conn()
    rows = db.get_open_tasks(conn, category="asked_of_me")
    return render_template("dashboard.html", view="todo", items=rows)


if __name__ == "__main__":
    app.run(port=5055)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd Automation/task-inbox && python -m pytest tests/test_dashboard.py -v`
Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add Automation/task-inbox/dashboard.py Automation/task-inbox/templates/dashboard.html Automation/task-inbox/tests/test_dashboard.py
git commit -m "feat(task-inbox): add 3-tab local dashboard"
```

---

## Task 11: Worker loop orchestration and PM2 config

**Files:**
- Create: `Automation/task-inbox/worker_loop.py`
- Create: `Automation/task-inbox/ecosystem.config.js`

No automated test — this wires already-tested modules together and starts real network connections. Verified manually in Task 12.

- [ ] **Step 1: Write `worker_loop.py`**

```python
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
        telegram_client.loop.run_until_complete(
            telegram_worker.run_backfill(telegram_client, conn, BACKFILL_DAYS)
        )
        db.set_sync_cursor(conn, "telegram", "done")

    if db.get_sync_cursor(conn, "gmail") is None:
        gmail_worker.run_backfill(gmail_service, conn, os.environ["GMAIL_ACCOUNT"], BACKFILL_DAYS)

    classify_pending(conn, anthropic_client)
    resolver.resolve_open_tasks(conn)

    telegram_worker.register_live_listener(telegram_client, conn)

    async def periodic_cycle():
        while True:
            gmail_worker.run_poll(gmail_service, conn, os.environ["GMAIL_ACCOUNT"])
            classify_pending(conn, anthropic_client)
            resolver.resolve_open_tasks(conn)
            reminder.check_and_send_reminders(conn, send_reminder)
            await asyncio.sleep(POLL_SECONDS)

    telegram_client.loop.create_task(periodic_cycle())
    telegram_client.run_until_disconnected()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write `ecosystem.config.js`**

```js
module.exports = {
  apps: [
    {
      name: "task-inbox-worker",
      script: "worker_loop.py",
      interpreter: "python3",
      cwd: __dirname,
      autorestart: true,
    },
    {
      name: "task-inbox-dashboard",
      script: "dashboard.py",
      interpreter: "python3",
      cwd: __dirname,
      autorestart: true,
    },
  ],
};
```

- [ ] **Step 3: Run the full test suite to confirm nothing broke**

Run: `cd Automation/task-inbox && python -m pytest tests/ -v`
Expected: PASS (all tests from Tasks 1-10, ~30 tests)

- [ ] **Step 4: Commit**

```bash
git add Automation/task-inbox/worker_loop.py Automation/task-inbox/ecosystem.config.js
git commit -m "feat(task-inbox): wire ingestion, classification, and reminders into a PM2 worker loop"
```

---

## Task 12: Deploy to the Hostinger VPS and complete manual setup

The user wants this always running, independent of their local machine — so setup, credentials, and the actual running processes all live on the existing BinRoute VPS (`srv1369298.hstgr.cloud`, code at `/opt/binroute`, same one already running `scoring_daemon.py` and `binroute-router` under PM2), not locally. This task has no automated tests — it's real-credential setup and a live smoke test, done over SSH. Walk through each step and confirm the expected result before moving to the next.

**Note:** server IP/SSH access details in project memory are several months old — confirm current SSH access works before proceeding (`ssh root@<current-ip>`); if it's changed, sort that out first since every later step depends on it.

- [ ] **Step 1: Push local commits to GitHub**

Run (from the repo root, locally): `git push`
Expected: all Task 0-11 commits land on the `binroute` GitHub repo.

- [ ] **Step 2: Pull the new code onto the VPS**

SSH in and run: `cd /opt/binroute && git pull`
Expected: `Automation/task-inbox/` now exists on the server with all the code from Tasks 0-11 (but not `.env`, `*.db`, or `*.session` — those are gitignored and never travel through git, by design, since they hold secrets/local state).

- [ ] **Step 3: Copy the existing Gmail OAuth files to the server**

The root `gmail-credentials.json` / `gmail-token.json` are gitignored, so `git pull` did NOT bring them over — they need to be copied directly. From your local machine, run:

```bash
scp gmail-credentials.json gmail-token.json root@<server-ip>:/opt/binroute/
```

Expected: both files now exist at `/opt/binroute/gmail-credentials.json` and `/opt/binroute/gmail-token.json` on the server, matching the paths `Automation/task-inbox/.env`'s `GMAIL_CREDENTIALS_FILE=../../gmail-credentials.json` / `GMAIL_TOKEN_FILE=../../gmail-token.json` already expect.

- [ ] **Step 4: Set up a dedicated Python virtualenv on the server**

On the server:

```bash
cd /opt/binroute/Automation/task-inbox
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Expected: installs without error, isolated from any other Python setup already on the server (matching the pattern already used for `Automation/klaviyo-brand-sync/venv/`).

- [ ] **Step 5: Run the automated test suite on the server**

With the venv still active: `python -m pytest tests/ -v`
Expected: PASS (all tests) — confirms the deployed code behaves the same as it did locally.

- [ ] **Step 6: Create the dedicated Telegram bot**

In Telegram (from your phone/desktop, not the server), message **@BotFather**: `/newbot`, follow the prompts, save the token it gives you. Then message your new bot anything (e.g. "hi") so it can see your chat. Get your chat ID by visiting `https://api.telegram.org/bot<TOKEN>/getUpdates` in a browser and reading the `chat.id` field from your message.

- [ ] **Step 7: Get Telegram user-session API credentials**

Go to my.telegram.org → API development tools → create an app → copy `api_id` and `api_hash`.

- [ ] **Step 8: Create the rotated Anthropic API key**

At console.anthropic.com → API Keys → create a new key (rotating the one shared earlier in chat, per the security note already given).

- [ ] **Step 9: Create `.env` directly on the server**

On the server: `cd /opt/binroute/Automation/task-inbox && cp .env.example .env && nano .env`
Fill in `TELEGRAM_BOT_TOKEN`, `TELEGRAM_BOT_CHAT_ID`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, and `ANTHROPIC_API_KEY` with the values from Steps 6-8, typed/pasted directly into the file on the server — never through git, never back through chat. Confirm `GMAIL_ACCOUNT=muhammad.zain@amalacademy.org`, `DB_PATH=task_inbox.db`, `BACKFILL_DAYS=30`, and that the Gmail file paths match where Step 3 copied them.

- [ ] **Step 10: First manual run of the worker on the server (not under PM2 yet) to complete Telegram login and backfill**

With the venv active: `python worker_loop.py`
Expected: prompts for your phone number and login code the first time (Telethon's one-time interactive login, done once over this SSH session), then starts backfilling the last 30 days from both Telegram and Gmail. Watch the terminal for errors. Let it run until backfill activity settles, then stop it with Ctrl+C.

- [ ] **Step 11: Verify the dashboard shows backfilled data**

With the venv active: `python dashboard.py` (leave it running in this SSH session for now)
From your local machine: `ssh -L 5055:localhost:5055 root@<server-ip>` in a second terminal, then open `http://localhost:5055` in a browser. Confirm "All Messages" shows real recent messages/emails, and check "Waiting on my reply" / "Asked to do something" for anything correctly classified from the last month. Stop the dashboard (Ctrl+C) once confirmed — it'll run permanently under PM2 in Step 14.

- [ ] **Step 12: Verify the Telegram deep-link format**

Click "Open original" on a Telegram-sourced task in the dashboard (via the SSH tunnel from Step 11). Confirm it actually opens the right chat/message in Telegram. If it doesn't, fix the `link` construction in `parse_telegram_message` (`Automation/task-inbox/telegram_worker.py`) locally, update the relevant test in `tests/test_telegram_worker.py`, commit, push, and `git pull` on the server again before continuing.

- [ ] **Step 13: Live smoke tests**

With the worker not yet running continuously, this is easiest to fully verify once Step 14's PM2 processes are up — do the live smoke tests as part of Step 15 below instead of here.

- [ ] **Step 14: Start under PM2 on the server**

```bash
cd /opt/binroute/Automation/task-inbox
pm2 start ecosystem.config.js
pm2 status
pm2 save
```

Expected: both `task-inbox-worker` and `task-inbox-dashboard` show status `online` alongside the existing `scoring_daemon`/`binroute-router` processes. `pm2 save` so these survive a server reboot (matching how the existing daemons are kept up).

- [ ] **Step 15: Live smoke tests against the running deployment**

Send yourself a Telegram message from another account saying "Can you confirm this is working?" — wait a few minutes, confirm it appears under "Waiting on my reply" (via the SSH tunnel to the dashboard), reply in that same chat, confirm it clears. Separately, send a test email to `muhammad.zain@amalacademy.org` saying "Can you send me the Q3 report?" — confirm it appears under "Asked to do something," and confirm it stays in "All Messages" permanently even after resolving.

- [ ] **Step 16: Verify reminder timing**

Either wait for a real 9:30 AM or 11:30 AM PST window with an open item to confirm the bot DMs you, or check `pm2 logs task-inbox-worker` around those times to confirm the digest/idle-nudge logic is firing as expected.

- [ ] **Step 17: Final sanity check**

Run on the server: `cd /opt/binroute && git status`
Confirm `Automation/task-inbox/.env`, `*.db`, and `*.session` files show as untracked/ignored, never staged. Locally, run `git status` too and confirm the same — secrets must never enter git on either side.

---

## Task 13: Public dashboard subdomain (task-inbox.cswebform.cloud)

Matches the existing `analytics.cswebform.cloud` / `binroute.cswebform.cloud` pattern (nginx reverse proxy + Let's Encrypt SSL, same VPS). No automated tests — infrastructure config, verified by loading the URL.

- [ ] **Step 1: Point DNS at the server**

In whatever DNS provider manages `cswebform.cloud` (Hostinger DNS panel, per the existing subdomains), add an **A record**: `task-inbox` → the VPS's IPv4 address (same one `analytics` and `binroute` already point to). Wait for propagation (usually a few minutes, sometimes longer).

- [ ] **Step 2: Create the nginx site config**

On the server: `sudo nano /etc/nginx/sites-available/task-inbox.cswebform.cloud`

```nginx
server {
    listen 80;
    server_name task-inbox.cswebform.cloud;

    location / {
        proxy_pass http://127.0.0.1:5055;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

- [ ] **Step 3: Enable the site and reload nginx**

```bash
sudo ln -s /etc/nginx/sites-available/task-inbox.cswebform.cloud /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

Expected: `nginx -t` reports syntax OK before reloading.

- [ ] **Step 4: Confirm plain HTTP works before adding SSL**

Visit `http://task-inbox.cswebform.cloud` in a browser. Expected: the dashboard loads (same content as the SSH-tunnel test in Task 12).

- [ ] **Step 5: Add SSL via Let's Encrypt**

```bash
sudo certbot --nginx -d task-inbox.cswebform.cloud
```

Follow the prompts (certbot auto-edits the nginx config to add the certificate and redirect HTTP → HTTPS). Expected: matches the existing auto-renewing setup already used for `analytics.cswebform.cloud`.

- [ ] **Step 6: Confirm HTTPS works**

Visit `https://task-inbox.cswebform.cloud`. Expected: loads over HTTPS with a valid certificate, dashboard fully functional, all 3 tabs working with live data.
