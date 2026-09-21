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
