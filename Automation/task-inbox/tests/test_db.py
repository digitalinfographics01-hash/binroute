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
