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
