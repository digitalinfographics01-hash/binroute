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
