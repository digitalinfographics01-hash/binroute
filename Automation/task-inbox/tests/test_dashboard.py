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
