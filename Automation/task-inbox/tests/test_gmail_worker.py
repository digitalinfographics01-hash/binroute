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
