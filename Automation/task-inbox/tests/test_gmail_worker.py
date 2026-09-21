import base64
import types

from googleapiclient.errors import HttpError

import db
import gmail_worker


def _b64(text):
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii")


def _http_error(status, reason):
    resp = types.SimpleNamespace(status=status, reason=reason)
    content = f'{{"error": {{"errors": [{{"reason": "{reason}"}}]}}}}'.encode("utf-8")
    return HttpError(resp, content)


class _FlakyGetRequest:
    def __init__(self, responses):
        self._responses = responses  # shared reference so state advances across .get() calls

    def execute(self):
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _FakeMessagesResource:
    def __init__(self, get_responses):
        self._get_responses = get_responses

    def get(self, userId, id, format):
        return _FlakyGetRequest(self._get_responses)


class _FakeUsersResource:
    def __init__(self, get_responses):
        self._get_responses = get_responses

    def messages(self):
        return _FakeMessagesResource(self._get_responses)


class FakeService:
    def __init__(self, get_responses):
        self._get_responses = get_responses

    def users(self):
        return _FakeUsersResource(self._get_responses)


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


def test_ingest_message_retries_on_rate_limit_then_succeeds(monkeypatch):
    monkeypatch.setattr(gmail_worker.time, "sleep", lambda seconds: None)
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    resource = {
        "id": "msg-1",
        "threadId": "thread-1",
        "internalDate": "1758360000000",
        "payload": {
            "headers": [{"name": "From", "value": "bob@example.com"}],
            "mimeType": "text/plain",
            "body": {"data": _b64("hi")},
        },
    }
    service = FakeService(
        [
            _http_error(403, "rateLimitExceeded"),
            _http_error(429, "userRateLimitExceeded"),
            resource,
        ]
    )
    gmail_worker._ingest_message(service, conn, "msg-1", "muhammad.zain@amalacademy.org")
    stored = conn.execute("SELECT * FROM messages").fetchall()
    assert len(stored) == 1
    assert stored[0]["external_id"] == "msg-1"


def test_ingest_message_reraises_non_rate_limit_error_immediately(monkeypatch):
    def _fail_if_called(seconds):
        raise AssertionError("should not sleep/retry on a non-rate-limit error")

    monkeypatch.setattr(gmail_worker.time, "sleep", _fail_if_called)
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    service = FakeService([_http_error(403, "forbidden")])

    raised = False
    try:
        gmail_worker._ingest_message(service, conn, "msg-1", "muhammad.zain@amalacademy.org")
    except HttpError:
        raised = True
    assert raised


def test_ingest_message_gives_up_after_max_retries(monkeypatch):
    monkeypatch.setattr(gmail_worker.time, "sleep", lambda seconds: None)
    conn = db.get_connection(":memory:")
    db.init_db(conn)
    service = FakeService(
        [_http_error(403, "rateLimitExceeded") for _ in range(gmail_worker.RATE_LIMIT_MAX_RETRIES + 1)]
    )

    raised = False
    try:
        gmail_worker._ingest_message(service, conn, "msg-1", "muhammad.zain@amalacademy.org")
    except HttpError:
        raised = True
    assert raised
