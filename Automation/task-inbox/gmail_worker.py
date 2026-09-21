import base64
import json
import time
from datetime import datetime, timedelta, timezone

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import db


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


RATE_LIMIT_MAX_RETRIES = 4
RATE_LIMIT_RETRY_SECONDS = 65  # Gmail's rate limit here is a per-minute bucket;
# a short exponential backoff (seconds) doesn't reliably clear it, so wait
# past a full minute instead of guessing a shorter delay.
BACKFILL_PACING_SECONDS = 0.3  # spread requests out during backfill so a large
# volume of messages doesn't immediately re-trip the same per-minute quota


def _is_rate_limit_error(error):
    if error.resp.status not in (403, 429):
        return False
    message = str(error)
    return any(
        marker in message
        for marker in ("rateLimitExceeded", "quotaExceeded", "userRateLimitExceeded")
    )


def _ingest_message(service, conn, message_id, account_email):
    attempt = 0
    while True:
        try:
            full = service.users().messages().get(userId="me", id=message_id, format="full").execute()
            break
        except HttpError as error:
            if not _is_rate_limit_error(error) or attempt >= RATE_LIMIT_MAX_RETRIES:
                raise
            time.sleep(RATE_LIMIT_RETRY_SECONDS)
            attempt += 1
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
            time.sleep(BACKFILL_PACING_SECONDS)
        request = service.users().messages().list_next(request, response)
    profile = service.users().getProfile(userId="me").execute()
    db.set_sync_cursor(conn, "gmail", profile["historyId"])


def run_poll(service, conn, account_email):
    cursor = db.get_sync_cursor(conn, "gmail")
    if cursor is None:
        run_backfill(service, conn, account_email, days=30)
        return
    try:
        response = service.users().history().list(userId="me", startHistoryId=cursor).execute()
    except HttpError as error:
        if error.resp.status == 404:
            run_backfill(service, conn, account_email, days=30)
            return
        raise
    for record in response.get("history", []):
        for added in record.get("messagesAdded", []):
            _ingest_message(service, conn, added["message"]["id"], account_email)
    new_history_id = response.get("historyId")
    if new_history_id:
        db.set_sync_cursor(conn, "gmail", new_history_id)
