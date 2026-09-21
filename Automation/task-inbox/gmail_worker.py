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
