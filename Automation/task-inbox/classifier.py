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
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        # If Claude returns malformed JSON, treat as "no task found"
        return {
            "waiting_on_reply": False,
            "asked_of_me": False,
            "task_text": "",
        }
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
