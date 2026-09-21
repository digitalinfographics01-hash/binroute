import db


def resolve_open_tasks(conn):
    resolved_count = 0
    for task in db.get_open_tasks(conn):
        newer_own_message = conn.execute(
            """SELECT 1 FROM messages
               WHERE chat_id = ? AND source = ? AND is_from_user = 1
                 AND timestamp > (SELECT timestamp FROM messages WHERE id = ?)
               LIMIT 1""",
            (task["chat_id"], task["source"], task["message_id"]),
        ).fetchone()
        if newer_own_message:
            db.resolve_task(conn, task["id"])
            resolved_count += 1
    return resolved_count
