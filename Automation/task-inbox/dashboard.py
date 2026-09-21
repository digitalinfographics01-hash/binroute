from flask import Flask, render_template

import db

app = Flask(__name__)
DB_PATH = "task_inbox.db"


def get_conn():
    conn = db.get_connection(DB_PATH)
    db.init_db(conn)
    return conn


@app.route("/")
def index():
    return waiting()


@app.route("/messages")
def all_messages():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM messages ORDER BY timestamp DESC LIMIT 200").fetchall()
    conn.close()
    return render_template("dashboard.html", view="messages", items=rows)


@app.route("/waiting")
def waiting():
    conn = get_conn()
    rows = db.get_open_tasks(conn, category="waiting_on_reply")
    conn.close()
    return render_template("dashboard.html", view="waiting", items=rows)


@app.route("/todo")
def todo():
    conn = get_conn()
    rows = db.get_open_tasks(conn, category="asked_of_me")
    conn.close()
    return render_template("dashboard.html", view="todo", items=rows)


if __name__ == "__main__":
    app.run(port=5055)
