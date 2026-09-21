# Task Inbox

A personal tool that watches every Telegram message and the work Gmail inbox
(`muhammad.zain@amalacademy.org`), figures out what actually needs a
response or an action, and surfaces it in one place — with reminders that
follow the user's real working hours instead of pinging at random.

## The problem it solves

Outstanding asks were scattered across dozens of Telegram group chats, DMs,
and client emails with no single view of "what am I still on the hook for."
This tool builds that view automatically and nags (gently, on a schedule)
until each item is actually handled.

## How it works, day to day

- **Every Telegram message and every email** in the work inbox gets logged.
- **Claude reads each one** and decides, independently: is someone waiting
  on a reply here? Is someone asking for an action? A message can be
  neither, either, or both.
- **A 3-tab dashboard** shows: everything (raw log), what's waiting on a
  reply, and what's been asked of you.
- **Replying in the original chat or email thread clears it automatically**
  — no manual checking-off.
- **Reminders** only happen 8:00 AM–12:00 PM PST: a digest at 9:30 AM, a
  final digest at 11:30 AM, and a nudge if an hour goes by with no reply
  sent and something's still open. Nothing outside that window, ever.
  Delivered by a dedicated Telegram bot — not the personal Telegram
  session, not any OS notification.

## What was built

12 small, single-purpose Python modules, each with its own tests
(42 tests total, all passing):

| File | Responsibility |
|---|---|
| `db.py` | SQLite schema + all persistence (messages, tasks, sync cursors, activity state) |
| `classifier.py` | Calls Claude to tag a message as waiting-on-reply / asked-of-me / neither |
| `bot.py` | Sends a message via the dedicated Telegram bot |
| `resolver.py` | Clears a task once the user has replied in that chat/thread |
| `reminder.py` | The fixed 8am–12pm PST digest/idle-nudge schedule |
| `gmail_worker.py` | Parses Gmail API messages; backfills 30 days; polls for new mail; retries with backoff on Gmail rate limits |
| `telegram_worker.py` | Parses Telethon messages; backfills 30 days across every chat; live-listens for new ones |
| `dashboard.py` + `templates/dashboard.html` | The 3-tab Flask UI |
| `worker_loop.py` | Wires everything above into one PM2-managed process; won't die permanently on a transient error |
| `ecosystem.config.js` | PM2 config for the worker + dashboard processes |

Full design reasoning: `docs/superpowers/specs/2026-09-20-task-inbox-design.md`
Full build plan (task-by-task, with all the code): `docs/superpowers/plans/2026-09-20-task-inbox.md`

## Deployment status

Running on the Hostinger VPS (`srv1369298`, same box as `scoring_daemon`
and `binroute-router`), not on any local machine — so it stays up
independent of anyone's laptop being on.

- [x] Code built, reviewed, and merged to `main`
- [x] Deployed to the VPS, Python venv + dependencies installed
- [x] Dedicated Telegram bot created, bot token + chat ID configured
- [x] `api_id`/`api_hash` obtained (my.telegram.org blocked repeatedly —
      root cause never fully confirmed, worked eventually)
- [x] Anthropic API key configured
- [x] One-time Telegram login completed — logged in as the real account
- [x] Telegram backfill (last 30 days, every chat) — **complete**
- [ ] Gmail backfill (last 30 days) — in progress; hit and fixed a real
      Gmail API rate-limit bug along the way (see `gmail_worker.py`'s
      `_ingest_message` retry logic)
- [ ] Start both processes under PM2 permanently
- [ ] Public dashboard at `task-inbox.cswebform.cloud` (nginx + SSL,
      matching the existing `analytics.cswebform.cloud` pattern)
- [ ] Live smoke test (send a real test message/email, confirm it shows
      up and clears correctly)

## Notable things learned building this

- Gmail's `messages.get` API has a real per-minute quota that a naive
  backfill loop can blow through — needed retry-with-backoff specifically
  for rate-limit-flavored errors, not a blanket catch-all.
- `my.telegram.org`'s API-app creation form is known to fail with a bare,
  unhelpful "ERROR" popup for reasons unrelated to the form itself
  (account eligibility, regional throttling) — persistence and retrying
  eventually got past it.
- The `anthropic` Python SDK version originally pinned needed `httpx`
  pinned alongside it, since a newer `httpx` release dropped a parameter
  the SDK still passed.
