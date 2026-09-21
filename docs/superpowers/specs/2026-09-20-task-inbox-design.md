# Task Inbox — Design Spec

**Date:** 2026-09-20
**Status:** Approved, pending implementation plan

## Goal

A personal tool that watches every Telegram message and one work Gmail
inbox, detects messages where someone is waiting on a reply/action from
the user, and surfaces those as a running "needs my reply" list — so
nothing asked of the user gets missed or buried in scrollback.

**Why:** the user currently has to manually track outstanding asks across
Telegram group chats, Telegram DMs, and client emails. There's no single
place to see "here's everything people are still waiting on me for."

## Scope

**In scope:**
- Telegram: every chat the user is a member of — all groups and all 1-on-1
  DMs. ("All group chats are work chats" — user confirmed, plus DMs since
  clients may message directly.)
- Gmail: `muhammad.zain@amalacademy.org` only — this is the client-facing
  work inbox where task requests land.
- Task = a message where someone appears to be waiting on a reply or
  action from the user. Not a general "extract every action item"
  system — the trigger is specifically "someone needs something from me."
- Auto-resolution: once the user replies in the original Telegram chat or
  Gmail thread, the task clears itself from the list — no manual
  bookkeeping.
- A simple local dashboard (own tool, not a third-party app) listing open
  items, newest first, grouped by source, linking back to the original
  chat/thread.

**Explicitly out of scope (for this version):**
- `digitalinfographics.01@gmail.com` (company email) — user confirmed
  work email only, for now.
- Syncing into a third-party task app (Notion/Todoist/Google Tasks) —
  user wants their own simple list, not integration with another tool.
- Proactive notifications/digests — this is a pull ("I go check it")
  system, not a push one.
- Any two-way action (replying, sending messages) — read-only extraction
  and detection. The user still does all replying themselves, in
  Telegram/Gmail directly.

## Existing infrastructure being reused

- **Gmail OAuth**: `gmail-credentials.json` / `gmail-token.json` at the
  repo root are already authorized against `muhammad.zain@amalacademy.org`
  with `gmail.readonly` scope (currently used by
  `scripts/archive/fetch-cascade-emails.js` for Sticky.io cascade CSV
  pulls). This project reuses the same token — read-only access is
  already sufficient, no new Gmail scope/consent needed.
- **PM2**: the project already runs background daemons this way
  (`scoring_daemon.py`, `binroute-router`). This tool follows the same
  pattern rather than introducing a new process-management approach.

## New infrastructure needed

- **Telegram user session**: requires a personal `api_id`/`api_hash` from
  my.telegram.org (free, one-time) and a one-time phone-number + login-code
  authorization (via Telethon). Must be a *user* session, not a bot — bots
  cannot see full DM/group history or be silently present in chats the way
  a logged-in user account can.
- **Anthropic API key**: needed for the classification step (see below).
  Separate from any Claude.ai/Claude Code subscription — created at
  console.anthropic.com, billed pay-per-token. Goes into a local `.env`
  file only, gitignored, never committed or shared in chat.

## Architecture

New project at `Automation/task-inbox/`, Python-based, running as a
PM2-managed background service. Two live ingestion workers feed a shared
local SQLite database; each new message is classified by Claude; a small
local Flask dashboard reads the database and shows what's still open.

```
Telegram (Telethon, live listener) ─┐
                                     ├─► SQLite (messages, tasks, sync cursors)
Gmail (poll every ~3 min)          ─┘              │
                                                    ▼
                                          classifier.py (Claude call)
                                                    │
                                                    ▼
                                          dashboard.py (localhost)
```

## Components

- **`telegram_worker.py`** — Telethon client logged in as the user's own
  account. Uses a live event listener (`events.NewMessage`) across all
  chats rather than polling, since a persistent session makes this the
  cheaper/simpler option and gives near-real-time capture. Writes every
  new message (sender, chat, text, timestamp, deep-link) to `messages`.

- **`gmail_worker.py`** — polls the Gmail History API on an interval
  (~3 min) for new messages in `muhammad.zain@amalacademy.org` since the
  last synced `historyId`. Writes new messages to `messages`.

- **`classifier.py`** — shared function called for every new row in
  `messages`. Sends the message text + minimal context to Claude (a
  fast/cheap model) with a single job: decide if this is a message where
  the sender is waiting on a reply/action from the user, and if so,
  produce a short task description. Non-tasks are marked classified and
  discarded (not shown, but not re-processed either).

- **`resolver.py`** — periodic pass that checks, for every open task,
  whether the user has sent a subsequent message in that same Telegram
  chat, or a reply in that Gmail thread. If so, marks the task `resolved`.

- **`db.py`** — SQLite schema:
  - `messages`: id, source (telegram/gmail), chat/thread id, sender,
    text, timestamp, link, classified (bool)
  - `tasks`: id, message_id (FK), task_text, status (open/resolved),
    created_at, resolved_at
  - `sync_state`: per-chat (Telegram) / single-row (Gmail) cursor so a
    restart doesn't reprocess history

- **`dashboard.py`** — local Flask app (e.g. `localhost:5055`) listing
  all `open` tasks, newest first, grouped by source, each linking directly
  to the originating Telegram chat or Gmail thread.

## Data flow

New message arrives → stored raw in `messages` → `classifier.py` runs →
if it's a task, a row is added to `tasks` (status `open`) → shown on the
dashboard → user replies in the original chat/thread → `resolver.py`
notices on its next pass → task marked `resolved` → disappears from the
dashboard.

## Error handling

- **Telegram session invalidated** (logged out elsewhere, etc.) — worker
  logs the failure clearly; requires the same one-time re-auth flow as
  initial setup. Does not silently stop without a visible error.
- **Gmail token expiring** — refreshed automatically via the stored
  refresh token; if the refresh itself fails, requires manual re-auth.
- **Claude API errors/rate limits** — retried with backoff; a message
  stays `classified = false` and is retried on the next pass rather than
  being silently dropped or duplicated.
- **Restarts are safe** — `sync_state` cursors and unique message IDs
  mean a restart never reprocesses old history or double-creates tasks.

## Testing / verification plan

- Manual pass: send a test message via Telegram ("can you get back to me
  on X") and via email to the work inbox, confirm each appears on the
  dashboard within one poll/listen cycle, then reply in the original
  chat/thread and confirm it clears automatically.
- A way to inspect messages the classifier marked as *not* a task (e.g. a
  simple `--show-skipped` view or log), so the classification prompt can
  be tuned early if it's missing real asks or over-flagging noise.

## Open items to confirm during implementation

- Exact wording/strictness of the classification prompt will likely need
  a tuning pass once real messages start flowing through it.
- Telegram deep-link format for jumping straight to a specific message
  (vs. just the chat) should be confirmed against what Telethon/Telegram
  actually support.
