# Task Inbox — Design Spec

**Date:** 2026-09-20 (revised same day — scope expanded from single task list to full comms log + multi-category classification + reminders)
**Status:** Approved, pending implementation plan

## Goal

A personal tool that logs every Telegram message and every email in one
work Gmail inbox in one place, classifies each one, and splits them into:
a full searchable log, a "waiting on my reply" view, and an "asked to do
something" view — so nothing asked of the user gets missed or buried
across two separate apps and dozens of chats/threads. Unresolved items
get progressively reminded to the user until handled.

**Why:** the user currently has to manually track outstanding asks across
Telegram group chats, Telegram DMs, and client emails, with no single
place to see everything, and no memory jog for the things that slip.

## Scope

**In scope — data sources:**
- Telegram: every chat the user is a member of — all groups and all
  1-on-1 DMs.
- Gmail: `muhammad.zain@amalacademy.org` only — the client-facing work
  inbox where task requests land.
- **Backfill**: on first run, pull the last **1 month** of history from
  both sources (not full lifetime history), so the tool isn't starting
  from a blank slate.

**In scope — dashboard, 3 tabs:**
1. **All Messages** — a full, searchable log of every Telegram message and
   every email from the work inbox, regardless of classification.
2. **Waiting on my reply** — messages where someone appears to expect a
   response/acknowledgment from the user.
3. **Asked to do something** — messages where someone has requested an
   action or deliverable from the user.

A single message can appear in both tab 2 and tab 3 if it both expects a
reply *and* asks for an action (e.g. "can you send me X by Friday?") —
these are not treated as mutually exclusive categories.

**In scope — resolution & reminders:**
- **Auto-resolution**: once the user replies in the original Telegram
  chat or Gmail thread, the item clears from tabs 2/3 automatically (it
  stays visible in "All Messages" regardless — that tab is a log, not a
  worklist).
- **Reminders**: an item still open in tab 2 or 3 gets a repeat nudge,
  on an **escalating** schedule — infrequent at first, more frequent the
  longer it stays unresolved (see Reminder schedule below). Delivered via
  **both** a Telegram message to the user AND increasing visual urgency
  on the dashboard itself.

**Explicitly out of scope (for this version):**
- `digitalinfographics.01@gmail.com` (company email).
- Syncing into a third-party task app (Notion/Todoist/Google Tasks).
- Any two-way action on the user's behalf (replying, sending messages to
  other people) — read-only extraction, detection, and self-reminders
  only. The user still does all real replying themselves.

## Existing infrastructure being reused

- **Gmail OAuth**: `gmail-credentials.json` / `gmail-token.json` at the
  repo root, already authorized against `muhammad.zain@amalacademy.org`
  with `gmail.readonly` scope (currently used by
  `scripts/archive/fetch-cascade-emails.js`). Reused as-is.
- **PM2**: same background-daemon pattern as `scoring_daemon.py` /
  `binroute-router`.

## New infrastructure needed

- **Telegram user session**: `api_id`/`api_hash` from my.telegram.org +
  one-time phone/code login via Telethon (must be a user session, not a
  bot, to see full DM/group history and to send reminder messages as the
  user rather than as a separate bot the user'd have to go set up and
  message first).
- **Anthropic API key**: from console.anthropic.com, for the
  classification step. Goes in a local `.env`, gitignored, never shared
  in chat.

## Architecture

New project at `Automation/task-inbox/`, Python-based, PM2-managed. Two
ingestion workers backfill + then live-sync into a shared SQLite
database; each message is classified by Claude into zero or more
categories; a resolver watches for replies; a reminder loop nudges on
unresolved items; a local Flask dashboard reads it all.

```
Telegram (Telethon: backfill 1mo, then live listener) ─┐
                                                         ├─► SQLite (messages, tasks, sync cursors)
Gmail (backfill 1mo, then poll every ~3 min)           ─┘              │
                                                                        ▼
                                                              classifier.py (Claude call)
                                                                        │
                                            ┌───────────────────────────┼───────────────────────┐
                                            ▼                           ▼                        ▼
                                    dashboard.py (localhost)     resolver.py (marks done)   reminder.py (nudges)
                                                                                                    │
                                                                                                    ▼
                                                                                    Telegram message to self
```

## Components

- **`telegram_worker.py`** — Telethon client, backfills the last 1 month
  across all dialogs on first run, then a live event listener
  (`events.NewMessage`) going forward. Writes every message to `messages`.

- **`gmail_worker.py`** — backfills the last 1 month via the Gmail API
  search (`after:` date filter) on first run, then polls the History API
  every ~3 min going forward. Writes every message to `messages`.

- **`classifier.py`** — called for every new/backfilled row in
  `messages`. Sends the message text + minimal thread context to Claude
  (a fast/cheap model), which returns independent yes/no judgments for
  each category: *waiting on reply* and *asked to do something* (both,
  either, or neither can be true), plus a short task description for
  whichever category(ies) applied.

- **`resolver.py`** — periodic pass: for every open item in tab 2 or 3,
  checks whether the user has since sent a message in that Telegram chat
  or a reply in that Gmail thread. If so, marks it `resolved`.

- **`reminder.py`** — periodic pass (e.g. every 30 min): for every open
  item, checks its age and `last_reminded_at` against the escalating
  schedule below. If due, sends a Telegram message to the user (to their
  own Saved Messages, via the same Telethon session — no separate bot
  needed) naming the specific item and linking back to it, and updates
  `last_reminded_at`. Respects quiet hours (no reminders ~11pm-7am local)
  so it doesn't page the user overnight.

- **`db.py`** — SQLite:
  - `messages`: id, source (telegram/gmail), chat/thread id, sender,
    text, timestamp, link, classified (bool) — the full log backing tab 1.
  - `tasks`: id, message_id (FK), category (waiting_on_reply /
    asked_of_me — a row per matched category, so a message with both gets
    two rows), task_text, status (open/resolved), created_at,
    resolved_at, last_reminded_at.
  - `sync_state`: per-chat (Telegram) / single-row (Gmail) cursor so a
    restart doesn't reprocess history.

- **`dashboard.py`** — local Flask app (e.g. `localhost:5055`) with the
  3 tabs described above. Tabs 2/3 sort oldest-open-first and show
  increasing visual urgency (e.g. a badge/color that shifts as an item
  ages) matching the reminder escalation tiers.

## Reminder schedule (default, tunable)

Age of unresolved item → reminder interval:
- 0-24h: one reminder, at the 24h mark
- 1-3 days: every 12h
- 3+ days: every 4-6h

Same tiers drive the dashboard's visual urgency so what's shown matches
what's being nudged.

## Data flow

New/backfilled message → stored raw in `messages` → classified → each
matched category becomes a row in `tasks` (status `open`) → shown on the
relevant dashboard tab(s) → user replies in the original chat/thread →
`resolver.py` notices → marked `resolved` → clears from tabs 2/3 (stays
in tab 1's log). If not resolved, `reminder.py` nudges on the escalating
schedule until it is.

## Error handling

- Telegram session invalidated → logged clearly, needs re-auth.
- Gmail token expiring → auto-refreshed; manual re-auth only if refresh
  itself fails.
- Claude API errors/rate limits → retried with backoff; message retried
  next pass, never silently dropped.
- Restarts are safe — cursors + unique message IDs prevent
  reprocessing/duplicates; reminder state (`last_reminded_at`) persists
  across restarts so a restart doesn't reset escalation back to tier 1.

## Testing / verification plan

- Send a test "can you get back to me on X" and a test "can you do X for
  me" via both Telegram and email; confirm each lands in the correct
  tab(s), confirm replying clears it, confirm an intentionally-ignored
  test item gets reminded on schedule.
- A way to inspect messages classified as neither category, to tune the
  prompt early if it's missing real asks or over-flagging noise.
- Confirm 1-month backfill doesn't re-surface/re-remind on things the
  user already resolved before the tool ever ran (resolver should catch
  same-thread replies that happened during the backfill window itself).

## Open items to confirm during implementation

- Exact wording/strictness of the classification prompt will need a
  tuning pass once real messages flow through it.
- Telegram deep-link format for jumping to a specific message (vs. just
  the chat) needs confirming against what Telethon/Telegram actually
  support.
- Reminder schedule tiers above are a starting default — adjust after
  living with it a few days if it's too noisy or too sparse.
