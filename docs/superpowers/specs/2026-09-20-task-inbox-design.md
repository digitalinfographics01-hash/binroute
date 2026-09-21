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
- **Delivery channel**: a new, dedicated Telegram bot (separate from the
  existing `MuhammadAlertsbot` used for chargeback alerts in
  `merchant-automation` — kept separate so the two alert streams don't
  mix). All reminders go through this bot DMing the user. **No OS/system
  notifications at all** — Telegram-bot messages are the only
  notification channel this tool uses.
- **Working hours**: 8:00 AM - 12:00 PM PST. All push reminders (bot
  messages) are confined to this window — nothing gets sent outside it,
  no exceptions. (The dashboard's own visual urgency on open items is
  unaffected by this, since that's pull — the user only sees it if they
  open the page — but no bot message is ever sent outside the window.)
- **9:30 AM PST daily digest**: one consolidated Telegram message listing
  everything currently outstanding across tabs 2/3.
- **11:30 AM PST final digest**: a second consolidated Telegram message,
  same content shape as the 9:30 AM one, listing everything still
  outstanding — a last call 30 minutes before the working day ends so the
  user can finalize/clear things before the 12:00 PM cutoff.
- **Hourly idle nudge**: during working hours, if the user hasn't sent
  any outbound message (Telegram, any chat, or a Gmail reply) in the last
  60 minutes AND at least one item is still open, send one consolidated
  Telegram reminder. The 60-minute idle clock resets on any outbound
  message the user sends, regardless of whether it resolved a tracked
  item.
- This replaces the age-based escalating-reminder idea from the first
  draft of this spec entirely — see the note under Reminder logic below.

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

- **Telegram user session** (for *reading*): `api_id`/`api_hash` from
  my.telegram.org + one-time phone/code login via Telethon — must be a
  user session, not a bot, to see full DM/group history across every
  chat the user is in.
- **Telegram bot** (for *notifying*): a new bot created via @BotFather,
  dedicated to this tool (separate from `MuhammadAlertsbot`). One-time
  setup: create it, get its token, and have the user message it once so
  it has a chat ID to DM back to — same pattern already used for the
  `merchant-automation` chargeback bot.
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
                                                                                    dedicated Telegram bot → user DM
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

- **`reminder.py`** — runs a check every few minutes, all times PST:
  - At 9:30 AM: always sends the daily digest (one consolidated Telegram
    message listing every open item in tabs 2/3), regardless of idle time.
  - At 11:30 AM: always sends the final digest — same shape, current
    outstanding items — regardless of idle time.
  - Between 8:00 AM-12:00 PM: if `now - last_outbound_activity_at >= 60
    min` and at least one item is open, sends one consolidated reminder,
    then updates `last_reminder_sent_at` so it doesn't repeat every cycle
    for the same idle stretch (only re-fires after either new outbound
    activity resets the clock, or another 60 idle minutes pass).
  - Outside 8:00 AM-12:00 PM: sends nothing, period.
  - Delivery is via the dedicated Telegram bot's `sendMessage` API call
    to the user's chat ID with it — not the Telethon user session, and
    not any OS-level notification.
  - `last_outbound_activity_at` is updated by the ingestion workers
    whenever they see the user (not a client) send a message in a
    Telegram chat, or by the Gmail worker whenever it sees a Sent-folder
    reply from the user.

- **`db.py`** — SQLite:
  - `messages`: id, source (telegram/gmail), chat/thread id, sender,
    text, timestamp, link, classified (bool) — the full log backing tab 1.
  - `tasks`: id, message_id (FK), category (waiting_on_reply /
    asked_of_me — a row per matched category, so a message with both gets
    two rows), task_text, status (open/resolved), created_at, resolved_at.
  - `sync_state`: per-chat (Telegram) / single-row (Gmail) cursor so a
    restart doesn't reprocess history.
  - `activity_state`: single row tracking `last_outbound_activity_at`,
    `last_reminder_sent_at`, `last_930_digest_sent_date`,
    `last_1130_digest_sent_date` (dates, not timestamps, so each fires
    once per calendar day even across restarts).

- **`dashboard.py`** — local Flask app (e.g. `localhost:5055`) with the
  3 tabs described above. Tabs 2/3 sort oldest-open-first and show
  increasing visual urgency (e.g. a badge/color that shifts the longer an
  item has been open) — purely visual/pull, independent of the push
  reminder timing above.

## Reminder logic (supersedes the first draft's age-based escalation)

The original draft of this spec proposed an age-based escalating schedule
(24h → 12h → 4-6h) independent of time of day. The user replaced that
with a simpler, concrete rule tied to their actual working hours:

- **9:30 AM PST** — daily digest of everything outstanding.
- **11:30 AM PST** — final digest of everything still outstanding, a last
  call 30 minutes before the working day ends.
- **Every 60 idle minutes within 8:00 AM-12:00 PM PST** — one nudge, only
  while something is actually open, only while the user's gone quiet.
- **Nothing outside 8:00 AM-12:00 PM PST** — hard cutoff, no exceptions.

## Data flow

New/backfilled message → stored raw in `messages` → classified → each
matched category becomes a row in `tasks` (status `open`) → shown on the
relevant dashboard tab(s) → user replies in the original chat/thread →
`resolver.py` notices → marked `resolved` → clears from tabs 2/3 (stays
in tab 1's log). If not resolved, `reminder.py` nudges per the working-
hours logic above (9:30 AM digest, hourly idle nudge, nothing outside
8:00 AM-12:00 PM PST) until it is.

## Error handling

- Telegram session invalidated → logged clearly, needs re-auth.
- Gmail token expiring → auto-refreshed; manual re-auth only if refresh
  itself fails.
- Claude API errors/rate limits → retried with backoff; message retried
  next pass, never silently dropped.
- Restarts are safe — cursors + unique message IDs prevent
  reprocessing/duplicates; `activity_state` persists across restarts so a
  restart mid-workday doesn't immediately re-fire a digest or idle nudge
  that already went out.

## Testing / verification plan

- Send a test "can you get back to me on X" and a test "can you do X for
  me" via both Telegram and email; confirm each lands in the correct
  tab(s), confirm replying clears it.
- Confirm the 9:30 AM and 11:30 AM digests both fire with current open
  items, confirm an hourly idle nudge fires after 60 quiet minutes within
  8:00 AM-12:00 PM with something open, and confirm nothing fires outside
  that window no matter how long items sit open.
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
