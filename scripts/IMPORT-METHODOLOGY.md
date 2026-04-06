# BinRoute Data Import Methodology

## Problem
Sticky.io `order_find` API ignores the `page` parameter — always returns page 1 (max 500 orders). Standard pagination does not work.

## Solution: Date-Chunked Import
Instead of paginating, use narrow date ranges where each chunk returns < 500 orders.

### Chunk Sizing
| Daily Volume | Chunk Size | Example |
|---|---|---|
| < 400/day | 1-day windows | Sep, Oct, Nov 2025 |
| 400-800/day | 12-hour windows | Dec 2025 |
| 800+/day | 6-hour windows | Future high-volume months |

### Run Commands
```bash
# Show plan without importing
node scripts/chunked-import.js plan

# Start fresh import
node scripts/chunked-import.js run

# Resume from last checkpoint
node scripts/chunked-import.js resume
```

### Safety Features
- `INSERT OR IGNORE` — duplicates silently skipped, safe to re-run
- Checkpoint saved every 5 chunks to `checkpoint-chunked.json`
- Resume picks up from last completed chunk
- Overflow detection: if chunk returns 450+ orders, flags it for manual splitting
- DB saved every 5 chunks (survives crash)

### Post-Import Cleanup (run after every import)
```bash
# 1. Tag new orders with offer_name
node scripts/tag-offers.js

# 2. Recalculate derived_cycle for NULL values
# (run transaction classifier)
node src/classifiers/runner.js 1

# 3. Run decline_category classification
node src/classifiers/runner.js 1

# 4. Clear analytics cache (restart server)
# Analytics cache is TTL-based (5 min), auto-clears

# 5. Verify counts
node scripts/date-range-check.js
```

### Reconciliation Check
After import, compare DB counts vs Sticky API counts:
```bash
node scripts/sticky-gap-check.js
```

### Monthly Maintenance
Run weekly or after any Sticky data changes:
1. `node scripts/chunked-import.js plan` — check for gaps
2. `node scripts/chunked-import.js run` — fill gaps (safe to re-run)
3. `node scripts/tag-offers.js` — tag new orders
4. Restart server to clear cache

### API Performance
- ~20-60 seconds per API call
- 35ms minimum throttle between requests (built into StickyClient)
- No rate limit headers from Sticky API
- ~500 orders max per response regardless of page size parameter

### Date Format
Sticky API uses MM/DD/YYYY format for dates.
Internal DB uses YYYY-MM-DD HH:MM:SS (SQLite datetime).
