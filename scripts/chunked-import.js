/**
 * Date-chunked importer for Sticky.io
 * Works around broken pagination by using narrow date ranges.
 *
 * Usage:
 *   node scripts/chunked-import.js plan                          — show chunk plan (client 1)
 *   node scripts/chunked-import.js run                           — execute import (client 1)
 *   node scripts/chunked-import.js resume                        — resume from checkpoint (client 1)
 *   node scripts/chunked-import.js run --client=3                — import client 3
 *   node scripts/chunked-import.js run --client=3 --start=2025-09-01 --end=2026-04-01
 */
const fs = require("fs");
const path = require("path");
const { initializeDatabase } = require("../src/db/schema");
const { querySql, runSql, saveDb, closeDb, checkpointWal } = require("../src/db/connection");
const StickyClient = require("../src/api/sticky-client");

// Parse --client=N from args
const CLIENT_ID = (() => {
  const arg = process.argv.find(a => a.startsWith('--client='));
  return arg ? parseInt(arg.split('=')[1], 10) : 1;
})();

const CHECKPOINT_FILE = path.join(__dirname, "..", `checkpoint-chunked-client${CLIENT_ID}.json`);
const MAX_PER_CHUNK = 450; // If a chunk returns this many, split it (near 500 limit)
const AVG_CHUNK_SECONDS = 60; // Estimated seconds per API call

// ──────────────────────────────────────────────
// Date helpers
// ──────────────────────────────────────────────

function formatDate(d) {
  return String(d.getMonth() + 1).padStart(2, "0") + "/" +
    String(d.getDate()).padStart(2, "0") + "/" + d.getFullYear();
}

function formatTime(d) {
  return String(d.getHours()).padStart(2, "0") + ":" +
    String(d.getMinutes()).padStart(2, "0") + ":" +
    String(d.getSeconds()).padStart(2, "0");
}

function addHours(d, h) {
  return new Date(d.getTime() + h * 3600000);
}

function addDays(d, n) {
  return new Date(d.getTime() + n * 86400000);
}

// ──────────────────────────────────────────────
// Build chunk plan
// ──────────────────────────────────────────────

function buildChunkPlan() {
  // Parse --start and --end from args, or auto-detect from DB
  const startArg = process.argv.find(a => a.startsWith('--start='));
  const endArg = process.argv.find(a => a.startsWith('--end='));

  let startDate, endDate;

  if (startArg && endArg) {
    startDate = new Date(startArg.split('=')[1] + 'T00:00:00');
    endDate = new Date(endArg.split('=')[1] + 'T00:00:00');
  } else {
    // Auto-detect: 6 months back from today, or from earliest order if no data
    const existing = querySql(
      `SELECT MIN(acquisition_date) as min_d, MAX(acquisition_date) as max_d
       FROM orders WHERE client_id = ? AND is_test = 0`, [CLIENT_ID]
    )[0];

    if (existing.max_d) {
      // Incremental: start from last order date
      startDate = new Date(existing.max_d.split(' ')[0] + 'T00:00:00');
      endDate = new Date();
    } else {
      // Fresh import: default to 6 months back
      endDate = new Date();
      startDate = new Date(endDate.getTime() - 180 * 86400000);
    }
  }

  // Generate 1-day chunks — auto-split handles high-volume days
  const chunks = [];
  let d = new Date(startDate);
  while (d < endDate) {
    const next = addDays(d, 1);
    const month = d.toLocaleString('en', { month: 'short' });
    chunks.push({
      start: new Date(d),
      end: new Date(Math.min(next.getTime() - 1000, endDate.getTime())),
      label: month + " " + d.getDate(),
    });
    d = next;
  }

  return chunks;
}

// ──────────────────────────────────────────────
// Show plan
// ──────────────────────────────────────────────

function showPlan(chunks) {
  const byMonth = {};
  for (const c of chunks) {
    const m = c.label.split(" ")[0];
    byMonth[m] = (byMonth[m] || 0) + 1;
  }

  console.log(`=== CHUNK PLAN — Client ${CLIENT_ID} ===`);
  console.log();
  for (const [m, cnt] of Object.entries(byMonth)) {
    console.log("  " + m + ": " + cnt + " chunks");
  }
  console.log();
  console.log("  Total chunks: " + chunks.length);
  console.log("  Avg time per chunk: ~" + AVG_CHUNK_SECONDS + "s");
  console.log("  Estimated total time: " + Math.round(chunks.length * AVG_CHUNK_SECONDS / 60) + " minutes (" + (chunks.length * AVG_CHUNK_SECONDS / 3600).toFixed(1) + " hours)");
  console.log();
  console.log("  First chunk: " + chunks[0].label + " (" + formatDate(chunks[0].start) + ")");
  console.log("  Last chunk:  " + chunks[chunks.length - 1].label + " (" + formatDate(chunks[chunks.length - 1].start) + ")");
  console.log();

  // Check DB counts for this client
  const dbCounts = querySql(`
    SELECT strftime('%Y-%m', acquisition_date) as month, COUNT(*) as cnt
    FROM orders WHERE client_id = ? AND is_test = 0
    GROUP BY month ORDER BY month
  `, [CLIENT_ID]);
  console.log("  Current DB counts for client " + CLIENT_ID + ":");
  for (const r of dbCounts) console.log("    " + r.month + ": " + r.cnt);
  const totalDb = dbCounts.reduce((s, r) => s + r.cnt, 0);
  console.log("    Total: " + totalDb);
}

// ──────────────────────────────────────────────
// Parse cascade chain from systemNotes
// ──────────────────────────────────────────────

function parseCascadeChain(systemNotes) {
  if (!systemNotes || !Array.isArray(systemNotes)) return null;
  const chain = [];
  for (const note of systemNotes) {
    const initialMatch = note.match(/Order attempted to process on gateway \((\d+)\) and declined due to (.+?), and cascade gateway id \((\d+)\)/);
    if (initialMatch) {
      if (!chain.find(c => c.gateway_id === parseInt(initialMatch[1]) && c.attempt === 0)) {
        chain.push({ gateway_id: parseInt(initialMatch[1]), decline_reason: initialMatch[2].trim(), attempt: 0, role: 'initial' });
      }
      continue;
    }
    const cascadeMatch = note.match(/Cascade gateway id \((\d+)\) also declined the sale due to (.+?) \((\d+)(?:st|nd|rd|th) attempt\)/);
    if (cascadeMatch) {
      chain.push({ gateway_id: parseInt(cascadeMatch[1]), decline_reason: cascadeMatch[2].trim(), attempt: parseInt(cascadeMatch[3]), role: 'cascade' });
      continue;
    }
    const declinedByMatch = note.match(/Declined by cascade gateway: \((\d+)\) (.+?) \((\d+)(?:st|nd|rd|th) attempt\)/);
    if (declinedByMatch) {
      chain.push({ gateway_id: parseInt(declinedByMatch[1]), decline_reason: declinedByMatch[2].trim(), attempt: parseInt(declinedByMatch[3]), role: 'cascade' });
      continue;
    }
    const reprocessMatch = note.match(/Reprocess attempt #(\d+), previous gateway id was (\d+)/);
    if (reprocessMatch) {
      chain.push({ gateway_id: parseInt(reprocessMatch[2]), decline_reason: null, attempt: parseInt(reprocessMatch[1]), role: 'reprocess_from' });
      continue;
    }
    const forceBillMatch = note.match(/Force bill failed by payment gateway \((.+?)\)/);
    if (forceBillMatch) {
      if (chain.length > 0 && !chain[chain.length - 1].decline_reason) {
        chain[chain.length - 1].decline_reason = forceBillMatch[1].trim();
      }
      continue;
    }
  }
  return chain.length > 0 ? chain : null;
}

/**
 * Fetch cascade chain for cascaded orders in a batch.
 * Calls order_view on each cascaded order to get systemNotes.
 */
async function backfillCascadeChain(client, orders) {
  // Find cascaded order IDs from the raw order data
  const cascadedIds = orders
    .filter(o => o.is_cascaded === '1' || o.is_cascaded === 1)
    .map(o => parseInt(o.order_id))
    .filter(Boolean);

  if (cascadedIds.length === 0) return { updated: 0, empty: 0, errors: 0 };

  // Check which ones already have cascade_chain
  const existing = querySql(`
    SELECT order_id FROM orders
    WHERE client_id = ${CLIENT_ID} AND cascade_chain IS NOT NULL
    AND order_id IN (${cascadedIds.join(',')})
  `);
  const existingSet = new Set(existing.map(r => r.order_id));
  const needChain = cascadedIds.filter(id => !existingSet.has(id));

  if (needChain.length === 0) return { updated: 0, empty: 0, errors: 0 };

  let updated = 0, empty = 0, errors = 0;

  // Fetch in parallel batches of 10 (within rate limiter)
  const CHAIN_BATCH = 10;
  for (let i = 0; i < needChain.length; i += CHAIN_BATCH) {
    const batch = needChain.slice(i, i + CHAIN_BATCH);
    const results = await Promise.all(
      batch.map(orderId =>
        client.orderView(orderId)
          .then(data => ({ orderId, data, error: null }))
          .catch(err => ({ orderId, data: null, error: err.message }))
      )
    );

    for (const { orderId, data, error } of results) {
      if (error) { errors++; continue; }
      const chain = parseCascadeChain(data.systemNotes);
      if (chain && chain.length > 0) {
        runSql(`UPDATE orders SET cascade_chain = ? WHERE order_id = ? AND client_id = ${CLIENT_ID}`,
          [JSON.stringify(chain), orderId]);
        updated++;
      } else {
        runSql(`UPDATE orders SET cascade_chain = ? WHERE order_id = ? AND client_id = ${CLIENT_ID}`,
          ['[]', orderId]);
        empty++;
      }
    }
  }

  return { updated, empty, errors };
}

// ──────────────────────────────────────────────
// Import a single chunk
// ──────────────────────────────────────────────

async function importChunk(client, ingestion, chunk) {
  const startDate = formatDate(chunk.start);
  const endDate = formatDate(chunk.end);
  const startTime = formatTime(chunk.start);
  const endTime = formatTime(chunk.end);

  const t = Date.now();
  let data;
  try {
    data = await client._post("order_find", {
      campaign_id: "all",
      start_date: startDate,
      end_date: endDate,
      start_time: startTime,
      end_time: endTime,
      date_type: "create",
      criteria: "all",
      search_type: "all",
      return_type: "order_view",
      resultsPerPage: 500,
      page: 1,
    });
  } catch (e) {
    return { error: e.message, ms: Date.now() - t, pulled: 0, inserted: 0, skipped: 0 };
  }
  const ms = Date.now() - t;

  const orders = client.parseOrdersFromResponse(data);
  if (orders.length === 0) return { ms, pulled: 0, inserted: 0, skipped: 0 };

  // Check overflow — chunk too large, auto-split
  if (orders.length >= MAX_PER_CHUNK) {
    const chunkMs = chunk.end.getTime() - chunk.start.getTime();
    if (chunkMs < 3600000) {
      // Less than 1 hour — can't split further, import what we got
      const beforeCount = ingestion._getDBCount();
      ingestion._saveOrderBatchToDB(orders);
      const afterCount = ingestion._getDBCount();
      const chainResult = await backfillCascadeChain(client, orders);
      return { ms, pulled: orders.length, inserted: afterCount - beforeCount, skipped: orders.length - (afterCount - beforeCount), chains: chainResult.updated, note: "MAXED (< 1hr window)" };
    }
    // Split into two halves and recurse
    const mid = new Date(chunk.start.getTime() + Math.floor(chunkMs / 2));
    const half1 = { start: chunk.start, end: new Date(mid.getTime() - 1000), label: chunk.label + " [1/2]" };
    const half2 = { start: mid, end: chunk.end, label: chunk.label + " [2/2]" };

    const r1 = await importChunk(client, ingestion, half1);
    const r2 = await importChunk(client, ingestion, half2);

    return {
      ms: (r1.ms || 0) + (r2.ms || 0),
      pulled: (r1.pulled || 0) + (r2.pulled || 0),
      inserted: (r1.inserted || 0) + (r2.inserted || 0),
      skipped: (r1.skipped || 0) + (r2.skipped || 0),
      note: "SPLIT → " + (r1.pulled || 0) + "+" + (r2.pulled || 0),
    };
  }

  // Insert using ingestion's batch method (handles normalization + INSERT OR IGNORE)
  const beforeCount = ingestion._getDBCount();
  ingestion._saveOrderBatchToDB(orders);
  const afterCount = ingestion._getDBCount();
  const inserted = afterCount - beforeCount;
  const skipped = orders.length - inserted;

  // Fetch cascade chain for cascaded orders in this batch
  const chainResult = await backfillCascadeChain(client, orders);

  return { ms: Date.now() - t, pulled: orders.length, inserted, skipped, chains: chainResult.updated };
}

// ──────────────────────────────────────────────
// Load/save checkpoint
// ──────────────────────────────────────────────

function loadCheckpoint() {
  if (fs.existsSync(CHECKPOINT_FILE)) {
    return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8"));
  }
  return { completedChunks: 0, totalImported: 0, totalSkipped: 0, lastChunkLabel: null };
}

function saveCheckpoint(cp) {
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(cp, null, 2));
}

// ──────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────

async function main() {
  const mode = process.argv[2] || "plan";
  await initializeDatabase();

  const chunks = buildChunkPlan();

  if (mode === "plan") {
    showPlan(chunks);
    process.exit(0);
  }

  // Run or resume
  const row = querySql(`SELECT * FROM clients WHERE id = ${CLIENT_ID}`)[0];
  if (!row) { console.error(`Client ${CLIENT_ID} not found.`); process.exit(1); }
  const client = new StickyClient({
    baseUrl: row.sticky_base_url,
    username: row.sticky_username,
    password: row.sticky_password,
  });

  // Load ingestion processor
  const DataIngestion = require("../src/api/ingestion");
  const ingestion = new DataIngestion(CLIENT_ID);
  ingestion.init(); // Loads client from DB

  const cp = mode === "resume" ? loadCheckpoint() : { completedChunks: 0, totalImported: 0, totalSkipped: 0, lastChunkLabel: null };
  const startFrom = cp.completedChunks;

  console.log(`=== CHUNKED IMPORT — Client ${CLIENT_ID} (${row.name}) — ` + (mode === "resume" ? "RESUMING from chunk " + startFrom : "STARTING") + " ===");
  console.log("Total chunks: " + chunks.length + " | Starting from: " + startFrom);
  console.log();

  for (let i = startFrom; i < chunks.length; i++) {
    const chunk = chunks[i];
    process.stdout.write("[" + (i + 1) + "/" + chunks.length + "] " + chunk.label + "... ");

    const result = await importChunk(client, ingestion, chunk);

    if (result.error) {
      console.log("ERROR: " + result.error.substring(0, 80));
      continue; // Skip this chunk, don't update checkpoint
    }

    const noteStr = result.note ? " [" + result.note + "]" : "";
    const chainStr = result.chains ? ", " + result.chains + " chains" : "";
    console.log(result.pulled + " pulled, " + result.inserted + " new, " + result.skipped + " skip" + chainStr + ", " + Math.round(result.ms / 1000) + "s" + noteStr);

    cp.completedChunks = i + 1;
    cp.totalImported += result.inserted;
    cp.totalSkipped += result.skipped;
    cp.lastChunkLabel = chunk.label;
    cp.timestamp = new Date().toISOString();

    // Save DB and checkpoint every 5 chunks
    if ((i + 1) % 5 === 0 || i === chunks.length - 1) {
      saveDb();
      saveCheckpoint(cp);
    }
    // Periodic WAL checkpoint every 10 chunks to keep WAL size manageable
    if ((i + 1) % 10 === 0) {
      checkpointWal();
    }
  }

  // Final save
  saveDb();
  saveCheckpoint(cp);

  console.log();
  console.log("=== IMPORT COMPLETE ===");
  console.log("  Chunks processed: " + cp.completedChunks + " / " + chunks.length);
  console.log("  Orders imported: " + cp.totalImported);
  console.log("  Duplicates skipped: " + cp.totalSkipped);

  // Reconciliation
  console.log();
  console.log(`=== RECONCILIATION — Client ${CLIENT_ID} ===`);
  const counts = querySql(`
    SELECT strftime('%Y-%m', acquisition_date) as month, COUNT(*) as cnt
    FROM orders WHERE is_test = 0 AND client_id = ?
    GROUP BY month ORDER BY month
  `, [CLIENT_ID]);
  for (const r of counts) console.log("  " + r.month + ": " + r.cnt);
  console.log("  Total: " + counts.reduce((s, r) => s + r.cnt, 0));

  closeDb();
}

main().catch(e => {
  console.error("Fatal:", e.message);
  process.exit(1);
});
