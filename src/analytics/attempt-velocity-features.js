/**
 * Attempt Velocity Features — Chunked computation for transaction_attempts.
 *
 * Memory-efficient: processes one gateway/BIN/customer chunk at a time.
 * Scales to millions of rows without loading entire dataset into memory.
 *
 * Features computed:
 *   mid_velocity_daily    — # of prior attempts on same gateway, same day
 *   mid_velocity_weekly   — # of prior attempts on same gateway in prior 7 days
 *   customer_history_on_proc — # of prior attempts by this customer on same processor
 *   bin_velocity_weekly   — # of prior attempts from same BIN in prior 7 days
 *
 * Sets feature_version = 2 after completion.
 */
const { querySql, getDb, checkpointWal } = require('../db/connection');

const BATCH_SIZE = 5000;
const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Compute all 4 velocity features for a client's transaction_attempts.
 * Only processes rows with feature_version = 1 (core features done, velocity pending).
 */
function computeAttemptVelocity(clientId) {
  const db = getDb();

  const pending = db.prepare(
    `SELECT COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? AND feature_version = 1`
  ).get(clientId).cnt;

  if (pending === 0) {
    console.log(`[AttemptVelocity] Client ${clientId}: no rows need velocity (all already >= v2)`);
    return 0;
  }

  console.log(`[AttemptVelocity] Client ${clientId}: ${pending.toLocaleString()} rows need velocity features`);

  // Step 1: MID velocity (daily + weekly) — chunked by gateway
  console.log(`[AttemptVelocity] Step 1/3: MID velocity (per-gateway)...`);
  _computeMidVelocityChunked(clientId, db);

  // Step 2: Customer history on processor — chunked by customer
  console.log(`[AttemptVelocity] Step 2/3: Customer history on processor...`);
  _computeCustomerHistoryChunked(clientId, db);

  // Step 3: BIN velocity weekly — chunked by BIN
  console.log(`[AttemptVelocity] Step 3/3: BIN velocity weekly...`);
  _computeBinVelocityChunked(clientId, db);

  // Bump feature_version to 2 for all rows that now have velocity
  const bumped = db.prepare(
    `UPDATE transaction_attempts SET feature_version = 2
     WHERE client_id = ? AND feature_version = 1
       AND mid_velocity_daily IS NOT NULL`
  ).run(clientId).changes;

  console.log(`[AttemptVelocity] Done — ${bumped.toLocaleString()} rows updated to feature_version=2`);
  return bumped;
}

// ---------------------------------------------------------------------------
// Step 1: MID Velocity (daily + weekly) — per gateway
// ---------------------------------------------------------------------------

function _computeMidVelocityChunked(clientId, db) {
  const gateways = db.prepare(
    `SELECT DISTINCT gateway_id FROM transaction_attempts WHERE client_id = ? AND feature_version = 1`
  ).all(clientId);

  console.log(`  ${gateways.length} gateways to process`);

  const updateStmt = db.prepare(
    `UPDATE transaction_attempts SET mid_velocity_daily = ?, mid_velocity_weekly = ? WHERE id = ?`
  );

  let totalUpdated = 0;

  for (let gi = 0; gi < gateways.length; gi++) {
    const gwId = gateways[gi].gateway_id;

    // Load ALL attempts for this gateway (both computed and pending) sorted by date
    // Need full population for accurate counts, not just pending rows
    const rows = db.prepare(
      `SELECT id, acquisition_date FROM transaction_attempts
       WHERE client_id = ? AND gateway_id = ?
       ORDER BY acquisition_date ASC, id ASC`
    ).all(clientId, gwId);

    // Pre-parse dates
    for (const r of rows) {
      r._ts = r.acquisition_date ? new Date(r.acquisition_date).getTime() : 0;
      r._day = r.acquisition_date ? r.acquisition_date.split(' ')[0] : '';
    }

    // Compute daily: count of prior same-day entries
    let currentDay = '';
    let dayCount = 0;
    const dailyMap = new Map();
    for (const r of rows) {
      if (r._day !== currentDay) {
        currentDay = r._day;
        dayCount = 0;
      }
      dailyMap.set(r.id, dayCount);
      dayCount++;
    }

    // Compute weekly: sliding window, 7-day lookback
    let windowStart = 0;
    const weeklyMap = new Map();
    for (let i = 0; i < rows.length; i++) {
      const cutoff = rows[i]._ts - SEVEN_DAYS_MS;
      while (windowStart < i && rows[windowStart]._ts < cutoff) windowStart++;
      weeklyMap.set(rows[i].id, i - windowStart);
    }

    // Batch update
    const updates = [];
    for (const r of rows) {
      updates.push([dailyMap.get(r.id) || 0, weeklyMap.get(r.id) || 0, r.id]);
    }

    for (let b = 0; b < updates.length; b += BATCH_SIZE) {
      const batch = updates.slice(b, b + BATCH_SIZE);
      db.transaction((batchRows) => {
        for (const params of batchRows) updateStmt.run(...params);
      })(batch);
    }

    totalUpdated += rows.length;

    if ((gi + 1) % 50 === 0 || gi === gateways.length - 1) {
      console.log(`  MID velocity: ${gi + 1}/${gateways.length} gateways (${totalUpdated.toLocaleString()} rows)`);
      checkpointWal();
    }
  }
}

// ---------------------------------------------------------------------------
// Step 2: Customer History on Processor — per customer
// ---------------------------------------------------------------------------

function _computeCustomerHistoryChunked(clientId, db) {
  // Get distinct customers in batches to avoid huge result sets
  const customers = db.prepare(
    `SELECT DISTINCT customer_id FROM transaction_attempts
     WHERE client_id = ? AND customer_id IS NOT NULL`
  ).all(clientId);

  console.log(`  ${customers.length.toLocaleString()} customers to process`);

  const updateStmt = db.prepare(
    `UPDATE transaction_attempts SET customer_history_on_proc = ? WHERE id = ?`
  );

  let totalUpdated = 0;
  const CUSTOMER_CHUNK = 500;

  for (let ci = 0; ci < customers.length; ci += CUSTOMER_CHUNK) {
    const chunk = customers.slice(ci, ci + CUSTOMER_CHUNK);
    const placeholders = chunk.map(() => '?').join(',');
    const custIds = chunk.map(c => c.customer_id);

    // Load all attempts for these customers
    const rows = db.prepare(
      `SELECT id, customer_id, processor_name, acquisition_date
       FROM transaction_attempts
       WHERE client_id = ? AND customer_id IN (${placeholders})
       ORDER BY customer_id, processor_name, acquisition_date ASC, id ASC`
    ).all(clientId, ...custIds);

    // Group by (customer_id, processor_name) and count prior
    const updates = [];
    let prevKey = '';
    let seqCount = 0;

    for (const r of rows) {
      const key = `${r.customer_id}|${r.processor_name || ''}`;
      if (key !== prevKey) {
        prevKey = key;
        seqCount = 0;
      }
      updates.push([seqCount, r.id]);
      seqCount++;
    }

    // Batch update
    for (let b = 0; b < updates.length; b += BATCH_SIZE) {
      const batch = updates.slice(b, b + BATCH_SIZE);
      db.transaction((batchRows) => {
        for (const params of batchRows) updateStmt.run(...params);
      })(batch);
    }

    totalUpdated += rows.length;

    if ((ci + CUSTOMER_CHUNK) % 5000 < CUSTOMER_CHUNK || ci + CUSTOMER_CHUNK >= customers.length) {
      console.log(`  Customer history: ${Math.min(ci + CUSTOMER_CHUNK, customers.length).toLocaleString()}/${customers.length.toLocaleString()} customers (${totalUpdated.toLocaleString()} rows)`);
      checkpointWal();
    }
  }

  // Set 0 for rows with NULL customer_id
  db.prepare(
    `UPDATE transaction_attempts SET customer_history_on_proc = 0
     WHERE client_id = ? AND customer_id IS NULL AND customer_history_on_proc IS NULL`
  ).run(clientId);
}

// ---------------------------------------------------------------------------
// Step 3: BIN Velocity Weekly — per BIN
// ---------------------------------------------------------------------------

function _computeBinVelocityChunked(clientId, db) {
  const bins = db.prepare(
    `SELECT DISTINCT cc_first_6 FROM transaction_attempts
     WHERE client_id = ? AND cc_first_6 IS NOT NULL`
  ).all(clientId);

  console.log(`  ${bins.length.toLocaleString()} BINs to process`);

  const updateStmt = db.prepare(
    `UPDATE transaction_attempts SET bin_velocity_weekly = ? WHERE id = ?`
  );

  let totalUpdated = 0;
  const BIN_CHUNK = 200;

  for (let bi = 0; bi < bins.length; bi += BIN_CHUNK) {
    const chunk = bins.slice(bi, bi + BIN_CHUNK);
    const placeholders = chunk.map(() => '?').join(',');
    const binValues = chunk.map(b => b.cc_first_6);

    const rows = db.prepare(
      `SELECT id, cc_first_6, acquisition_date
       FROM transaction_attempts
       WHERE client_id = ? AND cc_first_6 IN (${placeholders})
       ORDER BY cc_first_6, acquisition_date ASC, id ASC`
    ).all(clientId, ...binValues);

    // Pre-parse dates
    for (const r of rows) {
      r._ts = r.acquisition_date ? new Date(r.acquisition_date).getTime() : 0;
    }

    // Group by BIN and compute sliding window
    const updates = [];
    let currentBin = '';
    let binRows = [];

    const flush = () => {
      if (binRows.length === 0) return;
      let windowStart = 0;
      for (let i = 0; i < binRows.length; i++) {
        const cutoff = binRows[i]._ts - SEVEN_DAYS_MS;
        while (windowStart < i && binRows[windowStart]._ts < cutoff) windowStart++;
        updates.push([i - windowStart, binRows[i].id]);
      }
    };

    for (const r of rows) {
      if (r.cc_first_6 !== currentBin) {
        flush();
        currentBin = r.cc_first_6;
        binRows = [];
      }
      binRows.push(r);
    }
    flush();

    // Batch update
    for (let b = 0; b < updates.length; b += BATCH_SIZE) {
      const batch = updates.slice(b, b + BATCH_SIZE);
      db.transaction((batchRows) => {
        for (const params of batchRows) updateStmt.run(...params);
      })(batch);
    }

    totalUpdated += rows.length;

    if ((bi + BIN_CHUNK) % 2000 < BIN_CHUNK || bi + BIN_CHUNK >= bins.length) {
      console.log(`  BIN velocity: ${Math.min(bi + BIN_CHUNK, bins.length).toLocaleString()}/${bins.length.toLocaleString()} BINs (${totalUpdated.toLocaleString()} rows)`);
      checkpointWal();
    }
  }

  // Set 0 for rows with NULL BIN
  db.prepare(
    `UPDATE transaction_attempts SET bin_velocity_weekly = 0
     WHERE client_id = ? AND cc_first_6 IS NULL AND bin_velocity_weekly IS NULL`
  ).run(clientId);
}

module.exports = { computeAttemptVelocity };
