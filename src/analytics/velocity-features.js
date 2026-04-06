/**
 * Velocity Features — Layer 2 enrichment for tx_features.
 *
 * Computes 4 temporal features that capture transaction context at the moment
 * each order was processed:
 *
 *   16. mid_velocity_daily    — # of transactions on this MID earlier that same day
 *   17. mid_velocity_weekly   — # of transactions on this MID in the prior 7 days
 *   18. customer_history_on_proc — # of prior transactions by this customer on this processor
 *   19. bin_velocity_weekly   — # of transactions from this BIN in the prior 7 days
 *
 * All counts are STRICTLY prior (not including the current order).
 */
const { querySql, getDb, saveDb } = require('../db/connection');

const BATCH_SIZE = 5000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Compute velocity features for all tx_features rows that still have NULL velocities.
 * @param {number|null} clientId — if null, processes all clients
 * @returns {number} rows updated
 */
function computeVelocityFeatures(clientId = null) {
  const clientFilter = clientId ? `AND tf.client_id = ${clientId}` : '';

  // Load all tx_features joined with order data needed for velocity
  const rows = querySql(`
    SELECT tf.id as tf_id, tf.client_id, tf.processor_name,
           o.processing_gateway_id, o.cc_first_6, o.customer_id,
           o.acquisition_date, o.order_id
    FROM tx_features tf
    JOIN orders o ON o.id = tf.order_id
    WHERE tf.mid_velocity_daily IS NULL
      ${clientFilter}
    ORDER BY o.acquisition_date ASC, o.order_id ASC
  `);

  if (rows.length === 0) return 0;
  console.log(`[Velocity] Computing features for ${rows.length.toLocaleString()} rows...`);

  // Also load ALL qualified orders for velocity context (including rows already computed)
  // We need the full population to count against, not just the uncomputed ones
  const allRows = querySql(`
    SELECT tf.id as tf_id, tf.client_id, tf.processor_name,
           o.processing_gateway_id, o.cc_first_6, o.customer_id,
           o.acquisition_date, o.order_id
    FROM tx_features tf
    JOIN orders o ON o.id = tf.order_id
    ${clientId ? `WHERE tf.client_id = ${clientId}` : ''}
    ORDER BY o.acquisition_date ASC, o.order_id ASC
  `);

  // Pre-parse dates once
  for (const r of allRows) {
    r._ts = r.acquisition_date ? new Date(r.acquisition_date).getTime() : 0;
    r._day = r.acquisition_date ? r.acquisition_date.split(' ')[0] : '';
  }

  const needsUpdate = new Set(rows.map(r => r.tf_id));

  // Compute all 4 velocity features
  console.log('[Velocity] Computing MID velocity daily...');
  const midDaily = _computeMidVelocityDaily(allRows);

  console.log('[Velocity] Computing MID velocity weekly...');
  const midWeekly = _computeMidVelocityWeekly(allRows);

  console.log('[Velocity] Computing customer history on processor...');
  const custHistory = _computeCustomerHistoryOnProc(allRows);

  console.log('[Velocity] Computing BIN velocity weekly...');
  const binWeekly = _computeBinVelocityWeekly(allRows);

  // Batch update
  console.log('[Velocity] Writing to database...');
  const db = getDb();
  const updateStmt = db.prepare(`
    UPDATE tx_features SET
      mid_velocity_daily = ?,
      mid_velocity_weekly = ?,
      customer_history_on_proc = ?,
      bin_velocity_weekly = ?,
      feature_version = 2
    WHERE id = ?
  `);

  let updated = 0;
  const updates = [];
  for (const r of allRows) {
    if (!needsUpdate.has(r.tf_id)) continue;
    updates.push([
      midDaily.get(r.tf_id) || 0,
      midWeekly.get(r.tf_id) || 0,
      custHistory.get(r.tf_id) || 0,
      binWeekly.get(r.tf_id) || 0,
      r.tf_id,
    ]);
  }

  for (let b = 0; b < updates.length; b += BATCH_SIZE) {
    const batch = updates.slice(b, b + BATCH_SIZE);
    const runBatch = db.transaction((batchRows) => {
      for (const params of batchRows) {
        updateStmt.run(...params);
        updated++;
      }
    });
    runBatch(batch);
  }

  saveDb();
  return updated;
}

// ---------------------------------------------------------------------------
// Feature computations
// ---------------------------------------------------------------------------

const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

/**
 * MID velocity daily — count of prior transactions on same gateway, same day.
 * Returns Map<tf_id, count>
 */
function _computeMidVelocityDaily(rows) {
  const result = new Map();

  // Group by processing_gateway_id
  const byGateway = new Map();
  for (const r of rows) {
    const gw = r.processing_gateway_id;
    if (!gw) { result.set(r.tf_id, 0); continue; }
    if (!byGateway.has(gw)) byGateway.set(gw, []);
    byGateway.get(gw).push(r);
  }

  for (const [, gwRows] of byGateway) {
    // Already sorted by date ASC
    let currentDay = '';
    let dayCount = 0;

    for (const r of gwRows) {
      if (r._day !== currentDay) {
        currentDay = r._day;
        dayCount = 0;
      }
      result.set(r.tf_id, dayCount);
      dayCount++;
    }
  }

  return result;
}

/**
 * MID velocity weekly — count of prior transactions on same gateway in prior 7 days.
 * Uses sliding window with two pointers.
 * Returns Map<tf_id, count>
 */
function _computeMidVelocityWeekly(rows) {
  const result = new Map();

  const byGateway = new Map();
  for (const r of rows) {
    const gw = r.processing_gateway_id;
    if (!gw) { result.set(r.tf_id, 0); continue; }
    if (!byGateway.has(gw)) byGateway.set(gw, []);
    byGateway.get(gw).push(r);
  }

  for (const [, gwRows] of byGateway) {
    let windowStart = 0;

    for (let i = 0; i < gwRows.length; i++) {
      const r = gwRows[i];
      const cutoff = r._ts - SEVEN_DAYS_MS;

      // Advance window start past expired entries
      while (windowStart < i && gwRows[windowStart]._ts < cutoff) {
        windowStart++;
      }

      // Count = everything in window before current index
      result.set(r.tf_id, i - windowStart);
    }
  }

  return result;
}

/**
 * Customer history on processor — count of prior transactions by this customer
 * on the same processor (across all time, not windowed).
 * Returns Map<tf_id, count>
 */
function _computeCustomerHistoryOnProc(rows) {
  const result = new Map();

  // Group by (customer_id, processor_name)
  const byKey = new Map();
  for (const r of rows) {
    if (!r.customer_id || !r.processor_name) { result.set(r.tf_id, 0); continue; }
    const key = `${r.customer_id}|${r.processor_name}`;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(r);
  }

  for (const [, group] of byKey) {
    // Already sorted by date ASC
    for (let i = 0; i < group.length; i++) {
      result.set(group[i].tf_id, i); // i = count of prior entries
    }
  }

  return result;
}

/**
 * BIN velocity weekly — count of prior transactions from same BIN in prior 7 days.
 * Returns Map<tf_id, count>
 */
function _computeBinVelocityWeekly(rows) {
  const result = new Map();

  const byBin = new Map();
  for (const r of rows) {
    const bin = r.cc_first_6;
    if (!bin) { result.set(r.tf_id, 0); continue; }
    if (!byBin.has(bin)) byBin.set(bin, []);
    byBin.get(bin).push(r);
  }

  for (const [, binRows] of byBin) {
    let windowStart = 0;

    for (let i = 0; i < binRows.length; i++) {
      const r = binRows[i];
      const cutoff = r._ts - SEVEN_DAYS_MS;

      while (windowStart < i && binRows[windowStart]._ts < cutoff) {
        windowStart++;
      }

      result.set(r.tf_id, i - windowStart);
    }
  }

  return result;
}

module.exports = { computeVelocityFeatures };
