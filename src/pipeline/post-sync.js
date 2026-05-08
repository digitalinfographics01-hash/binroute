/**
 * Post-sync pipeline — runs after order import to prepare data for analytics.
 *
 * Steps (in order):
 *   1. Classify orders (product group, tx_type, product_type_classified)
 *   2. Compute derived_product_role
 *   3. Compute processing_gateway_id
 *   4. Compute derived_cycle + derived_attempt
 *   5. Extract tx_features for AI training
 *   6. Reconcile shadow decisions against imported orders
 *
 * Uses the server's in-memory DB connection (querySql/runSql).
 * Must complete BEFORE analytics recompute.
 */
const { querySql, queryOneSql, runSql, saveDb } = require('../db/connection');
const StickyClient = require('../api/sticky-client');

/**
 * Run the full post-sync pipeline for a client.
 * @param {number} clientId
 * @returns {{ classified: number, rolesSet: number, cyclesSet: number }}
 */
async function runPostSyncPipeline(clientId) {
  console.log(`[PostSync] Starting pipeline for client ${clientId}...`);
  const startTime = Date.now();
  const errors = [];

  function safeStep(name, fn) {
    try {
      return fn();
    } catch (err) {
      console.error(`[PostSync] ${name} FAILED for client ${clientId}: ${err.message}`);
      errors.push(name);
      return null;
    }
  }

  const HEAVY_THRESHOLD = 10000;
  const allowHeavy = process.env.ALLOW_HEAVY_MAIN_CLASSIFICATION === 'true';

  // Step 1: Classify new orders (fallback guard — staging should pre-compute)
  const classified = safeStep('Step 1 (classify)', () => {
    const eligible = queryOneSql(
      'SELECT COUNT(*) as cnt FROM orders WHERE client_id = ? AND product_type_classified IS NULL', [clientId]
    )?.cnt || 0;

    if (eligible === 0) {
      console.log(`[PostSync] Step 1: 0 eligible rows (staging pre-computed)`);
      return 0;
    }
    if (eligible >= 1000) {
      console.warn(`[PostSync] WARNING: ${eligible} rows need classification — staging post-sync may have failed`);
    }
    if (eligible >= HEAVY_THRESHOLD && !allowHeavy) {
      console.error(`[PostSync] SKIPPED Step 1: ${eligible} rows too large for main process. Set ALLOW_HEAVY_MAIN_CLASSIFICATION=true to override.`);
      return 0;
    }

    const n = _classifyOrders(clientId);
    console.log(`[PostSync] Step 1: Classified ${n} orders`);
    return n;
  });

  // Step 2: Compute derived_product_role (fallback guard)
  const rolesSet = safeStep('Step 2 (product_role)', () => {
    const eligible = queryOneSql(
      'SELECT COUNT(*) as cnt FROM orders WHERE client_id = ? AND derived_product_role IS NULL AND product_type_classified IS NOT NULL', [clientId]
    )?.cnt || 0;

    if (eligible === 0) {
      console.log(`[PostSync] Step 2: 0 eligible rows (staging pre-computed)`);
      return 0;
    }
    if (eligible >= HEAVY_THRESHOLD && !allowHeavy) {
      console.error(`[PostSync] SKIPPED Step 2: ${eligible} rows too large for main process.`);
      return 0;
    }

    const n = _computeDerivedProductRole(clientId);
    console.log(`[PostSync] Step 2: Set derived_product_role on ${n} orders`);
    return n;
  });

  // Step 2b: Parse cascade chains from system_notes (fallback guard)
  safeStep('Step 2b (cascade chains)', () => {
    const eligible = queryOneSql(
      "SELECT COUNT(*) as cnt FROM orders WHERE client_id = ? AND is_cascaded = 1 AND (cascade_chain IS NULL OR cascade_chain = '')", [clientId]
    )?.cnt || 0;

    if (eligible === 0) {
      console.log(`[PostSync] Step 2b: 0 eligible cascades (staging pre-computed)`);
      return;
    }

    const { parseCascadeChains } = require('./post-sync-vct');
    const n = parseCascadeChains(clientId);
    if (n > 0) console.log(`[PostSync] Step 2b: Parsed ${n} cascade chains`);
  });

  // Step 3: Compute processing_gateway_id
  safeStep('Step 3 (processing_gateway)', () => {
    runSql(`UPDATE orders SET processing_gateway_id =
      CASE
        WHEN processing_gateway_id IS NOT NULL AND processing_gateway_id != gateway_id THEN processing_gateway_id
        WHEN is_cascaded = 1 AND original_gateway_id IS NOT NULL THEN original_gateway_id
        ELSE gateway_id
      END
      WHERE client_id = ?`, [clientId]);
    saveDb();
    console.log(`[PostSync] Step 3: Updated processing_gateway_id`);
  });

  // Step 4: Compute derived_cycle + derived_attempt
  const cyclesSet = safeStep('Step 4 (cycle/attempt)', () => {
    const n = _computeCycleAndAttempt(clientId);
    console.log(`[PostSync] Step 4: Computed cycle/attempt for ${n} orders`);
    return n;
  });

  // Step 4b: Promote rebill attempt >= 2 to salvage roles
  safeStep('Step 4b (salvage roles)', () => {
    const n = runSql(`UPDATE orders SET derived_product_role =
      CASE derived_product_role
        WHEN 'main_rebill' THEN 'main_rebill_salvage'
        WHEN 'upsell_rebill' THEN 'upsell_rebill_salvage'
      END
      WHERE client_id = ? AND derived_attempt >= 2
        AND derived_product_role IN ('main_rebill', 'upsell_rebill')`,
      [clientId]);
    saveDb();
    const changed = n?.changes || 0;
    if (changed > 0) console.log(`[PostSync] Step 4b: Promoted ${changed} rebill orders to salvage`);
  });

  // Step 5: Extract transaction features for AI training
  const featuresExtracted = safeStep('Step 5 (tx features)', () => {
    const { extractFeatures } = require('../analytics/feature-extraction');
    const n = extractFeatures(clientId);
    console.log(`[PostSync] Step 5: Extracted ${n} tx features`);
    return n;
  });

  // Step 6a: Fetch employeeNotes for orders with pending shadow decisions
  let notesFetched = 0;
  try {
    notesFetched = await _fetchEmployeeNotesForShadow(clientId);
    console.log(`[PostSync] Step 6a: Fetched employeeNotes for ${notesFetched} shadow orders`);
  } catch (err) {
    console.error(`[PostSync] Step 6a: employeeNotes fetch failed — ${err.message}`);
    errors.push('Step 6a (employeeNotes)');
  }

  // Step 6b: Reconcile shadow decisions against imported orders
  const reconciled = safeStep('Step 6b (reconcile)', () => {
    const r = reconcileShadowDecisions(clientId);
    console.log(`[PostSync] Step 6b: Reconciled ${r.matched} shadow rows (${r.scanned} orders scanned)`);
    return r;
  });

  // Step 7: Run Layer 1 shadow alerts
  let shadowAlerts = [];
  shadowAlerts = safeStep('Step 7 (shadow alerts)', () => {
    const { runShadowAlertCheck } = require('../analytics/shadow-alert-runner');
    const alerts = runShadowAlertCheck(clientId);
    console.log(`[PostSync] Step 7: Shadow alerts — ${alerts.length} triggered`);
    return alerts;
  }) || [];

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  if (errors.length > 0) {
    console.error(`[PostSync] Pipeline finished in ${elapsed}s with ${errors.length} errors: ${errors.join(', ')}`);
  } else {
    console.log(`[PostSync] Pipeline complete in ${elapsed}s`);
  }

  saveDb();
  return { classified, rolesSet, cyclesSet, featuresExtracted, reconciled, shadowAlerts, errors };
}

// ---------------------------------------------------------------------------
// Step 1: Classify orders (tx_type, product_group, product_type_classified)
// ---------------------------------------------------------------------------
function _classifyOrders(clientId) {
  // Build product lookup
  const pgaRows = querySql(`
    SELECT pga.product_id, pga.product_type, pga.product_group_id, pg.group_name
    FROM product_group_assignments pga
    JOIN product_groups pg ON pga.product_group_id = pg.id
    WHERE pga.client_id = ?
  `, [clientId]);
  const productMap = {};
  for (const r of pgaRows) productMap[String(r.product_id)] = r;

  // Load orders that need classification (NULL product_type_classified)
  const orders = querySql(`
    SELECT id, order_id, customer_id, is_test, billing_cycle, is_recurring,
           retry_attempt, is_cascaded, product_ids, order_status, acquisition_date
    FROM orders WHERE client_id = ? AND product_type_classified IS NULL
    ORDER BY order_id
  `, [clientId]);

  if (orders.length === 0) return 0;

  // Build customer index for upsell/salvage detection
  // Need ALL customer orders (not just unclassified) for context
  const allOrders = querySql(`
    SELECT id, order_id, customer_id, product_ids, order_status, acquisition_date,
           billing_cycle, is_test, product_group_id
    FROM orders WHERE client_id = ? AND customer_id IS NOT NULL
    ORDER BY order_id
  `, [clientId]);

  const custIdx = {};
  for (const o of allOrders) {
    if (!custIdx[o.customer_id]) custIdx[o.customer_id] = [];
    let pid = null;
    try { const ids = JSON.parse(o.product_ids); if (Array.isArray(ids) && ids.length > 0) pid = String(ids[0]); } catch {}
    const pi = pid ? productMap[pid] : null;
    custIdx[o.customer_id].push({
      ...o,
      _pid: pid,
      _pi: pi,
      _date: o.acquisition_date ? o.acquisition_date.split(' ')[0] : null,
      _billingCycle: parseInt(o.billing_cycle) || 0,
      _isApproved: [2, 6, 8].includes(parseInt(o.order_status)),
      _isDeclined: parseInt(o.order_status) === 7,
    });
  }

  let count = 0;
  for (const o of orders) {
    let pid = null;
    try { const ids = JSON.parse(o.product_ids); if (Array.isArray(ids) && ids.length > 0) pid = String(ids[0]); } catch {}
    const pi = pid ? productMap[pid] : null;
    const billingCycle = parseInt(o.billing_cycle) || 0;
    const isRecurring = o.is_recurring === 1 || o.is_recurring === '1';
    const retryAttempt = parseInt(o.retry_attempt) || 0;
    const oDate = o.acquisition_date ? o.acquisition_date.split(' ')[0] : null;

    let tx_type = null;
    let derived_cycle = null;
    let product_group_id = pi ? pi.product_group_id : null;
    let product_group_name = pi ? pi.group_name : null;
    let product_type_classified = pi ? pi.product_type : null;

    // Anonymous decline
    if (o.customer_id === null || o.customer_id === 0) {
      tx_type = 'anonymous_decline';
    }
    // Test
    else if (o.is_test === 1) {
      tx_type = 'test_order';
    }
    // No product group
    else if (!pi) {
      tx_type = 'unclassified';
    }
    // Straight sale
    else if (pi.product_type === 'straight_sale') {
      tx_type = 'straight_sale';
    }
    // COF rebill
    else if (billingCycle > 0 && isRecurring) {
      tx_type = 'sticky_cof_rebill';
      derived_cycle = billingCycle;
    }
    // TP rebill
    else if (billingCycle > 0 && !isRecurring) {
      tx_type = 'tp_rebill';
      derived_cycle = billingCycle;
    }
    // billing_cycle = 0
    else {
      let matched = false;

      // Upsell detection
      if (o.customer_id && oDate && pi.product_type !== 'rebill') {
        const custOrders = custIdx[o.customer_id] || [];
        const sameDay = custOrders.filter(co =>
          co._date === oDate && co._billingCycle === 0 && co.is_test !== 1
        ).sort((a, b) => a.order_id - b.order_id);

        if (sameDay.length > 1) {
          const anchor = sameDay[0];
          if (o.order_id !== anchor.order_id && pid !== anchor._pid) {
            tx_type = 'upsell';
            matched = true;
          }
        }
      }

      if (!matched) {
        if (isRecurring && (pi.product_type === 'initial_rebill' || pi.product_type === 'rebill')) {
          const custOrders = custIdx[o.customer_id] || [];
          const priorApproved = custOrders.some(co =>
            co.order_id < o.order_id && co._pi && co._pi.product_group_id === pi.product_group_id && co._isApproved
          );
          tx_type = priorApproved ? 'sticky_cof_rebill' : 'cp_initial';
          matched = true;
        } else if (!isRecurring && pi.product_type === 'rebill') {
          tx_type = 'tp_rebill';
          matched = true;
        } else if (retryAttempt === 0 && !isRecurring &&
                   (pi.product_type === 'initial' || pi.product_type === 'initial_rebill')) {
          const custOrders = custIdx[o.customer_id] || [];
          const priorDeclined = custOrders.some(co =>
            co.order_id < o.order_id && co._pi && co._pi.product_group_id === pi.product_group_id && co._isDeclined
          );
          tx_type = priorDeclined ? 'initial_salvage' : 'cp_initial';
          matched = true;
        } else if (retryAttempt > 0) {
          tx_type = billingCycle === 0 ? 'cp_initial_retry' : 'tp_rebill_salvage';
          matched = true;
        }
      }

      if (!tx_type) tx_type = 'cp_initial';
    }

    runSql(`UPDATE orders SET
      tx_type = ?, derived_cycle = ?, product_group_id = ?,
      product_group_name = ?, product_type_classified = ?
      WHERE id = ?`,
      [tx_type, derived_cycle, product_group_id, product_group_name,
       product_type_classified, o.id]);
    count++;
  }

  saveDb();
  return count;
}

// ---------------------------------------------------------------------------
// Step 2: Compute derived_product_role
// ---------------------------------------------------------------------------
function _computeDerivedProductRole(clientId) {
  // Load orders missing derived_product_role
  const orders = querySql(`
    SELECT id, product_ids, product_type_classified
    FROM orders
    WHERE client_id = ? AND derived_product_role IS NULL AND product_type_classified IS NOT NULL
  `, [clientId]);

  if (orders.length === 0) return 0;

  // Build product_id → product_sequence lookup
  const seqRows = querySql(`
    SELECT pga.product_id, pg.product_sequence
    FROM product_group_assignments pga
    JOIN product_groups pg ON pg.id = pga.product_group_id AND pg.client_id = pga.client_id
    WHERE pga.client_id = ? AND pg.product_sequence IS NOT NULL
  `, [clientId]);
  const seqMap = {};
  for (const r of seqRows) seqMap[String(r.product_id)] = r.product_sequence;

  for (const o of orders) {
    // Parse product_ids JSON — use first product
    let pid = null;
    try {
      const ids = JSON.parse(o.product_ids);
      if (Array.isArray(ids) && ids.length > 0) pid = String(ids[0]);
    } catch {}

    const seq = pid ? seqMap[pid] : null; // 'main', 'upsell', or null
    const ptype = o.product_type_classified;
    let role = null;

    if (ptype === 'initial') {
      role = seq === 'upsell' ? 'upsell_initial' : 'main_initial';
    } else if (ptype === 'rebill') {
      role = seq === 'upsell' ? 'upsell_rebill' : 'main_rebill';
    } else if (ptype === 'initial_rebill' || ptype === 'straight_sale') {
      role = 'straight_sale';
    }

    if (role) {
      runSql('UPDATE orders SET derived_product_role = ? WHERE id = ?', [role, o.id]);
    }
  }

  saveDb();
  return orders.length;
}

// ---------------------------------------------------------------------------
// Step 4: Compute derived_cycle + derived_attempt
// ---------------------------------------------------------------------------
// Logic ported from scripts/recalc-derived-fields.js (canonical implementation):
//   - initial/initial_rebill → cycle 0, attempt increments, resets after approval
//   - rebill → cycle starts at 1, increments after each approved rebill
//   - straight_sale → cycle 0, attempt 1
//   - unknown product type → NULL/NULL
function _computeCycleAndAttempt(clientId) {
  const { getDb } = require('../db/connection');
  const db = getDb();
  const BATCH_SIZE = 500;
  const HEAP_LIMIT_MB = 400;

  // Also handle NULL customer/product_group orders
  const nullCount = queryOneSql(`
    SELECT COUNT(*) as cnt FROM orders
    WHERE client_id = ? AND is_test = 0 AND is_internal_test = 0
      AND (customer_id IS NULL OR product_group_id IS NULL)
      AND (derived_cycle IS NOT NULL OR derived_attempt IS NOT NULL)
  `, [clientId])?.cnt || 0;

  if (nullCount > 0) {
    runSql(`UPDATE orders SET derived_cycle = NULL, derived_attempt = NULL
      WHERE client_id = ? AND is_test = 0 AND is_internal_test = 0
        AND (customer_id IS NULL OR product_group_id IS NULL)`, [clientId]);
  }

  const updateStmt = db.prepare('UPDATE orders SET derived_cycle = ?, derived_attempt = ? WHERE id = ?');
  const selectStmt = db.prepare(`
    SELECT id, order_id, product_type_classified, order_status, acquisition_date, is_cascaded
    FROM orders
    WHERE client_id = ? AND customer_id = ? AND product_group_id = ?
      AND is_test = 0 AND is_internal_test = 0
    ORDER BY acquisition_date ASC, order_id ASC
  `);

  // Keyset batching: fetch groups in pages using LIMIT/OFFSET on a sorted query
  const groupCountStmt = db.prepare(`
    SELECT COUNT(*) as cnt FROM (
      SELECT DISTINCT customer_id, product_group_id FROM orders
      WHERE client_id = ? AND is_test = 0 AND is_internal_test = 0
        AND customer_id IS NOT NULL AND product_group_id IS NOT NULL
    )
  `);
  const totalGroups = groupCountStmt.get(clientId).cnt;

  const groupPageStmt = db.prepare(`
    SELECT DISTINCT customer_id, product_group_id FROM orders
    WHERE client_id = ? AND is_test = 0 AND is_internal_test = 0
      AND customer_id IS NOT NULL AND product_group_id IS NOT NULL
    ORDER BY customer_id, product_group_id
    LIMIT ? OFFSET ?
  `);

  let totalUpdated = nullCount;
  let batchNum = 0;
  let offset = 0;
  let memoryExceeded = false;

  while (offset < totalGroups) {
    // Memory guard
    const mem = process.memoryUsage();
    const heapMB = Math.round(mem.heapUsed / 1024 / 1024);
    batchNum++;

    if (batchNum % 10 === 0 || batchNum === 1) {
      console.log(`[PostSync] Step 4 batch ${batchNum}: ${heapMB}MB heap, ${totalUpdated} orders processed, ${offset}/${totalGroups} groups`);
    }

    if (heapMB > HEAP_LIMIT_MB) {
      console.error(`[PostSync] Step 4 STOPPED: heap ${heapMB}MB exceeds ${HEAP_LIMIT_MB}MB limit. Processed ${totalUpdated} orders (${offset}/${totalGroups} groups). Remaining groups will be processed next sync.`);
      memoryExceeded = true;
      break;
    }

    // Fetch next batch of groups
    const groups = groupPageStmt.all(clientId, BATCH_SIZE, offset);
    if (groups.length === 0) break;
    offset += groups.length;

    const updates = [];

    for (const { customer_id, product_group_id } of groups) {
      const group = selectStmt.all(clientId, customer_id, product_group_id);

      let currentCycle = 0;
      let attemptInCycle = 0;
      let initialApproved = false;

      for (const row of group) {
        const ptype = row.product_type_classified;
        const isApproved = [2, 6, 8].includes(row.order_status);
        let derivedCycle = null;
        let derivedAttempt = null;

        if (ptype === 'straight_sale') {
          derivedCycle = 0;
          derivedAttempt = 1;
        } else if (ptype === 'initial' || ptype === 'initial_rebill') {
          derivedCycle = 0;
          attemptInCycle++;
          derivedAttempt = attemptInCycle;

          if (isApproved) {
            initialApproved = true;
            currentCycle = 1;
            attemptInCycle = 0;
          }
        } else if (ptype === 'rebill') {
          derivedCycle = initialApproved ? currentCycle : (currentCycle || 1);
          attemptInCycle++;
          derivedAttempt = attemptInCycle;

          if (isApproved) {
            currentCycle++;
            attemptInCycle = 0;
          }
        }

        updates.push([derivedCycle, derivedAttempt, row.id]);
      }
    }

    // Flush this batch in a transaction
    const runBatch = db.transaction((rows) => {
      for (const [cycle, attempt, id] of rows) {
        updateStmt.run(cycle, attempt, id);
      }
    });
    runBatch(updates);
    totalUpdated += updates.length;
  }

  if (!memoryExceeded) {
    const finalMem = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
    console.log(`[PostSync] Step 4 complete: ${totalGroups} groups, ${totalUpdated} orders, peak ${finalMem}MB heap`);
  }

  saveDb();
  return totalUpdated;
}

// ---------------------------------------------------------------------------
// Step 6a: Fetch employeeNotes for orders with pending shadow decisions
// ---------------------------------------------------------------------------
//
// order_find (bulk import) does NOT return employeeNotes — only the single
// order_view endpoint does. Beast Insights and BinRoute both write markers
// to employeeNotes via the checkout framework's customNotes field.
//
// This step does targeted order_view calls for orders that were created in
// the same time window as unreconciled shadow decisions. Typically ~20-30
// calls/day for Stage 0 Kytsan traffic.
//
// Idempotent: skips orders that already have employee_notes populated.

async function _fetchEmployeeNotesForShadow(clientId) {
  // employee_notes are already populated by the bulk order_find import.
  // Just count how many orders have BinRouting markers ready for reconciliation.
  const pending = querySql(
    `SELECT COUNT(*) as n FROM shadow_decisions WHERE client_id = ? AND reconciled_at IS NULL`,
    [clientId]
  );
  if (pending[0].n === 0) return 0;

  const withMarkers = querySql(
    `SELECT COUNT(*) as n FROM orders
      WHERE client_id = ?
        AND (employee_notes LIKE '%BinRoute_shadow%' OR employee_notes LIKE '%BinRouting:%')`,
    [clientId]
  );

  return withMarkers[0].n;
}

// ---------------------------------------------------------------------------
// Step 6b: Reconcile shadow decisions against imported orders
// ---------------------------------------------------------------------------
//
// Flow:
//   - Shadow decisions are logged by /api/route at checkout time with a UUID.
//   - The PHP extension appends "BinRoute_shadow: id=<uuid> ..." to the order's
//     customNotes before submission.
//   - Sticky.io surfaces customNotes back on the imported order. The exact
//     column depends on how Sticky routes it — we scan all three candidates
//     (employee_notes, system_notes, custom_fields) for defensiveness.
//   - For each order carrying a shadow marker that hasn't been reconciled yet,
//     we extract the UUID, look up the shadow_decisions row, and fill in the
//     actual_* columns + would_match + reconciled_at.
//
// Idempotent: rows where reconciled_at IS NOT NULL are skipped.
// Returns: { scanned, matched, skipped_reconciled, skipped_orphan }.
const SHADOW_MARKER_RE = /BinRout(?:e_shadow|ing):\s*id=([a-fA-F0-9-]{8,})/;
const CLIENT_LAT_RE = /BinRout(?:e_shadow|ing):.*lat=(\d+)/;

function reconcileShadowDecisions(clientId) {
  // Fast path: any rows to scan?
  const pendingCount = queryOneSql(
    `SELECT COUNT(*) AS n FROM shadow_decisions
      WHERE client_id = ? AND reconciled_at IS NULL`,
    [clientId]
  );
  if (!pendingCount || pendingCount.n === 0) {
    return { scanned: 0, matched: 0, skipped_reconciled: 0, skipped_orphan: 0 };
  }

  // Pull orders for this client that carry the marker in ANY of the 3 notes fields.
  // We scope to the client to avoid scanning the whole orders table.
  const orders = querySql(
    `SELECT o.id               AS order_id,
            o.order_id         AS sticky_order_id,
            o.processing_gateway_id,
            o.gateway_id,
            o.cascade_chain,
            o.is_cascaded,
            o.order_status,
            o.employee_notes,
            o.system_notes,
            o.custom_fields,
            g.processor_name   AS processing_processor
       FROM orders o
  LEFT JOIN gateways g
         ON g.client_id = o.client_id
        AND g.gateway_id = COALESCE(o.processing_gateway_id, o.gateway_id)
      WHERE o.client_id = ?
        AND (
          (o.employee_notes IS NOT NULL AND (o.employee_notes LIKE '%BinRoute_shadow%' OR o.employee_notes LIKE '%BinRouting:%')) OR
          (o.system_notes   IS NOT NULL AND (o.system_notes   LIKE '%BinRoute_shadow%' OR o.system_notes   LIKE '%BinRouting:%')) OR
          (o.custom_fields  IS NOT NULL AND (o.custom_fields  LIKE '%BinRoute_shadow%' OR o.custom_fields  LIKE '%BinRouting:%'))
        )`,
    [clientId]
  );

  let matched = 0;
  let skippedReconciled = 0;
  let skippedOrphan = 0;
  const scanned = orders.length;

  for (const o of orders) {
    // Extract shadow_id from whichever notes field contains it.
    const shadowId = _extractShadowId(o.employee_notes)
                  || _extractShadowId(o.system_notes)
                  || _extractShadowId(o.custom_fields);
    if (!shadowId) continue;

    // Look up the shadow decision. Scope by client to prevent cross-client
    // matches even if a UUID somehow collided.
    const shadow = queryOneSql(
      `SELECT shadow_id, recommended_gateway_id, reconciled_at
         FROM shadow_decisions
        WHERE shadow_id = ? AND client_id = ?`,
      [shadowId, clientId]
    );
    if (!shadow) {
      // Orphan: order carries a marker but the shadow row is gone / came from
      // another env / was manually deleted. Skip silently; do not error.
      skippedOrphan++;
      continue;
    }
    if (shadow.reconciled_at != null) {
      // Already reconciled — idempotent skip.
      skippedReconciled++;
      continue;
    }

    // --- Determine the FIRST-ATTEMPT gateway ---
    // Source of truth: cascade_chain[0] (parsed from system_notes).
    // Verification: Beast marker gateway_id= in employee_notes.
    // Fallback: processing_gateway_id, then gateway_id.
    let firstAttemptGw = null;

    // 1. cascade_chain[0] — most reliable
    if (o.cascade_chain && o.cascade_chain.length > 0) {
      firstAttemptGw = parseInt(o.cascade_chain.split(',')[0], 10);
    }

    // 2. Verify against Beast marker if present
    const beastGwMatch = o.employee_notes
      ? o.employee_notes.match(/BeastInsights:\s*gateway_id=(\d+)/)
      : null;
    const beastGw = beastGwMatch ? parseInt(beastGwMatch[1], 10) : null;

    if (beastGw && firstAttemptGw && beastGw !== firstAttemptGw) {
      // Beast marker disagrees with cascade chain — Beast marker is authoritative
      // (cascade chain order from system_notes can be misleading)
      console.warn(`[Reconciler] Order ${o.sticky_order_id}: Beast marker gw=${beastGw} overrides cascade_chain[0]=${firstAttemptGw}`);
      firstAttemptGw = beastGw;
    }

    // 3. Fallback chain: Beast marker (if present) → cascade_chain[0] → processing_gateway_id → gateway_id
    if (!firstAttemptGw) firstAttemptGw = beastGw;
    if (!firstAttemptGw) firstAttemptGw = o.processing_gateway_id;
    if (!firstAttemptGw) firstAttemptGw = o.gateway_id;

    // Look up processor name for the first-attempt gateway
    const firstGwRow = queryOneSql(
      'SELECT processor_name FROM gateways WHERE client_id = ? AND gateway_id = ?',
      [clientId, firstAttemptGw]
    );
    const firstAttemptProc = firstGwRow ? firstGwRow.processor_name : null;

    // --- Outcome: first-attempt approval only ---
    // Cascaded orders = first attempt DECLINED (regardless of final order_status).
    // Non-cascaded: use order_status directly.
    const status = parseInt(o.order_status, 10);
    let outcome;
    let outcomeBinary;
    if (o.is_cascaded === 1 || o.is_cascaded === '1') {
      // Cascaded = first attempt declined, cascade tried other gateways
      outcome = 'declined';
      outcomeBinary = 0;
    } else if (status === 2 || status === 6 || status === 8) {
      outcome = 'approved';
      outcomeBinary = 1;
    } else if (status === 7) {
      outcome = 'declined';
      outcomeBinary = 0;
    } else {
      outcome = 'pending';
      outcomeBinary = null;
    }

    // --- would_match: compare our pick vs the first-attempt gateway ---
    const wouldMatch = (shadow.recommended_gateway_id != null
                    && shadow.recommended_gateway_id === firstAttemptGw) ? 1 : 0;

    // Extract client-side latency from the marker (lat=<ms>)
    const clientLat = _extractClientLatency(o.employee_notes);

    runSql(
      `UPDATE shadow_decisions
          SET actual_order_id        = ?,
              actual_sticky_order_id = ?,
              actual_gateway_id      = ?,
              actual_processor       = ?,
              actual_outcome         = ?,
              actual_outcome_binary  = ?,
              would_match            = ?,
              latency_ms_client      = ?,
              reconciled_at          = CURRENT_TIMESTAMP
        WHERE shadow_id = ? AND reconciled_at IS NULL`,
      [
        o.order_id,
        o.sticky_order_id,
        firstAttemptGw,
        firstAttemptProc,
        outcome,
        outcomeBinary,
        wouldMatch,
        clientLat,
        shadowId,
      ]
    );
    matched++;
  }

  saveDb();
  return { scanned, matched, skipped_reconciled: skippedReconciled, skipped_orphan: skippedOrphan };
}

/** Extract client-side latency (ms) from marker, or null. */
function _extractClientLatency(notesField) {
  if (notesField == null) return null;
  const s = typeof notesField === 'string' ? notesField : String(notesField);
  const m = s.match(CLIENT_LAT_RE);
  return m ? parseInt(m[1], 10) : null;
}

/** Extract the shadow UUID from a notes-field value, or null. */
function _extractShadowId(notesField) {
  if (notesField == null) return null;
  const s = typeof notesField === 'string' ? notesField : String(notesField);
  const m = s.match(SHADOW_MARKER_RE);
  return m ? m[1] : null;
}

module.exports = { runPostSyncPipeline, reconcileShadowDecisions };
