/**
 * Post-sync pipeline — runs after order import to prepare data for analytics.
 *
 * Steps (in order):
 *   1. Classify orders (product group, tx_type, product_type_classified)
 *   2. Compute derived_product_role
 *   3. Compute processing_gateway_id
 *   4. Compute derived_cycle + derived_attempt
 *
 * Uses the server's in-memory DB connection (querySql/runSql).
 * Must complete BEFORE analytics recompute.
 */
const { querySql, runSql, saveDb } = require('../db/connection');

/**
 * Run the full post-sync pipeline for a client.
 * @param {number} clientId
 * @returns {{ classified: number, rolesSet: number, cyclesSet: number }}
 */
function runPostSyncPipeline(clientId) {
  console.log(`[PostSync] Starting pipeline for client ${clientId}...`);
  const start = Date.now();

  // Step 1: Classify new orders
  const classified = _classifyOrders(clientId);
  console.log(`[PostSync] Step 1: Classified ${classified} orders`);

  // Step 2: Compute derived_product_role for orders missing it
  const rolesSet = _computeDerivedProductRole(clientId);
  console.log(`[PostSync] Step 2: Set derived_product_role on ${rolesSet} orders`);

  // Step 3: Compute processing_gateway_id (all orders — simple SQL)
  runSql(`UPDATE orders SET processing_gateway_id =
    CASE
      WHEN is_cascaded = 1 AND original_gateway_id IS NOT NULL THEN original_gateway_id
      ELSE gateway_id
    END
    WHERE client_id = ?`, [clientId]);
  saveDb();
  console.log(`[PostSync] Step 3: Updated processing_gateway_id`);

  // Step 4: Compute derived_cycle + derived_attempt
  const cyclesSet = _computeCycleAndAttempt(clientId);
  console.log(`[PostSync] Step 4: Computed cycle/attempt for ${cyclesSet} orders`);

  // Step 5: Extract transaction features for AI training
  const { extractFeatures } = require('../analytics/feature-extraction');
  const featuresExtracted = extractFeatures(clientId);
  console.log(`[PostSync] Step 5: Extracted ${featuresExtracted} tx features`);

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`[PostSync] Pipeline complete in ${elapsed}s`);

  saveDb();
  return { classified, rolesSet, cyclesSet, featuresExtracted };
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
  const orders = querySql(`
    SELECT id, order_id, customer_id, product_group_id, product_type_classified,
           order_status, acquisition_date, is_cascaded
    FROM orders
    WHERE client_id = ? AND is_test = 0 AND is_internal_test = 0
    ORDER BY customer_id, product_group_id, acquisition_date ASC, order_id ASC
  `, [clientId]);

  if (orders.length === 0) return 0;

  // Pre-compile the update statement and batch inside a transaction
  const db = getDb();
  const updateStmt = db.prepare('UPDATE orders SET derived_cycle = ?, derived_attempt = ? WHERE id = ?');
  const BATCH_SIZE = 5000;
  const updates = []; // collect [cycle, attempt, id] tuples

  let i = 0;

  while (i < orders.length) {
    const o = orders[i];

    // Orders without customer or product group → NULL
    if (!o.customer_id || !o.product_group_id) {
      updates.push([null, null, o.id]);
      i++;
      continue;
    }

    // Collect all orders for this customer+product_group journey
    const custId = o.customer_id;
    const pgId = o.product_group_id;
    const group = [];
    while (i < orders.length &&
           orders[i].customer_id === custId &&
           orders[i].product_group_id === pgId) {
      group.push(orders[i]);
      i++;
    }

    // Process journey
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

  // Flush in batched transactions for performance
  for (let b = 0; b < updates.length; b += BATCH_SIZE) {
    const batch = updates.slice(b, b + BATCH_SIZE);
    const runBatch = db.transaction((rows) => {
      for (const [cycle, attempt, id] of rows) {
        updateStmt.run(cycle, attempt, id);
      }
    });
    runBatch(batch);
  }

  saveDb();
  return updates.length;
}

module.exports = { runPostSyncPipeline };
