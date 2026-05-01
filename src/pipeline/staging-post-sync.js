/**
 * staging-post-sync.js — runs classification steps inside the import worker
 * on the isolated staging DB, BEFORE merge into main.
 *
 * Steps handled here (per-order, no full history needed):
 *   1. Classify orders (product_type_classified, tx_type, product_group_id)
 *   2. Compute derived_product_role
 *   2b. Parse cascade chains from system_notes
 *   3. Compute processing_gateway_id
 *
 * For client context (product_groups, product_group_assignments), ATTACHes
 * the main DB as read-only alias "maindb". No writes to main DB ever happen.
 *
 * Steps that need full order history (cycle/attempt, reconciliation, alerts)
 * stay in the main process and run AFTER merge.
 */

const Database = require('better-sqlite3');
const path = require('path');

const MAIN_DB_PATH = path.join(__dirname, '..', '..', 'data', 'binroute.db');

// ── VCT classification constants (client 6) ──────────────────────────────

const UPSELL_CAMPS = new Set([
  29, 73, 41, 64, 77, 13, 86, 88, 100, 101, 102, 98, 79, 82, 84,
  75, 90, 92, 95, 97, 104, 106, 108, 122, 136, 138, 140, 142, 144, 146,
  148, 152, 154, 156, 158, 162, 164, 166, 168, 170, 172, 174, 176,
]);

const UPSELL_PRODUCT_IDS = new Set([
  568,2808,2917,7062,9411,13035,13459,14879,14992,17933,18336,22595,23510,24180,
  24241,24242,24561,24562,24563,24564,24565,24566,24571,24572,24573,25161,25162,
  28332,28335,28354,28355,28361,28380,28383,28384,28409,28715,28740,28879,29218,
  30881,30883,31830,31856,32018,32019,32517,32682,32992,33402,33782,34015,34016,
  34042,34047,34048,34106,34358,34433,34562,34570,34787,34792,34938,34992,35161,
  35695,36161,40115,42624,43020,
]);

const DECLINE_SALVAGE_CAMPS = new Set([37, 49, 7, 12]);
const MEMBERSHIP_CAMPS = new Set([3, 11, 60, 4]);

// ── Main entry point ─────────────────────────────────────────────────────

/**
 * Run classification steps on a staging DB before merge.
 * @param {object} stagingHelpers - from createDbHelpers(stagingPath)
 * @param {number} clientId
 * @returns {{ success, fallbackRequired, classified, rolesSet, cascadesParsed, gatewaysSet, errors, errorMessage }}
 */
function runStagingPostSync(stagingHelpers, clientId) {
  const db = stagingHelpers.db;
  const result = {
    success: false,
    fallbackRequired: true,
    classified: 0,
    rolesSet: 0,
    cascadesParsed: 0,
    gatewaysSet: 0,
    errors: [],
    errorMessage: null,
  };

  console.log(`[StagingPostSync] Starting for client ${clientId}...`);
  const startTime = Date.now();

  // ATTACH main DB read-only for product lookups + classified order context
  let mainAttached = false;
  try {
    db.exec(`ATTACH DATABASE '${MAIN_DB_PATH.replace(/'/g, "''")}' AS maindb`);
    mainAttached = true;
  } catch (err) {
    console.error(`[StagingPostSync] Could not attach main DB: ${err.message}`);
    result.errors.push('attach_maindb');
    result.errorMessage = `ATTACH failed: ${err.message}`;
    return result;
  }

  try {
    if (clientId === 6) {
      _runVctSteps(db, stagingHelpers, clientId, result);
    } else {
      _runKpSteps(db, stagingHelpers, clientId, result);
    }

    // Step 3: processing_gateway_id (all clients, self-contained)
    _safeStep('Step 3 (processing_gateway)', result, () => {
      const info = stagingHelpers.runSql(`UPDATE orders SET processing_gateway_id =
        CASE
          WHEN processing_gateway_id IS NOT NULL AND processing_gateway_id != gateway_id THEN processing_gateway_id
          WHEN is_cascaded = 1 AND original_gateway_id IS NOT NULL THEN original_gateway_id
          ELSE gateway_id
        END
        WHERE client_id = ?`, [clientId]);
      result.gatewaysSet = info.changes;
      console.log(`[StagingPostSync] Step 3: Updated processing_gateway_id on ${info.changes} orders`);
    });

    stagingHelpers.saveDb();

    // Determine overall success
    result.success = result.errors.length === 0;
    result.fallbackRequired = result.errors.length > 0;

  } finally {
    if (mainAttached) {
      try { db.exec('DETACH DATABASE maindb'); } catch {}
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  if (result.errors.length > 0) {
    console.error(`[StagingPostSync] Done in ${elapsed}s with ${result.errors.length} errors: ${result.errors.join(', ')}`);
  } else {
    console.log(`[StagingPostSync] Done in ${elapsed}s — classified=${result.classified} roles=${result.rolesSet} cascades=${result.cascadesParsed} gateways=${result.gatewaysSet}`);
  }

  return result;
}

// ── Safe step wrapper ────────────────────────────────────────────────────

function _safeStep(name, result, fn) {
  try {
    return fn();
  } catch (err) {
    console.error(`[StagingPostSync] ${name} FAILED: ${err.message}`);
    result.errors.push(name);
    return null;
  }
}

// ── KP classification (clients 1, 2) ─────────────────────────────────────

function _runKpSteps(db, h, clientId, result) {
  // Step 1: Classify orders
  _safeStep('Step 1 (classify)', result, () => {
    // Product lookup from main DB (client-scoped)
    const pgaRows = db.prepare(`
      SELECT pga.product_id, pga.product_type, pga.product_group_id, pg.group_name
      FROM maindb.product_group_assignments pga
      JOIN maindb.product_groups pg ON pga.product_group_id = pg.id
      WHERE pga.client_id = ?
    `).all(clientId);
    const productMap = {};
    for (const r of pgaRows) productMap[String(r.product_id)] = r;

    // Unclassified orders in staging
    const orders = h.querySql(`
      SELECT id, order_id, customer_id, is_test, billing_cycle, is_recurring,
             retry_attempt, is_cascaded, product_ids, order_status, acquisition_date
      FROM orders WHERE client_id = ? AND product_type_classified IS NULL
      ORDER BY order_id
    `, [clientId]);

    if (orders.length === 0) {
      console.log(`[StagingPostSync] Step 1: 0 orders to classify`);
      return;
    }

    // Customer context: staging orders + main DB history (client-scoped, deduped)
    const allOrders = db.prepare(`
      SELECT id, order_id, customer_id, product_ids, order_status, acquisition_date,
             billing_cycle, is_test, product_group_id
      FROM orders WHERE client_id = ? AND customer_id IS NOT NULL
      UNION ALL
      SELECT id, order_id, customer_id, product_ids, order_status, acquisition_date,
             billing_cycle, is_test, product_group_id
      FROM maindb.orders WHERE client_id = ? AND customer_id IS NOT NULL
        AND order_id NOT IN (SELECT order_id FROM orders WHERE client_id = ?)
      ORDER BY order_id
    `).all(clientId, clientId, clientId);

    const custIdx = {};
    for (const o of allOrders) {
      if (!custIdx[o.customer_id]) custIdx[o.customer_id] = [];
      let pid = null;
      try { const ids = JSON.parse(o.product_ids); if (Array.isArray(ids) && ids.length > 0) pid = String(ids[0]); } catch {}
      const pi = pid ? productMap[pid] : null;
      custIdx[o.customer_id].push({
        ...o, _pid: pid, _pi: pi,
        _date: o.acquisition_date ? o.acquisition_date.split(' ')[0] : null,
        _billingCycle: parseInt(o.billing_cycle) || 0,
        _isApproved: [2, 6, 8].includes(parseInt(o.order_status)),
        _isDeclined: parseInt(o.order_status) === 7,
      });
    }

    const updateStmt = db.prepare(`UPDATE orders SET
      tx_type = ?, derived_cycle = ?, product_group_id = ?,
      product_group_name = ?, product_type_classified = ?
      WHERE id = ?`);

    let count = 0;
    db.transaction(() => {
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

        if (o.customer_id === null || o.customer_id === 0) {
          tx_type = 'anonymous_decline';
        } else if (o.is_test === 1) {
          tx_type = 'test_order';
        } else if (!pi) {
          tx_type = 'unclassified';
        } else if (pi.product_type === 'straight_sale') {
          tx_type = 'straight_sale';
        } else if (billingCycle > 0 && isRecurring) {
          tx_type = 'sticky_cof_rebill';
          derived_cycle = billingCycle;
        } else if (billingCycle > 0 && !isRecurring) {
          tx_type = 'tp_rebill';
          derived_cycle = billingCycle;
        } else {
          let matched = false;

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
            } else if (!isRecurring && pi.product_type === 'rebill') {
              tx_type = 'tp_rebill';
            } else if (retryAttempt === 0 && !isRecurring &&
                       (pi.product_type === 'initial' || pi.product_type === 'initial_rebill')) {
              const custOrders = custIdx[o.customer_id] || [];
              const priorDeclined = custOrders.some(co =>
                co.order_id < o.order_id && co._pi && co._pi.product_group_id === pi.product_group_id && co._isDeclined
              );
              tx_type = priorDeclined ? 'initial_salvage' : 'cp_initial';
            } else if (retryAttempt > 0) {
              tx_type = billingCycle === 0 ? 'cp_initial_retry' : 'tp_rebill_salvage';
            }
          }

          if (!tx_type) tx_type = 'cp_initial';
        }

        updateStmt.run(tx_type, derived_cycle, product_group_id, product_group_name,
          product_type_classified, o.id);
        count++;
      }
    })();

    result.classified = count;
    console.log(`[StagingPostSync] Step 1: Classified ${count} orders`);
  });

  // Step 2: derived_product_role
  _safeStep('Step 2 (product_role)', result, () => {
    const orders = h.querySql(`
      SELECT id, product_ids, product_type_classified
      FROM orders
      WHERE client_id = ? AND derived_product_role IS NULL AND product_type_classified IS NOT NULL
    `, [clientId]);

    if (orders.length === 0) {
      console.log(`[StagingPostSync] Step 2: 0 orders`);
      return;
    }

    // Product sequence lookup from main DB (client-scoped)
    const seqRows = db.prepare(`
      SELECT pga.product_id, pg.product_sequence
      FROM maindb.product_group_assignments pga
      JOIN maindb.product_groups pg ON pg.id = pga.product_group_id AND pg.client_id = pga.client_id
      WHERE pga.client_id = ? AND pg.product_sequence IS NOT NULL
    `).all(clientId);
    const seqMap = {};
    for (const r of seqRows) seqMap[String(r.product_id)] = r.product_sequence;

    const updateStmt = db.prepare('UPDATE orders SET derived_product_role = ? WHERE id = ?');
    db.transaction(() => {
      for (const o of orders) {
        let pid = null;
        try {
          const ids = JSON.parse(o.product_ids);
          if (Array.isArray(ids) && ids.length > 0) pid = String(ids[0]);
        } catch {}

        const seq = pid ? seqMap[pid] : null;
        const ptype = o.product_type_classified;
        let role = null;

        if (ptype === 'initial') {
          role = seq === 'upsell' ? 'upsell_initial' : 'main_initial';
        } else if (ptype === 'rebill') {
          role = seq === 'upsell' ? 'upsell_rebill' : 'main_rebill';
        } else if (ptype === 'initial_rebill' || ptype === 'straight_sale') {
          role = 'straight_sale';
        }

        if (role) updateStmt.run(role, o.id);
      }
    })();

    result.rolesSet = orders.length;
    console.log(`[StagingPostSync] Step 2: Set derived_product_role on ${orders.length} orders`);
  });

  // Step 2b: Parse cascade chains (self-contained)
  _parseCascadeChains(db, h, clientId, result);
}

// ── VCT classification (client 6) ────────────────────────────────────────

function _runVctSteps(db, h, clientId, result) {
  _safeStep('Step 1 (VCT classify)', result, () => {
    const unclassified = h.querySql(`
      SELECT order_id, customer_id, order_total, billing_cycle, retry_attempt,
             campaign_id, main_product_id, is_test, COALESCE(is_internal_test, 0) as is_internal_test,
             COALESCE(is_test_cc, 0) as is_test_cc
      FROM orders WHERE client_id = ? AND derived_product_role IS NULL
      ORDER BY order_id
    `, [clientId]);

    if (unclassified.length === 0) {
      console.log(`[StagingPostSync] Step 1: 0 VCT orders to classify`);
      return;
    }

    const phase2Queue = [];
    const results = new Map();

    for (const o of unclassified) {
      const role = _vctClassifyPhase1(o);
      if (role !== null) {
        results.set(o.order_id, role);
      } else {
        phase2Queue.push(o);
      }
    }

    // Phase 2: "seen" set from main DB + staging (client-scoped)
    if (phase2Queue.length > 0) {
      const seen = new Set();

      // Already-classified in staging
      const stagingClassified = h.querySql(`
        SELECT customer_id, campaign_id, main_product_id
        FROM orders
        WHERE client_id = ? AND derived_product_role IS NOT NULL
          AND derived_product_role IN ('main_initial', 'upsell_initial')
      `, [clientId]);
      for (const o of stagingClassified) {
        seen.add(o.customer_id + ':' + o.campaign_id + ':' + (o.main_product_id || 'null'));
      }

      // Already-classified in main DB (client-scoped)
      const mainClassified = db.prepare(`
        SELECT customer_id, campaign_id, main_product_id
        FROM maindb.orders
        WHERE client_id = ? AND derived_product_role IS NOT NULL
          AND derived_product_role IN ('main_initial', 'upsell_initial')
      `).all(clientId);
      for (const o of mainClassified) {
        seen.add(o.customer_id + ':' + o.campaign_id + ':' + (o.main_product_id || 'null'));
      }

      for (const o of phase2Queue) {
        const isUpsell = UPSELL_CAMPS.has(o.campaign_id) || UPSELL_PRODUCT_IDS.has(o.main_product_id);
        const key = o.customer_id + ':' + o.campaign_id + ':' + (o.main_product_id || 'null');

        if (seen.has(key)) {
          results.set(o.order_id, isUpsell ? 'upsell_reprocessing' : 'initial_reprocessing');
        } else {
          results.set(o.order_id, isUpsell ? 'upsell_initial' : 'main_initial');
          seen.add(key);
        }
      }
    }

    // Apply in batched transaction
    const updateStmt = db.prepare(
      'UPDATE orders SET derived_product_role = ? WHERE client_id = ? AND order_id = ?'
    );
    const entries = [...results.entries()];
    const BATCH_SIZE = 5000;
    for (let i = 0; i < entries.length; i += BATCH_SIZE) {
      const batch = entries.slice(i, i + BATCH_SIZE);
      db.transaction(() => {
        for (const [orderId, role] of batch) {
          updateStmt.run(role, clientId, orderId);
        }
      })();
    }

    result.classified = results.size;
    console.log(`[StagingPostSync] Step 1: Classified ${results.size} VCT orders`);
  });

  // Step 2b: Parse cascade chains (self-contained)
  _parseCascadeChains(db, h, clientId, result);
}

function _vctClassifyPhase1(order) {
  const { customer_id, order_total, billing_cycle, retry_attempt,
    campaign_id, main_product_id, is_test, is_internal_test, is_test_cc } = order;

  if (is_test || is_internal_test || is_test_cc) return 'excluded';
  if (customer_id == null) return 'anonymous_decline';

  const isUpsell = UPSELL_CAMPS.has(campaign_id) || UPSELL_PRODUCT_IDS.has(main_product_id);
  if (isUpsell) {
    if (billing_cycle === 0) return null;
    if (retry_attempt > 0) return 'upsell_rebill_retry';
    return 'upsell_rebill';
  }

  if (order_total === 0 || order_total === '0' || order_total === 0.0) return 'subscription_trigger';

  if (DECLINE_SALVAGE_CAMPS.has(campaign_id)) {
    if (billing_cycle === 0) return 'decline_salvage';
    if (retry_attempt > 0) return 'decline_salvage_retry';
    return 'decline_salvage_rebill';
  }

  if (MEMBERSHIP_CAMPS.has(campaign_id)) {
    if (billing_cycle === 0) return 'membership';
    if (retry_attempt > 0) return 'membership_retry';
    return 'membership_rebill';
  }

  if (billing_cycle > 0) {
    if (retry_attempt > 0) return 'rebill_retry';
    return 'main_rebill';
  }

  return null;
}

// ── Cascade chain parser (shared by KP and VCT) ─────────────────────────

function _parseCascadeChains(db, h, clientId, result) {
  _safeStep('Step 2b (cascade chains)', result, () => {
    const rows = h.querySql(`
      SELECT order_id, system_notes, gateway_id
      FROM orders
      WHERE client_id = ? AND is_cascaded = 1
        AND (cascade_chain IS NULL OR cascade_chain = '')
      ORDER BY order_id
    `, [clientId]);

    if (rows.length === 0) return;

    const updateStmt = db.prepare(
      'UPDATE orders SET cascade_chain = ?, processing_gateway_id = ? WHERE client_id = ? AND order_id = ?'
    );

    let parsed = 0;
    const BATCH_SIZE = 5000;

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      db.transaction(() => {
        for (const row of batch) {
          const chain = _parseSingleCascadeChain(row.system_notes);
          if (chain && chain.chain.length > 0) {
            updateStmt.run(chain.chain.join(','), chain.processingGw, clientId, row.order_id);
            parsed++;
          }
        }
      })();
    }

    result.cascadesParsed = parsed;
    if (parsed > 0) console.log(`[StagingPostSync] Step 2b: Parsed ${parsed} cascade chains`);
  });
}

function _parseSingleCascadeChain(systemNotes) {
  if (!systemNotes) return null;

  let notes;
  try { notes = JSON.parse(systemNotes); } catch { return null; }
  if (!Array.isArray(notes)) return null;

  const chain = [];
  let originalGw = null;

  for (const note of notes) {
    if (typeof note !== 'string') continue;

    const origMatch = note.match(/Order attempted to process on gateway \((\d+)\)/);
    if (origMatch) {
      const gw = parseInt(origMatch[1], 10);
      if (originalGw === null) originalGw = gw;
      if (!chain.includes(gw)) chain.push(gw);
    }

    const savedMatch = note.match(/cascade gateway id \((\d+)\) saved the sale/i);
    if (savedMatch) {
      const gw = parseInt(savedMatch[1], 10);
      if (!chain.includes(gw)) chain.push(gw);
    }

    const alsoDeclinedMatch = note.match(/[Cc]ascade gateway id \((\d+)\) also declined/);
    if (alsoDeclinedMatch) {
      const gw = parseInt(alsoDeclinedMatch[1], 10);
      if (!chain.includes(gw)) chain.push(gw);
    }

    const declinedByMatch = note.match(/Declined by cascade gateway: \((\d+)\)/);
    if (declinedByMatch) {
      const gw = parseInt(declinedByMatch[1], 10);
      if (!chain.includes(gw)) chain.push(gw);
    }
  }

  if (chain.length === 0) return null;

  return { chain, processingGw: chain[0] };
}

module.exports = { runStagingPostSync };
