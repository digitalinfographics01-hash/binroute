/**
 * Feature Extraction — Builds denormalized tx_features rows for AI training.
 *
 * Layer 1: 15 features from existing data (4 velocity features are Layer 2, NULL for now).
 *
 * Two modes:
 *   extractFeatures(clientId)  — incremental, processes only new orders
 *   rebuildFeatures(clientId)  — drops and recomputes all for a client
 */
const { querySql, runSql, getDb, saveDb } = require('../db/connection');
const { normalizeProcessor, normalizeBank } = require('./network-analysis');
const {
  CRM_ROUTING_EXCLUSION,
} = require('./engine');

const BATCH_SIZE = 5000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Incremental extraction — processes orders not yet in tx_features.
 * @param {number} clientId
 * @returns {number} rows inserted
 */
function extractFeatures(clientId) {
  const orders = _loadQualifiedOrders(clientId, true);
  if (orders.length === 0) return 0;
  return _processOrders(clientId, orders);
}

/**
 * Full rebuild — drops all tx_features for this client and recomputes.
 * @param {number} clientId
 * @returns {number} rows inserted
 */
function rebuildFeatures(clientId) {
  runSql('DELETE FROM tx_features WHERE client_id = ?', [clientId]);
  const orders = _loadQualifiedOrders(clientId, false);
  if (orders.length === 0) return 0;
  return _processOrders(clientId, orders);
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

/**
 * Load qualified orders for feature extraction.
 * @param {number} clientId
 * @param {boolean} incrementalOnly — if true, exclude orders already in tx_features
 */
function _loadQualifiedOrders(clientId, incrementalOnly) {
  const incrementalFilter = incrementalOnly
    ? `AND o.id NOT IN (SELECT order_id FROM tx_features WHERE client_id = ${clientId})`
    : '';

  // Replace 'o.' prefix in CRM_ROUTING_EXCLUSION for our alias
  const crmFilter = CRM_ROUTING_EXCLUSION;

  return querySql(`
    SELECT o.id, o.order_id, o.customer_id, o.order_status, o.order_total,
           o.cc_first_6, o.acquisition_date, o.decline_reason,
           o.derived_product_role, o.derived_cycle, o.derived_attempt,
           o.is_cascaded, o.processing_gateway_id, o.product_group_id,
           o.client_id, o.cascade_chain
    FROM orders o
    WHERE o.client_id = ?
      AND o.order_status IN (2, 6, 7, 8)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.processing_gateway_id IS NOT NULL
      AND o.cc_first_6 IS NOT NULL
      AND ${crmFilter}
      ${incrementalFilter}
    ORDER BY o.customer_id, o.product_group_id, o.derived_cycle, o.derived_attempt
  `, [clientId]);
}

/**
 * Process orders into tx_features rows.
 */
function _processOrders(clientId, orders) {
  // Pre-compute lookup maps
  const gatewayMap = _buildGatewayMap(clientId);
  const binMap = _buildBinMap();
  const initialProcMap = _buildInitialProcessorMap(clientId);
  const prevDeclineMap = _buildPrevDeclineMap(clientId);

  const db = getDb();

  // Ensure cascade feature columns exist
  const cols = querySql('PRAGMA table_info(tx_features)').map(c => c.name);
  if (!cols.includes('cascade_depth')) {
    runSql('ALTER TABLE tx_features ADD COLUMN cascade_depth INTEGER DEFAULT 0');
    runSql('ALTER TABLE tx_features ADD COLUMN cascade_processors_tried TEXT DEFAULT NULL');
    runSql('ALTER TABLE tx_features ADD COLUMN cascade_decline_reasons TEXT DEFAULT NULL');
    saveDb();
  }

  const insertStmt = db.prepare(`
    INSERT OR IGNORE INTO tx_features (
      order_id, client_id, sticky_order_id,
      outcome,
      processor_name, acquiring_bank, mcc_code,
      issuer_bank, card_brand, card_type, is_prepaid,
      amount, tx_class, attempt_number, cycle_depth,
      hour_of_day, day_of_week, prev_decline_reason,
      initial_processor,
      cascade_depth, cascade_processors_tried, cascade_decline_reasons,
      acquisition_date, feature_version
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 2)
  `);

  let inserted = 0;

  // Process in batches inside transactions
  for (let b = 0; b < orders.length; b += BATCH_SIZE) {
    const batch = orders.slice(b, b + BATCH_SIZE);
    const runBatch = db.transaction((rows) => {
      for (const o of rows) {
        const features = _extractSingleOrder(o, gatewayMap, binMap, initialProcMap, prevDeclineMap);
        if (!features) continue;

        const result = insertStmt.run(
          o.id,                          // order_id (internal row id)
          clientId,                      // client_id
          o.order_id,                    // sticky_order_id
          features.outcome,
          features.processor_name,
          features.acquiring_bank,
          features.mcc_code,
          features.issuer_bank,
          features.card_brand,
          features.card_type,
          features.is_prepaid,
          features.amount,
          features.tx_class,
          features.attempt_number,
          features.cycle_depth,
          features.hour_of_day,
          features.day_of_week,
          features.prev_decline_reason,
          features.initial_processor,
          features.cascade_depth,
          features.cascade_processors_tried,
          features.cascade_decline_reasons,
          features.acquisition_date
        );
        if (result.changes > 0) inserted++;
      }
    });
    runBatch(batch);
  }

  saveDb();
  return inserted;
}

/**
 * Extract features for a single order row.
 */
function _extractSingleOrder(o, gatewayMap, binMap, initialProcMap, prevDeclineMap) {
  // --- LABEL ---
  const outcome = [2, 6, 8].includes(o.order_status) ? 'approved' : 'declined';

  // --- ACQUIRING SIDE ---
  const gw = gatewayMap.get(o.processing_gateway_id);
  const processor_name = gw ? normalizeProcessor(gw.processor_name) : null;
  const acquiring_bank = gw ? gw.bank_name : null;
  const mcc_code = gw ? gw.mcc_code : null;

  // --- ISSUING SIDE ---
  const bin = binMap.get(o.cc_first_6);
  const issuer_bank = bin ? normalizeBank(bin.issuer_bank) : null;
  const card_brand = bin ? bin.card_brand : null;
  const card_type = bin ? bin.card_type : null;
  const is_prepaid = bin ? (bin.is_prepaid || 0) : 0;

  // --- TRANSACTION ---
  const amount = o.order_total;
  const tx_class = _deriveTxClass(o);
  const attempt_number = o.derived_attempt;
  const cycle_depth = _deriveCycleDepth(o.derived_cycle);

  // Hour/day from acquisition_date
  let hour_of_day = null;
  let day_of_week = null;
  if (o.acquisition_date) {
    const dt = new Date(o.acquisition_date);
    if (!isNaN(dt.getTime())) {
      hour_of_day = dt.getUTCHours();
      day_of_week = dt.getUTCDay(); // 0=Sun
    }
  }

  // --- RELATIONSHIP ---
  // prev_decline_reason: for attempt >= 2, find prior attempt's decline reason
  const prevKey = `${o.customer_id}|${o.product_group_id}|${o.derived_cycle}`;
  const prevAttempt = (o.derived_attempt || 1) - 1;
  const prev_decline_reason = prevAttempt >= 1
    ? (prevDeclineMap.get(`${prevKey}|${prevAttempt}`) || null)
    : null;

  // initial_processor: for non-initials, who approved the customer's initial
  let initial_processor = null;
  if (tx_class !== 'initial' && tx_class !== 'upsell') {
    initial_processor = initialProcMap.get(o.customer_id) || null;
  }

  // --- CASCADE CHAIN FEATURES ---
  let cascade_depth = 0;
  let cascade_processors_tried = null;
  let cascade_decline_reasons = null;

  if (o.cascade_chain && o.cascade_chain !== '[]') {
    try {
      const chain = JSON.parse(o.cascade_chain);
      if (Array.isArray(chain) && chain.length > 0) {
        cascade_depth = chain.length;

        // Map gateway_ids to processor names
        const procsTried = [];
        for (const entry of chain) {
          const gw = gatewayMap.get(entry.gateway_id);
          const proc = gw ? normalizeProcessor(gw.processor_name) : `GW${entry.gateway_id}`;
          if (!procsTried.includes(proc)) procsTried.push(proc);
        }
        cascade_processors_tried = procsTried.join(',');

        // Collect unique decline reasons in order
        const reasons = [];
        for (const entry of chain) {
          if (entry.decline_reason && !reasons.includes(entry.decline_reason)) {
            reasons.push(entry.decline_reason);
          }
        }
        cascade_decline_reasons = reasons.join(',');
      }
    } catch (e) {
      // Invalid JSON — leave defaults
    }
  }

  return {
    outcome,
    processor_name,
    acquiring_bank,
    mcc_code,
    issuer_bank,
    card_brand,
    card_type,
    is_prepaid,
    amount,
    tx_class,
    attempt_number,
    cycle_depth,
    hour_of_day,
    day_of_week,
    prev_decline_reason,
    initial_processor,
    cascade_depth,
    cascade_processors_tried,
    cascade_decline_reasons,
    acquisition_date: o.acquisition_date,
  };
}

// ---------------------------------------------------------------------------
// Derivation helpers
// ---------------------------------------------------------------------------

function _deriveTxClass(o) {
  if (o.is_cascaded === 1) return 'cascade';
  const role = o.derived_product_role;
  const attempt = o.derived_attempt || 1;

  if (role === 'upsell_initial' && attempt === 1) return 'upsell';
  if (role === 'main_initial' && attempt === 1) return 'initial';
  if ((role === 'main_rebill' || role === 'upsell_rebill') && attempt === 1) return 'rebill';
  if (attempt >= 2) return 'salvage';

  // Fallback
  if (role === 'main_initial' || role === 'upsell_initial') return 'initial';
  if (role === 'main_rebill' || role === 'upsell_rebill') return 'rebill';
  return 'initial'; // safe default
}

function _deriveCycleDepth(cycle) {
  if (cycle === null || cycle === undefined || cycle === 0) return 'C0';
  if (cycle === 1) return 'C1';
  if (cycle === 2) return 'C2';
  return 'C3+';
}

// ---------------------------------------------------------------------------
// Lookup map builders
// ---------------------------------------------------------------------------

/** Map<gateway_id, { processor_name, bank_name, mcc_code }> across all clients */
function _buildGatewayMap(clientId) {
  const rows = querySql(`
    SELECT gateway_id, processor_name, bank_name, mcc_code
    FROM gateways WHERE client_id = ?
  `, [clientId]);
  const m = new Map();
  for (const r of rows) m.set(r.gateway_id, r);
  return m;
}

/** Map<bin, { issuer_bank, card_brand, card_type, is_prepaid }> */
function _buildBinMap() {
  const rows = querySql('SELECT bin, issuer_bank, card_brand, card_type, is_prepaid FROM bin_lookup');
  const m = new Map();
  for (const r of rows) m.set(r.bin, r);
  return m;
}

/**
 * Map<customer_id, processor_name> — the processor that approved the customer's initial order.
 * Uses the earliest approved main_initial order per customer.
 */
function _buildInitialProcessorMap(clientId) {
  const rows = querySql(`
    SELECT o.customer_id, g.processor_name
    FROM orders o
    JOIN gateways g ON g.client_id = o.client_id AND g.gateway_id = o.processing_gateway_id
    WHERE o.client_id = ?
      AND o.derived_product_role = 'main_initial'
      AND o.order_status IN (2, 6, 8)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.customer_id IS NOT NULL
    ORDER BY o.acquisition_date ASC, o.order_id ASC
  `, [clientId]);

  const m = new Map();
  for (const r of rows) {
    // First approved initial wins (Map.set only if not already set)
    if (!m.has(r.customer_id)) {
      m.set(r.customer_id, normalizeProcessor(r.processor_name));
    }
  }
  return m;
}

/**
 * Map<"customer_id|product_group_id|cycle|attempt", decline_reason>
 * For looking up the decline reason of the prior attempt.
 */
function _buildPrevDeclineMap(clientId) {
  const rows = querySql(`
    SELECT customer_id, product_group_id, derived_cycle, derived_attempt, decline_reason
    FROM orders
    WHERE client_id = ?
      AND order_status = 7
      AND is_test = 0 AND is_internal_test = 0
      AND customer_id IS NOT NULL
      AND derived_attempt IS NOT NULL
  `, [clientId]);

  const m = new Map();
  for (const r of rows) {
    const key = `${r.customer_id}|${r.product_group_id}|${r.derived_cycle}|${r.derived_attempt}`;
    m.set(key, r.decline_reason);
  }
  return m;
}

module.exports = { extractFeatures, rebuildFeatures };
