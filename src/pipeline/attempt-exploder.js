/**
 * Attempt Exploder — Single-pass: explodes orders into transaction_attempts
 * with core features populated inline (no separate feature extraction pass).
 *
 * Pre-builds 6 lookup maps per client, then for each order:
 *   1. Parse cascade chain → generate attempt rows
 *   2. Enrich each row with BIN, temporal, MID age, cascade flags, relationships
 *   3. INSERT with feature_version = 1
 *
 * Velocity + subscription features are computed in separate passes (Phase B, C).
 */
const { querySql, getDb, checkpointWal } = require('../db/connection');
const {
  _buildGatewayMap,
  _buildBinMap,
  _buildInitialProcessorMap,
  _buildPrevDeclineMap,
  _buildLastApprovedProcessorMap,
  _buildParentDeclinedProcessorMap,
} = require('../analytics/feature-extraction');
const { normalizeProcessor, normalizeBank } = require('../analytics/network-analysis');

const BATCH_SIZE = 5000;
const WAL_CHECKPOINT_INTERVAL = 50000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Full backfill — explode all qualifying orders for a client.
 * Skips orders that already have rows in transaction_attempts.
 */
function explodeAllOrders(clientId, { onProgress } = {}) {
  const existing = querySql(
    `SELECT DISTINCT sticky_order_id FROM transaction_attempts WHERE client_id = ?`,
    [clientId]
  );
  const existingSet = new Set(existing.map(r => r.sticky_order_id));

  const orders = _loadQualifyingOrders(clientId);
  const toProcess = orders.filter(o => !existingSet.has(o.order_id));

  if (toProcess.length === 0) {
    console.log(`[Exploder] Client ${clientId}: all ${orders.length} orders already exploded`);
    return { total: 0, inserted: 0, skipped: orders.length };
  }

  console.log(`[Exploder] Client ${clientId}: ${toProcess.length} orders to explode (${existingSet.size} already done)`);
  console.log(`[Exploder] Building lookup maps...`);

  const maps = _buildAllMaps(clientId);
  const db = getDb();
  const insertStmt = db.prepare(_insertSql());

  let inserted = 0;
  for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
    const batch = toProcess.slice(i, i + BATCH_SIZE);
    const runBatch = db.transaction((rows) => {
      for (const order of rows) {
        const attempts = _explodeOrder(order, maps);
        for (const att of attempts) {
          insertStmt.run(..._attToParams(clientId, att));
          inserted++;
        }
      }
    });
    runBatch(batch);

    // WAL checkpoint to prevent bloat on large runs
    if (inserted % WAL_CHECKPOINT_INTERVAL < BATCH_SIZE) {
      checkpointWal();
    }

    if (onProgress) onProgress(Math.min(i + BATCH_SIZE, toProcess.length), toProcess.length, inserted);
  }

  return { total: toProcess.length, inserted, skipped: existingSet.size };
}

/**
 * Incremental — explode specific orders by their internal IDs.
 * Used by post-sync after new orders are imported.
 */
function explodeOrdersToAttempts(clientId, orderInternalIds) {
  if (!orderInternalIds || orderInternalIds.length === 0) return 0;

  const placeholders = orderInternalIds.map(() => '?').join(',');
  const orders = querySql(`
    SELECT id, order_id, customer_id, gateway_id, order_status, order_total,
           cc_first_6, acquisition_date, is_cascaded, cascade_chain,
           derived_product_role, product_type_classified, derived_cycle, derived_attempt,
           product_group_id, offer_name, billing_state, decline_reason
    FROM orders
    WHERE id IN (${placeholders})
      AND order_status IN (2, 6, 7, 8)
      AND is_test = 0 AND is_internal_test = 0
      AND product_type_classified IS NOT NULL
      AND product_type_classified != 'straight_sale'
      AND gateway_id NOT IN (
        SELECT gateway_id FROM gateways WHERE client_id = ${clientId} AND exclude_from_analysis = 1
      )
      AND NOT (customer_id IS NULL AND product_type_classified = 'rebill')
  `, orderInternalIds);

  if (orders.length === 0) return 0;

  const maps = _buildAllMaps(clientId);
  const db = getDb();
  const insertStmt = db.prepare(_insertSql());

  let inserted = 0;
  const runBatch = db.transaction((rows) => {
    for (const order of rows) {
      const attempts = _explodeOrder(order, maps);
      for (const att of attempts) {
        insertStmt.run(..._attToParams(clientId, att));
        inserted++;
      }
    }
  });
  runBatch(orders);

  return inserted;
}

// ---------------------------------------------------------------------------
// Lookup Maps
// ---------------------------------------------------------------------------

function _buildAllMaps(clientId) {
  const gatewayMap = _buildGatewayMap(clientId);
  const binMap = _buildBinMap();
  const initialProcMap = _buildInitialProcessorMap(clientId);
  const prevDeclineMap = _buildPrevDeclineMap(clientId);
  const lastApprovedProcMap = _buildLastApprovedProcessorMap(clientId);
  const parentDeclinedProcMap = _buildParentDeclinedProcessorMap(clientId);

  // Build set of excluded gateway IDs (Payfac, Dry Run, etc.)
  const excludedGwRows = querySql(
    'SELECT gateway_id FROM gateways WHERE client_id = ? AND exclude_from_analysis = 1', [clientId]
  );
  const excludedGateways = new Set(excludedGwRows.map(r => r.gateway_id));

  console.log(`[Exploder] Maps built — ${gatewayMap.size} gateways, ${binMap.size} BINs, ${initialProcMap.size} initial procs, ${excludedGateways.size} excluded GWs`);

  return { gatewayMap, binMap, initialProcMap, prevDeclineMap, lastApprovedProcMap, parentDeclinedProcMap, excludedGateways };
}

// ---------------------------------------------------------------------------
// Order Loading
// ---------------------------------------------------------------------------

function _loadQualifyingOrders(clientId) {
  return querySql(`
    SELECT id, order_id, customer_id, gateway_id, order_status, order_total,
           cc_first_6, acquisition_date, is_cascaded, cascade_chain,
           derived_product_role, product_type_classified, derived_cycle, derived_attempt,
           product_group_id, offer_name, billing_state, decline_reason
    FROM orders
    WHERE client_id = ?
      AND order_status IN (2, 6, 7, 8)
      AND is_test = 0 AND is_internal_test = 0
      AND product_type_classified IS NOT NULL
      AND product_type_classified != 'straight_sale'
      AND gateway_id NOT IN (
        SELECT gateway_id FROM gateways WHERE client_id = ? AND exclude_from_analysis = 1
      )
      AND NOT (customer_id IS NULL AND product_type_classified = 'rebill')
    ORDER BY order_id
  `, [clientId, clientId]);
}

// ---------------------------------------------------------------------------
// Core: Explode + Enrich
// ---------------------------------------------------------------------------

/**
 * Explode a single order into enriched attempt rows.
 * Returns array of attempt objects with all core features populated.
 */
function _explodeOrder(order, maps) {
  const { gatewayMap, binMap, initialProcMap, prevDeclineMap, lastApprovedProcMap, parentDeclinedProcMap, excludedGateways } = maps;
  const attempts = [];
  const isApproved = [2, 6, 8].includes(order.order_status);
  const orderOutcome = isApproved ? 'approved' : 'declined';

  // --- BIN enrichment (same for all attempts of this order) ---
  const bin = binMap.get(order.cc_first_6);
  const binFields = {
    issuer_bank: bin ? normalizeBank(bin.issuer_bank) : null,
    card_brand: bin ? bin.card_brand : null,
    card_type: bin ? bin.card_type : null,
    is_prepaid: bin ? (bin.is_prepaid || 0) : 0,
  };

  // --- Temporal (same for all attempts) ---
  let hour_of_day = null;
  let day_of_week = null;
  if (order.acquisition_date) {
    const dt = new Date(order.acquisition_date);
    if (!isNaN(dt.getTime())) {
      hour_of_day = dt.getUTCHours();
      day_of_week = dt.getUTCDay();
    }
  }

  // --- Relationship features (per order, from maps) ---
  const modelCtx = _getModelContext(order);
  const initial_processor = modelCtx.needsInitialProc
    ? (initialProcMap.get(order.customer_id) || null) : null;
  const last_approved_processor = modelCtx.needsLastApproved
    ? (lastApprovedProcMap.get(`${order.customer_id}|${order.product_group_id}`) || initial_processor) : null;
  const parent_declined_processor = modelCtx.needsParentDeclined
    ? (parentDeclinedProcMap.get(`${order.customer_id}|${order.product_group_id}|${order.derived_cycle}|1`) || null) : null;
  const prev_decline_reason = (order.derived_attempt || 1) >= 2
    ? (prevDeclineMap.get(`${order.customer_id}|${order.product_group_id}|${order.derived_cycle}|${(order.derived_attempt || 1) - 1}`) || null) : null;

  // Common fields shared across all attempts
  const common = {
    order_id: order.id,
    sticky_order_id: order.order_id,
    customer_id: order.customer_id,
    cc_first_6: order.cc_first_6,
    order_total: order.order_total,
    acquisition_date: order.acquisition_date,
    derived_product_role: order.derived_product_role,
    product_type_classified: order.product_type_classified,
    derived_cycle: order.derived_cycle,
    derived_attempt: order.derived_attempt,
    product_group_id: order.product_group_id,
    offer_name: order.offer_name,
    billing_state: order.billing_state,
    is_cascaded: order.is_cascaded || 0,
    // Features shared across attempts
    ...binFields,
    hour_of_day,
    day_of_week,
    initial_processor,
    last_approved_processor,
    parent_declined_processor,
    prev_decline_reason,
  };

  // Parse cascade chain
  let chain = [];
  if (order.cascade_chain && order.cascade_chain !== '[]') {
    try {
      const parsed = JSON.parse(order.cascade_chain);
      if (Array.isArray(parsed)) chain = parsed;
    } catch (e) { /* invalid JSON */ }
  }

  const hasCascadeData = order.is_cascaded === 1 && chain.length > 0;
  const isCascadedNoChain = order.is_cascaded === 1 && chain.length === 0;

  if (hasCascadeData) {
    _explodeCascadedOrder(order, chain, common, gatewayMap, excludedGateways, orderOutcome, isApproved, attempts);
  } else {
    _explodeSimpleOrder(order, common, gatewayMap, excludedGateways, orderOutcome, isCascadedNoChain, attempts);
  }

  return attempts;
}

/**
 * Cascaded order WITH chain data → multiple attempt rows.
 */
function _explodeCascadedOrder(order, chain, common, gatewayMap, excludedGateways, orderOutcome, isApproved, attempts) {
  const finalNotInChain = !chain.some(e => e.gateway_id === order.gateway_id);
  const totalAttempts = chain.length + (finalNotInChain ? 1 : 0);

  // Final cascade outcome for cross-reference on declined rows
  const finalGw = gatewayMap.get(order.gateway_id);
  const cascadeFinalOutcome = orderOutcome;
  const cascadeApprovedProc = isApproved && finalGw ? normalizeProcessor(finalGw.processor_name) : null;

  let initialDeclinedProcessor = null;
  let initialDeclineReason = null;
  const procsTried = [];

  // Track decline reasons for cascade flags
  const allDeclineReasons = chain.map(e => (e.decline_reason || '').toUpperCase());

  for (let i = 0; i < chain.length; i++) {
    const entry = chain[i];
    const gw = gatewayMap.get(entry.gateway_id);
    const procName = gw ? normalizeProcessor(gw.processor_name) : null;

    if (i === 0) {
      initialDeclinedProcessor = procName;
      initialDeclineReason = entry.decline_reason || null;
    }

    // Cascade flags: check decline reasons of attempts BEFORE this one
    const priorReasons = allDeclineReasons.slice(0, i);
    const cascadeFlags = _computeCascadeFlags(priorReasons);

    // MID age for this specific gateway
    const mid_age_days = _computeMidAge(gw, order.acquisition_date);

    attempts.push({
      ...common,
      attempt_seq: i + 1,
      gateway_id: entry.gateway_id,
      processor_name: procName,
      acquiring_bank: gw ? gw.bank_name : null,
      mcc_code: gw ? gw.mcc_code : null,
      outcome: 'declined',
      decline_reason: entry.decline_reason || null,
      initial_declined_processor: i > 0 ? initialDeclinedProcessor : null,
      initial_decline_reason: i > 0 ? initialDeclineReason : null,
      cascade_position: i,
      total_attempts: totalAttempts,
      processors_tried_before: i > 0 ? procsTried.join(',') : null,
      cascade_final_outcome: cascadeFinalOutcome,
      cascade_approved_processor: cascadeApprovedProc,
      mid_age_days,
      ...cascadeFlags,
      model_target: _assignModelTarget(common.derived_product_role, common.derived_attempt, i, entry.gateway_id, excludedGateways),
      source: 'chain',
      feature_version: 1,
    });

    if (procName && !procsTried.includes(procName)) procsTried.push(procName);
  }

  // Final row if gateway_id not in chain
  if (finalNotInChain) {
    const gw = gatewayMap.get(order.gateway_id);
    const procName = gw ? normalizeProcessor(gw.processor_name) : null;
    const mid_age_days = _computeMidAge(gw, order.acquisition_date);
    const cascadeFlags = _computeCascadeFlags(allDeclineReasons);

    attempts.push({
      ...common,
      attempt_seq: chain.length + 1,
      gateway_id: order.gateway_id,
      processor_name: procName,
      acquiring_bank: gw ? gw.bank_name : null,
      mcc_code: gw ? gw.mcc_code : null,
      outcome: orderOutcome,
      decline_reason: orderOutcome === 'declined' ? order.decline_reason : null,
      initial_declined_processor: initialDeclinedProcessor,
      initial_decline_reason: initialDeclineReason,
      cascade_position: chain.length,
      total_attempts: totalAttempts,
      processors_tried_before: procsTried.join(',') || null,
      cascade_final_outcome: null,
      cascade_approved_processor: null,
      mid_age_days,
      ...cascadeFlags,
      model_target: _assignModelTarget(common.derived_product_role, common.derived_attempt, chain.length, order.gateway_id, excludedGateways),
      source: 'chain',
      feature_version: 1,
    });
  }
}

/**
 * Non-cascaded order OR cascaded without chain → single attempt row.
 */
function _explodeSimpleOrder(order, common, gatewayMap, excludedGateways, orderOutcome, isCascadedNoChain, attempts) {
  const gw = gatewayMap.get(order.gateway_id);
  const procName = gw ? normalizeProcessor(gw.processor_name) : null;
  const mid_age_days = _computeMidAge(gw, order.acquisition_date);

  attempts.push({
    ...common,
    attempt_seq: 1,
    gateway_id: order.gateway_id,
    processor_name: procName,
    acquiring_bank: gw ? gw.bank_name : null,
    mcc_code: gw ? gw.mcc_code : null,
    outcome: orderOutcome,
    decline_reason: orderOutcome === 'declined' ? order.decline_reason : null,
    initial_declined_processor: null,
    initial_decline_reason: null,
    cascade_position: 0,
    total_attempts: 1,
    processors_tried_before: null,
    cascade_final_outcome: null,
    cascade_approved_processor: null,
    mid_age_days,
    had_nsf: 0,
    had_do_not_honor: 0,
    had_pickup: 0,
    model_target: _assignModelTarget(common.derived_product_role, common.derived_attempt, 0, order.gateway_id, excludedGateways),
    source: isCascadedNoChain ? 'incomplete' : 'order_direct',
    feature_version: 1,
  });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Determine which relationship features this order needs based on its role.
 */
function _getModelContext(order) {
  const role = order.derived_product_role;
  const attempt = order.derived_attempt || 1;
  const isInitial = role === 'main_initial' || role === 'upsell_initial';
  const isRebill = role === 'main_rebill' || role === 'upsell_rebill';

  return {
    needsInitialProc: !isInitial, // rebills and salvage need to know who approved the initial
    needsLastApproved: isRebill,  // rebills need the last successful processor
    needsParentDeclined: isRebill && attempt >= 2, // salvage needs the natural attempt's processor
  };
}

/**
 * Compute MID age in days at the time of the transaction.
 */
function _computeMidAge(gw, acquisitionDate) {
  if (!gw || !gw.gateway_created || !acquisitionDate) return null;
  const gwCreated = new Date(gw.gateway_created);
  const orderDate = new Date(acquisitionDate);
  if (isNaN(gwCreated.getTime()) || isNaN(orderDate.getTime())) return null;
  return Math.max(0, (orderDate - gwCreated) / 86400000);
}

/**
 * Check prior cascade decline reasons for NSF, Do Not Honor, Pick Up patterns.
 */
function _computeCascadeFlags(priorReasons) {
  let had_nsf = 0, had_do_not_honor = 0, had_pickup = 0;
  for (const reason of priorReasons) {
    if (reason.includes('NSF') || reason.includes('INSUFFICIENT')) had_nsf = 1;
    if (reason.includes('DO NOT HONOR')) had_do_not_honor = 1;
    if (reason.includes('PICK UP')) had_pickup = 1;
  }
  return { had_nsf, had_do_not_honor, had_pickup };
}

/**
 * Universal model_target assignment — no per-client logic.
 * If gatewayId is in excludedGateways, initial attempts are excluded
 * (cascade/rebill/salvage on excluded GWs are kept — real historical data).
 */
function _assignModelTarget(derivedProductRole, derivedAttempt, cascadePosition, gatewayId, excludedGateways) {
  if (!derivedProductRole) return 'excluded';

  if (cascadePosition > 0) return 'cascade';

  if (derivedProductRole === 'main_initial' || derivedProductRole === 'upsell_initial') {
    // Don't train initial model on excluded gateways (Payfac, etc.)
    if (excludedGateways && excludedGateways.has(gatewayId)) return 'excluded';
    return 'initial';
  }
  if (derivedProductRole === 'main_rebill' || derivedProductRole === 'upsell_rebill') {
    return (derivedAttempt || 1) >= 2 ? 'rebill_salvage' : 'rebill';
  }

  return 'excluded';
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

function _insertSql() {
  return `INSERT OR IGNORE INTO transaction_attempts (
    client_id, order_id, sticky_order_id, customer_id,
    attempt_seq, gateway_id, processor_name, acquiring_bank, mcc_code,
    outcome, decline_reason,
    cc_first_6, order_total, acquisition_date,
    derived_product_role, product_type_classified,
    derived_cycle, derived_attempt, product_group_id,
    offer_name, billing_state, is_cascaded,
    initial_declined_processor, initial_decline_reason,
    cascade_position, total_attempts, processors_tried_before,
    cascade_final_outcome, cascade_approved_processor,
    model_target, source,
    issuer_bank, card_brand, card_type, is_prepaid,
    hour_of_day, day_of_week, mid_age_days,
    had_nsf, had_do_not_honor, had_pickup,
    initial_processor, last_approved_processor, parent_declined_processor, prev_decline_reason,
    feature_version
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
}

function _attToParams(clientId, att) {
  return [
    clientId, att.order_id, att.sticky_order_id, att.customer_id,
    att.attempt_seq, att.gateway_id, att.processor_name, att.acquiring_bank, att.mcc_code,
    att.outcome, att.decline_reason,
    att.cc_first_6, att.order_total, att.acquisition_date,
    att.derived_product_role, att.product_type_classified,
    att.derived_cycle, att.derived_attempt, att.product_group_id,
    att.offer_name, att.billing_state, att.is_cascaded,
    att.initial_declined_processor, att.initial_decline_reason,
    att.cascade_position, att.total_attempts, att.processors_tried_before,
    att.cascade_final_outcome, att.cascade_approved_processor,
    att.model_target, att.source,
    att.issuer_bank, att.card_brand, att.card_type, att.is_prepaid,
    att.hour_of_day, att.day_of_week, att.mid_age_days,
    att.had_nsf, att.had_do_not_honor, att.had_pickup,
    att.initial_processor, att.last_approved_processor, att.parent_declined_processor, att.prev_decline_reason,
    att.feature_version,
  ];
}

module.exports = { explodeAllOrders, explodeOrdersToAttempts };
