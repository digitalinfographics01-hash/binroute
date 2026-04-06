/**
 * Subscription Features — Layer 2.5 enrichment for tx_features.
 *
 * Computes 8 rebill-specific features that capture subscription health
 * at the moment each order was processed:
 *
 *   20. consecutive_approvals     — streak of approved charges before this one
 *   21. days_since_last_charge    — days since customer's last approved charge (same product group)
 *   22. days_since_initial        — days since customer's initial approved order
 *   23. lifetime_charges          — total approved charges before this one (same product group)
 *   24. lifetime_revenue          — total approved revenue before this one (same product group)
 *   25. initial_amount            — what the customer paid on their initial order
 *   26. amount_ratio              — this order's amount / initial amount (downsell detection)
 *   27. prior_declines_in_cycle   — how many declines in this billing cycle before this attempt
 *
 * These features primarily help rebill/salvage prediction but are computed
 * for all orders (initials get NULL/0 for most fields — expected).
 */
const { querySql, getDb, saveDb } = require('../db/connection');

const BATCH_SIZE = 5000;
const DAY_MS = 24 * 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Compute subscription features for tx_features rows missing them.
 * @param {number|null} clientId — if null, processes all clients
 * @returns {number} rows updated
 */
function computeSubscriptionFeatures(clientId = null) {
  const clientFilter = clientId ? `AND tf.client_id = ${clientId}` : '';

  // Load tx_features rows that need subscription features
  const pending = querySql(`
    SELECT tf.id as tf_id, tf.order_id, tf.client_id, tf.sticky_order_id,
           tf.amount, tf.acquisition_date, tf.tx_class
    FROM tx_features tf
    WHERE tf.consecutive_approvals IS NULL
      ${clientFilter}
  `);

  if (pending.length === 0) return 0;

  const clientIds = [...new Set(pending.map(r => r.client_id))];
  console.log(`[SubFeatures] Computing for ${pending.length.toLocaleString()} rows across ${clientIds.length} client(s)...`);

  // Pre-load customer journeys for all relevant clients
  const journeyMap = _buildJourneyMap(clientIds);
  const initialMap = _buildInitialMap(clientIds);

  const db = getDb();
  const updateStmt = db.prepare(`
    UPDATE tx_features SET
      consecutive_approvals = ?,
      days_since_last_charge = ?,
      days_since_initial = ?,
      lifetime_charges = ?,
      lifetime_revenue = ?,
      initial_amount = ?,
      amount_ratio = ?,
      prior_declines_in_cycle = ?,
      feature_version = 3
    WHERE id = ?
  `);

  // Pre-load order → customer/product_group mapping in bulk
  const orderIds = pending.map(r => r.order_id);
  const orderInfoMap = _buildOrderInfoMap(orderIds);

  // For each pending row, compute features from the customer's journey
  const updates = [];
  for (const row of pending) {
    const features = _computeForOrder(row, journeyMap, initialMap, orderInfoMap);
    updates.push([
      features.consecutive_approvals,
      features.days_since_last_charge,
      features.days_since_initial,
      features.lifetime_charges,
      features.lifetime_revenue,
      features.initial_amount,
      features.amount_ratio,
      features.prior_declines_in_cycle,
      row.tf_id,
    ]);
  }

  // Batch write
  let updated = 0;
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
// Internal
// ---------------------------------------------------------------------------

/**
 * Build Map<"clientId|customerId|productGroupId", OrderRow[]> sorted by date.
 * Contains ALL terminal orders for customer journeys.
 */
function _buildJourneyMap(clientIds) {
  const placeholders = clientIds.join(',');
  const rows = querySql(`
    SELECT id, order_id, client_id, customer_id, product_group_id,
           order_status, order_total, acquisition_date,
           derived_cycle, derived_attempt, is_cascaded
    FROM orders
    WHERE client_id IN (${placeholders})
      AND order_status IN (2, 6, 7, 8)
      AND is_test = 0 AND is_internal_test = 0
      AND customer_id IS NOT NULL
      AND product_group_id IS NOT NULL
    ORDER BY customer_id, product_group_id, acquisition_date ASC, order_id ASC
  `);

  const m = new Map();
  for (const r of rows) {
    const key = `${r.client_id}|${r.customer_id}|${r.product_group_id}`;
    if (!m.has(key)) m.set(key, []);
    r._ts = r.acquisition_date ? new Date(r.acquisition_date).getTime() : 0;
    r._approved = [2, 6, 8].includes(r.order_status);
    m.get(key).push(r);
  }

  return m;
}

/**
 * Build Map<"clientId|customerId", { amount, date_ts }> — customer's first approved initial.
 */
function _buildInitialMap(clientIds) {
  const placeholders = clientIds.join(',');
  const rows = querySql(`
    SELECT client_id, customer_id, order_total, acquisition_date
    FROM orders
    WHERE client_id IN (${placeholders})
      AND derived_product_role = 'main_initial'
      AND order_status IN (2, 6, 8)
      AND is_test = 0 AND is_internal_test = 0
      AND customer_id IS NOT NULL
    ORDER BY acquisition_date ASC, order_id ASC
  `);

  const m = new Map();
  for (const r of rows) {
    const key = `${r.client_id}|${r.customer_id}`;
    if (!m.has(key)) {
      m.set(key, {
        amount: r.order_total,
        date_ts: r.acquisition_date ? new Date(r.acquisition_date).getTime() : 0,
      });
    }
  }
  return m;
}

/**
 * Pre-load order_id → {customer_id, product_group_id} mapping in bulk.
 */
function _buildOrderInfoMap(orderIds) {
  const m = new Map();
  // Process in chunks to avoid SQLite variable limit
  const CHUNK = 500;
  for (let i = 0; i < orderIds.length; i += CHUNK) {
    const chunk = orderIds.slice(i, i + CHUNK);
    const placeholders = chunk.map(() => '?').join(',');
    const rows = querySql(
      `SELECT id, customer_id, product_group_id FROM orders WHERE id IN (${placeholders})`,
      chunk
    );
    for (const r of rows) m.set(r.id, r);
  }
  return m;
}

/**
 * Compute subscription features for a single tx_features row.
 */
function _computeForOrder(row, journeyMap, initialMap, orderInfoMap) {
  const orderInfo = orderInfoMap.get(row.order_id);

  if (!orderInfo || !orderInfo.customer_id || !orderInfo.product_group_id) {
    return _emptyFeatures();
  }

  const journeyKey = `${row.client_id}|${orderInfo.customer_id}|${orderInfo.product_group_id}`;
  const journey = journeyMap.get(journeyKey);
  if (!journey) return _emptyFeatures();

  const initialKey = `${row.client_id}|${orderInfo.customer_id}`;
  const initial = initialMap.get(initialKey);

  const orderTs = row.acquisition_date ? new Date(row.acquisition_date).getTime() : 0;

  // Find this order's position in the journey
  // (match by internal order_id since sticky_order_id could be different for rebill attempts)
  const idx = journey.findIndex(j => j.id === row.order_id);

  // All prior orders in the journey (before this one)
  const prior = idx > 0 ? journey.slice(0, idx) : [];

  // --- consecutive_approvals: streak of approvals ending just before this order ---
  let consecutiveApprovals = 0;
  for (let i = prior.length - 1; i >= 0; i--) {
    if (prior[i]._approved) {
      consecutiveApprovals++;
    } else {
      break;
    }
  }

  // --- days_since_last_charge: days since last approved order in this journey ---
  let daysSinceLastCharge = null;
  for (let i = prior.length - 1; i >= 0; i--) {
    if (prior[i]._approved && prior[i]._ts > 0 && orderTs > 0) {
      daysSinceLastCharge = (orderTs - prior[i]._ts) / DAY_MS;
      break;
    }
  }

  // --- days_since_initial: days since customer's first approved initial ---
  let daysSinceInitial = null;
  if (initial && initial.date_ts > 0 && orderTs > 0) {
    daysSinceInitial = (orderTs - initial.date_ts) / DAY_MS;
  }

  // --- lifetime_charges: count of approved orders before this one ---
  const lifetimeCharges = prior.filter(p => p._approved).length;

  // --- lifetime_revenue: total revenue of approved orders before this one ---
  const lifetimeRevenue = prior
    .filter(p => p._approved)
    .reduce((sum, p) => sum + (p.order_total || 0), 0);

  // --- initial_amount: what customer paid on their initial ---
  const initialAmount = initial ? initial.amount : null;

  // --- amount_ratio: this amount / initial amount ---
  let amountRatio = null;
  if (initialAmount && initialAmount > 0 && row.amount) {
    amountRatio = Math.round((row.amount / initialAmount) * 100) / 100;
  }

  // --- prior_declines_in_cycle: declines before this attempt in same cycle ---
  const currentOrder = idx >= 0 ? journey[idx] : null;
  let priorDeclinesInCycle = 0;
  if (currentOrder) {
    const cycle = currentOrder.derived_cycle;
    for (let i = prior.length - 1; i >= 0; i--) {
      if (prior[i].derived_cycle === cycle && !prior[i]._approved) {
        priorDeclinesInCycle++;
      } else if (prior[i].derived_cycle !== cycle) {
        break; // different cycle, stop counting
      }
    }
  }

  return {
    consecutive_approvals: consecutiveApprovals,
    days_since_last_charge: daysSinceLastCharge !== null ? Math.round(daysSinceLastCharge * 10) / 10 : null,
    days_since_initial: daysSinceInitial !== null ? Math.round(daysSinceInitial * 10) / 10 : null,
    lifetime_charges: lifetimeCharges,
    lifetime_revenue: Math.round(lifetimeRevenue * 100) / 100,
    initial_amount: initialAmount,
    amount_ratio: amountRatio,
    prior_declines_in_cycle: priorDeclinesInCycle,
  };
}

function _emptyFeatures() {
  return {
    consecutive_approvals: 0,
    days_since_last_charge: null,
    days_since_initial: null,
    lifetime_charges: 0,
    lifetime_revenue: 0,
    initial_amount: null,
    amount_ratio: null,
    prior_declines_in_cycle: 0,
  };
}

module.exports = { computeSubscriptionFeatures };
