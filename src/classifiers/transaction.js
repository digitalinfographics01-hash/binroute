const { querySql, runSql, saveDb, transaction } = require('../db/connection');
const { APPROVED_STATUSES } = require('../analytics/engine');

/**
 * Transaction type classifier.
 * Assigns each order one of:
 *   cp_initial, cp_upsell, trial_conversion,
 *   recurring_rebill, simulated_cp_rebill, salvage_attempt
 *
 * Also computes cycle_number and attempt_number.
 */

/**
 * Classify all unclassified orders for a client.
 * Must be run AFTER orders are ingested and customers have order history.
 */
function classifyTransactions(clientId) {
  console.log(`[TxClassifier] Classifying transactions for client ${clientId}...`);

  // Load config rules for this client
  const cpSimRules = querySql(
    'SELECT campaign_id, product_id FROM tx_type_rules WHERE client_id = ? AND is_cp_simulation = 1',
    [clientId]
  );
  const typeRules = querySql(
    'SELECT campaign_id, product_id, assigned_type FROM tx_type_rules WHERE client_id = ? AND is_cp_simulation = 0',
    [clientId]
  );

  // Build lookup sets
  const cpSimCampaigns = new Set(cpSimRules.filter(r => r.campaign_id && !r.product_id).map(r => r.campaign_id));
  const cpSimProducts = new Set(cpSimRules.filter(r => r.product_id).map(r => `${r.campaign_id || '*'}_${r.product_id}`));
  const typeOverrides = new Map();
  for (const rule of typeRules) {
    const key = `${rule.campaign_id || '*'}_${rule.product_id || '*'}`;
    typeOverrides.set(key, rule.assigned_type);
  }

  // Get all unclassified orders, grouped by customer
  const orders = querySql(`
    SELECT o.id, o.order_id, o.customer_id, o.campaign_id, o.order_status,
           o.acquisition_date, o.products_json, o.is_cascaded, o.gateway_id,
           o.decline_reason
    FROM orders o
    WHERE o.client_id = ? AND o.transaction_type IS NULL
      AND o.is_anonymous_decline = 0
    ORDER BY o.customer_id, o.acquisition_date ASC
  `, [clientId]);

  if (orders.length === 0) {
    console.log('[TxClassifier] No unclassified orders found.');
    return { classified: 0 };
  }

  // Pre-load all customer order histories for cycle counting
  const customerIds = [...new Set(orders.map(o => o.customer_id).filter(Boolean))];
  const customerHistories = new Map();

  for (const custId of customerIds) {
    const history = querySql(`
      SELECT order_id, order_status, acquisition_date, campaign_id
      FROM orders
      WHERE client_id = ? AND customer_id = ?
      ORDER BY acquisition_date ASC
    `, [clientId, custId]);
    customerHistories.set(custId, history);
  }

  let classified = 0;

  transaction(() => {
    for (const order of orders) {
      const result = classifySingleOrder(order, customerHistories, cpSimCampaigns, cpSimProducts, typeOverrides);

      runSql(`
        UPDATE orders SET
          transaction_type = ?,
          cycle_number = ?,
          attempt_number = ?
        WHERE id = ?
      `, [result.type, result.cycleNumber, result.attemptNumber, order.id]);

      classified++;
    }
  });

  console.log(`[TxClassifier] Classified ${classified} orders.`);
  return { classified };
}

/**
 * Classify a single order based on context.
 */
function classifySingleOrder(order, customerHistories, cpSimCampaigns, cpSimProducts, typeOverrides) {
  // 1. Check explicit type overrides first
  const overrideKey1 = `${order.campaign_id}_*`;
  const overrideKey2 = `*_*`;
  let productIds = [];
  if (order.products_json) {
    try {
      const products = JSON.parse(order.products_json);
      if (Array.isArray(products)) {
        productIds = Object.keys(products).length > 0 ? Object.keys(products) : [];
        // products might be {product_id: {…}} or [{product_id: …}]
        if (typeof products === 'object' && !Array.isArray(products)) {
          productIds = Object.keys(products);
        }
      }
    } catch { /* ignore */ }
  }

  // Check product-level overrides
  for (const pid of productIds) {
    const key = `${order.campaign_id}_${pid}`;
    if (typeOverrides.has(key)) {
      return { type: typeOverrides.get(key), cycleNumber: 0, attemptNumber: 1 };
    }
  }
  if (typeOverrides.has(overrideKey1)) {
    return { type: typeOverrides.get(overrideKey1), cycleNumber: 0, attemptNumber: 1 };
  }

  // 2. Check if this is a CP simulation (third party CP tool)
  const isCpSim = cpSimCampaigns.has(order.campaign_id) ||
    productIds.some(pid => cpSimProducts.has(`${order.campaign_id}_${pid}`) || cpSimProducts.has(`*_${pid}`));

  // 3. Get customer order history
  const history = customerHistories.get(order.customer_id) || [];

  // Count prior successful orders for this customer (before this order's date)
  const priorSuccessful = history.filter(h =>
    h.order_id !== order.order_id &&
    APPROVED_STATUSES.includes(parseInt(h.order_status, 10)) &&
    h.acquisition_date <= order.acquisition_date
  );

  const cycleNumber = priorSuccessful.length;

  // Count prior failed attempts for this cycle (same customer, after last success, before this order)
  const lastSuccess = priorSuccessful.length > 0 ? priorSuccessful[priorSuccessful.length - 1] : null;
  const priorFailed = history.filter(h =>
    h.order_id !== order.order_id &&
    parseInt(h.order_status, 10) === 7 &&
    h.acquisition_date <= order.acquisition_date &&
    (!lastSuccess || h.acquisition_date > lastSuccess.acquisition_date)
  );
  const attemptNumber = priorFailed.length + 1;

  // 4. Determine transaction type
  let type;

  if (isCpSim && cycleNumber > 0) {
    // Rebill processed through third party CP tool — appears as initial but is really a rebill
    type = 'simulated_cp_rebill';
  } else if (cycleNumber === 0) {
    // First order for this customer
    // Check if this is an upsell (same session — within minutes of another order)
    const isUpsell = history.some(h =>
      h.order_id !== order.order_id &&
      APPROVED_STATUSES.includes(parseInt(h.order_status, 10)) &&
      h.acquisition_date === order.acquisition_date && // same timestamp = same session
      h.campaign_id !== order.campaign_id // different campaign = upsell
    );

    if (isUpsell) {
      type = 'cp_upsell';
    } else {
      type = 'cp_initial';
    }
  } else if (cycleNumber === 1) {
    // First rebill
    type = 'trial_conversion';
  } else {
    // Cycle 2+
    // Check if this is a salvage attempt (retry on a failed rebill)
    if (attemptNumber > 1) {
      type = 'salvage_attempt';
    } else {
      type = 'recurring_rebill';
    }
  }

  return { type, cycleNumber, attemptNumber };
}

/**
 * Get transaction type distribution for a client.
 */
function getTransactionTypeSummary(clientId) {
  return querySql(`
    SELECT
      transaction_type,
      COUNT(*) as count,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      SUM(CASE WHEN order_status = 7 THEN 1 ELSE 0 END) as declined,
      ROUND(
        100.0 * SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) /
        NULLIF(COUNT(*), 0), 2
      ) as approval_rate
    FROM orders
    WHERE client_id = ? AND transaction_type IS NOT NULL
    GROUP BY transaction_type
    ORDER BY count DESC
  `, [clientId]);
}

module.exports = {
  classifyTransactions,
  classifySingleOrder,
  getTransactionTypeSummary,
};
