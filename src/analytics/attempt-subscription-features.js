/**
 * Attempt Subscription Features — Journey-based computation for transaction_attempts.
 *
 * Computes 8 rebill-specific features by traversing each customer's
 * subscription journey (per product group).
 *
 * Features:
 *   consecutive_approvals     — streak of approved charges before this one
 *   days_since_last_charge    — days since last approved charge (same product group)
 *   days_since_initial        — days since customer's first approved initial
 *   lifetime_charges          — total approved charges before this one
 *   lifetime_revenue          — total approved revenue before this one
 *   initial_amount            — what customer paid on initial order
 *   amount_ratio              — this amount / initial amount
 *   prior_declines_in_cycle   — declines in current cycle before this attempt
 *
 * Sets feature_version = 3 after completion.
 */
const { querySql, getDb, checkpointWal } = require('../db/connection');

const BATCH_SIZE = 5000;
const DAY_MS = 24 * 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Compute subscription features for a client's transaction_attempts.
 * Only processes rows with feature_version = 2 (velocity done, subscription pending).
 */
function computeAttemptSubscription(clientId) {
  const db = getDb();

  const pending = db.prepare(
    `SELECT COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? AND feature_version = 2`
  ).get(clientId).cnt;

  if (pending === 0) {
    console.log(`[AttemptSub] Client ${clientId}: no rows need subscription features`);
    return 0;
  }

  console.log(`[AttemptSub] Client ${clientId}: ${pending.toLocaleString()} rows need subscription features`);

  // Build journey map from orders table (authoritative source for full customer history)
  console.log(`[AttemptSub] Building journey map...`);
  const journeyMap = _buildJourneyMap(clientId);
  console.log(`[AttemptSub] ${journeyMap.size} journeys loaded`);

  // Build initial map (first approved initial per customer)
  const initialMap = _buildInitialMap(clientId);
  console.log(`[AttemptSub] ${initialMap.size} customer initials loaded`);

  // Load pending attempt rows
  const rows = db.prepare(
    `SELECT id, order_id, customer_id, product_group_id, order_total,
            acquisition_date, derived_cycle, derived_attempt, model_target
     FROM transaction_attempts
     WHERE client_id = ? AND feature_version = 2
     ORDER BY customer_id, product_group_id, acquisition_date ASC, id ASC`
  ).all(clientId);

  console.log(`[AttemptSub] Computing features for ${rows.length.toLocaleString()} rows...`);

  const updateStmt = db.prepare(
    `UPDATE transaction_attempts SET
       consecutive_approvals = ?,
       days_since_last_charge = ?,
       days_since_initial = ?,
       lifetime_charges = ?,
       lifetime_revenue = ?,
       initial_amount = ?,
       amount_ratio = ?,
       prior_declines_in_cycle = ?,
       feature_version = 3
     WHERE id = ?`
  );

  const updates = [];
  for (const row of rows) {
    const features = _computeForAttempt(row, clientId, journeyMap, initialMap);
    updates.push([
      features.consecutive_approvals,
      features.days_since_last_charge,
      features.days_since_initial,
      features.lifetime_charges,
      features.lifetime_revenue,
      features.initial_amount,
      features.amount_ratio,
      features.prior_declines_in_cycle,
      row.id,
    ]);
  }

  // Batch write
  let updated = 0;
  for (let b = 0; b < updates.length; b += BATCH_SIZE) {
    const batch = updates.slice(b, b + BATCH_SIZE);
    db.transaction((batchRows) => {
      for (const params of batchRows) {
        updateStmt.run(...params);
        updated++;
      }
    })(batch);

    if ((b + BATCH_SIZE) % 50000 < BATCH_SIZE) {
      checkpointWal();
    }
  }

  console.log(`[AttemptSub] Done — ${updated.toLocaleString()} rows updated to feature_version=3`);
  return updated;
}

// ---------------------------------------------------------------------------
// Journey Maps (built from orders table for complete history)
// ---------------------------------------------------------------------------

/**
 * Build journey map from orders — all qualifying orders per customer+product_group.
 * Uses orders table as authoritative source (not transaction_attempts) because
 * the journey includes all order types, not just the ones being scored.
 */
function _buildJourneyMap(clientId) {
  const rows = querySql(`
    SELECT id, order_id, customer_id, product_group_id,
           order_status, order_total, acquisition_date,
           derived_cycle, derived_attempt, is_cascaded
    FROM orders
    WHERE client_id = ?
      AND order_status IN (2, 6, 7, 8)
      AND is_test = 0 AND is_internal_test = 0
      AND customer_id IS NOT NULL
      AND product_group_id IS NOT NULL
    ORDER BY customer_id, product_group_id, acquisition_date ASC, order_id ASC
  `, [clientId]);

  const m = new Map();
  for (const r of rows) {
    const key = `${r.customer_id}|${r.product_group_id}`;
    if (!m.has(key)) m.set(key, []);
    r._ts = r.acquisition_date ? new Date(r.acquisition_date).getTime() : 0;
    r._approved = [2, 6, 8].includes(r.order_status);
    m.get(key).push(r);
  }

  return m;
}

/**
 * Build initial map — first approved main_initial per customer.
 */
function _buildInitialMap(clientId) {
  const rows = querySql(`
    SELECT customer_id, order_total, acquisition_date
    FROM orders
    WHERE client_id = ?
      AND derived_product_role = 'main_initial'
      AND order_status IN (2, 6, 8)
      AND is_test = 0 AND is_internal_test = 0
      AND customer_id IS NOT NULL
    ORDER BY acquisition_date ASC, order_id ASC
  `, [clientId]);

  const m = new Map();
  for (const r of rows) {
    if (!m.has(r.customer_id)) {
      m.set(r.customer_id, {
        amount: r.order_total,
        date_ts: r.acquisition_date ? new Date(r.acquisition_date).getTime() : 0,
      });
    }
  }
  return m;
}

// ---------------------------------------------------------------------------
// Feature Computation
// ---------------------------------------------------------------------------

function _computeForAttempt(row, clientId, journeyMap, initialMap) {
  if (!row.customer_id || !row.product_group_id) return _emptyFeatures();

  const journeyKey = `${row.customer_id}|${row.product_group_id}`;
  const journey = journeyMap.get(journeyKey);
  if (!journey) return _emptyFeatures();

  const initial = initialMap.get(row.customer_id);
  const orderTs = row.acquisition_date ? new Date(row.acquisition_date).getTime() : 0;

  // Find this attempt's parent order in the journey
  const idx = journey.findIndex(j => j.id === row.order_id);
  const prior = idx > 0 ? journey.slice(0, idx) : [];

  // consecutive_approvals: streak ending just before this order
  let consecutiveApprovals = 0;
  for (let i = prior.length - 1; i >= 0; i--) {
    if (prior[i]._approved) consecutiveApprovals++;
    else break;
  }

  // days_since_last_charge
  let daysSinceLastCharge = null;
  for (let i = prior.length - 1; i >= 0; i--) {
    if (prior[i]._approved && prior[i]._ts > 0 && orderTs > 0) {
      daysSinceLastCharge = Math.round(((orderTs - prior[i]._ts) / DAY_MS) * 10) / 10;
      break;
    }
  }

  // days_since_initial
  let daysSinceInitial = null;
  if (initial && initial.date_ts > 0 && orderTs > 0) {
    daysSinceInitial = Math.round(((orderTs - initial.date_ts) / DAY_MS) * 10) / 10;
  }

  // lifetime_charges + lifetime_revenue
  let lifetimeCharges = 0;
  let lifetimeRevenue = 0;
  for (const p of prior) {
    if (p._approved) {
      lifetimeCharges++;
      lifetimeRevenue += p.order_total || 0;
    }
  }

  // initial_amount + amount_ratio
  const initialAmount = initial ? initial.amount : null;
  let amountRatio = null;
  if (initialAmount && initialAmount > 0 && row.order_total) {
    amountRatio = Math.round((row.order_total / initialAmount) * 100) / 100;
  }

  // prior_declines_in_cycle
  let priorDeclinesInCycle = 0;
  const currentOrder = idx >= 0 ? journey[idx] : null;
  if (currentOrder) {
    const cycle = currentOrder.derived_cycle;
    for (let i = prior.length - 1; i >= 0; i--) {
      if (prior[i].derived_cycle === cycle && !prior[i]._approved) {
        priorDeclinesInCycle++;
      } else if (prior[i].derived_cycle !== cycle) {
        break;
      }
    }
  }

  return {
    consecutive_approvals: consecutiveApprovals,
    days_since_last_charge: daysSinceLastCharge,
    days_since_initial: daysSinceInitial,
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

module.exports = { computeAttemptSubscription };
