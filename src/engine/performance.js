const { querySql, runSql, saveDb, transaction } = require('../db/connection');
const { calculateBinTiers } = require('./bin-tiering');

/**
 * Performance Matrix Engine.
 * Calculates approval rates per BIN+MID with recency weighting.
 * Excludes issuer-side declines from routing analysis (per spec).
 */

/**
 * Recency weight function.
 * Orders within last 7 days: weight 3.0
 * 8–30 days: weight 2.0
 * 31–60 days: weight 1.5
 * 61–90 days: weight 1.0
 */
function getRecencyWeight(daysAgo) {
  if (daysAgo <= 7) return 3.0;
  if (daysAgo <= 30) return 2.0;
  if (daysAgo <= 60) return 1.5;
  return 1.0;
}

/**
 * Build the full performance matrix for a client.
 * Materializes results into bin_performance table.
 */
function buildPerformanceMatrix(clientId, windowDays = 90) {
  console.log(`[Performance] Building performance matrix for client ${clientId}...`);

  // Step 1: Calculate BIN tiers
  const tierData = calculateBinTiers(clientId, windowDays);
  const binTierMap = new Map(tierData.bins.map(b => [b.bin, b.tier]));

  // Step 2: Get all orders within window, with day age for recency weighting
  // Exclude issuer-side declines from routing analysis
  const orders = querySql(`
    SELECT
      cc_first_6 as bin,
      cc_type,
      gateway_id,
      order_status,
      decline_category,
      transaction_type,
      CAST(julianday('now') - julianday(acquisition_date) AS INTEGER) as days_ago
    FROM orders
    WHERE client_id = ?
      AND is_test = 0
      AND cc_first_6 IS NOT NULL AND cc_first_6 != ''
      AND gateway_id IS NOT NULL
      AND acquisition_date >= date('now', '-' || ? || ' days')
  `, [clientId, windowDays]);

  if (orders.length === 0) {
    console.log('[Performance] No order data found.');
    return { entries: 0 };
  }

  // Step 3: Aggregate by BIN + gateway + transaction_type
  const matrix = new Map();

  for (const order of orders) {
    const key = `${order.bin}|${order.gateway_id}|${order.transaction_type || 'all'}`;
    if (!matrix.has(key)) {
      matrix.set(key, {
        bin: order.bin,
        cc_type: order.cc_type,
        gateway_id: order.gateway_id,
        transaction_type: order.transaction_type || null,
        total: 0,
        approved: 0,
        declined: 0,
        issuer_declines: 0,
        processor_declines: 0,
        soft_declines: 0,
        weighted_approved: 0,
        weighted_total: 0,
        // For routing analysis: exclude issuer declines
        routing_total: 0,
        routing_approved: 0,
        routing_weighted_total: 0,
        routing_weighted_approved: 0,
      });
    }

    const entry = matrix.get(key);
    const weight = getRecencyWeight(order.days_ago);
    const status = parseInt(order.order_status, 10);
    const isApproved = status === 2 || status === 6 || status === 8;
    const isDeclined = status === 7;
    const isIssuerDecline = order.decline_category === 'issuer';

    entry.total++;
    entry.weighted_total += weight;

    if (isApproved) {
      entry.approved++;
      entry.weighted_approved += weight;
      entry.routing_total++;
      entry.routing_approved++;
      entry.routing_weighted_total += weight;
      entry.routing_weighted_approved += weight;
    } else if (isDeclined) {
      entry.declined++;
      if (isIssuerDecline) {
        entry.issuer_declines++;
        // Issuer declines EXCLUDED from routing analysis
      } else {
        entry.routing_total++;
        entry.routing_weighted_total += weight;
        if (order.decline_category === 'processor') {
          entry.processor_declines++;
        } else if (order.decline_category === 'soft') {
          entry.soft_declines++;
        }
      }
    }
  }

  // Step 4: Calculate rates and materialize to database
  const today = new Date().toISOString().split('T')[0];
  const periodStart = new Date(Date.now() - windowDays * 86400000).toISOString().split('T')[0];

  // Get MCC codes from gateway config
  const gateways = querySql('SELECT gateway_id, mcc_code FROM gateways WHERE client_id = ?', [clientId]);
  const mccMap = new Map(gateways.map(g => [g.gateway_id, g.mcc_code]));

  let entries = 0;

  transaction(() => {
    // Clear old entries for this client and period
    runSql('DELETE FROM bin_performance WHERE client_id = ? AND period_end = ?', [clientId, today]);

    for (const entry of matrix.values()) {
      const approvalRate = entry.total > 0 ? (entry.approved / entry.total * 100) : 0;
      const weightedRate = entry.weighted_total > 0 ?
        (entry.weighted_approved / entry.weighted_total * 100) : 0;
      const tier = binTierMap.get(entry.bin) || 3;

      runSql(`
        INSERT INTO bin_performance (
          client_id, bin, cc_type, gateway_id, mcc_code, transaction_type,
          period_start, period_end,
          total_transactions, approved_count, declined_count,
          issuer_declines, processor_declines, soft_declines,
          approval_rate, weighted_approval_rate, tier
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        clientId, entry.bin, entry.cc_type, entry.gateway_id,
        mccMap.get(entry.gateway_id) || null, entry.transaction_type,
        periodStart, today,
        entry.total, entry.approved, entry.declined,
        entry.issuer_declines, entry.processor_declines, entry.soft_declines,
        Math.round(approvalRate * 100) / 100,
        Math.round(weightedRate * 100) / 100,
        tier,
      ]);

      entries++;
    }
  });

  console.log(`[Performance] Built ${entries} performance entries.`);
  return { entries, totalOrders: orders.length, uniqueBins: binTierMap.size };
}

/**
 * Get the performance matrix for display (BIN × MID grid).
 * Returns top BINs with their per-MID approval rates.
 */
function getPerformanceGrid(clientId, options = {}) {
  const tier = options.tier || null;
  const txType = options.transactionType || null;
  const limit = options.limit || 50;

  let where = 'WHERE client_id = ?';
  const params = [clientId];

  if (tier) {
    where += ' AND tier = ?';
    params.push(tier);
  }
  if (txType) {
    where += ' AND transaction_type = ?';
    params.push(txType);
  }

  // Get latest period
  const latest = querySql(`
    SELECT MAX(period_end) as latest FROM bin_performance WHERE client_id = ?
  `, [clientId]);
  const latestDate = latest[0]?.latest;
  if (!latestDate) return { bins: [], gateways: [], grid: [] };

  where += ' AND period_end = ?';
  params.push(latestDate);

  const rows = querySql(`
    SELECT bin, cc_type, gateway_id, mcc_code, transaction_type,
           total_transactions, approved_count, declined_count,
           issuer_declines, processor_declines, soft_declines,
           approval_rate, weighted_approval_rate, tier
    FROM bin_performance
    ${where}
    ORDER BY total_transactions DESC
    LIMIT ?
  `, [...params, limit]);

  return rows;
}

/**
 * Get MID-level performance summary.
 */
function getMidPerformanceSummary(clientId) {
  return querySql(`
    SELECT
      g.gateway_id,
      g.gateway_descriptor,
      g.gateway_alias,
      g.lifecycle_state,
      g.processor_name,
      g.bank_name,
      g.mcc_code,
      g.global_monthly_cap,
      g.monthly_sales,
      SUM(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 ELSE 0 END) as total_orders,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      SUM(CASE WHEN o.order_status = 7 THEN 1 ELSE 0 END) as declined,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 ELSE 0 END), 0), 2) as approval_rate
    FROM gateways g
    LEFT JOIN orders o ON o.client_id = g.client_id AND o.gateway_id = g.gateway_id
      AND o.acquisition_date >= date('now', '-90 days')
    WHERE g.client_id = ?
    GROUP BY g.gateway_id
    ORDER BY total_orders DESC
  `, [clientId]);
}

module.exports = {
  buildPerformanceMatrix,
  getPerformanceGrid,
  getMidPerformanceSummary,
  getRecencyWeight,
};
