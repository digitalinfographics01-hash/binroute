/**
 * Processor Intelligence — acquisition priority and active health.
 *
 * Computes revenue unlock potential for inactive processors.
 * Shows active processor health (rates + cap utilization).
 * Stored in processor_intelligence table, computed during recompute.
 */
const { querySql, runSql, saveDb } = require('../db/connection');
const { computeAllGroupRates } = require('./weighted-rates');
const { getCapStatus } = require('./cap-tracking');
const { checkProcessorHistory } = require('./new-processor');

// ---------------------------------------------------------------------------
// Compute processor intelligence
// ---------------------------------------------------------------------------

/**
 * Compute and persist processor intelligence for a client.
 *
 * @param {number} clientId
 * @returns {{ acquisitionPriority: Array, activeHealth: Array }}
 */
function computeProcessorIntelligence(clientId) {
  // Get all group rates at L2 (include inactive for historical comparison)
  const groupRates = computeAllGroupRates(clientId, 2, { includeInactive: true });

  // Get avg rebill order value
  const avgOvRow = querySql(
    "SELECT AVG(order_total) AS avg_val FROM orders WHERE client_id = ? AND derived_product_role IN ('main_rebill','upsell_rebill') AND order_status IN (2,6,8) AND is_test = 0 AND is_internal_test = 0",
    [clientId]
  );
  const avgOrderValue = avgOvRow[0]?.avg_val || 70;

  // Collect all processor names from historical data
  const allProcs = querySql(`
    SELECT DISTINCT g.processor_name
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = ? AND g.processor_name IS NOT NULL
      AND o.is_test = 0 AND o.derived_product_role IN ('main_rebill','upsell_rebill')
      AND o.acquisition_date >= date('now', '-180 days')
      AND COALESCE(g.exclude_from_analysis, 0) != 1
  `, [clientId]).map(r => r.processor_name);

  // Get active processor names
  const activeProcs = new Set(
    querySql('SELECT DISTINCT processor_name FROM gateways WHERE client_id = ? AND gateway_active = 1 AND COALESCE(exclude_from_analysis, 0) != 1 AND processor_name IS NOT NULL', [clientId])
      .map(r => r.processor_name)
  );

  // For each inactive processor: compute revenue unlock
  const acquisitionPriority = [];

  for (const proc of allProcs) {
    if (activeProcs.has(proc)) continue; // Skip active processors

    const history = checkProcessorHistory(clientId, proc);
    if (!history.hasHistorical) continue;

    let revenueUnlock = 0;
    let groupsCovered = 0;
    let totalRate = 0;
    let rateCount = 0;

    for (const [, group] of groupRates) {
      const procEntry = group.processors.get(proc);
      if (!procEntry || procEntry.approved_count < 5) continue;

      // Find current best active processor rate for this group
      let bestActiveRate = 0;
      for (const [pName, pEntry] of group.processors) {
        if (activeProcs.has(pName) && pEntry.weighted_rate > bestActiveRate) {
          bestActiveRate = pEntry.weighted_rate;
        }
      }

      // If this inactive processor outperforms current best
      if (procEntry.weighted_rate > bestActiveRate) {
        const liftPp = procEntry.weighted_rate - bestActiveRate;
        const monthlyAtt = Math.round((procEntry.total * 30) / 180);
        const monthlyRev = monthlyAtt * (liftPp / 100) * avgOrderValue;
        revenueUnlock += monthlyRev;
        groupsCovered++;
      }

      totalRate += procEntry.weighted_rate;
      rateCount++;
    }

    if (groupsCovered > 0) {
      acquisitionPriority.push({
        processor_name: proc,
        revenue_unlock_monthly: Math.round(revenueUnlock * 100) / 100,
        groups_covered: groupsCovered,
        avg_approval_rate: rateCount > 0 ? Math.round((totalRate / rateCount) * 100) / 100 : 0,
        data_source: history.hasRecent ? 'recent' : 'historical',
        coverage_pct: Math.round((groupsCovered / groupRates.size) * 10000) / 100,
      });
    }
  }

  // Rank by revenue unlock
  acquisitionPriority.sort((a, b) => b.revenue_unlock_monthly - a.revenue_unlock_monthly);

  // Persist to processor_intelligence table
  runSql('DELETE FROM processor_intelligence WHERE client_id = ?', [clientId]);
  for (let i = 0; i < acquisitionPriority.length; i++) {
    const p = acquisitionPriority[i];
    runSql(`INSERT INTO processor_intelligence
      (client_id, processor_name, revenue_unlock_monthly, coverage_pct, groups_covered,
       avg_approval_rate, data_source, rank_order)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [clientId, p.processor_name, p.revenue_unlock_monthly, p.coverage_pct,
       p.groups_covered, p.avg_approval_rate, p.data_source, i + 1]);
  }

  // Active processor health
  const capStatuses = getCapStatus(clientId);
  const activeHealth = [];

  for (const proc of activeProcs) {
    // Weighted rate across all groups
    let totalWeighted = 0;
    let totalCount = 0;
    let totalApproved = 0;

    for (const [, group] of groupRates) {
      const pe = group.processors.get(proc);
      if (pe) {
        totalWeighted += pe.weighted_rate * pe.total;
        totalCount += pe.total;
        totalApproved += pe.approved_count;
      }
    }

    const avgRate = totalCount > 0 ? Math.round((totalWeighted / totalCount) * 100) / 100 : 0;

    // Cap status per MID in this processor
    const procMids = capStatuses.filter(s => s.processor_name === proc);
    const totalCap = procMids.reduce((s, m) => s + (m.cap || 0), 0);
    const totalSales = procMids.reduce((s, m) => s + (m.sales || 0), 0);
    const capPct = totalCap > 0 ? Math.round((totalSales / totalCap) * 10000) / 100 : 0;

    activeHealth.push({
      processor_name: proc,
      avg_weighted_rate: avgRate,
      total_approved: totalApproved,
      total_cap: totalCap,
      total_sales: totalSales,
      cap_pct: capPct,
      mid_count: procMids.length,
      mids: procMids,
    });
  }

  saveDb();

  return { acquisitionPriority, activeHealth };
}

/**
 * Get stored processor intelligence from DB.
 *
 * @param {number} clientId
 * @returns {Array}
 */
function getProcessorIntelligence(clientId) {
  return querySql(
    'SELECT * FROM processor_intelligence WHERE client_id = ? ORDER BY rank_order',
    [clientId]
  );
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  computeProcessorIntelligence,
  getProcessorIntelligence,
};
