/**
 * Lift Opportunities — Revenue uplift analysis from routing optimization.
 *
 * For each BIN where a better gateway exists, calculates the expected
 * approval rate lift, monthly/annual revenue opportunity, and provides
 * summary breakdowns by tx_type group.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql, queryOneSql } = require('../db/connection');
const {
  CLEAN_FILTER, CASCADE_WHERE, TX_TYPE_GROUPS, getCachedOrCompute,
  formatGatewayName, daysAgoFilter, stddev,
} = require('./engine');

/**
 * Compute lift opportunities for a client.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {number} [opts.minSample]  - Minimum attempts per gateway to consider (default 20)
 * @param {number} [opts.days]       - Lookback window in days (default 90)
 * @returns {{ summary: object, opportunities: Array<object> }}
 */
function computeLiftOpportunities(clientId, opts = {}) {
  const minSample = opts.minSample ?? 20;
  const days      = opts.days ?? 180;

  const cacheKey = `${minSample}:${days}`;

  return getCachedOrCompute(clientId, 'lift-opportunities', cacheKey, () => {
    return _computeLiftOpportunities(clientId, minSample, days);
  });
}

function _computeLiftOpportunities(clientId, minSample, days) {
  // -----------------------------------------------------------------------
  // 1. Load gateway metadata (for filtering closed gateways)
  // -----------------------------------------------------------------------
  const gatewayMeta = querySql(`
    SELECT gateway_id, gateway_alias, bank_name, processor_name,
           mcc_code, lifecycle_state, gateway_active, exclude_from_analysis
    FROM gateways
    WHERE client_id = ?
  `, [clientId]);

  const gwMetaMap = new Map(gatewayMeta.map(g => [g.gateway_id, g]));

  const activeGwIds = new Set();
  for (const g of gatewayMeta) {
    if (g.lifecycle_state !== 'closed' && g.gateway_active !== 0 && g.exclude_from_analysis !== 1) {
      activeGwIds.add(g.gateway_id);
    }
  }

  // -----------------------------------------------------------------------
  // 2. Build product role group expression (uses derived_product_role to include anonymous declines)
  // -----------------------------------------------------------------------
  const roleGroupExpr = `CASE
    WHEN o.derived_product_role IN ('main_initial') THEN 'INITIALS'
    WHEN o.derived_product_role IN ('upsell_initial') THEN 'UPSELLS'
    WHEN o.derived_product_role IN ('main_rebill', 'upsell_rebill') THEN 'REBILLS'
    WHEN o.derived_product_role IN ('straight_sale') THEN 'STRAIGHT_SALES'
    ELSE 'OTHER'
  END`;
  const txGroupExpr = roleGroupExpr;

  // -----------------------------------------------------------------------
  // 3. Per-BIN, per-gateway, per-tx_group performance
  // -----------------------------------------------------------------------
  const perfRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      ${txGroupExpr} AS tx_group,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      ROUND(AVG(CASE WHEN o.order_total > 0 THEN o.order_total END), 2) AS avg_order_total,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') THEN 1 END) AS last_30d_total,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') AND o.order_status IN (2,6,8) THEN 1 END) AS last_30d_approved,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') AND o.order_status = 7 THEN 1 END) AS last_30d_declined
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      AND o.order_status IN (2,6,7,8)
      AND o.gateway_id IS NOT NULL
      AND o.derived_product_role IS NOT NULL
    GROUP BY o.cc_first_6, o.gateway_id, tx_group
    ORDER BY o.cc_first_6, tx_group, total DESC
  `, [clientId]);

  // Cascade correction: attribute declines to original_gateway_id
  const cascPerf = querySql(`
    SELECT o.cc_first_6 AS bin, o.original_gateway_id AS gateway_id,
      ${txGroupExpr} AS tx_group,
      COUNT(*) AS casc_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      AND o.order_status IN (2,6,7,8) AND o.original_gateway_id IS NOT NULL
    GROUP BY o.cc_first_6, o.original_gateway_id, tx_group
  `, [clientId]);

  for (const cr of cascPerf) {
    const match = perfRows.find(r => r.bin === cr.bin && r.gateway_id === cr.gateway_id && r.tx_group === cr.tx_group);
    if (match) { match.total += cr.casc_declines; match.declined += cr.casc_declines; }
    else { perfRows.push({ bin: cr.bin, gateway_id: cr.gateway_id, tx_group: cr.tx_group, total: cr.casc_declines, approved: 0, declined: cr.casc_declines, avg_order_total: 0, last_30d_total: 0, last_30d_approved: 0, last_30d_declined: 0 }); }
  }

  if (perfRows.length === 0) {
    return { summary: _emptySummary(), opportunities: [] };
  }

  // -----------------------------------------------------------------------
  // 4. BIN metadata
  // -----------------------------------------------------------------------
  const allBinsSet = new Set(perfRows.map(r => r.bin));
  const allBins = [...allBinsSet];
  const binMetaMap = new Map();
  if (allBins.length > 0) {
    const metaRows = querySql(`
      SELECT bin, issuer_bank, card_brand, card_type, card_level, is_prepaid
      FROM bin_lookup
      WHERE bin IN (${allBins.map(() => '?').join(',')})
    `, allBins);
    for (const r of metaRows) {
      binMetaMap.set(r.bin, r);
    }
  }

  // -----------------------------------------------------------------------
  // 5. Organize: bin -> txGroup -> [gwPerf]
  // -----------------------------------------------------------------------
  const binData = new Map();
  for (const row of perfRows) {
    if (!binData.has(row.bin)) binData.set(row.bin, new Map());
    const txMap = binData.get(row.bin);
    if (!txMap.has(row.tx_group)) txMap.set(row.tx_group, []);
    txMap.get(row.tx_group).push({
      gateway_id:      row.gateway_id,
      total:           row.total,
      approved:        row.approved,
      declined:        row.declined,
      rate:            row.total > 0 ? (row.approved / row.total) * 100 : 0,
      avg_order_total: row.avg_order_total || 0,
      last_30d_total:  row.last_30d_total,
      last_30d_approved: row.last_30d_approved,
      last_30d_declined: row.last_30d_declined,
    });
  }

  // -----------------------------------------------------------------------
  // 6. Find lift opportunities: BIN + txGroup where a better gateway exists
  // -----------------------------------------------------------------------
  const opportunities = [];

  for (const [bin, txMap] of binData) {
    for (const [txGroup, gws] of txMap) {
      if (txGroup === 'OTHER') continue;

      // Filter to active gateways with sufficient sample
      const qualified = gws.filter(g =>
        activeGwIds.has(g.gateway_id) && g.total >= minSample
      );
      if (qualified.length < 2) continue;

      // Compare gateways — group by MCC if shared, otherwise compare all
      const mccGroups = new Map();
      for (const gw of qualified) {
        const meta = gwMetaMap.get(gw.gateway_id);
        const mcc = (meta && meta.mcc_code) || 'unknown';
        if (!mccGroups.has(mcc)) mccGroups.set(mcc, []);
        mccGroups.get(mcc).push(gw);
      }
      const hasSharedMcc = [...mccGroups.values()].some(g => g.length >= 2);
      if (!hasSharedMcc) {
        mccGroups.clear();
        mccGroups.set('mixed', qualified);
      }

      for (const [, mccGws] of mccGroups) {
        if (mccGws.length < 2) continue;

        // Current = most volume, Best = highest rate
        const currentGw = mccGws.reduce((a, c) => c.total > a.total ? c : a, mccGws[0]);
        const bestGw = mccGws.reduce((a, c) => c.rate > a.rate ? c : a, mccGws[0]);

        if (bestGw.gateway_id === currentGw.gateway_id) continue;

        const currentRate = Math.round(currentGw.rate * 100) / 100;
        const bestRate = Math.round(bestGw.rate * 100) / 100;
        const liftPp = Math.round((bestRate - currentRate) * 100) / 100;

        if (liftPp <= 0) continue;

        // Monthly attempts from last 30 days data
        const monthlyAttempts = currentGw.last_30d_total || Math.round(currentGw.total * 30 / days);
        const avgOrderTotal = currentGw.avg_order_total || bestGw.avg_order_total || 0;
        const monthlyRevenueOpportunity = Math.round(monthlyAttempts * (liftPp / 100) * avgOrderTotal * 100) / 100;
        const annualRevenueOpportunity = Math.round(monthlyRevenueOpportunity * 12 * 100) / 100;

        const meta = binMetaMap.get(bin) || {};
        const currentMeta = gwMetaMap.get(currentGw.gateway_id) || {};
        const bestMeta = gwMetaMap.get(bestGw.gateway_id) || {};

        opportunities.push({
          bin,
          issuer_bank:   meta.issuer_bank || null,
          card_brand:    meta.card_brand || null,
          card_type:     meta.card_type || null,
          tx_group:      txGroup,

          current_gateway: {
            gateway_id:   currentGw.gateway_id,
            gateway_name: formatGatewayName(currentMeta),
            rate:         currentRate,
            total:        currentGw.total,
          },
          best_gateway: {
            gateway_id:   bestGw.gateway_id,
            gateway_name: formatGatewayName(bestMeta),
            rate:         bestRate,
            total:        bestGw.total,
          },

          current_rate:   currentRate,
          best_rate:      bestRate,
          lift_pp:        liftPp,

          monthly_attempts:            monthlyAttempts,
          avg_order_total:             avgOrderTotal,
          monthly_revenue_opportunity: monthlyRevenueOpportunity,
          annual_revenue_opportunity:  annualRevenueOpportunity,
        });
      }
    }
  }

  // Sort by monthly opportunity DESC
  opportunities.sort((a, b) => b.monthly_revenue_opportunity - a.monthly_revenue_opportunity);

  // -----------------------------------------------------------------------
  // 7. Build summary
  // -----------------------------------------------------------------------
  const totalMonthly = opportunities.reduce((s, o) => s + o.monthly_revenue_opportunity, 0);
  const totalAnnual = opportunities.reduce((s, o) => s + o.annual_revenue_opportunity, 0);

  // Top 10 BINs % of total
  const top10Opportunities = opportunities.slice(0, 10);
  const top10Monthly = top10Opportunities.reduce((s, o) => s + o.monthly_revenue_opportunity, 0);
  const top10PctOfTotal = totalMonthly > 0
    ? Math.round((top10Monthly / totalMonthly) * 10000) / 100 : 0;

  // Breakdown by tx_group
  const txGroupBreakdown = {};
  for (const opp of opportunities) {
    if (!txGroupBreakdown[opp.tx_group]) {
      txGroupBreakdown[opp.tx_group] = {
        tx_group:             opp.tx_group,
        opportunity_count:    0,
        total_monthly:        0,
        total_annual:         0,
        avg_lift_pp:          0,
        total_monthly_attempts: 0,
        _liftSum:             0,
      };
    }
    const grp = txGroupBreakdown[opp.tx_group];
    grp.opportunity_count += 1;
    grp.total_monthly += opp.monthly_revenue_opportunity;
    grp.total_annual += opp.annual_revenue_opportunity;
    grp.total_monthly_attempts += opp.monthly_attempts;
    grp._liftSum += opp.lift_pp;
  }

  // Finalize avg lift and round
  for (const grp of Object.values(txGroupBreakdown)) {
    grp.avg_lift_pp = grp.opportunity_count > 0
      ? Math.round((grp._liftSum / grp.opportunity_count) * 100) / 100 : 0;
    grp.total_monthly = Math.round(grp.total_monthly * 100) / 100;
    grp.total_annual = Math.round(grp.total_annual * 100) / 100;
    delete grp._liftSum;
  }

  const summary = {
    total_opportunities:    opportunities.length,
    total_monthly_revenue:  Math.round(totalMonthly * 100) / 100,
    total_annual_revenue:   Math.round(totalAnnual * 100) / 100,
    unique_bins:            new Set(opportunities.map(o => o.bin)).size,
    top_10_bins_pct:        top10PctOfTotal,
    by_tx_group:            txGroupBreakdown,
  };

  return { summary, opportunities };
}

function _emptySummary() {
  return {
    total_opportunities:   0,
    total_monthly_revenue: 0,
    total_annual_revenue:  0,
    unique_bins:           0,
    top_10_bins_pct:       0,
    by_tx_group:           {},
  };
}

module.exports = { computeLiftOpportunities };
