/**
 * Gateway Profiles — Deep per-gateway analytics.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql, queryOneSql } = require('../db/connection');
const {
  CLEAN_FILTER, TX_TYPE_GROUPS, CASCADE_WHERE, getCachedOrCompute,
  formatGatewayName, daysAgoFilter, trendDirection, stabilityFlag, stddev,
} = require('./engine');

/**
 * Compute detailed profiles for every gateway with sufficient volume.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {string} [opts.txType]     - Filter to a specific tx_type
 * @param {number} [opts.minSample]  - Minimum attempts to include (default 30)
 * @param {number} [opts.days]       - Lookback window in days (default 90)
 * @returns {Array<object>} Array of gateway profile objects
 */
function computeGatewayProfiles(clientId, opts = {}) {
  const txType    = opts.txType || null;
  const minSample = opts.minSample ?? 30;
  const days      = opts.days ?? 180;

  const cacheKey = `${txType || ''}:${minSample}:${days}`;

  return getCachedOrCompute(clientId, 'gateway-profiles', cacheKey, () => {
    return _computeGatewayProfiles(clientId, txType, minSample, days);
  });
}

function _computeGatewayProfiles(clientId, txType, minSample, days) {
  // -----------------------------------------------------------------------
  // 1. Gateway metadata
  // -----------------------------------------------------------------------
  const gateways = querySql(`
    SELECT
      gateway_id, gateway_alias, bank_name, processor_name,
      mcc_code, acquiring_bin, lifecycle_state, gateway_active, exclude_from_analysis,
      global_monthly_cap, monthly_sales
    FROM gateways
    WHERE client_id = ?
  `, [clientId]);

  const gwMeta = new Map(gateways.map(g => [g.gateway_id, g]));

  // -----------------------------------------------------------------------
  // 2. Overall stats per gateway
  // -----------------------------------------------------------------------
  let overallWhere = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.gateway_id IS NOT NULL
    AND o.order_status IN (2,6,7,8)`;
  const overallParams = [clientId];

  if (txType) {
    overallWhere += ' AND o.tx_type = ?';
    overallParams.push(txType);
  }

  const overallStats = querySql(`
    SELECT
      o.gateway_id,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      COUNT(CASE WHEN o.is_cascaded = 0 AND o.order_status IN (2,6,8) THEN 1 END) AS clean_approved,
      COUNT(CASE WHEN o.is_cascaded = 0 AND o.order_status IN (2,6,7,8) THEN 1 END) AS clean_total,
      COUNT(CASE WHEN o.is_cascaded = 1 THEN 1 END) AS cascade_count,
      ROUND(SUM(CASE WHEN o.order_status IN (2,6,8) AND o.order_total > 0 THEN o.order_total ELSE 0 END), 2) AS revenue,
      COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.order_total > 0 THEN 1 END) AS revenue_orders,
      ROUND(AVG(CASE WHEN o.order_status IN (2,6,8) AND o.order_total > 0 THEN o.order_total END), 2) AS avg_order_value
    FROM orders o
    WHERE ${overallWhere} AND o.is_cascaded = 0
    GROUP BY o.gateway_id
    HAVING total >= ?
    ORDER BY total DESC
  `, [...overallParams, minSample]);

  // -- Cascade correction: attribute synthetic declines to original_gateway_id --
  let cascWhere = `AND ${daysAgoFilter(days)} AND o.order_status IN (2,6,7,8)`;
  const cascParams = [];
  if (txType) { cascWhere += ' AND o.tx_type = ?'; cascParams.push(txType); }

  const cascOverall = querySql(`
    SELECT o.original_gateway_id AS gateway_id, COUNT(*) AS casc_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER}
      ${cascWhere}
    GROUP BY o.original_gateway_id
  `, [clientId, ...cascParams]);

  for (const cr of cascOverall) {
    const s = overallStats.find(s => s.gateway_id === cr.gateway_id);
    if (s) { s.total += cr.casc_declines; s.declined += cr.casc_declines; }
  }

  if (overallStats.length === 0) return [];

  const gwIds = overallStats.map(g => g.gateway_id);
  const inPlaceholders = gwIds.map(() => '?').join(',');

  // -----------------------------------------------------------------------
  // 3. Per tx_type breakdown
  // -----------------------------------------------------------------------
  let txWhere = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.gateway_id IN (${inPlaceholders})
    AND o.order_status IN (2,6,7,8)`;
  const txParams = [clientId, ...gwIds];

  const txRows = querySql(`
    SELECT
      o.gateway_id,
      o.tx_type,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      COUNT(CASE WHEN o.is_cascaded = 1 THEN 1 END) AS cascaded,
      ROUND(SUM(CASE WHEN o.order_status IN (2,6,8) AND o.order_total > 0 THEN o.order_total ELSE 0 END), 2) AS revenue
    FROM orders o
    WHERE ${txWhere} AND o.is_cascaded = 0
    GROUP BY o.gateway_id, o.tx_type
    ORDER BY o.gateway_id, total DESC
  `, txParams);

  // Cascade correction for tx breakdown
  const cascTx = querySql(`
    SELECT o.original_gateway_id AS gateway_id, o.tx_type, COUNT(*) AS casc_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status IN (2,6,7,8) AND o.original_gateway_id IN (${inPlaceholders})
    GROUP BY o.original_gateway_id, o.tx_type
  `, [clientId, ...gwIds]);

  for (const cr of cascTx) {
    const match = txRows.find(r => r.gateway_id === cr.gateway_id && r.tx_type === cr.tx_type);
    if (match) { match.total += cr.casc_declines; match.declined += cr.casc_declines; }
    else { txRows.push({ gateway_id: cr.gateway_id, tx_type: cr.tx_type, total: cr.casc_declines, approved: 0, declined: cr.casc_declines, cascaded: 0, revenue: 0 }); }
  }

  const txMap = new Map();
  for (const row of txRows) {
    if (!txMap.has(row.gateway_id)) txMap.set(row.gateway_id, []);
    txMap.get(row.gateway_id).push({
      tx_type:      row.tx_type || 'unknown',
      total:        row.total,
      approved:     row.approved,
      declined:     row.declined,
      cascaded:     row.cascaded,
      approval_rate: row.total > 0 ? Math.round((row.approved / row.total) * 10000) / 100 : null,
      cascade_rate:  row.total > 0 ? Math.round((row.cascaded / row.total) * 10000) / 100 : null,
      revenue:       row.revenue,
    });
  }

  // -----------------------------------------------------------------------
  // 4. Top/bottom 5 BINs per gateway
  // -----------------------------------------------------------------------
  let binWhere = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.gateway_id IN (${inPlaceholders})
    AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
    AND o.order_status IN (2,6,7,8)`;
  const binParams = [clientId, ...gwIds];

  if (txType) {
    binWhere += ' AND o.tx_type = ?';
    binParams.push(txType);
  }

  const binRows = querySql(`
    SELECT
      o.gateway_id,
      o.cc_first_6 AS bin,
      COALESCE(b.issuer_bank, 'Unknown') AS issuer_bank,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE ${binWhere}
    GROUP BY o.gateway_id, o.cc_first_6
    HAVING total >= 5
    ORDER BY o.gateway_id, total DESC
  `, binParams);

  // Cascade correction for BIN breakdown
  let cascBinWhere = `AND ${daysAgoFilter(days)} AND o.order_status IN (2,6,7,8)
    AND o.original_gateway_id IN (${inPlaceholders}) AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''`;
  const cascBinParams = [...gwIds];
  if (txType) { cascBinWhere += ' AND o.tx_type = ?'; cascBinParams.push(txType); }

  const cascBin = querySql(`
    SELECT o.original_gateway_id AS gateway_id, o.cc_first_6 AS bin,
      COALESCE(b.issuer_bank, 'Unknown') AS issuer_bank, COUNT(*) AS casc_declines
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} ${cascBinWhere}
    GROUP BY o.original_gateway_id, o.cc_first_6
  `, [clientId, ...cascBinParams]);

  for (const cr of cascBin) {
    const match = binRows.find(r => r.gateway_id === cr.gateway_id && r.bin === cr.bin);
    if (match) { match.total += cr.casc_declines; }
    else { binRows.push({ gateway_id: cr.gateway_id, bin: cr.bin, issuer_bank: cr.issuer_bank, total: cr.casc_declines, approved: 0 }); }
  }

  // Group by gateway and separate top/bottom
  const binMap = new Map();
  for (const row of binRows) {
    if (!binMap.has(row.gateway_id)) binMap.set(row.gateway_id, []);
    binMap.get(row.gateway_id).push({
      bin:         row.bin,
      issuer_bank: row.issuer_bank,
      total:       row.total,
      approved:    row.approved,
      rate:        Math.round((row.approved / row.total) * 10000) / 100,
    });
  }

  // -----------------------------------------------------------------------
  // 5. Best/worst card_type and card_brand per gateway
  // -----------------------------------------------------------------------
  let cardWhere = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.gateway_id IN (${inPlaceholders})
    AND o.order_status IN (2,6,7,8)`;
  const cardParams = [clientId, ...gwIds];

  if (txType) {
    cardWhere += ' AND o.tx_type = ?';
    cardParams.push(txType);
  }

  // Card type breakdown
  const cardTypeRows = querySql(`
    SELECT
      o.gateway_id,
      COALESCE(b.card_type, 'Unknown') AS card_type,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE ${cardWhere}
    GROUP BY o.gateway_id, card_type
    HAVING total >= 5
    ORDER BY o.gateway_id, total DESC
  `, cardParams);

  // Cascade correction for card type
  let cascCardWhere = `AND ${daysAgoFilter(days)} AND o.order_status IN (2,6,7,8)
    AND o.original_gateway_id IN (${inPlaceholders})`;
  const cascCardParams = [...gwIds];
  if (txType) { cascCardWhere += ' AND o.tx_type = ?'; cascCardParams.push(txType); }

  const cascCardType = querySql(`
    SELECT o.original_gateway_id AS gateway_id, COALESCE(b.card_type, 'Unknown') AS card_type, COUNT(*) AS casc_declines
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} ${cascCardWhere}
    GROUP BY o.original_gateway_id, card_type
  `, [clientId, ...cascCardParams]);

  for (const cr of cascCardType) {
    const match = cardTypeRows.find(r => r.gateway_id === cr.gateway_id && r.card_type === cr.card_type);
    if (match) { match.total += cr.casc_declines; }
    else { cardTypeRows.push({ gateway_id: cr.gateway_id, card_type: cr.card_type, total: cr.casc_declines, approved: 0 }); }
  }

  const cardTypeMap = new Map();
  for (const row of cardTypeRows) {
    if (!cardTypeMap.has(row.gateway_id)) cardTypeMap.set(row.gateway_id, []);
    cardTypeMap.get(row.gateway_id).push({
      card_type: row.card_type,
      total:     row.total,
      approved:  row.approved,
      rate:      Math.round((row.approved / row.total) * 10000) / 100,
    });
  }

  // Card brand breakdown
  const cardBrandRows = querySql(`
    SELECT
      o.gateway_id,
      COALESCE(b.card_brand, o.cc_type, 'Unknown') AS card_brand,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE ${cardWhere}
    GROUP BY o.gateway_id, card_brand
    HAVING total >= 5
    ORDER BY o.gateway_id, total DESC
  `, cardParams);

  // Cascade correction for card brand
  const cascCardBrand = querySql(`
    SELECT o.original_gateway_id AS gateway_id, COALESCE(b.card_brand, o.cc_type, 'Unknown') AS card_brand, COUNT(*) AS casc_declines
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} ${cascCardWhere}
    GROUP BY o.original_gateway_id, card_brand
  `, [clientId, ...cascCardParams]);

  for (const cr of cascCardBrand) {
    const match = cardBrandRows.find(r => r.gateway_id === cr.gateway_id && r.card_brand === cr.card_brand);
    if (match) { match.total += cr.casc_declines; }
    else { cardBrandRows.push({ gateway_id: cr.gateway_id, card_brand: cr.card_brand, total: cr.casc_declines, approved: 0 }); }
  }

  const cardBrandMap = new Map();
  for (const row of cardBrandRows) {
    if (!cardBrandMap.has(row.gateway_id)) cardBrandMap.set(row.gateway_id, []);
    cardBrandMap.get(row.gateway_id).push({
      card_brand: row.card_brand,
      total:      row.total,
      approved:   row.approved,
      rate:       Math.round((row.approved / row.total) * 10000) / 100,
    });
  }

  // -----------------------------------------------------------------------
  // 6. 7d/30d trend + stability per gateway
  // -----------------------------------------------------------------------
  let trendWhere = `o.client_id = ? AND ${CLEAN_FILTER}
    AND o.gateway_id IN (${inPlaceholders})
    AND o.order_status IN (2,6,7,8)`;
  const trendParams = [clientId, ...gwIds];

  if (txType) {
    trendWhere += ' AND o.tx_type = ?';
    trendParams.push(txType);
  }

  const trendRows = querySql(`
    SELECT
      o.gateway_id,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') AND o.order_status IN (2,6,8) THEN 1 END) AS ap_7d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') THEN 1 END) AS tot_7d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') AND o.order_status IN (2,6,8) THEN 1 END) AS ap_7d_prior,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') THEN 1 END) AS tot_7d_prior,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') AND o.order_status IN (2,6,8) THEN 1 END) AS ap_30d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') THEN 1 END) AS tot_30d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-60 days') AND o.acquisition_date < date('now', '-30 days') AND o.order_status IN (2,6,8) THEN 1 END) AS ap_30d_prior,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-60 days') AND o.acquisition_date < date('now', '-30 days') THEN 1 END) AS tot_30d_prior
    FROM orders o
    WHERE ${trendWhere}
    GROUP BY o.gateway_id
  `, trendParams);

  // Cascade correction for trends
  let cascTrendWhere = `AND o.order_status IN (2,6,7,8) AND o.original_gateway_id IN (${inPlaceholders})`;
  const cascTrendParams = [...gwIds];
  if (txType) { cascTrendWhere += ' AND o.tx_type = ?'; cascTrendParams.push(txType); }

  const cascTrend = querySql(`
    SELECT o.original_gateway_id AS gateway_id,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') THEN 1 END) AS tot_7d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') THEN 1 END) AS tot_7d_prior,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') THEN 1 END) AS tot_30d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-60 days') AND o.acquisition_date < date('now', '-30 days') THEN 1 END) AS tot_30d_prior
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} ${cascTrendWhere}
    GROUP BY o.original_gateway_id
  `, [clientId, ...cascTrendParams]);

  for (const cr of cascTrend) {
    const t = trendRows.find(r => r.gateway_id === cr.gateway_id);
    if (t) {
      t.tot_7d += cr.tot_7d; t.tot_7d_prior += cr.tot_7d_prior;
      t.tot_30d += cr.tot_30d; t.tot_30d_prior += cr.tot_30d_prior;
    }
  }

  const trendMap = new Map(trendRows.map(r => [r.gateway_id, r]));

  // Weekly variance for stability
  const weeklyRows = querySql(`
    SELECT
      o.gateway_id,
      CAST((julianday('now') - julianday(o.acquisition_date)) / 7 AS INTEGER) AS week_bucket,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    WHERE ${trendWhere} AND ${daysAgoFilter(days)}
    GROUP BY o.gateway_id, week_bucket
  `, trendParams);

  // Cascade correction for weekly variance
  const cascWeekly = querySql(`
    SELECT o.original_gateway_id AS gateway_id,
      CAST((julianday('now') - julianday(o.acquisition_date)) / 7 AS INTEGER) AS week_bucket,
      COUNT(*) AS casc_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER}
      ${cascTrendWhere} AND ${daysAgoFilter(days)}
    GROUP BY o.original_gateway_id, week_bucket
  `, [clientId, ...cascTrendParams]);

  for (const cr of cascWeekly) {
    const match = weeklyRows.find(r => r.gateway_id === cr.gateway_id && r.week_bucket === cr.week_bucket);
    if (match) { match.total += cr.casc_declines; }
    else { weeklyRows.push({ gateway_id: cr.gateway_id, week_bucket: cr.week_bucket, total: cr.casc_declines, approved: 0 }); }
  }

  const weeklyMap = new Map();
  for (const row of weeklyRows) {
    if (!weeklyMap.has(row.gateway_id)) weeklyMap.set(row.gateway_id, []);
    weeklyMap.get(row.gateway_id).push(
      row.total > 0 ? (row.approved / row.total) * 100 : 0
    );
  }

  // -----------------------------------------------------------------------
  // 7. Cap utilization — estimate current month volume vs cap
  // -----------------------------------------------------------------------
  let capWhere = `o.client_id = ? AND ${CLEAN_FILTER}
    AND o.gateway_id IN (${inPlaceholders})
    AND o.acquisition_date >= date('now', 'start of month')
    AND o.order_status IN (2,6,8)`;
  const capParams = [clientId, ...gwIds];

  const capRows = querySql(`
    SELECT
      o.gateway_id,
      COUNT(*) AS month_volume,
      ROUND(SUM(CASE WHEN o.order_total > 0 THEN o.order_total ELSE 0 END), 2) AS month_revenue
    FROM orders o
    WHERE ${capWhere}
    GROUP BY o.gateway_id
  `, capParams);

  const capMap = new Map(capRows.map(r => [r.gateway_id, r]));

  // -----------------------------------------------------------------------
  // 8. Assemble profiles
  // -----------------------------------------------------------------------
  const profiles = [];

  for (const gs of overallStats) {
    const meta   = gwMeta.get(gs.gateway_id) || {};
    const trend  = trendMap.get(gs.gateway_id) || {};
    const weekly = weeklyMap.get(gs.gateway_id) || [];
    const cap    = capMap.get(gs.gateway_id) || {};
    const bins   = binMap.get(gs.gateway_id) || [];

    // Overall rates
    const overallRate = gs.total > 0 ? Math.round((gs.approved / gs.total) * 10000) / 100 : null;
    const cleanRate   = gs.clean_total > 0 ? Math.round((gs.clean_approved / gs.clean_total) * 10000) / 100 : null;
    const cascadeRate = gs.total > 0 ? Math.round((gs.cascade_count / gs.total) * 10000) / 100 : null;

    // Trend rates
    const rate7d       = trend.tot_7d > 0 ? Math.round((trend.ap_7d / trend.tot_7d) * 10000) / 100 : null;
    const rate7dPrior  = trend.tot_7d_prior > 0 ? Math.round((trend.ap_7d_prior / trend.tot_7d_prior) * 10000) / 100 : null;
    const rate30d      = trend.tot_30d > 0 ? Math.round((trend.ap_30d / trend.tot_30d) * 10000) / 100 : null;
    const rate30dPrior = trend.tot_30d_prior > 0 ? Math.round((trend.ap_30d_prior / trend.tot_30d_prior) * 10000) / 100 : null;

    // Stability
    const variance = stddev(weekly);
    const stability = stabilityFlag(variance);

    // BINs — top 5 and bottom 5
    const sortedByRate = [...bins].sort((a, b) => b.rate - a.rate);
    const topBins    = sortedByRate.slice(0, 5);
    const bottomBins = sortedByRate.length > 5 ? sortedByRate.slice(-5).reverse() : [];

    // Card breakdowns
    const cardTypes  = cardTypeMap.get(gs.gateway_id) || [];
    const cardBrands = cardBrandMap.get(gs.gateway_id) || [];

    const bestCardType  = cardTypes.length > 0 ? cardTypes.reduce((a, c) => c.rate > a.rate ? c : a, cardTypes[0]) : null;
    const worstCardType = cardTypes.length > 1 ? cardTypes.reduce((a, c) => c.rate < a.rate ? c : a, cardTypes[0]) : null;
    const bestCardBrand  = cardBrands.length > 0 ? cardBrands.reduce((a, c) => c.rate > a.rate ? c : a, cardBrands[0]) : null;
    const worstCardBrand = cardBrands.length > 1 ? cardBrands.reduce((a, c) => c.rate < a.rate ? c : a, cardBrands[0]) : null;

    // Cap utilization
    const globalCap    = meta.global_monthly_cap || null;
    const monthVolume  = cap.month_volume || 0;
    const monthRevenue = cap.month_revenue || 0;
    let capUtilization = null;
    if (globalCap && globalCap > 0) {
      capUtilization = {
        cap:              globalCap,
        current_volume:   monthVolume,
        current_revenue:  monthRevenue,
        utilization_pct:  Math.round((monthRevenue / globalCap) * 10000) / 100,
        remaining:        Math.round((globalCap - monthRevenue) * 100) / 100,
      };
    }

    profiles.push({
      gateway_id:       gs.gateway_id,
      gateway_name:     formatGatewayName(meta),
      bank_name:        meta.bank_name || null,
      processor_name:   meta.processor_name || null,
      mcc_code:         meta.mcc_code || null,
      acquiring_bin:    meta.acquiring_bin || null,
      lifecycle_state:  meta.lifecycle_state || null,
      gateway_active:   meta.gateway_active ?? 1,
      exclude_from_analysis: meta.exclude_from_analysis ?? 0,

      // Overall stats
      overall: {
        total:          gs.total,
        approved:       gs.approved,
        declined:       gs.declined,
        approval_rate:  overallRate,
        clean_rate:     cleanRate,
        cascade_rate:   cascadeRate,
        cascade_count:  gs.cascade_count,
        revenue:        gs.revenue,
        revenue_orders: gs.revenue_orders,
        avg_order_value: gs.avg_order_value,
      },

      // Per tx_type breakdown
      tx_type_breakdown: txMap.get(gs.gateway_id) || [],

      // Top / bottom BINs
      top_bins:    topBins,
      bottom_bins: bottomBins,

      // Card analysis
      card_types:  cardTypes,
      card_brands: cardBrands,
      best_card_type:   bestCardType,
      worst_card_type:  worstCardType,
      best_card_brand:  bestCardBrand,
      worst_card_brand: worstCardBrand,

      // Trend
      trend: {
        '7d':  { rate: rate7d,  prior: rate7dPrior,  direction: trendDirection(rate7d, rate7dPrior),   volume: trend.tot_7d || 0 },
        '30d': { rate: rate30d, prior: rate30dPrior, direction: trendDirection(rate30d, rate30dPrior), volume: trend.tot_30d || 0 },
      },
      stability:       stability,
      weekly_variance: Math.round(variance * 100) / 100,

      // Cap utilization
      cap_utilization: capUtilization,
    });
  }

  return profiles;
}

module.exports = { computeGatewayProfiles };
