/**
 * BIN Profiles — Deep per-BIN analytics.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql, queryOneSql } = require('../db/connection');
const {
  CLEAN_FILTER, CASCADE_WHERE, TX_TYPE_GROUPS, approvalStats,
  confidenceScore, trendDirection, stabilityFlag,
  recencyWeight, getCachedOrCompute, formatGatewayName,
  sqlIn, daysAgoFilter, stddev,
} = require('./engine');

/**
 * Compute detailed profiles for every BIN with sufficient volume.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {string} [opts.txType]     - Filter to a specific tx_type
 * @param {number} [opts.minSample]  - Minimum attempts to include (default 20)
 * @param {number} [opts.days]       - Lookback window in days (default 90)
 * @param {number} [opts.gatewayId]  - Filter to a specific gateway
 * @returns {Array<object>} Array of BIN profile objects
 */
function computeBinProfiles(clientId, opts = {}) {
  const txType    = opts.txType || null;
  const minSample = opts.minSample ?? 20;
  const days      = opts.days ?? 180;
  const gatewayId = opts.gatewayId || null;

  const cacheKey = `${txType || ''}:${minSample}:${days}:${gatewayId || ''}`;

  return getCachedOrCompute(clientId, 'bin-profiles', cacheKey, () => {
    return _computeBinProfiles(clientId, txType, minSample, days, gatewayId);
  });
}

function _computeBinProfiles(clientId, txType, minSample, days, gatewayId) {
  // -----------------------------------------------------------------------
  // 1. Identify qualifying BINs
  // -----------------------------------------------------------------------
  let binFilter = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
    AND o.order_status IN (2,6,7,8)`;
  const binParams = [clientId];

  if (txType) {
    binFilter += ' AND o.tx_type = ?';
    binParams.push(txType);
  }
  if (gatewayId) {
    binFilter += ' AND o.gateway_id = ?';
    binParams.push(gatewayId);
  }

  const qualifiedBins = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      COUNT(CASE WHEN o.is_cascaded = 1 THEN 1 END) AS cascaded
    FROM orders o
    WHERE ${binFilter} AND o.is_cascaded = 0
    GROUP BY o.cc_first_6
    HAVING total >= ?
    ORDER BY total DESC
  `, [...binParams, minSample]);

  if (qualifiedBins.length === 0) return [];

  const binList = qualifiedBins.map(b => b.bin);

  // -----------------------------------------------------------------------
  // 2. BIN metadata from bin_lookup
  // -----------------------------------------------------------------------
  const metaRows = querySql(`
    SELECT bin, issuer_bank, card_brand, card_type, card_level, is_prepaid
    FROM bin_lookup
    WHERE bin IN (${binList.map(() => '?').join(',')})
  `, binList);
  const metaMap = new Map(metaRows.map(r => [r.bin, r]));

  // -----------------------------------------------------------------------
  // 3. Per tx_type breakdown for each BIN
  // -----------------------------------------------------------------------
  let txBreakdownFilter = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.cc_first_6 IN (${binList.map(() => '?').join(',')})
    AND o.order_status IN (2,6,7,8)`;
  const txParams = [clientId, ...binList];

  if (gatewayId) {
    txBreakdownFilter += ' AND o.gateway_id = ?';
    txParams.push(gatewayId);
  }

  const txBreakdown = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.tx_type,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      COUNT(CASE WHEN o.is_cascaded = 1 THEN 1 END) AS cascaded
    FROM orders o
    WHERE ${txBreakdownFilter}
    GROUP BY o.cc_first_6, o.tx_type
  `, txParams);

  const txMap = new Map();
  for (const row of txBreakdown) {
    if (!txMap.has(row.bin)) txMap.set(row.bin, []);
    txMap.get(row.bin).push({
      tx_type:      row.tx_type || 'unknown',
      total:        row.total,
      approved:     row.approved,
      declined:     row.declined,
      clean_rate:   row.total > 0 ? Math.round((row.approved / row.total) * 10000) / 100 : null,
      cascade_rate: row.total > 0 ? Math.round((row.cascaded / row.total) * 10000) / 100 : null,
    });
  }

  // -----------------------------------------------------------------------
  // 4. Top 3 decline reasons per BIN
  // -----------------------------------------------------------------------
  let declineFilter = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.cc_first_6 IN (${binList.map(() => '?').join(',')})
    AND o.order_status = 7 AND o.decline_reason IS NOT NULL AND o.decline_reason != ''`;
  const declineParams = [clientId, ...binList];

  if (gatewayId) {
    declineFilter += ' AND o.gateway_id = ?';
    declineParams.push(gatewayId);
  }

  const declineRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.decline_reason,
      o.decline_category,
      COUNT(*) AS cnt
    FROM orders o
    WHERE ${declineFilter}
    GROUP BY o.cc_first_6, o.decline_reason
    ORDER BY o.cc_first_6, cnt DESC
  `, declineParams);

  const declineMap = new Map();
  for (const row of declineRows) {
    if (!declineMap.has(row.bin)) declineMap.set(row.bin, []);
    const arr = declineMap.get(row.bin);
    if (arr.length < 3) {
      arr.push({
        reason:   row.decline_reason,
        category: row.decline_category || 'unclassified',
        count:    row.cnt,
      });
    }
  }

  // -----------------------------------------------------------------------
  // 5. Per-gateway approval rate per BIN
  // -----------------------------------------------------------------------
  let gwFilter = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.cc_first_6 IN (${binList.map(() => '?').join(',')})
    AND o.order_status IN (2,6,7,8)`;
  const gwParams = [clientId, ...binList];

  if (txType) {
    gwFilter += ' AND o.tx_type = ?';
    gwParams.push(txType);
  }

  const gwRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      COALESCE(g.gateway_alias, 'Gateway #' || o.gateway_id) AS gateway_name,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) AS approved
    FROM orders o
    LEFT JOIN gateways g ON o.gateway_id = g.gateway_id AND o.client_id = g.client_id
    WHERE ${gwFilter} AND o.is_cascaded = 0
    GROUP BY o.cc_first_6, o.gateway_id
    HAVING total >= 5
    ORDER BY o.cc_first_6, total DESC
  `, gwParams);

  // Cascade correction for per-gateway BIN rates
  let cascGwWhere = `AND ${daysAgoFilter(days)}
    AND o.cc_first_6 IN (${binList.map(() => '?').join(',')})
    AND o.order_status IN (2,6,7,8)`;
  const cascGwParams = [...binList];
  if (txType) { cascGwWhere += ' AND o.tx_type = ?'; cascGwParams.push(txType); }

  const cascGw = querySql(`
    SELECT o.cc_first_6 AS bin, o.original_gateway_id AS gateway_id,
      COALESCE(g.gateway_alias, 'Gateway #' || o.original_gateway_id) AS gateway_name,
      COUNT(*) AS casc_declines
    FROM orders o
    LEFT JOIN gateways g ON o.original_gateway_id = g.gateway_id AND o.client_id = g.client_id
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} ${cascGwWhere}
    GROUP BY o.cc_first_6, o.original_gateway_id
  `, [clientId, ...cascGwParams]);

  for (const cr of cascGw) {
    const match = gwRows.find(r => r.bin === cr.bin && r.gateway_id === cr.gateway_id);
    if (match) { match.total += cr.casc_declines; }
    else { gwRows.push({ bin: cr.bin, gateway_id: cr.gateway_id, gateway_name: cr.gateway_name, total: cr.casc_declines, approved: 0 }); }
  }

  const gwMap = new Map();
  for (const row of gwRows) {
    if (!gwMap.has(row.bin)) gwMap.set(row.bin, []);
    gwMap.get(row.bin).push({
      gateway_id:   row.gateway_id,
      gateway_name: row.gateway_name,
      total:        row.total,
      approved:     row.approved,
      rate:         Math.round((row.approved / row.total) * 10000) / 100,
    });
  }

  // -----------------------------------------------------------------------
  // 6. Weekly trend data (7d / 14d / 30d) per BIN
  // -----------------------------------------------------------------------
  const trendPeriods = [
    { label: '7d',  start: 7,  end: 0  },
    { label: '14d', start: 14, end: 0  },
    { label: '30d', start: 30, end: 0  },
  ];

  // Also get the prior period for comparison
  const trendComparisons = [
    { label: '7d',  currentDays: 7,  priorStart: 14, priorEnd: 7  },
    { label: '14d', currentDays: 14, priorStart: 28, priorEnd: 14 },
    { label: '30d', currentDays: 30, priorStart: 60, priorEnd: 30 },
  ];

  // Gather weekly rates for confidence / stability calculation
  let weeklyFilter = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.cc_first_6 IN (${binList.map(() => '?').join(',')})
    AND o.order_status IN (2,6,7,8)`;
  const weeklyParams = [clientId, ...binList];

  if (txType) {
    weeklyFilter += ' AND o.tx_type = ?';
    weeklyParams.push(txType);
  }
  if (gatewayId) {
    weeklyFilter += ' AND o.gateway_id = ?';
    weeklyParams.push(gatewayId);
  }

  const weeklyRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      CAST((julianday('now') - julianday(o.acquisition_date)) / 7 AS INTEGER) AS week_bucket,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    WHERE ${weeklyFilter}
    GROUP BY o.cc_first_6, week_bucket
    ORDER BY o.cc_first_6, week_bucket
  `, weeklyParams);

  const weeklyMap = new Map();
  for (const row of weeklyRows) {
    if (!weeklyMap.has(row.bin)) weeklyMap.set(row.bin, []);
    weeklyMap.get(row.bin).push({
      week_bucket: row.week_bucket,
      total:       row.total,
      rate:        row.total > 0 ? (row.approved / row.total) * 100 : 0,
    });
  }

  // Period rates for trend direction
  let periodFilter = `o.client_id = ? AND ${CLEAN_FILTER}
    AND o.cc_first_6 IN (${binList.map(() => '?').join(',')})
    AND o.order_status IN (2,6,7,8)`;
  const periodBaseParams = [clientId, ...binList];

  if (txType) {
    periodFilter += ' AND o.tx_type = ?';
    periodBaseParams.push(txType);
  }
  if (gatewayId) {
    periodFilter += ' AND o.gateway_id = ?';
    periodBaseParams.push(gatewayId);
  }

  // Compute period rates in one query using CASE
  const periodRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') AND o.order_status IN (2,6,8) THEN 1 END) AS ap_7d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') AND o.order_status IN (2,6,7,8) THEN 1 END) AS tot_7d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') AND o.order_status IN (2,6,8) THEN 1 END) AS ap_7d_prior,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') AND o.order_status IN (2,6,7,8) THEN 1 END) AS tot_7d_prior,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.order_status IN (2,6,8) THEN 1 END) AS ap_14d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.order_status IN (2,6,7,8) THEN 1 END) AS tot_14d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-28 days') AND o.acquisition_date < date('now', '-14 days') AND o.order_status IN (2,6,8) THEN 1 END) AS ap_14d_prior,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-28 days') AND o.acquisition_date < date('now', '-14 days') AND o.order_status IN (2,6,7,8) THEN 1 END) AS tot_14d_prior,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') AND o.order_status IN (2,6,8) THEN 1 END) AS ap_30d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') AND o.order_status IN (2,6,7,8) THEN 1 END) AS tot_30d,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-60 days') AND o.acquisition_date < date('now', '-30 days') AND o.order_status IN (2,6,8) THEN 1 END) AS ap_30d_prior,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-60 days') AND o.acquisition_date < date('now', '-30 days') AND o.order_status IN (2,6,7,8) THEN 1 END) AS tot_30d_prior,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') THEN 1 END) AS recent_14d,
      COUNT(*) AS total_all
    FROM orders o
    WHERE ${periodFilter}
    GROUP BY o.cc_first_6
  `, periodBaseParams);

  const periodMap = new Map(periodRows.map(r => [r.bin, r]));

  // -----------------------------------------------------------------------
  // 7. Revenue metrics per BIN (only where order_total > 0)
  // -----------------------------------------------------------------------
  let revFilter = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.cc_first_6 IN (${binList.map(() => '?').join(',')})
    AND o.order_status IN (2,6,8) AND o.order_total > 0`;
  const revParams = [clientId, ...binList];

  if (txType) {
    revFilter += ' AND o.tx_type = ?';
    revParams.push(txType);
  }
  if (gatewayId) {
    revFilter += ' AND o.gateway_id = ?';
    revParams.push(gatewayId);
  }

  const revRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      COUNT(*) AS revenue_orders,
      ROUND(SUM(o.order_total), 2) AS total_revenue,
      ROUND(AVG(o.order_total), 2) AS avg_order_value,
      ROUND(MIN(o.order_total), 2) AS min_order_value,
      ROUND(MAX(o.order_total), 2) AS max_order_value
    FROM orders o
    WHERE ${revFilter}
    GROUP BY o.cc_first_6
  `, revParams);

  const revMap = new Map(revRows.map(r => [r.bin, r]));

  // -----------------------------------------------------------------------
  // 8. Assemble profiles
  // -----------------------------------------------------------------------
  const profiles = [];

  for (const b of qualifiedBins) {
    const meta   = metaMap.get(b.bin) || {};
    const period = periodMap.get(b.bin) || {};
    const weekly = weeklyMap.get(b.bin) || [];
    const rev    = revMap.get(b.bin) || {};

    // Rates
    const cleanRate   = b.total > 0 ? Math.round((b.approved / b.total) * 10000) / 100 : null;
    const cascadeRate = b.total > 0 ? Math.round((b.cascaded / b.total) * 10000) / 100 : null;

    // Trends
    const rate7d       = period.tot_7d > 0 ? Math.round((period.ap_7d / period.tot_7d) * 10000) / 100 : null;
    const rate7dPrior  = period.tot_7d_prior > 0 ? Math.round((period.ap_7d_prior / period.tot_7d_prior) * 10000) / 100 : null;
    const rate14d      = period.tot_14d > 0 ? Math.round((period.ap_14d / period.tot_14d) * 10000) / 100 : null;
    const rate14dPrior = period.tot_14d_prior > 0 ? Math.round((period.ap_14d_prior / period.tot_14d_prior) * 10000) / 100 : null;
    const rate30d      = period.tot_30d > 0 ? Math.round((period.ap_30d / period.tot_30d) * 10000) / 100 : null;
    const rate30dPrior = period.tot_30d_prior > 0 ? Math.round((period.ap_30d_prior / period.tot_30d_prior) * 10000) / 100 : null;

    // Confidence
    const weeklyRates = weekly.map(w => w.rate);
    const weeklyVar   = stddev(weeklyRates);
    const recentPct   = period.total_all > 0 ? (period.recent_14d / period.total_all) * 100 : 0;
    const confidence  = confidenceScore(b.total, weeklyVar, recentPct);

    // Best / worst gateway
    const gwPerf  = gwMap.get(b.bin) || [];
    const bestGw  = gwPerf.length > 0 ? gwPerf.reduce((a, c) => c.rate > a.rate ? c : a, gwPerf[0]) : null;
    const worstGw = gwPerf.length > 1 ? gwPerf.reduce((a, c) => c.rate < a.rate ? c : a, gwPerf[0]) : null;

    profiles.push({
      bin:             b.bin,
      total:           b.total,
      approved:        b.approved,
      declined:        b.declined,
      clean_rate:      cleanRate,
      cascade_rate:    cascadeRate,

      // BIN metadata
      issuer_bank:     meta.issuer_bank || null,
      card_brand:      meta.card_brand || null,
      card_type:       meta.card_type || null,
      card_level:      meta.card_level || null,
      is_prepaid:      meta.is_prepaid || 0,

      // Per tx_type breakdown
      tx_type_breakdown: txMap.get(b.bin) || [],

      // Top decline reasons
      top_decline_reasons: declineMap.get(b.bin) || [],

      // Per-gateway approval rates
      gateway_performance: gwPerf,

      // Trend
      trend: {
        '7d':  { rate: rate7d,  prior: rate7dPrior,  direction: trendDirection(rate7d, rate7dPrior),   volume: period.tot_7d || 0 },
        '14d': { rate: rate14d, prior: rate14dPrior, direction: trendDirection(rate14d, rate14dPrior), volume: period.tot_14d || 0 },
        '30d': { rate: rate30d, prior: rate30dPrior, direction: trendDirection(rate30d, rate30dPrior), volume: period.tot_30d || 0 },
      },

      // Confidence
      confidence,

      // Revenue (only where order_total > 0)
      revenue: {
        total_revenue:   rev.total_revenue || 0,
        revenue_orders:  rev.revenue_orders || 0,
        avg_order_value: rev.avg_order_value || null,
        min_order_value: rev.min_order_value || null,
        max_order_value: rev.max_order_value || null,
      },

      // Recommendations
      best_gateway:  bestGw ? { gateway_id: bestGw.gateway_id, gateway_name: bestGw.gateway_name, rate: bestGw.rate, volume: bestGw.total } : null,
      worst_gateway: worstGw && worstGw.gateway_id !== (bestGw && bestGw.gateway_id)
        ? { gateway_id: worstGw.gateway_id, gateway_name: worstGw.gateway_name, rate: worstGw.rate, volume: worstGw.total }
        : null,
    });
  }

  return profiles;
}

module.exports = { computeBinProfiles };
