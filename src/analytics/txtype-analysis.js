/**
 * TX Type Analysis — Per-transaction-type deep analytics.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql } = require('../db/connection');
const {
  CLEAN_FILTER, CASCADE_WHERE, TX_TYPE_GROUPS, ALL_TX_TYPES,
  getCachedOrCompute, formatGatewayName, daysAgoFilter,
  trendDirection, stddev,
} = require('./engine');

/**
 * Compute detailed analytics for each tx_type independently.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {number} [opts.days]       - Lookback window in days (default 90)
 * @param {number} [opts.minSample]  - Minimum gateway volume to rank (default 10)
 * @returns {object} Object keyed by tx_type, each containing analytics
 */
function computeTxTypeAnalysis(clientId, opts = {}) {
  const days      = opts.days ?? 180;
  const minSample = opts.minSample ?? 10;

  const cacheKey = `${days}:${minSample}`;

  return getCachedOrCompute(clientId, 'txtype-analysis', cacheKey, () => {
    return _computeTxTypeAnalysis(clientId, days, minSample);
  });
}

function _computeTxTypeAnalysis(clientId, days, minSample) {
  // -----------------------------------------------------------------------
  // 1. Overview per tx_type
  // -----------------------------------------------------------------------
  const overview = querySql(`
    SELECT
      o.tx_type,
      COUNT(*) AS volume,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      COUNT(CASE WHEN o.is_cascaded = 0 AND o.order_status IN (2,6,8) THEN 1 END) AS clean_approved,
      COUNT(CASE WHEN o.is_cascaded = 0 AND o.order_status IN (2,6,7,8) THEN 1 END) AS clean_total,
      COUNT(CASE WHEN o.is_cascaded = 1 THEN 1 END) AS cascade_count,
      ROUND(SUM(CASE WHEN o.order_status IN (2,6,8) AND o.order_total > 0 THEN o.order_total ELSE 0 END), 2) AS revenue,
      COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.order_total > 0 THEN 1 END) AS revenue_orders,
      ROUND(AVG(CASE WHEN o.order_status IN (2,6,8) AND o.order_total > 0 THEN o.order_total END), 2) AS avg_order_value
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status IN (2,6,7,8)
      AND o.tx_type IS NOT NULL AND o.tx_type != ''
    GROUP BY o.tx_type
    ORDER BY volume DESC
  `, [clientId]);

  if (overview.length === 0) return {};

  const txTypes = overview.map(r => r.tx_type);
  const inPlaceholders = txTypes.map(() => '?').join(',');

  // -----------------------------------------------------------------------
  // 2. Gateway ranking per tx_type
  // -----------------------------------------------------------------------
  const gwRows = querySql(`
    SELECT
      o.tx_type,
      o.gateway_id,
      COALESCE(g.gateway_alias, 'Gateway #' || o.gateway_id) AS gateway_name,
      g.bank_name,
      g.processor_name,
      g.lifecycle_state,
      g.exclude_from_analysis,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      COUNT(CASE WHEN o.is_cascaded = 1 THEN 1 END) AS cascaded,
      ROUND(SUM(CASE WHEN o.order_status IN (2,6,8) AND o.order_total > 0 THEN o.order_total ELSE 0 END), 2) AS revenue
    FROM orders o
    LEFT JOIN gateways g ON o.gateway_id = g.gateway_id AND o.client_id = g.client_id
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status IN (2,6,7,8)
      AND o.tx_type IN (${inPlaceholders})
    GROUP BY o.tx_type, o.gateway_id
    HAVING total >= ?
    ORDER BY o.tx_type, approved * 1.0 / total DESC
  `, [clientId, ...txTypes, minSample]);

  // Cascade correction for gateway ranking
  const cascGw = querySql(`
    SELECT o.tx_type, o.original_gateway_id AS gateway_id, COUNT(*) AS casc_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status IN (2,6,7,8) AND o.tx_type IN (${inPlaceholders})
    GROUP BY o.tx_type, o.original_gateway_id
  `, [clientId, ...txTypes]);

  for (const cr of cascGw) {
    const match = gwRows.find(r => r.tx_type === cr.tx_type && r.gateway_id === cr.gateway_id);
    if (match) { match.total += cr.casc_declines; match.declined += cr.casc_declines; }
  }

  const gwMap = new Map();
  for (const row of gwRows) {
    if (!gwMap.has(row.tx_type)) gwMap.set(row.tx_type, []);
    gwMap.get(row.tx_type).push({
      gateway_id:     row.gateway_id,
      gateway_name:   row.gateway_name,
      bank_name:      row.bank_name || null,
      processor_name: row.processor_name || null,
      lifecycle_state: row.lifecycle_state || null,
      exclude_from_analysis: row.exclude_from_analysis ?? 0,
      total:          row.total,
      approved:       row.approved,
      declined:       row.declined,
      cascaded:       row.cascaded,
      approval_rate:  row.total > 0 ? Math.round((row.approved / row.total) * 10000) / 100 : null,
      cascade_rate:   row.total > 0 ? Math.round((row.cascaded / row.total) * 10000) / 100 : null,
      revenue:        row.revenue,
    });
  }

  // -----------------------------------------------------------------------
  // 3. Top 20 BINs by volume per tx_type
  // -----------------------------------------------------------------------
  const binRows = querySql(`
    SELECT
      o.tx_type,
      o.cc_first_6 AS bin,
      COALESCE(b.issuer_bank, 'Unknown') AS issuer_bank,
      COALESCE(b.card_brand, o.cc_type, 'Unknown') AS card_brand,
      COALESCE(b.card_type, 'Unknown') AS card_type,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status IN (2,6,7,8)
      AND o.tx_type IN (${inPlaceholders})
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
    GROUP BY o.tx_type, o.cc_first_6
    ORDER BY o.tx_type, total DESC
  `, [clientId, ...txTypes]);

  // Take top 20 per tx_type
  const binMap = new Map();
  const binCounts = new Map();
  for (const row of binRows) {
    const cnt = (binCounts.get(row.tx_type) || 0) + 1;
    binCounts.set(row.tx_type, cnt);
    if (cnt > 20) continue;

    if (!binMap.has(row.tx_type)) binMap.set(row.tx_type, []);
    binMap.get(row.tx_type).push({
      bin:         row.bin,
      issuer_bank: row.issuer_bank,
      card_brand:  row.card_brand,
      card_type:   row.card_type,
      total:       row.total,
      approved:    row.approved,
      declined:    row.declined,
      rate:        row.total > 0 ? Math.round((row.approved / row.total) * 10000) / 100 : null,
    });
  }

  // -----------------------------------------------------------------------
  // 4. Card type / brand breakdown per tx_type
  // -----------------------------------------------------------------------
  const cardTypeRows = querySql(`
    SELECT
      o.tx_type,
      COALESCE(b.card_type, 'Unknown') AS card_type,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status IN (2,6,7,8)
      AND o.tx_type IN (${inPlaceholders})
    GROUP BY o.tx_type, card_type
    HAVING total >= 5
    ORDER BY o.tx_type, total DESC
  `, [clientId, ...txTypes]);

  const cardTypeMap = new Map();
  for (const row of cardTypeRows) {
    if (!cardTypeMap.has(row.tx_type)) cardTypeMap.set(row.tx_type, []);
    cardTypeMap.get(row.tx_type).push({
      card_type: row.card_type,
      total:     row.total,
      approved:  row.approved,
      rate:      row.total > 0 ? Math.round((row.approved / row.total) * 10000) / 100 : null,
    });
  }

  const cardBrandRows = querySql(`
    SELECT
      o.tx_type,
      COALESCE(b.card_brand, o.cc_type, 'Unknown') AS card_brand,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status IN (2,6,7,8)
      AND o.tx_type IN (${inPlaceholders})
    GROUP BY o.tx_type, card_brand
    HAVING total >= 5
    ORDER BY o.tx_type, total DESC
  `, [clientId, ...txTypes]);

  const cardBrandMap = new Map();
  for (const row of cardBrandRows) {
    if (!cardBrandMap.has(row.tx_type)) cardBrandMap.set(row.tx_type, []);
    cardBrandMap.get(row.tx_type).push({
      card_brand: row.card_brand,
      total:      row.total,
      approved:   row.approved,
      rate:       row.total > 0 ? Math.round((row.approved / row.total) * 10000) / 100 : null,
    });
  }

  // -----------------------------------------------------------------------
  // 5. Gateway comparison — flag gateways performing differently vs overall
  // -----------------------------------------------------------------------
  // First, get overall gateway approval rates (across all tx_types)
  const overallGwRows = querySql(`
    SELECT
      o.gateway_id,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status IN (2,6,7,8)
    GROUP BY o.gateway_id
    HAVING total >= ?
  `, [clientId, minSample]);

  // Cascade correction for overall gateway rates
  const cascOverallGw = querySql(`
    SELECT o.original_gateway_id AS gateway_id, COUNT(*) AS casc_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status IN (2,6,7,8)
    GROUP BY o.original_gateway_id
  `, [clientId]);

  for (const cr of cascOverallGw) {
    const match = overallGwRows.find(r => r.gateway_id === cr.gateway_id);
    if (match) { match.total += cr.casc_declines; }
  }

  const overallGwRates = new Map();
  for (const row of overallGwRows) {
    overallGwRates.set(
      row.gateway_id,
      row.total > 0 ? Math.round((row.approved / row.total) * 10000) / 100 : null
    );
  }

  // Build gateway comparison per tx_type
  const gwCompMap = new Map();
  for (const [txT, gws] of gwMap) {
    const comparisons = [];
    for (const gw of gws) {
      const overallRate = overallGwRates.get(gw.gateway_id);
      if (overallRate == null || gw.approval_rate == null) continue;

      const delta = Math.round((gw.approval_rate - overallRate) * 100) / 100;
      const significantlyDifferent = Math.abs(delta) >= 5;

      if (significantlyDifferent) {
        comparisons.push({
          gateway_id:    gw.gateway_id,
          gateway_name:  gw.gateway_name,
          tx_type_rate:  gw.approval_rate,
          overall_rate:  overallRate,
          delta:         delta,
          direction:     delta > 0 ? 'outperforming' : 'underperforming',
          volume:        gw.total,
        });
      }
    }
    comparisons.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
    gwCompMap.set(txT, comparisons);
  }

  // -----------------------------------------------------------------------
  // 6. Assemble result keyed by tx_type
  // -----------------------------------------------------------------------
  const result = {};

  for (const ov of overview) {
    const txT = ov.tx_type;

    const overallRate = ov.volume > 0 ? Math.round((ov.approved / ov.volume) * 10000) / 100 : null;
    const cleanRate   = ov.clean_total > 0 ? Math.round((ov.clean_approved / ov.clean_total) * 10000) / 100 : null;
    const cascadeRate = ov.volume > 0 ? Math.round((ov.cascade_count / ov.volume) * 10000) / 100 : null;

    // Determine which group this tx_type belongs to
    let group = 'other';
    for (const [grpName, types] of Object.entries(TX_TYPE_GROUPS)) {
      if (types.includes(txT)) {
        group = grpName;
        break;
      }
    }

    // Gateway rankings
    const gateways = gwMap.get(txT) || [];

    result[txT] = {
      tx_type: txT,
      group:   group,

      // Overview
      overview: {
        volume:          ov.volume,
        approved:        ov.approved,
        declined:        ov.declined,
        approval_rate:   overallRate,
        clean_rate:      cleanRate,
        cascade_rate:    cascadeRate,
        cascade_count:   ov.cascade_count,
        revenue:         ov.revenue,
        revenue_orders:  ov.revenue_orders,
        avg_order_value: ov.avg_order_value,
      },

      // Gateway ranking (sorted by approval rate)
      gateway_ranking: gateways,

      // Top 20 BINs by volume
      top_bins: binMap.get(txT) || [],

      // Card breakdowns
      card_types:  cardTypeMap.get(txT) || [],
      card_brands: cardBrandMap.get(txT) || [],

      // Gateway comparison vs overall
      gateway_comparison: gwCompMap.get(txT) || [],
    };
  }

  return result;
}

module.exports = { computeTxTypeAnalysis };
