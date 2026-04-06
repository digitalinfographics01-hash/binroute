/**
 * BIN Clusters — Group similar BINs by issuer, card type, and performance.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql } = require('../db/connection');
const {
  CLEAN_FILTER, CASCADE_WHERE, getCachedOrCompute, formatGatewayName, daysAgoFilter,
} = require('./engine');

/**
 * Group BINs into clusters by issuer_bank + card_type + similar approval rate + same top decline category.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {string} [opts.txType]     - Filter to a specific tx_type
 * @param {number} [opts.minSample]  - Minimum attempts per BIN (default 10)
 * @param {number} [opts.days]       - Lookback window in days (default 90)
 * @returns {Array<object>} Array of cluster objects
 */
function computeBinClusters(clientId, opts = {}) {
  const txType    = opts.txType || null;
  const minSample = opts.minSample ?? 10;
  const days      = opts.days ?? 180;

  const cacheKey = `${txType || ''}:${minSample}:${days}`;

  return getCachedOrCompute(clientId, 'bin-clusters', cacheKey, () => {
    return _computeBinClusters(clientId, txType, minSample, days);
  });
}

/**
 * Performance label based on approval rate.
 */
function performanceLabel(rate) {
  if (rate >= 70) return 'Strong';
  if (rate >= 40) return 'Moderate';
  if (rate >= 20) return 'Low';
  return 'High Decline';
}

/**
 * Performance bucket for grouping (10pp bands).
 * Returns a bucket string that BINs within 10pp of each other will share.
 */
function performanceBucket(rate) {
  // Round to nearest 10pp band
  return Math.floor(rate / 10) * 10;
}

function _computeBinClusters(clientId, txType, minSample, days) {
  // -----------------------------------------------------------------------
  // 1. Get per-BIN stats with metadata
  // -----------------------------------------------------------------------
  let where = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
    AND o.order_status IN (2,6,7,8)`;
  const params = [clientId];

  if (txType) {
    where += ' AND o.tx_type = ?';
    params.push(txType);
  }

  const binStats = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      COALESCE(b.issuer_bank, 'Unknown Issuer') AS issuer_bank,
      COALESCE(b.card_type, 'Unknown Type') AS card_type,
      COALESCE(b.card_brand, o.cc_type, 'Unknown') AS card_brand,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      COUNT(CASE WHEN o.is_cascaded = 1 THEN 1 END) AS cascaded,
      ROUND(SUM(CASE WHEN o.order_status IN (2,6,8) AND o.order_total > 0 THEN o.order_total ELSE 0 END), 2) AS revenue
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE ${where}
    GROUP BY o.cc_first_6
    HAVING total >= ?
    ORDER BY total DESC
  `, [...params, minSample]);

  if (binStats.length === 0) return [];

  // -----------------------------------------------------------------------
  // 2. Get top decline category per BIN
  // -----------------------------------------------------------------------
  const binList = binStats.map(b => b.bin);
  let decWhere = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.cc_first_6 IN (${binList.map(() => '?').join(',')})
    AND o.order_status = 7`;
  const decParams = [clientId, ...binList];

  if (txType) {
    decWhere += ' AND o.tx_type = ?';
    decParams.push(txType);
  }

  const declineRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      COALESCE(o.decline_category, 'unclassified') AS decline_category,
      COUNT(*) AS cnt
    FROM orders o
    WHERE ${decWhere}
    GROUP BY o.cc_first_6, o.decline_category
    ORDER BY o.cc_first_6, cnt DESC
  `, decParams);

  // Keep only the top decline category per BIN
  const topDeclineMap = new Map();
  for (const row of declineRows) {
    if (!topDeclineMap.has(row.bin)) {
      topDeclineMap.set(row.bin, row.decline_category);
    }
  }

  // -----------------------------------------------------------------------
  // 3. Get best gateway per BIN
  // -----------------------------------------------------------------------
  let gwWhere = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.cc_first_6 IN (${binList.map(() => '?').join(',')})
    AND o.order_status IN (2,6,7,8)`;
  const gwParams = [clientId, ...binList];

  if (txType) {
    gwWhere += ' AND o.tx_type = ?';
    gwParams.push(txType);
  }

  const gwRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      COALESCE(g.gateway_alias, 'Gateway #' || o.gateway_id) AS gateway_name,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    LEFT JOIN gateways g ON o.gateway_id = g.gateway_id AND o.client_id = g.client_id
    WHERE ${gwWhere}
    GROUP BY o.cc_first_6, o.gateway_id
    HAVING total >= 5
    ORDER BY o.cc_first_6, approved * 1.0 / total DESC
  `, gwParams);

  // Cascade correction for per-gateway BIN cluster rates
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

  // Map: bin -> [{ gateway_id, name, rate }]
  const gwMap = new Map();
  for (const row of gwRows) {
    if (!gwMap.has(row.bin)) gwMap.set(row.bin, []);
    gwMap.get(row.bin).push({
      gateway_id:   row.gateway_id,
      gateway_name: row.gateway_name,
      total:        row.total,
      rate:         row.total > 0 ? Math.round((row.approved / row.total) * 10000) / 100 : 0,
    });
  }

  // -----------------------------------------------------------------------
  // 4. Assign BINs to enriched objects
  // -----------------------------------------------------------------------
  const enrichedBins = binStats.map(b => {
    const rate = b.total > 0 ? (b.approved / b.total) * 100 : 0;
    return {
      bin:              b.bin,
      issuer_bank:      b.issuer_bank,
      card_type:        b.card_type,
      card_brand:       b.card_brand,
      total:            b.total,
      approved:         b.approved,
      declined:         b.declined,
      cascaded:         b.cascaded,
      revenue:          b.revenue,
      rate:             Math.round(rate * 100) / 100,
      top_decline:      topDeclineMap.get(b.bin) || 'none',
      perf_bucket:      performanceBucket(rate),
      gateways:         gwMap.get(b.bin) || [],
    };
  });

  // -----------------------------------------------------------------------
  // 5. Cluster: group by issuer_bank + card_type + perf_bucket + top_decline
  // -----------------------------------------------------------------------
  const clusterMap = new Map();

  for (const b of enrichedBins) {
    const key = `${b.issuer_bank}|${b.card_type}|${b.perf_bucket}|${b.top_decline}`;
    if (!clusterMap.has(key)) {
      clusterMap.set(key, {
        issuer_bank:  b.issuer_bank,
        card_type:    b.card_type,
        perf_bucket:  b.perf_bucket,
        top_decline:  b.top_decline,
        bins:         [],
      });
    }
    clusterMap.get(key).bins.push(b);
  }

  // -----------------------------------------------------------------------
  // 6. Build cluster summaries
  // -----------------------------------------------------------------------
  const clusters = [];

  for (const [, cluster] of clusterMap) {
    const bins       = cluster.bins;
    const totalVol   = bins.reduce((s, b) => s + b.total, 0);
    const totalApp   = bins.reduce((s, b) => s + b.approved, 0);
    const totalDec   = bins.reduce((s, b) => s + b.declined, 0);
    const totalCasc  = bins.reduce((s, b) => s + b.cascaded, 0);
    const totalRev   = bins.reduce((s, b) => s + (b.revenue || 0), 0);
    const avgRate    = totalVol > 0 ? Math.round((totalApp / totalVol) * 10000) / 100 : 0;
    const cascRate   = totalVol > 0 ? Math.round((totalCasc / totalVol) * 10000) / 100 : 0;
    const perfLabel  = performanceLabel(avgRate);

    // Determine best and fallback gateway across all BINs in cluster
    const gwAgg = new Map();
    for (const b of bins) {
      for (const gw of b.gateways) {
        if (!gwAgg.has(gw.gateway_id)) {
          gwAgg.set(gw.gateway_id, { gateway_id: gw.gateway_id, gateway_name: gw.gateway_name, total: 0, approved: 0 });
        }
        const agg = gwAgg.get(gw.gateway_id);
        agg.total += gw.total;
        agg.approved += Math.round(gw.rate * gw.total / 100);
      }
    }

    const gwList = [...gwAgg.values()]
      .map(g => ({ ...g, rate: g.total > 0 ? Math.round((g.approved / g.total) * 10000) / 100 : 0 }))
      .sort((a, c) => c.rate - a.rate);

    const bestGw     = gwList.length > 0 ? gwList[0] : null;
    const fallbackGw = gwList.length > 1 ? gwList[1] : null;

    // Cluster name
    const name = `${cluster.issuer_bank} ${cluster.card_type} - ${perfLabel}`;

    clusters.push({
      cluster_name:       name,
      issuer_bank:        cluster.issuer_bank,
      card_type:          cluster.card_type,
      performance_label:  perfLabel,
      dominant_decline:   cluster.top_decline,
      bin_count:          bins.length,
      bins:               bins.map(b => b.bin),
      total_volume:       totalVol,
      total_approved:     totalApp,
      total_declined:     totalDec,
      avg_approval_rate:  avgRate,
      cascade_rate:       cascRate,
      total_revenue:      Math.round(totalRev * 100) / 100,
      best_gateway:       bestGw ? { gateway_id: bestGw.gateway_id, gateway_name: bestGw.gateway_name, rate: bestGw.rate, volume: bestGw.total } : null,
      fallback_gateway:   fallbackGw ? { gateway_id: fallbackGw.gateway_id, gateway_name: fallbackGw.gateway_name, rate: fallbackGw.rate, volume: fallbackGw.total } : null,
    });
  }

  // Sort clusters by total volume descending
  clusters.sort((a, b) => b.total_volume - a.total_volume);

  return clusters;
}

module.exports = { computeBinClusters };
