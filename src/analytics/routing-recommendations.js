/**
 * Routing Recommendations — Per-BIN gateway optimization suggestions.
 *
 * For each BIN (or cluster for low-volume BINs) with data on 2+ gateways,
 * determines the best gateway per tx_type group, expected lift, revenue
 * impact, and cascade rules.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql, queryOneSql } = require('../db/connection');
const {
  CLEAN_FILTER, CASCADE_WHERE, TX_TYPE_GROUPS, getCachedOrCompute,
  formatGatewayName, daysAgoFilter, confidenceScore, stddev,
} = require('./engine');
const { requiresBankChange } = require('../classifiers/decline');

/**
 * Compute routing recommendations for a client.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {number} [opts.minSample]  - Minimum attempts per gateway to consider (default 20)
 * @param {number} [opts.days]       - Lookback window in days (default 90)
 * @param {number} [opts.clusterThreshold] - BINs with fewer than this go into clusters (default 30)
 * @param {number} [opts.maxCascade] - Maximum cascade attempts (default 3)
 * @returns {Array<object>} Routing recommendations sorted by revenue impact DESC
 */
function computeRoutingRecommendations(clientId, opts = {}) {
  const minSample        = opts.minSample ?? 20;
  const days             = opts.days ?? 180;
  const clusterThreshold = opts.clusterThreshold ?? 30;
  const maxCascade       = opts.maxCascade ?? 3;

  const cacheKey = `${minSample}:${days}:${clusterThreshold}:${maxCascade}`;

  return getCachedOrCompute(clientId, 'routing-recommendations', cacheKey, () => {
    return _computeRoutingRecommendations(clientId, minSample, days, clusterThreshold, maxCascade);
  });
}

function _computeRoutingRecommendations(clientId, minSample, days, clusterThreshold, maxCascade) {
  // -----------------------------------------------------------------------
  // 1. Load gateway metadata (for MCC matching and active/closed filtering)
  // -----------------------------------------------------------------------
  const gatewayMeta = querySql(`
    SELECT gateway_id, gateway_alias, bank_name, processor_name,
           mcc_code, lifecycle_state, gateway_active, exclude_from_analysis
    FROM gateways
    WHERE client_id = ?
  `, [clientId]);

  const gwMetaMap = new Map(gatewayMeta.map(g => [g.gateway_id, g]));

  // Build set of active gateway IDs (exclude closed gateways)
  const activeGwIds = new Set();
  for (const g of gatewayMeta) {
    if (g.lifecycle_state !== 'closed' && g.gateway_active !== 0 && g.exclude_from_analysis !== 1) {
      activeGwIds.add(g.gateway_id);
    }
  }

  if (activeGwIds.size === 0) return [];

  // -----------------------------------------------------------------------
  // 2. Get per-BIN, per-gateway, per-product-role group performance
  // Uses derived_product_role instead of tx_type to include anonymous declines.
  // -----------------------------------------------------------------------
  const roleGroupExpr = `CASE
    WHEN o.derived_product_role IN ('main_initial') THEN 'INITIALS'
    WHEN o.derived_product_role IN ('upsell_initial') THEN 'UPSELLS'
    WHEN o.derived_product_role IN ('main_rebill', 'upsell_rebill') THEN 'REBILLS'
    WHEN o.derived_product_role IN ('straight_sale') THEN 'STRAIGHT_SALES'
    ELSE 'OTHER'
  END`;
  const txGroupExpr = roleGroupExpr;

  const perfRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      ${txGroupExpr} AS tx_group,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      ROUND(AVG(CASE WHEN o.order_total > 0 THEN o.order_total END), 2) AS avg_order_total,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') THEN 1 END) AS last_30d_total
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
    else { perfRows.push({ bin: cr.bin, gateway_id: cr.gateway_id, tx_group: cr.tx_group, total: cr.casc_declines, approved: 0, declined: cr.casc_declines, avg_order_total: 0, last_30d_total: 0 }); }
  }

  if (perfRows.length === 0) return [];

  // -----------------------------------------------------------------------
  // 3. Organize data: bin -> txGroup -> [{ gateway_id, total, approved, ... }]
  // -----------------------------------------------------------------------
  const binGwData = new Map();
  for (const row of perfRows) {
    if (!binGwData.has(row.bin)) binGwData.set(row.bin, new Map());
    const txMap = binGwData.get(row.bin);
    if (!txMap.has(row.tx_group)) txMap.set(row.tx_group, []);
    txMap.get(row.tx_group).push({
      gateway_id:      row.gateway_id,
      total:           row.total,
      approved:        row.approved,
      declined:        row.declined,
      rate:            row.total > 0 ? (row.approved / row.total) * 100 : 0,
      avg_order_total: row.avg_order_total || 0,
      last_30d_total:  row.last_30d_total,
    });
  }

  // -----------------------------------------------------------------------
  // 4. BIN metadata for enrichment
  // -----------------------------------------------------------------------
  const allBins = [...binGwData.keys()];
  const binMeta = {};
  if (allBins.length > 0) {
    const metaRows = querySql(`
      SELECT bin, issuer_bank, card_brand, card_type, card_level, is_prepaid
      FROM bin_lookup
      WHERE bin IN (${allBins.map(() => '?').join(',')})
    `, allBins);
    for (const r of metaRows) {
      binMeta[r.bin] = r;
    }
  }

  // -----------------------------------------------------------------------
  // 5. Weekly variance per BIN+gateway for confidence scoring
  // -----------------------------------------------------------------------
  const weeklyRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      CAST((julianday('now') - julianday(o.acquisition_date)) / 7 AS INTEGER) AS week_bucket,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      AND o.order_status IN (2,6,7,8)
      AND o.gateway_id IS NOT NULL
    GROUP BY o.cc_first_6, o.gateway_id, week_bucket
  `, [clientId]);

  // Cascade correction for weekly
  const cascWeekly = querySql(`
    SELECT o.cc_first_6 AS bin, o.original_gateway_id AS gateway_id,
      CAST((julianday('now') - julianday(o.acquisition_date)) / 7 AS INTEGER) AS week_bucket,
      COUNT(*) AS casc_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      AND o.order_status IN (2,6,7,8) AND o.original_gateway_id IS NOT NULL
    GROUP BY o.cc_first_6, o.original_gateway_id, week_bucket
  `, [clientId]);

  for (const cr of cascWeekly) {
    const match = weeklyRows.find(r => r.bin === cr.bin && r.gateway_id === cr.gateway_id && r.week_bucket === cr.week_bucket);
    if (match) { match.total += cr.casc_declines; }
    else { weeklyRows.push({ bin: cr.bin, gateway_id: cr.gateway_id, week_bucket: cr.week_bucket, total: cr.casc_declines, approved: 0 }); }
  }

  // bin:gw -> [weeklyRates]
  const weeklyMap = new Map();
  for (const row of weeklyRows) {
    const key = `${row.bin}:${row.gateway_id}`;
    if (!weeklyMap.has(key)) weeklyMap.set(key, []);
    weeklyMap.get(key).push(row.total > 0 ? (row.approved / row.total) * 100 : 0);
  }

  // Recency: % of data from last 14 days per BIN+gateway
  const recencyRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      COUNT(*) AS total_all,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') THEN 1 END) AS recent_14d
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      AND o.order_status IN (2,6,7,8)
      AND o.gateway_id IS NOT NULL
    GROUP BY o.cc_first_6, o.gateway_id
  `, [clientId]);

  // Cascade correction for recency
  const cascRecency = querySql(`
    SELECT o.cc_first_6 AS bin, o.original_gateway_id AS gateway_id,
      COUNT(*) AS total_all,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') THEN 1 END) AS recent_14d
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      AND o.order_status IN (2,6,7,8) AND o.original_gateway_id IS NOT NULL
    GROUP BY o.cc_first_6, o.original_gateway_id
  `, [clientId]);

  for (const cr of cascRecency) {
    const match = recencyRows.find(r => r.bin === cr.bin && r.gateway_id === cr.gateway_id);
    if (match) { match.total_all += cr.total_all; match.recent_14d += cr.recent_14d; }
    else { recencyRows.push({ bin: cr.bin, gateway_id: cr.gateway_id, total_all: cr.total_all, recent_14d: cr.recent_14d }); }
  }

  const recencyMap = new Map();
  for (const row of recencyRows) {
    const key = `${row.bin}:${row.gateway_id}`;
    recencyMap.set(key, row.total_all > 0 ? (row.recent_14d / row.total_all) * 100 : 0);
  }

  // -----------------------------------------------------------------------
  // 6. Decline category data for cascade eligibility
  // -----------------------------------------------------------------------
  const declineRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      COALESCE(o.decline_category, 'unclassified') AS decline_category,
      COUNT(*) AS cnt
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status = 7
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      AND o.gateway_id IS NOT NULL
    GROUP BY o.cc_first_6, o.gateway_id, o.decline_category
  `, [clientId]);

  // bin:gw -> { processor: N, soft: N, hard: N, ... }
  const declineCatMap = new Map();
  for (const row of declineRows) {
    const key = `${row.bin}:${row.gateway_id}`;
    if (!declineCatMap.has(key)) declineCatMap.set(key, {});
    const cats = declineCatMap.get(key);
    cats[row.decline_category] = (cats[row.decline_category] || 0) + row.cnt;
  }

  // -----------------------------------------------------------------------
  // 7. Identify low-volume BINs for clustering
  // -----------------------------------------------------------------------
  const binTotalVolume = new Map();
  for (const [bin, txMap] of binGwData) {
    let total = 0;
    for (const [, gws] of txMap) {
      for (const gw of gws) total += gw.total;
    }
    binTotalVolume.set(bin, total);
  }

  // Cluster low-volume BINs by issuer_bank + card_type
  const clusterMap = new Map(); // clusterKey -> { bins: [], aggregated txMap }
  const directBins = new Map(); // bin -> txMap (high-volume BINs processed individually)

  for (const [bin, txMap] of binGwData) {
    const vol = binTotalVolume.get(bin) || 0;
    if (vol >= clusterThreshold) {
      directBins.set(bin, txMap);
    } else {
      const meta = binMeta[bin] || {};
      const clusterKey = `${meta.issuer_bank || 'Unknown'}|${meta.card_type || 'Unknown'}`;
      if (!clusterMap.has(clusterKey)) {
        clusterMap.set(clusterKey, { bins: [], txGroupData: new Map() });
      }
      const cluster = clusterMap.get(clusterKey);
      cluster.bins.push(bin);

      // Aggregate performance data
      for (const [txGroup, gws] of txMap) {
        if (!cluster.txGroupData.has(txGroup)) cluster.txGroupData.set(txGroup, new Map());
        const clusterGwMap = cluster.txGroupData.get(txGroup);
        for (const gw of gws) {
          if (!clusterGwMap.has(gw.gateway_id)) {
            clusterGwMap.set(gw.gateway_id, {
              gateway_id: gw.gateway_id, total: 0, approved: 0, declined: 0,
              avg_order_total_sum: 0, avg_order_total_count: 0, last_30d_total: 0,
            });
          }
          const agg = clusterGwMap.get(gw.gateway_id);
          agg.total += gw.total;
          agg.approved += gw.approved;
          agg.declined += gw.declined;
          agg.last_30d_total += gw.last_30d_total;
          if (gw.avg_order_total > 0) {
            agg.avg_order_total_sum += gw.avg_order_total * gw.total;
            agg.avg_order_total_count += gw.total;
          }
        }
      }
    }
  }

  // -----------------------------------------------------------------------
  // 8. Helper: generate recommendations from a txGroup -> [gwPerf] map
  // -----------------------------------------------------------------------
  function generateRecsForEntity(entityType, entityId, entityLabel, txGroupData, entityBins) {
    const recs = [];

    for (const [txGroup, gwEntries] of txGroupData) {
      if (txGroup === 'OTHER') continue; // Skip unknown tx types

      // Convert to array and filter to active gateways with min_sample
      let gwList;
      if (gwEntries instanceof Map) {
        gwList = [...gwEntries.values()].map(g => ({
          ...g,
          rate: g.total > 0 ? (g.approved / g.total) * 100 : 0,
          avg_order_total: g.avg_order_total_count > 0
            ? g.avg_order_total_sum / g.avg_order_total_count : 0,
        }));
      } else {
        gwList = gwEntries;
      }

      // Only consider active gateways with sufficient data
      const qualifiedGws = gwList.filter(g =>
        activeGwIds.has(g.gateway_id) && g.total >= minSample
      );

      // Need 2+ gateways to make a recommendation
      if (qualifiedGws.length < 2) continue;

      // Compare gateways — group by MCC if multiple gateways share an MCC,
      // otherwise compare all qualified gateways together.
      // Many merchants use diverse MCCs across gateways serving the same products.
      const mccGroups = new Map();
      for (const gw of qualifiedGws) {
        const meta = gwMetaMap.get(gw.gateway_id);
        const mcc = (meta && meta.mcc_code) || 'unknown';
        if (!mccGroups.has(mcc)) mccGroups.set(mcc, []);
        mccGroups.get(mcc).push(gw);
      }

      // If no MCC group has 2+ gateways, compare all qualified gateways together
      const hasSharedMcc = [...mccGroups.values()].some(g => g.length >= 2);
      if (!hasSharedMcc) {
        mccGroups.clear();
        mccGroups.set('mixed', qualifiedGws);
      }

      for (const [mcc, mccGws] of mccGroups) {
        if (mccGws.length < 2) continue;

        // Current gateway = the one with most attempts
        const currentGw = mccGws.reduce((a, c) => c.total > a.total ? c : a, mccGws[0]);
        const currentRate = currentGw.rate;

        // Best gateway = highest clean approval rate
        const bestGw = mccGws.reduce((a, c) => c.rate > a.rate ? c : a, mccGws[0]);
        const bestRate = bestGw.rate;

        // Only recommend if lift is positive and gateways are different
        if (bestGw.gateway_id === currentGw.gateway_id) continue;
        const liftPp = bestRate - currentRate;
        if (liftPp <= 0) continue;

        // Monthly attempts estimate (last 30 days volume extrapolated)
        const monthlyAttempts = currentGw.last_30d_total || Math.round(currentGw.total * 30 / days);
        const avgOrderTotal = currentGw.avg_order_total || bestGw.avg_order_total || 0;
        const revenueImpact = Math.round(monthlyAttempts * (liftPp / 100) * avgOrderTotal * 100) / 100;

        // Confidence score for the best gateway
        const bestKey = entityType === 'bin'
          ? `${entityId}:${bestGw.gateway_id}`
          : null;
        const weeklyRates = bestKey ? (weeklyMap.get(bestKey) || []) : [];
        const weeklyVar = stddev(weeklyRates);
        const recentPct = bestKey ? (recencyMap.get(bestKey) || 0) : 50;
        const confidence = confidenceScore(bestGw.total, weeklyVar, recentPct);

        // Cascade eligibility: check decline categories for current gateway
        const currentDecKey = entityType === 'bin'
          ? `${entityId}:${currentGw.gateway_id}`
          : null;
        const declineCats = currentDecKey ? (declineCatMap.get(currentDecKey) || {}) : {};
        const totalDeclines = Object.values(declineCats).reduce((s, v) => s + v, 0);
        const cascadableDeclines = (declineCats.processor || 0) + (declineCats.soft || 0);
        const cascadeEligiblePct = totalDeclines > 0
          ? Math.round((cascadableDeclines / totalDeclines) * 10000) / 100 : 0;

        // Check if any declines for this entity require cross-bank cascade
        const bankChangeDeclines = querySql(`
          SELECT COUNT(*) as cnt FROM orders
          WHERE client_id = ? AND cc_first_6 = ? AND gateway_id = ?
            AND requires_bank_change = 1 AND order_status = 7
        `, [clientId, entityType === 'bin' ? entityId : '', currentGw.gateway_id]);
        const bankChangeCount = bankChangeDeclines.length > 0 ? bankChangeDeclines[0].cnt : 0;
        const hasBankChangeRequired = bankChangeCount > 0;

        // Build cascade chain: current -> best -> next best (up to maxCascade)
        // If bank-change required, ensure cascade targets a different bank
        const currentBank = (gwMetaMap.get(currentGw.gateway_id) || {}).bank_name;
        const cascadeChain = [currentGw.gateway_id];
        const sortedByRate = [...mccGws].sort((a, b) => b.rate - a.rate);
        for (const gw of sortedByRate) {
          if (cascadeChain.length >= maxCascade) break;
          if (cascadeChain.includes(gw.gateway_id)) continue;
          if (hasBankChangeRequired) {
            const gwBank = (gwMetaMap.get(gw.gateway_id) || {}).bank_name;
            if (gwBank === currentBank) continue; // Skip same bank
          }
          cascadeChain.push(gw.gateway_id);
        }

        const currentMeta = gwMetaMap.get(currentGw.gateway_id) || {};
        const bestMeta = gwMetaMap.get(bestGw.gateway_id) || {};

        recs.push({
          entity_type:       entityType,
          entity_id:         entityId,
          entity_label:      entityLabel,
          bins:              entityBins,
          tx_group:          txGroup,
          mcc_code:          mcc,

          current_gateway: {
            gateway_id:   currentGw.gateway_id,
            gateway_name: formatGatewayName(currentMeta),
            total:        currentGw.total,
            rate:         Math.round(currentRate * 100) / 100,
          },

          recommended_gateway: {
            gateway_id:   bestGw.gateway_id,
            gateway_name: formatGatewayName(bestMeta),
            total:        bestGw.total,
            rate:         Math.round(bestRate * 100) / 100,
          },

          lift_pp:             Math.round(liftPp * 100) / 100,
          monthly_attempts:    monthlyAttempts,
          avg_order_total:     avgOrderTotal,
          monthly_revenue_impact: revenueImpact,
          annual_revenue_impact:  Math.round(revenueImpact * 12 * 100) / 100,

          confidence,

          cascade: {
            max_attempts:           maxCascade,
            chain:                  cascadeChain.map(gwId => {
              const m = gwMetaMap.get(gwId) || {};
              return { gateway_id: gwId, gateway_name: formatGatewayName(m), bank_name: m.bank_name || null };
            }),
            cascade_eligible_pct:   cascadeEligiblePct,
            requires_bank_change:   hasBankChangeRequired,
            bank_change_declines:   bankChangeCount,
            rule:                   hasBankChangeRequired
              ? 'CROSS-BANK CASCADE REQUIRED: "Blocked, first used" declines detected. ' +
                'Retrying same bank = 0% recovery. Cascade must target a different acquiring bank.'
              : 'Only processor + soft declines eligible for cascade',
          },

          status: 'new',
        });
      }
    }

    return recs;
  }

  // -----------------------------------------------------------------------
  // 9. Generate recommendations for direct (high-volume) BINs
  // -----------------------------------------------------------------------
  const recommendations = [];

  for (const [bin, txMap] of directBins) {
    const meta = binMeta[bin] || {};
    const label = `BIN ${bin}${meta.issuer_bank ? ' (' + meta.issuer_bank + ')' : ''}`;
    const recs = generateRecsForEntity('bin', bin, label, txMap, [bin]);
    recommendations.push(...recs);
  }

  // -----------------------------------------------------------------------
  // 10. Generate recommendations for clustered (low-volume) BINs
  // -----------------------------------------------------------------------
  for (const [clusterKey, cluster] of clusterMap) {
    // Only process clusters with data on 2+ gateways
    const clusterLabel = `Cluster: ${clusterKey.replace('|', ' / ')} (${cluster.bins.length} BINs)`;
    const recs = generateRecsForEntity('cluster', clusterKey, clusterLabel, cluster.txGroupData, cluster.bins);
    recommendations.push(...recs);
  }

  // -----------------------------------------------------------------------
  // 11. Sort by monthly revenue impact descending
  // -----------------------------------------------------------------------
  recommendations.sort((a, b) => b.monthly_revenue_impact - a.monthly_revenue_impact);

  return recommendations;
}

module.exports = { computeRoutingRecommendations };
