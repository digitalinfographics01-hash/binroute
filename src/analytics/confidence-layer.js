/**
 * Confidence Layer — Per-BIN+gateway confidence assessments.
 *
 * For each BIN+gateway combination, computes a weighted confidence score
 * based on sample size (40%), consistency/variance (40%), and recency (20%).
 * Also flags stability and data quality indicators.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql, queryOneSql } = require('../db/connection');
const {
  CLEAN_FILTER, CASCADE_WHERE, getCachedOrCompute, formatGatewayName,
  daysAgoFilter, stabilityFlag, stddev,
} = require('./engine');

/**
 * Compute confidence layer for a client.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {number} [opts.minSample]  - Minimum attempts to include (default 10)
 * @param {number} [opts.days]       - Lookback window in days (default 90)
 * @returns {Array<object>} Confidence assessments sorted by overall score DESC
 */
function computeConfidenceLayer(clientId, opts = {}) {
  const minSample = opts.minSample ?? 10;
  const days      = opts.days ?? 180;

  const cacheKey = `${minSample}:${days}`;

  return getCachedOrCompute(clientId, 'confidence-layer', cacheKey, () => {
    return _computeConfidenceLayer(clientId, minSample, days);
  });
}

function _computeConfidenceLayer(clientId, minSample, days) {
  // -----------------------------------------------------------------------
  // 1. Per-BIN+gateway overall stats
  // -----------------------------------------------------------------------
  const overallRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      ROUND(AVG(CASE WHEN o.order_total > 0 THEN o.order_total END), 2) AS avg_order_total
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      AND o.order_status IN (2,6,7,8)
      AND o.gateway_id IS NOT NULL
    GROUP BY o.cc_first_6, o.gateway_id
    HAVING total >= ?
    ORDER BY total DESC
  `, [clientId, minSample]);

  // Cascade correction for overall stats
  const cascOverall = querySql(`
    SELECT o.cc_first_6 AS bin, o.original_gateway_id AS gateway_id, COUNT(*) AS casc_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      AND o.order_status IN (2,6,7,8) AND o.original_gateway_id IS NOT NULL
    GROUP BY o.cc_first_6, o.original_gateway_id
    HAVING casc_declines >= 1
  `, [clientId]);

  for (const cr of cascOverall) {
    const match = overallRows.find(r => r.bin === cr.bin && r.gateway_id === cr.gateway_id);
    if (match) { match.total += cr.casc_declines; match.declined += cr.casc_declines; }
  }

  if (overallRows.length === 0) return [];

  // -----------------------------------------------------------------------
  // 2. Weekly breakdown for consistency scoring
  // -----------------------------------------------------------------------
  const allBins = [...new Set(overallRows.map(r => r.bin))];
  const allGwIds = [...new Set(overallRows.map(r => r.gateway_id))];

  const weeklyRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      CAST((julianday('now') - julianday(o.acquisition_date)) / 7 AS INTEGER) AS week_bucket,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IN (${allBins.map(() => '?').join(',')})
      AND o.gateway_id IN (${allGwIds.map(() => '?').join(',')})
      AND o.order_status IN (2,6,7,8)
    GROUP BY o.cc_first_6, o.gateway_id, week_bucket
  `, [clientId, ...allBins, ...allGwIds]);

  // Cascade correction for weekly
  const cascWeekly = querySql(`
    SELECT o.cc_first_6 AS bin, o.original_gateway_id AS gateway_id,
      CAST((julianday('now') - julianday(o.acquisition_date)) / 7 AS INTEGER) AS week_bucket,
      COUNT(*) AS casc_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IN (${allBins.map(() => '?').join(',')})
      AND o.original_gateway_id IN (${allGwIds.map(() => '?').join(',')})
      AND o.order_status IN (2,6,7,8)
    GROUP BY o.cc_first_6, o.original_gateway_id, week_bucket
  `, [clientId, ...allBins, ...allGwIds]);

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

  // -----------------------------------------------------------------------
  // 3. Recency: data from last 14 days
  // -----------------------------------------------------------------------
  const recencyRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      COUNT(*) AS total_all,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') THEN 1 END) AS recent_14d
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IN (${allBins.map(() => '?').join(',')})
      AND o.gateway_id IN (${allGwIds.map(() => '?').join(',')})
      AND o.order_status IN (2,6,7,8)
    GROUP BY o.cc_first_6, o.gateway_id
  `, [clientId, ...allBins, ...allGwIds]);

  // Cascade correction for recency
  const cascRecency = querySql(`
    SELECT o.cc_first_6 AS bin, o.original_gateway_id AS gateway_id,
      COUNT(*) AS total_all,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') THEN 1 END) AS recent_14d
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IN (${allBins.map(() => '?').join(',')})
      AND o.original_gateway_id IN (${allGwIds.map(() => '?').join(',')})
      AND o.order_status IN (2,6,7,8)
    GROUP BY o.cc_first_6, o.original_gateway_id
  `, [clientId, ...allBins, ...allGwIds]);

  for (const cr of cascRecency) {
    const match = recencyRows.find(r => r.bin === cr.bin && r.gateway_id === cr.gateway_id);
    if (match) { match.total_all += cr.total_all; match.recent_14d += cr.recent_14d; }
    else { recencyRows.push({ bin: cr.bin, gateway_id: cr.gateway_id, total_all: cr.total_all, recent_14d: cr.recent_14d }); }
  }

  const recencyMap = new Map();
  for (const row of recencyRows) {
    const key = `${row.bin}:${row.gateway_id}`;
    recencyMap.set(key, {
      total_all:  row.total_all,
      recent_14d: row.recent_14d,
      pct:        row.total_all > 0 ? (row.recent_14d / row.total_all) * 100 : 0,
    });
  }

  // -----------------------------------------------------------------------
  // 4. BIN metadata for data quality flags
  // -----------------------------------------------------------------------
  const binMetaRows = querySql(`
    SELECT bin, issuer_bank, card_brand, card_type, card_level, is_prepaid
    FROM bin_lookup
    WHERE bin IN (${allBins.map(() => '?').join(',')})
  `, allBins);

  const binMetaMap = new Map(binMetaRows.map(r => [r.bin, r]));

  // -----------------------------------------------------------------------
  // 5. Gateway metadata for data quality flags
  // -----------------------------------------------------------------------
  const gwMetaRows = querySql(`
    SELECT gateway_id, gateway_alias, bank_name, processor_name,
           mcc_code, lifecycle_state, gateway_active, exclude_from_analysis
    FROM gateways
    WHERE client_id = ?
  `, [clientId]);

  const gwMetaMap = new Map(gwMetaRows.map(g => [g.gateway_id, g]));

  // -----------------------------------------------------------------------
  // 6. Decline classification coverage per BIN+gateway
  // -----------------------------------------------------------------------
  const declineClassRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      COUNT(*) AS total_declines,
      COUNT(CASE WHEN o.decline_category IS NOT NULL AND o.decline_category != '' AND o.decline_category != 'unclassified' THEN 1 END) AS classified_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IN (${allBins.map(() => '?').join(',')})
      AND o.gateway_id IN (${allGwIds.map(() => '?').join(',')})
      AND o.order_status = 7
    GROUP BY o.cc_first_6, o.gateway_id
  `, [clientId, ...allBins, ...allGwIds]);

  // Cascade correction for decline classification
  const cascDecline = querySql(`
    SELECT o.cc_first_6 AS bin, o.original_gateway_id AS gateway_id, COUNT(*) AS casc_declines
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IN (${allBins.map(() => '?').join(',')})
      AND o.original_gateway_id IN (${allGwIds.map(() => '?').join(',')})
      AND o.order_status IN (2,6,7,8)
    GROUP BY o.cc_first_6, o.original_gateway_id
  `, [clientId, ...allBins, ...allGwIds]);

  for (const cr of cascDecline) {
    const match = declineClassRows.find(r => r.bin === cr.bin && r.gateway_id === cr.gateway_id);
    if (match) { match.total_declines += cr.casc_declines; }
    else { declineClassRows.push({ bin: cr.bin, gateway_id: cr.gateway_id, total_declines: cr.casc_declines, classified_declines: 0 }); }
  }

  const declineClassMap = new Map();
  for (const row of declineClassRows) {
    const key = `${row.bin}:${row.gateway_id}`;
    declineClassMap.set(key, {
      total_declines:      row.total_declines,
      classified_declines: row.classified_declines,
      classification_pct:  row.total_declines > 0
        ? Math.round((row.classified_declines / row.total_declines) * 10000) / 100 : 100,
    });
  }

  // -----------------------------------------------------------------------
  // 7. Assemble confidence assessments
  // -----------------------------------------------------------------------
  const assessments = [];

  for (const row of overallRows) {
    const key = `${row.bin}:${row.gateway_id}`;
    const weeklyRates = weeklyMap.get(key) || [];
    const recency = recencyMap.get(key) || { total_all: 0, recent_14d: 0, pct: 0 };
    const binMeta = binMetaMap.get(row.bin) || {};
    const gwMeta = gwMetaMap.get(row.gateway_id) || {};
    const declineClass = declineClassMap.get(key) || { total_declines: 0, classified_declines: 0, classification_pct: 100 };

    // --- Sample size component (40% weight) ---
    let sampleScore;
    let sampleLevel;
    if (row.total >= 200) {
      sampleScore = 40;
      sampleLevel = 'HIGH';
    } else if (row.total >= 50) {
      // Linear interpolation from 20 to 40 between 50 and 200
      sampleScore = 20 + ((row.total - 50) / 150) * 20;
      sampleLevel = 'MEDIUM';
    } else {
      // Linear interpolation from 0 to 20 between minSample and 50
      const range = Math.max(50 - minSample, 1);
      sampleScore = ((row.total - minSample) / range) * 20;
      sampleLevel = 'LOW';
    }
    sampleScore = Math.max(0, Math.min(40, sampleScore));

    // --- Consistency component (40% weight) ---
    const weeklyVariance = stddev(weeklyRates);
    // Lower variance is better; max 40 points when variance is 0
    // Scale: 0 variance = 40pts, 30+ variance = 0pts
    const consistencyScore = Math.max(0, Math.min(40, 40 - (weeklyVariance * 40 / 30)));

    // --- Recency component (20% weight) ---
    const recentPct = recency.pct;
    // Higher recency % is better
    const recencyScore = (recentPct / 100) * 20;

    // --- Overall weighted score ---
    const overallScore = Math.round(Math.min(100, sampleScore + consistencyScore + recencyScore));

    // Confidence level
    let confidenceLevel;
    if (overallScore >= 75) confidenceLevel = 'HIGH';
    else if (overallScore >= 45) confidenceLevel = 'MEDIUM';
    else confidenceLevel = 'LOW';

    // Stability flag
    const stability = stabilityFlag(weeklyVariance);

    // Data quality flags
    const binEnriched = !!(binMeta.issuer_bank && binMeta.card_brand);
    const gatewayConfigured = !!(gwMeta.gateway_alias || gwMeta.bank_name);
    const declineClassified = declineClass.classification_pct >= 70;
    const sufficientSample = row.total >= minSample;

    const approvalRate = row.total > 0 ? Math.round((row.approved / row.total) * 10000) / 100 : null;

    assessments.push({
      bin:          row.bin,
      gateway_id:   row.gateway_id,
      gateway_name: formatGatewayName(gwMeta),

      // Core stats
      total:         row.total,
      approved:      row.approved,
      declined:      row.declined,
      approval_rate: approvalRate,

      // BIN metadata
      issuer_bank: binMeta.issuer_bank || null,
      card_brand:  binMeta.card_brand || null,
      card_type:   binMeta.card_type || null,

      // Confidence components
      confidence: {
        overall_score: overallScore,
        level:         confidenceLevel,
        components: {
          sample_size: {
            weight:   '40%',
            score:    Math.round(sampleScore * 10) / 10,
            max:      40,
            level:    sampleLevel,
            attempts: row.total,
          },
          consistency: {
            weight:          '40%',
            score:           Math.round(consistencyScore * 10) / 10,
            max:             40,
            weekly_variance: Math.round(weeklyVariance * 100) / 100,
            weeks_with_data: weeklyRates.length,
          },
          recency: {
            weight:       '20%',
            score:        Math.round(recencyScore * 10) / 10,
            max:          20,
            recent_14d:   recency.recent_14d,
            total:        recency.total_all,
            recent_pct:   Math.round(recentPct * 100) / 100,
          },
        },
      },

      // Stability
      stability: {
        flag:     stability,
        variance: Math.round(weeklyVariance * 100) / 100,
      },

      // Data quality flags
      data_quality: {
        bin_enriched:       binEnriched,
        gateway_configured: gatewayConfigured,
        decline_classified: declineClassified,
        sufficient_sample:  sufficientSample,
        decline_classification_pct: declineClass.classification_pct,
        quality_score: [binEnriched, gatewayConfigured, declineClassified, sufficientSample]
          .filter(Boolean).length,
        quality_max: 4,
      },
    });
  }

  // Sort by overall confidence score DESC
  assessments.sort((a, b) => b.confidence.overall_score - a.confidence.overall_score);

  return assessments;
}

module.exports = { computeConfidenceLayer };
