/**
 * Trend Detection — Short-term performance shift and decline pattern detection.
 *
 * Compares last 7 days vs days 8-14 across BINs, gateways, and decline
 * patterns. Flags significant changes and assigns alert severity levels.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql, queryOneSql } = require('../db/connection');
const {
  CLEAN_FILTER, CASCADE_WHERE, getCachedOrCompute, formatGatewayName, daysAgoFilter,
} = require('./engine');

/**
 * Compute trend detection for a client.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {number} [opts.minAttempts]         - Minimum attempts per period for BINs (default 20)
 * @param {number} [opts.minGatewayAttempts]  - Minimum attempts per period for gateways (default 30)
 * @param {number} [opts.binDeltaThreshold]   - BIN delta threshold in pp (default 10)
 * @param {number} [opts.gwDeltaThreshold]    - Gateway delta threshold in pp (default 10)
 * @param {number} [opts.declineIncreaseThreshold] - Decline increase % threshold (default 20)
 * @returns {{ binTrends: Array, gatewayTrends: Array, declineTrends: Array, alerts: Array }}
 */
function computeTrendDetection(clientId, opts = {}) {
  const minAttempts              = opts.minAttempts ?? 20;
  const minGatewayAttempts       = opts.minGatewayAttempts ?? 30;
  const binDeltaThreshold        = opts.binDeltaThreshold ?? 10;
  const gwDeltaThreshold         = opts.gwDeltaThreshold ?? 10;
  const declineIncreaseThreshold = opts.declineIncreaseThreshold ?? 20;

  const cacheKey = `${minAttempts}:${minGatewayAttempts}:${binDeltaThreshold}:${gwDeltaThreshold}:${declineIncreaseThreshold}`;

  return getCachedOrCompute(clientId, 'trend-detection', cacheKey, () => {
    return _computeTrendDetection(clientId, minAttempts, minGatewayAttempts, binDeltaThreshold, gwDeltaThreshold, declineIncreaseThreshold);
  });
}

function _computeTrendDetection(clientId, minAttempts, minGatewayAttempts, binDeltaThreshold, gwDeltaThreshold, declineIncreaseThreshold) {
  const alerts = [];

  // -----------------------------------------------------------------------
  // 1. BIN trends: last 7d vs days 8-14
  // -----------------------------------------------------------------------
  const binTrendRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') THEN 1 END) AS current_total,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') AND o.order_status IN (2,6,8) THEN 1 END) AS current_approved,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') AND o.order_status = 7 THEN 1 END) AS current_declined,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') THEN 1 END) AS prior_total,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') AND o.order_status IN (2,6,8) THEN 1 END) AS prior_approved,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') AND o.order_status = 7 THEN 1 END) AS prior_declined
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER}
      AND o.acquisition_date >= date('now', '-14 days')
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      AND o.order_status IN (2,6,7,8)
    GROUP BY o.cc_first_6
    HAVING current_total >= ? AND prior_total >= ?
  `, [clientId, minAttempts, minAttempts]);

  // BIN metadata
  const trendBins = binTrendRows.map(r => r.bin);
  const binMetaMap = new Map();
  if (trendBins.length > 0) {
    const binMetaRows = querySql(`
      SELECT bin, issuer_bank, card_brand, card_type
      FROM bin_lookup
      WHERE bin IN (${trendBins.map(() => '?').join(',')})
    `, trendBins);
    for (const r of binMetaRows) {
      binMetaMap.set(r.bin, r);
    }
  }

  const binTrends = [];
  for (const row of binTrendRows) {
    const currentRate = row.current_total > 0
      ? Math.round((row.current_approved / row.current_total) * 10000) / 100 : null;
    const priorRate = row.prior_total > 0
      ? Math.round((row.prior_approved / row.prior_total) * 10000) / 100 : null;

    if (currentRate == null || priorRate == null) continue;

    const delta = Math.round((currentRate - priorRate) * 100) / 100;
    const absDelta = Math.abs(delta);

    if (absDelta < binDeltaThreshold) continue;

    const direction = delta > 0 ? 'improving' : 'degrading';
    const meta = binMetaMap.get(row.bin) || {};

    const trend = {
      bin:          row.bin,
      issuer_bank:  meta.issuer_bank || null,
      card_brand:   meta.card_brand || null,
      card_type:    meta.card_type || null,
      current_rate: currentRate,
      prior_rate:   priorRate,
      delta_pp:     delta,
      direction,
      current_volume: row.current_total,
      prior_volume:   row.prior_total,
      severity:       absDelta >= 15 ? 'P2' : 'P2',
    };

    binTrends.push(trend);

    // Generate alert for significant BIN trends
    alerts.push({
      severity:    'P2',
      type:        'bin_trend',
      entity:      `BIN ${row.bin}${meta.issuer_bank ? ' (' + meta.issuer_bank + ')' : ''}`,
      message:     `BIN ${row.bin} approval rate ${direction}: ${priorRate}% -> ${currentRate}% (${delta > 0 ? '+' : ''}${delta}pp)`,
      delta_pp:    delta,
      direction,
      volume:      row.current_total,
      current_rate: currentRate,
      prior_rate:  priorRate,
    });
  }

  // Sort BIN trends by absolute delta DESC
  binTrends.sort((a, b) => Math.abs(b.delta_pp) - Math.abs(a.delta_pp));

  // -----------------------------------------------------------------------
  // 2. Gateway trends: last 7d vs days 8-14
  // -----------------------------------------------------------------------
  const gwTrendRows = querySql(`
    SELECT
      o.gateway_id,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') THEN 1 END) AS current_total,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') AND o.order_status IN (2,6,8) THEN 1 END) AS current_approved,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') AND o.order_status = 7 THEN 1 END) AS current_declined,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') THEN 1 END) AS prior_total,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') AND o.order_status IN (2,6,8) THEN 1 END) AS prior_approved,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') AND o.order_status = 7 THEN 1 END) AS prior_declined
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER}
      AND o.acquisition_date >= date('now', '-14 days')
      AND o.gateway_id IS NOT NULL
      AND o.order_status IN (2,6,7,8)
    GROUP BY o.gateway_id
    HAVING current_total >= ? AND prior_total >= ?
  `, [clientId, minGatewayAttempts, minGatewayAttempts]);

  // Cascade correction for gateway trends
  const cascGwTrend = querySql(`
    SELECT o.original_gateway_id AS gateway_id,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-7 days') THEN 1 END) AS current_total,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.acquisition_date < date('now', '-7 days') THEN 1 END) AS prior_total
    FROM orders o
    WHERE o.client_id = ? AND ${CASCADE_WHERE} AND ${CLEAN_FILTER}
      AND o.acquisition_date >= date('now', '-14 days')
      AND o.original_gateway_id IS NOT NULL AND o.order_status IN (2,6,7,8)
    GROUP BY o.original_gateway_id
  `, [clientId]);

  for (const cr of cascGwTrend) {
    const match = gwTrendRows.find(r => r.gateway_id === cr.gateway_id);
    if (match) { match.current_total += cr.current_total; match.prior_total += cr.prior_total; }
  }

  // Gateway metadata
  const gwMetaRows = querySql(`
    SELECT gateway_id, gateway_alias, bank_name, processor_name,
           lifecycle_state, gateway_active, exclude_from_analysis
    FROM gateways
    WHERE client_id = ?
  `, [clientId]);
  const gwMetaMap = new Map(gwMetaRows.map(g => [g.gateway_id, g]));

  const gatewayTrends = [];
  for (const row of gwTrendRows) {
    const currentRate = row.current_total > 0
      ? Math.round((row.current_approved / row.current_total) * 10000) / 100 : null;
    const priorRate = row.prior_total > 0
      ? Math.round((row.prior_approved / row.prior_total) * 10000) / 100 : null;

    if (currentRate == null || priorRate == null) continue;

    const delta = Math.round((currentRate - priorRate) * 100) / 100;
    const absDelta = Math.abs(delta);

    if (absDelta < gwDeltaThreshold) continue;

    const direction = delta > 0 ? 'improving' : 'degrading';
    const meta = gwMetaMap.get(row.gateway_id) || {};
    const gwName = formatGatewayName(meta);

    // P1 for high-volume gateway with >15pp drop
    const isHighVolume = row.current_total >= 100;
    const severity = (absDelta >= 15 && isHighVolume && direction === 'degrading') ? 'P1' : 'P2';

    const trend = {
      gateway_id:     row.gateway_id,
      gateway_name:   gwName,
      bank_name:      meta.bank_name || null,
      processor_name: meta.processor_name || null,
      current_rate:   currentRate,
      prior_rate:     priorRate,
      delta_pp:       delta,
      direction,
      current_volume: row.current_total,
      prior_volume:   row.prior_total,
      severity,
    };

    gatewayTrends.push(trend);

    alerts.push({
      severity,
      type:        'gateway_trend',
      entity:      gwName,
      message:     `${gwName} approval rate ${direction}: ${priorRate}% -> ${currentRate}% (${delta > 0 ? '+' : ''}${delta}pp) on ${row.current_total} attempts`,
      delta_pp:    delta,
      direction,
      volume:      row.current_total,
      current_rate: currentRate,
      prior_rate:  priorRate,
    });
  }

  gatewayTrends.sort((a, b) => Math.abs(b.delta_pp) - Math.abs(a.delta_pp));

  // -----------------------------------------------------------------------
  // 3. Decline pattern trends
  // -----------------------------------------------------------------------

  // 3a. Decline reason counts: current 7d vs prior 7d
  const declineCurrentRows = querySql(`
    SELECT
      COALESCE(o.decline_reason, 'Unknown') AS decline_reason,
      COALESCE(o.decline_category, 'unclassified') AS decline_category,
      COUNT(*) AS cnt
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER}
      AND o.acquisition_date >= date('now', '-7 days')
      AND o.order_status = 7
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
    GROUP BY o.decline_reason
    ORDER BY cnt DESC
  `, [clientId]);

  const declinePriorRows = querySql(`
    SELECT
      COALESCE(o.decline_reason, 'Unknown') AS decline_reason,
      COALESCE(o.decline_category, 'unclassified') AS decline_category,
      COUNT(*) AS cnt
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER}
      AND o.acquisition_date >= date('now', '-14 days')
      AND o.acquisition_date < date('now', '-7 days')
      AND o.order_status = 7
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
    GROUP BY o.decline_reason
    ORDER BY cnt DESC
  `, [clientId]);

  const currentDeclineMap = new Map();
  for (const row of declineCurrentRows) {
    currentDeclineMap.set(row.decline_reason, { count: row.cnt, category: row.decline_category });
  }

  const priorDeclineMap = new Map();
  for (const row of declinePriorRows) {
    priorDeclineMap.set(row.decline_reason, { count: row.cnt, category: row.decline_category });
  }

  const declineTrends = [];

  // All unique decline reasons across both periods
  const allReasons = new Set([...currentDeclineMap.keys(), ...priorDeclineMap.keys()]);

  for (const reason of allReasons) {
    const current = currentDeclineMap.get(reason) || { count: 0, category: 'unclassified' };
    const prior = priorDeclineMap.get(reason) || { count: 0, category: 'unclassified' };

    // New decline reason (not in prior period but present now with 3+ occurrences)
    if (prior.count === 0 && current.count >= 3) {
      const trend = {
        decline_reason:   reason,
        decline_category: current.category,
        type:             'new_reason',
        current_count:    current.count,
        prior_count:      0,
        change_pct:       null,
        severity:         'P3',
      };
      declineTrends.push(trend);

      alerts.push({
        severity: 'P3',
        type:     'new_decline_reason',
        entity:   reason,
        message:  `New decline reason detected: "${reason}" (${current.count} occurrences in last 7 days)`,
        current_count: current.count,
        prior_count:   0,
      });
      continue;
    }

    // Increasing decline reason
    if (prior.count > 0 && current.count > 0) {
      const changePct = Math.round(((current.count - prior.count) / prior.count) * 10000) / 100;

      if (changePct > declineIncreaseThreshold) {
        const severity = current.count >= 50 ? 'P2' : 'P4';

        const trend = {
          decline_reason:   reason,
          decline_category: current.category || prior.category,
          type:             'increasing',
          current_count:    current.count,
          prior_count:      prior.count,
          change_pct:       changePct,
          severity,
        };
        declineTrends.push(trend);

        alerts.push({
          severity,
          type:     'increasing_decline',
          entity:   reason,
          message:  `Decline reason "${reason}" increased by ${changePct}%: ${prior.count} -> ${current.count} (week over week)`,
          current_count: current.count,
          prior_count:   prior.count,
          change_pct:    changePct,
        });
      }

      // Decreasing (notable improvement)
      if (changePct < -declineIncreaseThreshold && prior.count >= 5) {
        declineTrends.push({
          decline_reason:   reason,
          decline_category: current.category || prior.category,
          type:             'decreasing',
          current_count:    current.count,
          prior_count:      prior.count,
          change_pct:       changePct,
          severity:         'P4',
        });
      }
    }

    // Disappeared reason (was present, now gone)
    if (prior.count >= 5 && current.count === 0) {
      declineTrends.push({
        decline_reason:   reason,
        decline_category: prior.category,
        type:             'resolved',
        current_count:    0,
        prior_count:      prior.count,
        change_pct:       -100,
        severity:         'P4',
      });
    }
  }

  // Sort decline trends: P-level ascending, then by current count DESC
  const severityOrder = { P1: 1, P2: 2, P3: 3, P4: 4 };
  declineTrends.sort((a, b) => {
    const sevDiff = (severityOrder[a.severity] || 5) - (severityOrder[b.severity] || 5);
    if (sevDiff !== 0) return sevDiff;
    return b.current_count - a.current_count;
  });

  // -----------------------------------------------------------------------
  // 4. Sort alerts by severity (P1 first)
  // -----------------------------------------------------------------------
  alerts.sort((a, b) => {
    const sevDiff = (severityOrder[a.severity] || 5) - (severityOrder[b.severity] || 5);
    if (sevDiff !== 0) return sevDiff;
    return Math.abs(b.delta_pp || 0) - Math.abs(a.delta_pp || 0);
  });

  return {
    binTrends,
    gatewayTrends,
    declineTrends,
    alerts,
  };
}

module.exports = { computeTrendDetection };
