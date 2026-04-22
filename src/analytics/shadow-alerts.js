/**
 * Shadow Alert Engine — Layer 1 deterministic alerts for shadow routing monitoring.
 *
 * Checks hard-coded thresholds against shadow_decisions + orders data to catch
 * known failure patterns that should never go unnoticed. Designed to run after
 * every daily sync (post-reconciliation).
 *
 * All functions are read-only — they never mutate shadow_decisions or orders.
 * Safe to call multiple times (idempotent).
 *
 * Each alert returns:
 *   { alert_name, severity, message, value, threshold, triggered }
 */

const { querySql, queryOneSql } = require('../db/connection');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** ISO timestamp N days ago. */
function _daysAgo(n) {
  return new Date(Date.now() - n * 86400000).toISOString();
}

/** ISO timestamp N hours ago. */
function _hoursAgo(n) {
  return new Date(Date.now() - n * 3600000).toISOString();
}

/** Build a standard alert object. */
function _alert(name, severity, message, value, threshold, triggered) {
  return { alert_name: name, severity, message, value, threshold, triggered };
}

// ---------------------------------------------------------------------------
// Individual alert checks
// ---------------------------------------------------------------------------

/**
 * 1. Closed MID approaching — consecutive processor-level declines per MID.
 *
 * For each MID that has reconciled shadow decisions, look at the last 20
 * reconciled rows. Count the longest streak of consecutive non-customer
 * (processor/soft) declines from the tail end. If >= 3, fire.
 *
 * Uses idx_shadow_client_time for the ORDER BY.
 */
function checkClosedMidApproaching(clientId) {
  const THRESHOLD = 3;
  const alerts = [];

  // Get all MIDs that have recent reconciled shadow decisions
  const mids = querySql(`
    SELECT DISTINCT recommended_gateway_id AS gw_id, recommended_processor AS proc
    FROM shadow_decisions
    WHERE client_id = ? AND reconciled_at IS NOT NULL
      AND recommended_gateway_id IS NOT NULL
  `, [clientId]);

  for (const mid of mids) {
    // Last 20 reconciled rows for this MID, newest first
    const rows = querySql(`
      SELECT actual_outcome,
             actual_sticky_order_id
      FROM shadow_decisions
      WHERE client_id = ?
        AND recommended_gateway_id = ?
        AND reconciled_at IS NOT NULL
        AND actual_outcome IN ('approved', 'declined')
      ORDER BY request_received_at DESC
      LIMIT 20
    `, [clientId, mid.gw_id]);

    if (rows.length === 0) continue;

    // For declined rows, check if it's a processor/gateway-level decline
    // by joining to orders.decline_category. We need the order IDs.
    const orderIds = rows
      .filter(r => r.actual_outcome === 'declined' && r.actual_sticky_order_id)
      .map(r => r.actual_sticky_order_id);

    const nonCustomerDeclineSet = new Set();
    if (orderIds.length > 0) {
      // Batch lookup decline categories — processor and soft declines signal MID issues
      const placeholders = orderIds.map(() => '?').join(',');
      const catRows = querySql(`
        SELECT order_id, decline_category
        FROM orders
        WHERE client_id = ? AND order_id IN (${placeholders})
          AND decline_category IN ('processor', 'soft')
      `, [clientId, ...orderIds]);
      for (const cr of catRows) nonCustomerDeclineSet.add(cr.order_id);
    }

    // Count consecutive gateway-type declines from the tail (newest first)
    let consecutive = 0;
    for (const r of rows) {
      if (r.actual_outcome === 'declined' && nonCustomerDeclineSet.has(r.actual_sticky_order_id)) {
        consecutive++;
      } else {
        break; // streak broken
      }
    }

    if (consecutive >= THRESHOLD) {
      alerts.push(_alert(
        'closed_mid_approaching',
        'warning',
        `MID ${mid.gw_id} (${mid.proc || 'unknown'}) has ${consecutive} consecutive processor/soft declines — possible closure`,
        consecutive,
        THRESHOLD,
        true
      ));
    }
  }

  // If no MID triggered, return a single non-triggered summary
  if (alerts.length === 0) {
    return [_alert('closed_mid_approaching', 'warning', 'No MIDs showing consecutive decline streaks', 0, THRESHOLD, false)];
  }
  return alerts;
}

/**
 * 2. MID capacity — placeholder.
 * TODO: Needs capacity data (global_monthly_cap vs monthly_sales on gateways table).
 * Will check when cap data is populated.
 */
function checkMidCapacity(clientId) {
  const THRESHOLD = 0.85; // 85% of cap

  // Check if any gateway has cap data
  const hasCapData = queryOneSql(`
    SELECT COUNT(*) AS n FROM gateways
    WHERE client_id = ? AND global_monthly_cap IS NOT NULL AND global_monthly_cap > 0
  `, [clientId]);

  if (!hasCapData || hasCapData.n === 0) {
    return [_alert('mid_capacity', 'warning', 'TODO: No capacity data available — skipping', null, THRESHOLD, false)];
  }

  // If we do have cap data, check utilization
  const alerts = [];
  const gwRows = querySql(`
    SELECT gateway_id, processor_name, global_monthly_cap, monthly_sales
    FROM gateways
    WHERE client_id = ? AND global_monthly_cap IS NOT NULL AND global_monthly_cap > 0
      AND gateway_active = 1
  `, [clientId]);

  for (const gw of gwRows) {
    const utilization = (gw.monthly_sales || 0) / gw.global_monthly_cap;
    if (utilization >= THRESHOLD) {
      alerts.push(_alert(
        'mid_capacity',
        'warning',
        `MID ${gw.gateway_id} (${gw.processor_name || 'unknown'}) at ${(utilization * 100).toFixed(1)}% capacity ($${(gw.monthly_sales || 0).toLocaleString()} / $${gw.global_monthly_cap.toLocaleString()})`,
        utilization,
        THRESHOLD,
        true
      ));
    }
  }

  if (alerts.length === 0) {
    return [_alert('mid_capacity', 'warning', 'All MIDs within capacity limits', 0, THRESHOLD, false)];
  }
  return alerts;
}

/**
 * 3. MID degradation — approval rate of last 20 non-customer-decline attempts
 * vs lookup rate, per MID.
 *
 * For each MID with reconciled shadow data, compute recent approval rate
 * (excluding issuer declines) and compare against the lookup_best_rate
 * stored in the shadow decision. If >15pp below, fire.
 */
function checkMidDegradation(clientId) {
  const THRESHOLD_PP = 15; // percentage points
  const alerts = [];

  const mids = querySql(`
    SELECT DISTINCT recommended_gateway_id AS gw_id, recommended_processor AS proc
    FROM shadow_decisions
    WHERE client_id = ? AND reconciled_at IS NOT NULL
      AND recommended_gateway_id IS NOT NULL
  `, [clientId]);

  for (const mid of mids) {
    // Last 20 reconciled decisions for this MID that have a definitive outcome
    const rows = querySql(`
      SELECT actual_outcome, actual_sticky_order_id, lookup_best_rate
      FROM shadow_decisions
      WHERE client_id = ?
        AND recommended_gateway_id = ?
        AND reconciled_at IS NOT NULL
        AND actual_outcome IN ('approved', 'declined')
      ORDER BY request_received_at DESC
      LIMIT 20
    `, [clientId, mid.gw_id]);

    if (rows.length < 5) continue; // not enough data

    // Exclude issuer-level declines to get routing-relevant approval rate
    const orderIds = rows
      .filter(r => r.actual_outcome === 'declined' && r.actual_sticky_order_id)
      .map(r => r.actual_sticky_order_id);

    const issuerDeclineSet = new Set();
    if (orderIds.length > 0) {
      const placeholders = orderIds.map(() => '?').join(',');
      const catRows = querySql(`
        SELECT order_id FROM orders
        WHERE client_id = ? AND order_id IN (${placeholders})
          AND decline_category = 'issuer'
      `, [clientId, ...orderIds]);
      for (const cr of catRows) issuerDeclineSet.add(cr.order_id);
    }

    // Filter out issuer declines
    const routingRows = rows.filter(r =>
      !(r.actual_outcome === 'declined' && issuerDeclineSet.has(r.actual_sticky_order_id))
    );

    if (routingRows.length < 5) continue;

    const approved = routingRows.filter(r => r.actual_outcome === 'approved').length;
    const actualRate = approved / routingRows.length;

    // Use the average lookup_best_rate from these rows as the baseline
    const lookupRates = rows.map(r => r.lookup_best_rate).filter(r => r != null && r > 0);
    if (lookupRates.length === 0) continue;
    const avgLookupRate = lookupRates.reduce((a, b) => a + b, 0) / lookupRates.length;

    const gapPp = (avgLookupRate - actualRate) * 100;
    if (gapPp >= THRESHOLD_PP) {
      alerts.push(_alert(
        'mid_degradation',
        'warning',
        `MID ${mid.gw_id} (${mid.proc || 'unknown'}) actual ${(actualRate * 100).toFixed(1)}% vs lookup ${(avgLookupRate * 100).toFixed(1)}% (${gapPp.toFixed(1)}pp gap, ${routingRows.length} routing attempts)`,
        gapPp,
        THRESHOLD_PP,
        true
      ));
    }
  }

  if (alerts.length === 0) {
    return [_alert('mid_degradation', 'warning', 'No MIDs showing significant degradation vs lookup', 0, THRESHOLD_PP, false)];
  }
  return alerts;
}

/**
 * 4. Unknown BIN hot — BINs in shadow_decisions not found in bin_lookup.
 * Grouped by count; fires if any BIN seen 5+ times without lookup data.
 */
function checkUnknownBinHot(clientId) {
  const THRESHOLD = 5;

  const rows = querySql(`
    SELECT s.bin, COUNT(*) AS n
    FROM shadow_decisions s
    LEFT JOIN bin_lookup b ON b.bin = s.bin
    WHERE s.client_id = ? AND b.bin IS NULL
    GROUP BY s.bin
    HAVING COUNT(*) >= ?
    ORDER BY n DESC
    LIMIT 20
  `, [clientId, THRESHOLD]);

  if (rows.length === 0) {
    return [_alert('unknown_bin_hot', 'info', 'No unknown BINs above threshold', 0, THRESHOLD, false)];
  }

  return rows.map(r => _alert(
    'unknown_bin_hot',
    'info',
    `BIN ${r.bin} seen ${r.n} times with no lookup data — consider enriching`,
    r.n,
    THRESHOLD,
    true
  ));
}

/**
 * 5. Exploration gate — exploration MIDs approaching the 50-sample evaluation
 * threshold. Fires at 40+ samples so the operator knows a gate check is near.
 */
function checkExplorationGate(clientId) {
  const THRESHOLD = 40;
  const GATE = 50;

  const rows = querySql(`
    SELECT s.recommended_gateway_id AS gw_id, s.recommended_processor AS proc,
           COUNT(*) AS n
    FROM shadow_decisions s
    JOIN gateways g ON g.client_id = s.client_id AND g.gateway_id = s.recommended_gateway_id
    WHERE s.client_id = ?
      AND g.is_exploration = 1
      AND s.reconciled_at IS NOT NULL
    GROUP BY s.recommended_gateway_id, s.recommended_processor
    HAVING COUNT(*) >= ?
    ORDER BY n DESC
  `, [clientId, THRESHOLD]);

  if (rows.length === 0) {
    return [_alert('exploration_gate', 'info', 'No exploration MIDs near evaluation threshold', 0, THRESHOLD, false)];
  }

  return rows.map(r => _alert(
    'exploration_gate',
    'info',
    `Exploration MID ${r.gw_id} (${r.proc || 'unknown'}) has ${r.n}/${GATE} samples — ${r.n >= GATE ? 'READY for evaluation' : `${GATE - r.n} more needed`}`,
    r.n,
    THRESHOLD,
    true
  ));
}

/**
 * 6. Concentration — any single MID recommended for >70% of last 7 days' decisions.
 * Over-concentration signals the engine is collapsing to one processor.
 */
function checkConcentration(clientId) {
  const THRESHOLD = 0.70;
  const cutoff = _daysAgo(7);

  const total = queryOneSql(`
    SELECT COUNT(*) AS n FROM shadow_decisions
    WHERE client_id = ? AND request_received_at >= ?
  `, [clientId, cutoff]);

  if (!total || total.n < 10) {
    return [_alert('concentration', 'warning', 'Too few shadow decisions in last 7 days to assess concentration', 0, THRESHOLD, false)];
  }

  const rows = querySql(`
    SELECT recommended_gateway_id AS gw_id, recommended_processor AS proc,
           COUNT(*) AS n
    FROM shadow_decisions
    WHERE client_id = ? AND request_received_at >= ?
    GROUP BY recommended_gateway_id, recommended_processor
    ORDER BY n DESC
  `, [clientId, cutoff]);

  const alerts = [];
  for (const r of rows) {
    const share = r.n / total.n;
    if (share > THRESHOLD) {
      alerts.push(_alert(
        'concentration',
        'warning',
        `MID ${r.gw_id} (${r.proc || 'unknown'}) recommended for ${(share * 100).toFixed(1)}% of last 7 days (${r.n}/${total.n}) — engine may be over-concentrated`,
        share,
        THRESHOLD,
        true
      ));
    }
  }

  if (alerts.length === 0) {
    return [_alert('concentration', 'warning', 'No MID concentration above 70% threshold', 0, THRESHOLD, false)];
  }
  return alerts;
}

/**
 * 7. Latency spike — server-side p95 latency in last 24h exceeds 200ms.
 * Pulls raw latency values and computes percentile in JS (SQLite lacks
 * percentile_cont).
 */
function checkLatencySpike(clientId) {
  const THRESHOLD_MS = 200;
  const cutoff = _hoursAgo(24);

  const rows = querySql(`
    SELECT latency_ms_server
    FROM shadow_decisions
    WHERE client_id = ? AND request_received_at >= ?
      AND latency_ms_server IS NOT NULL
    ORDER BY latency_ms_server ASC
  `, [clientId, cutoff]);

  if (rows.length < 5) {
    return [_alert('latency_spike', 'warning', 'Too few latency samples in last 24h', null, THRESHOLD_MS, false)];
  }

  const sorted = rows.map(r => r.latency_ms_server);
  const p95Idx = Math.max(0, Math.min(sorted.length - 1, Math.ceil(0.95 * sorted.length) - 1));
  const p95 = sorted[p95Idx];

  return [_alert(
    'latency_spike',
    'warning',
    p95 > THRESHOLD_MS
      ? `Server p95 latency ${p95}ms exceeds ${THRESHOLD_MS}ms threshold (${rows.length} samples last 24h)`
      : `Server p95 latency ${p95}ms within limits (${rows.length} samples last 24h)`,
    p95,
    THRESHOLD_MS,
    p95 > THRESHOLD_MS
  )];
}

/**
 * 8. Daemon timeout rate — percentage of shadow decisions where the AI daemon
 * timed out in the last 7 days.
 */
function checkDaemonTimeoutRate(clientId) {
  const THRESHOLD = 0.03; // 3%
  const cutoff = _daysAgo(7);

  const agg = queryOneSql(`
    SELECT COUNT(*) AS total,
           SUM(CASE WHEN daemon_timed_out = 1 THEN 1 ELSE 0 END) AS timeouts
    FROM shadow_decisions
    WHERE client_id = ? AND request_received_at >= ?
  `, [clientId, cutoff]);

  if (!agg || agg.total < 10) {
    return [_alert('daemon_timeout_rate', 'warning', 'Too few shadow decisions in last 7 days to assess timeout rate', null, THRESHOLD, false)];
  }

  const rate = (agg.timeouts || 0) / agg.total;

  return [_alert(
    'daemon_timeout_rate',
    'warning',
    rate > THRESHOLD
      ? `Daemon timeout rate ${(rate * 100).toFixed(1)}% exceeds ${(THRESHOLD * 100).toFixed(0)}% threshold (${agg.timeouts}/${agg.total} last 7 days)`
      : `Daemon timeout rate ${(rate * 100).toFixed(1)}% within limits (${agg.timeouts || 0}/${agg.total} last 7 days)`,
    rate,
    THRESHOLD,
    rate > THRESHOLD
  )];
}

/**
 * 9. Reconciliation gap — percentage of shadow decisions older than 3 days
 * that remain unreconciled. High rates signal broken customNotes parsing
 * or sync pipeline issues.
 */
function checkReconciliationGap(clientId) {
  const THRESHOLD = 0.30; // 30%
  const cutoff = _daysAgo(3);

  const agg = queryOneSql(`
    SELECT COUNT(*) AS total,
           SUM(CASE WHEN reconciled_at IS NULL THEN 1 ELSE 0 END) AS unreconciled
    FROM shadow_decisions
    WHERE client_id = ? AND request_received_at <= ?
  `, [clientId, cutoff]);

  if (!agg || agg.total === 0) {
    return [_alert('reconciliation_gap', 'warning', 'No shadow decisions older than 3 days to check', null, THRESHOLD, false)];
  }

  const rate = (agg.unreconciled || 0) / agg.total;

  return [_alert(
    'reconciliation_gap',
    'warning',
    rate > THRESHOLD
      ? `${(rate * 100).toFixed(1)}% of shadow decisions older than 3 days are unreconciled (${agg.unreconciled}/${agg.total}) — check customNotes parsing or sync pipeline`
      : `Reconciliation gap ${(rate * 100).toFixed(1)}% within limits (${agg.unreconciled || 0}/${agg.total} older than 3 days)`,
    rate,
    THRESHOLD,
    rate > THRESHOLD
  )];
}

/**
 * 10. Fallback rate — combined lookup_only_fallback + daemon_timeout reason
 * rate over last 7 days. High rates mean the AI daemon is frequently
 * unavailable and the engine is running on lookup tables alone.
 */
function checkFallbackRate(clientId) {
  const THRESHOLD = 0.03; // 3%
  const cutoff = _daysAgo(7);

  const agg = queryOneSql(`
    SELECT COUNT(*) AS total,
           SUM(CASE WHEN reason IN ('lookup_only_fallback', 'daemon_timeout') THEN 1 ELSE 0 END) AS fallbacks
    FROM shadow_decisions
    WHERE client_id = ? AND request_received_at >= ?
  `, [clientId, cutoff]);

  if (!agg || agg.total < 10) {
    return [_alert('fallback_rate', 'warning', 'Too few shadow decisions in last 7 days to assess fallback rate', null, THRESHOLD, false)];
  }

  const rate = (agg.fallbacks || 0) / agg.total;

  return [_alert(
    'fallback_rate',
    'warning',
    rate > THRESHOLD
      ? `Fallback rate ${(rate * 100).toFixed(1)}% exceeds ${(THRESHOLD * 100).toFixed(0)}% threshold (${agg.fallbacks}/${agg.total} decisions used lookup-only or timed out)`
      : `Fallback rate ${(rate * 100).toFixed(1)}% within limits (${agg.fallbacks || 0}/${agg.total} last 7 days)`,
    rate,
    THRESHOLD,
    rate > THRESHOLD
  )];
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/**
 * Run all shadow alerts for a given client.
 * @param {object} db — unused (kept for interface consistency; uses global connection)
 * @param {number} clientId
 * @returns {Array<Object>} flat array of all alert results (triggered and non-triggered)
 */
function runShadowAlerts(db, clientId) {
  const allAlerts = [];

  // Quick check: any shadow data at all?
  const count = queryOneSql(
    'SELECT COUNT(*) AS n FROM shadow_decisions WHERE client_id = ?',
    [clientId]
  );
  if (!count || count.n === 0) {
    return [_alert('shadow_alerts', 'info', 'No shadow decisions found for this client — alerts skipped', 0, 0, false)];
  }

  allAlerts.push(...checkClosedMidApproaching(clientId));
  allAlerts.push(...checkMidCapacity(clientId));
  allAlerts.push(...checkMidDegradation(clientId));
  allAlerts.push(...checkUnknownBinHot(clientId));
  allAlerts.push(...checkExplorationGate(clientId));
  allAlerts.push(...checkConcentration(clientId));
  allAlerts.push(...checkLatencySpike(clientId));
  allAlerts.push(...checkDaemonTimeoutRate(clientId));
  allAlerts.push(...checkReconciliationGap(clientId));
  allAlerts.push(...checkFallbackRate(clientId));

  return allAlerts;
}

module.exports = {
  runShadowAlerts,
  // Exported individually for testing / one-off checks
  checkClosedMidApproaching,
  checkMidCapacity,
  checkMidDegradation,
  checkUnknownBinHot,
  checkExplorationGate,
  checkConcentration,
  checkLatencySpike,
  checkDaemonTimeoutRate,
  checkReconciliationGap,
  checkFallbackRate,
};
