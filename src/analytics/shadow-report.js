/**
 * Shadow-report — SQL helpers that power the Stage 0 shadow dashboard + gate
 * monitoring. All functions are read-only; they never mutate shadow_decisions.
 *
 * Overview of the metrics:
 *   - call volume + reconciled share
 *   - server/daemon/client latency percentiles
 *   - arrived_before_submit rate (did our response land before Place Order?)
 *   - would-be approval rate vs actual approval rate (lift estimate)
 *   - decision-reason distribution (spot fallback spikes)
 *   - concentration (top recommended_gateway_id share, over-collapsing check)
 *   - per-issuer × card_type breakdown
 *   - aging unreconciled queue (has customNotes format drifted?)
 *
 * All helpers are scoped by clientId because Stage 0 shadow runs Kytsan-only
 * today, but we want the API stable for multi-client later.
 */

const { querySql, queryOneSql } = require('../db/connection');

/**
 * Parse a date arg — accepts ISO strings or Date objects; null/undefined → null.
 */
function _toIso(d) {
  if (!d) return null;
  if (d instanceof Date) return d.toISOString();
  return String(d);
}

/**
 * Compute an integer percentile over a sorted-ascending array of numbers.
 * No interpolation — uses the nearest-rank method. Empty array → null.
 */
function _pct(sortedAsc, p) {
  if (!sortedAsc || sortedAsc.length === 0) return null;
  const i = Math.max(0, Math.min(sortedAsc.length - 1, Math.ceil(p * sortedAsc.length) - 1));
  return sortedAsc[i];
}

/**
 * High-level summary for the dashboard top card.
 * @param {number} clientId
 * @param {Object} opts — { fromDate, toDate } (ISO strings or Date)
 */
function getShadowSummary(clientId, opts = {}) {
  const from = _toIso(opts.fromDate);
  const to   = _toIso(opts.toDate);

  const where = ['client_id = ?'];
  const params = [clientId];
  if (from) { where.push('request_received_at >= ?'); params.push(from); }
  if (to)   { where.push('request_received_at <= ?'); params.push(to); }
  const whereSql = 'WHERE ' + where.join(' AND ');

  // Aggregate counters in one pass — SQLite is fine with this for our volume.
  const agg = queryOneSql(
    `SELECT
       COUNT(*)                                            AS total,
       SUM(CASE WHEN reconciled_at IS NOT NULL THEN 1 ELSE 0 END) AS reconciled,
       SUM(CASE WHEN arrived_before_submit = 1 THEN 1 ELSE 0 END) AS arrived_ok,
       SUM(CASE WHEN arrived_before_submit IS NOT NULL THEN 1 ELSE 0 END) AS arrived_reported,
       SUM(CASE WHEN daemon_timed_out = 1 THEN 1 ELSE 0 END) AS daemon_timeouts,
       SUM(CASE WHEN actual_outcome = 'approved' THEN 1 ELSE 0 END) AS actual_approved,
       SUM(CASE WHEN actual_outcome IN ('approved','declined') THEN 1 ELSE 0 END) AS actual_decided,
       AVG(would_have_approved)                            AS mean_would_have_approved,
       SUM(CASE WHEN would_match = 1 THEN 1 ELSE 0 END)    AS would_match_count,
       SUM(CASE WHEN would_match IS NOT NULL THEN 1 ELSE 0 END) AS would_match_scope
     FROM shadow_decisions ${whereSql}`,
    params
  ) || {};

  // Pull latency arrays once, compute percentiles in JS — SQLite lacks percentile_cont.
  const latencies = querySql(
    `SELECT latency_ms_server, daemon_latency_ms, latency_ms_client
       FROM shadow_decisions ${whereSql}`,
    params
  );
  const srv = latencies.map(r => r.latency_ms_server).filter(x => x != null).sort((a, b) => a - b);
  const dmn = latencies.map(r => r.daemon_latency_ms).filter(x => x != null).sort((a, b) => a - b);
  const cli = latencies.map(r => r.latency_ms_client).filter(x => x != null).sort((a, b) => a - b);

  const total = agg.total || 0;
  const reconciled = agg.reconciled || 0;
  const actualDecided = agg.actual_decided || 0;
  const arrivedReported = agg.arrived_reported || 0;
  const wouldMatchScope = agg.would_match_scope || 0;

  const actualApprovalRate = actualDecided > 0 ? (agg.actual_approved || 0) / actualDecided : null;
  const liftPp = (agg.mean_would_have_approved != null && actualApprovalRate != null)
    ? (agg.mean_would_have_approved - actualApprovalRate)
    : null;

  return {
    total,
    reconciled,
    reconciled_pct: total > 0 ? reconciled / total : null,
    arrived_before_submit_pct: arrivedReported > 0 ? (agg.arrived_ok || 0) / arrivedReported : null,
    arrived_reported_count: arrivedReported,
    daemon_timeout_pct: total > 0 ? (agg.daemon_timeouts || 0) / total : null,
    actual_approval_rate: actualApprovalRate,
    mean_would_have_approved: agg.mean_would_have_approved != null ? Number(agg.mean_would_have_approved) : null,
    lift_pp: liftPp,
    would_match_pct: wouldMatchScope > 0 ? (agg.would_match_count || 0) / wouldMatchScope : null,
    latency_ms_server: { p50: _pct(srv, 0.50), p95: _pct(srv, 0.95), p99: _pct(srv, 0.99), n: srv.length },
    latency_ms_daemon: { p50: _pct(dmn, 0.50), p95: _pct(dmn, 0.95), p99: _pct(dmn, 0.99), n: dmn.length },
    latency_ms_client: { p50: _pct(cli, 0.50), p95: _pct(cli, 0.95), p99: _pct(cli, 0.99), n: cli.length },
  };
}

/**
 * Distribution of decision reasons — spot when fallbacks spike (daemon flaky,
 * lookup table missing for a combo, etc.).
 */
function getReasonBreakdown(clientId, opts = {}) {
  const from = _toIso(opts.fromDate);
  const to   = _toIso(opts.toDate);
  const where = ['client_id = ?'];
  const params = [clientId];
  if (from) { where.push('request_received_at >= ?'); params.push(from); }
  if (to)   { where.push('request_received_at <= ?'); params.push(to); }
  const whereSql = 'WHERE ' + where.join(' AND ');

  return querySql(
    `SELECT reason, COUNT(*) AS n
       FROM shadow_decisions ${whereSql}
      GROUP BY reason
      ORDER BY n DESC`,
    params
  );
}

/**
 * Concentration of recommended gateway — gate E in Phase 5.
 * Returns [{ gateway_id, processor, n, share }] sorted by n desc.
 */
function getConcentration(clientId, opts = {}) {
  const from = _toIso(opts.fromDate);
  const to   = _toIso(opts.toDate);
  const where = ['client_id = ?'];
  const params = [clientId];
  if (from) { where.push('request_received_at >= ?'); params.push(from); }
  if (to)   { where.push('request_received_at <= ?'); params.push(to); }
  const whereSql = 'WHERE ' + where.join(' AND ');

  const rows = querySql(
    `SELECT recommended_gateway_id AS gateway_id,
            recommended_processor  AS processor,
            COUNT(*) AS n
       FROM shadow_decisions ${whereSql}
      GROUP BY recommended_gateway_id, recommended_processor
      ORDER BY n DESC`,
    params
  );
  const total = rows.reduce((a, r) => a + r.n, 0);
  return rows.map(r => ({ ...r, share: total > 0 ? r.n / total : null }));
}

/**
 * Bucketed breakdown by issuer × card_type. Joins bin_lookup for the
 * tx-level issuer/card classification.
 */
function getShadowByIssuer(clientId, opts = {}) {
  const from = _toIso(opts.fromDate);
  const to   = _toIso(opts.toDate);
  const where = ['s.client_id = ?'];
  const params = [clientId];
  if (from) { where.push('s.request_received_at >= ?'); params.push(from); }
  if (to)   { where.push('s.request_received_at <= ?'); params.push(to); }
  const whereSql = 'WHERE ' + where.join(' AND ');

  return querySql(
    `SELECT COALESCE(b.issuer_bank, 'UNKNOWN') AS issuer_bank,
            COALESCE(b.card_type, 'UNKNOWN')   AS card_type,
            COUNT(*)                           AS n,
            SUM(CASE WHEN s.actual_outcome = 'approved' THEN 1 ELSE 0 END) AS actual_approved,
            SUM(CASE WHEN s.actual_outcome IN ('approved','declined') THEN 1 ELSE 0 END) AS actual_decided,
            AVG(s.would_have_approved)         AS mean_would_have_approved,
            SUM(CASE WHEN s.would_match = 1 THEN 1 ELSE 0 END)     AS would_match_n,
            SUM(CASE WHEN s.would_match IS NOT NULL THEN 1 ELSE 0 END) AS would_match_scope
       FROM shadow_decisions s
  LEFT JOIN bin_lookup b ON b.bin = s.bin
       ${whereSql}
      GROUP BY issuer_bank, card_type
      ORDER BY n DESC`,
    params
  );
}

/**
 * Aging queue — rows older than `hours` that still have no reconciled_at.
 * Used to detect drift in the customNotes pattern or stuck sync pipelines.
 */
function getUnreconciledAging(clientId, hours = 48) {
  const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  const rows = querySql(
    `SELECT shadow_id, bin, recommended_processor, request_received_at
       FROM shadow_decisions
      WHERE client_id = ?
        AND reconciled_at IS NULL
        AND request_received_at <= ?
      ORDER BY request_received_at ASC
      LIMIT 500`,
    [clientId, cutoff]
  );
  const countRow = queryOneSql(
    `SELECT COUNT(*) AS n
       FROM shadow_decisions
      WHERE client_id = ?
        AND reconciled_at IS NULL
        AND request_received_at <= ?`,
    [clientId, cutoff]
  );
  return {
    total: countRow ? countRow.n : 0,
    older_than_hours: hours,
    sample: rows,
  };
}

/**
 * Recent decisions (for table at bottom of dashboard). Returns the
 * candidate_pool_json as parsed objects for UI convenience.
 */
function getRecentDecisions(clientId, limit = 100) {
  const rows = querySql(
    `SELECT shadow_id, bin, recommended_gateway_id, recommended_processor,
            reason, confidence, latency_ms_server, daemon_latency_ms,
            daemon_timed_out, lookup_has_data, lookup_best_rate,
            would_have_approved,
            actual_gateway_id, actual_processor, actual_outcome, would_match,
            request_received_at, reconciled_at, candidate_pool_json,
            ai_recommended_gateway_id, ai_score
       FROM shadow_decisions
      WHERE client_id = ?
      ORDER BY request_received_at DESC
      LIMIT ?`,
    [clientId, limit]
  );
  return rows.map(r => {
    let pool = null;
    try { pool = r.candidate_pool_json ? JSON.parse(r.candidate_pool_json) : null; } catch { /* leave null */ }
    return { ...r, candidate_pool: pool, candidate_pool_json: undefined };
  });
}

module.exports = {
  getShadowSummary,
  getReasonBreakdown,
  getConcentration,
  getShadowByIssuer,
  getUnreconciledAging,
  getRecentDecisions,
};
