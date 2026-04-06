const { querySql, runSql, saveDb, transaction } = require('../db/connection');

/**
 * Optimization Window Detection Engine.
 *
 * Fires ONLY when BOTH conditions are simultaneously true:
 * 1. Tier 1 BIN+MID approval rate dropped below configurable threshold
 * 2. A better-performing MID exists for same BIN+MCC with statistical confidence
 *
 * Excludes issuer-side declines from analysis (already handled in performance matrix).
 */

const DEFAULT_THRESHOLD_DROP = 5.0;  // % below average to trigger
const MIN_SAMPLE_SIZE = 30;          // Minimum transactions for statistical confidence
const MIN_CONFIDENCE = 0.75;         // Z-test confidence threshold

/**
 * Detect optimization windows for a client.
 * Creates recommendations for BIN+MID combos that should be rerouted.
 */
function detectOptimizationWindows(clientId) {
  console.log(`[Optimizer] Detecting optimization windows for client ${clientId}...`);

  // Get client config
  const client = querySql('SELECT alert_threshold FROM clients WHERE id = ?', [clientId]);
  const threshold = client[0]?.alert_threshold || DEFAULT_THRESHOLD_DROP;

  // Get the latest performance data, Tier 1 only
  const latest = querySql('SELECT MAX(period_end) as d FROM bin_performance WHERE client_id = ?', [clientId]);
  const latestDate = latest[0]?.d;
  if (!latestDate) {
    console.log('[Optimizer] No performance data. Run buildPerformanceMatrix first.');
    return { recommendations: 0 };
  }

  // Get all Tier 1 BIN+MID performance entries
  const tier1 = querySql(`
    SELECT bp.*, g.mcc_code as gw_mcc, g.lifecycle_state
    FROM bin_performance bp
    JOIN gateways g ON g.client_id = bp.client_id AND g.gateway_id = bp.gateway_id
    WHERE bp.client_id = ? AND bp.period_end = ? AND bp.tier = 1
      AND g.lifecycle_state = 'active'
      AND bp.total_transactions >= ?
  `, [clientId, latestDate, MIN_SAMPLE_SIZE]);

  if (tier1.length === 0) {
    console.log('[Optimizer] No Tier 1 BIN+MID combos with sufficient data.');
    return { recommendations: 0 };
  }

  // Group by BIN to find alternatives
  const binGroups = new Map();
  for (const entry of tier1) {
    const key = entry.bin;
    if (!binGroups.has(key)) binGroups.set(key, []);
    binGroups.get(key).push(entry);
  }

  // Also get all active MID performance for same MCC codes (for finding better alternatives)
  const allActive = querySql(`
    SELECT bp.*, g.mcc_code as gw_mcc
    FROM bin_performance bp
    JOIN gateways g ON g.client_id = bp.client_id AND g.gateway_id = bp.gateway_id
    WHERE bp.client_id = ? AND bp.period_end = ?
      AND g.lifecycle_state = 'active'
      AND bp.total_transactions >= ?
  `, [clientId, latestDate, Math.floor(MIN_SAMPLE_SIZE / 2)]);

  // Build MCC-level BIN performance lookup
  const mccBinPerf = new Map();
  for (const entry of allActive) {
    const mcc = entry.gw_mcc || 'unknown';
    const key = `${entry.bin}|${mcc}`;
    if (!mccBinPerf.has(key)) mccBinPerf.set(key, []);
    mccBinPerf.get(key).push(entry);
  }

  let recommendationCount = 0;

  transaction(() => {
    for (const [bin, entries] of binGroups) {
      // Calculate average weighted approval rate for this BIN across all MIDs
      const totalWeighted = entries.reduce((s, e) => s + (e.weighted_approval_rate * e.total_transactions), 0);
      const totalTx = entries.reduce((s, e) => s + e.total_transactions, 0);
      const avgRate = totalTx > 0 ? totalWeighted / totalTx : 0;

      for (const current of entries) {
        // Condition 1: Is this BIN+MID below threshold?
        const drop = avgRate - current.weighted_approval_rate;
        if (drop < threshold) continue;

        // Find better alternatives with same MCC
        const mcc = current.gw_mcc || current.mcc_code || 'unknown';
        const alternatives = (mccBinPerf.get(`${bin}|${mcc}`) || [])
          .filter(alt => alt.gateway_id !== current.gateway_id)
          .filter(alt => alt.weighted_approval_rate > current.weighted_approval_rate)
          .sort((a, b) => b.weighted_approval_rate - a.weighted_approval_rate);

        if (alternatives.length === 0) continue;

        const best = alternatives[0];

        // Condition 2: Statistical confidence check (two-proportion z-test)
        const confidence = calculateConfidence(
          current.approved_count - current.issuer_declines,
          current.total_transactions - current.issuer_declines,
          best.approved_count - best.issuer_declines,
          best.total_transactions - best.issuer_declines
        );

        if (confidence < MIN_CONFIDENCE) continue;

        // BOTH conditions met — create recommendation
        const lift = best.weighted_approval_rate - current.weighted_approval_rate;
        const priorityScore = current.total_transactions * (lift / 100);

        const summary = `BIN ${bin} (${current.cc_type || 'unknown'}) on Gateway ${current.gateway_id} ` +
          `has ${current.weighted_approval_rate.toFixed(1)}% approval (${drop.toFixed(1)}pp below avg). ` +
          `Gateway ${best.gateway_id} shows ${best.weighted_approval_rate.toFixed(1)}% for same BIN ` +
          `(+${lift.toFixed(1)}pp lift, ${(confidence * 100).toFixed(0)}% confidence, ${current.total_transactions} txns).`;

        // Check for existing open recommendation for same BIN+MID
        const existing = querySql(
          'SELECT id FROM recommendations WHERE client_id = ? AND bin = ? AND current_gateway_id = ? AND status = ?',
          [clientId, bin, current.gateway_id, 'open']
        );

        if (existing.length > 0) {
          // Update existing recommendation
          runSql(`
            UPDATE recommendations SET
              recommended_gateway_id = ?, current_approval_rate = ?,
              recommended_approval_rate = ?, expected_lift = ?,
              confidence_score = ?, transaction_volume = ?,
              priority_score = ?, summary = ?, updated_at = datetime('now')
            WHERE id = ?
          `, [
            best.gateway_id, current.weighted_approval_rate,
            best.weighted_approval_rate, lift,
            confidence, current.total_transactions,
            priorityScore, summary, existing[0].id,
          ]);
        } else {
          runSql(`
            INSERT INTO recommendations (
              client_id, bin, cc_type, mcc_code, transaction_type,
              current_gateway_id, recommended_gateway_id,
              current_approval_rate, recommended_approval_rate,
              expected_lift, confidence_score, transaction_volume,
              priority_score, summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `, [
            clientId, bin, current.cc_type, mcc, current.transaction_type,
            current.gateway_id, best.gateway_id,
            current.weighted_approval_rate, best.weighted_approval_rate,
            lift, confidence, current.total_transactions,
            priorityScore, summary,
          ]);
        }

        recommendationCount++;
      }
    }
  });

  console.log(`[Optimizer] Generated ${recommendationCount} recommendations.`);
  return { recommendations: recommendationCount };
}

/**
 * Two-proportion z-test for comparing approval rates.
 * Returns confidence level (0-1) that rate2 > rate1.
 */
function calculateConfidence(successes1, total1, successes2, total2) {
  if (total1 < 5 || total2 < 5) return 0;

  const p1 = successes1 / total1;
  const p2 = successes2 / total2;
  const pooled = (successes1 + successes2) / (total1 + total2);

  if (pooled === 0 || pooled === 1) return 0;

  const se = Math.sqrt(pooled * (1 - pooled) * (1/total1 + 1/total2));
  if (se === 0) return 0;

  const z = (p2 - p1) / se;

  // Convert z-score to one-sided p-value using approximation
  // Φ(z) ≈ using rational approximation
  return normalCDF(z);
}

/**
 * Standard normal CDF approximation (Abramowitz and Stegun).
 */
function normalCDF(z) {
  if (z < -8) return 0;
  if (z > 8) return 1;

  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;

  const sign = z < 0 ? -1 : 1;
  const x = Math.abs(z) / Math.sqrt(2);

  const t = 1.0 / (1.0 + p * x);
  const y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-x * x);

  return 0.5 * (1.0 + sign * y);
}

/**
 * Detect MID degradation and create alerts.
 */
function detectMidDegradation(clientId) {
  console.log(`[Optimizer] Checking for MID degradation for client ${clientId}...`);

  const client = querySql('SELECT alert_threshold FROM clients WHERE id = ?', [clientId]);
  const threshold = client[0]?.alert_threshold || DEFAULT_THRESHOLD_DROP;

  // Compare current 7-day approval rate vs 30-day baseline
  const mids = querySql(`
    SELECT
      gateway_id,
      SUM(CASE WHEN acquisition_date >= date('now', '-7 days') AND order_status IN (2,6,8) THEN 1 ELSE 0 END) as recent_approved,
      SUM(CASE WHEN acquisition_date >= date('now', '-7 days') AND order_status IN (2,6,7,8) THEN 1 ELSE 0 END) as recent_total,
      SUM(CASE WHEN acquisition_date >= date('now', '-30 days') AND order_status IN (2,6,8) THEN 1 ELSE 0 END) as baseline_approved,
      SUM(CASE WHEN acquisition_date >= date('now', '-30 days') AND order_status IN (2,6,7,8) THEN 1 ELSE 0 END) as baseline_total
    FROM orders
    WHERE client_id = ? AND gateway_id IS NOT NULL AND is_test = 0
    GROUP BY gateway_id
    HAVING recent_total >= 10 AND baseline_total >= 30
  `, [clientId]);

  let degradedCount = 0;

  for (const mid of mids) {
    const recentRate = mid.recent_total > 0 ? (mid.recent_approved / mid.recent_total * 100) : 0;
    const baselineRate = mid.baseline_total > 0 ? (mid.baseline_approved / mid.baseline_total * 100) : 0;
    const drop = baselineRate - recentRate;

    if (drop >= threshold) {
      // Mark gateway as degrading
      runSql(
        "UPDATE gateways SET lifecycle_state = 'degrading', updated_at = datetime('now') WHERE client_id = ? AND gateway_id = ? AND lifecycle_state = 'active'",
        [clientId, mid.gateway_id]
      );

      // Create alert
      const existing = querySql(
        "SELECT id FROM alerts WHERE client_id = ? AND alert_type = 'mid_degradation' AND gateway_id = ? AND is_resolved = 0",
        [clientId, mid.gateway_id]
      );
      if (existing.length === 0) {
        runSql(
          "INSERT INTO alerts (client_id, priority, alert_type, title, description, gateway_id) VALUES (?, 'P1', 'mid_degradation', ?, ?, ?)",
          [clientId,
           `MID Degrading: Gateway ${mid.gateway_id}`,
           `Approval rate dropped ${drop.toFixed(1)}pp (${baselineRate.toFixed(1)}% → ${recentRate.toFixed(1)}%) in last 7 days.`,
           mid.gateway_id]
        );
      }

      degradedCount++;
    }
  }

  if (degradedCount > 0) saveDb();
  console.log(`[Optimizer] Found ${degradedCount} degrading MIDs.`);
  return { degraded: degradedCount };
}

module.exports = {
  detectOptimizationWindows,
  detectMidDegradation,
  calculateConfidence,
  normalCDF,
};
