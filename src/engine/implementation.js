const { querySql, runSql, saveDb, transaction } = require('../db/connection');

/**
 * Implementation Tracker.
 * - Manual confirmation (user marks as implemented)
 * - 7-day waiting period after marking
 * - 30-day baseline snapshot taken at mark date
 * - Comparison opens on day 8
 * - Resolves to: Confirmed / Inconclusive / Regression
 * - Outcomes feed back into routing confidence scores
 */

/**
 * Mark a recommendation as implemented.
 * Takes a baseline snapshot and starts the 7-day waiting period.
 */
function markImplemented(recommendationId) {
  const rec = querySql('SELECT * FROM recommendations WHERE id = ?', [recommendationId]);
  if (rec.length === 0) throw new Error(`Recommendation ${recommendationId} not found`);
  if (rec[0].status !== 'open') throw new Error(`Recommendation is ${rec[0].status}, not open`);

  const r = rec[0];

  // Take baseline snapshot: 30 days of data for this BIN+MID before implementation
  const baseline = querySql(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      SUM(CASE WHEN order_status = 7 AND decline_category = 'processor' THEN 1 ELSE 0 END) as processor_declines,
      ROUND(100.0 * SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as approval_rate
    FROM orders
    WHERE client_id = ? AND cc_first_6 = ? AND gateway_id = ?
      AND acquisition_date >= date('now', '-30 days')
  `, [r.client_id, r.bin, r.current_gateway_id]);

  const snapshot = {
    bin: r.bin,
    current_gateway_id: r.current_gateway_id,
    recommended_gateway_id: r.recommended_gateway_id,
    baseline: baseline[0] || { total: 0, approved: 0, approval_rate: 0 },
    snapshot_date: new Date().toISOString(),
  };

  transaction(() => {
    // Update recommendation status
    runSql(
      "UPDATE recommendations SET status = 'implemented', updated_at = datetime('now') WHERE id = ?",
      [recommendationId]
    );

    // Create implementation record
    runSql(`
      INSERT INTO implementations (
        recommendation_id, client_id, marked_at,
        baseline_snapshot_json, comparison_start_date, result
      ) VALUES (?, ?, datetime('now'), ?, date('now', '+7 days'), 'waiting')
    `, [recommendationId, r.client_id, JSON.stringify(snapshot)]);
  });

  return { implementationCreated: true, snapshot };
}

/**
 * Check all implementations in 'waiting' state.
 * Transition to 'comparing' after 7-day waiting period.
 */
function checkWaitingImplementations() {
  const waiting = querySql(`
    SELECT i.*, r.bin, r.current_gateway_id, r.recommended_gateway_id, r.client_id as rec_client_id
    FROM implementations i
    JOIN recommendations r ON r.id = i.recommendation_id
    WHERE i.result = 'waiting' AND i.comparison_start_date <= date('now')
  `);

  let transitioned = 0;
  for (const impl of waiting) {
    runSql("UPDATE implementations SET result = 'comparing', updated_at = datetime('now') WHERE id = ?", [impl.id]);
    transitioned++;
  }

  if (transitioned > 0) saveDb();
  return { transitioned };
}

/**
 * Evaluate implementations in 'comparing' state.
 * Compares post-implementation performance against baseline.
 * Runs from day 8 onward; needs at least 7 days of post-implementation data.
 */
function evaluateImplementations() {
  const comparing = querySql(`
    SELECT i.*, r.bin, r.current_gateway_id, r.recommended_gateway_id, r.client_id as rec_client_id
    FROM implementations i
    JOIN recommendations r ON r.id = i.recommendation_id
    WHERE i.result = 'comparing'
  `);

  let evaluated = 0;

  for (const impl of comparing) {
    const baseline = JSON.parse(impl.baseline_snapshot_json || '{}');
    const baselineRate = baseline.baseline?.approval_rate || 0;

    // Get post-implementation data (from recommended gateway for this BIN)
    const postData = querySql(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
        ROUND(100.0 * SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as approval_rate
      FROM orders
      WHERE client_id = ? AND cc_first_6 = ? AND gateway_id = ?
        AND acquisition_date >= ?
    `, [impl.client_id, impl.bin, impl.recommended_gateway_id, impl.comparison_start_date]);

    const post = postData[0] || { total: 0, approved: 0, approval_rate: 0 };

    // Need minimum sample to evaluate
    if (post.total < 20) continue;

    const postRate = post.approval_rate || 0;
    const lift = postRate - baselineRate;

    let result;
    if (lift >= 2.0) {
      result = 'confirmed';
    } else if (lift <= -2.0) {
      result = 'regression';
    } else {
      result = 'inconclusive';
    }

    const resultData = {
      baseline_rate: baselineRate,
      post_rate: postRate,
      lift,
      post_transactions: post.total,
      evaluated_at: new Date().toISOString(),
    };

    transaction(() => {
      runSql(
        "UPDATE implementations SET result = ?, result_data_json = ?, updated_at = datetime('now') WHERE id = ?",
        [result, JSON.stringify(resultData), impl.id]
      );

      // Update recommendation status to match
      runSql(
        "UPDATE recommendations SET status = ?, updated_at = datetime('now') WHERE id = ?",
        [result, impl.recommendation_id]
      );

      // Feed result back into confidence scoring
      if (result === 'confirmed') {
        // Boost confidence for similar patterns
        runSql(`
          UPDATE recommendations SET confidence_score = MIN(confidence_score * 1.1, 1.0)
          WHERE client_id = ? AND bin = ? AND status = 'open'
        `, [impl.client_id, impl.bin]);
      } else if (result === 'regression') {
        // Lower confidence for similar patterns
        runSql(`
          UPDATE recommendations SET confidence_score = confidence_score * 0.8
          WHERE client_id = ? AND bin = ? AND status = 'open'
        `, [impl.client_id, impl.bin]);
      }
    });

    evaluated++;
  }

  return { evaluated };
}

/**
 * Get implementation status summary for a client.
 */
function getImplementationSummary(clientId) {
  return querySql(`
    SELECT
      i.id, i.recommendation_id, i.marked_at, i.comparison_start_date,
      i.result, i.result_data_json,
      r.bin, r.cc_type, r.current_gateway_id, r.recommended_gateway_id,
      r.current_approval_rate, r.recommended_approval_rate, r.expected_lift,
      r.summary
    FROM implementations i
    JOIN recommendations r ON r.id = i.recommendation_id
    WHERE i.client_id = ?
    ORDER BY i.marked_at DESC
  `, [clientId]);
}

module.exports = {
  markImplemented,
  checkWaitingImplementations,
  evaluateImplementations,
  getImplementationSummary,
};
