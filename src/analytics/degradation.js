/**
 * Degradation Detection — identifies approval rate drops per gateway.
 *
 * Compares last 14 days vs previous 14 days (14-28 days ago).
 * Distinguishes issuer-level changes (all processors affected)
 * from gateway-specific degradation.
 *
 * Runs after every sync.
 */
const { querySql, runSql, saveDb } = require('../db/connection');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DROP_THRESHOLD_PP = 5;

// ---------------------------------------------------------------------------
// Core: Detect degradation
// ---------------------------------------------------------------------------

/**
 * Detect approval rate drops per gateway per issuer group.
 *
 * @param {number} clientId
 * @returns {Array<{ gateway_id, gateway_alias, issuer_bank, card_brand,
 *                    rate_last_14d, rate_prev_14d, drop_pp, is_issuer_level, alert_type }>}
 */
function detectDegradation(clientId) {
  // Get rates for last 14d and prev 14d per gateway per issuer+brand
  const rows = querySql(`
    SELECT o.gateway_id, g.gateway_alias, g.processor_name,
      b.issuer_bank, b.card_brand,
      SUM(CASE WHEN o.acquisition_date >= date('now', '-14 days') THEN 1 ELSE 0 END) AS att_14d,
      SUM(CASE WHEN o.acquisition_date >= date('now', '-14 days') AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) AS app_14d,
      SUM(CASE WHEN o.acquisition_date >= date('now', '-28 days') AND o.acquisition_date < date('now', '-14 days') THEN 1 ELSE 0 END) AS att_prev,
      SUM(CASE WHEN o.acquisition_date >= date('now', '-28 days') AND o.acquisition_date < date('now', '-14 days') AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) AS app_prev
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.derived_cycle IN (1, 2)
      AND o.is_cascaded = 0
      AND o.acquisition_date >= date('now', '-28 days')
      AND g.gateway_active = 1
      AND COALESCE(g.exclude_from_analysis, 0) != 1
    GROUP BY o.gateway_id, b.issuer_bank, b.card_brand
    HAVING att_14d >= 10 AND att_prev >= 10
  `, [clientId]);

  const alerts = [];

  // Also compute issuer-level rates (all gateways combined) to detect issuer changes
  const issuerRates = new Map(); // issuer|brand → { rate_14d, rate_prev }
  for (const row of rows) {
    const key = `${row.issuer_bank}|${row.card_brand}`;
    if (!issuerRates.has(key)) {
      issuerRates.set(key, { att_14d: 0, app_14d: 0, att_prev: 0, app_prev: 0 });
    }
    const ir = issuerRates.get(key);
    ir.att_14d += row.att_14d;
    ir.app_14d += row.app_14d;
    ir.att_prev += row.att_prev;
    ir.app_prev += row.app_prev;
  }

  for (const row of rows) {
    const rate14d = row.att_14d > 0 ? Math.round((row.app_14d / row.att_14d) * 10000) / 100 : 0;
    const ratePrev = row.att_prev > 0 ? Math.round((row.app_prev / row.att_prev) * 10000) / 100 : 0;
    const drop = Math.round((ratePrev - rate14d) * 100) / 100;

    if (drop < DROP_THRESHOLD_PP) continue;

    // Check if ALL processors dropped for this issuer (issuer-level change)
    const key = `${row.issuer_bank}|${row.card_brand}`;
    const ir = issuerRates.get(key);
    const issuerRate14d = ir.att_14d > 0 ? (ir.app_14d / ir.att_14d) * 100 : 0;
    const issuerRatePrev = ir.att_prev > 0 ? (ir.app_prev / ir.att_prev) * 100 : 0;
    const issuerDrop = issuerRatePrev - issuerRate14d;
    const isIssuerLevel = issuerDrop >= DROP_THRESHOLD_PP;

    alerts.push({
      gateway_id: row.gateway_id,
      gateway_alias: row.gateway_alias,
      processor_name: row.processor_name,
      issuer_bank: row.issuer_bank,
      card_brand: row.card_brand,
      rate_last_14d: rate14d,
      rate_prev_14d: ratePrev,
      drop_pp: drop,
      is_issuer_level: isIssuerLevel ? 1 : 0,
      alert_type: isIssuerLevel ? 'issuer_behavior_change' : 'gateway_degradation',
    });
  }

  return alerts;
}

/**
 * Run degradation detection and persist alerts.
 * Auto-resolves alerts when rate recovers.
 *
 * @param {number} clientId
 * @returns {{ new_alerts: number, resolved: number, issuer_level: number }}
 */
function runDegradationCheck(clientId) {
  const detected = detectDegradation(clientId);
  let newAlerts = 0;
  let issuerLevel = 0;

  for (const alert of detected) {
    // Check if alert already exists (unresolved)
    const existing = querySql(
      `SELECT id FROM degradation_alerts
       WHERE client_id = ? AND gateway_id = ? AND issuer_bank = ? AND card_brand = ? AND resolved_at IS NULL`,
      [clientId, alert.gateway_id, alert.issuer_bank, alert.card_brand]
    );

    if (existing.length === 0) {
      runSql(`INSERT INTO degradation_alerts
        (client_id, gateway_id, issuer_bank, card_brand, rate_last_14d, rate_prev_14d,
         drop_pp, alert_type, is_issuer_level)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [clientId, alert.gateway_id, alert.issuer_bank, alert.card_brand,
         alert.rate_last_14d, alert.rate_prev_14d, alert.drop_pp,
         alert.alert_type, alert.is_issuer_level]);
      newAlerts++;
      if (alert.is_issuer_level) issuerLevel++;
    } else {
      // Update existing alert with latest rates
      runSql(`UPDATE degradation_alerts SET
        rate_last_14d = ?, rate_prev_14d = ?, drop_pp = ?
        WHERE id = ?`,
        [alert.rate_last_14d, alert.rate_prev_14d, alert.drop_pp, existing[0].id]);
    }
  }

  // Auto-resolve alerts where rate has recovered
  const activeAlerts = querySql(
    'SELECT id, gateway_id, issuer_bank, card_brand FROM degradation_alerts WHERE client_id = ? AND resolved_at IS NULL',
    [clientId]
  );
  const detectedKeys = new Set(detected.map(d => `${d.gateway_id}|${d.issuer_bank}|${d.card_brand}`));
  let resolved = 0;

  for (const alert of activeAlerts) {
    const key = `${alert.gateway_id}|${alert.issuer_bank}|${alert.card_brand}`;
    if (!detectedKeys.has(key)) {
      runSql('UPDATE degradation_alerts SET resolved_at = datetime(\'now\') WHERE id = ?', [alert.id]);
      resolved++;
    }
  }

  if (newAlerts > 0 || resolved > 0) saveDb();

  return { new_alerts: newAlerts, resolved, issuer_level: issuerLevel };
}

/**
 * Get active (unresolved) degradation alerts for a client.
 *
 * @param {number} clientId
 * @returns {Array}
 */
function getActiveAlerts(clientId) {
  return querySql(
    `SELECT * FROM degradation_alerts
     WHERE client_id = ? AND resolved_at IS NULL
     ORDER BY drop_pp DESC`,
    [clientId]
  );
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  detectDegradation,
  runDegradationCheck,
  getActiveAlerts,
  DROP_THRESHOLD_PP,
};
