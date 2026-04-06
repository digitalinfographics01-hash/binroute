/**
 * MID Volume & Cap Tracking — monitors gateway cap utilization.
 *
 * Uses existing gateways.global_monthly_cap and gateways.monthly_sales
 * from Sticky API gateway import. Does NOT parse from gateway names
 * or calculate from orders table.
 *
 * Thresholds:
 *   80% → amber warning
 *   95% → red warning + suggest fallback
 *   100% → treat as inactive, auto-promote fallback
 */
const { querySql, runSql, saveDb } = require('../db/connection');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CAP_WARN_AMBER = 0.80;
const CAP_WARN_RED = 0.95;
const CAP_REACHED = 1.00;

// ---------------------------------------------------------------------------
// Compute cap status for all active gateways
// ---------------------------------------------------------------------------

/**
 * Get cap utilization for all active gateways.
 *
 * @param {number} clientId
 * @returns {Array<{ gateway_id, gateway_alias, processor_name, cap, sales, cap_pct, status }>}
 */
function getCapStatus(clientId) {
  const gateways = querySql(`
    SELECT gateway_id, gateway_alias, processor_name,
      COALESCE(global_monthly_cap, monthly_cap) AS cap,
      monthly_sales AS sales
    FROM gateways
    WHERE client_id = ? AND gateway_active = 1
      AND COALESCE(exclude_from_analysis, 0) != 1
    ORDER BY gateway_id
  `, [clientId]);

  return gateways.map(gw => {
    const cap = gw.cap || 0;
    const sales = gw.sales || 0;
    const capPct = cap > 0 ? Math.round((sales / cap) * 10000) / 100 : 0;

    let status = 'ok';
    if (cap > 0) {
      if (capPct >= CAP_REACHED * 100) status = 'capped';
      else if (capPct >= CAP_WARN_RED * 100) status = 'red';
      else if (capPct >= CAP_WARN_AMBER * 100) status = 'amber';
    }

    return {
      gateway_id: gw.gateway_id,
      gateway_alias: gw.gateway_alias,
      processor_name: gw.processor_name,
      cap,
      sales,
      cap_pct: capPct,
      remaining: Math.max(0, cap - sales),
      status,
    };
  });
}

/**
 * Check cap alerts and return warnings for UI.
 *
 * @param {number} clientId
 * @returns {Array<{ gateway_id, gateway_alias, cap_pct, remaining, severity, message }>}
 */
function checkCapAlerts(clientId) {
  const statuses = getCapStatus(clientId);
  const alerts = [];

  for (const gw of statuses) {
    if (gw.status === 'capped') {
      alerts.push({
        gateway_id: gw.gateway_id,
        gateway_alias: gw.gateway_alias,
        processor_name: gw.processor_name,
        cap_pct: gw.cap_pct,
        remaining: 0,
        severity: 'critical',
        message: `${gw.gateway_alias} cap reached — routing to fallback`,
      });
    } else if (gw.status === 'red') {
      alerts.push({
        gateway_id: gw.gateway_id,
        gateway_alias: gw.gateway_alias,
        processor_name: gw.processor_name,
        cap_pct: gw.cap_pct,
        remaining: gw.remaining,
        severity: 'red',
        message: `${gw.gateway_alias} nearly at cap — $${Math.round(gw.remaining)} remaining this month`,
      });
    } else if (gw.status === 'amber') {
      alerts.push({
        gateway_id: gw.gateway_id,
        gateway_alias: gw.gateway_alias,
        processor_name: gw.processor_name,
        cap_pct: gw.cap_pct,
        remaining: gw.remaining,
        severity: 'amber',
        message: `${gw.gateway_alias} at ${gw.cap_pct.toFixed(0)}% monthly cap — $${Math.round(gw.remaining)} remaining`,
      });
    }
  }

  return alerts;
}

/**
 * Get capped gateway IDs (treat as inactive for routing).
 *
 * @param {number} clientId
 * @returns {Set<number>}
 */
function getCappedGateways(clientId) {
  const statuses = getCapStatus(clientId);
  return new Set(statuses.filter(s => s.status === 'capped').map(s => s.gateway_id));
}

/**
 * Record monthly volume snapshot in mid_volume_tracking.
 *
 * @param {number} clientId
 */
function recordVolumeSnapshot(clientId) {
  const month = new Date().toISOString().slice(0, 7); // YYYY-MM
  const statuses = getCapStatus(clientId);

  for (const gw of statuses) {
    runSql(`INSERT OR REPLACE INTO mid_volume_tracking
      (client_id, gateway_id, month, volume_usd, cap_pct, updated_at)
      VALUES (?, ?, ?, ?, ?, datetime('now'))`,
      [clientId, gw.gateway_id, month, gw.sales, gw.cap_pct]);
  }
  saveDb();
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  getCapStatus,
  checkCapAlerts,
  getCappedGateways,
  recordVolumeSnapshot,
  CAP_WARN_AMBER,
  CAP_WARN_RED,
  CAP_REACHED,
};
