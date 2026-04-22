/**
 * Shadow Alert Runner — orchestrates alert checks and produces reports.
 *
 * Runs all Layer 1 deterministic alerts for a given client, formats results as
 * both text summary and JSON, writes the JSON to disk, and returns triggered
 * alerts for upstream consumption (e.g. post-sync pipeline).
 *
 * Usage:
 *   const { runShadowAlertCheck } = require('./shadow-alert-runner');
 *   const triggered = runShadowAlertCheck(clientId);
 */

const fs = require('fs');
const path = require('path');
const { runShadowAlerts } = require('./shadow-alerts');

const DATA_DIR = path.join(__dirname, '..', '..', 'data');

/**
 * Run all shadow alerts for a client, log summary, write JSON report.
 *
 * @param {number} clientId
 * @returns {Array<Object>} only the triggered alerts
 */
function runShadowAlertCheck(clientId) {
  const start = Date.now();
  console.log(`[ShadowAlerts] Running Layer 1 alerts for client ${clientId}...`);

  // Run all checks (db param kept for interface consistency)
  const allAlerts = runShadowAlerts(null, clientId);

  const triggered = allAlerts.filter(a => a.triggered);
  const elapsed = ((Date.now() - start) / 1000).toFixed(2);

  // --- Console summary ---
  const warnings = triggered.filter(a => a.severity === 'warning');
  const infos = triggered.filter(a => a.severity === 'info');

  if (triggered.length === 0) {
    console.log(`[ShadowAlerts] All clear -- ${allAlerts.length} checks passed (${elapsed}s)`);
  } else {
    console.log(`[ShadowAlerts] ${triggered.length} alert(s) triggered (${warnings.length} warning, ${infos.length} info) in ${elapsed}s:`);
    for (const a of triggered) {
      const icon = a.severity === 'warning' ? 'WARN' : 'INFO';
      console.log(`  [${icon}] ${a.alert_name}: ${a.message}`);
    }
  }

  // --- Write JSON report ---
  const today = new Date().toISOString().split('T')[0];
  const reportPath = path.join(DATA_DIR, `shadow-alerts-${clientId}-${today}.json`);

  const report = {
    client_id: clientId,
    generated_at: new Date().toISOString(),
    elapsed_s: parseFloat(elapsed),
    total_checks: allAlerts.length,
    triggered_count: triggered.length,
    warnings_count: warnings.length,
    infos_count: infos.length,
    triggered: triggered,
    all: allAlerts,
  };

  try {
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), 'utf8');
    console.log(`[ShadowAlerts] Report written to ${reportPath}`);
  } catch (err) {
    console.error(`[ShadowAlerts] Failed to write report to ${reportPath}: ${err.message}`);
  }

  return triggered;
}

/**
 * Format a text summary suitable for logging or email notifications.
 *
 * @param {Array<Object>} allAlerts — full alert array from runShadowAlerts
 * @param {number} clientId
 * @returns {string} human-readable summary
 */
function formatAlertSummary(allAlerts, clientId) {
  const triggered = allAlerts.filter(a => a.triggered);
  const lines = [];

  lines.push(`=== Shadow Alert Report — Client ${clientId} ===`);
  lines.push(`Time: ${new Date().toISOString()}`);
  lines.push(`Checks: ${allAlerts.length} total, ${triggered.length} triggered`);
  lines.push('');

  if (triggered.length === 0) {
    lines.push('All checks passed. No alerts.');
  } else {
    const warnings = triggered.filter(a => a.severity === 'warning');
    const infos = triggered.filter(a => a.severity === 'info');

    if (warnings.length > 0) {
      lines.push(`--- WARNINGS (${warnings.length}) ---`);
      for (const a of warnings) {
        lines.push(`  [WARN] ${a.alert_name}`);
        lines.push(`         ${a.message}`);
        lines.push(`         value=${a.value}, threshold=${a.threshold}`);
        lines.push('');
      }
    }

    if (infos.length > 0) {
      lines.push(`--- INFO (${infos.length}) ---`);
      for (const a of infos) {
        lines.push(`  [INFO] ${a.alert_name}`);
        lines.push(`         ${a.message}`);
        lines.push('');
      }
    }
  }

  lines.push('=== End Report ===');
  return lines.join('\n');
}

module.exports = { runShadowAlertCheck, formatAlertSummary };
