/**
 * Fallback Chain Manager — persistence, auto-promote, and emergency detection.
 *
 * Stores ranked processor chains in flow_optix_rules.
 * Detects MID closures and promotes fallbacks.
 * Detects emergency state (all processors down).
 */
const { querySql, runSql, saveDb } = require('../db/connection');
const { computeWeightedRates, selectProcessor, buildFallbackChain } = require('./weighted-rates');

// ---------------------------------------------------------------------------
// Persist chain for a BIN group
// ---------------------------------------------------------------------------

/**
 * Update or insert the fallback chain for a BIN group in flow_optix_rules.
 *
 * @param {number} clientId
 * @param {string} ruleId - groupKey or rule_id
 * @param {object} chain - { primary_gateway_id, secondary_gateway_id, tertiary_gateway_id }
 * @param {string} selectionType - 'CLEAR_WINNER' | 'SPLIT_TEST' | 'CONVERGED' | 'SINGLE' | 'GATHERING'
 */
function persistChain(clientId, ruleId, chain, selectionType) {
  const existing = querySql(
    'SELECT id FROM flow_optix_rules WHERE client_id = ? AND rule_id = ?',
    [clientId, ruleId]
  );

  if (existing.length > 0) {
    runSql(`UPDATE flow_optix_rules SET
      primary_gateway_id = ?, secondary_gateway_id = ?, tertiary_gateway_id = ?,
      chain_updated_at = datetime('now'), processor_selection_type = ?,
      updated_at = datetime('now')
      WHERE client_id = ? AND rule_id = ?`,
      [chain.primary_gateway_id, chain.secondary_gateway_id, chain.tertiary_gateway_id,
       selectionType, clientId, ruleId]);
  } else {
    runSql(`INSERT INTO flow_optix_rules
      (client_id, rule_id, primary_gateway_id, secondary_gateway_id, tertiary_gateway_id,
       chain_updated_at, processor_selection_type, status, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, datetime('now'), ?, 'recommended', datetime('now'), datetime('now'))`,
      [clientId, ruleId, chain.primary_gateway_id, chain.secondary_gateway_id,
       chain.tertiary_gateway_id, selectionType]);
  }
}

// ---------------------------------------------------------------------------
// Auto-promote on MID closure
// ---------------------------------------------------------------------------

/**
 * Check all active chains for closed/inactive gateways and auto-promote.
 * Returns list of promotion events for UI banners.
 *
 * @param {number} clientId
 * @returns {Array<{ ruleId, event, oldGw, newGw, severity }>}
 */
function checkAndPromote(clientId) {
  const events = [];

  // Get active gateways
  const activeGws = new Set(
    querySql('SELECT gateway_id FROM gateways WHERE client_id = ? AND gateway_active = 1 AND COALESCE(exclude_from_analysis, 0) != 1', [clientId])
      .map(r => r.gateway_id)
  );

  // Get all chains
  const chains = querySql(
    `SELECT rule_id, primary_gateway_id, secondary_gateway_id, tertiary_gateway_id,
            processor_selection_type
     FROM flow_optix_rules
     WHERE client_id = ? AND status NOT IN ('archived', 'merged')
       AND primary_gateway_id IS NOT NULL`,
    [clientId]
  );

  for (const chain of chains) {
    const primaryOk = chain.primary_gateway_id && activeGws.has(chain.primary_gateway_id);
    const secondaryOk = chain.secondary_gateway_id && activeGws.has(chain.secondary_gateway_id);
    const tertiaryOk = chain.tertiary_gateway_id && activeGws.has(chain.tertiary_gateway_id);

    if (primaryOk) continue; // Primary is fine

    // Primary is down — promote
    if (!primaryOk && secondaryOk) {
      // Get gateway names for event
      const oldGw = _gwName(clientId, chain.primary_gateway_id);
      const newGw = _gwName(clientId, chain.secondary_gateway_id);

      runSql(`UPDATE flow_optix_rules SET
        primary_gateway_id = ?, secondary_gateway_id = ?, tertiary_gateway_id = NULL,
        chain_updated_at = datetime('now'), updated_at = datetime('now')
        WHERE client_id = ? AND rule_id = ?`,
        [chain.secondary_gateway_id, tertiaryOk ? chain.tertiary_gateway_id : null,
         clientId, chain.rule_id]);

      events.push({
        ruleId: chain.rule_id,
        event: 'primary_promoted',
        oldGw,
        newGw,
        severity: 'amber',
        message: `${oldGw} unavailable — ${newGw} promoted to primary. Update your routing rules.`,
      });
    } else if (!primaryOk && !secondaryOk && tertiaryOk) {
      // Both primary and secondary down
      const oldGw = _gwName(clientId, chain.primary_gateway_id);
      const newGw = _gwName(clientId, chain.tertiary_gateway_id);

      runSql(`UPDATE flow_optix_rules SET
        primary_gateway_id = ?, secondary_gateway_id = NULL, tertiary_gateway_id = NULL,
        chain_updated_at = datetime('now'), updated_at = datetime('now')
        WHERE client_id = ? AND rule_id = ?`,
        [chain.tertiary_gateway_id, clientId, chain.rule_id]);

      events.push({
        ruleId: chain.rule_id,
        event: 'emergency_promote',
        oldGw,
        newGw,
        severity: 'red',
        message: `Multiple processors unavailable — ${newGw} promoted to primary. Update your routing rules now.`,
      });
    } else {
      // All down
      events.push({
        ruleId: chain.rule_id,
        event: 'no_fallback',
        oldGw: _gwName(clientId, chain.primary_gateway_id),
        newGw: null,
        severity: 'critical',
        message: 'No fallback processor — add a second processor MID.',
      });
    }
  }

  if (events.length > 0) saveDb();
  return events;
}

// ---------------------------------------------------------------------------
// Emergency detection
// ---------------------------------------------------------------------------

/**
 * Check if ALL active processors are down for a client.
 *
 * @param {number} clientId
 * @returns {{ emergency: boolean, recoveryPriority: Array }}
 */
function checkEmergencyState(clientId) {
  const activeCount = querySql(
    'SELECT COUNT(*) as cnt FROM gateways WHERE client_id = ? AND gateway_active = 1 AND COALESCE(exclude_from_analysis, 0) != 1',
    [clientId]
  )[0]?.cnt || 0;

  if (activeCount > 0) {
    return { emergency: false, recoveryPriority: [] };
  }

  // All processors down — build recovery priority by historical coverage
  const historicalProcs = querySql(`
    SELECT g.processor_name,
      COUNT(DISTINCT o.cc_first_6) AS bins_covered,
      COUNT(*) AS total_orders,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.acquisition_date >= date('now', '-180 days')
      AND g.processor_name IS NOT NULL
    GROUP BY g.processor_name
    ORDER BY bins_covered DESC
  `, [clientId]);

  const totalBins = querySql(
    "SELECT COUNT(DISTINCT cc_first_6) AS cnt FROM orders WHERE client_id = ? AND is_test = 0 AND derived_product_role IN ('main_rebill','upsell_rebill') AND acquisition_date >= date('now','-180 days')",
    [clientId]
  )[0]?.cnt || 1;

  const recoveryPriority = historicalProcs.map((p, i) => ({
    rank: i + 1,
    processor: p.processor_name,
    bins_covered: p.bins_covered,
    coverage_pct: Math.round((p.bins_covered / totalBins) * 10000) / 100,
    total_orders: p.total_orders,
    approval_rate: p.total_orders > 0 ? Math.round((p.approved / p.total_orders) * 10000) / 100 : 0,
  }));

  return { emergency: true, recoveryPriority };
}

// ---------------------------------------------------------------------------
// Rebuild all chains for a client (runs during recompute)
// ---------------------------------------------------------------------------

/**
 * Rebuild fallback chains for all BIN groups.
 * Called after analytics recompute.
 *
 * @param {number} clientId
 * @param {Map} groupRates - From computeAllGroupRates()
 * @returns {{ updated: number, events: Array }}
 */
function rebuildAllChains(clientId, groupRates) {
  let updated = 0;

  for (const [groupKey, group] of groupRates) {
    const selection = selectProcessor(group.processors, group.mids);
    const chain = buildFallbackChain(selection, group.mids);
    persistChain(clientId, groupKey, chain, selection.type);
    updated++;
  }

  saveDb();

  // Check for promotions needed
  const events = checkAndPromote(clientId);

  return { updated, events };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function _gwName(clientId, gwId) {
  if (!gwId) return 'Unknown';
  const row = querySql('SELECT gateway_alias FROM gateways WHERE client_id = ? AND gateway_id = ?', [clientId, gwId]);
  return row[0]?.gateway_alias || `GW ${gwId}`;
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  persistChain,
  checkAndPromote,
  checkEmergencyState,
  rebuildAllChains,
};
