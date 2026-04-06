/**
 * New Processor Handling — warming up, historical data detection.
 *
 * New processor with no history → warming up (30 days or 10+ approved)
 * New processor with history → immediate recommendations, flagged HISTORICAL
 * New MID same processor → warming up 30 days, inherits processor confidence
 */
const { querySql, runSql, saveDb } = require('../db/connection');

const WARMUP_DAYS = 30;
const WARMUP_MIN_APPROVED = 10;

// ---------------------------------------------------------------------------
// Detect and flag new processors/MIDs
// ---------------------------------------------------------------------------

/**
 * Check all active gateways for warming up status.
 * Sets is_warming_up = 1 for new gateways.
 * Removes flag when 30 days passed OR 10+ approved.
 *
 * @param {number} clientId
 * @returns {{ newWarmups: number, graduated: number }}
 */
function updateWarmupStatus(clientId) {
  let newWarmups = 0;
  let graduated = 0;

  const gateways = querySql(`
    SELECT g.gateway_id, g.gateway_alias, g.processor_name, g.gateway_active,
      g.is_warming_up, g.warming_up_since, g.gateway_created,
      (SELECT COUNT(*) FROM orders o
       WHERE o.gateway_id = g.gateway_id AND o.client_id = g.client_id
         AND o.order_status IN (2,6,8) AND o.is_test = 0 AND o.is_internal_test = 0
         AND o.derived_product_role IN ('main_rebill','upsell_rebill')
         AND o.derived_cycle IN (1,2) AND o.is_cascaded = 0
      ) AS approved_count
    FROM gateways g
    WHERE g.client_id = ? AND g.gateway_active = 1
      AND COALESCE(g.exclude_from_analysis, 0) != 1
  `, [clientId]);

  for (const gw of gateways) {
    const created = gw.gateway_created ? new Date(gw.gateway_created) : null;
    const daysActive = created ? (Date.now() - created.getTime()) / (1000 * 60 * 60 * 24) : 999;

    if (gw.is_warming_up === 1) {
      // Check if should graduate
      if (daysActive >= WARMUP_DAYS || gw.approved_count >= WARMUP_MIN_APPROVED) {
        runSql('UPDATE gateways SET is_warming_up = 0 WHERE client_id = ? AND gateway_id = ?',
          [clientId, gw.gateway_id]);
        graduated++;
      }
    } else if (daysActive < WARMUP_DAYS && gw.approved_count < WARMUP_MIN_APPROVED) {
      // New gateway that should be warming up
      runSql('UPDATE gateways SET is_warming_up = 1, warming_up_since = ? WHERE client_id = ? AND gateway_id = ?',
        [gw.gateway_created || new Date().toISOString(), clientId, gw.gateway_id]);
      newWarmups++;
    }
  }

  if (newWarmups > 0 || graduated > 0) saveDb();
  return { newWarmups, graduated };
}

/**
 * Check if a processor has historical data but no recent live traffic.
 *
 * @param {number} clientId
 * @param {string} processorName
 * @returns {{ hasHistorical: boolean, hasRecent: boolean, totalHistorical: number }}
 */
function checkProcessorHistory(clientId, processorName) {
  const row = querySql(`
    SELECT
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') THEN 1 END) AS recent,
      COUNT(CASE WHEN o.acquisition_date < date('now', '-30 days') THEN 1 END) AS historical
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = ? AND g.processor_name = ?
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.acquisition_date >= date('now', '-180 days')
  `, [clientId, processorName])[0] || { recent: 0, historical: 0 };

  return {
    hasHistorical: row.historical > 0,
    hasRecent: row.recent > 0,
    totalHistorical: row.historical,
  };
}

/**
 * Get all warming up gateways for a client.
 *
 * @param {number} clientId
 * @returns {Array}
 */
function getWarmingUpGateways(clientId) {
  return querySql(`
    SELECT gateway_id, gateway_alias, processor_name, warming_up_since, gateway_created,
      (SELECT COUNT(*) FROM orders o
       WHERE o.gateway_id = g.gateway_id AND o.client_id = g.client_id
         AND o.order_status IN (2,6,8) AND o.is_test = 0
         AND o.derived_product_role IN ('main_rebill','upsell_rebill')
         AND o.derived_cycle IN (1,2) AND o.is_cascaded = 0
      ) AS approved_count
    FROM gateways g
    WHERE g.client_id = ? AND g.is_warming_up = 1
      AND COALESCE(g.exclude_from_analysis, 0) != 1
  `, [clientId]);
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  updateWarmupStatus,
  checkProcessorHistory,
  getWarmingUpGateways,
  WARMUP_DAYS,
  WARMUP_MIN_APPROVED,
};
