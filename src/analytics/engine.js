/**
 * Analytics Engine — Core shared utilities.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql, queryOneSql, runSql, saveDb } = require('../db/connection');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Approved order statuses: 2=Approved, 6=Partial Refund, 8=Shipped.
 *  Pending (queued subscription) is NOT approved. 7=Declined. */
const APPROVED_STATUSES = [2, 6, 8];
const APPROVED_STATUS_SQL = 'IN (2,6,8)';
const DECLINED_STATUS = 7;

const CLEAN_FILTER = 'o.is_test = 0 AND o.is_internal_test = 0';

// CRM routing rule declines — orders blocked before reaching a gateway.
// Must be excluded from approval rate calculations. Built dynamically from
// known CRM routing block strings. NULL-safe: approved orders have NULL
// decline_reason, and NULL != 'string' is falsy in SQL.
const CRM_ROUTING_STRINGS = [
  'Prepaid Credit Cards Are Not Accepted',
];
const CRM_ROUTING_EXCLUSION = CRM_ROUTING_STRINGS.length > 0
  ? `(o.decline_reason IS NULL OR o.decline_reason NOT IN (${CRM_ROUTING_STRINGS.map(s => `'${s.replace(/'/g, "''")}'`).join(',')}))`
  : '1=1';

const TX_TYPE_GROUPS = {
  INITIALS:       ['cp_initial', 'initial_salvage'],
  REBILLS:        ['tp_rebill', 'tp_rebill_salvage', 'sticky_cof_rebill'],
  UPSELLS:        ['upsell', 'upsell_cascade'],
  STRAIGHT_SALES: ['straight_sale'],
};

/** Flatten all tx types into a single list for convenience */
const ALL_TX_TYPES = [
  ...TX_TYPE_GROUPS.INITIALS,
  ...TX_TYPE_GROUPS.REBILLS,
  ...TX_TYPE_GROUPS.UPSELLS,
  ...TX_TYPE_GROUPS.STRAIGHT_SALES,
];

// ---------------------------------------------------------------------------
// approvalStats
// ---------------------------------------------------------------------------

/**
 * Return approval statistics for a client, with optional extra WHERE clause.
 * The WHERE clause should reference the `o` alias for the orders table.
 *
 * @param {number} clientId
 * @param {string} [where] - Additional SQL conditions (already uses AND prefix)
 * @param {Array}  [params] - Bind parameters for the extra where clause
 * @returns {{ total: number, approved: number, declined: number, rate: number|null }}
 */
function approvalStats(clientId, where = '', params = []) {
  let sql = `
    SELECT
      COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)   AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END)          AS declined
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER}
      AND ${CRM_ROUTING_EXCLUSION}
  `;
  if (where) sql += ` AND ${where}`;

  const row = queryOneSql(sql, [clientId, ...params]);
  if (!row || row.total === 0) {
    return { total: 0, approved: 0, declined: 0, rate: null };
  }
  return {
    total:    row.total,
    approved: row.approved,
    declined: row.declined,
    rate:     Math.round((row.approved / row.total) * 10000) / 100,
  };
}

// ---------------------------------------------------------------------------
// confidenceScore
// ---------------------------------------------------------------------------

/**
 * Compute a confidence score for an analytical data point.
 *
 * @param {number} attempts       - Number of transaction attempts in the sample
 * @param {number} weeklyVariance - Standard deviation of weekly approval rates (0-100)
 * @param {number} recentPct      - Percentage of data from the last 14 days (0-100)
 * @returns {{ level: string, score: number, components: object }}
 */
function confidenceScore(attempts, weeklyVariance, recentPct) {
  // Volume component: log scale, capped at 40 points
  const volumePts = Math.min(40, Math.log10(Math.max(attempts, 1)) * 13.3);

  // Stability component: lower variance = higher score, max 30 points
  const stabilityPts = Math.max(0, 30 - weeklyVariance);

  // Recency component: higher recent data pct = higher score, max 30 points
  const recencyPts = (recentPct / 100) * 30;

  const score = Math.round(Math.min(100, volumePts + stabilityPts + recencyPts));

  let level;
  if (score >= 75) level = 'high';
  else if (score >= 45) level = 'medium';
  else level = 'low';

  return {
    level,
    score,
    components: {
      volume:    Math.round(volumePts * 10) / 10,
      stability: Math.round(stabilityPts * 10) / 10,
      recency:   Math.round(recencyPts * 10) / 10,
    },
  };
}

// ---------------------------------------------------------------------------
// trendDirection
// ---------------------------------------------------------------------------

/**
 * Determine the trend direction between two rates.
 *
 * @param {number} current  - Current period rate
 * @param {number} previous - Previous period rate
 * @returns {'improving'|'stable'|'degrading'}
 */
function trendDirection(current, previous) {
  if (current == null || previous == null) return 'stable';
  const delta = current - previous;
  if (delta >= 2)  return 'improving';
  if (delta <= -2) return 'degrading';
  return 'stable';
}

// ---------------------------------------------------------------------------
// stabilityFlag
// ---------------------------------------------------------------------------

/**
 * Categorize approval-rate variance.
 *
 * @param {number} variance - Standard deviation of approval rate over time
 * @returns {'stable'|'volatile'|'unstable'}
 */
function stabilityFlag(variance) {
  if (variance == null) return 'stable';
  if (variance <= 5)  return 'stable';
  if (variance <= 15) return 'volatile';
  return 'unstable';
}

// ---------------------------------------------------------------------------
// recencyWeight
// ---------------------------------------------------------------------------

/**
 * Compute a weighted average from bucketed data with recency multipliers.
 *
 * @param {Array<{ value: number, count: number, bucket: string }>} daysBuckets
 *   bucket must be one of: '0-7', '8-14', '15-30', '31+'
 * @returns {number|null} Weighted average value
 */
function recencyWeight(daysBuckets) {
  const multipliers = { '0-7': 3, '8-14': 2, '15-30': 1, '31+': 0.5 };
  let weightedSum = 0;
  let totalWeight = 0;

  for (const b of daysBuckets) {
    const mult = multipliers[b.bucket] ?? 1;
    const w = b.count * mult;
    weightedSum += b.value * w;
    totalWeight += w;
  }

  return totalWeight > 0 ? Math.round((weightedSum / totalWeight) * 100) / 100 : null;
}

// ---------------------------------------------------------------------------
// getCachedOrCompute — simple in-memory cache
// ---------------------------------------------------------------------------

const _cache = new Map();
let _forceCompute = false; // Set true only during recomputeAllAnalytics

// Cache never expires automatically — only cleared by explicit clearCache() call
// or server restart. Analytics are pre-computed via run-all and served from cache.

/**
 * Return a cached result or compute and cache it.
 * Results persist in memory until clearCache() is called.
 * On first access after server start, also checks the DB for persisted results.
 * On cache miss: returns null UNLESS _forceCompute is set (recompute path).
 *
 * @param {number}   clientId
 * @param {string}   outputType - e.g. 'bin-profiles', 'gateway-profiles'
 * @param {string}   txType     - tx type filter key (or 'all')
 * @param {Function} computeFn  - () => result
 * @returns {*} The cached or freshly computed result
 */
function getCachedOrCompute(clientId, outputType, txType, computeFn) {
  const key = `${clientId}:${outputType}:${txType || 'all'}`;
  const cached = _cache.get(key);
  if (cached) {
    return cached.data;
  }

  // Check DB for persisted result
  try {
    const rows = querySql(
      'SELECT result_json FROM analytics_cache WHERE client_id = ? AND output_type = ? AND cache_key = ?',
      [clientId, outputType, txType || 'all']
    );
    if (rows.length > 0 && rows[0].result_json) {
      const data = JSON.parse(rows[0].result_json);
      _cache.set(key, { data, ts: Date.now() });
      return data;
    }
  } catch (e) {
    // analytics_cache table may not exist yet — that's fine
  }

  // Force compute path (recomputeAllAnalytics / manual recompute)
  if (_forceCompute) {
    const data = computeFn();
    _cache.set(key, { data, ts: Date.now() });

    // Persist to DB
    try {
      runSql(
        'INSERT OR REPLACE INTO analytics_cache (client_id, output_type, cache_key, result_json, computed_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)',
        [clientId, outputType, txType || 'all', JSON.stringify(data)]
      );
      saveDb();
    } catch (e) {
      // Persist failed — not critical, in-memory cache still works
    }

    return data;
  }

  // No cache hit, not in force-compute mode — return null.
  // Analytics are only computed via:
  //   a) Manual POST /:clientId/recompute (Refresh button)
  //   b) recomputeAllAnalytics() after daily sync
  return null;
}

/**
 * Clear the analytics cache (useful after data refresh).
 */
function clearCache() {
  _cache.clear();
  try {
    runSql('DELETE FROM analytics_cache');
    saveDb();
  } catch (e) {
    // Table may not exist — fine
  }
}

/**
 * Return cache timestamps for all analytics of a client.
 * @param {number} clientId
 * @returns {Object} Map of output_type → computed_at ISO string
 */
function getCacheInfo(clientId) {
  try {
    const rows = querySql(
      'SELECT output_type, cache_key, computed_at FROM analytics_cache WHERE client_id = ?',
      [clientId]
    );
    const info = {};
    for (const r of rows) {
      info[r.output_type] = r.computed_at;
    }
    return info;
  } catch (e) {
    return {};
  }
}

/**
 * Recompute all analytics for a client and persist to DB cache.
 * Runs in background — clears old cache first, then computes each module sequentially.
 * @param {number} clientId
 */
async function recomputeAllAnalytics(clientId) {
  // Lazy-require to avoid circular deps — modules require engine.js
  const { computeBinProfiles } = require('./bin-profiles');
  const { computeBinClusters } = require('./bin-clusters');
  const { computeGatewayProfiles } = require('./gateway-profiles');
  const { computeTxTypeAnalysis } = require('./txtype-analysis');
  const { computeRoutingRecommendations } = require('./routing-recommendations');
  const { computeLiftOpportunities } = require('./lift-opportunities');
  const { computeConfidenceLayer } = require('./confidence-layer');
  const { computeTrendDetection } = require('./trend-detection');
  const { computeCrmRules } = require('./crm-rules');
  const { computePricePoints, computeSalvageSequence } = require('./price-points');
  const analyses = [
    { name: 'bin-profiles', fn: computeBinProfiles },
    { name: 'bin-clusters', fn: computeBinClusters },
    { name: 'gateway-profiles', fn: computeGatewayProfiles },
    // { name: 'decline-matrix', fn: computeDeclineMatrix }, // disabled — 6+ min query, not needed now
    { name: 'txtype-analysis', fn: computeTxTypeAnalysis },
    { name: 'routing-recommendations', fn: computeRoutingRecommendations },
    { name: 'lift-opportunities', fn: computeLiftOpportunities },
    { name: 'confidence-layer', fn: computeConfidenceLayer },
    { name: 'trend-detection', fn: computeTrendDetection },
    { name: 'crm-rules', fn: computeCrmRules },
    { name: 'price-points', fn: computePricePoints },
    { name: 'salvage-sequence', fn: computeSalvageSequence },
    // flow-optix V1 removed from recompute — V2 is the active engine
    { name: 'flow-optix-v2', fn: require('./flow-optix-v2').computeFlowOptixV2 },
    { name: 'flow-optix-v2-initials', fn: require('./flow-optix-v2').computeFlowOptixV2Initials },
    { name: 'routing-playbook', fn: require('./routing-playbook').computeRoutingPlaybook },
  ];

  console.log(`[Cache] Recomputing all analytics for client ${clientId}...`);
  clearCache();
  _forceCompute = true;

  let succeeded = 0;
  let failed = 0;

  try {
    for (const { name, fn } of analyses) {
      try {
        const start = Date.now();
        await fn(clientId, {});
        const elapsed = ((Date.now() - start) / 1000).toFixed(1);
        console.log(`[Cache]   ${name} — ${elapsed}s`);
        succeeded++;
      } catch (err) {
        console.error(`[Cache]   ${name} FAILED:`, err.message);
        failed++;
      }
    }
  } finally {
    _forceCompute = false;
  }

  console.log(`[Cache] Recompute complete: ${succeeded} ok, ${failed} failed.`);
}

// ---------------------------------------------------------------------------
// formatGatewayName
// ---------------------------------------------------------------------------

/**
 * Return a human-friendly display name for a gateway.
 *
 * @param {{ gateway_alias?: string, gateway_id?: number, bank_name?: string, processor_name?: string }} gw
 * @returns {string}
 */
function formatGatewayName(gw) {
  if (!gw) return 'Unknown Gateway';
  if (gw.gateway_alias) return gw.gateway_alias;
  const parts = [];
  if (gw.bank_name) parts.push(gw.bank_name);
  if (gw.processor_name) parts.push(gw.processor_name);
  if (parts.length > 0) return parts.join(' / ');
  return `Gateway #${gw.gateway_id || '?'}`;
}

// ---------------------------------------------------------------------------
// Shared SQL helpers (used across analytics modules)
// ---------------------------------------------------------------------------

/**
 * Build a SQL IN clause placeholder string for an array.
 * @param {Array} arr
 * @returns {string} e.g. "(?,?,?)"
 */
function sqlIn(arr) {
  return `(${arr.map(() => '?').join(',')})`;
}

/**
 * Build a date-range WHERE fragment.
 * @param {number} days
 * @returns {string}
 */
function daysAgoFilter(days) {
  return `o.acquisition_date >= date('now', '-${parseInt(days, 10)} days')`;
}

/**
 * Compute variance of an array of numbers.
 * @param {number[]} values
 * @returns {number}
 */
function stddev(values) {
  if (!values || values.length < 2) return 0;
  const mean = values.reduce((s, v) => s + v, 0) / values.length;
  const sqDiffs = values.map(v => (v - mean) ** 2);
  return Math.sqrt(sqDiffs.reduce((s, v) => s + v, 0) / values.length);
}

// ---------------------------------------------------------------------------
// Cascade correction helpers
// ---------------------------------------------------------------------------

/**
 * Base WHERE for cascade correction — any cascaded order with original_gateway_id.
 * No product role filter — applies to initials, upsells, rebills, straight sales.
 * Clients may cascade any order type.
 */
const CASCADE_WHERE = 'o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL';

/**
 * Fetch cascade decline rows — synthetic declines attributed to original_gateway_id.
 * Applies to ALL order types (not just initials — some clients cascade rebills too).
 *
 * @param {number} clientId
 * @param {string} selectClause - SELECT columns (use `o.original_gateway_id AS gateway_id`)
 * @param {string} joinClause   - JOIN fragments (use `o.original_gateway_id` for gateway join)
 * @param {string} extraWhere   - Additional WHERE conditions (AND-prefixed)
 * @param {Array}  extraParams  - Bind params for extraWhere
 * @returns {Array} Rows with gateway_id = original declining gateway
 */
function fetchCascadeDeclines(clientId, selectClause, joinClause, extraWhere = '', extraParams = []) {
  const sql = `
    SELECT ${selectClause}
    FROM orders o
    ${joinClause}
    WHERE o.client_id = ? AND ${CASCADE_WHERE}
      AND ${CLEAN_FILTER}
      ${extraWhere}
  `;
  return querySql(sql, [clientId, ...extraParams]);
}

/**
 * Merge cascade decline rows into a main result set keyed by one or more fields.
 * For each cascade row, finds the matching main row by key and adds to its totals,
 * or pushes a new row if no match exists.
 *
 * @param {Array}    mainRows     - Primary result rows (mutated in place)
 * @param {Array}    cascadeRows  - Cascade decline rows to merge
 * @param {string[]} keyFields    - Fields that define a unique group (e.g. ['bin', 'gateway_id'])
 * @param {string}   totalField   - Field name for total count (default 'total')
 * @param {string}   declinedField - Field name for declined count (default 'declined'), null to skip
 */
function mergeCascadeDeclines(mainRows, cascadeRows, keyFields, totalField = 'total', declinedField = 'declined') {
  if (!cascadeRows || cascadeRows.length === 0) return;

  // Build index of main rows by composite key
  const index = new Map();
  for (const row of mainRows) {
    const key = keyFields.map(f => row[f]).join('|');
    index.set(key, row);
  }

  for (const cr of cascadeRows) {
    const key = keyFields.map(f => cr[f]).join('|');
    const existing = index.get(key);
    if (existing) {
      existing[totalField] = (existing[totalField] || 0) + (cr[totalField] || cr.casc_count || 0);
      if (declinedField && existing[declinedField] !== undefined) {
        existing[declinedField] = (existing[declinedField] || 0) + (cr[totalField] || cr.casc_count || 0);
      }
    } else {
      // New group — push cascade row as a decline-only entry
      mainRows.push(cr);
    }
  }
}

module.exports = {
  APPROVED_STATUSES,
  APPROVED_STATUS_SQL,
  DECLINED_STATUS,
  CLEAN_FILTER,
  CRM_ROUTING_EXCLUSION,
  CRM_ROUTING_STRINGS,
  TX_TYPE_GROUPS,
  ALL_TX_TYPES,
  CASCADE_WHERE,
  approvalStats,
  confidenceScore,
  trendDirection,
  stabilityFlag,
  recencyWeight,
  getCachedOrCompute,
  clearCache,
  getCacheInfo,
  recomputeAllAnalytics,
  isRecomputing: () => _forceCompute,
  setForceCompute: (val) => { _forceCompute = !!val; },
  formatGatewayName,
  sqlIn,
  daysAgoFilter,
  stddev,
  fetchCascadeDeclines,
  mergeCascadeDeclines,
};
