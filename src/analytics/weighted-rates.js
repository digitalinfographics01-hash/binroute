/**
 * Weighted Approval Rate Engine — shared module for all rebill analysis.
 *
 * Filters:
 *   derived_product_role IN ('main_rebill', 'upsell_rebill')
 *   derived_cycle IN (1, 2)
 *   is_cascaded = 0
 *   is_test = 0, is_internal_test = 0
 *   exclude_from_analysis != 1
 *
 * Weighting: 30d=50%, 30-90d=30%, 90-180d=20%
 *
 * Calculates at both processor and MID level.
 * MID stands out only if 5pp+ above processor average.
 */
const { querySql } = require('../db/connection');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const REBILL_TX_TYPES = ['tp_rebill', 'sticky_cof_rebill'];
const WEIGHTS = { d30: 0.5, d90: 0.3, d180: 0.2 };
const MID_STANDOUT_PP = 5;

// ---------------------------------------------------------------------------
// Core: Compute weighted rates for a BIN group
// ---------------------------------------------------------------------------

/**
 * Compute weighted approval rates for a BIN group at both processor and MID level.
 *
 * @param {number} clientId
 * @param {string[]} bins - BINs in the group (pass [] for all client BINs)
 * @param {object} [opts] - { days: 180 }
 * @returns {{ processors: Map, mids: Map, bestProcessor, bestMid }}
 *
 * Each processor/mid entry:
 *   { name, total, approved, weighted_rate, approved_count,
 *     d30: { att, app }, d90: { att, app }, d180: { att, app } }
 */
function computeWeightedRates(clientId, bins, opts = {}) {
  const binFilter = bins && bins.length > 0
    ? `AND o.cc_first_6 IN (${bins.map(() => '?').join(',')})`
    : '';
  const binParams = bins && bins.length > 0 ? bins : [];

  const rows = querySql(`
    SELECT g.processor_name, o.gateway_id, g.gateway_alias,
      CASE
        WHEN o.acquisition_date >= date('now', '-30 days') THEN '30'
        WHEN o.acquisition_date >= date('now', '-90 days') THEN '90'
        ELSE '180'
      END AS bucket,
      COUNT(*) AS attempts,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.derived_cycle IN (1, 2)
      AND o.is_cascaded = 0
      AND o.order_status IN (2, 6, 7, 8)
      AND o.acquisition_date >= date('now', '-180 days')
      AND COALESCE(g.exclude_from_analysis, 0) != 1
      ${opts.includeInactive ? '' : 'AND g.gateway_active = 1'}
      ${binFilter}
    GROUP BY g.processor_name, o.gateway_id, bucket
  `, [clientId, ...binParams]);

  // Aggregate by processor
  const processors = new Map();
  // Aggregate by MID
  const mids = new Map();

  for (const row of rows) {
    const proc = row.processor_name || 'Unknown';
    const gwId = row.gateway_id;
    const gwName = row.gateway_alias || `GW ${gwId}`;
    const bucket = row.bucket; // '30', '90', '180'

    // Processor level
    if (!processors.has(proc)) {
      processors.set(proc, _emptyEntry(proc));
    }
    const pe = processors.get(proc);
    _addBucket(pe, bucket, row.attempts, row.approved);

    // MID level
    if (!mids.has(gwId)) {
      mids.set(gwId, { ..._emptyEntry(gwName), gateway_id: gwId, processor_name: proc });
    }
    const me = mids.get(gwId);
    _addBucket(me, bucket, row.attempts, row.approved);
  }

  // Finalize weighted rates
  for (const e of processors.values()) _finalize(e);
  for (const e of mids.values()) _finalize(e);

  // Find best processor
  let bestProcessor = null;
  for (const [name, e] of processors) {
    if (!bestProcessor || e.weighted_rate > bestProcessor.weighted_rate) {
      bestProcessor = { name, ...e };
    }
  }

  // Find best MID (must be 5pp+ above its processor avg to stand out)
  let bestMid = null;
  for (const [gwId, e] of mids) {
    const procRate = processors.get(e.processor_name)?.weighted_rate || 0;
    e.standout = e.weighted_rate - procRate >= MID_STANDOUT_PP;
    if (!bestMid || e.weighted_rate > bestMid.weighted_rate) {
      bestMid = { gateway_id: gwId, ...e };
    }
  }

  return { processors, mids, bestProcessor, bestMid };
}

/**
 * Compute weighted rates for ALL BIN groups at a given level for a client.
 * Returns a Map of groupKey → { bins, processors, mids, bestProcessor, bestMid }.
 *
 * @param {number} clientId
 * @param {number} level - Grouping level (1-4)
 * @returns {Map<string, object>}
 */
function computeAllGroupRates(clientId, level, opts = {}) {
  // First, get all BINs with their metadata
  const binRows = querySql(`
    SELECT DISTINCT o.cc_first_6 AS bin, b.issuer_bank, b.card_brand, b.card_type, b.card_level, b.is_prepaid
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.derived_cycle IN (1, 2)
      AND o.is_cascaded = 0
      AND o.acquisition_date >= date('now', '-180 days')
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
  `, [clientId]);

  // Group BINs by level
  const groups = new Map();
  for (const row of binRows) {
    const key = _groupKey(row, level);
    if (!groups.has(key)) {
      groups.set(key, { bins: [], meta: row });
    }
    groups.get(key).bins.push(row.bin);
  }

  // Compute weighted rates per group
  const results = new Map();
  for (const [key, group] of groups) {
    const rates = computeWeightedRates(clientId, group.bins, opts);
    results.set(key, {
      groupKey: key,
      bins: group.bins,
      meta: group.meta,
      ...rates,
    });
  }

  return results;
}

// ---------------------------------------------------------------------------
// Confidence calculation (based on approved orders, not total attempts)
// ---------------------------------------------------------------------------

/**
 * @param {number} approvedCount - Total approved orders (unweighted)
 * @param {'natural'|'salvage'} type
 * @returns {'HIGH'|'MEDIUM'|'LOW'|'GATHERING'|'HIDDEN'}
 */
function getConfidence(approvedCount, type = 'natural') {
  if (type === 'salvage') {
    if (approvedCount >= 30) return 'HIGH';
    if (approvedCount >= 10) return 'MEDIUM';
    if (approvedCount >= 5) return 'LOW';
    return 'HIDDEN';
  }
  // Natural attempt
  if (approvedCount >= 75) return 'HIGH';
  if (approvedCount >= 30) return 'MEDIUM';
  if (approvedCount >= 10) return 'LOW';
  return 'GATHERING';
}

/**
 * Check if processor has only historical data (no traffic in last 30 days).
 */
function isHistoricalOnly(entry) {
  return entry && entry.d30.att === 0 && (entry.d90.att > 0 || entry.d180.att > 0);
}

// ---------------------------------------------------------------------------
// Processor selection logic
// ---------------------------------------------------------------------------

/**
 * Determine processor selection type for a BIN group.
 *
 * @param {Map} processors - From computeWeightedRates
 * @param {Map} mids - From computeWeightedRates
 * @returns {{ type, primary, secondary, bestMid, variance }}
 *   type: 'CLEAR_WINNER' | 'SPLIT_TEST' | 'CONVERGED' | 'SINGLE' | 'GATHERING'
 */
function selectProcessor(processors, mids) {
  const active = [...processors.entries()]
    .filter(([, e]) => e.approved_count >= 10)
    .sort((a, b) => b[1].weighted_rate - a[1].weighted_rate);

  const gathering = [...processors.entries()]
    .filter(([, e]) => e.approved_count < 10);

  if (active.length === 0) {
    return {
      type: 'GATHERING',
      primary: null,
      secondary: null,
      bestMid: null,
      variance: 0,
      gatheringProcessors: gathering.map(([name, e]) => ({ name, approved: e.approved_count })),
    };
  }

  if (active.length === 1) {
    const [name, entry] = active[0];
    const standoutMid = _findStandoutMid(mids, name);
    return {
      type: 'SINGLE',
      primary: { name, ...entry },
      secondary: null,
      bestMid: standoutMid,
      variance: 0,
      missingProcessors: gathering.map(([n]) => n),
    };
  }

  // 2+ active processors
  const [first, second] = active;
  const variance = Math.round((first[1].weighted_rate - second[1].weighted_rate) * 100) / 100;
  const standoutMid = _findStandoutMid(mids, first[0]);

  let type;
  if (variance > 5) type = 'CLEAR_WINNER';
  else if (variance >= 3) type = 'SPLIT_TEST';
  else type = 'CONVERGED';

  return {
    type,
    primary: { name: first[0], ...first[1] },
    secondary: { name: second[0], ...second[1] },
    tertiary: active[2] ? { name: active[2][0], ...active[2][1] } : null,
    bestMid: standoutMid,
    variance,
  };
}

// ---------------------------------------------------------------------------
// Fallback chain
// ---------------------------------------------------------------------------

/**
 * Build a ranked fallback chain from processor selection results.
 *
 * @param {object} selection - From selectProcessor()
 * @param {Map} mids - From computeWeightedRates
 * @returns {{ primary_gateway_id, secondary_gateway_id, tertiary_gateway_id }}
 */
function buildFallbackChain(selection, mids) {
  const chain = { primary_gateway_id: null, secondary_gateway_id: null, tertiary_gateway_id: null };

  if (!selection.primary) return chain;

  // Find best MID for primary processor
  chain.primary_gateway_id = _bestMidForProcessor(mids, selection.primary.name);

  if (selection.secondary) {
    chain.secondary_gateway_id = _bestMidForProcessor(mids, selection.secondary.name);
  }

  if (selection.tertiary) {
    chain.tertiary_gateway_id = _bestMidForProcessor(mids, selection.tertiary.name);
  }

  return chain;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function _emptyEntry(name) {
  return {
    name,
    total: 0,
    approved_count: 0,
    weighted_rate: 0,
    d30: { att: 0, app: 0 },
    d90: { att: 0, app: 0 },
    d180: { att: 0, app: 0 },
  };
}

function _addBucket(entry, bucket, attempts, approved) {
  entry.total += attempts;
  entry.approved_count += approved;
  if (bucket === '30') { entry.d30.att += attempts; entry.d30.app += approved; }
  else if (bucket === '90') { entry.d90.att += attempts; entry.d90.app += approved; }
  else { entry.d180.att += attempts; entry.d180.app += approved; }
}

function _finalize(entry) {
  const wApp = entry.d30.app * WEIGHTS.d30 + entry.d90.app * WEIGHTS.d90 + entry.d180.app * WEIGHTS.d180;
  const wAtt = entry.d30.att * WEIGHTS.d30 + entry.d90.att * WEIGHTS.d90 + entry.d180.att * WEIGHTS.d180;
  entry.weighted_rate = wAtt > 0 ? Math.round((wApp / wAtt) * 10000) / 100 : 0;
}

function _groupKey(row, level) {
  const bank = row.issuer_bank || 'Unknown';
  const brand = row.card_brand || 'Unknown';
  const isPrepaid = row.is_prepaid ? true : false;
  const type = isPrepaid ? 'PREPAID' : (row.card_type || 'Unknown');
  const cardLevel = row.card_level || 'Unknown';
  switch (level) {
    case 1: return bank;
    case 2: return `${bank}|${brand}`;
    case 3: return `${bank}|${brand}|${type}`;
    case 4: return `${bank}|${brand}|${type}|${cardLevel}`;
    default: return `${bank}|${brand}`;
  }
}

function _findStandoutMid(mids, processorName) {
  let best = null;
  for (const [gwId, e] of mids) {
    if (e.processor_name !== processorName) continue;
    if (e.standout && (!best || e.weighted_rate > best.weighted_rate)) {
      best = { gateway_id: gwId, ...e };
    }
  }
  return best;
}

function _bestMidForProcessor(mids, processorName) {
  let bestGwId = null;
  let bestRate = -1;
  for (const [gwId, e] of mids) {
    if (e.processor_name !== processorName) continue;
    if (e.weighted_rate > bestRate) {
      bestRate = e.weighted_rate;
      bestGwId = gwId;
    }
  }
  return bestGwId;
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  computeWeightedRates,
  computeAllGroupRates,
  getConfidence,
  isHistoricalOnly,
  selectProcessor,
  buildFallbackChain,
  REBILL_TX_TYPES,
  WEIGHTS,
  MID_STANDOUT_PP,
};
