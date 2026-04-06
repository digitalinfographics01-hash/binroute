/**
 * Level Engine — Shared 5-level analysis engine.
 *
 * Used by both Beast Insights (initials) and Flow Optix (rebills).
 * Implements hierarchical BIN group analysis with automatic drill-down
 * based on variance thresholds.
 *
 * Levels:
 *   L1: Issuer Bank
 *   L2: Card Brand (within an issuer)
 *   L3: Card Type + Prepaid (within a brand)
 *   L4: Card Level (within a card type)
 *   L5: Individual BIN (outlier detection within a card level)
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql } = require('../db/connection');
const { CLEAN_FILTER, CASCADE_WHERE, CRM_ROUTING_EXCLUSION, daysAgoFilter, sqlIn } = require('./engine');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Variance thresholds (percentage points) that trigger drill-down to the next level. */
const VARIANCE_THRESHOLDS = {
  1: 5,   // L1→L2: 5pp between brands
  2: 5,   // L2→L3: 5pp between card types
  3: 5,   // L3→L4: 5pp between card levels
  4: 10,  // L4→L5: 10pp between individual BINs (outlier)
};

/** Confidence tiers keyed by level — attempt thresholds for HIGH / MEDIUM / LOW. */
const CONFIDENCE = {
  1: { HIGH: 200, MEDIUM: 100, LOW: 50 },
  2: { HIGH: 150, MEDIUM: 75,  LOW: 30 },
  3: { HIGH: 100, MEDIUM: 50,  LOW: 30 },
  4: { HIGH: 75,  MEDIUM: 40,  LOW: 30 },
  5: { HIGH: 50,  MEDIUM: 30,  LOW: 0 },
};

/** Minimum total attempts for a group to be included in analysis. */
const MIN_ATTEMPTS = 30;

/** Minimum attempts for a sub-group to count as "qualified" in variance calc. */
const MIN_SUB_GROUP_ATTEMPTS = 30;

// ---------------------------------------------------------------------------
// getConfidence
// ---------------------------------------------------------------------------

/**
 * Determine confidence tier for a given level and attempt count.
 *
 * @param {number} level    - Analysis level (1-5)
 * @param {number} attempts - Number of transaction attempts
 * @returns {'HIGH'|'MEDIUM'|'LOW'|'INSUFFICIENT'}
 */
function getConfidence(level, attempts) {
  const tiers = CONFIDENCE[level];
  if (!tiers) return 'INSUFFICIENT';
  if (attempts >= tiers.HIGH) return 'HIGH';
  if (attempts >= tiers.MEDIUM) return 'MEDIUM';
  if (attempts >= tiers.LOW) return 'LOW';
  return 'INSUFFICIENT';
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Build the base WHERE clause and params for level-engine queries.
 * Joins orders → bin_lookup, filters by client, tx types, date range,
 * clean traffic, CRM exclusion, and gateway exclusion.
 */
function _baseWhere(clientId, txTypes, days) {
  const placeholders = txTypes.map(() => '?').join(',');
  const where = `
    o.client_id = ?
    AND ${CLEAN_FILTER}
    AND ${CRM_ROUTING_EXCLUSION}
    AND o.order_status IN (2,6,7,8)
    AND o.tx_type IN (${placeholders})
    AND ${daysAgoFilter(days)}
    AND COALESCE(g.exclude_from_analysis, 0) != 1
  `;
  const params = [clientId, ...txTypes];
  return { where, params };
}

/**
 * Compute approval rate rounded to 2 decimal places.
 */
function _rate(approved, attempts) {
  if (!attempts || attempts === 0) return 0;
  return Math.round((approved / attempts) * 10000) / 100;
}

/**
 * Determine the current level based on which optional filters are provided.
 */
function _currentLevel(opts) {
  if (opts.cardLevel) return 4;
  if (opts.cardType != null || opts.isPrepaid != null) return 3;
  if (opts.cardBrand) return 2;
  return 1;
}

/**
 * Build a human-readable group name from the filter opts.
 */
function _groupName(opts, level) {
  const parts = [opts.issuerBank];
  if (level >= 2 && opts.cardBrand) parts.push(opts.cardBrand);
  if (level >= 3) {
    if (opts.isPrepaid) {
      parts.push('Prepaid');
    } else if (opts.cardType) {
      parts.push(opts.cardType);
    }
  }
  if (level >= 4 && opts.cardLevel) parts.push(opts.cardLevel);
  return parts.join(' › ');
}

/**
 * Build a stable machine key from the filter opts.
 */
function _groupKey(opts, level) {
  const parts = [(opts.issuerBank || '').toLowerCase().replace(/[^a-z0-9]+/g, '_')];
  if (level >= 2 && opts.cardBrand) parts.push(opts.cardBrand.toLowerCase().replace(/[^a-z0-9]+/g, '_'));
  if (level >= 3) {
    if (opts.isPrepaid) {
      parts.push('prepaid');
    } else if (opts.cardType) {
      parts.push(opts.cardType.toLowerCase().replace(/[^a-z0-9]+/g, '_'));
    }
  }
  if (level >= 4 && opts.cardLevel) parts.push(opts.cardLevel.toLowerCase().replace(/[^a-z0-9]+/g, '_'));
  return parts.join(':');
}

/**
 * Build the additional WHERE fragment + params to filter to the current group
 * based on issuerBank, cardBrand, cardType, isPrepaid, and cardLevel.
 */
function _groupFilter(opts) {
  let where = ' AND b.issuer_bank = ?';
  const params = [opts.issuerBank];

  if (opts.cardBrand) {
    where += ' AND b.card_brand = ?';
    params.push(opts.cardBrand);
  }
  if (opts.cardType != null) {
    where += ' AND b.card_type = ?';
    params.push(opts.cardType);
  }
  if (opts.isPrepaid != null) {
    where += ' AND b.is_prepaid = ?';
    params.push(opts.isPrepaid);
  }
  if (opts.cardLevel) {
    where += ' AND b.card_level = ?';
    params.push(opts.cardLevel);
  }

  return { where, params };
}

// ---------------------------------------------------------------------------
// analyzeGroup
// ---------------------------------------------------------------------------

/**
 * Analyze a single BIN group at the appropriate level.
 *
 * @param {number} clientId
 * @param {object} opts
 * @param {string[]} opts.txTypes      - Transaction type filters
 * @param {number}   [opts.days=180]   - Lookback window
 * @param {string}   opts.issuerBank   - L1 issuer bank (required)
 * @param {string}   [opts.cardBrand]  - L2 card brand
 * @param {string}   [opts.cardType]   - L3 card type
 * @param {number}   [opts.isPrepaid]  - L3 prepaid flag (0 or 1)
 * @param {string}   [opts.cardLevel]  - L4 card level
 * @returns {object} Analysis result (see module docs for shape)
 */
function analyzeGroup(clientId, opts) {
  const txTypes = opts.txTypes;
  const days = opts.days || 180;
  const level = _currentLevel(opts);

  // --- Base query components ------------------------------------------------
  const base = _baseWhere(clientId, txTypes, days);
  const group = _groupFilter(opts);

  const joinSql = `
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN gateways g ON o.gateway_id = g.gateway_id AND o.client_id = g.client_id
    WHERE ${base.where} ${group.where}
  `;
  const allParams = [...base.params, ...group.params];

  // --- Current group aggregate ----------------------------------------------
  const aggRow = querySql(`
    SELECT
      COUNT(*) AS attempts,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    ${joinSql}
  `, allParams)[0] || { attempts: 0, approved: 0 };

  // Cascade correction: count cascaded orders in this group as additional declines
  const cascAgg = querySql(`
    SELECT COUNT(*) AS casc_declines
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN gateways g ON o.original_gateway_id = g.gateway_id AND o.client_id = g.client_id
    WHERE ${base.where} ${group.where}
      AND ${CASCADE_WHERE}
  `, [...base.params, ...group.params])[0] || { casc_declines: 0 };

  const attempts = aggRow.attempts + cascAgg.casc_declines;
  const approved = aggRow.approved;
  const rate = _rate(approved, attempts);
  const confidence = getConfidence(level, attempts);

  // --- BINs in this group (all distinct, for reference) ---------------------
  const binRows = querySql(`
    SELECT DISTINCT o.cc_first_6 AS bin
    ${joinSql}
    AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
  `, allParams);
  const bins = binRows.map(r => r.bin);

  // --- Sub-group analysis (next level down) ---------------------------------
  const subGroups = _analyzeSubGroups(clientId, txTypes, days, opts, level, joinSql, allParams);

  // --- Variance analysis ----------------------------------------------------
  const qualified = subGroups.filter(sg => !sg.gathering);
  const gathering = subGroups.filter(sg => sg.gathering);
  const threshold = VARIANCE_THRESHOLDS[level] || Infinity;

  let varianceValue = 0;
  if (qualified.length >= 2) {
    const rates = qualified.map(sg => sg.rate);
    varianceValue = Math.round((Math.max(...rates) - Math.min(...rates)) * 100) / 100;
  }

  const variance = {
    value: varianceValue,
    threshold,
    shouldPromote: varianceValue >= threshold && qualified.length >= 2,
    qualifiedSubGroups: qualified.length,
    gatheringSubGroups: gathering.length,
  };

  // --- Outlier detection (only for L4 groups → L5 BIN level) ----------------
  const outliers = level === 4 ? _detectOutliers(clientId, txTypes, days, opts, joinSql, allParams, rate) : [];

  return {
    level,
    groupName: _groupName(opts, level),
    groupKey: _groupKey(opts, level),
    attempts,
    approved,
    rate,
    confidence,
    bins,
    subGroups,
    variance,
    outliers,
  };
}

// ---------------------------------------------------------------------------
// Sub-group analysis
// ---------------------------------------------------------------------------

/**
 * Query and compute sub-groups for the next level down.
 */
function _analyzeSubGroups(clientId, txTypes, days, opts, level, joinSql, allParams) {
  // Determine the GROUP BY expression and label builder for the next level
  let groupByExpr, keyFn, labelFn;

  if (level === 1) {
    // L1 → L2: group by card_brand
    groupByExpr = 'b.card_brand';
    keyFn = row => row.sub_key || 'Unknown';
    labelFn = row => row.sub_key || 'Unknown Brand';
  } else if (level === 2) {
    // L2 → L3: prepaid split first, then card_type within non-prepaid
    // Prepaid is its own final group — no further card_type split
    // Non-prepaid splits into Credit/Debit
    groupByExpr = 'b.is_prepaid, CASE WHEN b.is_prepaid = 1 THEN \'Prepaid\' ELSE b.card_type END';
    keyFn = row => row.is_prepaid ? 'prepaid' : (row.card_type || 'Unknown').toLowerCase();
    labelFn = row => row.is_prepaid ? 'Prepaid' : (row.card_type || 'Unknown');
  } else if (level === 3) {
    // L3 → L4: group by card_level
    groupByExpr = 'b.card_level';
    keyFn = row => row.sub_key || 'Unknown';
    labelFn = row => row.sub_key || 'Unknown Level';
  } else if (level === 4) {
    // L4 → L5: group by individual BIN (for outlier detection)
    groupByExpr = 'o.cc_first_6';
    keyFn = row => row.sub_key || 'Unknown';
    labelFn = row => `BIN ${row.sub_key || 'Unknown'}`;
  } else {
    // L5 has no further drill-down
    return [];
  }

  // For L2→L3 split: select both is_prepaid and card_type so keyFn/labelFn can use them
  const isL2Split = level === 2;
  const selectExpr = isL2Split
    ? 'b.is_prepaid, b.card_type'
    : groupByExpr + ' AS sub_key';

  const subRows = querySql(`
    SELECT
      ${selectExpr},
      COUNT(*) AS attempts,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
      GROUP_CONCAT(DISTINCT o.cc_first_6) AS bin_list
    ${joinSql}
    AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
    GROUP BY ${groupByExpr}
    ORDER BY attempts DESC
  `, allParams);

  return subRows.map(row => {
    const sgAttempts = row.attempts;
    const sgApproved = row.approved;
    const sgRate = _rate(sgApproved, sgAttempts);
    const nextLevel = level + 1;
    const sgConfidence = getConfidence(nextLevel, sgAttempts);
    const isGathering = sgAttempts < MIN_SUB_GROUP_ATTEMPTS;
    const progress = isGathering ? Math.round((sgAttempts / MIN_SUB_GROUP_ATTEMPTS) * 100) / 100 : 1;
    const sgBins = row.bin_list ? row.bin_list.split(',') : [];

    return {
      key: keyFn(row),
      label: labelFn(row),
      attempts: sgAttempts,
      approved: sgApproved,
      rate: sgRate,
      confidence: sgConfidence,
      bins: sgBins,
      gathering: isGathering,
      progress,
    };
  });
}

// ---------------------------------------------------------------------------
// Outlier detection (L4 → L5)
// ---------------------------------------------------------------------------

/**
 * Detect BINs that deviate 10+ pp from the group average.
 * Only considers BINs with MIN_SUB_GROUP_ATTEMPTS or more.
 */
function _detectOutliers(clientId, txTypes, days, opts, joinSql, allParams, groupAvgRate) {
  const binRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      COUNT(*) AS attempts,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    ${joinSql}
    AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
    GROUP BY o.cc_first_6
    HAVING attempts >= ?
    ORDER BY attempts DESC
  `, [...allParams, MIN_SUB_GROUP_ATTEMPTS]);

  const outliers = [];
  const outlierThreshold = VARIANCE_THRESHOLDS[4]; // 10pp

  for (const row of binRows) {
    const binRate = _rate(row.approved, row.attempts);
    const deviation = Math.round((binRate - groupAvgRate) * 100) / 100;

    if (Math.abs(deviation) >= outlierThreshold) {
      outliers.push({
        bin: row.bin,
        attempts: row.attempts,
        rate: binRate,
        deviation,
        direction: deviation > 0 ? 'up' : 'down',
      });
    }
  }

  // Sort by absolute deviation descending
  outliers.sort((a, b) => Math.abs(b.deviation) - Math.abs(a.deviation));
  return outliers;
}

// ---------------------------------------------------------------------------
// analyzeAllGroups
// ---------------------------------------------------------------------------

/**
 * Analyze ALL L1 issuer-bank groups that meet the minimum attempt threshold,
 * then automatically drill down where variance warrants it.
 *
 * @param {number} clientId
 * @param {object} opts
 * @param {string[]} opts.txTypes       - Transaction type filters
 * @param {number}   [opts.days=180]    - Lookback window
 * @param {number}   [opts.minAttempts=30] - Minimum attempts for an L1 group
 * @returns {Array<object>} Array of analysis trees sorted by attempts DESC
 */
function analyzeAllGroups(clientId, opts) {
  const txTypes = opts.txTypes;
  const days = opts.days || 180;
  const minAttempts = opts.minAttempts || MIN_ATTEMPTS;

  // 1. Get all distinct issuer_bank values with attempt counts
  const base = _baseWhere(clientId, txTypes, days);
  const issuerRows = querySql(`
    SELECT
      b.issuer_bank,
      COUNT(*) AS attempts
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN gateways g ON o.gateway_id = g.gateway_id AND o.client_id = g.client_id
    WHERE ${base.where}
      AND b.issuer_bank IS NOT NULL AND b.issuer_bank != ''
    GROUP BY b.issuer_bank
    HAVING attempts >= ?
    ORDER BY attempts DESC
  `, [...base.params, minAttempts]);

  // 2. For each issuer, run L1 analysis then auto-drill
  const results = [];

  for (const row of issuerRows) {
    const l1 = analyzeGroup(clientId, {
      txTypes,
      days,
      issuerBank: row.issuer_bank,
    });

    // Attach drill-down results to sub-groups where variance warrants it
    l1.drillDown = _autoDrill(clientId, txTypes, days, l1);
    results.push(l1);
  }

  return results;
}

/**
 * Recursively drill into sub-groups where variance exceeds the threshold.
 * Returns an array of deeper analysis objects keyed by sub-group key.
 */
function _autoDrill(clientId, txTypes, days, analysis) {
  if (!analysis.variance.shouldPromote) return null;
  if (analysis.level >= 4) return null; // L5 is the deepest

  const drilled = {};

  for (const sg of analysis.subGroups) {
    if (sg.gathering) continue; // not enough data yet

    // Build opts for the next level down
    const nextOpts = { txTypes, days };

    // Inherit existing group filters based on current level
    if (analysis.level >= 1) nextOpts.issuerBank = _extractIssuerBank(analysis);
    if (analysis.level >= 2) nextOpts.cardBrand = _extractCardBrand(analysis);

    // Apply the sub-group's distinguishing attribute
    if (analysis.level === 1) {
      // Drilling L1→L2: sub-group key is card brand
      nextOpts.cardBrand = sg.key;
    } else if (analysis.level === 2) {
      // Drilling L2→L3: sub-group key is "prepaid" or card_type name (e.g. "credit", "debit")
      if (sg.key === 'prepaid') {
        nextOpts.isPrepaid = 1;
      } else {
        nextOpts.isPrepaid = 0;
        nextOpts.cardType = sg.key.charAt(0).toUpperCase() + sg.key.slice(1); // "credit" → "Credit"
      }
    } else if (analysis.level === 3) {
      // Drilling L3→L4: sub-group key is card level
      nextOpts.cardType = _extractCardType(analysis);
      nextOpts.isPrepaid = _extractIsPrepaid(analysis);
      nextOpts.cardLevel = sg.key;
    }

    const deeper = analyzeGroup(clientId, nextOpts);
    deeper.drillDown = _autoDrill(clientId, txTypes, days, deeper);
    drilled[sg.key] = deeper;
  }

  return Object.keys(drilled).length > 0 ? drilled : null;
}

/**
 * Extract the issuer bank from an analysis object's groupKey.
 */
function _extractIssuerBank(analysis) {
  // The groupName always starts with the issuer bank
  const parts = analysis.groupName.split(' › ');
  return parts[0];
}

/**
 * Extract the card brand from an L2+ analysis object.
 */
function _extractCardBrand(analysis) {
  const parts = analysis.groupName.split(' › ');
  return parts.length >= 2 ? parts[1] : null;
}

/**
 * Extract the card type from an L3+ analysis object.
 */
function _extractCardType(analysis) {
  const parts = analysis.groupName.split(' › ');
  if (parts.length < 3) return null;
  // Remove "Prepaid " prefix if present
  return parts[2].replace(/^Prepaid\s+/i, '');
}

/**
 * Extract the isPrepaid flag from an L3+ analysis object.
 */
function _extractIsPrepaid(analysis) {
  const parts = analysis.groupName.split(' › ');
  if (parts.length < 3) return 0;
  return /^Prepaid\s/i.test(parts[2]) ? 1 : 0;
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  analyzeGroup,
  analyzeAllGroups,
  getConfidence,
  VARIANCE_THRESHOLDS,
  CONFIDENCE,
  MIN_ATTEMPTS,
  MIN_SUB_GROUP_ATTEMPTS,
};
