/**
 * Split Engine — Smart sub-group splitting with manual confirmation.
 *
 * Detects when a sub-group within a rule card's level behaves differently
 * (approval rate variance exceeds threshold), suggests splits, and manages
 * the full lifecycle: suggest → confirm → track → merge back.
 *
 * Used by both Beast Insights (initials) and Flow Optix (rebills).
 *
 * All splits and merges require manual user confirmation.
 * BinRoute never pushes changes to Beast or Flow Optix automatically.
 */
const { querySql, runSql, saveDb } = require('../db/connection');
const { analyzeGroup, VARIANCE_THRESHOLDS, MIN_SUB_GROUP_ATTEMPTS } = require('./level-engine');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Split confidence tiers based on sub-group attempts */
const SPLIT_CONFIDENCE = {
  LOW:    { min: 30, max: 49 },
  MEDIUM: { min: 50, max: 74 },
  HIGH:   { min: 75, max: Infinity },
};

function splitConfidence(attempts) {
  if (attempts >= SPLIT_CONFIDENCE.HIGH.min) return 'HIGH';
  if (attempts >= SPLIT_CONFIDENCE.MEDIUM.min) return 'MEDIUM';
  if (attempts >= SPLIT_CONFIDENCE.LOW.min) return 'LOW';
  return 'INSUFFICIENT';
}

// ---------------------------------------------------------------------------
// Core: Compute split suggestions for a rule card
// ---------------------------------------------------------------------------

/**
 * Analyze a rule card's sub-groups and return at most ONE split suggestion
 * (the highest-variance qualified sub-group).
 *
 * @param {number} clientId
 * @param {object} rule - A rule object from crm-rules or flow-optix
 * @param {object} opts - { txTypes, days }
 * @returns {object|null} Split suggestion or null
 */
function computeSplitSuggestion(clientId, rule, opts) {
  if (!rule || !rule.appliesTo) return null;

  const level = rule.level;
  if (level >= 5) return null; // L5 (single BIN) can't be split further

  const a = rule.appliesTo;
  if (!a.issuer_bank || a.issuer_bank === 'Unknown') return null;

  const analysisOpts = {
    txTypes: opts.txTypes,
    days: opts.days || 180,
    issuerBank: a.issuer_bank,
  };
  if (a.card_brand) analysisOpts.cardBrand = a.card_brand;
  if (a.card_type) {
    analysisOpts.cardType = a.card_type;
    analysisOpts.isPrepaid = a.is_prepaid || 0;
  }
  if (a.card_level) analysisOpts.cardLevel = a.card_level;

  // Use the existing level-engine to get sub-group analysis
  const analysis = analyzeGroup(clientId, analysisOpts);

  // Find already-split sub-groups for this rule
  const existingSplits = _getActiveSplitsForRule(clientId, rule.ruleId);
  const splitKeys = new Set(existingSplits.map(s => s.split_reason));

  // Base result — always returned (gathering state, outliers)
  const result = {
    ruleId: rule.ruleId,
    ruleName: rule.ruleName,
    level,
    splitReady: false,
    splitSubGroup: null,
    siblingRate: 0,
    siblingAttempts: 0,
    variance: 0,
    threshold: VARIANCE_THRESHOLDS[level] || 5,
    remainingBins: rule.binsInGroup || [],
    remainingAttempts: 0,
    remainingRate: 0,
    subGroupStatus: (analysis?.subGroups || []).map(sg => ({
      key: sg.key,
      label: sg.label,
      attempts: sg.attempts,
      rate: sg.rate,
      gathering: sg.gathering,
      progress: sg.progress,
      alreadySplit: splitKeys.has(sg.key),
    })),
    // L4 outlier data
    outliers: (level === 4 && analysis?.outliers) ? analysis.outliers : [],
    totalBinsMonitored: level === 4 ? (analysis?.bins || []).length : 0,
    existingSplits,
    pendingCandidates: 0,
  };

  if (!analysis || !analysis.subGroups || analysis.subGroups.length < 2) return result;

  // Filter to qualified sub-groups (30+ attempts)
  const qualified = analysis.subGroups.filter(sg => !sg.gathering);
  if (qualified.length < 2) return result;

  // Score each qualified sub-group by variance vs siblings
  const threshold = result.threshold;
  const candidates = [];
  for (const sg of qualified) {
    if (splitKeys.has(sg.key)) continue;
    const siblingAttempts = qualified.reduce((s, o) => s + (o.key === sg.key ? 0 : o.attempts), 0);
    const siblingApproved = qualified.reduce((s, o) => s + (o.key === sg.key ? 0 : o.approved), 0);
    const siblingRate = siblingAttempts > 0 ? Math.round((siblingApproved / siblingAttempts) * 10000) / 100 : 0;
    const variance = Math.round(Math.abs(sg.rate - siblingRate) * 100) / 100;

    if (variance >= threshold) {
      candidates.push({ subGroup: sg, siblingRate, siblingAttempts, siblingApproved, variance, confidence: splitConfidence(sg.attempts) });
    }
  }

  if (candidates.length === 0) return result;

  // Return ONLY the highest-variance candidate (sequential split rule)
  candidates.sort((a, b) => b.variance - a.variance);
  const best = candidates[0];
  const remainingBins = (rule.binsInGroup || []).filter(bin => !best.subGroup.bins.includes(bin));

  result.splitReady = true;
  result.splitSubGroup = {
    key: best.subGroup.key, label: best.subGroup.label,
    rate: best.subGroup.rate, attempts: best.subGroup.attempts,
    approved: best.subGroup.approved, bins: best.subGroup.bins,
    confidence: best.confidence,
  };
  result.siblingRate = best.siblingRate;
  result.siblingAttempts = best.siblingAttempts;
  result.variance = best.variance;
  result.remainingBins = remainingBins;
  result.remainingAttempts = best.siblingAttempts;
  result.remainingRate = best.siblingRate;
  result.pendingCandidates = candidates.length - 1;

  return result;
}

// ---------------------------------------------------------------------------
// Confirm split
// ---------------------------------------------------------------------------

/**
 * Confirm a split. Creates new child rule, updates parent BINs, logs to rule_splits.
 *
 * @param {number} clientId
 * @param {string} ruleId - Parent rule ID (e.g. "BR-002")
 * @param {string} ruleType - "beast" or "flow_optix"
 * @param {object} splitData - { subGroupKey, subGroupLabel, bins, variance, attempts, siblingRate }
 * @returns {{ success: boolean, error?: string, childRuleId?: string }}
 */
function confirmSplit(clientId, ruleId, ruleType, splitData) {
  // --- Recompute lock check ---
  try {
    const { isRecomputing } = require('./engine');
    if (isRecomputing()) {
      return { success: false, error: 'Analysis update in progress. Please wait and try again.' };
    }
  } catch (e) { /* engine may not expose this yet */ }

  const table = ruleType === 'flow_optix' ? 'flow_optix_rules' : 'beast_rules';

  // --- BIN ownership check ---
  const conflictBin = _checkBinConflicts(clientId, splitData.bins, ruleId, table);
  if (conflictBin) {
    return {
      success: false,
      error: `BIN ${conflictBin.bin} already assigned to ${conflictBin.ruleName}. Resolve conflict before splitting.`,
    };
  }

  // --- Get parent rule from DB (or use computed data) ---
  const parentRows = querySql(
    `SELECT * FROM ${table} WHERE client_id = ? AND rule_id = ?`,
    [clientId, ruleId]
  );

  // Generate child rule ID
  const prefix = ruleType === 'flow_optix' ? 'FO' : 'BR';
  const maxIdRow = querySql(
    `SELECT rule_id FROM ${table} WHERE client_id = ? AND rule_id LIKE ? ORDER BY rule_id DESC LIMIT 1`,
    [clientId, `${prefix}-%`]
  );
  let nextNum = 1;
  if (maxIdRow.length > 0) {
    const match = maxIdRow[0].rule_id.match(/\d+/);
    if (match) nextNum = parseInt(match[0], 10) + 1;
  }
  const childRuleId = `${prefix}-${String(nextNum).padStart(3, '0')}`;

  // Insert child rule
  runSql(`INSERT INTO ${table}
    (client_id, rule_id, rule_name, tx_group, group_type, group_conditions,
     stage, status, split_from_rule_id, split_at, split_variance_pp, split_attempts, bins_at_split,
     created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, 1, 'recommended', ?, datetime('now'), ?, ?, ?, datetime('now'), datetime('now'))`,
    [
      clientId, childRuleId, splitData.subGroupLabel,
      splitData.txGroup || 'INITIALS',
      'bin', splitData.bins.join(','),
      ruleId, splitData.variance, splitData.attempts,
      JSON.stringify(splitData.bins),
    ]
  );

  // Update parent rule — remove split BINs from group_conditions
  if (parentRows.length > 0) {
    const parent = parentRows[0];
    const parentBins = (parent.group_conditions || '').split(',').filter(b => b.trim());
    const remainingBins = parentBins.filter(b => !splitData.bins.includes(b.trim()));
    if (remainingBins.length === 0) {
      // Archive empty card
      runSql(`UPDATE ${table} SET status = 'archived', updated_at = datetime('now') WHERE client_id = ? AND rule_id = ?`,
        [clientId, ruleId]);
    } else {
      runSql(`UPDATE ${table} SET group_conditions = ?, updated_at = datetime('now') WHERE client_id = ? AND rule_id = ?`,
        [remainingBins.join(','), clientId, ruleId]);
    }
  }

  // Log to rule_splits
  runSql(`INSERT INTO rule_splits
    (client_id, rule_type, parent_rule_id, child_rule_id, split_level, split_reason,
     variance_pp, attempts_at_split, bins_at_split, split_confirmed_by)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual')`,
    [
      clientId, ruleType, ruleId, childRuleId,
      (splitData.level || 1) + 1,
      splitData.subGroupKey,
      splitData.variance, splitData.attempts,
      JSON.stringify(splitData.bins),
    ]
  );

  saveDb();
  return { success: true, childRuleId };
}

// ---------------------------------------------------------------------------
// Merge back
// ---------------------------------------------------------------------------

/**
 * Merge a child card back into its parent.
 *
 * @param {number} clientId
 * @param {string} childRuleId
 * @param {string} ruleType - "beast" or "flow_optix"
 * @param {string} [reason] - Optional merge reason
 * @returns {{ success: boolean, error?: string }}
 */
function mergeBack(clientId, childRuleId, ruleType, reason) {
  const table = ruleType === 'flow_optix' ? 'flow_optix_rules' : 'beast_rules';

  // Find the child rule
  const childRows = querySql(
    `SELECT * FROM ${table} WHERE client_id = ? AND rule_id = ?`,
    [clientId, childRuleId]
  );
  if (childRows.length === 0) {
    return { success: false, error: 'Child rule not found.' };
  }
  const child = childRows[0];
  const parentRuleId = child.split_from_rule_id;
  if (!parentRuleId) {
    return { success: false, error: 'This rule was not created by a split.' };
  }

  // Get the child BINs
  const childBins = (child.group_conditions || '').split(',').filter(b => b.trim());

  // Update parent — add BINs back
  const parentRows = querySql(
    `SELECT * FROM ${table} WHERE client_id = ? AND rule_id = ?`,
    [clientId, parentRuleId]
  );
  if (parentRows.length > 0) {
    const parent = parentRows[0];
    const parentBins = (parent.group_conditions || '').split(',').filter(b => b.trim());
    const mergedBins = [...new Set([...parentBins, ...childBins])];

    // If parent was archived, restore it
    const newStatus = parent.status === 'archived' ? 'recommended' : parent.status;
    runSql(`UPDATE ${table} SET group_conditions = ?, status = ?, updated_at = datetime('now') WHERE client_id = ? AND rule_id = ?`,
      [mergedBins.join(','), newStatus, clientId, parentRuleId]);
  }

  // Mark child as merged
  runSql(`UPDATE ${table} SET status = 'merged', updated_at = datetime('now') WHERE client_id = ? AND rule_id = ?`,
    [clientId, childRuleId]);

  // Update rule_splits
  runSql(`UPDATE rule_splits SET is_active = 0, merged_back_at = datetime('now'), merged_back_reason = ?
    WHERE client_id = ? AND child_rule_id = ? AND is_active = 1`,
    [reason || null, clientId, childRuleId]);

  saveDb();
  return { success: true, parentRuleId };
}

// ---------------------------------------------------------------------------
// Convergence detection
// ---------------------------------------------------------------------------

/**
 * Check all active splits for convergence (variance dropped below 3pp).
 * Called after analytics recompute.
 *
 * @param {number} clientId
 * @param {object} opts - { txTypes, days }
 * @returns {Array} Converged split pairs
 */
function detectConvergence(clientId, opts) {
  const activeSplits = querySql(
    `SELECT rs.*, br_parent.rule_name AS parent_name, br_child.rule_name AS child_name,
            br_child.group_conditions AS child_bins, br_parent.group_conditions AS parent_bins
     FROM rule_splits rs
     LEFT JOIN beast_rules br_parent ON rs.parent_rule_id = br_parent.rule_id AND br_parent.client_id = rs.client_id
     LEFT JOIN beast_rules br_child ON rs.child_rule_id = br_child.rule_id AND br_child.client_id = rs.client_id
     WHERE rs.client_id = ? AND rs.is_active = 1`,
    [clientId]
  );

  const converged = [];
  for (const split of activeSplits) {
    const childBins = (split.child_bins || '').split(',').filter(b => b.trim());
    const parentBins = (split.parent_bins || '').split(',').filter(b => b.trim());
    if (childBins.length === 0 || parentBins.length === 0) continue;

    // Compute current rates for child and parent BIN groups
    const txTypes = opts.txTypes || ['cp_initial', 'initial_salvage', 'straight_sale'];
    const days = opts.days || 180;
    const childRate = _computeBinGroupRate(clientId, childBins, txTypes, days);
    const parentRate = _computeBinGroupRate(clientId, parentBins, txTypes, days);

    if (childRate === null || parentRate === null) continue;

    const currentVariance = Math.round(Math.abs(childRate - parentRate) * 100) / 100;
    const originalVariance = split.variance_pp || 0;

    if (currentVariance < 3 && originalVariance >= 5) {
      converged.push({
        splitId: split.id,
        parentRuleId: split.parent_rule_id,
        childRuleId: split.child_rule_id,
        parentName: split.parent_name || split.parent_rule_id,
        childName: split.child_name || split.child_rule_id,
        originalVariance,
        currentVariance,
        splitAt: split.split_at,
        childBinCount: childBins.length,
      });
    }
  }

  return converged;
}

// ---------------------------------------------------------------------------
// Split history for a rule
// ---------------------------------------------------------------------------

/**
 * Get split history events for a rule (as parent or child).
 *
 * @param {number} clientId
 * @param {string} ruleId
 * @returns {Array} History events, newest first, max 10
 */
function getSplitHistory(clientId, ruleId) {
  const rows = querySql(
    `SELECT rs.*,
            br_parent.rule_name AS parent_name,
            br_child.rule_name AS child_name
     FROM rule_splits rs
     LEFT JOIN beast_rules br_parent ON rs.parent_rule_id = br_parent.rule_id AND br_parent.client_id = rs.client_id
     LEFT JOIN beast_rules br_child ON rs.child_rule_id = br_child.rule_id AND br_child.client_id = rs.client_id
     WHERE rs.client_id = ? AND (rs.parent_rule_id = ? OR rs.child_rule_id = ?)
     ORDER BY rs.split_at DESC
     LIMIT 10`,
    [clientId, ruleId, ruleId]
  );

  return rows.map(r => ({
    id: r.id,
    type: r.merged_back_at ? 'merge' : 'split',
    parentRuleId: r.parent_rule_id,
    childRuleId: r.child_rule_id,
    parentName: r.parent_name || r.parent_rule_id,
    childName: r.child_name || r.child_rule_id,
    splitLevel: r.split_level,
    splitReason: r.split_reason,
    variance: r.variance_pp,
    attempts: r.attempts_at_split,
    bins: r.bins_at_split ? JSON.parse(r.bins_at_split) : [],
    confidence: splitConfidence(r.attempts_at_split || 0),
    splitAt: r.split_at,
    mergedAt: r.merged_back_at,
    mergedReason: r.merged_back_reason,
    isActive: r.is_active === 1,
  }));
}

// ---------------------------------------------------------------------------
// New BIN assignment
// ---------------------------------------------------------------------------

/**
 * Assign a new BIN to the most specific matching card.
 *
 * @param {number} clientId
 * @param {string} bin - The new BIN
 * @param {object} binMeta - { issuer_bank, card_brand, card_type, card_level, is_prepaid }
 * @param {string} table - "beast_rules" or "flow_optix_rules"
 * @returns {{ assigned: boolean, ruleId?: string, ruleName?: string }}
 */
function assignNewBin(clientId, bin, binMeta, table) {
  if (!binMeta || !binMeta.issuer_bank) return { assigned: false };

  // Find all active rules for this client that might match
  const rules = querySql(
    `SELECT rule_id, rule_name, group_type, group_conditions, status
     FROM ${table}
     WHERE client_id = ? AND status IN ('recommended', 'active') AND group_type = 'bin'`,
    [clientId]
  );

  // Score each rule by specificity (more specific = better match)
  let bestMatch = null;
  let bestScore = -1;

  for (const rule of rules) {
    const ruleBins = (rule.group_conditions || '').split(',').map(b => b.trim());
    if (ruleBins.includes(bin)) return { assigned: true, ruleId: rule.rule_id, ruleName: rule.rule_name }; // already assigned

    // Check if this rule's BINs share the same bank/brand/type
    // Score: bank=1, brand=2, type=3, level=4
    let score = 0;
    // We'd need to check the rule's metadata context; for now use rule_name heuristics
    const name = (rule.rule_name || '').toLowerCase();
    const bank = (binMeta.issuer_bank || '').toLowerCase();
    if (name.includes(bank.split(' ')[0])) score = 1;
    if (binMeta.card_brand && name.includes(binMeta.card_brand.toLowerCase())) score = 2;
    if (binMeta.card_type && name.includes(binMeta.card_type.toLowerCase())) score = 3;
    if (binMeta.card_level && name.includes(binMeta.card_level.toLowerCase())) score = 4;

    if (score > bestScore) {
      bestScore = score;
      bestMatch = rule;
    }
  }

  if (bestMatch && bestScore > 0) {
    const currentBins = (bestMatch.group_conditions || '').split(',').filter(b => b.trim());
    currentBins.push(bin);
    runSql(`UPDATE ${table} SET group_conditions = ?, updated_at = datetime('now') WHERE client_id = ? AND rule_id = ?`,
      [currentBins.join(','), clientId, bestMatch.rule_id]);
    saveDb();
    return { assigned: true, ruleId: bestMatch.rule_id, ruleName: bestMatch.rule_name };
  }

  return { assigned: false };
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function _getActiveSplitsForRule(clientId, ruleId) {
  return querySql(
    `SELECT * FROM rule_splits WHERE client_id = ? AND parent_rule_id = ? AND is_active = 1`,
    [clientId, ruleId]
  );
}

function _checkBinConflicts(clientId, bins, excludeRuleId, table) {
  if (!bins || bins.length === 0) return null;
  const rules = querySql(
    `SELECT rule_id, rule_name, group_conditions FROM ${table}
     WHERE client_id = ? AND status IN ('recommended', 'active') AND rule_id != ?`,
    [clientId, excludeRuleId]
  );
  for (const rule of rules) {
    const ruleBins = new Set((rule.group_conditions || '').split(',').map(b => b.trim()));
    for (const bin of bins) {
      if (ruleBins.has(bin)) {
        return { bin, ruleId: rule.rule_id, ruleName: rule.rule_name };
      }
    }
  }
  return null;
}

function _computeBinGroupRate(clientId, bins, txTypes, days) {
  if (!bins || bins.length === 0) return null;
  const binPh = bins.map(() => '?').join(',');
  const txPh = txTypes.map(() => '?').join(',');
  const row = querySql(`
    SELECT COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
    FROM orders o
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.acquisition_date >= date('now', '-${parseInt(days, 10)} days')
      AND o.cc_first_6 IN (${binPh})
      AND o.tx_type IN (${txPh})
      AND o.order_status IN (2,6,7,8)
  `, [clientId, ...bins, ...txTypes])[0];
  if (!row || row.total === 0) return null;
  return Math.round((row.approved / row.total) * 10000) / 100;
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  computeSplitSuggestion,
  confirmSplit,
  mergeBack,
  detectConvergence,
  getSplitHistory,
  assignNewBin,
  splitConfidence,
  SPLIT_CONFIDENCE,
};
