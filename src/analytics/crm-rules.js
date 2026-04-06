/**
 * CRM Rules — 5-Level Analysis Engine.
 *
 * Generates structured rules using a 5-level BIN/Bank hierarchy:
 *   Level 5: Single BIN (most specific)
 *   Level 4: BIN group by issuer_bank + card_brand + card_type + card_level
 *   Level 3: BIN group by issuer_bank + card_brand + card_type + is_prepaid
 *   Level 2: BIN group by issuer_bank + card_brand
 *   Level 1: Bank only (broadest)
 *
 * Confidence -> Target Type:
 *   HIGH   (200+):  MID or Acquirer based on traffic share
 *   MEDIUM (100-199): MID or Acquirer based on traffic share
 *   LOW    (70-99):  A/B split 70/30
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql, queryOneSql } = require('../db/connection');
const {
  CLEAN_FILTER, CRM_ROUTING_EXCLUSION, getCachedOrCompute,
  daysAgoFilter, confidenceScore, stddev,
} = require('./engine');
const { analyzeGroup, getConfidence, VARIANCE_THRESHOLDS } = require('./level-engine');
let _splitEngine = null;
function _getSplitEngine() {
  if (!_splitEngine) _splitEngine = require('./split-engine');
  return _splitEngine;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const INITIALS_TYPES = ['cp_initial', 'initial_salvage', 'straight_sale'];
const UPSELL_TYPES = ['upsell', 'upsell_cascade'];
const REBILL_TYPES = ['tp_rebill', 'tp_rebill_salvage', 'sticky_cof_rebill'];

// derived_product_role values (preferred over tx_type for filtering)
const INITIALS_ROLES = ['main_initial', 'straight_sale'];
const UPSELL_ROLES = ['upsell_initial'];
const REBILL_ROLES = ['main_rebill', 'upsell_rebill'];

const MIN_ATTEMPTS = 30;    // minimum total attempts to generate a rule
const MIN_GATEWAYS = 2;     // must have data on 2+ active gateways
const MIN_LIFT_PP = 3;      // minimum lift percentage points
const MIN_GW_ATTEMPTS = 10; // minimum attempts on a single gateway to consider it

// Level label mapping
const LEVEL_LABELS = {
  5: 'BIN Specific',
  4: 'Bank+Brand+Type+Level',
  3: 'Prepaid / Card Type',
  2: 'Bank+Brand',
  1: 'Bank',
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

function computeCrmRules(clientId, opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `5level:${days}`;

  const data = getCachedOrCompute(clientId, 'crm-rules', cacheKey, () => {
    return _compute(clientId, days);
  });

  if (data && data.rules) {
    // 1. Overlay beast_rules status from DB (runs on cache hits too)
    _overlayBeastStatus(clientId, data.rules);

    // 2. Exclude split-out BINs from parent cards + build child cards
    const splitChildren = _processSplitCards(clientId, data.rules, days);
    if (splitChildren.length > 0) {
      data.rules.push(...splitChildren);
    }

    // 3. Attach split suggestions (from cache, not lazy)
    _attachSplitSuggestions(clientId, data.rules, days);
  }

  return data;
}

function _overlayBeastStatus(clientId, rules) {
  const existingRules = querySql(
    'SELECT rule_id, status, stage, attempts_since_active FROM beast_rules WHERE client_id = ?',
    [clientId]
  );
  const existingMap = new Map(existingRules.map(r => [r.rule_id, r]));

  for (const rule of rules) {
    const existing = existingMap.get(rule.ruleId);
    if (existing) {
      rule.status = existing.status;
      rule.stage = existing.stage;
      rule.attempts_since_active = existing.attempts_since_active;
    }
  }
}

function _attachSplitSuggestions(clientId, rules, days) {
  try {
    const { computeSplitSuggestion } = _getSplitEngine();
    const INITIALS_RL = ['main_initial', 'straight_sale'];
    const UPSELLS_RL = ['upsell_initial'];

    for (const rule of rules) {
      if (rule.level >= 5) { rule.splitSuggestion = null; continue; }
      const txTypes = rule.txGroup === 'UPSELLS' ? UPSELLS_RL : INITIALS_RL;
      rule.splitSuggestion = computeSplitSuggestion(clientId, rule, { txTypes, days, useProductRole: true });
    }
  } catch (e) {
    // Split engine failure should never break CRM rules rendering
    console.error('[SplitEngine] Error attaching suggestions:', e.message);
  }
}

function _processSplitCards(clientId, rules, days) {
  // Get all active split children from beast_rules
  const splitRows = querySql(
    `SELECT br.*, rs.parent_rule_id, rs.variance_pp AS split_variance,
            rs.split_at AS split_date, rs.split_level,
            parent.rule_name AS parent_name
     FROM beast_rules br
     JOIN rule_splits rs ON br.rule_id = rs.child_rule_id AND br.client_id = rs.client_id AND rs.is_active = 1
     LEFT JOIN beast_rules parent ON rs.parent_rule_id = parent.rule_id AND parent.client_id = rs.client_id
     WHERE br.client_id = ? AND br.status NOT IN ('merged', 'archived')
       AND br.split_from_rule_id IS NOT NULL`,
    [clientId]
  );

  if (splitRows.length === 0) return [];

  const INITIALS_RL = ['main_initial', 'straight_sale'];
  const UPSELLS_RL = ['upsell_initial'];

  // Build a set of all split-out BINs keyed by parent rule ID
  const splitBinsByParent = new Map(); // parentRuleId → Set of BINs
  const childCards = [];

  for (const row of splitRows) {
    const childBins = (row.group_conditions || '').split(',').filter(b => b.trim());
    if (childBins.length === 0) continue;

    const parentRuleId = row.split_from_rule_id || row.parent_rule_id;

    // Track split BINs for parent exclusion
    if (!splitBinsByParent.has(parentRuleId)) splitBinsByParent.set(parentRuleId, new Set());
    for (const bin of childBins) splitBinsByParent.get(parentRuleId).add(bin);

    // Compute child card data from order data
    const txGroup = row.tx_group || 'INITIALS';
    const roles = txGroup === 'UPSELLS' ? UPSELLS_RL : INITIALS_RL;
    const binPh = childBins.map(() => '?').join(',');
    const rolePh = roles.map(() => '?').join(',');

    const stats = querySql(`
      SELECT COUNT(*) AS total,
        COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
        AVG(CASE WHEN o.order_status IN (2,6,8) THEN o.order_total END) AS avg_order_total
      FROM orders o
      WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
        AND o.acquisition_date >= date('now', '-${parseInt(days, 10)} days')
        AND o.cc_first_6 IN (${binPh})
        AND o.derived_product_role IN (${rolePh})
        AND o.order_status IN (2,6,7,8)
    `, [clientId, ...childBins, ...roles])[0] || { total: 0, approved: 0 };

    const rate = stats.total > 0 ? Math.round((stats.approved / stats.total) * 10000) / 100 : 0;

    // Find best gateway for these BINs
    const gwStats = querySql(`
      SELECT o.gateway_id, g.gateway_alias,
        COUNT(*) AS total,
        COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved
      FROM orders o
      JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
      WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
        AND o.acquisition_date >= date('now', '-${parseInt(days, 10)} days')
        AND o.cc_first_6 IN (${binPh})
        AND o.derived_product_role IN (${rolePh})
        AND o.order_status IN (2,6,7,8)
        AND COALESCE(g.exclude_from_analysis, 0) != 1
      GROUP BY o.gateway_id
      HAVING total >= 10
      ORDER BY (CAST(approved AS REAL) / total) DESC
    `, [clientId, ...childBins, ...roles]);

    // Filter to gateways with active processor name
    const activeGwStats = gwStats.filter(g => {
      const gw = gwMap.get(g.gateway_id);
      return gw && activeProcessorNames.has(gw.processor_name);
    });
    const bestGw = activeGwStats[0] || {};
    const bestGwRate = bestGw.total > 0 ? Math.round((bestGw.approved / bestGw.total) * 10000) / 100 : 0;

    // Build child card object matching parent card shape
    childCards.push({
      ruleId: row.rule_id,
      ruleName: row.rule_name || `Split child ${row.rule_id}`,
      txGroup,
      level: row.split_level || (rules.find(r => r.ruleId === parentRuleId)?.level || 2) + 1,
      levelLabel: '',
      category: 'BIN-level',
      groupType: 'bin',
      groupConditions: row.group_conditions,
      binsInGroup: childBins,
      targetType: 'mid',
      targetValue: bestGw.gateway_alias || '',
      stage: row.stage || 1,
      status: row.status || 'recommended',
      split_from_rule_id: parentRuleId,
      _parentName: row.parent_name || parentRuleId,
      _splitDate: row.split_date,
      _splitVariance: row.split_variance,
      appliesTo: {
        tx_group: txGroup,
        issuer_bank: 'Unknown',
        to_gateway: bestGw.gateway_id ? [bestGw.gateway_id] : [],
      },
      expectedImpact: {
        current_rate: rate,
        expected_rate: bestGwRate,
        lift_pp: Math.round((bestGwRate - rate) * 100) / 100,
        monthly_attempts: Math.round(stats.total * 30 / days),
        monthly_revenue_impact: 0,
      },
      confidence: stats.total >= 200 ? 'HIGH' : stats.total >= 100 ? 'MEDIUM' : 'LOW',
      sampleSize: stats.total,
      midProgress: gwStats.map(g => ({
        gateway_id: g.gateway_id,
        gateway_name: g.gateway_alias || `GW ${g.gateway_id}`,
        total: g.total,
        rate: g.total > 0 ? Math.round((g.approved / g.total) * 10000) / 100 : 0,
      })),
      softDeclines: [],
      hardDeclines: [],
      issuerExceptions: [],
    });
  }

  // Exclude split-out BINs from parent cards
  for (const rule of rules) {
    const splitBins = splitBinsByParent.get(rule.ruleId);
    if (splitBins && rule.binsInGroup) {
      rule.binsInGroup = rule.binsInGroup.filter(b => !splitBins.has(b));
      // Archive parent if no BINs remain
      if (rule.binsInGroup.length === 0) {
        rule.status = 'archived';
      }
    }
  }

  return childCards;
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

function _compute(clientId, days) {
  // Clear per-compute caches
  _attemptExcCache = null;
  _attemptExcCacheKey = null;

  // -----------------------------------------------------------------------
  // 1. Gateway metadata & acquirer map
  // -----------------------------------------------------------------------
  const gwRows = querySql(`
    SELECT gateway_id, gateway_alias, bank_name, processor_name, mcc_code,
           acquiring_bin, lifecycle_state, gateway_active, exclude_from_analysis
    FROM gateways WHERE client_id = ?
  `, [clientId]);

  const gwMap = new Map(gwRows.map(g => [g.gateway_id, g]));
  const acquirerMap = {};
  for (const g of gwRows) {
    if (g.lifecycle_state === 'closed' || g.gateway_active === 0 || g.exclude_from_analysis === 1) continue;
    const proc = g.processor_name || g.bank_name || 'Unknown';
    if (!acquirerMap[proc]) acquirerMap[proc] = [];
    acquirerMap[proc].push(g.gateway_id);
  }

  // Map gateway_id → processor_name (used for acquirer targeting)
  const gwToBank = {};
  for (const [proc, ids] of Object.entries(acquirerMap)) {
    for (const id of ids) gwToBank[id] = proc;
  }

  const activeGwIds = new Set(
    gwRows
      .filter(g => g.lifecycle_state !== 'closed' && g.gateway_active !== 0 && g.exclude_from_analysis !== 1)
      .map(g => g.gateway_id)
  );

  // Active processor names (any gateway with this processor is active)
  const activeProcessorNames = new Set(
    gwRows
      .filter(g => g.gateway_active !== 0 && g.exclude_from_analysis !== 1 && g.processor_name)
      .map(g => g.processor_name)
  );

  // -----------------------------------------------------------------------
  // 2. BIN performance matrix
  // -----------------------------------------------------------------------
  const txGroupSql = `CASE
    WHEN o.derived_product_role IN (${INITIALS_ROLES.map(t => `'${t}'`).join(',')}) THEN 'INITIALS'
    WHEN o.derived_product_role IN (${UPSELL_ROLES.map(t => `'${t}'`).join(',')}) THEN 'UPSELLS'
    WHEN o.derived_product_role IN (${REBILL_ROLES.map(t => `'${t}'`).join(',')}) THEN 'REBILLS'
    ELSE 'OTHER' END`;

  const perfRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      ${txGroupSql} AS tx_group,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      ROUND(AVG(CASE WHEN o.order_total > 0 THEN o.order_total END), 2) AS avg_order_total,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') THEN 1 END) AS last_30d
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IS NOT NULL AND o.order_status IN (2,6,7,8) AND o.gateway_id IS NOT NULL
      AND ${CRM_ROUTING_EXCLUSION} AND o.is_cascaded = 0
    GROUP BY o.cc_first_6, o.gateway_id, tx_group
  `, [clientId]);

  // 2b. Add cascaded orders as declines on original gateway (for INITIALS/UPSELLS)
  const cascadeRows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.original_gateway_id AS gateway_id,
      ${txGroupSql} AS tx_group,
      COUNT(*) AS total
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL
      AND o.cc_first_6 IS NOT NULL AND o.gateway_id IS NOT NULL
      AND ${CRM_ROUTING_EXCLUSION}
    GROUP BY o.cc_first_6, o.original_gateway_id, tx_group
  `, [clientId]);

  // Merge cascade declines into perfRows
  for (const cr of cascadeRows) {
    if (cr.tx_group === 'REBILLS' || cr.tx_group === 'OTHER') continue;
    // Find existing perf entry for this bin+gateway+txgroup
    const existing = perfRows.find(p => p.bin === cr.bin && p.gateway_id === cr.gateway_id && p.tx_group === cr.tx_group);
    if (existing) {
      existing.total += cr.total;
      existing.declined += cr.total; // all cascade origins are declines
    } else {
      perfRows.push({
        bin: cr.bin,
        gateway_id: cr.gateway_id,
        tx_group: cr.tx_group,
        total: cr.total,
        approved: 0,
        declined: cr.total,
        avg_order_total: 0,
        last_30d: 0,
      });
    }
  }

  // -----------------------------------------------------------------------
  // 3. BIN metadata
  // -----------------------------------------------------------------------
  const allBins = [...new Set(perfRows.map(r => r.bin))];
  const binMeta = {};
  if (allBins.length > 0) {
    const metaRows = querySql(`
      SELECT bin, issuer_bank, card_brand, card_type, card_level, is_prepaid
      FROM bin_lookup
      WHERE bin IN (${allBins.map(() => '?').join(',')})
    `, allBins);
    for (const r of metaRows) binMeta[r.bin] = r;
  }

  // -----------------------------------------------------------------------
  // 4. Organize: Map<bin, Map<txGroup, gwPerf[]>>
  // -----------------------------------------------------------------------
  const data = new Map();
  for (const r of perfRows) {
    if (!data.has(r.bin)) data.set(r.bin, new Map());
    const txMap = data.get(r.bin);
    if (!txMap.has(r.tx_group)) txMap.set(r.tx_group, []);
    txMap.get(r.tx_group).push({
      gateway_id: r.gateway_id,
      total: r.total,
      approved: r.approved,
      declined: r.declined,
      rate: r.total > 0 ? (r.approved / r.total) * 100 : 0,
      avg_order_total: r.avg_order_total || 0,
      last_30d: r.last_30d,
    });
  }

  // -----------------------------------------------------------------------
  // 5. Generate rules per tx_group (INITIALS, UPSELLS only)
  // -----------------------------------------------------------------------
  const rules = [];
  let ruleCounter = 1;

  for (const txGroup of ['INITIALS', 'UPSELLS']) {
    const txLabel = txGroup === 'INITIALS' ? 'Initials' : 'Upsells';
    const cycleLabel = txGroup === 'INITIALS' ? 'Initials/Straight Sales' : 'Upsells';
    const handledBins = new Set();

    // =====================================================================
    // LEVEL 5: Single BIN
    // =====================================================================
    for (const [bin, txMap] of data) {
      const gwList = (txMap.get(txGroup) || [])
        .filter(g => activeGwIds.has(g.gateway_id) && g.total >= MIN_GW_ATTEMPTS);
      if (gwList.length < MIN_GATEWAYS) continue;

      const totalAttempts = gwList.reduce((s, g) => s + g.total, 0);
      if (totalAttempts < MIN_ATTEMPTS) continue;

      const best = [...gwList].sort((a, b) => b.rate - a.rate)[0];
      const current = gwList.reduce((a, b) => b.total > a.total ? b : a, gwList[0]);
      if (best.gateway_id === current.gateway_id) continue;

      const liftPp = best.rate - current.rate;
      if (liftPp <= MIN_LIFT_PP) continue;

      const meta = binMeta[bin] || {};
      const rule = _buildRule({
        ruleCounter: ruleCounter++,
        ruleName: `BIN ${bin} - ${txLabel}`,
        txGroup,
        level: 5,
        groupType: 'bin',
        groupConditions: bin,
        binsInGroup: [bin],
        best,
        current,
        gwList,
        totalAttempts,
        liftPp,
        cycleLabel,
        gwMap,
        gwToBank,
        meta,
        clientId,
        days,
      });
      rules.push(rule);
      handledBins.add(bin);
    }

    // =====================================================================
    // LEVELS 4, 3, 2, 1: Group levels
    // =====================================================================
    const levelDefs = [
      {
        level: 4,
        keyFn: (meta) => `${meta.issuer_bank || ''}|${meta.card_brand || ''}|${meta.card_type || ''}|${meta.card_level || ''}`,
        nameFn: (meta) => `${meta.issuer_bank || 'Unknown'} ${meta.card_brand || ''} ${meta.card_type || ''} ${meta.card_level || ''} - ${txLabel}`,
      },
      {
        level: 3,
        keyFn: (meta) => `${meta.issuer_bank || ''}|${meta.card_brand || ''}|${meta.is_prepaid ? 'PREPAID' : (meta.card_type || '')}`,
        nameFn: (meta) => `${meta.issuer_bank || 'Unknown'} ${meta.card_brand || ''} ${meta.is_prepaid ? 'Prepaid' : (meta.card_type || '')} - ${txLabel}`,
      },
      {
        level: 2,
        keyFn: (meta) => `${meta.issuer_bank || ''}|${meta.card_brand || ''}`,
        nameFn: (meta) => `${meta.issuer_bank || 'Unknown'} ${meta.card_brand || ''} - ${txLabel}`,
      },
      {
        level: 1,
        keyFn: (meta) => `${meta.issuer_bank || ''}`,
        nameFn: (meta) => `${meta.issuer_bank || 'Unknown'} - ${txLabel}`,
      },
    ];

    for (const def of levelDefs) {
      // Group remaining BINs
      const groups = new Map(); // key -> { bins: [], meta (first entry) }
      for (const [bin, txMap] of data) {
        if (handledBins.has(bin)) continue;
        if (!txMap.has(txGroup)) continue;

        const meta = binMeta[bin];
        if (!meta) continue;

        const key = def.keyFn(meta);
        if (!groups.has(key)) groups.set(key, { bins: [], meta });
        groups.get(key).bins.push(bin);
      }

      for (const [, group] of groups) {
        // Aggregate gwPerf across all BINs in the group
        const aggGw = new Map(); // gateway_id -> aggregated stats
        for (const bin of group.bins) {
          const txMap = data.get(bin);
          const gwList = txMap.get(txGroup) || [];
          for (const gw of gwList) {
            if (!activeGwIds.has(gw.gateway_id)) continue;
            if (!aggGw.has(gw.gateway_id)) {
              aggGw.set(gw.gateway_id, {
                gateway_id: gw.gateway_id,
                total: 0,
                approved: 0,
                declined: 0,
                avg_order_total_sum: 0,
                avg_order_total_count: 0,
                last_30d: 0,
              });
            }
            const agg = aggGw.get(gw.gateway_id);
            agg.total += gw.total;
            agg.approved += gw.approved;
            agg.declined += gw.declined;
            if (gw.avg_order_total > 0) {
              agg.avg_order_total_sum += gw.avg_order_total * gw.total;
              agg.avg_order_total_count += gw.total;
            }
            agg.last_30d += gw.last_30d;
          }
        }

        // Finalize aggregated gateway stats
        const gwList = [];
        for (const [, agg] of aggGw) {
          if (agg.total < MIN_GW_ATTEMPTS) continue;
          gwList.push({
            gateway_id: agg.gateway_id,
            total: agg.total,
            approved: agg.approved,
            declined: agg.declined,
            rate: agg.total > 0 ? (agg.approved / agg.total) * 100 : 0,
            avg_order_total: agg.avg_order_total_count > 0
              ? Math.round((agg.avg_order_total_sum / agg.avg_order_total_count) * 100) / 100
              : 0,
            last_30d: agg.last_30d,
          });
        }

        if (gwList.length < MIN_GATEWAYS) continue;

        const groupTotal = gwList.reduce((s, g) => s + g.total, 0);
        if (groupTotal < MIN_ATTEMPTS) continue;

        const best = [...gwList].sort((a, b) => b.rate - a.rate)[0];
        const current = gwList.reduce((a, b) => b.total > a.total ? b : a, gwList[0]);
        if (best.gateway_id === current.gateway_id) continue;

        const liftPp = best.rate - current.rate;
        if (liftPp <= MIN_LIFT_PP) continue;

        const groupType = def.level === 1 ? 'bank' : 'bin';
        const groupConditions = def.level === 1
          ? (group.meta.issuer_bank || 'Unknown')
          : group.bins.join(',');

        const rule = _buildRule({
          ruleCounter: ruleCounter++,
          ruleName: def.nameFn(group.meta),
          txGroup,
          level: def.level,
          groupType,
          groupConditions,
          binsInGroup: [...group.bins],
          best,
          current,
          gwList,
          totalAttempts: groupTotal,
          liftPp,
          cycleLabel,
          gwMap,
          gwToBank,
          meta: group.meta,
          clientId,
          days,
        });
        rules.push(rule);

        // Mark all BINs in the group as handled
        for (const bin of group.bins) {
          handledBins.add(bin);
        }
      }
    }
  }

  // -----------------------------------------------------------------------
  // 6. Count rebill opportunities
  // -----------------------------------------------------------------------
  let rebillOpportunities = 0;
  for (const [, txMap] of data) {
    if (txMap.has('REBILLS')) rebillOpportunities++;
  }

  // -----------------------------------------------------------------------
  // SECTION 2: Processor Affinity Rules
  // -----------------------------------------------------------------------

  // 2a. Build processor performance matrix (ALL gateways, no gateway_active filter)
  //     But still exclude gateways with exclude_from_analysis = 1 (paused from analysis)
  const procPerfRows = querySql(`
    SELECT
      o.cc_first_6 AS bin, g.processor_name,
      ${txGroupSql} AS tx_group,
      COUNT(*) AS total,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined,
      ROUND(AVG(CASE WHEN o.order_total > 0 THEN o.order_total END), 2) AS avg_order_total,
      COUNT(CASE WHEN o.acquisition_date >= date('now', '-30 days') THEN 1 END) AS last_30d
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IS NOT NULL AND o.order_status IN (2,6,7,8) AND g.processor_name IS NOT NULL
      AND ${CRM_ROUTING_EXCLUSION}
      AND COALESCE(g.exclude_from_analysis, 0) != 1
    GROUP BY o.cc_first_6, g.processor_name, tx_group
  `, [clientId]);

  // 2b. Organize into Map<bin, Map<txGroup, processorPerf[]>>
  const procData = new Map();
  for (const r of procPerfRows) {
    if (!procData.has(r.bin)) procData.set(r.bin, new Map());
    const txMap = procData.get(r.bin);
    if (!txMap.has(r.tx_group)) txMap.set(r.tx_group, []);
    txMap.get(r.tx_group).push({
      processor_name: r.processor_name,
      total: r.total,
      approved: r.approved,
      declined: r.declined,
      rate: r.total > 0 ? (r.approved / r.total) * 100 : 0,
      avg_order_total: r.avg_order_total || 0,
      last_30d: r.last_30d,
    });
  }

  // 2c. Generate processor affinity rules per tx_group
  const processorRules = [];
  let procRuleCounter = 1;

  for (const txGroup of ['INITIALS', 'UPSELLS']) {
    const txLabel = txGroup === 'INITIALS' ? 'Initials' : 'Upsells';
    const cycleLabel = txGroup === 'INITIALS' ? 'Initials/Straight Sales' : 'Upsells';
    const handledBinsProc = new Set();

    // =====================================================================
    // LEVEL 5: Single BIN — Processor Affinity
    // =====================================================================
    for (const [bin, txMap] of procData) {
      const procList = (txMap.get(txGroup) || [])
        .filter(p => p.total >= MIN_GW_ATTEMPTS);
      if (procList.length < MIN_GATEWAYS) continue;

      const totalAttempts = procList.reduce((s, p) => s + p.total, 0);
      if (totalAttempts < MIN_ATTEMPTS) continue;

      const bestProc = [...procList].sort((a, b) => b.rate - a.rate)[0];
      const currentProc = procList.reduce((a, b) => b.total > a.total ? b : a, procList[0]);
      if (bestProc.processor_name === currentProc.processor_name) continue;

      const liftPp = bestProc.rate - currentProc.rate;
      if (liftPp <= MIN_LIFT_PP) continue;

      const meta = binMeta[bin] || {};
      const rule = _buildProcessorRule({
        ruleCounter: procRuleCounter++,
        ruleName: `BIN ${bin} - ${txLabel} (Processor)`,
        txGroup,
        level: 5,
        groupType: 'bin',
        groupConditions: bin,
        binsInGroup: [bin],
        bestProc,
        currentProc,
        procList,
        totalAttempts,
        liftPp,
        cycleLabel,
        meta,
        gwRows,
        clientId,
        days,
      });
      processorRules.push(rule);
      handledBinsProc.add(bin);
    }

    // =====================================================================
    // LEVELS 4, 3, 2, 1: Group levels — Processor Affinity
    // =====================================================================
    const levelDefs = [
      {
        level: 4,
        keyFn: (meta) => `${meta.issuer_bank || ''}|${meta.card_brand || ''}|${meta.card_type || ''}|${meta.card_level || ''}`,
        nameFn: (meta) => `${meta.issuer_bank || 'Unknown'} ${meta.card_brand || ''} ${meta.card_type || ''} ${meta.card_level || ''} - ${txLabel} (Processor)`,
      },
      {
        level: 3,
        keyFn: (meta) => `${meta.issuer_bank || ''}|${meta.card_brand || ''}|${meta.is_prepaid ? 'PREPAID' : (meta.card_type || '')}`,
        nameFn: (meta) => `${meta.issuer_bank || 'Unknown'} ${meta.card_brand || ''} ${meta.is_prepaid ? 'Prepaid' : (meta.card_type || '')} - ${txLabel} (Processor)`,
      },
      {
        level: 2,
        keyFn: (meta) => `${meta.issuer_bank || ''}|${meta.card_brand || ''}`,
        nameFn: (meta) => `${meta.issuer_bank || 'Unknown'} ${meta.card_brand || ''} - ${txLabel} (Processor)`,
      },
      {
        level: 1,
        keyFn: (meta) => `${meta.issuer_bank || ''}`,
        nameFn: (meta) => `${meta.issuer_bank || 'Unknown'} - ${txLabel} (Processor)`,
      },
    ];

    for (const def of levelDefs) {
      // Group remaining BINs
      const groups = new Map();
      for (const [bin, txMap] of procData) {
        if (handledBinsProc.has(bin)) continue;
        if (!txMap.has(txGroup)) continue;

        const meta = binMeta[bin];
        if (!meta) continue;

        const key = def.keyFn(meta);
        if (!groups.has(key)) groups.set(key, { bins: [], meta });
        groups.get(key).bins.push(bin);
      }

      for (const [, group] of groups) {
        // Aggregate processor perf across all BINs in the group
        const aggProc = new Map(); // processor_name -> aggregated stats
        for (const bin of group.bins) {
          const txMap = procData.get(bin);
          const procList = txMap.get(txGroup) || [];
          for (const p of procList) {
            if (!aggProc.has(p.processor_name)) {
              aggProc.set(p.processor_name, {
                processor_name: p.processor_name,
                total: 0,
                approved: 0,
                declined: 0,
                avg_order_total_sum: 0,
                avg_order_total_count: 0,
                last_30d: 0,
              });
            }
            const agg = aggProc.get(p.processor_name);
            agg.total += p.total;
            agg.approved += p.approved;
            agg.declined += p.declined;
            if (p.avg_order_total > 0) {
              agg.avg_order_total_sum += p.avg_order_total * p.total;
              agg.avg_order_total_count += p.total;
            }
            agg.last_30d += p.last_30d;
          }
        }

        // Finalize aggregated processor stats
        const procList = [];
        for (const [, agg] of aggProc) {
          if (agg.total < MIN_GW_ATTEMPTS) continue;
          procList.push({
            processor_name: agg.processor_name,
            total: agg.total,
            approved: agg.approved,
            declined: agg.declined,
            rate: agg.total > 0 ? (agg.approved / agg.total) * 100 : 0,
            avg_order_total: agg.avg_order_total_count > 0
              ? Math.round((agg.avg_order_total_sum / agg.avg_order_total_count) * 100) / 100
              : 0,
            last_30d: agg.last_30d,
          });
        }

        if (procList.length < MIN_GATEWAYS) continue;

        const groupTotal = procList.reduce((s, p) => s + p.total, 0);
        if (groupTotal < MIN_ATTEMPTS) continue;

        const bestProc = [...procList].sort((a, b) => b.rate - a.rate)[0];
        const currentProc = procList.reduce((a, b) => b.total > a.total ? b : a, procList[0]);
        if (bestProc.processor_name === currentProc.processor_name) continue;

        const liftPp = bestProc.rate - currentProc.rate;
        if (liftPp <= MIN_LIFT_PP) continue;

        const groupType = def.level === 1 ? 'bank' : 'bin';
        const groupConditions = def.level === 1
          ? (group.meta.issuer_bank || 'Unknown')
          : group.bins.join(',');

        const rule = _buildProcessorRule({
          ruleCounter: procRuleCounter++,
          ruleName: def.nameFn(group.meta),
          txGroup,
          level: def.level,
          groupType,
          groupConditions,
          binsInGroup: [...group.bins],
          bestProc,
          currentProc,
          procList,
          totalAttempts: groupTotal,
          liftPp,
          cycleLabel,
          meta: group.meta,
          gwRows,
          clientId,
          days,
        });
        processorRules.push(rule);

        // Mark all BINs in the group as handled
        for (const bin of group.bins) {
          handledBinsProc.add(bin);
        }
      }
    }
  }

  // Sort processor rules by lift DESC
  processorRules.sort((a, b) =>
    (b.expectedImpact?.lift_pp || 0) - (a.expectedImpact?.lift_pp || 0)
  );

  // -----------------------------------------------------------------------
  // 7. Sort rules by monthly_revenue_impact DESC
  // -----------------------------------------------------------------------
  rules.sort((a, b) =>
    (b.expectedImpact?.monthly_revenue_impact || 0) -
    (a.expectedImpact?.monthly_revenue_impact || 0)
  );

  // -----------------------------------------------------------------------
  // 7b. Level analysis — loaded on demand via separate endpoint, not here
  // Keeps crm-rules fast. Frontend fetches /api/analytics/:clientId/level-analysis/:ruleId
  // -----------------------------------------------------------------------
  for (const rule of rules) {
    rule.levelAnalysis = null; // Populated on demand
  }

  // -----------------------------------------------------------------------
  // 8. (beast_rules overlay moved to computeCrmRules — runs on cache hits too)
  // -----------------------------------------------------------------------

  // -----------------------------------------------------------------------
  // 9. Summary
  // -----------------------------------------------------------------------
  const summary = {
    levelAnalysisEnabled: true,
    total: rules.length,
    byTxGroup: {
      INITIALS: rules.filter(r => r.txGroup === 'INITIALS').length,
      UPSELLS: rules.filter(r => r.txGroup === 'UPSELLS').length,
      REBILLS: 0,
    },
    byLevel: {
      5: rules.filter(r => r.level === 5).length,
      4: rules.filter(r => r.level === 4).length,
      3: rules.filter(r => r.level === 3).length,
      2: rules.filter(r => r.level === 2).length,
      1: rules.filter(r => r.level === 1).length,
    },
    byCategory: {
      'BIN-level': rules.filter(r => r.category === 'BIN-level').length,
      'Bank-level': rules.filter(r => r.category === 'Bank-level').length,
    },
    byStage: {
      recommended: rules.filter(r => r.stage === 1).length,
      active_bank: rules.filter(r => r.stage === 2).length,
      promote_now: rules.filter(r => r.stage === 3).length,
      optimized: rules.filter(r => r.stage === 4).length,
    },
    totalMonthlyRevenue: Math.round(
      rules.reduce((s, r) => s + (r.expectedImpact?.monthly_revenue_impact || 0), 0) * 100
    ) / 100,
    totalAnnualRevenue: Math.round(
      rules.reduce((s, r) => s + (r.expectedImpact?.annual_revenue_impact || 0), 0) * 100
    ) / 100,
    rebillOpportunities,
    acquirerMap,
    processorAffinity: {
      total: processorRules.length,
      byTxGroup: {
        INITIALS: processorRules.filter(r => r.txGroup === 'INITIALS').length,
        UPSELLS: processorRules.filter(r => r.txGroup === 'UPSELLS').length,
      },
      byLevel: {
        5: processorRules.filter(r => r.level === 5).length,
        4: processorRules.filter(r => r.level === 4).length,
        3: processorRules.filter(r => r.level === 3).length,
        2: processorRules.filter(r => r.level === 2).length,
        1: processorRules.filter(r => r.level === 1).length,
      },
    },
  };

  return { rules, processorRules, summary };
}

// ---------------------------------------------------------------------------
// _computeDeclineData — shared decline analysis for both rule types
// ---------------------------------------------------------------------------

// Cache attempt exception groups per compute cycle (client-level, not per-rule)
let _attemptExcCache = null;
let _attemptExcCacheKey = null;

function _getAttemptExceptions(clientId, days) {
  const key = `${clientId}:${days}`;
  if (_attemptExcCacheKey === key && _attemptExcCache) return _attemptExcCache;

  const allowRows = querySql(`
    WITH salvage AS (
      SELECT s.customer_id, s.derived_initial_attempt AS attempt_num,
        s.order_status AS salv_status
      FROM orders s
      WHERE s.client_id = ? AND s.tx_type = 'initial_salvage'
        AND s.is_test = 0 AND s.is_internal_test = 0
        AND s.acquisition_date >= date('now', '-${parseInt(days, 10)} days')
    )
    SELECT sv.attempt_num, orig.decline_reason,
      COUNT(*) AS attempts,
      SUM(CASE WHEN sv.salv_status IN (2,6,8) THEN 1 ELSE 0 END) AS recovered,
      ROUND(SUM(CASE WHEN sv.salv_status IN (2,6,8) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS recovery_rate
    FROM salvage sv
    JOIN orders orig ON orig.customer_id = sv.customer_id AND orig.client_id = ?
    WHERE orig.tx_type = 'cp_initial' AND orig.order_status = 7
      AND orig.is_test = 0 AND orig.is_internal_test = 0
      AND orig.decline_reason IS NOT NULL AND orig.decline_reason != ''
      AND orig.decline_reason != 'Prepaid Credit Cards Are Not Accepted'
    GROUP BY sv.attempt_num, orig.decline_reason
    HAVING attempts >= 15
    ORDER BY sv.attempt_num, recovery_rate DESC
  `, [clientId, clientId]);

  // Group by attempt number and action
  const byAttempt = new Map();
  const blockByAttempt = new Map();
  for (const row of allowRows) {
    const att = row.attempt_num || 2;
    if (row.recovery_rate >= 4) {
      if (!byAttempt.has(att)) byAttempt.set(att, []);
      byAttempt.get(att).push(row.decline_reason);
    } else if (row.recovered === 0) {
      if (!blockByAttempt.has(att)) blockByAttempt.set(att, []);
      blockByAttempt.get(att).push(row.decline_reason);
    }
  }

  const groups = [];
  for (const [att, reasons] of [...byAttempt.entries()].sort((a, b) => a[0] - b[0])) {
    groups.push({ attempt: att, reasons, action: 'allow' });
  }
  for (const [att, reasons] of [...blockByAttempt.entries()].sort((a, b) => a[0] - b[0])) {
    groups.push({ attempt: att, reasons, action: 'block' });
  }
  groups.sort((a, b) => a.action === b.action ? a.attempt - b.attempt : a.action === 'allow' ? -1 : 1);

  _attemptExcCache = groups;
  _attemptExcCacheKey = key;
  return groups;
}

function _computeDeclineData(clientId, days, binsInGroup, txGroup) {
  let softDeclines = [];
  let hardDeclines = [];
  let issuerExceptions = [];

  if (!binsInGroup.length || !clientId || !days) {
    return { softDeclines, hardDeclines, issuerExceptions };
  }

  const binPlaceholders = binsInGroup.map(() => '?').join(',');
  const roles = txGroup === 'INITIALS'
    ? INITIALS_ROLES : txGroup === 'UPSELLS' ? UPSELL_ROLES : REBILL_ROLES;
  const rolePlaceholders = roles.map(() => '?').join(',');

  // --- Soft/Hard decline categorization ---
  const declineRows = querySql(`
    SELECT o.decline_reason, o.decline_category,
      COUNT(*) AS cnt
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.cc_first_6 IN (${binPlaceholders})
      AND o.derived_product_role IN (${rolePlaceholders})
      AND o.order_status = 7 AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
    GROUP BY o.decline_reason, o.decline_category
    ORDER BY cnt DESC
  `, [clientId, ...binsInGroup, ...roles]);

  for (const dr of declineRows) {
    const cat = (dr.decline_category || '').toLowerCase();
    const entry = { reason: dr.decline_reason, count: dr.cnt };
    if (cat === 'hard' || cat.includes('fraud') || cat.includes('stolen') || cat.includes('lost')) {
      hardDeclines.push(entry);
    } else {
      softDeclines.push(entry);
    }
  }
  softDeclines = softDeclines.slice(0, 6);
  hardDeclines = hardDeclines.slice(0, 6);

  // --- Attempt exceptions: cached at client level, shared across all rules ---
  let attemptGroups = [];
  if (txGroup === 'INITIALS') {
    attemptGroups = _getAttemptExceptions(clientId, days);
  }

  return { softDeclines, hardDeclines, issuerExceptions, attemptGroups };
}

// ---------------------------------------------------------------------------
// _buildRule — constructs a complete rule object
// ---------------------------------------------------------------------------

function _buildRule({
  ruleCounter, ruleName, txGroup, level, groupType, groupConditions,
  binsInGroup, best, current, gwList, totalAttempts, liftPp,
  cycleLabel, gwMap, gwToBank, meta, clientId, days,
}) {
  const levelLabel = LEVEL_LABELS[level];
  const category = level === 1 ? 'Bank-level' : 'BIN-level';

  // --- Target type logic ---
  const bestGwAttempts = best.total;
  const totalGroupAttempts = gwList.reduce((s, g) => s + g.total, 0);
  const bestGwPct = totalGroupAttempts > 0 ? bestGwAttempts / totalGroupAttempts : 0;

  let targetType, targetValue, weightageConfig, confidenceNote;
  const bestGwMeta = gwMap.get(best.gateway_id);
  const bestAlias = bestGwMeta?.gateway_alias || '';
  const bestBank = gwToBank[best.gateway_id] || 'Unknown';
  const currentBank = gwToBank[current.gateway_id] || 'Unknown';

  if (bestGwPct >= 0.30) {
    targetType = 'mid';
    targetValue = `GW ${best.gateway_id} (${bestAlias})`;
    weightageConfig = { [best.gateway_id]: 100 };
    confidenceNote = 'MID-level — sufficient gateway data';
  } else {
    targetType = 'acquirer';
    targetValue = gwToBank[best.gateway_id] || 'Unknown';
    weightageConfig = { [targetValue]: 100 };
    confidenceNote = 'Bank-level — building gateway data';
  }

  // LOW confidence override: A/B split
  if (totalGroupAttempts < 100) {
    targetType = 'acquirer';
    targetValue = `${bestBank} (70%) / ${currentBank} (30%)`;
    weightageConfig = { [bestBank]: 70, [currentBank]: 30 };
    confidenceNote = 'A/B split — gathering more data';
  }

  // --- Confidence level ---
  let confidence;
  if (totalAttempts >= 200) {
    confidence = 'HIGH';
  } else if (totalAttempts >= 100) {
    confidence = 'MEDIUM';
  } else {
    confidence = 'LOW';
  }

  const confScore = confidenceScore(totalAttempts, 0, 50);

  // --- Revenue impact ---
  const avgOrderTotal = current.avg_order_total || best.avg_order_total || 0;
  const monthlyAttempts = Math.round(current.total * 30 / 90);
  const monthlyRevenue = Math.round(monthlyAttempts * (liftPp / 100) * avgOrderTotal * 100) / 100;

  // --- MID progress ---
  const sortedGws = [...gwList].sort((a, b) => b.rate - a.rate);
  const midProgress = sortedGws.map(g => {
    const gw = gwMap.get(g.gateway_id);
    return {
      gateway_id: g.gateway_id,
      gateway_name: gw?.gateway_alias || `GW ${g.gateway_id}`,
      bank_name: gwToBank[g.gateway_id] || 'Unknown',
      total: g.total,
      rate: Math.round(g.rate * 100) / 100,
      progress_pct: Math.min(100, Math.round((g.total / 200) * 100)),
      gateway_active: gw?.gateway_active ?? 1,
    };
  });

  // --- Determine from/to gateways ---
  const fromGateways = [current.gateway_id];
  const toGateways = [best.gateway_id];

  // --- Gateway insight banner state ---
  const currentGwMeta = gwMap.get(current.gateway_id);
  const bestActiveGw = midProgress.find(m => m.gateway_active === 1);
  const bestInactiveGw = midProgress.find(m => m.gateway_active === 0 && m.rate > (bestActiveGw?.rate || 0));
  const currentIsOptimal = bestActiveGw && bestActiveGw.gateway_id === current.gateway_id;
  const liftFromInactive = bestInactiveGw ? Math.round((bestInactiveGw.rate - (bestActiveGw?.rate || 0)) * 100) / 100 : 0;

  // Check for recently activated gateways (created within last 30 days)
  let newlyActivatedGw = null;
  for (const m of midProgress) {
    if (m.gateway_active !== 1) continue;
    const gw = gwMap.get(m.gateway_id);
    if (gw?.gateway_created) {
      const created = new Date(gw.gateway_created);
      const daysAgo = (Date.now() - created.getTime()) / (1000 * 60 * 60 * 24);
      if (daysAgo <= 30 && m.rate > (current.rate + 5)) {
        newlyActivatedGw = { name: m.gateway_name, rate: m.rate, lift: Math.round((m.rate - current.rate) * 100) / 100 };
        break;
      }
    }
  }

  let gatewayInsight;
  if (newlyActivatedGw) {
    gatewayInsight = { state: 'new_gateway', name: newlyActivatedGw.name, rate: newlyActivatedGw.rate, lift: newlyActivatedGw.lift };
  } else if (bestInactiveGw && liftFromInactive >= 5) {
    gatewayInsight = { state: 'inactive_better', name: bestInactiveGw.gateway_name, rate: bestInactiveGw.rate, lift: liftFromInactive };
  } else if (currentIsOptimal) {
    gatewayInsight = { state: 'optimal' };
  } else {
    gatewayInsight = { state: 'optimal' }; // default to optimal if best active is the target
  }

  // --- Beast config ---
  const beastGroupType = level === 1 ? 'Bank' : 'BIN';
  const beastTargetType = targetType === 'mid' ? 'MID' : 'Acquirer';

  // --- Decline reasons for these BINs ---
  const { softDeclines, hardDeclines, issuerExceptions, attemptGroups } =
    _computeDeclineData(clientId, days, binsInGroup, txGroup);

  return {
    ruleId: `BR-${String(ruleCounter).padStart(3, '0')}`,
    ruleName,
    txGroup,
    level,
    levelLabel,
    category,
    groupType,
    groupConditions,
    binsInGroup,
    targetType,
    targetValue,
    weightageConfig,
    cycleLabel,
    confidenceNote,
    stage: 1,
    status: 'recommended',
    appliesTo: {
      tx_group: txGroup,
      issuer_bank: meta.issuer_bank || 'Unknown',
      card_brand: meta.card_brand || null,
      card_type: meta.card_type || null,
      card_level: meta.card_level || null,
      from_gateway: fromGateways,
      to_gateway: toGateways,
    },
    expectedImpact: {
      current_rate: Math.round(current.rate * 100) / 100,
      expected_rate: Math.round(best.rate * 100) / 100,
      lift_pp: Math.round(liftPp * 100) / 100,
      monthly_attempts: monthlyAttempts,
      monthly_revenue_impact: monthlyRevenue,
      annual_revenue_impact: Math.round(monthlyRevenue * 12 * 100) / 100,
    },
    confidence,
    confidenceScore: confScore.score,
    sampleSize: totalAttempts,
    midProgress,
    gatewayInsight,
    softDeclines,
    hardDeclines,
    issuerExceptions,
    attemptGroups,
    beastConfig: {
      ruleName,
      cycle: cycleLabel,
      groupType: beastGroupType,
      groupConditions,
      targetType: beastTargetType,
      target: targetValue,
      weightage: weightageConfig,
    },
  };
}

// ---------------------------------------------------------------------------
// _buildProcessorRule — constructs a processor affinity rule object
// ---------------------------------------------------------------------------

function _buildProcessorRule({
  ruleCounter, ruleName, txGroup, level, groupType, groupConditions,
  binsInGroup, bestProc, currentProc, procList, totalAttempts, liftPp,
  cycleLabel, meta, gwRows, clientId, days,
}) {
  const levelLabel = LEVEL_LABELS[level];
  const category = level === 1 ? 'Bank-level' : 'BIN-level';
  const padded = String(ruleCounter).padStart(3, '0');

  // --- Confidence level ---
  let confidence;
  if (totalAttempts >= 200) {
    confidence = 'HIGH';
  } else if (totalAttempts >= 100) {
    confidence = 'MEDIUM';
  } else {
    confidence = 'LOW';
  }

  // --- Revenue impact ---
  const avgOrderTotal = currentProc.avg_order_total || bestProc.avg_order_total || 0;
  const monthlyAttempts = Math.round(currentProc.total * 30 / 90);
  const monthlyRevenue = Math.round(monthlyAttempts * (liftPp / 100) * avgOrderTotal * 100) / 100;

  // --- Insight ---
  let insight;
  const bestRateStr = Math.round(bestProc.rate * 10) / 10;
  const currentRateStr = Math.round(currentProc.rate * 10) / 10;
  if (level === 5) {
    const bin = binsInGroup[0];
    insight = `BIN ${bin} approves at ${bestRateStr}% on ${bestProc.processor_name} vs ${currentRateStr}% on ${currentProc.processor_name}`;
  } else {
    const bankName = meta.issuer_bank || 'Unknown';
    insight = `${bankName} cards approve at ${bestRateStr}% on ${bestProc.processor_name} vs ${currentRateStr}% on ${currentProc.processor_name}`;
  }

  // --- Recommendation ---
  const groupDescription = level === 5
    ? `BIN ${binsInGroup[0]}`
    : (groupType === 'bank' ? (meta.issuer_bank || 'Unknown') : `these BINs`);
  const recommendation = `When adding new ${bestProc.processor_name} MIDs → Route ${groupDescription} there immediately`;

  // --- Active gateways for best processor ---
  const activeGateways = gwRows
    .filter(g => g.processor_name === bestProc.processor_name && g.gateway_active === 1 && g.exclude_from_analysis !== 1)
    .map(g => ({
      gateway_id: g.gateway_id,
      gateway_alias: g.gateway_alias,
      bank_name: g.bank_name,
    }));

  // --- MID progress (processor performance) ---
  const sortedProcs = [...procList].sort((a, b) => b.rate - a.rate);
  const midProgress = sortedProcs.map(p => ({
    processor_name: p.processor_name,
    total: p.total,
    approved: p.approved,
    rate: Math.round(p.rate * 100) / 100,
    avg_order_total: p.avg_order_total || 0,
  }));

  // --- Decline reasons for these BINs ---
  const { softDeclines, hardDeclines, issuerExceptions, attemptGroups } =
    _computeDeclineData(clientId, days, binsInGroup, txGroup);

  return {
    ruleId: `PA-${padded}`,
    section: 'processor_affinity',
    ruleName,
    txGroup,
    level,
    levelLabel,
    category,
    groupType,
    groupConditions,
    binsInGroup,
    bestProcessor: {
      name: bestProc.processor_name,
      total: bestProc.total,
      approved: bestProc.approved,
      rate: Math.round(bestProc.rate * 100) / 100,
    },
    currentProcessor: {
      name: currentProc.processor_name,
      total: currentProc.total,
      approved: currentProc.approved,
      rate: Math.round(currentProc.rate * 100) / 100,
    },
    insight,
    recommendation,
    activeGateways,
    expectedImpact: {
      current_rate: Math.round(currentProc.rate * 100) / 100,
      expected_rate: Math.round(bestProc.rate * 100) / 100,
      lift_pp: Math.round(liftPp * 100) / 100,
      monthly_attempts: monthlyAttempts,
      monthly_revenue_impact: monthlyRevenue,
      annual_revenue_impact: Math.round(monthlyRevenue * 12 * 100) / 100,
    },
    confidence,
    sampleSize: totalAttempts,
    midProgress,
    softDeclines,
    hardDeclines,
    issuerExceptions,
    attemptGroups,
  };
}

module.exports = { computeCrmRules };
