const express = require('express');
const router = express.Router();

const { computeBinProfiles } = require('../analytics/bin-profiles');
const { computeBinClusters } = require('../analytics/bin-clusters');
const { computeGatewayProfiles } = require('../analytics/gateway-profiles');
const { computeDeclineMatrix } = require('../analytics/decline-matrix');
const { computeTxTypeAnalysis } = require('../analytics/txtype-analysis');
const { computeRoutingRecommendations } = require('../analytics/routing-recommendations');
const { computeLiftOpportunities } = require('../analytics/lift-opportunities');
const { computeConfidenceLayer } = require('../analytics/confidence-layer');
const { computeTrendDetection } = require('../analytics/trend-detection');
const { computeCrmRules } = require('../analytics/crm-rules');
const { computePricePoints, computeSalvageSequence } = require('../analytics/price-points');
const { computeFlowOptix } = require('../analytics/flow-optix');
const { computeFlowOptixV2, computeFlowOptixV2Initials } = require('../analytics/flow-optix-v2');
const { computeRoutingPlaybook } = require('../analytics/routing-playbook');
const { getDataQuality } = require('./analytics-helpers');
const { getCacheInfo, recomputeAllAnalytics } = require('../analytics/engine');

function parseOpts(query) {
  return {
    txType: query.tx_type || undefined,
    minSample: query.min_sample ? Number(query.min_sample) : undefined,
    days: query.days ? Number(query.days) : undefined,
    gatewayId: query.gateway_id || undefined
  };
}

const analyses = [
  { name: 'bin-profiles',              fn: computeBinProfiles },
  { name: 'bin-clusters',              fn: computeBinClusters },
  { name: 'gateway-profiles',          fn: computeGatewayProfiles },
  { name: 'decline-matrix',            fn: computeDeclineMatrix },
  { name: 'txtype-analysis',           fn: computeTxTypeAnalysis },
  { name: 'routing-recommendations',   fn: computeRoutingRecommendations },
  { name: 'lift-opportunities',        fn: computeLiftOpportunities },
  { name: 'confidence-layer',          fn: computeConfidenceLayer },
  { name: 'trend-detection',           fn: computeTrendDetection },
  { name: 'crm-rules',                fn: computeCrmRules },
  { name: 'price-points',             fn: computePricePoints },
  { name: 'salvage-sequence',         fn: computeSalvageSequence },
  { name: 'flow-optix',              fn: computeFlowOptix },
  { name: 'flow-optix-v2',          fn: computeFlowOptixV2 },
  { name: 'flow-optix-v2-initials', fn: computeFlowOptixV2Initials },
  { name: 'routing-playbook',      fn: computeRoutingPlaybook }
];

// Individual analytics endpoints — serve from cache only, never compute on GET
for (const { name, fn } of analyses) {
  router.get(`/:clientId/${name}`, async (req, res) => {
    try {
      const { clientId } = req.params;
      const opts = parseOpts(req.query);
      const result = await fn(clientId, opts);
      if (result == null) {
        return res.json({ status: 'not_computed', data: null });
      }
      res.json(result);
    } catch (err) {
      console.error(`Error computing ${name}:`, err);
      res.status(500).json({ error: err.message });
    }
  });
}

// Data quality endpoint
router.get('/:clientId/data-quality', async (req, res) => {
  try {
    const { clientId } = req.params;
    const result = await getDataQuality(clientId);
    res.json(result);
  } catch (err) {
    console.error('Error computing data-quality:', err);
    res.status(500).json({ error: err.message });
  }
});

// Run all analyses
router.post('/:clientId/run-all', async (req, res) => {
  try {
    const { clientId } = req.params;
    const opts = parseOpts(req.query);
    const results = {};

    for (const { name, fn } of analyses) {
      try {
        results[name] = { status: 'ok', data: await fn(clientId, opts) };
      } catch (err) {
        results[name] = { status: 'error', error: err.message };
      }
    }

    const succeeded = Object.values(results).filter(r => r.status === 'ok').length;
    const failed = Object.values(results).filter(r => r.status === 'error').length;

    res.json({ summary: { total: analyses.length, succeeded, failed }, results });
  } catch (err) {
    console.error('Error running all analyses:', err);
    res.status(500).json({ error: err.message });
  }
});

// Cache info — timestamps for all cached analyses
router.get('/:clientId/cache-info', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    res.json(getCacheInfo(clientId));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Trigger background recompute (manual)
router.post('/:clientId/recompute', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  // Check classification completeness — warn but don't block
  const { queryOneSql } = require('../db/connection');
  const unclassified = queryOneSql(
    `SELECT COUNT(*) as cnt FROM orders
     WHERE client_id = ? AND is_test = 0 AND is_internal_test = 0
       AND order_status IN (2, 6, 7, 8)
       AND derived_product_role IS NULL`,
    [clientId]
  )?.cnt || 0;

  if (unclassified > 0 && !req.body.force) {
    return res.status(409).json({
      error: 'classification_incomplete',
      unclassified,
      message: `${unclassified} orders have no classification. Run the post-sync pipeline first, or pass { "force": true } to override.`,
    });
  }

  res.json({ status: 'started', warning: unclassified > 0 ? `${unclassified} unclassified orders — results may be incomplete` : null });
  recomputeAllAnalytics(clientId).catch(err =>
    console.error('[Recompute] Failed:', err.message)
  );
});

// Level Analysis — on-demand per rule
const { analyzeGroup } = require('../analytics/level-engine');

router.get('/:clientId/level-analysis', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    const { issuer_bank, card_brand, card_type, is_prepaid, card_level, tx_group } = req.query;
    const txTypes = tx_group === 'INITIALS'
      ? ['cp_initial', 'initial_salvage', 'straight_sale']
      : tx_group === 'UPSELLS'
        ? ['upsell', 'upsell_cascade']
        : ['tp_rebill', 'tp_rebill_salvage'];

    const opts = { txTypes, days: 180, issuerBank: issuer_bank };
    if (card_brand) opts.cardBrand = card_brand;
    if (card_type) { opts.cardType = card_type; opts.isPrepaid = parseInt(is_prepaid) || 0; }
    if (card_level) opts.cardLevel = card_level;

    const result = analyzeGroup(clientId, opts);
    res.json(result);
  } catch (err) {
    console.error('Level analysis error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Split Engine
const {
  computeSplitSuggestion, confirmSplit, mergeBack,
  detectConvergence, getSplitHistory,
} = require('../analytics/split-engine');

// New routing engine modules
const { computeProcessorIntelligence, getProcessorIntelligence } = require('../analytics/processor-intelligence');
const { checkCapAlerts, getCapStatus } = require('../analytics/cap-tracking');
const { getActiveAlerts: getDegradationAlerts, runDegradationCheck } = require('../analytics/degradation');

// Processor intelligence endpoint
router.get('/:clientId/processor-intelligence', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    const stored = getProcessorIntelligence(clientId);
    const capAlerts = checkCapAlerts(clientId);
    const capStatus = getCapStatus(clientId);
    const degradation = getDegradationAlerts(clientId);
    res.json({ acquisitionPriority: stored, capAlerts, capStatus, degradation });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Split suggestion for a rule
router.get('/:clientId/split-suggestion/:ruleId', (req, res) => {
  try {
    const { clientId, ruleId } = req.params;
    const txGroup = req.query.tx_group || 'INITIALS';
    const txTypes = txGroup === 'INITIALS'
      ? ['cp_initial', 'initial_salvage', 'straight_sale']
      : txGroup === 'UPSELLS'
        ? ['upsell', 'upsell_cascade']
        : ['tp_rebill', 'tp_rebill_salvage', 'sticky_cof_rebill'];

    // Get the rule from cached CRM rules
    const { computeCrmRules } = require('../analytics/crm-rules');
    const crmData = computeCrmRules(parseInt(clientId, 10), {});
    const allRules = [...(crmData?.rules || []), ...(crmData?.processorRules || [])];
    const rule = allRules.find(r => r.ruleId === ruleId);
    if (!rule) return res.json({ suggestion: null });

    const suggestion = computeSplitSuggestion(parseInt(clientId, 10), rule, { txTypes, days: 180 });
    res.json({ suggestion });
  } catch (err) {
    console.error('Split suggestion error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Confirm a split
router.post('/:clientId/split/confirm', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    const { ruleId, ruleType, splitData } = req.body;
    const result = confirmSplit(clientId, ruleId, ruleType || 'beast', splitData);
    res.json(result);
  } catch (err) {
    console.error('Split confirm error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Merge back a split child
router.post('/:clientId/split/merge-back', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    const { childRuleId, ruleType, reason } = req.body;
    const result = mergeBack(clientId, childRuleId, ruleType || 'beast', reason);
    res.json(result);
  } catch (err) {
    console.error('Merge back error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Convergence detection
router.get('/:clientId/split/convergence', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    const txTypes = ['cp_initial', 'initial_salvage', 'straight_sale'];
    const converged = detectConvergence(clientId, { txTypes, days: 180 });
    res.json({ converged });
  } catch (err) {
    console.error('Convergence error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Split history for a rule
router.get('/:clientId/split/history/:ruleId', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    const history = getSplitHistory(clientId, req.params.ruleId);
    res.json({ history });
  } catch (err) {
    console.error('Split history error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Beast Rules — Mark Active / Dismiss
const { runSql, querySql: beastQuerySql, saveDb } = require('../db/connection');

router.post('/:clientId/beast-rules/:ruleId/activate', (req, res) => {
  const { clientId, ruleId } = req.params;
  const rule = req.body;
  try {
    // Remove any existing rows for this rule to prevent duplicates (PK is autoincrement id)
    runSql('DELETE FROM beast_rules WHERE client_id = ? AND rule_id = ?', [clientId, ruleId]);
    runSql(`INSERT INTO beast_rules
      (client_id, rule_id, rule_name, tx_group, group_type, group_conditions,
       target_type, target_value, weightage_config, stage, status,
       baseline_rate, predicted_rate, predicted_lift_pp, monthly_revenue_impact,
       implemented_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, datetime('now'), datetime('now'))`,
      [clientId, ruleId, rule.ruleName || '', rule.txGroup || '', rule.groupType || '',
       rule.groupConditions || '', rule.targetType || '', rule.targetValue || '',
       JSON.stringify(rule.weightageConfig || {}),
       rule.targetType === 'mid' ? 4 : 2,
       rule.expectedImpact?.current_rate || 0, rule.expectedImpact?.expected_rate || 0,
       rule.expectedImpact?.lift_pp || 0, rule.expectedImpact?.monthly_revenue_impact || 0]);
    saveDb();
    res.json({ success: true, status: 'active' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/:clientId/beast-rules/:ruleId/dismiss', (req, res) => {
  const { clientId, ruleId } = req.params;
  try {
    // Remove any existing rows for this rule to prevent duplicates (PK is autoincrement id)
    runSql('DELETE FROM beast_rules WHERE client_id = ? AND rule_id = ?', [clientId, ruleId]);
    runSql(`INSERT INTO beast_rules (client_id, rule_id, status, stage, updated_at)
      VALUES (?, ?, 'dismissed', 0, datetime('now'))`,
      [clientId, ruleId]);
    saveDb();
    res.json({ success: true, status: 'dismissed' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/:clientId/beast-rules/:ruleId/restore', (req, res) => {
  const { clientId, ruleId } = req.params;
  try {
    // Remove duplicates then insert clean row
    runSql('DELETE FROM beast_rules WHERE client_id = ? AND rule_id = ?', [clientId, ruleId]);
    runSql(`INSERT INTO beast_rules (client_id, rule_id, status, stage, updated_at)
      VALUES (?, ?, 'recommended', 1, datetime('now'))`,
      [clientId, ruleId]);
    saveDb();
    res.json({ success: true, status: 'recommended' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/:clientId/beast-rules/restore-all', (req, res) => {
  const { clientId } = req.params;
  try {
    // Get dismissed rule_ids, clean up duplicates, re-insert as recommended
    const dismissed = beastQuerySql(
      'SELECT DISTINCT rule_id FROM beast_rules WHERE client_id = ? AND status = ?',
      [clientId, 'dismissed']
    );
    for (const d of dismissed) {
      runSql('DELETE FROM beast_rules WHERE client_id = ? AND rule_id = ?', [clientId, d.rule_id]);
      runSql(`INSERT INTO beast_rules (client_id, rule_id, status, stage, updated_at)
        VALUES (?, ?, 'recommended', 1, datetime('now'))`, [clientId, d.rule_id]);
    }
    saveDb();
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
