/**
 * Playbook Implementation Tracker.
 *
 * Tracks routing rules implemented from the Playbook at the bank+card group level.
 * Each rule type (initial_routing, cascade, rebill_routing, salvage) is tracked
 * independently with its own baseline, evaluation logic, and verdict thresholds.
 *
 * State machine:
 *   waiting → collecting → evaluating → confirmed | inconclusive | regression
 *                                        regression → rolled_back
 *                        any active → superseded | archived
 *
 * Rebill tracking uses cohort-based evaluation: only customers acquired AFTER
 * the implementation date are measured for their first rebill performance.
 */

const { querySql, runSql, saveDb, transaction } = require('../db/connection');
const { CLEAN_FILTER, CRM_ROUTING_EXCLUSION, CASCADE_WHERE } = require('../analytics/engine');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MIN_SAMPLE = {
  initial_routing: 50,
  upsell_routing: 50,
  cascade: 30,
  rebill_routing: 20,
  salvage: 30,
};

const VERDICT = {
  initial_routing:  { confirm_pp: 2.0,  regress_pp: -3.0, timeout_days: 30 },
  upsell_routing:   { confirm_pp: 2.0,  regress_pp: -3.0, timeout_days: 30 },
  cascade:          { confirm_pp: 5.0,  regress_pp: -5.0, timeout_days: 30 },
  rebill_routing:   { confirm_pp: 2.0,  regress_pp: -3.0, timeout_days: 60 },
  salvage:          { confirm_pp: 2.0,  regress_pp: -3.0, timeout_days: 30 },
  // Salvage also uses RPA comparison: +$0.50 = confirm, -$0.50 = regress
};

const SETTLING_DAYS = 1;   // Minimum days before collecting starts
const ACTIVE_STATUSES = ['waiting', 'collecting', 'evaluating'];

// ---------------------------------------------------------------------------
// BIN filter helper — builds WHERE clause for bank group matching
// ---------------------------------------------------------------------------

function _bankGroupFilter(impl, tableAlias = 'b') {
  const clauses = [
    `${tableAlias}.issuer_bank = '${impl.issuer_bank.replace(/'/g, "''")}'`,
    `${tableAlias}.is_prepaid = ${impl.is_prepaid}`,
  ];
  if (impl.card_brand) {
    clauses.push(`${tableAlias}.card_brand = '${impl.card_brand.replace(/'/g, "''")}'`);
  }
  if (impl.card_type) {
    clauses.push(`${tableAlias}.card_type = '${impl.card_type.replace(/'/g, "''")}'`);
  }
  return clauses.join(' AND ');
}

/**
 * Build a BIN exclusion subquery for bank-level impls that have active L4 sub-rules.
 * This prevents double-counting when a bank-level impl coexists with L4 impls.
 */
function _l4ExclusionClause(impl) {
  if (impl.rule_level === 'l4') return ''; // L4 rules don't need exclusion

  const activeL4 = querySql(`
    SELECT card_brand, card_type FROM playbook_implementations
    WHERE client_id = ? AND issuer_bank = ? AND is_prepaid = ?
      AND rule_type = ? AND rule_level = 'l4'
      AND status IN ('waiting','collecting','evaluating')
      AND id != ?
  `, [impl.client_id, impl.issuer_bank, impl.is_prepaid, impl.rule_type, impl.id]);

  if (activeL4.length === 0) return '';

  const conditions = activeL4.map(r => {
    const parts = [];
    if (r.card_brand) parts.push(`b.card_brand = '${r.card_brand.replace(/'/g, "''")}'`);
    if (r.card_type) parts.push(`b.card_type = '${r.card_type.replace(/'/g, "''")}'`);
    return `(${parts.join(' AND ')})`;
  });

  return ` AND NOT (${conditions.join(' OR ')})`;
}

// ---------------------------------------------------------------------------
// Mark as implemented
// ---------------------------------------------------------------------------

/**
 * Mark a playbook routing rule as implemented.
 *
 * @param {number} clientId
 * @param {object} params
 * @param {string} params.issuer_bank
 * @param {number} params.is_prepaid - 0 or 1
 * @param {string} [params.card_brand] - null for bank-level
 * @param {string} [params.card_type] - null for bank-level
 * @param {string} params.rule_type
 * @param {string} [params.recommended_processor]
 * @param {string} [params.recommended_gateway_ids] - JSON array string
 * @param {string} params.actual_processor
 * @param {string} params.actual_gateway_ids - JSON array string
 * @param {object} [params.split_config] - { new_pct, old_pct, old_processor, old_gateway_ids }
 * @param {object} [params.playbook_snapshot] - the full playbook card data for this section
 * @returns {object} created implementation with baseline
 */
function markPlaybookImplemented(clientId, params) {
  const {
    issuer_bank, is_prepaid,
    card_brand, card_type,
    rule_type,
    recommended_processor, recommended_gateway_ids,
    actual_processor, actual_gateway_ids,
    split_config, playbook_snapshot,
  } = params;

  if (!issuer_bank || rule_type == null || !actual_processor) {
    throw new Error('Missing required fields: issuer_bank, rule_type, actual_processor');
  }

  if (!MIN_SAMPLE[rule_type]) {
    throw new Error(`Invalid rule_type: ${rule_type}`);
  }

  const ruleLevel = (card_brand || card_type) ? 'l4' : 'bank';

  // 1. Supersede any existing active implementation on same group+type
  const existing = querySql(`
    SELECT id FROM playbook_implementations
    WHERE client_id = ? AND issuer_bank = ? AND is_prepaid = ?
      AND rule_type = ?
      AND (card_brand IS ? OR (card_brand IS NULL AND ? IS NULL))
      AND (card_type IS ? OR (card_type IS NULL AND ? IS NULL))
      AND status IN ('waiting','collecting','evaluating')
  `, [clientId, issuer_bank, is_prepaid, rule_type,
      card_brand, card_brand, card_type, card_type]);

  // 2. Capture baseline
  const baseline = _captureBaseline(clientId, {
    issuer_bank, is_prepaid, card_brand, card_type, rule_type,
  });

  // 3. Find rollback target (the top processor from baseline)
  const rollbackInfo = _findRollbackTarget(clientId, {
    issuer_bank, is_prepaid, card_brand, card_type, rule_type,
  });

  let newId;
  transaction(() => {
    // Supersede existing
    for (const ex of existing) {
      runSql(`
        UPDATE playbook_implementations
        SET status = 'superseded', updated_at = datetime('now')
        WHERE id = ?
      `, [ex.id]);
    }

    // Insert new
    runSql(`
      INSERT INTO playbook_implementations (
        client_id, issuer_bank, is_prepaid, card_brand, card_type, rule_level,
        rule_type, recommended_processor, recommended_gateway_ids, recommended_detail_json,
        actual_processor, actual_gateway_ids, split_config_json,
        baseline_json, baseline_period_start, baseline_period_end,
        status, collecting_start_date, min_sample_target,
        rollback_to_processor, rollback_to_gateway_ids,
        implemented_at, created_at, updated_at
      ) VALUES (
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, date('now', '-30 days'), date('now'),
        'waiting', date('now'), ?,
        ?, ?,
        datetime('now'), datetime('now'), datetime('now')
      )
    `, [
      clientId, issuer_bank, is_prepaid, card_brand || null, card_type || null, ruleLevel,
      rule_type, recommended_processor || null, recommended_gateway_ids || null,
      playbook_snapshot ? JSON.stringify(playbook_snapshot) : null,
      actual_processor, actual_gateway_ids || null,
      split_config ? JSON.stringify(split_config) : null,
      JSON.stringify(baseline),
      MIN_SAMPLE[rule_type],
      rollbackInfo.processor || null, rollbackInfo.gateway_ids || null,
    ]);

    // Update superseded_by_id on old impls
    const last = querySql('SELECT last_insert_rowid() as id');
    newId = last[0].id;

    for (const ex of existing) {
      runSql('UPDATE playbook_implementations SET superseded_by_id = ? WHERE id = ?', [newId, ex.id]);
    }
  });

  saveDb();

  return {
    id: newId,
    status: 'waiting',
    baseline,
    superseded: existing.map(e => e.id),
  };
}

// ---------------------------------------------------------------------------
// Baseline capture
// ---------------------------------------------------------------------------

function _captureBaseline(clientId, params) {
  const { issuer_bank, is_prepaid, card_brand, card_type, rule_type } = params;

  const bankFilter = _bankGroupFilter({
    issuer_bank, is_prepaid, card_brand, card_type,
  });

  switch (rule_type) {
    case 'initial_routing':
      return _baselineInitial(clientId, bankFilter);
    case 'upsell_routing':
      return _baselineUpsell(clientId, bankFilter);
    case 'cascade':
      return _baselineCascade(clientId, bankFilter);
    case 'rebill_routing':
      return _baselineRebill(clientId, bankFilter);
    case 'salvage':
      return _baselineSalvage(clientId, bankFilter);
    default:
      return { attempts: 0, approvals: 0, approval_rate: 0, period_days: 30 };
  }
}

function _baselineInitial(clientId, bankFilter) {
  const row = querySql(`
    SELECT
      COUNT(*) as attempts,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as approvals,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) as approval_rate,
      ROUND(AVG(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN o.order_total END), 2) as avg_rpa
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role = 'main_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND ${CLEAN_FILTER} AND ${CRM_ROUTING_EXCLUSION}
      AND o.order_status IN (2,6,7,8)
      AND o.acquisition_date >= date('now', '-30 days')
      AND ${bankFilter}
  `, [clientId])[0];

  return {
    attempts: row.attempts || 0,
    approvals: row.approvals || 0,
    approval_rate: row.approval_rate || 0,
    avg_rpa: row.avg_rpa || 0,
    period_days: 30,
    type: 'initial_routing',
  };
}

function _baselineUpsell(clientId, bankFilter) {
  const row = querySql(`
    SELECT
      COUNT(*) as attempts,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as approvals,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) as approval_rate,
      ROUND(AVG(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN o.order_total END), 2) as avg_rpa
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role = 'upsell_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND ${CLEAN_FILTER} AND ${CRM_ROUTING_EXCLUSION}
      AND o.order_status IN (2,6,7,8)
      AND o.acquisition_date >= date('now', '-30 days')
      AND ${bankFilter}
  `, [clientId])[0];

  return {
    attempts: row.attempts || 0,
    approvals: row.approvals || 0,
    approval_rate: row.approval_rate || 0,
    avg_rpa: row.avg_rpa || 0,
    period_days: 30,
    type: 'upsell_routing',
  };
}

function _baselineCascade(clientId, bankFilter) {
  const row = querySql(`
    SELECT
      COUNT(*) as cascade_attempts,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as cascade_recovered,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) as recovery_rate
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${CASCADE_WHERE}
      AND ${CLEAN_FILTER}
      AND o.order_status IN (2,6,7,8)
      AND o.acquisition_date >= date('now', '-30 days')
      AND ${bankFilter}
  `, [clientId])[0];

  return {
    attempts: row.cascade_attempts || 0,
    approvals: row.cascade_recovered || 0,
    approval_rate: row.recovery_rate || 0,
    period_days: 30,
    type: 'cascade',
  };
}

function _baselineRebill(clientId, bankFilter) {
  const row = querySql(`
    SELECT
      COUNT(*) as attempts,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as approvals,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) as approval_rate,
      ROUND(AVG(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN o.order_total END), 2) as avg_rpa
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.derived_cycle = 1 AND o.derived_attempt = 1
      AND ${CLEAN_FILTER} AND ${CRM_ROUTING_EXCLUSION}
      AND o.order_status IN (2,6,7,8)
      AND o.acquisition_date >= date('now', '-30 days')
      AND ${bankFilter}
  `, [clientId])[0];

  return {
    attempts: row.attempts || 0,
    approvals: row.approvals || 0,
    approval_rate: row.approval_rate || 0,
    avg_rpa: row.avg_rpa || 0,
    period_days: 30,
    type: 'rebill_routing',
  };
}

function _baselineSalvage(clientId, bankFilter) {
  const row = querySql(`
    SELECT
      COUNT(*) as attempts,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approvals,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) as approval_rate,
      ROUND(AVG(CASE WHEN o.order_status IN (2,6,8) THEN o.order_total END), 2) as avg_rpa
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.derived_attempt >= 2
      AND ${CLEAN_FILTER}
      AND o.order_status IN (2,6,7,8)
      AND o.acquisition_date >= date('now', '-30 days')
      AND ${bankFilter}
  `, [clientId])[0];

  return {
    attempts: row.attempts || 0,
    approvals: row.approvals || 0,
    approval_rate: row.approval_rate || 0,
    avg_rpa: row.avg_rpa || 0,
    period_days: 30,
    type: 'salvage',
  };
}

// ---------------------------------------------------------------------------
// Rollback target — identify what to revert to
// ---------------------------------------------------------------------------

function _findRollbackTarget(clientId, params) {
  const { issuer_bank, is_prepaid, card_brand, card_type, rule_type } = params;
  const bankFilter = _bankGroupFilter({ issuer_bank, is_prepaid, card_brand, card_type });

  // Find the most-used processor for this bank group in the last 30 days
  let roleFilter;
  switch (rule_type) {
    case 'initial_routing': roleFilter = "o.derived_product_role = 'main_initial'"; break;
    case 'upsell_routing':  roleFilter = "o.derived_product_role = 'upsell_initial'"; break;
    case 'cascade':         roleFilter = `${CASCADE_WHERE}`; break;
    case 'rebill_routing':  roleFilter = "o.derived_product_role IN ('main_rebill','upsell_rebill') AND o.derived_cycle = 1"; break;
    case 'salvage':         roleFilter = "o.derived_product_role IN ('main_rebill','upsell_rebill') AND o.derived_attempt >= 2"; break;
    default:                roleFilter = '1=1';
  }

  const topProc = querySql(`
    SELECT g.processor_name, g.gateway_id, COUNT(*) as cnt
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${roleFilter}
      AND ${CLEAN_FILTER}
      AND o.acquisition_date >= date('now', '-30 days')
      AND ${bankFilter}
    GROUP BY g.processor_name
    ORDER BY cnt DESC
    LIMIT 1
  `, [clientId])[0];

  if (!topProc) return { processor: null, gateway_ids: null };

  // Get all gateway_ids for that processor
  const gws = querySql(`
    SELECT DISTINCT gateway_id FROM gateways
    WHERE client_id = ? AND processor_name = ? AND lifecycle_state != 'closed'
  `, [clientId, topProc.processor_name]);

  return {
    processor: topProc.processor_name,
    gateway_ids: JSON.stringify(gws.map(g => g.gateway_id)),
  };
}

// ---------------------------------------------------------------------------
// Evaluate all active implementations
// ---------------------------------------------------------------------------

/**
 * Run evaluation on all active playbook implementations.
 * Called by scheduler every 6 hours + after daily sync.
 */
function evaluatePlaybookImplementations() {
  const active = querySql(`
    SELECT * FROM playbook_implementations
    WHERE status IN ('waiting', 'collecting', 'evaluating')
  `);

  if (active.length === 0) return { evaluated: 0, transitioned: 0 };

  let evaluated = 0;
  let transitioned = 0;

  for (const impl of active) {
    const daysSince = _daysSince(impl.implemented_at);
    const result = _evaluateOne(impl, daysSince);

    if (result.newStatus && result.newStatus !== impl.status) {
      transitioned++;
    }
    if (result.checkpointRecorded) {
      evaluated++;
    }
  }

  if (evaluated > 0 || transitioned > 0) saveDb();
  return { evaluated, transitioned };
}

function _daysSince(dateStr) {
  const d = new Date(dateStr);
  const now = new Date();
  return Math.floor((now - d) / (1000 * 60 * 60 * 24));
}

function _evaluateOne(impl, daysSince) {
  const result = { newStatus: null, checkpointRecorded: false };

  // State: waiting → collecting (after settling period)
  if (impl.status === 'waiting' && daysSince >= SETTLING_DAYS) {
    runSql(
      "UPDATE playbook_implementations SET status = 'collecting', updated_at = datetime('now') WHERE id = ?",
      [impl.id]
    );
    result.newStatus = 'collecting';
    impl.status = 'collecting'; // Continue evaluation in same pass
  }

  // Only proceed if collecting or evaluating
  if (impl.status !== 'collecting' && impl.status !== 'evaluating') {
    return result;
  }

  // Get post-implementation data
  const postData = _getPostData(impl);

  // Check if we should record a checkpoint (at minimum every 7 days, or if data changed)
  const lastCheckpoint = querySql(`
    SELECT * FROM implementation_checkpoints
    WHERE implementation_id = ?
    ORDER BY checkpoint_day DESC LIMIT 1
  `, [impl.id])[0];

  const shouldCheckpoint = !lastCheckpoint
    || daysSince - lastCheckpoint.checkpoint_day >= 7
    || postData.attempts !== (lastCheckpoint.post_attempts || 0);

  if (!shouldCheckpoint) return result;

  // Record checkpoint
  const baseline = JSON.parse(impl.baseline_json || '{}');
  const baselineRate = baseline.approval_rate || 0;
  const liftPp = Math.round(((postData.rate || 0) - baselineRate) * 100) / 100;
  const meetsSample = postData.attempts >= impl.min_sample_target ? 1 : 0;

  // Detect confounding factors
  const confounding = _checkConfoundingFactors(impl);

  const checkpointStatus = meetsSample ? 'evaluating' : 'collecting';

  runSql(`
    INSERT INTO implementation_checkpoints (
      implementation_id, checkpoint_day, checked_at,
      post_attempts, post_approvals, post_approval_rate, post_avg_rpa,
      new_side_attempts, new_side_approvals, new_side_rate,
      old_side_attempts, old_side_approvals, old_side_rate,
      baseline_rate, lift_pp,
      cohort_customers_acquired, cohort_first_rebills_attempted, cohort_first_rebills_approved,
      meets_minimum_sample, confounding_factors_json, status_at_checkpoint
    ) VALUES (?, ?, datetime('now'),
      ?, ?, ?, ?,
      ?, ?, ?,
      ?, ?, ?,
      ?, ?,
      ?, ?, ?,
      ?, ?, ?)
  `, [
    impl.id, daysSince,
    postData.attempts, postData.approvals, postData.rate, postData.avg_rpa || null,
    postData.new_side?.attempts || null, postData.new_side?.approvals || null, postData.new_side?.rate || null,
    postData.old_side?.attempts || null, postData.old_side?.approvals || null, postData.old_side?.rate || null,
    baselineRate, liftPp,
    postData.cohort?.customers_acquired || null,
    postData.cohort?.rebills_attempted || null,
    postData.cohort?.rebills_approved || null,
    meetsSample,
    confounding.length > 0 ? JSON.stringify(confounding) : null,
    checkpointStatus,
  ]);

  result.checkpointRecorded = true;

  // Update latest checkpoint on the impl
  runSql(`
    UPDATE playbook_implementations
    SET latest_checkpoint_json = ?, updated_at = datetime('now')
    WHERE id = ?
  `, [JSON.stringify({
    day: daysSince,
    attempts: postData.attempts,
    approvals: postData.approvals,
    rate: postData.rate,
    lift_pp: liftPp,
    meets_sample: meetsSample,
    new_side: postData.new_side || null,
    old_side: postData.old_side || null,
    cohort: postData.cohort || null,
  }), impl.id]);

  // State: collecting → evaluating (when sample met)
  if (impl.status === 'collecting' && meetsSample) {
    runSql(
      "UPDATE playbook_implementations SET status = 'evaluating', updated_at = datetime('now') WHERE id = ?",
      [impl.id]
    );
    result.newStatus = 'evaluating';
    impl.status = 'evaluating';
  }

  // State: evaluating → verdict
  if (impl.status === 'evaluating' && meetsSample) {
    const verdictResult = _computeVerdict(impl, postData, liftPp, daysSince, confounding);
    if (verdictResult) {
      _applyVerdict(impl, verdictResult, postData);
      result.newStatus = verdictResult.status;
    }
  }

  // Timeout check: force inconclusive after max days
  const thresholds = VERDICT[impl.rule_type] || VERDICT.initial_routing;
  if (daysSince >= thresholds.timeout_days && ACTIVE_STATUSES.includes(impl.status)) {
    if (!meetsSample) {
      _applyVerdict(impl, {
        status: 'inconclusive',
        reason: `Insufficient volume after ${daysSince} days (${postData.attempts}/${impl.min_sample_target} attempts)`,
      }, postData);
      result.newStatus = 'inconclusive';
    }
  }

  return result;
}

// ---------------------------------------------------------------------------
// Post-implementation data queries
// ---------------------------------------------------------------------------

function _getPostData(impl) {
  const splitConfig = impl.split_config_json ? JSON.parse(impl.split_config_json) : null;

  switch (impl.rule_type) {
    case 'initial_routing':
      return _postDataInitial(impl, splitConfig);
    case 'upsell_routing':
      return _postDataUpsell(impl, splitConfig);
    case 'cascade':
      return _postDataCascade(impl);
    case 'rebill_routing':
      return _postDataRebill(impl, splitConfig);
    case 'salvage':
      return _postDataSalvage(impl, splitConfig);
    default:
      return { attempts: 0, approvals: 0, rate: 0 };
  }
}

function _postDataInitial(impl, splitConfig) {
  const bankFilter = _bankGroupFilter(impl);
  const l4Excl = _l4ExclusionClause(impl);

  // All initials for this bank group since collecting date
  const all = querySql(`
    SELECT
      g.processor_name,
      COUNT(*) as attempts,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as approvals,
      ROUND(AVG(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN o.order_total END), 2) as avg_rpa
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role = 'main_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND ${CLEAN_FILTER} AND ${CRM_ROUTING_EXCLUSION}
      AND o.order_status IN (2,6,7,8)
      AND o.acquisition_date >= ?
      AND ${bankFilter} ${l4Excl}
    GROUP BY g.processor_name
  `, [impl.client_id, impl.collecting_start_date]);

  return _aggregateWithSplit(all, impl.actual_processor, splitConfig);
}

function _postDataUpsell(impl, splitConfig) {
  const bankFilter = _bankGroupFilter(impl);
  const l4Excl = _l4ExclusionClause(impl);

  const all = querySql(`
    SELECT
      g.processor_name,
      COUNT(*) as attempts,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as approvals,
      ROUND(AVG(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN o.order_total END), 2) as avg_rpa
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role = 'upsell_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND ${CLEAN_FILTER} AND ${CRM_ROUTING_EXCLUSION}
      AND o.order_status IN (2,6,7,8)
      AND o.acquisition_date >= ?
      AND ${bankFilter} ${l4Excl}
    GROUP BY g.processor_name
  `, [impl.client_id, impl.collecting_start_date]);

  return _aggregateWithSplit(all, impl.actual_processor, splitConfig);
}

function _postDataCascade(impl) {
  const bankFilter = _bankGroupFilter(impl);
  const l4Excl = _l4ExclusionClause(impl);

  const row = querySql(`
    SELECT
      COUNT(*) as attempts,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approvals,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) as rate
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${CASCADE_WHERE}
      AND ${CLEAN_FILTER}
      AND o.order_status IN (2,6,7,8)
      AND o.acquisition_date >= ?
      AND ${bankFilter} ${l4Excl}
  `, [impl.client_id, impl.collecting_start_date])[0];

  return {
    attempts: row.attempts || 0,
    approvals: row.approvals || 0,
    rate: row.rate || 0,
  };
}

function _postDataRebill(impl, splitConfig) {
  const bankFilter = _bankGroupFilter(impl);
  const l4Excl = _l4ExclusionClause(impl);

  // Cohort-based: customers acquired AFTER implementation date
  const cohort = querySql(`
    WITH post_impl_cohort AS (
      SELECT DISTINCT o.customer_id
      FROM orders o
      JOIN bin_lookup b ON o.cc_first_6 = b.bin
      WHERE o.client_id = ? AND o.derived_product_role = 'main_initial'
        AND o.order_status IN (2,6,8) AND o.is_cascaded = 0
        AND ${CLEAN_FILTER}
        AND o.acquisition_date >= ?
        AND ${bankFilter} ${l4Excl}
    )
    SELECT
      (SELECT COUNT(*) FROM post_impl_cohort) as customers_acquired,
      COUNT(*) as c1_attempts,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as c1_approvals,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) as c1_rate,
      ROUND(AVG(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN o.order_total END), 2) as avg_rpa
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ?
      AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.derived_cycle = 1 AND o.derived_attempt = 1
      AND ${CLEAN_FILTER} AND ${CRM_ROUTING_EXCLUSION}
      AND o.order_status IN (2,6,7,8)
      AND o.customer_id IN (SELECT customer_id FROM post_impl_cohort)
      AND ${bankFilter} ${l4Excl}
  `, [impl.client_id, impl.implemented_at, impl.client_id])[0];

  return {
    attempts: cohort.c1_attempts || 0,
    approvals: cohort.c1_approvals || 0,
    rate: cohort.c1_rate || 0,
    avg_rpa: cohort.avg_rpa || 0,
    cohort: {
      customers_acquired: cohort.customers_acquired || 0,
      rebills_attempted: cohort.c1_attempts || 0,
      rebills_approved: cohort.c1_approvals || 0,
    },
  };
}

function _postDataSalvage(impl, splitConfig) {
  const bankFilter = _bankGroupFilter(impl);
  const l4Excl = _l4ExclusionClause(impl);

  const all = querySql(`
    SELECT
      g.processor_name,
      COUNT(*) as attempts,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approvals,
      ROUND(AVG(CASE WHEN o.order_status IN (2,6,8) THEN o.order_total END), 2) as avg_rpa
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.derived_attempt >= 2
      AND ${CLEAN_FILTER}
      AND o.order_status IN (2,6,7,8)
      AND o.acquisition_date >= ?
      AND ${bankFilter} ${l4Excl}
    GROUP BY g.processor_name
  `, [impl.client_id, impl.collecting_start_date]);

  return _aggregateWithSplit(all, impl.actual_processor, splitConfig);
}

// ---------------------------------------------------------------------------
// Split aggregation helper
// ---------------------------------------------------------------------------

function _aggregateWithSplit(procRows, actualProcessor, splitConfig) {
  let totalAtt = 0, totalApp = 0, totalRpaSum = 0, totalRpaCount = 0;
  let newAtt = 0, newApp = 0;
  let oldAtt = 0, oldApp = 0;

  const normalActual = (actualProcessor || '').toUpperCase();
  const normalOld = splitConfig ? (splitConfig.old_processor || '').toUpperCase() : null;

  for (const r of procRows) {
    totalAtt += r.attempts;
    totalApp += r.approvals;
    if (r.avg_rpa && r.approvals > 0) {
      totalRpaSum += r.avg_rpa * r.approvals;
      totalRpaCount += r.approvals;
    }

    const norm = (r.processor_name || '').toUpperCase();
    if (norm === normalActual) {
      newAtt += r.attempts;
      newApp += r.approvals;
    } else if (normalOld && norm === normalOld) {
      oldAtt += r.attempts;
      oldApp += r.approvals;
    }
  }

  const rate = totalAtt > 0 ? Math.round((totalApp / totalAtt) * 10000) / 100 : 0;
  const newRate = newAtt > 0 ? Math.round((newApp / newAtt) * 10000) / 100 : 0;
  const oldRate = oldAtt > 0 ? Math.round((oldApp / oldAtt) * 10000) / 100 : 0;
  const avgRpa = totalRpaCount > 0 ? Math.round((totalRpaSum / totalRpaCount) * 100) / 100 : 0;

  const result = {
    attempts: newAtt > 0 ? newAtt : totalAtt,
    approvals: newAtt > 0 ? newApp : totalApp,
    rate: newAtt > 0 ? newRate : rate,
    avg_rpa: avgRpa,
  };

  // Include split data if configured
  if (splitConfig) {
    result.new_side = { attempts: newAtt, approvals: newApp, rate: newRate };
    result.old_side = { attempts: oldAtt, approvals: oldApp, rate: oldRate };
  }

  return result;
}

// ---------------------------------------------------------------------------
// Verdict computation
// ---------------------------------------------------------------------------

function _computeVerdict(impl, postData, liftPp, daysSince, confounding) {
  const thresholds = VERDICT[impl.rule_type] || VERDICT.initial_routing;

  // For split tracking: if both sides regress equally, it's likely external
  if (impl.split_config_json && postData.new_side && postData.old_side) {
    const baseline = JSON.parse(impl.baseline_json || '{}');
    const baselineRate = baseline.approval_rate || 0;
    const newLift = (postData.new_side.rate || 0) - baselineRate;
    const oldLift = (postData.old_side.rate || 0) - baselineRate;

    // Both sides dropped similarly → external factor, not routing
    if (newLift <= thresholds.regress_pp && oldLift <= thresholds.regress_pp) {
      if (Math.abs(newLift - oldLift) < 2) {
        return {
          status: 'inconclusive',
          reason: `Both new (${postData.new_side.rate}%) and old (${postData.old_side.rate}%) sides dropped from baseline (${baselineRate}%). Likely external factor.`,
        };
      }
    }

    // Use new side lift for verdict
    if (newLift >= thresholds.confirm_pp && daysSince >= 7) {
      return {
        status: 'confirmed',
        reason: `New side (${postData.new_side.rate}%) lifted +${newLift.toFixed(1)}pp vs baseline (${baselineRate}%). Old side: ${postData.old_side.rate}%.`,
      };
    }
    if (newLift <= thresholds.regress_pp && daysSince >= 7) {
      return {
        status: 'regression',
        reason: `New side (${postData.new_side.rate}%) dropped ${newLift.toFixed(1)}pp vs baseline (${baselineRate}%). Old side: ${postData.old_side.rate}%.`,
      };
    }
  } else {
    // Non-split: simple comparison
    if (liftPp >= thresholds.confirm_pp && daysSince >= 7) {
      return {
        status: 'confirmed',
        reason: `Approval rate improved from ${(JSON.parse(impl.baseline_json || '{}')).approval_rate}% to ${postData.rate}% (+${liftPp.toFixed(1)}pp) after ${postData.attempts} attempts.`,
      };
    }
    if (liftPp <= thresholds.regress_pp && daysSince >= 7) {
      // Check confounding before declaring regression
      if (confounding.length > 0) {
        return {
          status: 'inconclusive',
          reason: `Rate dropped ${liftPp.toFixed(1)}pp but confounding factors detected: ${confounding.map(c => c.description).join('; ')}`,
        };
      }
      return {
        status: 'regression',
        reason: `Approval rate dropped from ${(JSON.parse(impl.baseline_json || '{}')).approval_rate}% to ${postData.rate}% (${liftPp.toFixed(1)}pp) after ${postData.attempts} attempts.`,
      };
    }
  }

  // Timeout → inconclusive
  if (daysSince >= thresholds.timeout_days) {
    return {
      status: 'inconclusive',
      reason: `No significant change after ${daysSince} days. Lift: ${liftPp >= 0 ? '+' : ''}${liftPp.toFixed(1)}pp (${postData.attempts} attempts).`,
    };
  }

  return null; // Not ready for verdict yet
}

function _applyVerdict(impl, verdictResult, postData) {
  transaction(() => {
    runSql(`
      UPDATE playbook_implementations
      SET status = ?, verdict_at = datetime('now'), verdict_reason = ?,
          latest_checkpoint_json = ?, updated_at = datetime('now')
      WHERE id = ?
    `, [
      verdictResult.status,
      verdictResult.reason,
      JSON.stringify({
        attempts: postData.attempts,
        approvals: postData.approvals,
        rate: postData.rate,
        new_side: postData.new_side || null,
        old_side: postData.old_side || null,
        cohort: postData.cohort || null,
      }),
      impl.id,
    ]);

    // Network feedback
    _recordNetworkFeedback(impl, verdictResult, postData);
  });
}

// ---------------------------------------------------------------------------
// Confounding factor detection
// ---------------------------------------------------------------------------

function _checkConfoundingFactors(impl) {
  const factors = [];

  // Check degradation alerts for this bank group in the tracking period
  const alerts = querySql(`
    SELECT * FROM degradation_alerts
    WHERE client_id = ? AND issuer_bank = ?
      AND resolved_at IS NULL
      AND created_at >= ?
  `, [impl.client_id, impl.issuer_bank, impl.implemented_at]);

  for (const a of alerts) {
    factors.push({
      type: 'degradation_alert',
      description: `${a.alert_type}: ${a.issuer_bank} dropped ${a.drop_pp}pp on gateway ${a.gateway_id}`,
      alert_id: a.id,
    });
  }

  return factors;
}

// ---------------------------------------------------------------------------
// Network feedback
// ---------------------------------------------------------------------------

function _recordNetworkFeedback(impl, verdictResult, postData) {
  // Lazy-load to avoid circular dependency
  let normalizeBank, normalizeProcessor;
  try {
    const na = require('../analytics/network-analysis');
    normalizeBank = na.normalizeBank;
    normalizeProcessor = na.normalizeProcessor;
  } catch {
    // Fallback if network-analysis not available
    normalizeBank = (n) => n || 'Unknown';
    normalizeProcessor = (n) => (n || 'UNKNOWN').toUpperCase();
  }

  const baseline = JSON.parse(impl.baseline_json || '{}');
  const liftPp = (postData.rate || 0) - (baseline.approval_rate || 0);

  runSql(`
    INSERT INTO implementation_network_feedback (
      implementation_id, client_id,
      normalized_bank, normalized_processor, rule_type,
      outcome, lift_pp, post_attempts
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `, [
    impl.id, impl.client_id,
    normalizeBank(impl.issuer_bank),
    normalizeProcessor(impl.actual_processor),
    impl.rule_type,
    verdictResult.status,
    Math.round(liftPp * 100) / 100,
    postData.attempts,
  ]);
}

// ---------------------------------------------------------------------------
// Rollback
// ---------------------------------------------------------------------------

function rollbackImplementation(implId) {
  const impl = querySql('SELECT * FROM playbook_implementations WHERE id = ?', [implId])[0];
  if (!impl) throw new Error(`Implementation ${implId} not found`);
  if (impl.status !== 'regression') throw new Error(`Can only rollback implementations with regression status (current: ${impl.status})`);

  runSql(`
    UPDATE playbook_implementations
    SET status = 'rolled_back', rolled_back_at = datetime('now'), updated_at = datetime('now')
    WHERE id = ?
  `, [implId]);
  saveDb();

  return {
    rolled_back: true,
    rollback_to_processor: impl.rollback_to_processor,
    rollback_to_gateway_ids: impl.rollback_to_gateway_ids,
  };
}

// ---------------------------------------------------------------------------
// Archive
// ---------------------------------------------------------------------------

function archiveImplementation(implId) {
  runSql(`
    UPDATE playbook_implementations
    SET status = 'archived', updated_at = datetime('now')
    WHERE id = ?
  `, [implId]);
  saveDb();
  return { archived: true };
}

// ---------------------------------------------------------------------------
// Dashboard / query helpers
// ---------------------------------------------------------------------------

function getImplementationDashboard(clientId) {
  const impls = querySql(`
    SELECT pi.*,
      g.processor_name as actual_processor_name,
      g.gateway_alias as actual_gateway_alias
    FROM playbook_implementations pi
    LEFT JOIN gateways g ON g.gateway_id = CAST(
      REPLACE(REPLACE(COALESCE(pi.actual_gateway_ids, '[]'), '[', ''), ']', '') AS INTEGER
    ) AND g.client_id = pi.client_id
    WHERE pi.client_id = ?
    ORDER BY
      CASE pi.status
        WHEN 'regression' THEN 0
        WHEN 'evaluating' THEN 1
        WHEN 'collecting' THEN 2
        WHEN 'waiting' THEN 3
        WHEN 'confirmed' THEN 4
        ELSE 5
      END,
      pi.implemented_at DESC
  `, [clientId]);

  // Compute summary
  const summary = {
    total: impls.length,
    waiting: 0, collecting: 0, evaluating: 0,
    confirmed: 0, regression: 0, inconclusive: 0,
    rolled_back: 0, superseded: 0, archived: 0,
    total_confirmed_lift_pp: 0,
    est_monthly_revenue_impact: 0,
  };

  for (const impl of impls) {
    summary[impl.status] = (summary[impl.status] || 0) + 1;

    if (impl.status === 'confirmed' && impl.latest_checkpoint_json) {
      const cp = JSON.parse(impl.latest_checkpoint_json);
      summary.total_confirmed_lift_pp += cp.lift_pp || 0;
      // Rough revenue estimate: lift * attempts * avg_rpa / 100
      const baseline = JSON.parse(impl.baseline_json || '{}');
      const monthlyAtt = (baseline.attempts || 0); // baseline is 30d
      const avgRpa = baseline.avg_rpa || cp.avg_rpa || 0;
      summary.est_monthly_revenue_impact += Math.round(monthlyAtt * (cp.lift_pp || 0) / 100 * avgRpa);
    }
  }

  summary.total_confirmed_lift_pp = Math.round(summary.total_confirmed_lift_pp * 100) / 100;

  // Enrich each impl with computed fields
  const enriched = impls.map(impl => {
    const baseline = JSON.parse(impl.baseline_json || '{}');
    const checkpoint = impl.latest_checkpoint_json ? JSON.parse(impl.latest_checkpoint_json) : null;
    const daysSince = _daysSince(impl.implemented_at);

    return {
      id: impl.id,
      client_id: impl.client_id,
      issuer_bank: impl.issuer_bank,
      is_prepaid: impl.is_prepaid,
      card_brand: impl.card_brand,
      card_type: impl.card_type,
      rule_level: impl.rule_level,
      rule_type: impl.rule_type,
      status: impl.status,
      actual_processor: impl.actual_processor,
      recommended_processor: impl.recommended_processor,
      has_split: !!impl.split_config_json,
      split_config: impl.split_config_json ? JSON.parse(impl.split_config_json) : null,
      implemented_at: impl.implemented_at,
      days_since: daysSince,
      baseline_rate: baseline.approval_rate || 0,
      baseline_attempts: baseline.attempts || 0,
      current_rate: checkpoint?.rate || null,
      current_attempts: checkpoint?.attempts || 0,
      lift_pp: checkpoint?.lift_pp || 0,
      sample_progress: `${checkpoint?.attempts || 0}/${impl.min_sample_target}`,
      meets_sample: (checkpoint?.attempts || 0) >= impl.min_sample_target,
      verdict_reason: impl.verdict_reason,
      verdict_at: impl.verdict_at,
      rollback_to_processor: impl.rollback_to_processor,
      new_side: checkpoint?.new_side || null,
      old_side: checkpoint?.old_side || null,
      cohort: checkpoint?.cohort || null,
    };
  });

  return { summary, implementations: enriched };
}

function getImplementationDetail(implId) {
  const impl = querySql('SELECT * FROM playbook_implementations WHERE id = ?', [implId])[0];
  if (!impl) return null;

  const checkpoints = querySql(`
    SELECT * FROM implementation_checkpoints
    WHERE implementation_id = ?
    ORDER BY checkpoint_day ASC
  `, [implId]);

  const baseline = JSON.parse(impl.baseline_json || '{}');

  return {
    implementation: {
      ...impl,
      baseline,
      split_config: impl.split_config_json ? JSON.parse(impl.split_config_json) : null,
      latest_checkpoint: impl.latest_checkpoint_json ? JSON.parse(impl.latest_checkpoint_json) : null,
      recommended_detail: impl.recommended_detail_json ? JSON.parse(impl.recommended_detail_json) : null,
      days_since: _daysSince(impl.implemented_at),
    },
    checkpoints: checkpoints.map(cp => ({
      ...cp,
      confounding_factors: cp.confounding_factors_json ? JSON.parse(cp.confounding_factors_json) : null,
    })),
  };
}

/**
 * Check if an active implementation exists for a given group+rule_type.
 * Used by the playbook UI to show tracking status on cards.
 */
function checkExistingImplementation(clientId, params) {
  const { issuer_bank, is_prepaid, rule_type, card_brand, card_type } = params;

  return querySql(`
    SELECT id, status, implemented_at, min_sample_target, latest_checkpoint_json,
           actual_processor, verdict_reason, rollback_to_processor
    FROM playbook_implementations
    WHERE client_id = ? AND issuer_bank = ? AND is_prepaid = ?
      AND rule_type = ?
      AND (card_brand IS ? OR (card_brand IS NULL AND ? IS NULL))
      AND (card_type IS ? OR (card_type IS NULL AND ? IS NULL))
      AND status NOT IN ('superseded', 'archived')
    ORDER BY implemented_at DESC
    LIMIT 1
  `, [clientId, issuer_bank, is_prepaid, rule_type,
      card_brand || null, card_brand || null,
      card_type || null, card_type || null])[0] || null;
}

/**
 * Get all active implementations for a client (for bulk status on playbook cards).
 */
function getAllActiveImplementations(clientId) {
  return querySql(`
    SELECT id, issuer_bank, is_prepaid, card_brand, card_type,
           rule_type, status, actual_processor,
           min_sample_target, latest_checkpoint_json,
           verdict_reason, rollback_to_processor, implemented_at
    FROM playbook_implementations
    WHERE client_id = ? AND status NOT IN ('superseded', 'archived')
    ORDER BY implemented_at DESC
  `, [clientId]);
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  markPlaybookImplemented,
  evaluatePlaybookImplementations,
  rollbackImplementation,
  archiveImplementation,
  getImplementationDashboard,
  getImplementationDetail,
  checkExistingImplementation,
  getAllActiveImplementations,
};
