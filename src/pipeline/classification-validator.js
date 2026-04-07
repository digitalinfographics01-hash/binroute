/**
 * Classification Validator — Layer 1 validation gate.
 *
 * Validates that all classification data is complete and correct
 * before downstream processing (transaction_attempts, features, AI models).
 *
 * Returns { valid, errors[], warnings[], stats } — blocks if valid=false.
 */
const { querySql } = require('../db/connection');

function validateClassification(clientId) {
  const errors = [];
  const warnings = [];
  const stats = {};

  const totalOrders = querySql(
    `SELECT COUNT(*) as cnt FROM orders
     WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0`,
    [clientId]
  )[0].cnt;
  stats.totalOrders = totalOrders;

  if (totalOrders === 0) {
    errors.push('No qualifying orders found');
    return { valid: false, errors, warnings, stats };
  }

  // 1. product_type_classified — every non-test order needs initial/rebill/straight_sale
  const noPtc = querySql(
    `SELECT COUNT(*) as cnt FROM orders
     WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
       AND product_type_classified IS NULL`,
    [clientId]
  )[0].cnt;
  stats.missingProductType = noPtc;
  if (noPtc > 0) {
    const pct = ((noPtc / totalOrders) * 100).toFixed(1);
    if (noPtc / totalOrders > 0.10) {
      errors.push(`${noPtc} orders (${pct}%) missing product_type_classified`);
    } else {
      warnings.push(`${noPtc} orders (${pct}%) missing product_type_classified`);
    }
  }

  // 2. derived_product_role — must use _initial/_rebill suffixes, never bare main/upsell
  const oldStyleRoles = querySql(
    `SELECT derived_product_role, COUNT(*) as cnt FROM orders
     WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
       AND derived_product_role IN ('main', 'upsell')
     GROUP BY derived_product_role`,
    [clientId]
  );
  if (oldStyleRoles.length > 0) {
    const total = oldStyleRoles.reduce((s, r) => s + r.cnt, 0);
    errors.push(`${total} orders have old-style derived_product_role (${oldStyleRoles.map(r => r.derived_product_role + ':' + r.cnt).join(', ')}). Run post-sync to fix.`);
  }

  const missingRole = querySql(
    `SELECT COUNT(*) as cnt FROM orders
     WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
       AND derived_product_role IS NULL AND product_type_classified IS NOT NULL`,
    [clientId]
  )[0].cnt;
  stats.missingRole = missingRole;
  if (missingRole > 0) {
    const pct = ((missingRole / totalOrders) * 100).toFixed(1);
    if (missingRole / totalOrders > 0.05) {
      errors.push(`${missingRole} orders (${pct}%) have product_type but no derived_product_role`);
    } else {
      warnings.push(`${missingRole} orders (${pct}%) have product_type but no derived_product_role`);
    }
  }

  // 3. product_group_id — required on initial/rebill orders
  const noGroup = querySql(
    `SELECT COUNT(*) as cnt FROM orders
     WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
       AND product_type_classified IN ('initial', 'rebill')
       AND product_group_id IS NULL`,
    [clientId]
  )[0].cnt;
  stats.missingProductGroup = noGroup;
  if (noGroup > 0) {
    const pct = ((noGroup / totalOrders) * 100).toFixed(1);
    if (noGroup / totalOrders > 0.05) {
      errors.push(`${noGroup} initial/rebill orders (${pct}%) missing product_group_id`);
    } else {
      warnings.push(`${noGroup} initial/rebill orders (${pct}%) missing product_group_id`);
    }
  }

  // 4. derived_cycle + derived_attempt — not NULL on initial/rebill orders
  //    straight_sale and unclassified orders are allowed to have NULL
  const noCycle = querySql(
    `SELECT COUNT(*) as cnt FROM orders
     WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
       AND derived_product_role IN ('main_initial', 'upsell_initial', 'main_rebill', 'upsell_rebill')
       AND (derived_cycle IS NULL OR derived_attempt IS NULL)`,
    [clientId]
  )[0].cnt;
  stats.missingCycleAttempt = noCycle;
  if (noCycle > 0) {
    const pct = ((noCycle / totalOrders) * 100).toFixed(1);
    if (noCycle / totalOrders > 0.05) {
      errors.push(`${noCycle} orders (${pct}%) missing derived_cycle or derived_attempt`);
    } else {
      warnings.push(`${noCycle} orders (${pct}%) missing derived_cycle or derived_attempt`);
    }
  }

  // 5. Gateway config — active gateways with recent orders must have processor_name
  const unconfiguredGws = querySql(
    `SELECT DISTINCT g.gateway_id FROM gateways g
     JOIN orders o ON o.client_id = g.client_id AND o.gateway_id = g.gateway_id
     WHERE g.client_id = ? AND g.exclude_from_analysis = 0
       AND (g.processor_name IS NULL OR g.processor_name = '')
       AND o.order_status IN (2,6,7,8) AND o.is_test = 0 AND o.is_internal_test = 0
       AND o.acquisition_date >= date('now', '-180 days')`,
    [clientId]
  );
  stats.unconfiguredGateways = unconfiguredGws.length;
  if (unconfiguredGws.length > 0) {
    errors.push(`${unconfiguredGws.length} active gateways missing processor_name: ${unconfiguredGws.map(g => g.gateway_id).join(', ')}`);
  }

  // 6. product_sequence — product groups should have main/upsell tagged
  const untaggedGroups = querySql(
    `SELECT id, group_name FROM product_groups
     WHERE client_id = ? AND product_sequence IS NULL`,
    [clientId]
  );
  stats.untaggedGroups = untaggedGroups.length;
  if (untaggedGroups.length > 0) {
    warnings.push(`${untaggedGroups.length} product groups missing product_sequence: ${untaggedGroups.map(g => g.group_name).join(', ')}`);
  }

  // Summary
  const valid = errors.length === 0;
  const roleDistrib = querySql(
    `SELECT derived_product_role, COUNT(*) as cnt FROM orders
     WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
     GROUP BY derived_product_role ORDER BY cnt DESC`,
    [clientId]
  );
  stats.roleDistribution = roleDistrib;

  return { valid, errors, warnings, stats };
}

module.exports = { validateClassification };
