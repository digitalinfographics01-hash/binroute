/**
 * Decline Matrix — Recovery analysis per decline reason.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql, queryOneSql } = require('../db/connection');
const {
  CLEAN_FILTER, getCachedOrCompute, daysAgoFilter, formatGatewayName,
} = require('./engine');
const { requiresBankChange } = require('../classifiers/decline');

/**
 * Compute a decline recovery matrix: for each decline reason, analyze
 * recovery rates, gateway performance, and recommended actions.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {string} [opts.txType]      - Filter to a specific tx_type
 * @param {number} [opts.minOccurrences] - Minimum decline occurrences (default 10)
 * @param {number} [opts.days]        - Lookback window in days (default 90)
 * @param {number} [opts.recoveryDays] - Window to look for recovery (default 30)
 * @returns {Array<object>} Array of decline recovery objects
 */
function computeDeclineMatrix(clientId, opts = {}) {
  const txType         = opts.txType || null;
  const minOccurrences = opts.minOccurrences ?? 10;
  const days           = opts.days ?? 180;
  const recoveryDays   = opts.recoveryDays ?? 30;

  const cacheKey = `${txType || ''}:${minOccurrences}:${days}:${recoveryDays}`;

  return getCachedOrCompute(clientId, 'decline-matrix', cacheKey, () => {
    return _computeDeclineMatrix(clientId, txType, minOccurrences, days, recoveryDays);
  });
}

/**
 * Map decline categories to recommended actions.
 */
function recommendedAction(category, recoveryRate, declineReason) {
  // Cross-bank cascade required — override all other recommendations
  if (requiresBankChange(declineReason)) {
    return 'Cascade to different acquiring bank immediately. ' +
      'Retrying same bank = 0% recovery. Different bank = possible recovery. ' +
      'Do NOT retry on same MID or same bank — the gateway fraud filter will block every attempt.';
  }

  const actions = {
    issuer:           'Consider BIN-level routing away from this gateway for these BINs. Issuer declines are rarely recoverable by retry.',
    processor:        'Check processor configuration and MID health. May benefit from cascade to alternate processor.',
    soft:             'Eligible for retry/salvage. Schedule automatic retry within 24-48 hours.',
    fraud:            'Review fraud screening rules. Consider 3DS enrollment for these BINs.',
    insufficient:     'Schedule retry at billing cycle intervals. Consider downsell or installment offers.',
    velocity:         'Reduce transaction velocity for affected BINs. Spread volume across more MIDs.',
    invalid:          'Card data issue — prompt customer for updated payment info.',
    crm_routing_rule: 'CRM routing rule block — order never reached a gateway. Exclude from approval rate calculations.',
    unclassified:     'Classify these declines to enable targeted recovery strategies.',
  };

  let action = actions[category] || actions.unclassified;

  if (recoveryRate > 40) {
    action += ' Current recovery rate is strong — maintain existing recovery flow.';
  } else if (recoveryRate > 15) {
    action += ' Moderate recovery rate — consider optimizing retry timing and gateway selection.';
  } else if (recoveryRate > 0) {
    action += ' Low recovery rate — investigate root cause before allocating more retry volume.';
  }

  return action;
}

function _computeDeclineMatrix(clientId, txType, minOccurrences, days, recoveryDays) {
  // -----------------------------------------------------------------------
  // 1. Decline reasons with 10+ occurrences
  // -----------------------------------------------------------------------
  let reasonWhere = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.order_status = 7
    AND o.decline_reason IS NOT NULL AND o.decline_reason != ''`;
  const reasonParams = [clientId];

  if (txType) {
    reasonWhere += ' AND o.tx_type = ?';
    reasonParams.push(txType);
  }

  const reasonStats = querySql(`
    SELECT
      o.decline_reason,
      COALESCE(o.decline_category, 'unclassified') AS decline_category,
      COUNT(*) AS total,
      COUNT(DISTINCT o.customer_id) AS unique_customers,
      COUNT(DISTINCT o.cc_first_6) AS unique_bins,
      COUNT(DISTINCT o.gateway_id) AS gateways_affected
    FROM orders o
    WHERE ${reasonWhere}
    GROUP BY o.decline_reason
    HAVING total >= ?
    ORDER BY total DESC
  `, [...reasonParams, minOccurrences]);

  if (reasonStats.length === 0) return [];

  const reasons = reasonStats.map(r => r.decline_reason);
  const inPlaceholders = reasons.map(() => '?').join(',');

  // -----------------------------------------------------------------------
  // 2. Category breakdown per decline reason
  // -----------------------------------------------------------------------
  const categoryRows = querySql(`
    SELECT
      o.decline_reason,
      COALESCE(o.decline_category, 'unclassified') AS decline_category,
      COUNT(*) AS cnt
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
      AND o.order_status = 7
      AND o.decline_reason IN (${inPlaceholders})
    GROUP BY o.decline_reason, o.decline_category
    ORDER BY o.decline_reason, cnt DESC
  `, [clientId, ...reasons]);

  const categoryMap = new Map();
  for (const row of categoryRows) {
    if (!categoryMap.has(row.decline_reason)) categoryMap.set(row.decline_reason, []);
    categoryMap.get(row.decline_reason).push({
      category: row.decline_category,
      count:    row.cnt,
    });
  }

  // -----------------------------------------------------------------------
  // 3. Recovery analysis — PRE-AGGREGATED approach (no correlated subqueries)
  //    Step A: Build a lookup of customers who had ANY approval after a decline
  //    Step B: Count per decline_reason how many customers recovered
  // -----------------------------------------------------------------------
  // First, get all declined orders with customers, grouped by decline_reason
  const declineCounts = querySql(`
    SELECT decline_reason, COUNT(*) AS declined_with_customer
    FROM orders o
    WHERE o.client_id = ? AND ${CLEAN_FILTER}
      AND ${daysAgoFilter(days)}
      AND o.order_status = 7
      AND o.decline_reason IN (${inPlaceholders})
      AND o.customer_id IS NOT NULL
      ${txType ? 'AND o.tx_type = ?' : ''}
    GROUP BY o.decline_reason
  `, txType ? [clientId, ...reasons, txType] : [clientId, ...reasons]);

  // Then, find unique (customer, product_group, decline_reason) combos where
  // customer later got approved — using a single efficient JOIN
  const recoveredCounts = querySql(`
    SELECT d.decline_reason, COUNT(DISTINCT d.customer_id || ':' || COALESCE(d.product_group_id,'')) AS recovered
    FROM orders d
    INNER JOIN orders r ON r.client_id = d.client_id
      AND r.customer_id = d.customer_id
      AND COALESCE(r.product_group_id,0) = COALESCE(d.product_group_id,0)
      AND r.order_status IN (2,6,8)
      AND r.order_id > d.order_id
    WHERE d.client_id = ? AND d.is_test = 0 AND d.is_internal_test = 0
      AND ${daysAgoFilter(days).replace(/o\./g, 'd.')}
      AND d.order_status = 7
      AND d.decline_reason IN (${inPlaceholders})
      AND d.customer_id IS NOT NULL
      ${txType ? 'AND d.tx_type = ?' : ''}
    GROUP BY d.decline_reason
  `, txType ? [clientId, ...reasons, txType] : [clientId, ...reasons]);

  const recoveryMap = new Map();
  for (const dc of declineCounts) {
    recoveryMap.set(dc.decline_reason, { declined_with_customer: dc.declined_with_customer, recovered: 0 });
  }
  for (const rc of recoveredCounts) {
    const entry = recoveryMap.get(rc.decline_reason);
    if (entry) entry.recovered = rc.recovered;
  }

  // -----------------------------------------------------------------------
  // 4. Best/worst recovery gateway — simple aggregation approach
  //    For declined orders where customer had a later attempt on a different gateway
  // -----------------------------------------------------------------------
  const gwRecoveryRows = querySql(`
    SELECT
      d.decline_reason,
      r.gateway_id,
      COALESCE(g.gateway_alias, 'Gateway #' || r.gateway_id) AS gateway_name,
      COUNT(*) AS recovery_attempts,
      COUNT(CASE WHEN r.order_status IN (2,6,8) THEN 1 END) AS recovery_approved
    FROM orders d
    INNER JOIN orders r ON r.client_id = d.client_id
      AND r.customer_id = d.customer_id
      AND r.order_id > d.order_id
      AND r.gateway_id != d.gateway_id
      AND r.order_status IN (2,6,7,8)
    LEFT JOIN gateways g ON r.gateway_id = g.gateway_id AND r.client_id = g.client_id
    WHERE d.client_id = ? AND d.is_test = 0 AND d.is_internal_test = 0
      AND ${daysAgoFilter(days).replace(/o\./g, 'd.')}
      AND d.order_status = 7
      AND d.decline_reason IN (${inPlaceholders})
      AND d.customer_id IS NOT NULL
    GROUP BY d.decline_reason, r.gateway_id
    HAVING recovery_attempts >= 3
    ORDER BY d.decline_reason, recovery_approved * 1.0 / recovery_attempts DESC
  `, [clientId, ...reasons]);

  const gwRecoveryMap = new Map();
  for (const row of gwRecoveryRows) {
    if (!gwRecoveryMap.has(row.decline_reason)) gwRecoveryMap.set(row.decline_reason, []);
    gwRecoveryMap.get(row.decline_reason).push({
      gateway_id:        row.gateway_id,
      gateway_name:      row.gateway_name,
      recovery_attempts: row.recovery_attempts,
      recovery_approved: row.recovery_approved,
      recovery_rate:     row.recovery_attempts > 0
        ? Math.round((row.recovery_approved / row.recovery_attempts) * 10000) / 100
        : 0,
    });
  }

  // -----------------------------------------------------------------------
  // 5. Recovery rate per tx_type — simple GROUP BY, no correlated subquery
  // -----------------------------------------------------------------------
  const txRecoveryRows = querySql(`
    SELECT
      d.decline_reason,
      d.tx_type,
      COUNT(DISTINCT d.customer_id) AS declined_count,
      COUNT(DISTINCT CASE WHEN r.order_id IS NOT NULL THEN d.customer_id END) AS recovered
    FROM orders d
    LEFT JOIN orders r ON r.client_id = d.client_id
      AND r.customer_id = d.customer_id
      AND COALESCE(r.product_group_id,0) = COALESCE(d.product_group_id,0)
      AND r.order_status IN (2,6,8)
      AND r.order_id > d.order_id
    WHERE d.client_id = ? AND d.is_test = 0 AND d.is_internal_test = 0
      AND ${daysAgoFilter(days).replace(/o\./g, 'd.')}
      AND d.order_status = 7
      AND d.decline_reason IN (${inPlaceholders})
      AND d.customer_id IS NOT NULL
    GROUP BY d.decline_reason, d.tx_type
    HAVING declined_count >= 3
    ORDER BY d.decline_reason, declined_count DESC
  `, [clientId, ...reasons]);

  const txRecoveryMap = new Map();
  for (const row of txRecoveryRows) {
    if (!txRecoveryMap.has(row.decline_reason)) txRecoveryMap.set(row.decline_reason, []);
    txRecoveryMap.get(row.decline_reason).push({
      tx_type:        row.tx_type || 'unknown',
      declined_count: row.declined_count,
      recovered:      row.recovered,
      recovery_rate:  row.declined_count > 0
        ? Math.round((row.recovered / row.declined_count) * 10000) / 100
        : 0,
    });
  }

  // -----------------------------------------------------------------------
  // 6. Per-gateway breakdown (which gateways see this decline most)
  // -----------------------------------------------------------------------
  let gwWhere = `o.client_id = ? AND ${CLEAN_FILTER} AND ${daysAgoFilter(days)}
    AND o.order_status = 7
    AND o.decline_reason IN (${inPlaceholders})`;
  const gwParams = [clientId, ...reasons];

  if (txType) {
    gwWhere += ' AND o.tx_type = ?';
    gwParams.push(txType);
  }

  const gwBreakdownRows = querySql(`
    SELECT
      o.decline_reason,
      o.gateway_id,
      COALESCE(g.gateway_alias, 'Gateway #' || o.gateway_id) AS gateway_name,
      COUNT(*) AS decline_count
    FROM orders o
    LEFT JOIN gateways g ON o.gateway_id = g.gateway_id AND o.client_id = g.client_id
    WHERE ${gwWhere}
    GROUP BY o.decline_reason, o.gateway_id
    ORDER BY o.decline_reason, decline_count DESC
  `, gwParams);

  const gwBreakdownMap = new Map();
  for (const row of gwBreakdownRows) {
    if (!gwBreakdownMap.has(row.decline_reason)) gwBreakdownMap.set(row.decline_reason, []);
    gwBreakdownMap.get(row.decline_reason).push({
      gateway_id:    row.gateway_id,
      gateway_name:  row.gateway_name,
      decline_count: row.decline_count,
    });
  }

  // -----------------------------------------------------------------------
  // 7. Same-bank vs different-bank recovery (for cross-bank cascade analysis)
  // -----------------------------------------------------------------------
  const bankRecoveryRows = querySql(`
    SELECT
      d.decline_reason,
      CASE WHEN dg.bank_name = rg.bank_name THEN 'same_bank' ELSE 'different_bank' END AS bank_match,
      COUNT(DISTINCT d.customer_id) AS attempts,
      COUNT(DISTINCT CASE WHEN r.order_status IN (2,6,8) THEN d.customer_id END) AS recovered
    FROM orders d
    INNER JOIN orders r ON r.client_id = d.client_id
      AND r.customer_id = d.customer_id
      AND r.order_id > d.order_id
      AND r.order_status IN (2,6,7,8)
    LEFT JOIN gateways dg ON d.gateway_id = dg.gateway_id AND d.client_id = dg.client_id
    LEFT JOIN gateways rg ON r.gateway_id = rg.gateway_id AND r.client_id = rg.client_id
    WHERE d.client_id = ? AND d.is_test = 0 AND d.is_internal_test = 0
      AND ${daysAgoFilter(days).replace(/o\./g, 'd.')}
      AND d.order_status = 7
      AND d.decline_reason IN (${inPlaceholders})
      AND d.customer_id IS NOT NULL
      AND d.requires_bank_change = 1
    GROUP BY d.decline_reason, bank_match
  `, [clientId, ...reasons]);

  const bankRecoveryMap = new Map();
  for (const row of bankRecoveryRows) {
    if (!bankRecoveryMap.has(row.decline_reason)) {
      bankRecoveryMap.set(row.decline_reason, { same_bank: { attempts: 0, recovered: 0 }, different_bank: { attempts: 0, recovered: 0 } });
    }
    bankRecoveryMap.get(row.decline_reason)[row.bank_match] = {
      attempts:  row.attempts,
      recovered: row.recovered,
    };
  }

  // -----------------------------------------------------------------------
  // 8. Assemble decline recovery objects
  // -----------------------------------------------------------------------
  const results = [];

  for (const rs of reasonStats) {
    const recovery    = recoveryMap.get(rs.decline_reason) || {};
    const gwRecovery  = gwRecoveryMap.get(rs.decline_reason) || [];
    const txRecovery  = txRecoveryMap.get(rs.decline_reason) || [];
    const gwBreakdown = gwBreakdownMap.get(rs.decline_reason) || [];
    const categories  = categoryMap.get(rs.decline_reason) || [];

    const recoveryRate = recovery.declined_with_customer > 0
      ? Math.round((recovery.recovered / recovery.declined_with_customer) * 10000) / 100
      : 0;

    const bestRecoveryGw  = gwRecovery.length > 0 ? gwRecovery[0] : null;
    const worstRecoveryGw = gwRecovery.length > 1 ? gwRecovery[gwRecovery.length - 1] : null;

    // Primary category for action recommendation
    const primaryCategory = categories.length > 0 ? categories[0].category : 'unclassified';

    const needsBankChange = requiresBankChange(rs.decline_reason);

    // Build routing rule block for cross-bank cascade declines
    let routingRule = null;
    if (needsBankChange) {
      const bankStats = bankRecoveryMap.get(rs.decline_reason) || {
        same_bank: { attempts: 0, recovered: 0 },
        different_bank: { attempts: 0, recovered: 0 },
      };
      const sameBankRate = bankStats.same_bank.attempts > 0
        ? Math.round((bankStats.same_bank.recovered / bankStats.same_bank.attempts) * 10000) / 100 : 0;
      const diffBankRate = bankStats.different_bank.attempts > 0
        ? Math.round((bankStats.different_bank.recovered / bankStats.different_bank.attempts) * 10000) / 100 : 0;

      routingRule = {
        recoverable:       true,
        recovery_condition: 'bank change required',
        same_bank_retry:   'NEVER',
        same_bank_recovery_rate:      sameBankRate,
        same_bank_attempts:           bankStats.same_bank.attempts,
        same_bank_recovered:          bankStats.same_bank.recovered,
        different_bank:               diffBankRate > 0 ? 'possible' : 'unconfirmed',
        different_bank_recovery_rate: diffBankRate,
        different_bank_attempts:      bankStats.different_bank.attempts,
        different_bank_recovered:     bankStats.different_bank.recovered,
        best_action: 'Cascade to different acquiring bank immediately. ' +
          'Retrying same bank = 0% recovery. Different bank = possible recovery.',
      };
    }

    results.push({
      decline_reason:        rs.decline_reason,
      decline_category:      rs.decline_category,
      requires_bank_change:  needsBankChange,
      total:                 rs.total,
      unique_customers:      rs.unique_customers,
      unique_bins:           rs.unique_bins,
      gateways_affected:     rs.gateways_affected,

      // Category breakdown
      category_breakdown: categories,

      // Recovery analysis
      recovery: {
        declined_with_customer: recovery.declined_with_customer || 0,
        recovered:              recovery.recovered || 0,
        recovery_rate:          recoveryRate,
        recovery_window_days:   recoveryDays,
      },

      // Cross-bank cascade routing rule (only for requires_bank_change declines)
      routing_rule: routingRule,

      // Best/worst recovery gateway
      best_recovery_gateway:  bestRecoveryGw,
      worst_recovery_gateway: worstRecoveryGw,
      recovery_gateways:      gwRecovery,

      // Recovery rate per tx_type
      recovery_by_tx_type: txRecovery,

      // Gateway decline distribution
      gateway_breakdown: gwBreakdown,

      // Recommended action
      recommended_action: recommendedAction(primaryCategory, recoveryRate, rs.decline_reason),
    });
  }

  return results;
}

module.exports = { computeDeclineMatrix };
