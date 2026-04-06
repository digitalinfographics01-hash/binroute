/**
 * Analytics helpers — graceful NULL handling and data quality indicators.
 *
 * General rule: Never skip a record because a field is NULL.
 * Always substitute a meaningful label. Always show coverage %.
 */
const { querySql, queryOneSql } = require('../db/connection');

const CLEAN_FILTER = 'is_test = 0 AND is_internal_test = 0';

/**
 * Get data quality indicators for a client.
 * Returns coverage percentages for all key fields.
 */
function getDataQuality(clientId) {
  const total = queryOneSql(
    `SELECT COUNT(*) as c FROM orders WHERE client_id = ? AND ${CLEAN_FILTER}`,
    [clientId]
  )?.c || 0;

  if (total === 0) return { total: 0, indicators: [] };

  const revenue = queryOneSql(
    `SELECT COUNT(*) as c FROM orders WHERE client_id = ? AND ${CLEAN_FILTER} AND order_total IS NOT NULL AND order_total > 0`,
    [clientId]
  )?.c || 0;

  const totalBins = queryOneSql(
    `SELECT COUNT(DISTINCT cc_first_6) as c FROM orders WHERE client_id = ? AND ${CLEAN_FILTER} AND cc_first_6 IS NOT NULL`,
    [clientId]
  )?.c || 0;

  const enrichedBins = queryOneSql(
    `SELECT COUNT(DISTINCT o.cc_first_6) as c FROM orders o
     JOIN bin_lookup b ON o.cc_first_6 = b.bin AND b.issuer_bank IS NOT NULL
     WHERE o.client_id = ? AND o.${CLEAN_FILTER} AND o.cc_first_6 IS NOT NULL`,
    [clientId]
  )?.c || 0;

  const totalGateways = queryOneSql(
    `SELECT COUNT(*) as c FROM gateways WHERE client_id = ? AND lifecycle_state IN ('active','degrading')`,
    [clientId]
  )?.c || 0;

  const configuredGateways = queryOneSql(
    `SELECT COUNT(*) as c FROM gateways WHERE client_id = ?
     AND lifecycle_state IN ('active','degrading')
     AND bank_name IS NOT NULL AND processor_name IS NOT NULL AND mcc_code IS NOT NULL`,
    [clientId]
  )?.c || 0;

  const totalDeclines = queryOneSql(
    `SELECT COUNT(*) as c FROM orders WHERE client_id = ? AND ${CLEAN_FILTER} AND order_status = 7`,
    [clientId]
  )?.c || 0;

  const classifiedDeclines = queryOneSql(
    `SELECT COUNT(*) as c FROM orders WHERE client_id = ? AND ${CLEAN_FILTER} AND order_status = 7 AND decline_category IS NOT NULL`,
    [clientId]
  )?.c || 0;

  const pct = (n, d) => d > 0 ? Math.round((n / d) * 100) : 0;

  return {
    total,
    indicators: [
      { label: 'Orders with revenue data', value: pct(revenue, total), count: revenue, of: total },
      { label: 'BINs with issuer data', value: pct(enrichedBins, totalBins), count: enrichedBins, of: totalBins },
      { label: 'Active gateways fully configured', value: pct(configuredGateways, totalGateways), count: configuredGateways, of: totalGateways },
      { label: 'Declines classified', value: pct(classifiedDeclines, totalDeclines), count: classifiedDeclines, of: totalDeclines },
    ],
  };
}

/**
 * Coalesce helpers for analytics queries.
 * Use these in SQL to substitute meaningful labels for NULLs.
 */
const SQL_COALESCE = {
  bank_name: "COALESCE(g.bank_name, 'Unknown Bank')",
  processor_name: "COALESCE(g.processor_name, 'Unknown Processor')",
  mcc_code: "COALESCE(g.mcc_code, 'Not Configured')",
  issuer_bank: "COALESCE(b.issuer_bank, 'Unknown Issuer')",
  card_brand: "COALESCE(b.card_brand, o.cc_type, 'Unknown')",
  card_type: "COALESCE(b.card_type, 'Unknown Type')",
  card_level: "COALESCE(b.card_level, 'Unknown Level')",
  decline_category: "COALESCE(o.decline_category, 'Unclassified')",
  product_group_name: "COALESCE(o.product_group_name, 'Unassigned')",
};

/**
 * Build a standard analytics query with graceful NULL handling.
 * Automatically joins bin_lookup and gateways tables.
 * Applies clean filter (is_test=0, is_internal_test=0).
 *
 * @param {object} opts
 * @param {string} opts.select - SELECT columns (use SQL_COALESCE keys)
 * @param {string} opts.where - Additional WHERE conditions
 * @param {string} opts.groupBy - GROUP BY clause
 * @param {string} opts.orderBy - ORDER BY clause
 * @param {number} opts.clientId - Client ID
 * @param {number} [opts.limit] - Optional LIMIT
 */
function analyticsQuery(opts) {
  const { select, where, groupBy, orderBy, clientId, limit } = opts;

  let sql = `
    SELECT ${select}
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN gateways g ON o.gateway_id = g.gateway_id AND o.client_id = g.client_id
    WHERE o.client_id = ? AND o.${CLEAN_FILTER}
  `;

  if (where) sql += ` AND ${where}`;
  if (groupBy) sql += ` GROUP BY ${groupBy}`;
  if (orderBy) sql += ` ORDER BY ${orderBy}`;
  if (limit) sql += ` LIMIT ${parseInt(limit, 10)}`;

  return querySql(sql, [clientId]);
}

/**
 * Revenue aggregation helper.
 * Returns { total_revenue, order_count, avg_revenue, coverage_pct }
 * Only sums order_total where it's > 0. Shows coverage %.
 */
function revenueMetrics(clientId, whereClause) {
  let sql = `
    SELECT
      COUNT(*) as order_count,
      COUNT(CASE WHEN order_total > 0 THEN 1 END) as with_revenue,
      ROUND(SUM(CASE WHEN order_total > 0 THEN order_total ELSE 0 END), 2) as total_revenue,
      ROUND(AVG(CASE WHEN order_total > 0 THEN order_total END), 2) as avg_revenue
    FROM orders
    WHERE client_id = ? AND ${CLEAN_FILTER}
  `;
  if (whereClause) sql += ` AND ${whereClause}`;

  const row = queryOneSql(sql, [clientId]);
  return {
    ...row,
    coverage_pct: row.order_count > 0
      ? Math.round((row.with_revenue / row.order_count) * 100)
      : 0,
  };
}

/**
 * Approval rate helper with graceful handling.
 * Always includes all orders in count, regardless of NULL fields.
 */
function approvalRate(clientId, whereClause) {
  let sql = `
    SELECT
      COUNT(*) as total,
      COUNT(CASE WHEN order_status IN (2,6,8) THEN 1 END) as approved,
      COUNT(CASE WHEN order_status = 7 THEN 1 END) as declined,
      ROUND(
        COUNT(CASE WHEN order_status IN (2,6,8) THEN 1 END) * 100.0 /
        NULLIF(COUNT(CASE WHEN order_status IN (2,6,7,8) THEN 1 END), 0)
      , 2) as approval_rate
    FROM orders
    WHERE client_id = ? AND ${CLEAN_FILTER}
  `;
  if (whereClause) sql += ` AND ${whereClause}`;

  return queryOneSql(sql, [clientId]);
}

module.exports = {
  CLEAN_FILTER,
  SQL_COALESCE,
  getDataQuality,
  analyticsQuery,
  revenueMetrics,
  approvalRate,
};
