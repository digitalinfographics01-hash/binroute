const express = require('express');
const { querySql } = require('../db/connection');
const router = express.Router();

// GET /api/pnl/:clientId?start=YYYY-MM-DD&end=YYYY-MM-DD&granularity=daily|weekly|monthly
router.get('/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const granularity = req.query.granularity || 'daily';
  const end = req.query.end || new Date().toISOString().slice(0, 10);
  const start = req.query.start || (() => {
    const d = new Date(end);
    d.setDate(d.getDate() - 30);
    return d.toISOString().slice(0, 10);
  })();

  let dateExpr;
  switch (granularity) {
    case 'weekly':
      dateExpr = "strftime('%Y-W%W', acquisition_date)";
      break;
    case 'monthly':
      dateExpr = "strftime('%Y-%m', acquisition_date)";
      break;
    default:
      dateExpr = "acquisition_date";
  }

  let adDateExpr;
  switch (granularity) {
    case 'weekly':
      adDateExpr = "strftime('%Y-W%W', date)";
      break;
    case 'monthly':
      adDateExpr = "strftime('%Y-%m', date)";
      break;
    default:
      adDateExpr = "date";
  }

  // Revenue, orders, approval rates by period
  const revenue = querySql(`
    SELECT
      ${dateExpr} as period,
      COUNT(*) as total_orders,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved_orders,
      SUM(CASE WHEN order_status = 7 THEN 1 ELSE 0 END) as declined_orders,
      ROUND(SUM(CASE WHEN order_status IN (2,6,8) THEN order_total ELSE 0 END), 2) as gross_revenue,
      ROUND(100.0 * SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN order_status IN (2,6,7,8) THEN 1 ELSE 0 END), 0), 2) as approval_rate,
      SUM(CASE WHEN order_status IN (2,6,8) AND billing_cycle = 0 THEN 1 ELSE 0 END) as approved_initials,
      SUM(CASE WHEN order_status IN (2,6,8) AND billing_cycle > 0 THEN 1 ELSE 0 END) as approved_rebills,
      ROUND(SUM(CASE WHEN order_status IN (2,6,8) AND billing_cycle = 0 THEN order_total ELSE 0 END), 2) as initial_revenue,
      ROUND(SUM(CASE WHEN order_status IN (2,6,8) AND billing_cycle > 0 THEN order_total ELSE 0 END), 2) as rebill_revenue,
      ROUND(SUM(CASE WHEN order_status IN (2,6,8) THEN COALESCE(amount_refunded_to_date, 0) ELSE 0 END), 2) as total_refunds,
      SUM(CASE WHEN is_chargeback = 1 THEN 1 ELSE 0 END) as chargebacks
    FROM orders
    WHERE client_id = ? AND is_test = 0 AND COALESCE(is_internal_test, 0) = 0
      AND acquisition_date BETWEEN ? AND ?
    GROUP BY period
    ORDER BY period
  `, [clientId, start, end]);

  // COGS by period (via product_cogs_match)
  const cogs = querySql(`
    SELECT
      ${dateExpr} as period,
      ROUND(SUM(CASE WHEN order_status IN (2,6,8) THEN pcm.cogs ELSE 0 END), 2) as total_cogs,
      COUNT(CASE WHEN order_status IN (2,6,8) AND pcm.cogs IS NOT NULL THEN 1 END) as orders_with_cogs,
      COUNT(CASE WHEN order_status IN (2,6,8) AND pcm.cogs IS NULL THEN 1 END) as orders_without_cogs
    FROM orders o
    LEFT JOIN product_cogs_match pcm
      ON pcm.client_id = o.client_id AND pcm.product_id = CAST(o.main_product_id AS TEXT)
    WHERE o.client_id = ? AND o.is_test = 0 AND COALESCE(o.is_internal_test, 0) = 0
      AND o.acquisition_date BETWEEN ? AND ?
    GROUP BY period
    ORDER BY period
  `, [clientId, start, end]);

  // Ad spend by period
  const adSpend = querySql(`
    SELECT
      ${adDateExpr} as period,
      ROUND(SUM(ad_cost), 2) as total_ad_spend
    FROM ad_spend_daily_store
    WHERE date BETWEEN ? AND ?
    GROUP BY period
    ORDER BY period
  `, [start, end]);

  // Merge into unified P&L rows
  const cogsMap = new Map(cogs.map(r => [r.period, r]));
  const adMap = new Map(adSpend.map(r => [r.period, r]));

  const rows = revenue.map(r => {
    const c = cogsMap.get(r.period) || { total_cogs: 0, orders_with_cogs: 0, orders_without_cogs: 0 };
    const a = adMap.get(r.period) || { total_ad_spend: 0 };

    const grossRevenue = r.gross_revenue || 0;
    const netRevenue = grossRevenue - (r.total_refunds || 0);
    const totalCogs = c.total_cogs || 0;
    const adCost = a.total_ad_spend || 0;
    const grossProfit = netRevenue - totalCogs;
    const netProfit = grossProfit - adCost;

    return {
      period: r.period,
      total_orders: r.total_orders,
      approved_orders: r.approved_orders,
      declined_orders: r.declined_orders,
      approval_rate: r.approval_rate,
      approved_initials: r.approved_initials,
      approved_rebills: r.approved_rebills,
      initial_revenue: r.initial_revenue || 0,
      rebill_revenue: r.rebill_revenue || 0,
      gross_revenue: grossRevenue,
      refunds: r.total_refunds || 0,
      net_revenue: Math.round(netRevenue * 100) / 100,
      chargebacks: r.chargebacks,
      cogs: totalCogs,
      cogs_coverage_pct: r.approved_orders > 0
        ? Math.round(100 * c.orders_with_cogs / r.approved_orders)
        : 0,
      ad_spend: adCost,
      gross_profit: Math.round(grossProfit * 100) / 100,
      net_profit: Math.round(netProfit * 100) / 100,
      margin_pct: netRevenue > 0 ? Math.round(10000 * netProfit / netRevenue) / 100 : 0,
      cac: r.approved_initials > 0
        ? Math.round(100 * adCost / r.approved_initials) / 100
        : null,
    };
  });

  // Summary totals
  const summary = rows.reduce((acc, r) => {
    acc.total_orders += r.total_orders;
    acc.approved_orders += r.approved_orders;
    acc.declined_orders += r.declined_orders;
    acc.approved_initials += r.approved_initials;
    acc.approved_rebills += r.approved_rebills;
    acc.initial_revenue += r.initial_revenue;
    acc.rebill_revenue += r.rebill_revenue;
    acc.gross_revenue += r.gross_revenue;
    acc.refunds += r.refunds;
    acc.net_revenue += r.net_revenue;
    acc.chargebacks += r.chargebacks;
    acc.cogs += r.cogs;
    acc.ad_spend += r.ad_spend;
    acc.gross_profit += r.gross_profit;
    acc.net_profit += r.net_profit;
    return acc;
  }, {
    total_orders: 0, approved_orders: 0, declined_orders: 0,
    approved_initials: 0, approved_rebills: 0,
    initial_revenue: 0, rebill_revenue: 0,
    gross_revenue: 0, refunds: 0, net_revenue: 0,
    chargebacks: 0, cogs: 0, ad_spend: 0,
    gross_profit: 0, net_profit: 0,
  });
  summary.approval_rate = summary.approved_orders + summary.declined_orders > 0
    ? Math.round(10000 * summary.approved_orders / (summary.approved_orders + summary.declined_orders)) / 100
    : 0;
  summary.margin_pct = summary.net_revenue > 0
    ? Math.round(10000 * summary.net_profit / summary.net_revenue) / 100
    : 0;
  summary.cac = summary.approved_initials > 0
    ? Math.round(100 * summary.ad_spend / summary.approved_initials) / 100
    : null;

  // Round summary money fields
  for (const k of ['initial_revenue', 'rebill_revenue', 'gross_revenue', 'refunds', 'net_revenue', 'cogs', 'ad_spend', 'gross_profit', 'net_profit']) {
    summary[k] = Math.round(summary[k] * 100) / 100;
  }

  res.json({ rows, summary, start, end, granularity });
});

module.exports = router;
