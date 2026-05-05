const express = require('express');
const { querySql } = require('../db/connection');
const router = express.Router();

// Cost rates (VCT-confirmed)
const COST_RATES = {
  processing_pct: 0.11,
  cb_fee: 25,
  cb_representment: 4,
  rdr_cdrn_fee: 11.50,
  rdr_cdrn_bank_fine: 10,
  ethoca_fee: 17,
};

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

  // Read from pre-computed cache
  let periodExpr;
  switch (granularity) {
    case 'weekly':
      periodExpr = "strftime('%Y-W%W', period)";
      break;
    case 'monthly':
      periodExpr = "strftime('%Y-%m', period)";
      break;
    default:
      periodExpr = "period";
  }

  const cached = querySql(`
    SELECT
      ${periodExpr} as period,
      SUM(total_orders) as total_orders,
      SUM(approved_orders) as approved_orders,
      SUM(declined_orders) as declined_orders,
      SUM(first_attempt_approved) as first_attempt_approved,
      SUM(first_attempt_declined) as first_attempt_declined,
      SUM(approved_initials) as approved_initials,
      SUM(approved_rebills) as approved_rebills,
      ROUND(SUM(initial_revenue), 2) as initial_revenue,
      ROUND(SUM(rebill_revenue), 2) as rebill_revenue,
      ROUND(SUM(gross_revenue), 2) as gross_revenue,
      ROUND(SUM(refunds), 2) as refunds,
      SUM(chargebacks) as chargebacks,
      ROUND(SUM(cb_revenue_clawback), 2) as cb_revenue_clawback,
      SUM(cb911_visa) as cb911_visa,
      SUM(cb911_mc) as cb911_mc,
      SUM(cb911_total) as cb911_total,
      SUM(double_hits) as double_hits,
      ROUND(SUM(ad_spend), 2) as ad_spend,
      ROUND(SUM(cogs), 2) as cogs,
      ROUND(SUM(processing), 2) as processing,
      ROUND(SUM(cb_fees), 2) as cb_fees,
      ROUND(SUM(cb_representment), 2) as cb_representment,
      ROUND(SUM(rdr_cdrn_fees), 2) as rdr_cdrn_fees,
      ROUND(SUM(rdr_cdrn_bank_fines), 2) as rdr_cdrn_bank_fines,
      ROUND(SUM(ethoca_fees), 2) as ethoca_fees,
      ROUND(SUM(total_costs), 2) as total_costs,
      ROUND(SUM(net_profit), 2) as net_profit
    FROM pnl_daily_cache
    WHERE client_id = ? AND period BETWEEN ? AND ?
    GROUP BY ${periodExpr}
    ORDER BY ${periodExpr}
  `, [clientId, start, end]);

  if (!cached.length) {
    return res.json({ rows: [], summary: {}, cost_rates: COST_RATES, start, end, granularity, cached: false });
  }

  // Add derived fields to each row
  const rows = cached.map(r => ({
    ...r,
    approval_rate: (r.first_attempt_approved + r.first_attempt_declined) > 0
      ? Math.round(10000 * r.first_attempt_approved / (r.first_attempt_approved + r.first_attempt_declined)) / 100 : 0,
    margin_pct: r.gross_revenue > 0
      ? Math.round(10000 * r.net_profit / r.gross_revenue) / 100 : 0,
    cpa: r.approved_initials > 0
      ? Math.round(100 * r.ad_spend / r.approved_initials) / 100 : null,
  }));

  // Summary
  const sumKeys = [
    'total_orders', 'approved_orders', 'declined_orders',
    'first_attempt_approved', 'first_attempt_declined',
    'approved_initials', 'approved_rebills',
    'initial_revenue', 'rebill_revenue', 'gross_revenue',
    'ad_spend', 'cogs', 'processing', 'refunds',
    'cb_revenue_clawback', 'chargebacks',
    'cb_fees', 'cb_representment',
    'cb911_visa', 'cb911_mc', 'cb911_total',
    'rdr_cdrn_fees', 'rdr_cdrn_bank_fines', 'ethoca_fees',
    'double_hits', 'total_costs', 'net_profit',
  ];
  const summary = {};
  for (const k of sumKeys) summary[k] = 0;
  for (const r of rows) {
    for (const k of sumKeys) summary[k] += r[k] || 0;
  }

  summary.approval_rate = (summary.first_attempt_approved + summary.first_attempt_declined) > 0
    ? Math.round(10000 * summary.first_attempt_approved / (summary.first_attempt_approved + summary.first_attempt_declined)) / 100 : 0;
  summary.margin_pct = summary.gross_revenue > 0
    ? Math.round(10000 * summary.net_profit / summary.gross_revenue) / 100 : 0;
  summary.cpa = summary.approved_initials > 0
    ? Math.round(100 * summary.ad_spend / summary.approved_initials) / 100 : null;

  const moneyKeys = [
    'initial_revenue', 'rebill_revenue', 'gross_revenue',
    'ad_spend', 'cogs', 'processing', 'refunds',
    'cb_revenue_clawback', 'cb_fees', 'cb_representment',
    'rdr_cdrn_fees', 'rdr_cdrn_bank_fines', 'ethoca_fees',
    'total_costs', 'net_profit',
  ];
  for (const k of moneyKeys) {
    summary[k] = Math.round(summary[k] * 100) / 100;
  }

  res.json({ rows, summary, cost_rates: COST_RATES, start, end, granularity, cached: true });
});

// GET /api/pnl/:clientId/approvals?start=YYYY-MM-DD&end=YYYY-MM-DD
router.get('/:clientId/approvals', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const end = req.query.end || new Date().toISOString().slice(0, 10);
  const start = req.query.start || (() => {
    const d = new Date(end);
    d.setDate(d.getDate() - 30);
    return d.toISOString().slice(0, 10);
  })();

  // Daily trend
  const trend = querySql(`
    SELECT period, tx_type, total_attempts, first_attempt_approved, first_attempt_declined, approval_rate
    FROM approval_daily_cache
    WHERE client_id = ? AND period BETWEEN ? AND ?
    ORDER BY period
  `, [clientId, start, end]);

  // Gateway breakdown — aggregate across the date range
  const gateways = querySql(`
    SELECT
      ag.gateway_id,
      ag.tx_type,
      SUM(ag.total_attempts) as total_attempts,
      SUM(ag.first_attempt_approved) as approved,
      SUM(ag.first_attempt_declined) as declined,
      ROUND(100.0 * SUM(ag.first_attempt_approved) / NULLIF(SUM(ag.first_attempt_approved) + SUM(ag.first_attempt_declined), 0), 2) as approval_rate
    FROM approval_gateway_cache ag
    WHERE ag.client_id = ? AND ag.period BETWEEN ? AND ?
    GROUP BY ag.gateway_id, ag.tx_type
    ORDER BY total_attempts DESC
  `, [clientId, start, end]);

  // Get gateway names
  const gwNames = {};
  const gwRows = querySql('SELECT gateway_id, gateway_alias FROM gateways WHERE client_id = ?', [clientId]);
  for (const g of gwRows) gwNames[g.gateway_id] = g.gateway_alias;

  // Reshape trend into { date, initial_rate, rebill_rate }
  const trendMap = {};
  for (const r of trend) {
    if (!trendMap[r.period]) trendMap[r.period] = { period: r.period };
    if (r.tx_type === 'initial') {
      trendMap[r.period].initial_rate = r.approval_rate;
      trendMap[r.period].initial_attempts = r.total_attempts;
    } else if (r.tx_type === 'rebill') {
      trendMap[r.period].rebill_rate = r.approval_rate;
      trendMap[r.period].rebill_attempts = r.total_attempts;
    }
  }
  const trendRows = Object.values(trendMap).sort((a, b) => a.period.localeCompare(b.period));

  // Reshape gateways into per-gateway objects with initial + rebill rates
  const gwMap = {};
  for (const r of gateways) {
    if (!gwMap[r.gateway_id]) {
      gwMap[r.gateway_id] = {
        gateway_id: r.gateway_id,
        gateway_name: gwNames[r.gateway_id] || `GW ${r.gateway_id}`,
      };
    }
    const gw = gwMap[r.gateway_id];
    if (r.tx_type === 'initial') {
      gw.initial_attempts = r.total_attempts;
      gw.initial_approved = r.approved;
      gw.initial_rate = r.approval_rate;
    } else if (r.tx_type === 'rebill') {
      gw.rebill_attempts = r.total_attempts;
      gw.rebill_approved = r.approved;
      gw.rebill_rate = r.approval_rate;
    }
  }
  const gwList = Object.values(gwMap)
    .map(g => ({
      ...g,
      total_attempts: (g.initial_attempts || 0) + (g.rebill_attempts || 0),
    }))
    .sort((a, b) => b.total_attempts - a.total_attempts);

  res.json({ trend: trendRows, gateways: gwList, start, end });
});

// GET /api/pnl/:clientId/cohorts — LTV cohort analysis from cache
router.get('/:clientId/cohorts', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  const rows = querySql(`
    SELECT cohort_month, month_offset, cohort_size, active_customers,
      revenue, orders, retention_pct, cumulative_ltv
    FROM cohort_cache
    WHERE client_id = ?
    ORDER BY cohort_month, month_offset
  `, [clientId]);

  if (!rows.length) return res.json({ cohorts: [] });

  // Group by cohort_month
  const cohortMap = {};
  for (const r of rows) {
    if (!cohortMap[r.cohort_month]) {
      cohortMap[r.cohort_month] = { month: r.cohort_month, size: r.cohort_size, months: [] };
    }
    cohortMap[r.cohort_month].months.push({
      offset: r.month_offset,
      active: r.active_customers,
      revenue: r.revenue,
      orders: r.orders,
      retention: r.retention_pct,
      ltv: r.cumulative_ltv,
    });
  }

  const cohorts = Object.values(cohortMap).sort((a, b) => a.month.localeCompare(b.month));
  res.json({ cohorts });
});

// GET /api/pnl/:clientId/product-lifetime — product lifetime profitability from cache
router.get('/:clientId/product-lifetime', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  const products = querySql(`
    SELECT product_id, product_name, initial_customers, avg_initial_price,
      cogs_per_unit, customers_with_rebills, rebill_rate,
      avg_rebill_revenue,
      ltv_m0, ltv_m1, ltv_m2, ltv_m3, ltv_m4, ltv_m5, ltv_m6, lifetime_revenue,
      processing_per_cust, refunds_per_cust, chargebacks, cb_rate,
      cb_cost_per_cust, cb_clawback_per_cust, alerts, alert_cost_per_cust,
      total_costs_per_cust, lifetime_profit_per_cust,
      initial_refund_rate, initial_refund_count
    FROM product_lifetime_cache WHERE client_id = ?
    ORDER BY initial_customers DESC
  `, [clientId]);

  res.json({ products });
});

// GET /api/pnl/:clientId/gateway-drilldown — per-gateway deep view from cache
router.get('/:clientId/gateway-drilldown', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  const gateways = querySql(`
    SELECT gateway_id, gateway_name, tx_type, total_attempts, first_attempt_approved, approved_revenue
    FROM gateway_drilldown_cache WHERE client_id = ?
  `, [clientId]);

  const declines = querySql(`
    SELECT gateway_id, decline_reason, cnt
    FROM gateway_decline_cache WHERE client_id = ?
    ORDER BY gateway_id, cnt DESC
  `, [clientId]);

  // Build decline map
  const declineMap = {};
  for (const r of declines) {
    if (!declineMap[r.gateway_id]) declineMap[r.gateway_id] = [];
    declineMap[r.gateway_id].push({ reason: r.decline_reason, count: r.cnt });
  }

  // Reshape per-gateway
  const gwMap = {};
  for (const r of gateways) {
    if (!gwMap[r.gateway_id]) {
      gwMap[r.gateway_id] = {
        gateway_id: r.gateway_id,
        gateway_name: r.gateway_name,
        decline_reasons: declineMap[r.gateway_id] || [],
      };
    }
    const gw = gwMap[r.gateway_id];
    if (r.tx_type === 'initial') {
      gw.initial_attempts = r.total_attempts;
      gw.initial_approved = r.first_attempt_approved;
      gw.initial_rate = r.total_attempts > 0 ? Math.round(10000 * r.first_attempt_approved / r.total_attempts) / 100 : 0;
      gw.initial_revenue = r.approved_revenue;
    } else {
      gw.rebill_attempts = r.total_attempts;
      gw.rebill_approved = r.first_attempt_approved;
      gw.rebill_rate = r.total_attempts > 0 ? Math.round(10000 * r.first_attempt_approved / r.total_attempts) / 100 : 0;
      gw.rebill_revenue = r.approved_revenue;
    }
  }

  const gwList = Object.values(gwMap)
    .map(g => ({ ...g, total_attempts: (g.initial_attempts || 0) + (g.rebill_attempts || 0), total_revenue: (g.initial_revenue || 0) + (g.rebill_revenue || 0) }))
    .sort((a, b) => b.total_attempts - a.total_attempts);

  res.json({ gateways: gwList });
});

// GET /api/pnl/:clientId/product-pnl — product-level P&L from cache
router.get('/:clientId/product-pnl', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  const prodList = querySql(`
    SELECT product_id, product_name, total_orders, approved_orders,
      revenue, cogs, margin, margin_pct, approval_rate,
      initial_orders, rebill_orders, initial_revenue, rebill_revenue
    FROM product_pnl_cache WHERE client_id = ?
    ORDER BY revenue DESC
  `, [clientId]);

  const summary = prodList.reduce((s, p) => {
    s.total_orders += p.total_orders;
    s.approved_orders += p.approved_orders;
    s.revenue += p.revenue;
    s.cogs += p.cogs;
    return s;
  }, { total_orders: 0, approved_orders: 0, revenue: 0, cogs: 0 });
  summary.margin = Math.round((summary.revenue - summary.cogs) * 100) / 100;
  summary.margin_pct = summary.revenue > 0 ? Math.round(((summary.revenue - summary.cogs) / summary.revenue) * 10000) / 100 : 0;

  res.json({ products: prodList, summary });
});

module.exports = router;
