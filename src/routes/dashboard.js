const express = require('express');
const { querySql } = require('../db/connection');
const router = express.Router();

// GET /api/dashboard/:clientId
router.get('/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  // KPIs
  // Approved = status IN (2,6,8), Declined = status 7, Pending/other excluded from rate calc
  const kpis = querySql(`
    SELECT
      COUNT(*) as total_transactions,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      SUM(CASE WHEN order_status = 7 THEN 1 ELSE 0 END) as declined,
      SUM(CASE WHEN is_anonymous_decline = 1 THEN 1 ELSE 0 END) as anonymous_declines,
      ROUND(100.0 * SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN order_status IN (2,6,7,8) THEN 1 ELSE 0 END), 0), 2) as approval_rate
    FROM orders
    WHERE client_id = ? AND is_test = 0 AND acquisition_date >= date('now', '-90 days')
  `, [clientId])[0] || {};

  const openRecs = querySql(
    "SELECT COUNT(*) as count FROM recommendations WHERE client_id = ? AND status = 'open'",
    [clientId]
  )[0]?.count || 0;

  const activeMids = querySql(
    "SELECT COUNT(*) as count FROM gateways WHERE client_id = ? AND lifecycle_state != 'closed' AND gateway_active = 1",
    [clientId]
  )[0]?.count || 0;

  const unroutedBins = querySql(`
    SELECT COUNT(DISTINCT cc_first_6) as count
    FROM orders
    WHERE client_id = ? AND cc_first_6 IS NOT NULL
      AND acquisition_date >= date('now', '-90 days')
      AND cc_first_6 NOT IN (
        SELECT DISTINCT bin FROM bin_performance WHERE client_id = ? AND tier IS NOT NULL
      )
  `, [clientId, clientId])[0]?.count || 0;

  // Alerts (limit 10)
  const alerts = querySql(
    'SELECT * FROM alerts WHERE client_id = ? AND is_resolved = 0 ORDER BY CASE priority WHEN \'P0\' THEN 0 WHEN \'P1\' THEN 1 WHEN \'P2\' THEN 2 ELSE 3 END, created_at DESC LIMIT 10',
    [clientId]
  );

  // MID performance table (active MIDs only, limit 10)
  const midPerformance = querySql(`
    SELECT
      g.gateway_id, g.gateway_descriptor, g.gateway_alias, g.lifecycle_state,
      g.processor_name, g.bank_name, g.mcc_code, g.global_monthly_cap, g.monthly_sales,
      SUM(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 ELSE 0 END) as total_orders,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      SUM(CASE WHEN o.order_status = 7 THEN 1 ELSE 0 END) as declined,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 ELSE 0 END), 0), 2) as approval_rate
    FROM gateways g
    LEFT JOIN orders o ON o.client_id = g.client_id AND o.gateway_id = g.gateway_id
      AND o.is_test = 0 AND o.acquisition_date >= date('now', '-90 days')
    WHERE g.client_id = ? AND g.lifecycle_state != 'closed' AND g.gateway_active = 1
    GROUP BY g.gateway_id
    ORDER BY total_orders DESC
    LIMIT 10
  `, [clientId]);

  // Top optimization windows
  const topWindows = querySql(`
    SELECT r.*,
      COALESCE(g1.gateway_alias, g1.gateway_descriptor) as current_gateway_name,
      COALESCE(g2.gateway_alias, g2.gateway_descriptor) as recommended_gateway_name
    FROM recommendations r
    LEFT JOIN gateways g1 ON g1.client_id = r.client_id AND g1.gateway_id = r.current_gateway_id
    LEFT JOIN gateways g2 ON g2.client_id = r.client_id AND g2.gateway_id = r.recommended_gateway_id
    WHERE r.client_id = ? AND r.status = 'open'
    ORDER BY r.priority_score DESC
    LIMIT 10
  `, [clientId]);

  // Implementation tracker
  const implementations = querySql(`
    SELECT
      i.id, i.marked_at, i.result, i.comparison_start_date,
      r.bin, r.current_gateway_id, r.recommended_gateway_id,
      r.expected_lift, r.summary,
      COALESCE(g1.gateway_alias, g1.gateway_descriptor) as current_gateway_name,
      COALESCE(g2.gateway_alias, g2.gateway_descriptor) as recommended_gateway_name
    FROM implementations i
    JOIN recommendations r ON r.id = i.recommendation_id
    LEFT JOIN gateways g1 ON g1.client_id = r.client_id AND g1.gateway_id = r.current_gateway_id
    LEFT JOIN gateways g2 ON g2.client_id = r.client_id AND g2.gateway_id = r.recommended_gateway_id
    WHERE i.client_id = ?
    ORDER BY i.marked_at DESC
    LIMIT 20
  `, [clientId]);

  // BIN tier summary
  const tierSummary = querySql(`
    SELECT
      tier,
      COUNT(DISTINCT bin) as bin_count,
      SUM(total_transactions) as total_tx,
      ROUND(AVG(weighted_approval_rate), 2) as avg_approval_rate
    FROM bin_performance
    WHERE client_id = ? AND period_end = (
      SELECT MAX(period_end) FROM bin_performance WHERE client_id = ?
    )
    GROUP BY tier
    ORDER BY tier
  `, [clientId, clientId]);

  // Config completeness (for banner)
  const incompleteGateways = querySql(`
    SELECT COUNT(*) as count FROM gateways
    WHERE client_id = ? AND lifecycle_state != 'closed'
      AND (processor_name IS NULL OR bank_name IS NULL OR mcc_code IS NULL)
  `, [clientId])[0]?.count || 0;

  // Sync state
  const syncState = querySql('SELECT * FROM sync_state WHERE client_id = ?', [clientId]);

  // Data quality loaded on-demand via /api/analytics/:clientId/data-quality
  // Never run analytics queries on dashboard load.

  res.json({
    kpis: {
      approval_rate: kpis.approval_rate || 0,
      total_transactions: kpis.total_transactions || 0,
      anonymous_declines: kpis.anonymous_declines || 0,
      open_recommendations: openRecs,
      active_mids: activeMids,
      unrouted_bins: unroutedBins,
    },
    alerts,
    midPerformance,
    topWindows,
    implementations,
    tierSummary,
    incompleteGateways,
    syncState,
  });
});

module.exports = router;
