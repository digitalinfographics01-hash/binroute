const express = require('express');
const { querySql } = require('../db/connection');
const router = express.Router();

// GET /api/lifecycle/:clientId — all MIDs across all states
router.get('/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  const mids = querySql(`
    SELECT
      g.*,
      COUNT(o.id) as total_orders_90d,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved_90d,
      SUM(CASE WHEN o.order_status = 7 THEN 1 ELSE 0 END) as declined_90d,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) / NULLIF(COUNT(o.id), 0), 2) as approval_rate_90d,
      -- 7-day trend
      SUM(CASE WHEN o.acquisition_date >= date('now', '-7 days') AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved_7d,
      SUM(CASE WHEN o.acquisition_date >= date('now', '-7 days') THEN 1 ELSE 0 END) as total_7d,
      ROUND(100.0 * SUM(CASE WHEN o.acquisition_date >= date('now', '-7 days') AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN o.acquisition_date >= date('now', '-7 days') THEN 1 ELSE 0 END), 0), 2) as approval_rate_7d
    FROM gateways g
    LEFT JOIN orders o ON o.client_id = g.client_id AND o.gateway_id = g.gateway_id
      AND o.acquisition_date >= date('now', '-90 days')
    WHERE g.client_id = ?
    GROUP BY g.gateway_id
    ORDER BY
      CASE g.lifecycle_state
        WHEN 'closed' THEN 0
        WHEN 'degrading' THEN 1
        WHEN 'ramp-up' THEN 2
        ELSE 3
      END,
      total_orders_90d DESC
  `, [clientId]);

  res.json(mids);
});

// GET /api/lifecycle/:clientId/closed — closure emergency view
router.get('/:clientId/closed', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  // Get closed MIDs
  const closedMids = querySql(
    "SELECT gateway_id, gateway_descriptor, gateway_alias FROM gateways WHERE client_id = ? AND lifecycle_state = 'closed'",
    [clientId]
  );

  // For each closed MID, find affected BINs and rank replacement options
  const closureData = closedMids.map(closed => {
    // BINs that were routed through this closed MID
    const affectedBins = querySql(`
      SELECT DISTINCT cc_first_6 as bin, cc_type, COUNT(*) as volume
      FROM orders
      WHERE client_id = ? AND gateway_id = ? AND cc_first_6 IS NOT NULL
        AND acquisition_date >= date('now', '-90 days')
      GROUP BY cc_first_6
      ORDER BY volume DESC
    `, [clientId, closed.gateway_id]);

    // For each BIN, rank active MID alternatives
    const replacements = affectedBins.map(ab => {
      const alternatives = querySql(`
        SELECT
          bp.gateway_id, g.gateway_descriptor, g.processor_name,
          bp.weighted_approval_rate, bp.total_transactions
        FROM bin_performance bp
        JOIN gateways g ON g.client_id = bp.client_id AND g.gateway_id = bp.gateway_id
        WHERE bp.client_id = ? AND bp.bin = ? AND g.lifecycle_state = 'active'
          AND bp.gateway_id != ?
        ORDER BY bp.weighted_approval_rate DESC
        LIMIT 5
      `, [clientId, ab.bin, closed.gateway_id]);

      return { bin: ab.bin, cc_type: ab.cc_type, volume: ab.volume, alternatives };
    });

    return { ...closed, affectedBins: replacements };
  });

  res.json(closureData);
});

// GET /api/lifecycle/:clientId/ramp-up — ramp-up tracker
router.get('/:clientId/ramp-up', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  const rampUp = querySql(`
    SELECT
      g.gateway_id, g.gateway_descriptor, g.gateway_alias, g.created_at,
      g.processor_name, g.bank_name,
      COUNT(o.id) as total_orders,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) / NULLIF(COUNT(o.id), 0), 2) as approval_rate
    FROM gateways g
    LEFT JOIN orders o ON o.client_id = g.client_id AND o.gateway_id = g.gateway_id
    WHERE g.client_id = ? AND g.lifecycle_state = 'ramp-up'
    GROUP BY g.gateway_id
    ORDER BY g.created_at DESC
  `, [clientId]);

  // Add confidence progress (need 30+ transactions for minimum confidence)
  const MIN_CONFIDENCE_TX = 30;
  const result = rampUp.map(r => ({
    ...r,
    confidence_progress: Math.min(100, Math.round((r.total_orders / MIN_CONFIDENCE_TX) * 100)),
    ready_for_routing: r.total_orders >= MIN_CONFIDENCE_TX,
  }));

  res.json(result);
});

// GET /api/lifecycle/:clientId/trend/:gatewayId — MID trend data
router.get('/:clientId/trend/:gatewayId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const gatewayId = parseInt(req.params.gatewayId, 10);

  // Daily approval rate for last 90 days
  const trend = querySql(`
    SELECT
      date(acquisition_date) as day,
      COUNT(*) as total,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      ROUND(100.0 * SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as approval_rate
    FROM orders
    WHERE client_id = ? AND gateway_id = ?
      AND acquisition_date >= date('now', '-90 days')
    GROUP BY date(acquisition_date)
    ORDER BY day ASC
  `, [clientId, gatewayId]);

  res.json(trend);
});

module.exports = router;
