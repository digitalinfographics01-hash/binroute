const express = require('express');
const { querySql, runSql, saveDb } = require('../db/connection');
const { markImplemented } = require('../engine/implementation');
const router = express.Router();

// GET /api/recommendations/:clientId
router.get('/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const status = req.query.status || null;
  const mcc = req.query.mcc || null;
  const txType = req.query.transaction_type || null;
  const bin = req.query.bin || null;

  let where = 'WHERE r.client_id = ?';
  const params = [clientId];

  if (status) { where += ' AND r.status = ?'; params.push(status); }
  if (mcc) { where += ' AND r.mcc_code = ?'; params.push(mcc); }
  if (txType) { where += ' AND r.transaction_type = ?'; params.push(txType); }
  if (bin) { where += ' AND r.bin = ?'; params.push(bin); }

  const recs = querySql(`
    SELECT r.*,
      COALESCE(g1.gateway_alias, g1.gateway_descriptor) as current_gateway_name,
      g1.processor_name as current_processor,
      COALESCE(g2.gateway_alias, g2.gateway_descriptor) as recommended_gateway_name,
      g2.processor_name as recommended_processor
    FROM recommendations r
    LEFT JOIN gateways g1 ON g1.client_id = r.client_id AND g1.gateway_id = r.current_gateway_id
    LEFT JOIN gateways g2 ON g2.client_id = r.client_id AND g2.gateway_id = r.recommended_gateway_id
    ${where}
    ORDER BY r.priority_score DESC
  `, params);

  res.json(recs);
});

// GET /api/recommendations/:clientId/:recId
router.get('/:clientId/:recId', (req, res) => {
  const recId = parseInt(req.params.recId, 10);
  const rec = querySql('SELECT * FROM recommendations WHERE id = ?', [recId])[0];
  if (!rec) return res.status(404).json({ error: 'Not found' });

  // Get before/after comparison if implemented
  const impl = querySql(`
    SELECT * FROM implementations WHERE recommendation_id = ? ORDER BY marked_at DESC LIMIT 1
  `, [recId])[0];

  res.json({ recommendation: rec, implementation: impl || null });
});

// POST /api/recommendations/:recId/implement
router.post('/:recId/implement', (req, res) => {
  try {
    const result = markImplemented(parseInt(req.params.recId, 10));
    res.json(result);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// POST /api/recommendations/:recId/dismiss
router.post('/:recId/dismiss', (req, res) => {
  const recId = parseInt(req.params.recId, 10);
  runSql("UPDATE recommendations SET status = 'dismissed', updated_at = datetime('now') WHERE id = ?", [recId]);
  saveDb();
  res.json({ success: true });
});

// GET /api/recommendations/:clientId/summary
router.get('/:clientId/summary', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const summary = querySql(`
    SELECT
      status,
      COUNT(*) as count,
      ROUND(AVG(expected_lift), 2) as avg_lift,
      SUM(transaction_volume) as total_volume
    FROM recommendations
    WHERE client_id = ?
    GROUP BY status
  `, [clientId]);
  res.json(summary);
});

module.exports = router;
