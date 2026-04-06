const express = require('express');
const router = express.Router();
const {
  markPlaybookImplemented,
  rollbackImplementation,
  archiveImplementation,
  getImplementationDashboard,
  getImplementationDetail,
  checkExistingImplementation,
  getAllActiveImplementations,
} = require('../engine/playbook-implementation');

// POST /api/implementations/:clientId/mark
// Mark a playbook rule as implemented
router.post('/:clientId/mark', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    const result = markPlaybookImplemented(clientId, req.body);
    res.json(result);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// GET /api/implementations/:clientId/dashboard
// Full dashboard: scorecard + all implementations
router.get('/:clientId/dashboard', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    const data = getImplementationDashboard(clientId);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/implementations/:clientId/active
// All active implementations (for playbook card status badges)
router.get('/:clientId/active', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    const impls = getAllActiveImplementations(clientId);
    // Parse JSON fields for convenience
    const enriched = impls.map(impl => ({
      ...impl,
      latest_checkpoint: impl.latest_checkpoint_json ? JSON.parse(impl.latest_checkpoint_json) : null,
    }));
    res.json(enriched);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/implementations/:clientId/detail/:implId
// Full detail with all checkpoints
router.get('/:clientId/detail/:implId', (req, res) => {
  try {
    const implId = parseInt(req.params.implId, 10);
    const data = getImplementationDetail(implId);
    if (!data) return res.status(404).json({ error: 'Not found' });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/implementations/:clientId/check-existing
// Check if active impl exists for a group+rule_type
router.get('/:clientId/check-existing', (req, res) => {
  try {
    const clientId = parseInt(req.params.clientId, 10);
    const impl = checkExistingImplementation(clientId, {
      issuer_bank: req.query.issuer_bank,
      is_prepaid: parseInt(req.query.is_prepaid || '0', 10),
      rule_type: req.query.rule_type,
      card_brand: req.query.card_brand || null,
      card_type: req.query.card_type || null,
    });
    res.json(impl);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/implementations/:implId/rollback
router.post('/:implId/rollback', (req, res) => {
  try {
    const implId = parseInt(req.params.implId, 10);
    const result = rollbackImplementation(implId);
    res.json(result);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// POST /api/implementations/:implId/archive
router.post('/:implId/archive', (req, res) => {
  try {
    const implId = parseInt(req.params.implId, 10);
    const result = archiveImplementation(implId);
    res.json(result);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

module.exports = router;
