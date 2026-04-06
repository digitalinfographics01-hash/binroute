/**
 * Network Analysis API Routes — Unified Network Playbook.
 * Mounted at /api/network (no :clientId parameter).
 */
const express = require('express');
const router = express.Router();
const { computeUnifiedNetworkPlaybook } = require('../analytics/network-playbook');
const { clearNetworkCache, NETWORK_CLIENT_ID } = require('../analytics/network-analysis');
const { getCacheInfo, setForceCompute } = require('../analytics/engine');

function parseOpts(req) {
  return { days: parseInt(req.query.days, 10) || 180 };
}

// Unified network playbook — auto-computes on first request if no cache
router.get('/playbook', (req, res) => {
  try {
    let data = computeUnifiedNetworkPlaybook(parseOpts(req));
    if (!data) {
      // No cache — auto-compute on first load
      console.log('[Network] No cache found, auto-computing playbook...');
      setForceCompute(true);
      const start = Date.now();
      try {
        data = computeUnifiedNetworkPlaybook(parseOpts(req));
      } finally {
        setForceCompute(false);
      }
      console.log(`[Network] Auto-compute complete (${((Date.now() - start) / 1000).toFixed(1)}s)`);
    }
    res.json(data || { error: 'No data available. Ensure clients have order data imported.' });
  } catch (err) {
    setForceCompute(false);
    console.error('[Network] Playbook error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Recompute unified playbook
router.post('/recompute', (req, res) => {
  try {
    const opts = parseOpts(req);
    console.log('[Network] Recomputing unified network playbook...');
    clearNetworkCache();
    setForceCompute(true);

    const start = Date.now();
    try {
      computeUnifiedNetworkPlaybook(opts);
    } finally {
      setForceCompute(false);
    }

    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`[Network] Recompute complete (${elapsed}s)`);
    res.json({ ok: true, elapsed: `${elapsed}s` });
  } catch (err) {
    setForceCompute(false);
    console.error('[Network] Recompute failed:', err);
    res.status(500).json({ error: err.message });
  }
});

// Cache info
router.get('/cache-info', (req, res) => {
  try {
    const info = getCacheInfo(NETWORK_CLIENT_ID);
    res.json(info);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
