const express = require('express');
const { querySql, runSql, saveDb } = require('../db/connection');
const DataIngestion = require('../api/ingestion');
const { acquireImportLock, releaseImportLock, getImportStatus, setIngestionRef } = require('../api/import-lock');
const { runClassifiers } = require('../classifiers/runner');
const { buildPerformanceMatrix } = require('../engine/performance');
const { detectOptimizationWindows, detectMidDegradation } = require('../engine/optimizer');
const { checkWaitingImplementations, evaluateImplementations } = require('../engine/implementation');
const { recomputeAllAnalytics } = require('../analytics/engine');
const { runPostSyncPipeline } = require('../pipeline/post-sync');
const router = express.Router();

// ──────────────────────────────────────────────
// SYNC PROTECTION: Sync is disabled by default.
// Must be explicitly enabled per request via enable-sync endpoint.
// ──────────────────────────────────────────────
let ALLOW_SYNC = false;
let syncTimeout = null;

// POST /api/actions/enable-sync — manually enable sync for one use
// Auto-disables after 4 hours as safety net (not 5 minutes — imports can run for hours).
router.post('/enable-sync', (req, res) => {
  ALLOW_SYNC = true;
  if (syncTimeout) clearTimeout(syncTimeout);
  syncTimeout = setTimeout(() => { ALLOW_SYNC = false; }, 4 * 60 * 60 * 1000);
  console.log('[Sync] Sync ENABLED — will auto-disable in 4 hours.');
  res.json({ enabled: true, message: 'Sync enabled. Will auto-disable in 4 hours.' });
});

// POST /api/actions/disable-sync — manually disable sync
router.post('/disable-sync', (req, res) => {
  ALLOW_SYNC = false;
  console.log('[Sync] Sync DISABLED.');
  res.json({ enabled: false });
});

// GET /api/actions/sync-status — check if sync is enabled
router.get('/sync-status', (req, res) => {
  res.json({ enabled: ALLOW_SYNC });
});

// GET /api/actions/import-status/:clientId — real-time import progress
router.get('/import-status/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  res.json(getImportStatus(clientId));
});

// POST /api/actions/sync/:clientId — incremental sync (default)
// CRITICAL: Uses pullUpdated (incremental) NOT pullTransactions (full history).
// pullTransactions re-downloads ALL orders in the date window and should ONLY
// be used for the very first import of a new client. For regular syncs,
// pullUpdated uses order_find + return_type=order_view with a narrow date
// range and INSERT OR REPLACE to upsert orders efficiently.
router.post('/sync/:clientId', async (req, res) => {
  if (!ALLOW_SYNC) {
    return res.status(403).json({ error: 'Sync disabled. Call POST /api/actions/enable-sync first.' });
  }

  const clientId = parseInt(req.params.clientId, 10);

  try {
    acquireImportLock(clientId, 'incremental');
  } catch (err) {
    return res.status(409).json({ error: err.message });
  }

  try {
    const ingestion = new DataIngestion(clientId);
    ingestion.init();
    setIngestionRef(clientId, ingestion);

    const startDate = req.body.start_date || formatDate(daysAgo(7));
    const endDate = req.body.end_date || formatDate(new Date());

    await ingestion.syncGateways();
    await new Promise(r => setTimeout(r, 3000));
    await ingestion.pullUpdated(startDate, endDate);

    ALLOW_SYNC = false;
    console.log('[Sync] Sync completed — auto-disabled.');
    res.json({ success: true, stats: ingestion.getStats() });

    // Post-sync pipeline: classify → derive → recompute (in correct order)
    try {
      runPostSyncPipeline(clientId);
    } catch (err) {
      console.error('[Sync] Post-sync pipeline failed:', err.message);
    }
    recomputeAllAnalytics(clientId).catch(err =>
      console.error('[Sync] Analytics recompute failed:', err.message)
    );
  } catch (err) {
    ALLOW_SYNC = false;
    res.status(500).json({ error: err.message });
  } finally {
    releaseImportLock(clientId);
  }
});

// POST /api/actions/sync-full/:clientId — full historical import (first-time only)
// WARNING: This re-downloads ALL orders in the date window. Only use for
// initial client setup or disaster recovery. For regular syncs use /sync.
router.post('/sync-full/:clientId', async (req, res) => {
  if (!ALLOW_SYNC) {
    return res.status(403).json({ error: 'Sync disabled. Call POST /api/actions/enable-sync first.' });
  }

  const clientId = parseInt(req.params.clientId, 10);

  // Acquire import lock — prevents concurrent imports on same client
  try {
    acquireImportLock(clientId, 'full');
  } catch (err) {
    return res.status(409).json({ error: err.message });
  }

  try {
    const ingestion = new DataIngestion(clientId);
    ingestion.init();
    setIngestionRef(clientId, ingestion);

    const startDate = req.body.start_date || formatDate(daysAgo(180));
    const endDate = req.body.end_date || formatDate(new Date());

    await ingestion.syncGateways();
    await new Promise(r => setTimeout(r, 3000));
    await ingestion.pullTransactions(startDate, endDate);

    ALLOW_SYNC = false;
    console.log('[Sync] Full sync completed — auto-disabled.');
    res.json({ success: true, stats: ingestion.getStats(), progress: ingestion.progress });

    // Post-sync pipeline: classify → derive → recompute (in correct order)
    try {
      runPostSyncPipeline(clientId);
    } catch (err) {
      console.error('[Sync] Post-sync pipeline failed:', err.message);
    }
    recomputeAllAnalytics(clientId).catch(err =>
      console.error('[Sync] Analytics recompute failed:', err.message)
    );
  } catch (err) {
    ALLOW_SYNC = false;
    res.status(500).json({ error: err.message });
  } finally {
    releaseImportLock(clientId);
  }
});

// POST /api/actions/sync-gateways/:clientId
router.post('/sync-gateways/:clientId', async (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  try {
    const ingestion = new DataIngestion(clientId);
    ingestion.init();
    const count = await ingestion.syncGateways(
      parseInt(req.body.start_id, 10) || 1,
      parseInt(req.body.end_id, 10) || 300
    );
    res.json({ success: true, count });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/actions/sync-campaigns/:clientId
router.post('/sync-campaigns/:clientId', async (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  try {
    const ingestion = new DataIngestion(clientId);
    ingestion.init();
    const count = await ingestion.syncCampaigns();
    res.json({ success: true, count });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/actions/mid-check/:clientId
router.post('/mid-check/:clientId', async (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  try {
    const ingestion = new DataIngestion(clientId);
    ingestion.init();
    const changes = await ingestion.checkMidStatus();
    res.json({ success: true, changes });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/actions/classify/:clientId
router.post('/classify/:clientId', async (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  try {
    const results = await runClassifiers(clientId, {
      anthropicApiKey: req.body.anthropic_api_key || process.env.ANTHROPIC_API_KEY,
    });
    res.json({ success: true, ...results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/actions/analyze/:clientId — run full analysis pipeline
router.post('/analyze/:clientId', async (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  try {
    // Classify
    const classResults = await runClassifiers(clientId, {
      anthropicApiKey: req.body.anthropic_api_key || process.env.ANTHROPIC_API_KEY,
    });

    // Build performance matrix
    const perfResults = buildPerformanceMatrix(clientId);

    // Detect optimization windows
    const optResults = detectOptimizationWindows(clientId);

    // Check for MID degradation
    const degResults = detectMidDegradation(clientId);

    // Check implementations
    checkWaitingImplementations();
    const evalResults = evaluateImplementations();

    res.json({
      success: true,
      classification: classResults,
      performance: perfResults,
      optimization: optResults,
      degradation: degResults,
      implementations: evalResults,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/actions/alerts/:alertId/resolve
router.post('/alerts/:alertId/resolve', (req, res) => {
  const alertId = parseInt(req.params.alertId, 10);
  runSql("UPDATE alerts SET is_resolved = 1, resolved_at = datetime('now') WHERE id = ?", [alertId]);
  saveDb();
  res.json({ success: true });
});

// GET /api/actions/bin-heatmap/:clientId
router.get('/bin-heatmap/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const data = querySql(`
    SELECT
      bin, gateway_id, cc_type, tier,
      total_transactions, approval_rate, weighted_approval_rate,
      processor_declines, soft_declines
    FROM bin_performance
    WHERE client_id = ? AND period_end = (
      SELECT MAX(period_end) FROM bin_performance WHERE client_id = ?
    )
    ORDER BY tier ASC, total_transactions DESC
  `, [clientId, clientId]);
  res.json(data);
});

function daysAgo(n) {
  const d = new Date(); d.setDate(d.getDate() - n); return d;
}
function formatDate(d) {
  return `${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getDate()).padStart(2,'0')}/${d.getFullYear()}`;
}

module.exports = router;
