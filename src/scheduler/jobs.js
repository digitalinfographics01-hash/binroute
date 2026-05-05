const cron = require('node-cron');
const path = require('path');
const fs = require('fs');
const { fork } = require('child_process');
const { querySql, checkpointWal } = require('../db/connection');
const DataIngestion = require('../api/ingestion');
const { runClassifiers } = require('../classifiers/runner');
const { buildPerformanceMatrix } = require('../engine/performance');
const { detectOptimizationWindows, detectMidDegradation } = require('../engine/optimizer');
const { checkWaitingImplementations, evaluateImplementations } = require('../engine/implementation');
const { evaluatePlaybookImplementations } = require('../engine/playbook-implementation');
const { recomputeAllAnalytics } = require('../analytics/engine');
const { runPostSyncPipeline } = require('../pipeline/post-sync');
const { runVctPostSyncPipeline } = require('../pipeline/post-sync-vct');
const { mergeStagingToMain } = require('../db/staging-merge');

const WORKER_PATH = path.join(__dirname, '..', '..', 'scripts', 'import-worker.js');
const STAGING_DIR = path.join(__dirname, '..', '..', 'data', 'staging');

// ---------------------------------------------------------------------------
// Spawn an import worker as a child process
// ---------------------------------------------------------------------------
function spawnImportWorker(clientId, mode, startDate, endDate) {
  // VCT id_based imports can take 2+ hours for 100K+ orders.
  // Status updates use order_view per-order and can take even longer.
  const timeoutMs = mode === 'updates'
    ? (clientId === 6 ? 18 * 3600000 : 2 * 3600000)   // updates: 18h VCT, 2h others
    : (clientId === 6 ? 3 * 3600000 : 1 * 3600000);    // imports: 3h VCT, 1h others

  return new Promise((resolve, reject) => {
    const maxMem = clientId === 6 ? 512 : 384;
    const child = fork(WORKER_PATH, [
      `--client=${clientId}`,
      `--mode=${mode}`,
      `--start=${startDate}`,
      `--end=${endDate}`,
    ], {
      execArgv: [`--max-old-space-size=${maxMem}`],
      stdio: ['pipe', 'inherit', 'inherit', 'ipc'],
    });

    let result = null;
    let settled = false;

    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        console.error(`[Scheduler] Worker timeout: client ${clientId} mode=${mode} after ${timeoutMs / 3600000}h — killing`);
        child.kill('SIGTERM');
        setTimeout(() => { try { child.kill('SIGKILL'); } catch {} }, 5000);
        reject(new Error(`Worker timed out after ${timeoutMs / 3600000}h`));
      }
    }, timeoutMs);

    child.on('message', (msg) => { result = msg; });
    child.on('error', (err) => {
      if (!settled) { settled = true; clearTimeout(timer); reject(err); }
    });
    child.on('exit', (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (code === 0 && result && result.status === 'success') {
        resolve(result);
      } else {
        reject(new Error(
          result && result.error
            ? result.error
            : `Worker exited with code ${code}`
        ));
      }
    });
  });
}

// ---------------------------------------------------------------------------
// Pre-flight: clean stale staging files, check WAL size
// ---------------------------------------------------------------------------
function preFlightChecks() {
  // Clean staging files older than 24h (skip lock files and files in use)
  if (fs.existsSync(STAGING_DIR)) {
    const now = Date.now();
    const MAX_AGE = 24 * 60 * 60 * 1000;
    for (const file of fs.readdirSync(STAGING_DIR)) {
      // Skip lock files
      if (file.startsWith('.')) continue;
      try {
        const fp = path.join(STAGING_DIR, file);
        const stat = fs.statSync(fp);
        if (now - stat.mtimeMs > MAX_AGE) {
          // Check if any sync lock exists for this client (file might be in use by a stale process)
          const clientMatch = file.match(/^client-(\d+)-/);
          if (clientMatch) {
            const lockPath = path.join(STAGING_DIR, `.sync-lock-client-${clientMatch[1]}`);
            if (fs.existsSync(lockPath)) {
              console.log(`[Scheduler] Skipping stale file ${file} — sync lock exists for client ${clientMatch[1]}`);
              continue;
            }
          }
          // Check via lsof if available (Linux)
          try {
            const { execSync } = require('child_process');
            const lsofOut = execSync(`lsof "${fp}" 2>/dev/null || true`, { encoding: 'utf8', timeout: 5000 });
            if (lsofOut.trim().length > 0) {
              console.log(`[Scheduler] Skipping stale file ${file} — still in use by another process`);
              continue;
            }
          } catch { /* lsof not available, proceed with delete */ }

          fs.unlinkSync(fp);
          console.log(`[Scheduler] Cleaned stale staging file: ${file}`);
        }
      } catch {}
    }
  }

  // WAL checkpoint if WAL is large
  try {
    const walPath = path.join(__dirname, '..', '..', 'data', 'binroute.db-wal');
    if (fs.existsSync(walPath)) {
      const walSize = fs.statSync(walPath).size;
      if (walSize > 500 * 1024 * 1024) {
        console.log(`[Scheduler] WAL is ${(walSize / 1024 / 1024).toFixed(0)}MB — running checkpoint...`);
        try { checkpointWal(); } catch {}
      }
    }
  } catch {}
}

// ---------------------------------------------------------------------------
// Per-client sync lock management
// ---------------------------------------------------------------------------
const LOCK_DIR = path.join(__dirname, '..', '..', 'data', 'staging');
const STALE_SYNC_LOCK_MS = 2 * 60 * 60 * 1000; // 2 hours

function acquireSyncLock(clientId) {
  const lockPath = path.join(LOCK_DIR, `.sync-lock-client-${clientId}`);
  // Clean stale lock
  if (fs.existsSync(lockPath)) {
    try {
      const stat = fs.statSync(lockPath);
      if (Date.now() - stat.mtimeMs > STALE_SYNC_LOCK_MS) {
        console.log(`[Scheduler] Stale sync lock for client ${clientId} (${Math.round((Date.now() - stat.mtimeMs) / 3600000)}h old) — removing`);
        fs.unlinkSync(lockPath);
      } else {
        console.error(`[Scheduler] Client ${clientId} sync already in progress (lock exists). Skipping.`);
        return false;
      }
    } catch {}
  }
  try {
    fs.writeFileSync(lockPath, `pid=${process.pid} ts=${Date.now()}`, { flag: 'wx' });
    return true;
  } catch (err) {
    if (err.code === 'EEXIST') {
      console.error(`[Scheduler] Client ${clientId} sync lock conflict. Skipping.`);
      return false;
    }
    throw err;
  }
}

function releaseSyncLock(clientId) {
  const lockPath = path.join(LOCK_DIR, `.sync-lock-client-${clientId}`);
  try { fs.unlinkSync(lockPath); } catch {}
}

// ---------------------------------------------------------------------------
// Import phase: runs in isolated worker, returns result (no main DB writes)
// ---------------------------------------------------------------------------
async function runClientImport(clientId, dayWindow, options = {}) {
  const label = `Client ${clientId}`;
  const endDate = formatDate(new Date());
  const startDate = formatDate(daysAgo(dayWindow));
  const importMode = options.importMode || 'transactions';

  console.log(`[Scheduler] ${label}: starting import (${startDate} to ${endDate}, mode=${importMode})...`);

  // Gateway sync stays in main process (lightweight, needs main DB for lifecycle)
  try {
    const ingestion = new DataIngestion(clientId);
    ingestion.init();
    await ingestion.syncGateways();
  } catch (err) {
    console.error(`[Scheduler] ${label}: gateway sync failed:`, err.message);
  }

  // --- Import orders via worker process ---
  let txResult;
  try {
    txResult = await spawnImportWorker(clientId, importMode, startDate, endDate);
    console.log(`[Scheduler] ${label}: import done — ${txResult.stats?.stagingRows || 0} rows in ${txResult.elapsed}s`);
    if (txResult.stagingPostSync) {
      const ps = txResult.stagingPostSync;
      console.log(`[Scheduler] ${label}: staging post-sync: classified=${ps.classified} roles=${ps.rolesSet} cascades=${ps.cascadesParsed} fallback=${ps.fallbackRequired}`);
    }
  } catch (err) {
    console.error(`[Scheduler] ${label}: import FAILED — ${err.message}`);
    // Retry once
    console.log(`[Scheduler] ${label}: retrying import in 30s...`);
    await sleep(30000);
    try {
      txResult = await spawnImportWorker(clientId, 'transactions', startDate, endDate);
      console.log(`[Scheduler] ${label}: retry import done — ${txResult.stats?.stagingRows || 0} rows in ${txResult.elapsed}s`);
    } catch (retryErr) {
      console.error(`[Scheduler] ${label}: retry also FAILED — ${retryErr.message}. Skipping this client.`);
      return null;
    }
  }

  return { clientId, txResult, endDate };
}

// ---------------------------------------------------------------------------
// Merge+PostSync phase: runs sequentially in main process (one at a time)
// ---------------------------------------------------------------------------
async function runClientMergeAndPostSync(importResult, postSyncFn) {
  const { clientId, txResult, endDate } = importResult;
  const label = `Client ${clientId}`;

  // --- Merge staging into main DB ---
  try {
    const mergeResult = mergeStagingToMain(txResult.stagingPath, clientId);
    console.log(`[Scheduler] ${label}: merge done — ${mergeResult.inserted} new, ${mergeResult.updated} updated in ${mergeResult.elapsed}s`);
  } catch (err) {
    console.error(`[Scheduler] ${label}: merge FAILED — ${err.message}. Staging file preserved for debugging.`);
    return;
  }

  // --- Post-sync pipeline FIRST (caches refresh immediately after merge) ---
  // Status updates only change order_status on older orders and can take 14+ hours
  // for VCT. Running caches first ensures analytics are fresh right after import.
  try {
    await postSyncFn(clientId);
  } catch (err) {
    console.error(`[Scheduler] ${label}: post-sync failed:`, err.message);
  }

  // --- Status updates via worker process (runs AFTER caches are fresh) ---
  const updatesStart = formatDate(daysAgo(5));
  try {
    const updResult = await spawnImportWorker(clientId, 'updates', updatesStart, endDate);
    console.log(`[Scheduler] ${label}: status updates done — ${updResult.stats?.stagingRows || 0} rows in ${updResult.elapsed}s`);
    // Merge status updates
    try {
      mergeStagingToMain(updResult.stagingPath, clientId);
    } catch (mergeErr) {
      console.error(`[Scheduler] ${label}: status update merge failed:`, mergeErr.message);
    }
  } catch (err) {
    console.error(`[Scheduler] ${label}: status updates failed:`, err.message);
    // Non-fatal — orders were already merged
  }
}

// ---------------------------------------------------------------------------
// Post-sync functions per client type
// ---------------------------------------------------------------------------
async function kpPostSync(clientId) {
  // Analysis pipeline
  try {
    await runClassifiers(clientId);
    buildPerformanceMatrix(clientId);
    detectOptimizationWindows(clientId);
    detectMidDegradation(clientId);
    checkWaitingImplementations();
    evaluateImplementations();
  } catch (err) {
    console.error(`[Scheduler] Analysis pipeline failed for client ${clientId}:`, err.message);
  }

  // Post-sync: classify → derive → reconcile → alerts
  try {
    await runPostSyncPipeline(clientId);
  } catch (err) {
    console.error(`[Scheduler] Post-sync pipeline failed for client ${clientId}:`, err.message);
  }

  // Analytics recompute — now AWAITED (no more fire-and-forget)
  try {
    await recomputeAllAnalytics(clientId);
  } catch (err) {
    console.error(`[Scheduler] Analytics recompute failed for client ${clientId}:`, err.message);
  }

  try {
    const pbResult = evaluatePlaybookImplementations();
    if (pbResult.evaluated > 0 || pbResult.transitioned > 0) {
      console.log(`[Scheduler] Playbook implementations: ${pbResult.evaluated} evaluated, ${pbResult.transitioned} transitioned`);
    }
  } catch (err) {
    console.error(`[Scheduler] Playbook eval failed:`, err.message);
  }
}

async function vctPostSync(clientId) {
  // Import COGS + Ad Spend before P&L cache refresh so everything is computed once
  try {
    await new Promise((resolve, reject) => {
      const child = fork(path.join(__dirname, '..', '..', 'scripts', 'import-daily-cogs.js'), [], {
        stdio: ['pipe', 'inherit', 'inherit', 'ipc'],
      });
      child.on('exit', (code) => code === 0 ? resolve() : reject(new Error(`exit code ${code}`)));
      child.on('error', reject);
    });
    console.log('[Scheduler] COGS import complete.');
  } catch (err) {
    console.error('[Scheduler] COGS import failed:', err.message);
  }

  try {
    await new Promise((resolve, reject) => {
      const child = fork(path.join(__dirname, '..', '..', 'scripts', 'import-ad-spend.js'), [], {
        stdio: ['pipe', 'inherit', 'inherit', 'ipc'],
      });
      child.on('exit', (code) => code === 0 ? resolve() : reject(new Error(`exit code ${code}`)));
      child.on('error', reject);
    });
    console.log('[Scheduler] Ad Spend import complete.');
  } catch (err) {
    console.error('[Scheduler] Ad Spend import failed:', err.message);
  }

  try {
    const result = runVctPostSyncPipeline(clientId);
    console.log(`[Scheduler] VCT post-sync: ${result.classified} classified, ${result.cascadeParsed} cascades parsed`);
  } catch (err) {
    console.error(`[Scheduler] VCT post-sync pipeline failed:`, err.message);
  }
}

// ---------------------------------------------------------------------------
// Schedule all recurring jobs
// ---------------------------------------------------------------------------
function startScheduler() {
  console.log('[Scheduler] Starting scheduled jobs...');

  // Daily sync — imports in parallel (isolated workers), merges+post-sync sequential
  cron.schedule('0 6 * * *', async () => {
    console.log('[Scheduler] === DAILY SYNC START ===');
    preFlightChecks();

    // Define client configs
    const clientConfigs = [
      { clientId: 1, dayWindow: 7, postSyncFn: kpPostSync },
      { clientId: 2, dayWindow: 7, postSyncFn: kpPostSync },
      { clientId: 6, dayWindow: 15, postSyncFn: vctPostSync, importMode: 'id_based' },
    ];

    // Acquire per-client locks, skip any that are locked
    const lockedClients = [];
    for (const cfg of clientConfigs) {
      if (acquireSyncLock(cfg.clientId)) {
        lockedClients.push(cfg);
      }
    }

    try {
      // Phase 1: Imports in parallel (each is its own process + staging DB)
      const importPromises = lockedClients.map(cfg =>
        runClientImport(cfg.clientId, cfg.dayWindow, { importMode: cfg.importMode })
          .catch(err => {
            console.error(`[Scheduler] Client ${cfg.clientId}: import crashed — ${err.message}`);
            return null;
          })
      );
      const importResults = await Promise.allSettled(importPromises);

      // Phase 2: Merges + post-sync SEQUENTIAL (one at a time, no DB contention)
      for (let i = 0; i < lockedClients.length; i++) {
        const cfg = lockedClients[i];
        const settled = importResults[i];
        const result = settled.status === 'fulfilled' ? settled.value : null;

        if (!result) {
          console.error(`[Scheduler] Client ${cfg.clientId}: skipping merge (import failed)`);
          continue;
        }

        try {
          await runClientMergeAndPostSync(result, cfg.postSyncFn);
        } catch (err) {
          console.error(`[Scheduler] Client ${cfg.clientId}: merge/post-sync crashed — ${err.message}`);
        }
      }
    } finally {
      // Always release all locks
      for (const cfg of lockedClients) {
        releaseSyncLock(cfg.clientId);
      }
    }

    console.log('[Scheduler] === DAILY SYNC COMPLETE ===');
  });

  // Hourly MID status check (at :30 to avoid colliding with daily sync at :00)
  cron.schedule('30 * * * *', async () => {
    console.log('[Scheduler] Running hourly MID status check...');
    const clients = querySql('SELECT id FROM clients');

    for (const { id } of clients) {
      try {
        const ingestion = new DataIngestion(id);
        ingestion.init();
        await ingestion.checkMidStatus();
      } catch (err) {
        console.error(`[Scheduler] MID check failed for client ${id}:`, err.message);
      }
    }
  });

  // Every 6 hours: check implementations
  cron.schedule('0 */6 * * *', () => {
    console.log('[Scheduler] Checking implementation statuses...');
    try {
      checkWaitingImplementations();
      evaluateImplementations();
      const pbResult = evaluatePlaybookImplementations();
      if (pbResult.evaluated > 0 || pbResult.transitioned > 0) {
        console.log(`[Scheduler] Playbook implementations: ${pbResult.evaluated} evaluated, ${pbResult.transitioned} transitioned`);
      }
    } catch (err) {
      console.error('[Scheduler] Implementation check failed:', err.message);
    }
  });

  // Weekly AI retrain: Sunday 7:00 AM (after daily sync completes)
  cron.schedule('0 7 * * 0', () => {
    console.log('[Scheduler] Running weekly AI retrain...');
    try {
      const { runRetrain } = require('../ml/retrain-runner');
      const result = runRetrain();
      console.log(`[Scheduler] AI retrain complete. Velocity: ${result.velocityUpdated}, Subscription: ${result.subscriptionUpdated}`);
    } catch (err) {
      console.error('[Scheduler] AI retrain failed:', err.message);
    }
  });


  console.log('[Scheduler] Jobs scheduled:');
  console.log('  - Daily sync: clients 1,2,6 at 6:00 AM UTC (parallel import, serialized merge)');
  console.log('  - VCT post-sync includes: COGS + Ad Spend import → classify → P&L cache');
  console.log('  - Hourly MID check: every hour at :30');
  console.log('  - Implementation check: every 6 hours');
  console.log('  - Weekly AI retrain: Sunday 7:00 AM');
}

function daysAgo(n) {
  const d = new Date(); d.setDate(d.getDate() - n); return d;
}
function formatDate(d) {
  return `${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getDate()).padStart(2,'0')}/${d.getFullYear()}`;
}
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = { startScheduler };
