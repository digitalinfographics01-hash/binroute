#!/usr/bin/env node
/**
 * import-worker.js — standalone child process for importing orders into
 * an isolated staging SQLite DB.
 *
 * Spawned by the scheduler via child_process.fork(). Receives args via CLI,
 * reports results via IPC (process.send). Crashes here never affect the main
 * process or main DB.
 *
 * Usage:
 *   node scripts/import-worker.js --client=6 --mode=transactions --start=04/26/2026 --end=04/29/2026
 *   node scripts/import-worker.js --client=6 --mode=updates --start=04/27/2026 --end=04/29/2026
 */

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const { createDbHelpers } = require('../src/db/connection-factory');
const { initStagingSchema } = require('../src/db/staging-schema');
const DataIngestion = require('../src/api/ingestion');

const DB_PATH = path.join(__dirname, '..', 'data', 'binroute.db');
const STAGING_DIR = path.join(__dirname, '..', 'data', 'staging');

// ---------------------------------------------------------------------------
// Parse CLI args
// ---------------------------------------------------------------------------
function parseArgs() {
  const args = {};
  for (const arg of process.argv.slice(2)) {
    const m = arg.match(/^--(\w+)=(.+)$/);
    if (m) args[m[1]] = m[2];
  }
  const skipClassify = process.argv.includes('--skip-classify');
  if (!args.client || !args.mode || !args.start || !args.end) {
    console.error('Usage: --client=ID --mode=transactions|updates --start=MM/DD/YYYY --end=MM/DD/YYYY [--skip-classify]');
    process.exit(2);
  }
  return {
    clientId: parseInt(args.client, 10),
    mode: args.mode,
    startDate: args.start,
    endDate: args.end,
    skipClassify,
  };
}

// ---------------------------------------------------------------------------
// Pre-flight checks
// ---------------------------------------------------------------------------
function checkDiskSpace() {
  try {
    const { execSync } = require('child_process');
    const out = execSync("df -BG --output=avail / 2>/dev/null | tail -1", { encoding: 'utf8' });
    const gb = parseInt(out.trim(), 10);
    if (!isNaN(gb) && gb < 2) {
      throw new Error(`Only ${gb}GB free disk space — need at least 2GB`);
    }
  } catch (e) {
    if (e.message.includes('free disk space')) throw e;
    // df not available (Windows dev) — skip check
  }
}

function readClientCredentials(clientId) {
  const mainDb = new Database(DB_PATH, { readonly: true });
  try {
    const row = mainDb.prepare('SELECT sticky_base_url, sticky_username, sticky_password FROM clients WHERE id = ?').get(clientId);
    if (!row) throw new Error(`Client ${clientId} not found in main DB`);
    return row;
  } finally {
    mainDb.close();
  }
}

// ---------------------------------------------------------------------------
// Send result to parent (or log if no IPC channel)
// ---------------------------------------------------------------------------
function sendResult(result) {
  if (typeof process.send === 'function') {
    process.send(result);
  } else {
    console.log('[import-worker] Result:', JSON.stringify(result));
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const { clientId, mode, startDate, endDate, skipClassify } = parseArgs();
  const startTime = Date.now();

  console.log(`[import-worker] Client ${clientId}, mode=${mode}, ${startDate} to ${endDate}${skipClassify ? ' (skip-classify)' : ''}`);

  // Pre-flight
  checkDiskSpace();
  if (!fs.existsSync(STAGING_DIR)) fs.mkdirSync(STAGING_DIR, { recursive: true });

  // Create staging DB
  const timestamp = Date.now();
  const stagingPath = path.join(STAGING_DIR, `client-${clientId}-${timestamp}.db`);
  console.log(`[import-worker] Staging DB: ${stagingPath}`);

  const stagingHelpers = createDbHelpers(stagingPath, { cacheSizeMB: 32 });
  initStagingSchema(stagingHelpers);

  // Read credentials from main DB (read-only, close immediately)
  const creds = readClientCredentials(clientId);

  // Build ingestion instance pointing at staging DB
  const ingestion = new DataIngestion(clientId, { dbHelpers: stagingHelpers });
  ingestion._mainDbPath = DB_PATH; // For filtering existing IDs during id_based import
  ingestion.initWithCredentials(creds);

  try {
    if (mode === 'transactions') {
      await ingestion.pullTransactions(startDate, endDate);
    } else if (mode === 'id_based') {
      await ingestion.pullTransactionsById(startDate, endDate);
    } else if (mode === 'updates') {
      await ingestion.pullStatusUpdates(startDate, endDate);
    } else {
      throw new Error(`Unknown mode: ${mode}`);
    }

    // Count what we got
    const count = stagingHelpers.queryOneSql(
      'SELECT COUNT(*) as cnt FROM orders WHERE client_id = ?', [clientId]
    );
    const stats = {
      ...ingestion.getStats(),
      stagingRows: count ? count.cnt : 0,
    };

    // --- Staging post-sync: classify on isolated DB before merge ---
    // VCT (--skip-classify): staging stays purely raw. Classification happens
    // on the main DB in Phase 4 of vct-daily-sync.js with full context.
    let stagingPostSync = null;
    if (skipClassify) {
      console.log('[import-worker] Skipping staging classification (--skip-classify)');
      stagingPostSync = { skipped: true };
    } else {
      try {
        const { runStagingPostSync } = require('../src/pipeline/staging-post-sync');
        stagingPostSync = runStagingPostSync(stagingHelpers, clientId);
      } catch (err) {
        console.error(`[import-worker] Staging post-sync failed (non-fatal): ${err.message}`);
        stagingPostSync = {
          success: false,
          fallbackRequired: true,
          classified: 0, rolesSet: 0, cascadesParsed: 0, gatewaysSet: 0,
          errors: ['staging_post_sync_crash'],
          errorMessage: err.message,
        };
      }
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[import-worker] Complete: ${stats.stagingRows} rows in ${elapsed}s`);

    stagingHelpers.closeDb();
    sendResult({
      status: 'success',
      stagingPath,
      stats,
      elapsed: parseFloat(elapsed),
      importSuccess: true,
      stagingPostSync,
    });
    process.exit(0);

  } catch (err) {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.error(`[import-worker] Failed after ${elapsed}s:`, err.message);
    stagingHelpers.closeDb();
    sendResult({
      status: 'error',
      error: err.message,
      stagingPath,
      elapsed: parseFloat(elapsed),
      importSuccess: false,
      stagingPostSync: null,
    });
    process.exit(1);
  }
}

main();
