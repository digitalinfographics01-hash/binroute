#!/usr/bin/env node
/**
 * vct-daily-sync.js — 5-phase crash-resilient daily sync for VCT (client 6).
 *
 * Designed to be forked by the scheduler with its own 1GB heap:
 *   fork('scripts/vct-daily-sync.js', { execArgv: ['--max-old-space-size=1024'] })
 *
 * Phases:
 *   1. Pre-flight  — backup DB, check disk, clean stale staging
 *   2. Import      — new orders via import-worker (id_based, --skip-classify)
 *   3. Updates     — status updates via import-worker (updates, --skip-classify)
 *   4. Classify    — derived_product_role + cascade chains on main DB
 *   5. Post-sync   — COGS, ad spend, P&L/approval/product/cohort caches
 *
 * Progress tracked in data/vct-sync-progress.json. On crash+restart,
 * resumes from the last completed phase.
 */

const fs = require('fs');
const path = require('path');
const { fork, execSync } = require('child_process');
const Database = require('better-sqlite3');

const ROOT = path.join(__dirname, '..');
const DB_PATH = path.join(ROOT, 'data', 'binroute.db');
const PROGRESS_PATH = path.join(ROOT, 'data', 'vct-sync-progress.json');
const BACKUP_DIR = path.join(ROOT, 'data', 'backups');
const STAGING_DIR = path.join(ROOT, 'data', 'staging');
const LOG_DIR = path.join(ROOT, 'data', 'sync-logs');
const WORKER_PATH = path.join(ROOT, 'scripts', 'import-worker.js');

const CLIENT_ID = 6;
const IMPORT_DAY_WINDOW = 15;
const UPDATE_DAY_WINDOW = 5;
const MAX_BACKUPS = 3;

// ---------------------------------------------------------------------------
// Logging — all output goes to sync log file + stdout
// ---------------------------------------------------------------------------
let logStream = null;

function initLog() {
  if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });
  const today = new Date().toISOString().slice(0, 10);
  const logPath = path.join(LOG_DIR, `vct-${today}.log`);
  logStream = fs.createWriteStream(logPath, { flags: 'a' });
  return logPath;
}

function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  if (logStream) logStream.write(line + '\n');
}

function logError(msg) {
  const line = `[${new Date().toISOString()}] ERROR: ${msg}`;
  console.error(line);
  if (logStream) logStream.write(line + '\n');
}

// ---------------------------------------------------------------------------
// Progress file management
// ---------------------------------------------------------------------------
function readProgress() {
  try {
    if (fs.existsSync(PROGRESS_PATH)) {
      return JSON.parse(fs.readFileSync(PROGRESS_PATH, 'utf8'));
    }
  } catch {}
  return null;
}

function writeProgress(phase, data = {}) {
  const existing = readProgress() || {
    date: today(),
    startedAt: new Date().toISOString(),
    attempt: 0,
    phases: {},
  };
  existing.phase = phase;
  existing.phases[phase] = { completedAt: new Date().toISOString(), ...data };
  fs.writeFileSync(PROGRESS_PATH, JSON.stringify(existing, null, 2));
}

function initProgress() {
  const progress = readProgress();
  const attempt = (progress && progress.date === today()) ? (progress.attempt || 0) + 1 : 1;
  const data = {
    date: today(),
    startedAt: new Date().toISOString(),
    phase: 'started',
    attempt,
    phases: progress && progress.date === today() ? progress.phases : {},
    log: path.relative(ROOT, path.join(LOG_DIR, `vct-${today()}.log`)),
  };
  fs.writeFileSync(PROGRESS_PATH, JSON.stringify(data, null, 2));
  return data;
}

function phaseComplete(phase) {
  const progress = readProgress();
  if (!progress || progress.date !== today()) return false;
  return progress.phases && progress.phases[phase] && progress.phases[phase].completedAt;
}

function today() {
  return new Date().toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------
function formatDate(d) {
  return `${String(d.getMonth() + 1).padStart(2, '0')}/${String(d.getDate()).padStart(2, '0')}/${d.getFullYear()}`;
}

function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d;
}

function checkDiskSpace() {
  try {
    const out = execSync("df -BG --output=avail / 2>/dev/null | tail -1", { encoding: 'utf8' });
    const gb = parseInt(out.trim(), 10);
    if (!isNaN(gb) && gb < 5) {
      throw new Error(`Only ${gb}GB free disk space — need at least 5GB`);
    }
    log(`Disk space: ${gb}GB available`);
  } catch (e) {
    if (e.message.includes('free disk space')) throw e;
  }
}

// ---------------------------------------------------------------------------
// Phase 1: Pre-flight
// ---------------------------------------------------------------------------
async function phase1Preflight() {
  if (phaseComplete('preflight')) {
    log('Phase 1 (pre-flight): already done, skipping');
    return;
  }

  log('=== Phase 1: Pre-flight ===');

  // 1b. DB backup via SQLite .backup API
  if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive: true });
  const backupPath = path.join(BACKUP_DIR, `binroute-${today()}.db`);

  if (!fs.existsSync(backupPath)) {
    log('Creating DB backup...');
    const sourceDb = new Database(DB_PATH, { readonly: true });
    try {
      await sourceDb.backup(backupPath);
      log(`Backup created: ${backupPath}`);

      // Verify backup integrity
      const backupDb = new Database(backupPath, { readonly: true });
      try {
        const result = backupDb.pragma('integrity_check');
        const ok = result && result.length === 1 && result[0].integrity_check === 'ok';
        if (!ok) {
          throw new Error(`Backup integrity check failed: ${JSON.stringify(result)}`);
        }
        log('Backup integrity check: ok');
      } finally {
        backupDb.close();
      }
    } finally {
      sourceDb.close();
    }
  } else {
    log(`Backup already exists for today: ${backupPath}`);
  }

  // Clean old backups (keep last MAX_BACKUPS)
  const backups = fs.readdirSync(BACKUP_DIR)
    .filter(f => f.startsWith('binroute-') && f.endsWith('.db'))
    .sort()
    .reverse();
  for (const old of backups.slice(MAX_BACKUPS)) {
    fs.unlinkSync(path.join(BACKUP_DIR, old));
    log(`Deleted old backup: ${old}`);
  }

  // 1c. Disk space check
  checkDiskSpace();

  // 1d. Clean stale staging files for client 6
  if (fs.existsSync(STAGING_DIR)) {
    const now = Date.now();
    const MAX_AGE = 24 * 60 * 60 * 1000;
    for (const file of fs.readdirSync(STAGING_DIR)) {
      if (file.startsWith('.')) continue;
      if (!file.startsWith('client-6-')) continue;
      try {
        const fp = path.join(STAGING_DIR, file);
        const stat = fs.statSync(fp);
        if (now - stat.mtimeMs > MAX_AGE) {
          fs.unlinkSync(fp);
          log(`Cleaned stale staging file: ${file}`);
        }
      } catch {}
    }
  }

  // 1e. Clear stale sync locks for client 6
  const lockPath = path.join(STAGING_DIR, '.sync-lock-client-6');
  if (fs.existsSync(lockPath)) {
    fs.unlinkSync(lockPath);
    log('Cleared stale sync lock for client 6');
  }

  writeProgress('preflight', { backupPath });
  log('Phase 1 complete');
}

// ---------------------------------------------------------------------------
// Phase 2 & 3: Import / Status Updates (via import-worker child process)
// ---------------------------------------------------------------------------
function spawnImportWorker(mode, startDate, endDate) {
  const timeoutMs = mode === 'updates' ? 18 * 3600000 : 3 * 3600000;

  return new Promise((resolve, reject) => {
    const child = fork(WORKER_PATH, [
      `--client=${CLIENT_ID}`,
      `--mode=${mode}`,
      `--start=${startDate}`,
      `--end=${endDate}`,
      '--skip-classify',
    ], {
      execArgv: ['--max-old-space-size=512'],
      stdio: ['pipe', 'inherit', 'inherit', 'ipc'],
    });

    let result = null;
    let settled = false;

    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        logError(`Worker timeout: mode=${mode} after ${timeoutMs / 3600000}h — killing`);
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
          result && result.error ? result.error : `Worker exited with code ${code}`
        ));
      }
    });
  });
}

function mergeStaging(stagingPath) {
  // Import merge function fresh (uses main DB singleton)
  const { initDb } = require('../src/db/connection');
  const { mergeStagingToMain } = require('../src/db/staging-merge');
  initDb();
  return mergeStagingToMain(stagingPath, CLIENT_ID);
}

function walCheckpoint() {
  try {
    const { checkpointWal } = require('../src/db/connection');
    checkpointWal();
  } catch {}
}

async function phase2Import() {
  if (phaseComplete('import')) {
    log('Phase 2 (import): already done, skipping');
    return;
  }

  log('=== Phase 2: Import new orders ===');
  const startDate = formatDate(daysAgo(IMPORT_DAY_WINDOW));
  const endDate = formatDate(new Date());

  const result = await spawnImportWorker('id_based', startDate, endDate);
  const rows = result.stats?.stagingRows || 0;
  log(`Import done: ${rows} rows in ${result.elapsed}s`);

  if (rows > 0) {
    log('Merging staging into main DB...');
    const mergeResult = mergeStaging(result.stagingPath);
    log(`Merge: ${mergeResult.inserted} new, ${mergeResult.updated} updated in ${mergeResult.elapsed}s`);
    walCheckpoint();
  }

  writeProgress('import', { rows, elapsed: result.elapsed });
  log('Phase 2 complete');
}

async function phase3Updates() {
  if (phaseComplete('updates')) {
    log('Phase 3 (updates): already done, skipping');
    return;
  }

  log('=== Phase 3: Status updates ===');
  const startDate = formatDate(daysAgo(UPDATE_DAY_WINDOW));
  const endDate = formatDate(new Date());

  const result = await spawnImportWorker('updates', startDate, endDate);
  const rows = result.stats?.stagingRows || 0;
  log(`Updates done: ${rows} rows in ${result.elapsed}s`);

  if (rows > 0) {
    log('Merging status updates into main DB...');
    const mergeResult = mergeStaging(result.stagingPath);
    log(`Merge: ${mergeResult.inserted} new, ${mergeResult.updated} updated in ${mergeResult.elapsed}s`);
    walCheckpoint();
  }

  writeProgress('updates', { rows, elapsed: result.elapsed });
  log('Phase 3 complete');
}

// ---------------------------------------------------------------------------
// Phase 4: Classify on main DB
// ---------------------------------------------------------------------------
async function phase4Classify() {
  if (phaseComplete('classify')) {
    log('Phase 4 (classify): already done, skipping');
    return;
  }

  log('=== Phase 4: Classify on main DB ===');

  // Use existing classifiers from post-sync-vct.js
  const { initDb } = require('../src/db/connection');
  const { classifyVctOrders, parseCascadeChains } = require('../src/pipeline/post-sync-vct');
  initDb();

  const classified = classifyVctOrders(CLIENT_ID);
  log(`Classified ${classified} orders`);

  walCheckpoint();

  const cascades = parseCascadeChains(CLIENT_ID);
  log(`Parsed ${cascades} cascade chains`);

  walCheckpoint();

  writeProgress('classify', { classified, cascades });
  log('Phase 4 complete');
}

// ---------------------------------------------------------------------------
// Phase 5: Post-sync caches
// ---------------------------------------------------------------------------
async function phase5PostSync() {
  if (phaseComplete('postsync')) {
    log('Phase 5 (post-sync): already done, skipping');
    return;
  }

  log('=== Phase 5: Post-sync caches ===');

  const cacheSteps = [
    { name: 'cogs', label: 'COGS import', cmd: 'node scripts/import-daily-cogs.js', timeout: 120000 },
    { name: 'adspend', label: 'Ad Spend import', cmd: 'node scripts/import-ad-spend.js', timeout: 120000 },
    { name: 'pnl', label: 'P&L cache', cmd: 'node scripts/compute-pnl-cache.js --days 90', timeout: 300000 },
    { name: 'approval', label: 'Approval cache', cmd: 'node scripts/compute-approval-cache.js --days 90', timeout: 300000 },
    { name: 'product_lifetime', label: 'Product lifetime cache', cmd: 'node scripts/compute-product-lifetime-cache.js --months 7', timeout: 600000 },
    { name: 'cohort', label: 'Cohort cache', cmd: 'node scripts/compute-cohort-cache.js --months 7', timeout: 300000 },
  ];

  const progress = readProgress();
  const cacheProgress = (progress && progress.phases && progress.phases.postsync_partial) || {};

  for (const step of cacheSteps) {
    if (cacheProgress[step.name]) {
      log(`  ${step.label}: already done, skipping`);
      continue;
    }

    try {
      log(`  ${step.label}: starting...`);
      execSync(step.cmd, { cwd: ROOT, timeout: step.timeout, stdio: 'inherit' });
      log(`  ${step.label}: done`);

      // Track per-cache progress
      cacheProgress[step.name] = true;
      writeProgress('postsync_partial', cacheProgress);
    } catch (err) {
      logError(`${step.label} failed: ${err.message}`);
      // Non-fatal — continue with other caches
    }
  }

  writeProgress('postsync', cacheProgress);
  log('Phase 5 complete');
}

// ---------------------------------------------------------------------------
// Main orchestrator
// ---------------------------------------------------------------------------
async function main() {
  const logPath = initLog();
  const progress = initProgress();

  log(`=== VCT DAILY SYNC START (attempt ${progress.attempt}) ===`);
  log(`Log: ${logPath}`);

  const startTime = Date.now();

  try {
    await phase1Preflight();
    await phase2Import();
    await phase3Updates();
    await phase4Classify();
    await phase5PostSync();

    writeProgress('complete');
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    log(`=== VCT DAILY SYNC COMPLETE in ${elapsed}s ===`);

    // Report to parent if forked
    if (typeof process.send === 'function') {
      process.send({ status: 'success', elapsed: parseFloat(elapsed) });
    }
    process.exit(0);
  } catch (err) {
    logError(`SYNC FAILED: ${err.message}`);
    if (typeof process.send === 'function') {
      process.send({ status: 'error', error: err.message });
    }
    process.exit(1);
  }
}

main();
