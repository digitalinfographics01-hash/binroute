/**
 * Backfill missing orders for customers with broken ancestor/parent links.
 *
 * For each affected customer:
 *   1. Call customer_view to get their full order list
 *   2. Diff against what's already in our DB
 *   3. Fetch and save missing orders via order_view
 *
 * Checkpointed — safe to restart.
 */
const { initDb, runSql, querySql, checkpointWal } = require('../src/db/connection');
const DataIngestion = require('../src/api/ingestion');
const fs = require('fs');

const { execSync } = require('child_process');

const CLIENT_ID = 6;
const LOG_FILE = '/opt/binroute/logs/backfill-customer-orders.log';
const CHECKPOINT_FILE = '/opt/binroute/checkpoint-backfill-customer-orders.json';
const DB_FILE = require('path').join(__dirname, '..', 'data', 'binroute.db');
const WAL_FILE = DB_FILE + '-wal';
const THROTTLE_MS = 600; // ~100 req/min to stay under 120/min limit
const MAX_WAL_MB = 500;  // abort if WAL exceeds 500 MB
const MIN_DISK_GB = 5;   // abort if free disk drops below 5 GB

const sleep = ms => new Promise(r => setTimeout(r, ms));

function checkDiskSafety() {
  // Check WAL size
  try {
    const walStats = fs.statSync(WAL_FILE);
    const walMB = walStats.size / (1024 * 1024);
    if (walMB > MAX_WAL_MB) {
      log(`SAFETY ABORT: WAL is ${walMB.toFixed(0)} MB (limit ${MAX_WAL_MB} MB)`);
      process.exit(1);
    }
  } catch (e) {} // WAL may not exist yet

  // Check free disk space
  try {
    const dfOut = execSync('df --output=avail /opt/binroute 2>/dev/null || df /opt/binroute', { encoding: 'utf8' });
    const lines = dfOut.trim().split('\n');
    // Parse available KB from df output
    const match = lines[lines.length - 1].match(/(\d+)/);
    if (match) {
      const freeGB = parseInt(match[1]) / (1024 * 1024);
      if (freeGB < MIN_DISK_GB) {
        log(`SAFETY ABORT: Only ${freeGB.toFixed(1)} GB free disk (limit ${MIN_DISK_GB} GB)`);
        process.exit(1);
      }
    }
  } catch (e) {}
}

function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

async function main() {
  await initDb();

  const ingestion = new DataIngestion(CLIENT_ID);
  ingestion.init();

  // Find all customers whose orders reference ancestor_id or parent_id not in our DB
  log('Finding customers with broken ancestor/parent links...');
  const customers = querySql(`
    SELECT DISTINCT customer_id FROM orders
    WHERE client_id = ? AND customer_id IS NOT NULL AND (
      (ancestor_id IS NOT NULL AND ancestor_id != order_id
       AND ancestor_id NOT IN (SELECT order_id FROM orders WHERE client_id = ?))
      OR
      (parent_id IS NOT NULL AND parent_id != order_id
       AND parent_id NOT IN (SELECT order_id FROM orders WHERE client_id = ?))
    )
  `, [CLIENT_ID, CLIENT_ID, CLIENT_ID]).map(r => r.customer_id);

  log(`Found ${customers.length} customers needing backfill`);

  // Load checkpoint
  let checkpoint = { done: [], stats: { fetched: 0, saved: 0, skipped: 0, errors: 0 } };
  try { checkpoint = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf8')); } catch (e) {}
  if (!checkpoint.stats) checkpoint.stats = { fetched: 0, saved: 0, skipped: 0, errors: 0 };
  const doneSet = new Set(checkpoint.done);
  const pending = customers.filter(c => !doneSet.has(c));
  log(`${doneSet.size} already done, ${pending.length} remaining`);

  let { fetched, saved, skipped, errors } = checkpoint.stats;
  let consecutiveErrors = 0;

  for (let i = 0; i < pending.length; i++) {
    const cid = pending[i];

    try {
      // Step 1: customer_view to get full order list
      await sleep(THROTTLE_MS);
      const cv = await ingestion.client._post('customer_view', { customer_id: cid });

      if (!cv || cv.response_code !== '100' || !cv.order_list) {
        // No data — customer may not exist on this CRM instance
        checkpoint.done.push(cid);
        skipped++;
        consecutiveErrors = 0;
        if ((i + 1) % 100 === 0) saveCheckpoint();
        continue;
      }

      // Normalize order_list — API returns string for single-order customers
      const apiOrderIds = Array.isArray(cv.order_list)
        ? cv.order_list.map(String)
        : [String(cv.order_list)];

      // Step 2: diff against DB
      const existingIds = new Set(
        querySql('SELECT order_id FROM orders WHERE client_id = ? AND customer_id = ?', [CLIENT_ID, cid])
          .map(r => String(r.order_id))
      );
      const missing = apiOrderIds.filter(id => !existingIds.has(id));

      if (missing.length === 0) {
        checkpoint.done.push(cid);
        consecutiveErrors = 0;
        if ((i + 1) % 100 === 0) saveCheckpoint();
        continue;
      }

      // Step 3: fetch and save each missing order
      let customerSaved = 0;
      for (const oid of missing) {
        try {
          await sleep(THROTTLE_MS);
          const raw = await ingestion.client._post('order_view', { order_id: oid });
          if (!raw || !raw.order_id) { skipped++; continue; }
          fetched++;

          const order = ingestion.client.normalizeOrder(raw);
          ingestion._insertOrderSafe(order);
          saved++;
          customerSaved++;
        } catch (e) {
          // Individual order fetch failure — log and continue
          log(`  order ${oid} for customer ${cid}: ${e.message}`);
          errors++;
        }
      }

      if (customerSaved > 0) {
        log(`  customer ${cid}: ${customerSaved}/${missing.length} orders saved (had ${existingIds.size} in DB, API reports ${apiOrderIds.length})`);
      }

      checkpoint.done.push(cid);
      consecutiveErrors = 0;

    } catch (e) {
      errors++;
      consecutiveErrors++;
      log(`  customer ${cid}: ERROR ${e.message}`);

      if (consecutiveErrors >= 20) {
        log('Too many consecutive errors, pausing 60s...');
        await new Promise(r => setTimeout(r, 60000));
        consecutiveErrors = 0;
      }
    }

    // Progress + checkpoint + WAL safety every 100 customers
    if ((i + 1) % 100 === 0) {
      saveCheckpoint();
      checkpointWal();  // prevent WAL from growing unbounded
      log(`Progress: ${i + 1}/${pending.length} | fetched=${fetched} saved=${saved} skipped=${skipped} errors=${errors}`);
    }
  }

  saveCheckpoint();
  log(`\nDone! ${pending.length} customers processed. fetched=${fetched} saved=${saved} skipped=${skipped} errors=${errors}`);
  process.exit(0);

  function saveCheckpoint() {
    checkpoint.stats = { fetched, saved, skipped, errors };
    fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint));
  }
}

main().catch(e => { console.error(e); process.exit(1); });
