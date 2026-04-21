/**
 * Concurrent cascade chain backfill from Sticky.io order notes
 *
 * Spawns N workers, each processing a segment of the remaining orders.
 * Orders are processed newest-first (DESC).
 *
 * Usage:
 *   node scripts/backfill-cascade-concurrent.js --client=6 --workers=3     — launch 3 workers
 *   node scripts/backfill-cascade-concurrent.js --client=6 --worker=0 --of=3  — run as worker 0 of 3
 *   node scripts/backfill-cascade-concurrent.js --client=6 plan            — show counts
 */

const fs = require('fs');
const path = require('path');
const { fork } = require('child_process');
const { initializeDatabase } = require('../src/db/schema');
const { initDb, querySql, runSql, saveDb, closeDb, checkpointWal } = require('../src/db/connection');
const StickyClient = require('../src/api/sticky-client');

// ──────────────────────────────────────────────
// Parse args
// ──────────────────────────────────────────────

function parseArg(name, fallback) {
  const arg = process.argv.find(a => a.startsWith(`--${name}=`));
  return arg ? parseInt(arg.split('=')[1], 10) : fallback;
}

const CLIENT_ID = parseArg('client', 1);
const WORKER_ID = parseArg('worker', -1);       // -1 = orchestrator
const WORKER_COUNT = parseArg('of', -1) !== -1 ? parseArg('of', 5) : parseArg('workers', 5);
const BATCH_SIZE = 100;
const SAVE_EVERY = 500;
const IS_WORKER = WORKER_ID >= 0;
const IS_PLAN = process.argv.includes('plan');

// Rate limiting: with 5 workers × 100 batch, we stagger starts and add a
// cooldown between batches so total API load stays manageable.
// Target: ~500 req/min total across all workers (100 req/min per worker).
const WORKER_STAGGER_MS = 3000;          // 3s between worker launches
const BATCH_COOLDOWN_MS = 6000;          // 6s pause between batches per worker
                                          // 100 calls / 6s = ~1000/min per worker
                                          // but order_view takes ~2-4s itself, so
                                          // effective rate is much lower

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function checkpointPath(workerId) {
  return path.join(__dirname, '..', `checkpoint-cascade-concurrent-c${CLIENT_ID}-w${workerId}.json`);
}

// ──────────────────────────────────────────────
// Parse cascade chain from systemNotes
// ──────────────────────────────────────────────

function parseCascadeChain(systemNotes) {
  if (!systemNotes || !Array.isArray(systemNotes)) return null;

  const chain = [];

  for (const note of systemNotes) {
    const initialMatch = note.match(/Order attempted to process on gateway \((\d+)\) and declined due to (.+?), and cascade gateway id \((\d+)\)/);
    if (initialMatch) {
      if (!chain.find(c => c.gateway_id === parseInt(initialMatch[1]) && c.attempt === 0)) {
        chain.push({
          gateway_id: parseInt(initialMatch[1]),
          decline_reason: initialMatch[2].trim(),
          attempt: 0,
          role: 'initial',
        });
      }
      continue;
    }

    const cascadeMatch = note.match(/Cascade gateway id \((\d+)\) also declined the sale due to (.+?) \((\d+)(?:st|nd|rd|th) attempt\)/);
    if (cascadeMatch) {
      chain.push({
        gateway_id: parseInt(cascadeMatch[1]),
        decline_reason: cascadeMatch[2].trim(),
        attempt: parseInt(cascadeMatch[3]),
        role: 'cascade',
      });
      continue;
    }

    const declinedByMatch = note.match(/Declined by cascade gateway: \((\d+)\) (.+?) \((\d+)(?:st|nd|rd|th) attempt\)/);
    if (declinedByMatch) {
      chain.push({
        gateway_id: parseInt(declinedByMatch[1]),
        decline_reason: declinedByMatch[2].trim(),
        attempt: parseInt(declinedByMatch[3]),
        role: 'cascade',
      });
      continue;
    }

    const finalDeclineMatch = note.match(/Declined by Payment Gateway \((.+?)\)/);
    if (finalDeclineMatch) {
      if (chain.length > 0) {
        chain[chain.length - 1].final = true;
      }
      continue;
    }

    const reprocessMatch = note.match(/Reprocess attempt #(\d+), previous gateway id was (\d+)/);
    if (reprocessMatch) {
      chain.push({
        gateway_id: parseInt(reprocessMatch[2]),
        decline_reason: null,
        attempt: parseInt(reprocessMatch[1]),
        role: 'reprocess_from',
      });
      continue;
    }

    const forceBillMatch = note.match(/Force bill failed by payment gateway \((.+?)\)/);
    if (forceBillMatch) {
      if (chain.length > 0 && !chain[chain.length - 1].decline_reason) {
        chain[chain.length - 1].decline_reason = forceBillMatch[1].trim();
      }
      continue;
    }
  }

  return chain.length > 0 ? chain : null;
}

// ──────────────────────────────────────────────
// Ensure cascade_chain column exists
// ──────────────────────────────────────────────

function ensureColumn() {
  try {
    runSql('ALTER TABLE orders ADD COLUMN cascade_chain TEXT DEFAULT NULL');
    console.log('  Added cascade_chain column to orders table');
  } catch (e) {
    if (!e.message.includes('duplicate column')) throw e;
  }
}

// ──────────────────────────────────────────────
// Checkpoint
// ──────────────────────────────────────────────

function loadCheckpoint(workerId) {
  const f = checkpointPath(workerId);
  if (fs.existsSync(f)) {
    return JSON.parse(fs.readFileSync(f, 'utf8'));
  }
  return { processed: 0, updated: 0, skipped: 0, errors: 0 };
}

function saveCheckpointFile(workerId, cp) {
  cp.timestamp = new Date().toISOString();
  fs.writeFileSync(checkpointPath(workerId), JSON.stringify(cp, null, 2));
}

// ──────────────────────────────────────────────
// Orchestrator — spawns N workers
// ──────────────────────────────────────────────

async function orchestrate() {
  await initializeDatabase();
  ensureColumn();

  const remaining = querySql(`
    SELECT COUNT(*) as cnt FROM orders
    WHERE is_cascaded = 1 AND client_id = ${CLIENT_ID} AND cascade_chain IS NULL
  `)[0].cnt;

  const total = querySql(`
    SELECT COUNT(*) as cnt FROM orders WHERE is_cascaded = 1 AND client_id = ${CLIENT_ID}
  `)[0].cnt;

  console.log('='.repeat(70));
  console.log(`Cascade Chain Backfill — Client ${CLIENT_ID} (Concurrent)`);
  console.log('='.repeat(70));
  console.log(`  Total cascaded orders: ${total}`);
  console.log(`  Already done: ${total - remaining}`);
  console.log(`  Remaining: ${remaining}`);
  console.log(`  Workers: ${WORKER_COUNT}`);
  console.log(`  Orders per worker: ~${Math.ceil(remaining / WORKER_COUNT)}`);
  console.log(`  Batch size: ${BATCH_SIZE}`);
  console.log(`  Direction: newest first (DESC)`);

  if (IS_PLAN) {
    process.exit(0);
  }

  closeDb();

  console.log(`\n  Launching ${WORKER_COUNT} workers (staggered ${WORKER_STAGGER_MS}ms apart)...\n`);

  const workers = [];
  for (let i = 0; i < WORKER_COUNT; i++) {
    if (i > 0) await sleep(WORKER_STAGGER_MS);

    const child = fork(__filename, [
      `--client=${CLIENT_ID}`,
      `--worker=${i}`,
      `--of=${WORKER_COUNT}`,
    ], {
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    });

    // Prefix output with worker ID
    const prefix = `  [W${i}] `;
    child.stdout.on('data', (data) => {
      const lines = data.toString().split('\n').filter(l => l.trim());
      lines.forEach(l => process.stdout.write(prefix + l + '\n'));
    });
    child.stderr.on('data', (data) => {
      const lines = data.toString().split('\n').filter(l => l.trim());
      lines.forEach(l => process.stderr.write(prefix + 'ERR: ' + l + '\n'));
    });

    child.on('exit', (code) => {
      console.log(`${prefix}exited with code ${code}`);
    });

    workers.push(child);
    console.log(`  Worker ${i} launched`);
  }

  // Wait for all workers to finish
  await Promise.all(workers.map(w => new Promise(resolve => w.on('exit', resolve))));

  console.log('\n' + '='.repeat(70));
  console.log('ALL WORKERS COMPLETE');
  console.log('='.repeat(70));

  // Print combined stats
  let totalProcessed = 0, totalUpdated = 0, totalSkipped = 0, totalErrors = 0;
  for (let i = 0; i < WORKER_COUNT; i++) {
    const cp = loadCheckpoint(i);
    totalProcessed += cp.processed;
    totalUpdated += cp.updated;
    totalSkipped += cp.skipped;
    totalErrors += cp.errors;
    console.log(`  Worker ${i}: ${cp.processed} processed, ${cp.updated} updated, ${cp.errors} errors`);
  }
  console.log(`  TOTAL: ${totalProcessed} processed, ${totalUpdated} updated, ${totalSkipped} empty, ${totalErrors} errors`);
}

// ──────────────────────────────────────────────
// Worker — processes its segment
// ──────────────────────────────────────────────

async function runWorker() {
  // Stagger DB open so workers don't all contend on WAL at once
  await sleep(WORKER_ID * 2000);

  // Workers only open the DB — schema init + ensureColumn already done by orchestrator
  await initDb();

  // Get ALL remaining order IDs, newest first
  const allOrders = querySql(`
    SELECT order_id FROM orders
    WHERE is_cascaded = 1 AND client_id = ${CLIENT_ID} AND cascade_chain IS NULL
    ORDER BY order_id DESC
  `);
  const allIds = allOrders.map(r => r.order_id);

  // Split into segments — this worker takes every Nth slice
  const myIds = [];
  const segmentSize = Math.ceil(allIds.length / WORKER_COUNT);
  const start = WORKER_ID * segmentSize;
  const end = Math.min(start + segmentSize, allIds.length);
  for (let i = start; i < end; i++) {
    myIds.push(allIds[i]);
  }

  console.log(`Segment: ${myIds.length} orders (${myIds[0]} → ${myIds[myIds.length - 1]})`);

  // Load client credentials
  const row = querySql(`SELECT * FROM clients WHERE id = ${CLIENT_ID}`)[0];
  const client = new StickyClient({
    baseUrl: row.sticky_base_url,
    username: row.sticky_username,
    password: row.sticky_password,
  });

  // Load checkpoint
  const cp = loadCheckpoint(WORKER_ID);
  const startFrom = cp.processed;
  if (startFrom > 0) {
    console.log(`Resuming from position ${startFrom}`);
  }

  // Stagger worker start so they don't all hit the API at once
  await sleep(WORKER_ID * WORKER_STAGGER_MS);

  const startTime = Date.now();
  let consecutive429s = 0;

  for (let i = startFrom; i < myIds.length; i += BATCH_SIZE) {
    const batch = myIds.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(myIds.length / BATCH_SIZE);

    // Parallel order_view calls with 429 retry
    const results = await Promise.all(
      batch.map(async (orderId) => {
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            const data = await client.orderView(orderId);
            return { orderId, data, error: null };
          } catch (err) {
            if (err.message && err.message.includes('429') && attempt < 2) {
              await sleep(5000 * (attempt + 1));   // 5s, 10s backoff
              continue;
            }
            return { orderId, data: null, error: err.message };
          }
        }
      })
    );

    // Track 429 pressure — if too many, slow down
    const got429 = results.filter(r => r.error && r.error.includes('429')).length;
    if (got429 > 0) {
      consecutive429s++;
      const backoff = Math.min(consecutive429s * 5000, 30000);
      console.log(`429 detected (${got429}/${batch.length}), backing off ${backoff / 1000}s`);
      await sleep(backoff);
    } else {
      consecutive429s = 0;
    }

    let batchUpdated = 0;
    let batchSkipped = 0;
    let batchErrors = 0;

    for (const { orderId, data, error } of results) {
      if (error) {
        batchErrors++;
        cp.errors++;
        continue;
      }

      const chain = parseCascadeChain(data.systemNotes);

      if (chain && chain.length > 0) {
        runSql(
          `UPDATE orders SET cascade_chain = ? WHERE order_id = ? AND client_id = ${CLIENT_ID}`,
          [JSON.stringify(chain), orderId]
        );
        batchUpdated++;
        cp.updated++;
      } else {
        runSql(
          `UPDATE orders SET cascade_chain = ? WHERE order_id = ? AND client_id = ${CLIENT_ID}`,
          ['[]', orderId]
        );
        batchSkipped++;
        cp.skipped++;
      }
    }

    cp.processed = i + batch.length;

    const elapsed = (Date.now() - startTime) / 1000;
    const done = cp.processed - startFrom;
    const rate = done / elapsed;
    const remaining = rate > 0 ? Math.ceil((myIds.length - cp.processed) / rate) : 0;
    const remainMin = Math.floor(remaining / 60);
    const remainHr = (remaining / 3600).toFixed(1);

    console.log(`[${batchNum}/${totalBatches}] ${batchUpdated} ok, ${batchSkipped} empty, ${batchErrors} err | ${remainHr}h left`);

    // Save checkpoint + WAL checkpoint periodically
    if (cp.processed % SAVE_EVERY === 0 || i + BATCH_SIZE >= myIds.length) {
      saveCheckpointFile(WORKER_ID, cp);
    }
    if (cp.processed % 1000 === 0) {
      checkpointWal();
    }

    // Cooldown between batches to avoid API overload
    if (i + BATCH_SIZE < myIds.length) {
      await sleep(BATCH_COOLDOWN_MS);
    }
  }

  // Final
  saveCheckpointFile(WORKER_ID, cp);
  checkpointWal();

  const totalTime = Math.round((Date.now() - startTime) / 1000);
  console.log(`DONE — ${cp.processed} processed, ${cp.updated} updated, ${cp.skipped} empty, ${cp.errors} errors in ${Math.floor(totalTime / 60)}m`);

  closeDb();
}

// ──────────────────────────────────────────────
// Entry
// ──────────────────────────────────────────────

if (IS_WORKER) {
  runWorker().catch(e => {
    console.error('Fatal:', e.message);
    process.exit(1);
  });
} else {
  orchestrate().catch(e => {
    console.error('Fatal:', e.message);
    process.exit(1);
  });
}
