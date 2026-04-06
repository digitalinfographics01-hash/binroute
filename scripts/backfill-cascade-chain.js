/**
 * Backfill cascade chain from Sticky.io order notes
 *
 * Calls order_view on all cascaded orders, parses systemNotes to extract
 * the full cascade chain (every gateway tried + decline reason), and stores
 * it in the cascade_chain column as JSON.
 *
 * Usage:
 *   node scripts/backfill-cascade-chain.js plan              — show counts for client 1
 *   node scripts/backfill-cascade-chain.js run               — start backfill for client 1
 *   node scripts/backfill-cascade-chain.js resume            — resume for client 1
 *   node scripts/backfill-cascade-chain.js plan --client=3   — show counts for client 3
 *   node scripts/backfill-cascade-chain.js run --client=3    — start backfill for client 3
 *   node scripts/backfill-cascade-chain.js resume --client=3 — resume for client 3
 */

const fs = require('fs');
const path = require('path');
const { initializeDatabase } = require('../src/db/schema');
const { querySql, runSql, saveDb, closeDb, checkpointWal } = require('../src/db/connection');
const StickyClient = require('../src/api/sticky-client');

// Parse --client=N from args
const CLIENT_ID = (() => {
  const arg = process.argv.find(a => a.startsWith('--client='));
  return arg ? parseInt(arg.split('=')[1], 10) : 1;
})();
const CHECKPOINT_FILE = path.join(__dirname, '..', `checkpoint-cascade-chain-client${CLIENT_ID}.json`);
const BATCH_SIZE = 50;         // Orders per batch (parallel order_view calls)
const SAVE_EVERY = 200;       // Save DB + checkpoint every N orders

// ──────────────────────────────────────────────
// Parse cascade chain from systemNotes
// ──────────────────────────────────────────────

function parseCascadeChain(systemNotes) {
  if (!systemNotes || !Array.isArray(systemNotes)) return null;

  const chain = [];

  for (const note of systemNotes) {
    // Pattern 1: "Order attempted to process on gateway (188) and declined due to Issuer Declined, and cascade gateway id (191) also declined"
    const initialMatch = note.match(/Order attempted to process on gateway \((\d+)\) and declined due to (.+?), and cascade gateway id \((\d+)\)/);
    if (initialMatch) {
      // Only add initial gateway if not already in chain
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

    // Pattern 2: "Cascade gateway id (188) also declined the sale due to Issuer Declined (1st attempt)"
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

    // Pattern 3: "Declined by cascade gateway: (172) Pick up card - SF (2nd attempt)"
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

    // Pattern 4: "Declined by Payment Gateway (Do Not Honor)" — final decline on last cascade
    const finalDeclineMatch = note.match(/Declined by Payment Gateway \((.+?)\)/);
    if (finalDeclineMatch) {
      // This is the final decline — update the last chain entry if it doesn't have a reason
      // Or just note it as the final outcome
      if (chain.length > 0) {
        chain[chain.length - 1].final = true;
      }
      continue;
    }

    // Pattern 5: Reprocess attempts — "Reprocess attempt #1, previous gateway id was 172"
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

    // Pattern 6: "Force bill failed by payment gateway (Pick up card - SF)"
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
    // Column already exists
  }
}

// ──────────────────────────────────────────────
// Load/save checkpoint
// ──────────────────────────────────────────────

function loadCheckpoint() {
  if (fs.existsSync(CHECKPOINT_FILE)) {
    return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf8'));
  }
  return { processed: 0, updated: 0, skipped: 0, errors: 0, lastOrderId: null };
}

function saveCheckpoint(cp) {
  cp.timestamp = new Date().toISOString();
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(cp, null, 2));
}

// ──────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────

async function main() {
  const mode = process.argv[2] || 'plan';
  await initializeDatabase();
  ensureColumn();

  // Get all cascaded order IDs that don't have cascade_chain yet
  const allOrders = querySql(`
    SELECT order_id FROM orders
    WHERE is_cascaded = 1 AND client_id = ${CLIENT_ID} AND cascade_chain IS NULL
    ORDER BY order_id ASC
  `);
  const orderIds = allOrders.map(r => r.order_id);

  const totalCascaded = querySql(`SELECT COUNT(*) as cnt FROM orders WHERE is_cascaded = 1 AND client_id = ${CLIENT_ID}`)[0].cnt;
  const alreadyDone = totalCascaded - orderIds.length;

  console.log('='.repeat(70));
  console.log(`Cascade Chain Backfill — Client ${CLIENT_ID}`);
  console.log('='.repeat(70));
  console.log(`  Total cascaded orders (Kytsan): ${totalCascaded}`);
  console.log(`  Already backfilled: ${alreadyDone}`);
  console.log(`  Remaining: ${orderIds.length}`);
  console.log(`  Batch size: ${BATCH_SIZE} (parallel order_view calls)`);
  console.log(`  Estimated time: ~${Math.ceil(orderIds.length / 120)} minutes (120 req/min)`);

  if (mode === 'plan') {
    console.log('\n  Run with "run" to start, "resume" to resume from checkpoint.');
    process.exit(0);
  }

  // Load client credentials
  const row = querySql(`SELECT * FROM clients WHERE id = ${CLIENT_ID}`)[0];
  const client = new StickyClient({
    baseUrl: row.sticky_base_url,
    username: row.sticky_username,
    password: row.sticky_password,
  });

  // Load or reset checkpoint
  const cp = mode === 'resume' ? loadCheckpoint() : { processed: 0, updated: 0, skipped: 0, errors: 0, lastOrderId: null };
  const startFrom = cp.processed;

  console.log(`\n  ${mode === 'resume' ? 'RESUMING from order #' + startFrom : 'STARTING'}`);
  console.log();

  const updateStmt = runSql; // Using runSql for updates

  const startTime = Date.now();

  for (let i = startFrom; i < orderIds.length; i += BATCH_SIZE) {
    const batch = orderIds.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(orderIds.length / BATCH_SIZE);

    process.stdout.write(`  [${batchNum}/${totalBatches}] Orders ${batch[0]}-${batch[batch.length - 1]}... `);

    // Parallel order_view calls
    const results = await Promise.all(
      batch.map(orderId =>
        client.orderView(orderId)
          .then(data => ({ orderId, data, error: null }))
          .catch(err => ({ orderId, data: null, error: err.message }))
      )
    );

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
        // No cascade info found in notes — mark as empty array so we don't re-fetch
        runSql(
          `UPDATE orders SET cascade_chain = ? WHERE order_id = ? AND client_id = ${CLIENT_ID}`,
          ['[]', orderId]
        );
        batchSkipped++;
        cp.skipped++;
      }
    }

    cp.processed = i + batch.length;
    cp.lastOrderId = batch[batch.length - 1];

    const elapsed = (Date.now() - startTime) / 1000;
    const rate = cp.processed / elapsed;
    const remaining = Math.ceil((orderIds.length - cp.processed) / rate);

    console.log(`${batchUpdated} updated, ${batchSkipped} empty, ${batchErrors} err | ${remaining}s left`);

    // Save DB + checkpoint periodically
    if (cp.processed % SAVE_EVERY === 0 || i + BATCH_SIZE >= orderIds.length) {
      saveDb();
      saveCheckpoint(cp);
    }
    // Periodic WAL checkpoint every 500 orders to keep WAL size manageable
    if (cp.processed % 500 === 0) {
      checkpointWal();
    }
  }

  // Final save
  saveDb();
  saveCheckpoint(cp);

  const totalTime = Math.round((Date.now() - startTime) / 1000);
  console.log('\n' + '='.repeat(70));
  console.log('BACKFILL COMPLETE');
  console.log('='.repeat(70));
  console.log(`  Processed: ${cp.processed}`);
  console.log(`  Updated with chain: ${cp.updated}`);
  console.log(`  Empty/no cascade notes: ${cp.skipped}`);
  console.log(`  Errors: ${cp.errors}`);
  console.log(`  Time: ${Math.floor(totalTime / 60)}m ${totalTime % 60}s`);

  // Verify
  const withChain = querySql(`SELECT COUNT(*) as cnt FROM orders WHERE cascade_chain IS NOT NULL AND cascade_chain != '[]' AND client_id = ${CLIENT_ID}`)[0].cnt;
  const avgLen = querySql(`
    SELECT AVG(json_array_length(cascade_chain)) as avg_len
    FROM orders WHERE cascade_chain IS NOT NULL AND cascade_chain != '[]' AND client_id = ${CLIENT_ID}
  `)[0].avg_len;
  console.log(`\n  Orders with cascade chain data: ${withChain}`);
  console.log(`  Average chain length: ${avgLen ? avgLen.toFixed(1) : 'N/A'} gateways`);

  closeDb();
}

main().catch(e => {
  console.error('Fatal:', e.message);
  saveDb();
  process.exit(1);
});
