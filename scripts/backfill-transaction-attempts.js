#!/usr/bin/env node
/**
 * Backfill transaction_attempts — explodes orders into per-gateway-attempt rows.
 *
 * Usage:
 *   node scripts/backfill-transaction-attempts.js                  # all clients
 *   node scripts/backfill-transaction-attempts.js --client=1       # specific client
 *   node scripts/backfill-transaction-attempts.js --client=1 --reset  # clear and re-backfill
 */
const path = require('path');

// Initialize DB before importing modules that need it
const { initDb, querySql, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();

  const { explodeAllOrders } = require(path.join(__dirname, '..', 'src', 'pipeline', 'attempt-exploder'));

  // Parse args
  const args = process.argv.slice(2);
  let clientId = null;
  let reset = false;
  for (const arg of args) {
    if (arg.startsWith('--client=')) clientId = parseInt(arg.split('=')[1]);
    if (arg === '--reset') reset = true;
  }

  // Get client list
  const clients = clientId
    ? querySql('SELECT id, name FROM clients WHERE id = ?', [clientId])
    : querySql('SELECT id, name FROM clients ORDER BY id');

  if (clients.length === 0) {
    console.log('No clients found.');
    process.exit(1);
  }

  const db = getDb();

  for (const client of clients) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`Client ${client.id}: ${client.name}`);
    console.log('='.repeat(60));

    if (reset) {
      console.log('[Reset] Deleting existing attempts...');
      db.prepare('DELETE FROM transaction_attempts WHERE client_id = ?').run(client.id);
      console.log('[Reset] Done.');
    }

    const start = Date.now();
    const result = explodeAllOrders(client.id, {
      onProgress: (done, total, inserted) => {
        if (done % 10000 === 0 || done === total) {
          const pct = ((done / total) * 100).toFixed(1);
          console.log(`  [Progress] ${done}/${total} orders (${pct}%) → ${inserted} attempts`);
        }
      }
    });

    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`\n[Result] ${result.inserted} attempts inserted from ${result.total} orders in ${elapsed}s (${result.skipped} skipped)`);

    // Print reconciliation
    _printReconciliation(client.id);
  }

  console.log('\nDone.');
}

function _printReconciliation(clientId) {
  const db = getDb();

  const totalAttempts = db.prepare(
    'SELECT COUNT(*) as cnt FROM transaction_attempts WHERE client_id = ?'
  ).get(clientId).cnt;

  const bySource = db.prepare(
    `SELECT source, COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? GROUP BY source ORDER BY cnt DESC`
  ).all(clientId);

  const byModelTarget = db.prepare(
    `SELECT model_target, outcome, COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? GROUP BY model_target, outcome ORDER BY model_target, outcome`
  ).all(clientId);

  const byAttemptSeq = db.prepare(
    `SELECT attempt_seq, COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? GROUP BY attempt_seq ORDER BY attempt_seq`
  ).all(clientId);

  console.log(`\n[Reconciliation] Total attempts: ${totalAttempts}`);

  console.log('\n  By source:');
  for (const r of bySource) console.log(`    ${r.source}: ${r.cnt}`);

  console.log('\n  By model_target + outcome:');
  for (const r of byModelTarget) console.log(`    ${r.model_target} / ${r.outcome}: ${r.cnt}`);

  console.log('\n  By attempt_seq:');
  for (const r of byAttemptSeq) console.log(`    attempt ${r.attempt_seq}: ${r.cnt}`);
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
