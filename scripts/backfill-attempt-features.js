#!/usr/bin/env node
/**
 * Backfill attempt features — runs velocity and subscription on transaction_attempts.
 *
 * Usage:
 *   node scripts/backfill-attempt-features.js --client=1                # both velocity + subscription
 *   node scripts/backfill-attempt-features.js --client=1 --velocity     # velocity only
 *   node scripts/backfill-attempt-features.js --client=1 --subscription # subscription only
 */
const path = require('path');
const { initDb, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();

  const { computeAttemptVelocity } = require(path.join(__dirname, '..', 'src', 'analytics', 'attempt-velocity-features'));
  const { computeAttemptSubscription } = require(path.join(__dirname, '..', 'src', 'analytics', 'attempt-subscription-features'));

  // Parse args
  const args = process.argv.slice(2);
  let clientId = null;
  let runVelocity = true;
  let runSubscription = true;

  for (const arg of args) {
    if (arg.startsWith('--client=')) clientId = parseInt(arg.split('=')[1]);
    if (arg === '--velocity') { runVelocity = true; runSubscription = false; }
    if (arg === '--subscription') { runVelocity = false; runSubscription = true; }
  }

  if (!clientId) {
    console.error('Usage: node scripts/backfill-attempt-features.js --client=N [--velocity] [--subscription]');
    process.exit(1);
  }

  const start = Date.now();

  if (runVelocity) {
    console.log('\n' + '='.repeat(60));
    console.log('Phase B: Velocity Features');
    console.log('='.repeat(60));
    const velStart = Date.now();
    const velCount = computeAttemptVelocity(clientId);
    console.log(`Velocity done in ${((Date.now() - velStart) / 1000).toFixed(1)}s — ${velCount.toLocaleString()} rows`);
  }

  if (runSubscription) {
    console.log('\n' + '='.repeat(60));
    console.log('Phase C: Subscription Features');
    console.log('='.repeat(60));
    const subStart = Date.now();
    const subCount = computeAttemptSubscription(clientId);
    console.log(`Subscription done in ${((Date.now() - subStart) / 1000).toFixed(1)}s — ${subCount.toLocaleString()} rows`);
  }

  // Print final feature version distribution
  const db = getDb();
  const dist = db.prepare(
    `SELECT feature_version, COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? GROUP BY feature_version ORDER BY feature_version`
  ).all(clientId);

  console.log('\n' + '='.repeat(60));
  console.log('Feature version distribution:');
  for (const r of dist) console.log(`  v${r.feature_version}: ${r.cnt.toLocaleString()}`);

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\nTotal time: ${elapsed}s`);
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
