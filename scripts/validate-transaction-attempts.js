#!/usr/bin/env node
/**
 * Validate transaction_attempts — checks data integrity after backfill.
 *
 * Usage:
 *   node scripts/validate-transaction-attempts.js                  # all clients
 *   node scripts/validate-transaction-attempts.js --client=1       # specific client
 */
const path = require('path');
const { initDb, querySql, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();

  const args = process.argv.slice(2);
  let clientId = null;
  for (const arg of args) {
    if (arg.startsWith('--client=')) clientId = parseInt(arg.split('=')[1]);
  }

  const clients = clientId
    ? querySql('SELECT id, name FROM clients WHERE id = ?', [clientId])
    : querySql('SELECT id, name FROM clients ORDER BY id');

  let allPassed = true;

  for (const client of clients) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`Validating client ${client.id}: ${client.name}`);
    console.log('='.repeat(60));

    const passed = validateClient(client.id);
    if (!passed) allPassed = false;
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(allPassed ? 'ALL VALIDATIONS PASSED' : 'SOME VALIDATIONS FAILED');
  process.exit(allPassed ? 0 : 1);
}

function validateClient(clientId) {
  const db = getDb();
  const errors = [];
  const warnings = [];

  // --- 1. Total counts ---
  const totalAttempts = db.prepare(
    'SELECT COUNT(*) as cnt FROM transaction_attempts WHERE client_id = ?'
  ).get(clientId).cnt;

  const totalOrders = db.prepare(
    `SELECT COUNT(*) as cnt FROM orders WHERE client_id = ?
     AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
     AND product_type_classified IS NOT NULL AND product_type_classified != 'straight_sale'`
  ).get(clientId).cnt;

  console.log(`  Total orders (qualifying): ${totalOrders}`);
  console.log(`  Total attempts: ${totalAttempts}`);

  if (totalAttempts === 0) {
    errors.push('No transaction_attempts rows found');
    _report(errors, warnings);
    return false;
  }

  // --- 2. Non-cascaded orders should have exactly 1 row ---
  const nonCascadedOrders = db.prepare(
    `SELECT COUNT(DISTINCT order_id) as cnt FROM orders
     WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
     AND product_type_classified IS NOT NULL AND product_type_classified != 'straight_sale'
     AND is_cascaded = 0`
  ).get(clientId).cnt;

  const nonCascadedAttempts = db.prepare(
    `SELECT COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? AND source = 'order_direct'`
  ).get(clientId).cnt;

  if (nonCascadedAttempts !== nonCascadedOrders) {
    errors.push(`Non-cascaded mismatch: ${nonCascadedOrders} orders but ${nonCascadedAttempts} attempts (expected 1:1)`);
  } else {
    console.log(`  Non-cascaded: ${nonCascadedOrders} orders → ${nonCascadedAttempts} attempts (1:1) ✓`);
  }

  // --- 3. No orphan attempts ---
  const orphans = db.prepare(
    `SELECT COUNT(*) as cnt FROM transaction_attempts ta
     WHERE ta.client_id = ? AND NOT EXISTS (
       SELECT 1 FROM orders o WHERE o.id = ta.order_id
     )`
  ).get(clientId).cnt;

  if (orphans > 0) {
    errors.push(`${orphans} orphan attempts (order_id not in orders table)`);
  } else {
    console.log(`  No orphan attempts ✓`);
  }

  // --- 4. No NULL model_target on non-excluded ---
  const nullModelTarget = db.prepare(
    `SELECT COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? AND model_target IS NULL`
  ).get(clientId).cnt;

  if (nullModelTarget > 0) {
    errors.push(`${nullModelTarget} rows with NULL model_target`);
  } else {
    console.log(`  No NULL model_target ✓`);
  }

  // --- 5. All outcomes valid ---
  const badOutcome = db.prepare(
    `SELECT COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? AND outcome NOT IN ('approved', 'declined')`
  ).get(clientId).cnt;

  if (badOutcome > 0) {
    errors.push(`${badOutcome} rows with invalid outcome`);
  } else {
    console.log(`  All outcomes valid ✓`);
  }

  // --- 6. Feature version ---
  const featureVersionDist = db.prepare(
    `SELECT feature_version, COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? GROUP BY feature_version`
  ).all(clientId);

  console.log(`  Feature version distribution:`);
  for (const r of featureVersionDist) console.log(`    v${r.feature_version}: ${r.cnt}`);

  const v0Count = featureVersionDist.find(r => r.feature_version === 0);
  if (v0Count && v0Count.cnt > 0) {
    errors.push(`${v0Count.cnt} rows still at feature_version=0 (core features not populated)`);
  }

  // --- 7. BIN coverage ---
  const binCoverage = db.prepare(
    `SELECT
       COUNT(*) as total,
       SUM(CASE WHEN issuer_bank IS NOT NULL THEN 1 ELSE 0 END) as with_bin
     FROM transaction_attempts WHERE client_id = ?`
  ).get(clientId);

  const binPct = binCoverage.total > 0 ? ((binCoverage.with_bin / binCoverage.total) * 100).toFixed(1) : 0;
  if (parseFloat(binPct) < 85) {
    warnings.push(`BIN coverage: ${binPct}% (${binCoverage.with_bin}/${binCoverage.total}) — below 85% threshold`);
  } else {
    console.log(`  BIN coverage: ${binPct}% ✓`);
  }

  // --- 8. Processor name coverage ---
  const procNull = db.prepare(
    `SELECT COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? AND processor_name IS NULL`
  ).get(clientId).cnt;

  if (procNull > 0) {
    const procPct = ((procNull / totalAttempts) * 100).toFixed(1);
    warnings.push(`${procNull} rows (${procPct}%) missing processor_name`);
  } else {
    console.log(`  Processor name: 100% coverage ✓`);
  }

  // --- 9. No duplicate (client_id, sticky_order_id, attempt_seq) ---
  const dupes = db.prepare(
    `SELECT COUNT(*) as cnt FROM (
       SELECT client_id, sticky_order_id, attempt_seq, COUNT(*) as c
       FROM transaction_attempts WHERE client_id = ?
       GROUP BY client_id, sticky_order_id, attempt_seq HAVING c > 1
     )`
  ).get(clientId).cnt;

  if (dupes > 0) {
    errors.push(`${dupes} duplicate (sticky_order_id, attempt_seq) combinations`);
  } else {
    console.log(`  No duplicates ✓`);
  }

  // --- 10. Model target distribution ---
  const modelDist = db.prepare(
    `SELECT model_target, outcome, COUNT(*) as cnt FROM transaction_attempts
     WHERE client_id = ? GROUP BY model_target, outcome ORDER BY model_target, outcome`
  ).all(clientId);

  console.log(`\n  Model target breakdown:`);
  for (const r of modelDist) console.log(`    ${r.model_target} / ${r.outcome}: ${r.cnt.toLocaleString()}`);

  _report(errors, warnings);
  return errors.length === 0;
}

function _report(errors, warnings) {
  if (warnings.length > 0) {
    console.log(`\n  WARNINGS:`);
    for (const w of warnings) console.log(`    ⚠ ${w}`);
  }
  if (errors.length > 0) {
    console.log(`\n  ERRORS:`);
    for (const e of errors) console.log(`    ✗ ${e}`);
  }
  if (errors.length === 0) {
    console.log(`\n  Result: PASSED`);
  } else {
    console.log(`\n  Result: FAILED (${errors.length} errors)`);
  }
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
