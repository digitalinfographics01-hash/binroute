/**
 * ML Retrain Runner — Node.js wrapper for the Python retrain pipeline.
 *
 * Called by the scheduler weekly. Runs:
 *   1. Explode new orders into transaction_attempts
 *   2. Velocity feature backfill (for any new orders)
 *   3. Subscription feature backfill (for any new orders)
 *   4. Rebuild lookup tables from transaction_attempts
 *   5. Python retrain script (trains, compares, promotes if better)
 *
 * Usage: require('./src/ml/retrain-runner').runRetrain()
 */
const { execSync } = require('child_process');
const path = require('path');
const { computeVelocityFeatures } = require('../analytics/velocity-features');
const { computeSubscriptionFeatures } = require('../analytics/subscription-features');
const { computeAttemptVelocity } = require('../analytics/attempt-velocity-features');
const { computeAttemptSubscription } = require('../analytics/attempt-subscription-features');
const { explodeAllOrders } = require('../pipeline/attempt-exploder');
const { querySql } = require('../db/connection');

const RETRAIN_SCRIPT = path.join(__dirname, '..', '..', 'scripts', 'ml', 'train_four_models.py');
const LOOKUP_SCRIPT = path.join(__dirname, '..', '..', 'scripts', 'build-all-lookups.js');

/**
 * Run the full retrain pipeline.
 * @returns {{ velocityUpdated: number, subscriptionUpdated: number, retrainOutput: string }}
 */
function runRetrain() {
  const start = Date.now();
  console.log('[ML Retrain] Starting weekly retrain pipeline...');

  // Step 1: Explode new orders into transaction_attempts
  console.log('[ML Retrain] Step 1: Explode new orders into transaction_attempts...');
  let attemptsInserted = 0;
  try {
    const clients = querySql('SELECT id, name FROM clients ORDER BY id');
    for (const client of clients) {
      const result = explodeAllOrders(client.id);
      attemptsInserted += result.inserted;
      if (result.inserted > 0) console.log(`  [${client.name}] ${result.inserted} attempts exploded`);
    }
  } catch (err) {
    console.error('[ML Retrain] Attempt exploder failed:', err.message);
  }

  // Step 2: Backfill attempt-level velocity features (v1→v2)
  console.log('[ML Retrain] Step 2: Attempt velocity features...');
  let attemptVelocityUpdated = 0;
  try {
    const clients = querySql('SELECT id, name FROM clients ORDER BY id');
    for (const client of clients) {
      const count = computeAttemptVelocity(client.id);
      attemptVelocityUpdated += count;
      if (count > 0) console.log(`  [${client.name}] ${count} attempt velocity features computed`);
    }
  } catch (err) {
    console.error('[ML Retrain] Attempt velocity backfill failed:', err.message);
  }

  // Step 3: Backfill attempt-level subscription features (v2→v3)
  console.log('[ML Retrain] Step 3: Attempt subscription features...');
  let attemptSubscriptionUpdated = 0;
  try {
    const clients = querySql('SELECT id, name FROM clients ORDER BY id');
    for (const client of clients) {
      const count = computeAttemptSubscription(client.id);
      attemptSubscriptionUpdated += count;
      if (count > 0) console.log(`  [${client.name}] ${count} attempt subscription features computed`);
    }
  } catch (err) {
    console.error('[ML Retrain] Attempt subscription backfill failed:', err.message);
  }

  // Step 3b: Backfill tx_features velocity + subscription (for Python training)
  console.log('[ML Retrain] Step 3b: tx_features velocity + subscription...');
  let velocityUpdated = 0, subscriptionUpdated = 0;
  try {
    const clients = querySql('SELECT id, name FROM clients ORDER BY id');
    for (const client of clients) {
      const v = computeVelocityFeatures(client.id);
      velocityUpdated += v;
      const s = computeSubscriptionFeatures(client.id);
      subscriptionUpdated += s;
      if (v + s > 0) console.log(`  [${client.name}] ${v} velocity, ${s} subscription`);
    }
  } catch (err) {
    console.error('[ML Retrain] tx_features backfill failed:', err.message);
  }

  // Step 4: Rebuild lookup tables
  console.log('[ML Retrain] Step 4: Rebuild lookup tables...');
  try {
    const output = execSync(`node "${LOOKUP_SCRIPT}"`, {
      cwd: path.join(__dirname, '..', '..'),
      timeout: 300000, // 5 min max
      encoding: 'utf8',
    });
    console.log(output);
  } catch (err) {
    console.error('[ML Retrain] Lookup rebuild failed:', err.message);
    if (err.stdout) console.log(err.stdout);
    if (err.stderr) console.error(err.stderr);
  }

  // Step 5: Run Python retrain
  console.log('[ML Retrain] Step 5: Python retrain...');
  let retrainOutput = '';
  try {
    retrainOutput = execSync(`python3 "${RETRAIN_SCRIPT}"`, {
      cwd: path.join(__dirname, '..', '..'),
      timeout: 1200000, // 20 min max (enrichment + 5 model training)
      encoding: 'utf8',
    });
    console.log(retrainOutput);
  } catch (err) {
    console.error('[ML Retrain] Python retrain failed:', err.message);
    if (err.stdout) console.log(err.stdout);
    if (err.stderr) console.error(err.stderr);
    retrainOutput = `ERROR: ${err.message}`;
  }

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`[ML Retrain] Pipeline complete in ${elapsed}s`);

  return { attemptsInserted, attemptVelocityUpdated, attemptSubscriptionUpdated, velocityUpdated, subscriptionUpdated, retrainOutput };
}

module.exports = { runRetrain };
