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

  // Step 2: Backfill velocity features for any new orders
  console.log('[ML Retrain] Step 2: Velocity features...');
  let velocityUpdated = 0;
  try {
    const clients = querySql('SELECT id, name FROM clients ORDER BY id');
    for (const client of clients) {
      const count = computeVelocityFeatures(client.id);
      velocityUpdated += count;
      if (count > 0) console.log(`  [${client.name}] ${count} velocity features computed`);
    }
  } catch (err) {
    console.error('[ML Retrain] Velocity backfill failed:', err.message);
  }

  // Step 3: Backfill subscription features for any new orders
  console.log('[ML Retrain] Step 3: Subscription features...');
  let subscriptionUpdated = 0;
  try {
    const clients = querySql('SELECT id, name FROM clients ORDER BY id');
    for (const client of clients) {
      const count = computeSubscriptionFeatures(client.id);
      subscriptionUpdated += count;
      if (count > 0) console.log(`  [${client.name}] ${count} subscription features computed`);
    }
  } catch (err) {
    console.error('[ML Retrain] Subscription backfill failed:', err.message);
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

  return { attemptsInserted, velocityUpdated, subscriptionUpdated, retrainOutput };
}

module.exports = { runRetrain };
