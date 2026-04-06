/**
 * Backfill subscription features (Layer 2.5) for all tx_features rows.
 *
 * Usage:
 *   node scripts/backfill-subscription-features.js              — all clients
 *   node scripts/backfill-subscription-features.js --client=3   — client 3 only
 */
const { initDb, querySql, closeDb } = require('../src/db/connection');
const { initializeDatabase } = require('../src/db/schema');
const { computeSubscriptionFeatures } = require('../src/analytics/subscription-features');

const CLIENT_ID = (() => {
  const arg = process.argv.find(a => a.startsWith('--client='));
  return arg ? parseInt(arg.split('=')[1], 10) : null;
})();

(async () => {
  console.log(`=== Backfill Subscription Features (Layer 2.5)${CLIENT_ID ? ` — Client ${CLIENT_ID}` : ''} ===\n`);

  await initializeDatabase();

  const clients = CLIENT_ID
    ? querySql('SELECT id, name FROM clients WHERE id = ?', [CLIENT_ID])
    : querySql('SELECT id, name FROM clients ORDER BY id');

  let grandTotal = 0;
  for (const client of clients) {
    const start = Date.now();
    console.log(`\n[${client.name}] Computing subscription features...`);
    const count = computeSubscriptionFeatures(client.id);
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`[${client.name}] ${count.toLocaleString()} rows updated in ${elapsed}s`);
    grandTotal += count;
  }

  console.log(`\n=== Done: ${grandTotal.toLocaleString()} rows updated across ${clients.length} clients ===`);
  closeDb();
})();
