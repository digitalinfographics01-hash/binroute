/**
 * Backfill tx_features — one-time script to populate the AI training feature table
 * for all existing orders across all clients.
 *
 * CAN run while the server is running (better-sqlite3 file-based access).
 *
 * Usage:
 *   node scripts/backfill-tx-features.js              — all clients
 *   node scripts/backfill-tx-features.js --client=3   — client 3 only
 */
const { initDb, querySql, closeDb } = require('../src/db/connection');
const { initializeDatabase } = require('../src/db/schema');
const { rebuildFeatures } = require('../src/analytics/feature-extraction');

const CLIENT_ID = (() => {
  const arg = process.argv.find(a => a.startsWith('--client='));
  return arg ? parseInt(arg.split('=')[1], 10) : null;
})();

(async () => {
  console.log(`=== Backfill tx_features${CLIENT_ID ? ` — Client ${CLIENT_ID}` : ''} ===\n`);

  // Initialize DB + ensure tx_features table exists
  await initializeDatabase();

  const clients = CLIENT_ID
    ? querySql('SELECT id, name FROM clients WHERE id = ?', [CLIENT_ID])
    : querySql('SELECT id, name FROM clients ORDER BY id');
  if (clients.length === 0) {
    console.log('No clients found.');
    closeDb();
    return;
  }

  let grandTotal = 0;

  for (const client of clients) {
    const start = Date.now();
    console.log(`[${client.name}] Rebuilding features...`);
    const count = rebuildFeatures(client.id);
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`[${client.name}] ${count.toLocaleString()} features extracted in ${elapsed}s`);
    grandTotal += count;
  }

  console.log(`\n=== Done: ${grandTotal.toLocaleString()} total features across ${clients.length} clients ===`);
  closeDb();
})();
