#!/usr/bin/env node
/**
 * issue-api-key.js — mint an API key for the checkout-facing /api/route endpoint.
 *
 * Usage:
 *   node scripts/issue-api-key.js <client_id> [label]
 *
 * Prints the plaintext key ONCE to stdout and stores a bcrypt hash in the
 * api_keys table. The plaintext is not recoverable — re-run if you lose it.
 *
 * Example:
 *   node scripts/issue-api-key.js 1 "kytsan-checkout"
 *
 * To revoke:
 *   sqlite3 data/binroute.db "UPDATE api_keys SET revoked_at=CURRENT_TIMESTAMP WHERE id=<id>"
 */

const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const { initDb, runSql, queryOneSql, closeDb } = require('../src/db/connection');

async function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.error('Usage: node scripts/issue-api-key.js <client_id> [label]');
    process.exit(2);
  }
  const clientId = parseInt(args[0], 10);
  const label = args[1] || null;

  if (!Number.isInteger(clientId) || clientId <= 0) {
    console.error('client_id must be a positive integer');
    process.exit(2);
  }

  await initDb();

  const client = queryOneSql('SELECT id, name FROM clients WHERE id = ?', [clientId]);
  if (!client) {
    console.error(`client ${clientId} not found`);
    closeDb();
    process.exit(2);
  }

  const plaintext = crypto.randomBytes(32).toString('hex');
  const hash = bcrypt.hashSync(plaintext, 10);

  runSql(
    'INSERT INTO api_keys (client_id, api_key_hash, label) VALUES (?, ?, ?)',
    [clientId, hash, label]
  );

  const row = queryOneSql(
    'SELECT id, created_at FROM api_keys WHERE client_id = ? ORDER BY id DESC LIMIT 1',
    [clientId]
  );

  console.log('');
  console.log('  ╔══════════════════════════════════════════════════════════════════════╗');
  console.log('  ║                API KEY ISSUED — STORE IT NOW                         ║');
  console.log('  ║                This value will NOT be shown again.                   ║');
  console.log('  ╚══════════════════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  Client:    ${client.name} (id=${client.id})`);
  console.log(`  Key id:    ${row.id}`);
  console.log(`  Label:     ${label || '(none)'}`);
  console.log(`  Created:   ${row.created_at}`);
  console.log('');
  console.log(`  x-client-id:  ${clientId}`);
  console.log(`  x-api-key:    ${plaintext}`);
  console.log('');
  console.log('  Usage example (curl):');
  console.log(`    curl -X POST https://binroute.cswebform.cloud/api/route \\`);
  console.log(`      -H "x-client-id: ${clientId}" \\`);
  console.log(`      -H "x-api-key: ${plaintext}" \\`);
  console.log(`      -H "Content-Type: application/json" \\`);
  console.log(`      -d '{"bin":"411111","amount":49.95,"sales_type":"INITIALS"}'`);
  console.log('');

  closeDb();
}

main().catch(err => {
  console.error('Failed to issue key:', err);
  try { closeDb(); } catch {}
  process.exit(1);
});
