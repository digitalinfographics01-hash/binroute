#!/usr/bin/env node
/**
 * Import cascaded order data from CSV files.
 * Maps original gateway ID and decline reason to existing orders.
 *
 * Usage:
 *   node scripts/import-cascade-csv.js           — import all unprocessed kytsan CSVs
 *   node scripts/import-cascade-csv.js --dry-run  — preview without writing
 */

const fs = require('fs');
const path = require('path');

const CSV_DIR = path.join(__dirname, '..', 'Cascaded orders data', 'StickyIO_Attachments');

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  // Optional: filter to a specific client by domain prefix
  const domainArg = process.argv.find(a => a.startsWith('--domain='));
  const domainFilter = domainArg ? domainArg.split('=')[1] : null;

  // Init DB
  const { initDb, querySql, execSql, saveDb, getDb } = require('../src/db/connection');
  await initDb();
  const db = getDb();

  // Run migrations to ensure columns exist
  try { execSql('ALTER TABLE orders ADD COLUMN original_gateway_id INTEGER DEFAULT NULL'); } catch (e) { /* already exists */ }
  try { execSql('ALTER TABLE orders ADD COLUMN original_decline_reason TEXT DEFAULT NULL'); } catch (e) { /* already exists */ }

  // Create tracking table for imported files
  execSql(`CREATE TABLE IF NOT EXISTS cascade_csv_imports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL UNIQUE,
    rows_parsed INTEGER DEFAULT 0,
    rows_matched INTEGER DEFAULT 0,
    client_id INTEGER,
    imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Build domain → client_id mapping from clients table
  const clients = querySql('SELECT id, name, sticky_base_url FROM clients');
  const domainMap = new Map();
  for (const c of clients) {
    // Extract domain from sticky_base_url (e.g., "kytsanmanagementllc.sticky.io" → "kytsanmanagementllc")
    const domain = (c.sticky_base_url || '').replace(/https?:\/\//, '').split('.')[0].toLowerCase();
    if (domain) {
      domainMap.set(domain, c.id);
      console.log(`  Client ${c.id} (${c.name}) → domain: ${domain}`);
    }
  }

  if (domainMap.size === 0) {
    console.log('No clients configured. Add clients first.');
    return;
  }

  // Find all cascade CSV files (optionally filtered by domain)
  const allFiles = fs.readdirSync(CSV_DIR).filter(f => {
    if (!f.endsWith('.csv') || !f.includes('original_gateway_decline')) return false;
    if (domainFilter) return f.startsWith(domainFilter);
    // Only include files whose domain prefix matches a known client
    const fileDomain = f.split('_original_gateway_decline')[0].toLowerCase();
    return domainMap.has(fileDomain);
  }).sort();

  // Check which files are already imported
  const imported = new Set(
    querySql('SELECT filename FROM cascade_csv_imports').map(r => r.filename)
  );

  const pending = allFiles.filter(f => !imported.has(f));
  console.log(`Found ${allFiles.length} cascade CSV files, ${imported.size} already imported, ${pending.length} pending`);

  if (pending.length === 0) {
    console.log('Nothing to import.');
    return;
  }

  // Prepare statements
  const checkStmt = db.prepare('SELECT 1 FROM orders WHERE client_id = ? AND order_id = ? AND is_cascaded = 1');
  const updateStmt = db.prepare('UPDATE orders SET original_gateway_id = ?, original_decline_reason = ? WHERE client_id = ? AND order_id = ? AND is_cascaded = 1');
  const trackStmt = db.prepare('INSERT OR REPLACE INTO cascade_csv_imports (filename, rows_parsed, rows_matched, client_id) VALUES (?, ?, ?, ?)');

  let totalParsed = 0, totalMatched = 0, totalSkipped = 0;

  for (const file of pending) {
    // Resolve client_id from filename domain prefix
    const fileDomain = file.split('_original_gateway_decline')[0].toLowerCase();
    const clientId = domainMap.get(fileDomain);
    if (!clientId) {
      console.log(`  Skipping ${file} — no client found for domain "${fileDomain}"`);
      continue;
    }

    const filePath = path.join(CSV_DIR, file);
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(l => l.trim());

    // Skip header
    const header = lines[0];
    if (!header.includes('Orders ID')) {
      console.log(`  Skipping ${file} — no header found`);
      continue;
    }

    // Parse all rows first
    const rows = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;

      const parts = line.split(',');
      if (parts.length < 5) continue;

      const orderId = parseInt(parts[2]?.trim(), 10);
      const origGatewayId = parseInt(parts[3]?.trim(), 10);
      const declineReason = parts.slice(4).join(',').trim();

      if (!orderId || !origGatewayId) continue;
      rows.push({ orderId, origGatewayId, declineReason });
    }

    // Process file in a single transaction
    let fileMatched = 0;
    const importFile = db.transaction(() => {
      for (const r of rows) {
        const exists = checkStmt.get(clientId, r.orderId);
        if (exists) {
          fileMatched++;
          if (!dryRun) {
            updateStmt.run(r.origGatewayId, r.declineReason, clientId, r.orderId);
          }
        }
      }
      if (!dryRun) {
        trackStmt.run(file, rows.length, fileMatched, clientId);
      }
    });
    importFile();

    totalParsed += rows.length;
    totalMatched += fileMatched;
    totalSkipped += (rows.length - fileMatched);

    console.log(`  ${file} [client ${clientId}]: ${rows.length} rows, ${fileMatched} matched, ${rows.length - fileMatched} no match`);
  }

  if (!dryRun) saveDb();

  console.log(`\n=== Summary ===`);
  console.log(`Files processed: ${pending.length}`);
  console.log(`Total rows parsed: ${totalParsed}`);
  console.log(`Matched to orders: ${totalMatched}`);
  console.log(`No match (order not in DB or not cascaded): ${totalSkipped}`);
  if (dryRun) console.log('(DRY RUN — no changes written)');

  // Checkpoint WAL
  const { checkpointWal, closeDb } = require('../src/db/connection');
  checkpointWal();
  console.log('WAL checkpoint done.');
  closeDb();
}

main().catch(err => { console.error('Import failed:', err); process.exit(1); });
