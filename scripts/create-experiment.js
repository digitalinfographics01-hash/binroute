#!/usr/bin/env node
/**
 * Create a new A/B experiment (status=draft, not active).
 *
 * Usage:
 *   node scripts/create-experiment.js --client 1 --name "Stage 1 AI routing test" --traffic 10 --treatment 50
 */
const path = require('path');
const { initDb, runSql, querySql } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();

  const args = process.argv.slice(2);
  let clientId = null, name = null, trafficPct = 10, treatmentPct = 50, notes = null;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--client' && args[i + 1]) clientId = parseInt(args[++i], 10);
    if (args[i] === '--name' && args[i + 1]) name = args[++i];
    if (args[i] === '--traffic' && args[i + 1]) trafficPct = parseFloat(args[++i]);
    if (args[i] === '--treatment' && args[i + 1]) treatmentPct = parseFloat(args[++i]);
    if (args[i] === '--notes' && args[i + 1]) notes = args[++i];
  }

  if (!clientId || !name) {
    console.error('Usage: node scripts/create-experiment.js --client <id> --name "<name>" [--traffic 10] [--treatment 50] [--notes "..."]');
    process.exit(1);
  }

  const result = runSql(
    `INSERT INTO experiments (client_id, name, status, traffic_pct, treatment_pct, notes)
     VALUES (?, ?, 'draft', ?, ?, ?)`,
    [clientId, name, trafficPct, treatmentPct, notes]
  );

  const id = result.lastInsertRowid;
  console.log(`Experiment created:`);
  console.log(`  ID: ${id}`);
  console.log(`  Client: ${clientId}`);
  console.log(`  Name: ${name}`);
  console.log(`  Status: draft`);
  console.log(`  Traffic: ${trafficPct}%`);
  console.log(`  Treatment: ${treatmentPct}% (of experiment traffic)`);
  console.log(`\nTo activate: node scripts/manage-experiment.js activate ${id}`);
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
