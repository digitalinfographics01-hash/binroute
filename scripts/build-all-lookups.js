/**
 * Build all 4 lookup tables from transaction_attempts.
 *
 * Tables:
 *   1. Main Initial (3D): issuer × card_type_merged × target_processor
 *   2. Upsell (4D): issuer × card_type_merged × initial_processor × target_processor
 *   3. Rebill First-Attempt (4D): issuer × card_type_merged × initial_proc × target
 *   4. Rebill Salvage (4D primary + 5D optional): decline × issuer × failed_proc × target [+ card_type_merged]
 *
 * Shared rules:
 *   35+ rows AND rate < 5% → hard_exclude
 *   20-34 rows AND rate < 5% → soft_downrank
 *   < 20 rows → no_action
 *
 * Data filters:
 *   Main Initial: source='order_direct', derived_cycle=0, derived_product_role='main_initial'
 *   Upsell: source='order_direct', derived_cycle=0, derived_product_role='upsell_initial'
 *   Rebill: source='order_direct', derived_cycle IN (1,2), derived_attempt=1
 *   Salvage: source='order_direct', derived_cycle>=1, derived_attempt>1
 *
 * Usage:
 *   node scripts/build-all-lookups.js
 */

const fs = require('fs');
const path = require('path');
const { initDb, querySql } = require('../src/db/connection');

const OUTPUT_DIR = path.join(__dirname, '..', 'data', 'models');
const THRESHOLD = 0.05;
const HARD_MIN = 35;
const SOFT_MIN = 20;
const CLIENT_OVERRIDE_MIN = 50;

const CTM = "CASE WHEN is_prepaid=1 THEN 'PREPAID' ELSE card_type END";
const BASE = "feature_version >= 3 AND model_target != 'excluded' AND processor_name != 'UNKNOWN'";
const NO_UNK_INIT = "AND (initial_processor IS NULL OR initial_processor != 'UNKNOWN')";
const NO_UNK_PARENT = "AND (parent_declined_processor IS NULL OR parent_declined_processor != 'UNKNOWN')";

function classifyAction(rate, sampleSize) {
  if (sampleSize >= HARD_MIN && rate < THRESHOLD) return 'hard_exclude';
  if (sampleSize >= SOFT_MIN && rate < THRESHOLD) return 'soft_downrank';
  return 'allow';
}

function buildEntries(rows, keyFn) {
  const entries = {};
  for (const row of rows) {
    const key = keyFn(row);
    if (!key) continue;
    const rate = row.approved / row.n;
    entries[key] = {
      approval_rate: Math.round(rate * 10000) / 10000,
      sample_size: row.n,
      approved: row.approved,
      action: classifyAction(rate, row.n),
    };
  }
  return entries;
}

function buildClientOverrides(rows, keyFn) {
  const overrides = {};
  for (const row of rows) {
    if (row.n < CLIENT_OVERRIDE_MIN) continue;
    const key = keyFn(row);
    if (!key) continue;
    const rate = row.approved / row.n;
    if (!overrides[key]) overrides[key] = {};
    overrides[key][row.client_id] = {
      approval_rate: Math.round(rate * 10000) / 10000,
      sample_size: row.n,
      approved: row.approved,
      action: classifyAction(rate, row.n),
    };
  }
  return overrides;
}

function saveTable(filename, data) {
  const filePath = path.join(OUTPUT_DIR, filename);
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
  const size = (fs.statSync(filePath).size / 1024).toFixed(1);
  console.log(`  Saved: ${filename} (${size} KB)`);
}

function summarize(label, entries) {
  const all = Object.values(entries);
  const hard = all.filter(e => e.action === 'hard_exclude').length;
  const soft = all.filter(e => e.action === 'soft_downrank').length;
  const allow = all.filter(e => e.action === 'allow').length;
  console.log(`  ${label}: ${all.length} entries — ${hard} hard, ${soft} soft, ${allow} allow`);
}

(async () => {
  await initDb();
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const q = (sql) => querySql(sql);
  const now = new Date().toISOString();

  console.log('╔═══════════════════════════════════════════╗');
  console.log('║     BUILDING ALL 4 LOOKUP TABLES (v2)     ║');
  console.log('╚═══════════════════════════════════════════╝\n');

  // ═══════════════════════════════════════════
  // TABLE 1: MAIN INITIAL (3D)
  // main_initial only, cycle=0, no NULL cycles
  // ═══════════════════════════════════════════
  console.log('═══ TABLE 1: MAIN INITIAL (3D) ═══');
  const INIT_FILTER = `${BASE} AND source='order_direct' AND derived_cycle=0 AND derived_product_role='main_initial'`;

  const init3d = q(
    `SELECT issuer_bank, ${CTM} as ctm, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${INIT_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL ` +
    `GROUP BY issuer_bank, ctm, target HAVING n >= ${SOFT_MIN}`
  );
  const init3dEntries = buildEntries(init3d, r => `${r.issuer_bank}|${r.ctm}|${r.target}`);
  summarize('3D (issuer×card_type×target)', init3dEntries);

  const init2d = q(
    `SELECT issuer_bank, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${INIT_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL ` +
    `GROUP BY issuer_bank, target HAVING n >= ${SOFT_MIN}`
  );
  const init2dEntries = buildEntries(init2d, r => `${r.issuer_bank}|${r.target}`);
  summarize('2D fallback (issuer×target)', init2dEntries);

  const init3dClients = q(
    `SELECT client_id, issuer_bank, ${CTM} as ctm, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${INIT_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL ` +
    `GROUP BY client_id, issuer_bank, ctm, target HAVING n >= ${CLIENT_OVERRIDE_MIN}`
  );
  const init3dOverrides = buildClientOverrides(init3dClients, r => `${r.issuer_bank}|${r.ctm}|${r.target}`);

  // Total attempts for coverage calc
  const initTotal = q(`SELECT COUNT(*) as n FROM transaction_attempts WHERE ${INIT_FILTER} AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL`)[0].n;
  const initCovered = init3d.filter(r => r.n >= HARD_MIN).reduce((s, r) => s + r.n, 0);
  console.log(`  Coverage: ${(100 * initCovered / initTotal).toFixed(1)}% of ${initTotal} attempts at 35+ threshold`);

  saveTable('initial_lookup.json', {
    metadata: { built_at: now, table: 'main_initial', dimensions: '3D', threshold: THRESHOLD,
      filter: 'derived_cycle=0, derived_product_role=main_initial',
      total_attempts: initTotal, total_3d: Object.keys(init3dEntries).length, total_2d: Object.keys(init2dEntries).length },
    tier_3d: init3dEntries,
    tier_2d: init2dEntries,
    client_overrides_3d: init3dOverrides,
  });

  // ═══════════════════════════════════════════
  // TABLE 2: UPSELL (4D)
  // upsell_initial only, cycle=0, has initial_processor
  // ═══════════════════════════════════════════
  console.log('\n═══ TABLE 2: UPSELL (4D) ═══');
  const UPS_FILTER = `${BASE} AND source='order_direct' AND derived_cycle=0 AND derived_product_role='upsell_initial'`;

  const ups4d = q(
    `SELECT issuer_bank, ${CTM} as ctm, initial_processor, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${UPS_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL AND initial_processor != 'UNKNOWN' ` +
    `GROUP BY issuer_bank, ctm, initial_processor, target HAVING n >= ${SOFT_MIN}`
  );
  const ups4dEntries = buildEntries(ups4d, r => `${r.issuer_bank}|${r.ctm}|${r.initial_processor}|${r.target}`);
  summarize('4D (issuer×card_type×init_proc×target)', ups4dEntries);

  const ups3d = q(
    `SELECT issuer_bank, initial_processor, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${UPS_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL AND initial_processor != 'UNKNOWN' ` +
    `GROUP BY issuer_bank, initial_processor, target HAVING n >= ${SOFT_MIN}`
  );
  const ups3dEntries = buildEntries(ups3d, r => `${r.issuer_bank}|${r.initial_processor}|${r.target}`);
  summarize('3D fallback (issuer×init_proc×target)', ups3dEntries);

  const ups2d = q(
    `SELECT issuer_bank, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${UPS_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL ` +
    `GROUP BY issuer_bank, target HAVING n >= ${SOFT_MIN}`
  );
  const ups2dEntries = buildEntries(ups2d, r => `${r.issuer_bank}|${r.target}`);
  summarize('2D fallback (issuer×target)', ups2dEntries);

  const ups4dClients = q(
    `SELECT client_id, issuer_bank, ${CTM} as ctm, initial_processor, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${UPS_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL AND initial_processor != 'UNKNOWN' ` +
    `GROUP BY client_id, issuer_bank, ctm, initial_processor, target HAVING n >= ${CLIENT_OVERRIDE_MIN}`
  );
  const ups4dOverrides = buildClientOverrides(ups4dClients, r => `${r.issuer_bank}|${r.ctm}|${r.initial_processor}|${r.target}`);

  const upsTotal = q(`SELECT COUNT(*) as n FROM transaction_attempts WHERE ${UPS_FILTER} AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL AND initial_processor != 'UNKNOWN'`)[0].n;
  const upsCovered = ups4d.filter(r => r.n >= HARD_MIN).reduce((s, r) => s + r.n, 0);
  console.log(`  Coverage: ${(100 * upsCovered / upsTotal).toFixed(1)}% of ${upsTotal} attempts at 35+ threshold`);

  saveTable('upsell_lookup.json', {
    metadata: { built_at: now, table: 'upsell', dimensions: '4D', threshold: THRESHOLD,
      filter: 'derived_cycle=0, derived_product_role=upsell_initial',
      total_attempts: upsTotal, total_4d: Object.keys(ups4dEntries).length,
      total_3d: Object.keys(ups3dEntries).length, total_2d: Object.keys(ups2dEntries).length },
    tier_4d: ups4dEntries,
    tier_3d: ups3dEntries,
    tier_2d: ups2dEntries,
    client_overrides_4d: ups4dOverrides,
  });

  // ═══════════════════════════════════════════
  // TABLE 3: REBILL FIRST-ATTEMPT (4D)
  // ═══════════════════════════════════════════
  console.log('\n═══ TABLE 3: REBILL FIRST-ATTEMPT (4D) ═══');
  const REB_FILTER = `${BASE} AND source='order_direct' AND derived_cycle IN (1,2) AND derived_attempt = 1`;

  const reb4d = q(
    `SELECT issuer_bank, ${CTM} as ctm, initial_processor, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${REB_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL AND initial_processor != 'UNKNOWN' ` +
    `GROUP BY issuer_bank, ctm, initial_processor, target HAVING n >= ${SOFT_MIN}`
  );
  const reb4dEntries = buildEntries(reb4d, r => `${r.issuer_bank}|${r.ctm}|${r.initial_processor}|${r.target}`);
  summarize('4D (issuer×card_type×init_proc×target) C1-C2', reb4dEntries);

  const reb3d = q(
    `SELECT issuer_bank, initial_processor, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${REB_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL AND initial_processor != 'UNKNOWN' ` +
    `GROUP BY issuer_bank, initial_processor, target HAVING n >= ${SOFT_MIN}`
  );
  const reb3dEntries = buildEntries(reb3d, r => `${r.issuer_bank}|${r.initial_processor}|${r.target}`);
  summarize('3D fallback (issuer×init_proc×target)', reb3dEntries);

  const reb2d = q(
    `SELECT issuer_bank, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${REB_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL ` +
    `GROUP BY issuer_bank, target HAVING n >= ${SOFT_MIN}`
  );
  const reb2dEntries = buildEntries(reb2d, r => `${r.issuer_bank}|${r.target}`);
  summarize('2D fallback (issuer×target)', reb2dEntries);

  const reb4dClients = q(
    `SELECT client_id, issuer_bank, ${CTM} as ctm, initial_processor, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${REB_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL AND initial_processor != 'UNKNOWN' ` +
    `GROUP BY client_id, issuer_bank, ctm, initial_processor, target HAVING n >= ${CLIENT_OVERRIDE_MIN}`
  );
  const reb4dOverrides = buildClientOverrides(reb4dClients, r => `${r.issuer_bank}|${r.ctm}|${r.initial_processor}|${r.target}`);

  const rebTotal = q(`SELECT COUNT(*) as n FROM transaction_attempts WHERE ${REB_FILTER} AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL AND initial_processor != 'UNKNOWN'`)[0].n;
  const rebCovered = reb4d.filter(r => r.n >= HARD_MIN).reduce((s, r) => s + r.n, 0);
  console.log(`  Coverage: ${(100 * rebCovered / rebTotal).toFixed(1)}% of ${rebTotal} attempts at 35+ threshold`);

  saveTable('rebill_first_attempt_lookup.json', {
    metadata: { built_at: now, table: 'rebill_first_attempt', dimensions: '4D', threshold: THRESHOLD,
      scope: 'C1-C2 only', total_attempts: rebTotal,
      total_4d: Object.keys(reb4dEntries).length, total_3d: Object.keys(reb3dEntries).length,
      total_2d: Object.keys(reb2dEntries).length },
    tier_4d: reb4dEntries,
    tier_3d: reb3dEntries,
    tier_2d: reb2dEntries,
    client_overrides_4d: reb4dOverrides,
  });

  // ═══════════════════════════════════════════
  // TABLE 4: REBILL SALVAGE (4D primary + 5D optional)
  // ═══════════════════════════════════════════
  console.log('\n═══ TABLE 4: REBILL SALVAGE (4D+5D) ═══');
  const SALV_FILTER = `${BASE} AND source='order_direct' AND derived_cycle >= 1 AND derived_attempt > 1`;

  const salv4d = q(
    `SELECT decline_reason, issuer_bank, parent_declined_processor as failed, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${SALV_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL ` +
    `AND decline_reason IS NOT NULL AND parent_declined_processor IS NOT NULL AND parent_declined_processor != 'UNKNOWN' ` +
    `GROUP BY decline_reason, issuer_bank, failed, target HAVING n >= ${SOFT_MIN}`
  );
  const salv4dEntries = buildEntries(salv4d, r => `${r.decline_reason}|${r.issuer_bank}|${r.failed}|${r.target}`);
  summarize('4D primary (decline×issuer×failed×target)', salv4dEntries);

  const salv5d = q(
    `SELECT decline_reason, issuer_bank, ${CTM} as ctm, parent_declined_processor as failed, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${SALV_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL ` +
    `AND decline_reason IS NOT NULL AND parent_declined_processor IS NOT NULL AND parent_declined_processor != 'UNKNOWN' ` +
    `GROUP BY decline_reason, issuer_bank, ctm, failed, target HAVING n >= ${HARD_MIN}`
  );
  const salv5dEntries = buildEntries(salv5d, r => `${r.decline_reason}|${r.issuer_bank}|${r.ctm}|${r.failed}|${r.target}`);
  summarize('5D optional (decline×issuer×card_type×failed×target)', salv5dEntries);

  const salv3d = q(
    `SELECT issuer_bank, parent_declined_processor as failed, processor_name as target, ` +
    `COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved ` +
    `FROM transaction_attempts WHERE ${SALV_FILTER} ` +
    `AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND parent_declined_processor IS NOT NULL ` +
    `GROUP BY issuer_bank, failed, target HAVING n >= ${SOFT_MIN}`
  );
  const salv3dEntries = buildEntries(salv3d, r => `${r.issuer_bank}|${r.failed}|${r.target}`);
  summarize('3D fallback (issuer×failed×target)', salv3dEntries);

  const salvTotal = q(`SELECT COUNT(*) as n FROM transaction_attempts WHERE ${SALV_FILTER} AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND decline_reason IS NOT NULL AND parent_declined_processor IS NOT NULL`)[0].n;
  const salvCovered = salv4d.filter(r => r.n >= HARD_MIN).reduce((s, r) => s + r.n, 0);
  console.log(`  Coverage: ${(100 * salvCovered / salvTotal).toFixed(1)}% of ${salvTotal} attempts at 35+ threshold`);

  saveTable('rebill_salvage_lookup.json', {
    metadata: { built_at: now, table: 'rebill_salvage', dimensions: '4D+5D', threshold: THRESHOLD,
      total_attempts: salvTotal, total_5d: Object.keys(salv5dEntries).length,
      total_4d: Object.keys(salv4dEntries).length, total_3d: Object.keys(salv3dEntries).length },
    tier_5d_optional: salv5dEntries,
    tier_4d: salv4dEntries,
    tier_3d: salv3dEntries,
  });

  // ═══════════════════════════════════════════
  // FINAL SUMMARY
  // ═══════════════════════════════════════════
  console.log('\n╔═══════════════════════════════════════════╗');
  console.log('║              FINAL SUMMARY                 ║');
  console.log('╚═══════════════════════════════════════════╝');

  const tables = [
    { name: 'Main Initial (3D)', primary: init3dEntries, fallback: init2dEntries, total: initTotal },
    { name: 'Upsell (4D)', primary: ups4dEntries, fallback: ups3dEntries, total: upsTotal },
    { name: 'Rebill (4D)', primary: reb4dEntries, fallback: reb3dEntries, total: rebTotal },
    { name: 'Salvage (4D+5D)', primary: salv4dEntries, refine: salv5dEntries, fallback: salv3dEntries, total: salvTotal },
  ];

  for (const t of tables) {
    const primary = Object.values(t.primary);
    const hard = primary.filter(e => e.action === 'hard_exclude').length;
    const soft = primary.filter(e => e.action === 'soft_downrank').length;
    const allow = primary.filter(e => e.action === 'allow').length;
    const covered = primary.filter(e => e.sample_size >= HARD_MIN).reduce((s, e) => s + e.sample_size, 0);
    const refineCount = t.refine ? Object.keys(t.refine).length : 0;
    const fallbackCount = Object.keys(t.fallback).length;
    console.log(`\n  ${t.name}:`);
    console.log(`    Primary: ${primary.length} entries (${hard} hard, ${soft} soft, ${allow} allow)`);
    console.log(`    Coverage: ${(100 * covered / t.total).toFixed(1)}% of ${t.total} attempts`);
    if (refineCount) console.log(`    Refinement: ${refineCount} entries`);
    console.log(`    Fallback: ${fallbackCount} entries`);
  }

  console.log('\n  All tables saved to: ' + OUTPUT_DIR);
  process.exit(0);
})();
