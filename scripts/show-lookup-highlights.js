/**
 * Show interesting highlights from the 4 lookup tables.
 *  - Top hard-excludes (most data, lowest approval)
 *  - Top high-approval combos (most data, highest approval)
 *  - Distribution stats
 */
const fs = require('fs');
const path = require('path');

const DIR = path.join(__dirname, '..', 'data', 'models');

function loadTable(name) {
  const p = path.join(DIR, name);
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function showTable(label, table, primaryKey) {
  console.log('═'.repeat(80));
  console.log(label);
  console.log('═'.repeat(80));

  const primary = table[primaryKey] || {};
  const entries = Object.entries(primary);
  console.log('Total primary entries: ' + entries.length);
  console.log();

  // Distribution by action
  const counts = { hard_exclude: 0, soft_downrank: 0, allow: 0 };
  for (const [, v] of entries) counts[v.action]++;
  console.log('  hard_exclude: ' + counts.hard_exclude);
  console.log('  soft_downrank: ' + counts.soft_downrank);
  console.log('  allow:        ' + counts.allow);
  console.log();

  // Top 5 hard excludes by sample size
  const hardExcludes = entries
    .filter(([, v]) => v.action === 'hard_exclude')
    .sort((a, b) => b[1].sample_size - a[1].sample_size)
    .slice(0, 5);
  if (hardExcludes.length > 0) {
    console.log('Top 5 HARD EXCLUDES (lowest approval, most data):');
    hardExcludes.forEach(([k, v]) => {
      console.log('  ' + (v.approval_rate * 100).toFixed(1) + '% (' + v.sample_size + ' samples)  ' + k.substring(0, 80));
    });
    console.log();
  }

  // Top 5 highest approval (allow) by sample size
  const topAllows = entries
    .filter(([, v]) => v.action === 'allow' && v.sample_size >= 50)
    .sort((a, b) => b[1].approval_rate - a[1].approval_rate)
    .slice(0, 5);
  if (topAllows.length > 0) {
    console.log('Top 5 BEST APPROVAL combos (50+ samples):');
    topAllows.forEach(([k, v]) => {
      console.log('  ' + (v.approval_rate * 100).toFixed(1) + '% (' + v.sample_size + ' samples)  ' + k.substring(0, 80));
    });
    console.log();
  }
}

const initial = loadTable('initial_lookup.json');
const upsell = loadTable('upsell_lookup.json');
const rebillFA = loadTable('rebill_first_attempt_lookup.json');
const salvage = loadTable('rebill_salvage_lookup.json');

showTable('TABLE 1: MAIN INITIAL (3D: issuer × card_type × target)', initial, 'tier_3d');
showTable('TABLE 2: UPSELL (4D: issuer × card_type × init_proc × target)', upsell, 'tier_4d');
showTable('TABLE 3: REBILL FIRST ATTEMPT (4D: issuer × card_type × init_proc × target)', rebillFA, 'tier_4d');
showTable('TABLE 4: REBILL SALVAGE (4D: decline × issuer × failed × target)', salvage, 'tier_4d');
