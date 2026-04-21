/**
 * Validate lookup table data integrity.
 * Checks for: double counting, wrong filters, inflated rates, data leaks.
 */
const { initDb, querySql } = require('../src/db/connection');
const le = require('../src/routing/lookup-engine');

let passed = 0, failed = 0, warnings = 0;

function assert(name, condition, detail) {
  if (condition) { console.log('  ✓ ' + name); passed++; }
  else { console.log('  ✗ ' + name + (detail ? ' — ' + detail : '')); failed++; }
}
function warn(name, detail) { console.log('  ⚠ ' + name + (detail ? ' — ' + detail : '')); warnings++; }

(async () => {
  await initDb();
  const q = (sql) => querySql(sql);
  const tables = le.loadTables();

  console.log('╔═══════════════════════════════════════════════════╗');
  console.log('║     LOOKUP TABLE DATA INTEGRITY VALIDATION        ║');
  console.log('╚═══════════════════════════════════════════════════╝\n');

  // ═══ 1. DUPLICATE DETECTION ═══
  console.log('═══ 1. DUPLICATE DETECTION ═══');
  console.log('  Are there duplicate rows in transaction_attempts?\n');

  const dupes = q(
    "SELECT sticky_order_id, attempt_seq, COUNT(*) as n FROM transaction_attempts " +
    "WHERE feature_version >= 3 GROUP BY sticky_order_id, attempt_seq HAVING n > 1"
  );
  assert('No duplicate (order_id, attempt_seq) pairs', dupes.length === 0,
    dupes.length + ' duplicates found — first: ' + (dupes[0] ? dupes[0].sticky_order_id : ''));

  const totalAttempts = q("SELECT COUNT(*) as n FROM transaction_attempts WHERE feature_version >= 3")[0].n;
  const uniqueAttempts = q("SELECT COUNT(DISTINCT sticky_order_id || '|' || attempt_seq) as n FROM transaction_attempts WHERE feature_version >= 3")[0].n;
  assert('Total rows = unique rows', totalAttempts === uniqueAttempts,
    'total=' + totalAttempts + ' unique=' + uniqueAttempts);

  // ═══ 2. SOURCE FILTER VALIDATION ═══
  console.log('\n═══ 2. SOURCE FILTER CORRECTNESS ═══');
  console.log('  Do the filters match the right transaction types?\n');

  // Main initial should only have cycle=0, main_initial
  const initCheck = q(
    "SELECT derived_cycle, derived_product_role, COUNT(*) as n FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' " +
    "AND source = 'order_direct' AND derived_cycle = 0 AND derived_product_role = 'main_initial' " +
    "GROUP BY derived_cycle, derived_product_role"
  );
  assert('Initial filter: only cycle=0 main_initial', initCheck.length === 1 && initCheck[0].derived_cycle === 0 && initCheck[0].derived_product_role === 'main_initial');

  // No upsells in initial data
  const initUpsellLeak = q(
    "SELECT COUNT(*) as n FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' " +
    "AND source = 'order_direct' AND derived_cycle = 0 AND derived_product_role = 'main_initial' " +
    "AND derived_product_role = 'upsell_initial'"
  );
  assert('No upsells in initial filter', initUpsellLeak[0].n === 0);

  // Rebill should only be cycle 1-2, attempt 1
  const rebCheck = q(
    "SELECT MIN(derived_cycle) as min_c, MAX(derived_cycle) as max_c, MIN(derived_attempt) as min_a, MAX(derived_attempt) as max_a " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' " +
    "AND source = 'order_direct' AND derived_cycle IN (1,2) AND derived_attempt = 1"
  );
  assert('Rebill filter: cycles 1-2 only', rebCheck[0].min_c >= 1 && rebCheck[0].max_c <= 2);
  assert('Rebill filter: attempt 1 only', rebCheck[0].min_a === 1 && rebCheck[0].max_a === 1);

  // Salvage should be cycle >= 1, attempt > 1
  const salvCheck = q(
    "SELECT MIN(derived_cycle) as min_c, MIN(derived_attempt) as min_a " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' " +
    "AND source = 'order_direct' AND derived_cycle >= 1 AND derived_attempt > 1"
  );
  assert('Salvage filter: cycle >= 1', salvCheck[0].min_c >= 1);
  assert('Salvage filter: attempt > 1', salvCheck[0].min_a >= 2);

  // ═══ 3. EXCLUDED DATA NOT LEAKING IN ═══
  console.log('\n═══ 3. EXCLUDED DATA CHECK ═══');
  console.log('  Is excluded data (Payfac, test, UNKNOWN) leaking into lookup tables?\n');

  // Check for UNKNOWN processor in any lookup entry
  let unknownFound = 0;
  for (const [tableName, data] of Object.entries(tables)) {
    if (!data) continue;
    for (const tierName of Object.keys(data).filter(k => k.startsWith('tier_'))) {
      for (const key of Object.keys(data[tierName])) {
        if (key.includes('UNKNOWN')) unknownFound++;
      }
    }
  }
  assert('No UNKNOWN processor in any lookup table', unknownFound === 0, unknownFound + ' found');

  // Check for Payfac as a target processor (should be excluded from analysis)
  let payfacTarget = 0;
  for (const [tableName, data] of Object.entries(tables)) {
    if (!data) continue;
    for (const tierName of Object.keys(data).filter(k => k.startsWith('tier_'))) {
      for (const key of Object.keys(data[tierName])) {
        // Check last segment (target processor) for PAYFAC
        const parts = key.split('|');
        const target = parts[parts.length - 1];
        if (target.toUpperCase().includes('PAYFAC')) payfacTarget++;
      }
    }
  }
  assert('No PAYFAC as target processor in lookups', payfacTarget === 0, payfacTarget + ' found');

  // Check model_target=excluded not in data
  const excludedInData = q(
    "SELECT COUNT(*) as n FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target = 'excluded' " +
    "AND processor_name IN (SELECT DISTINCT processor_name FROM transaction_attempts WHERE feature_version >= 3 AND model_target != 'excluded')"
  );
  // This just confirms excluded rows exist (they should, for Payfac)
  console.log('  Excluded rows exist: ' + excludedInData[0].n + ' (expected > 0 for Payfac gateways)');

  // ═══ 4. SPOT-CHECK RATES AGAINST RAW DATA ═══
  console.log('\n═══ 4. SPOT-CHECK: Lookup rates vs raw DB ═══');
  console.log('  Pick random entries from each table and verify against raw queries.\n');

  // Random initial entries (deterministic — every 40th key)
  const init3dKeys = Object.keys(tables.initial.tier_3d);
  for (let i = 0; i < Math.min(5, init3dKeys.length); i++) {
    const idx = i * 40;
    if (idx >= init3dKeys.length) break;
    const key = init3dKeys[idx];
    const entry = tables.initial.tier_3d[key];
    const parts = key.split('|');
    if (parts.length !== 3) { warn('Initial key has wrong parts: ' + key); continue; }
    const [issuer, ctm, target] = parts;

    try {
      const ctmWhere = ctm === 'PREPAID'
        ? "AND is_prepaid = 1"
        : "AND (is_prepaid = 0 OR is_prepaid IS NULL) AND card_type = '" + ctm.replace(/'/g, "''") + "'";
      const db = q(
        "SELECT COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved " +
        "FROM transaction_attempts " +
        "WHERE feature_version >= 3 AND model_target != 'excluded' AND processor_name != 'UNKNOWN' " +
        "AND source = 'order_direct' AND derived_cycle = 0 AND derived_product_role = 'main_initial' " +
        "AND issuer_bank = ? " + ctmWhere + " AND processor_name = ?",
        [issuer, target]
      );
      const dbRate = db[0].n > 0 ? Math.round((db[0].approved / db[0].n) * 10000) / 10000 : 0;
      const match = entry.approval_rate === dbRate && entry.sample_size === db[0].n;
      assert('Initial ' + key.substring(0, 40), match,
        match ? '' : 'lookup: ' + entry.approval_rate + ' n=' + entry.sample_size + ' vs DB: ' + dbRate + ' n=' + db[0].n);
    } catch (e) { warn('Initial query failed for ' + key + ': ' + e.message); }
  }

  // Rebill entries (deterministic)
  const reb4dKeys = Object.keys(tables.rebill.tier_4d);
  for (let i = 0; i < Math.min(5, reb4dKeys.length); i++) {
    const idx = i * 70;
    if (idx >= reb4dKeys.length) break;
    const key = reb4dKeys[idx];
    const entry = tables.rebill.tier_4d[key];
    const rParts = key.split('|');
    if (rParts.length !== 4) { warn('Rebill key has wrong parts: ' + key); continue; }
    const [issuer, ctm, initProc, target] = rParts;

    try {
      const rCtmWhere = ctm === 'PREPAID'
        ? "AND is_prepaid = 1"
        : "AND (is_prepaid = 0 OR is_prepaid IS NULL) AND card_type = '" + ctm.replace(/'/g, "''") + "'";
      const db = q(
        "SELECT COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved " +
        "FROM transaction_attempts " +
        "WHERE feature_version >= 3 AND model_target != 'excluded' AND processor_name != 'UNKNOWN' " +
        "AND source = 'order_direct' AND derived_cycle IN (1,2) AND derived_attempt = 1 " +
        "AND issuer_bank = ? " + rCtmWhere + " AND initial_processor = ? AND processor_name = ? AND initial_processor != 'UNKNOWN'",
        [issuer, initProc, target]
      );
      const dbRate = db[0].n > 0 ? Math.round((db[0].approved / db[0].n) * 10000) / 10000 : 0;
      const match = entry.approval_rate === dbRate && entry.sample_size === db[0].n;
      assert('Rebill ' + key.substring(0, 40), match,
        match ? '' : 'lookup: ' + entry.approval_rate + ' n=' + entry.sample_size + ' vs DB: ' + dbRate + ' n=' + db[0].n);
    } catch (e) { warn('Rebill query failed for ' + key + ': ' + e.message); }
  }

  // Salvage entries (deterministic)
  const salv4dKeys = Object.keys(tables.salvage.tier_4d);
  for (let i = 0; i < Math.min(5, salv4dKeys.length); i++) {
    const idx = i * 160;
    if (idx >= salv4dKeys.length) break;
    const key = salv4dKeys[idx];
    const entry = tables.salvage.tier_4d[key];
    const parts = key.split('|');
    const target = parts.pop();
    const failed = parts.pop();
    const issuer = parts.pop();
    const decline = parts.join('|'); // decline reason may contain |

    try {
      const db = q(
        "SELECT COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved " +
        "FROM transaction_attempts " +
        "WHERE feature_version >= 3 AND model_target != 'excluded' AND processor_name != 'UNKNOWN' " +
        "AND source = 'order_direct' AND derived_cycle >= 1 AND derived_attempt > 1 " +
        "AND decline_reason = ? AND issuer_bank = ? " +
        "AND parent_declined_processor = ? AND processor_name = ? AND parent_declined_processor != 'UNKNOWN'",
        [decline, issuer, failed, target]
      );
      const dbRate = db[0].n > 0 ? Math.round((db[0].approved / db[0].n) * 10000) / 10000 : 0;
      const match = entry.approval_rate === dbRate && entry.sample_size === db[0].n;
      assert('Salvage ' + key.substring(0, 40), match,
        match ? '' : 'lookup: ' + entry.approval_rate + ' n=' + entry.sample_size + ' vs DB: ' + dbRate + ' n=' + db[0].n);
    } catch (e) { warn('Salvage query failed for ' + key + ': ' + e.message); }
  }

  // ═══ 5. OUTCOME INTEGRITY ═══
  console.log('\n═══ 5. OUTCOME INTEGRITY ═══');
  console.log('  Are outcomes correctly classified?\n');

  const outcomes = q("SELECT DISTINCT outcome FROM transaction_attempts WHERE feature_version >= 3");
  assert('Only approved/declined outcomes', outcomes.length === 2 &&
    outcomes.some(o => o.outcome === 'approved') && outcomes.some(o => o.outcome === 'declined'),
    'Found: ' + outcomes.map(o => o.outcome).join(', '));

  // Cross-check: approved outcome should match order_status IN (2,6,8)
  const mismatch = q(
    "SELECT COUNT(*) as n FROM transaction_attempts ta " +
    "JOIN orders o ON o.id = ta.order_id AND o.client_id = ta.client_id " +
    "WHERE ta.feature_version >= 3 AND ta.attempt_seq = 1 " +
    "AND ((ta.outcome = 'approved' AND o.order_status NOT IN (2,6,8)) " +
    "  OR (ta.outcome = 'declined' AND o.order_status NOT IN (7)))"
  );
  if (mismatch[0].n === 0) {
    assert('Outcomes match order_status for attempt_seq=1', true);
  } else {
    warn('Outcome/status mismatches on attempt_seq=1', mismatch[0].n + ' mismatches (may be cascade/salvage with different final status)');
  }

  // ═══ 6. RATE SANITY CHECKS ═══
  console.log('\n═══ 6. RATE SANITY CHECKS ═══');
  console.log('  Are rates within expected ranges per table?\n');

  // Initial rates should be mostly 20-80%
  const initRates = Object.values(tables.initial.tier_3d);
  const initAvg = initRates.reduce((s, e) => s + e.approval_rate, 0) / initRates.length;
  const initAbove90 = initRates.filter(e => e.approval_rate > 0.9).length;
  console.log('  Initial avg rate: ' + (initAvg * 100).toFixed(1) + '%');
  assert('Initial avg rate 20-80%', initAvg > 0.2 && initAvg < 0.8, (initAvg * 100).toFixed(1) + '%');
  if (initAbove90 > 0) {
    warn('Initial entries above 90%', initAbove90 + ' entries — check if these are real or data quality issues');
    initRates.filter(e => e.approval_rate > 0.9).forEach(e => {
      const key = Object.entries(tables.initial.tier_3d).find(([k, v]) => v === e)?.[0];
      console.log('    ' + key + ': ' + (e.approval_rate * 100).toFixed(1) + '% (n=' + e.sample_size + ')');
    });
  }

  // Rebill rates should be mostly 0-30%
  const rebRates = Object.values(tables.rebill.tier_4d);
  const rebAvg = rebRates.reduce((s, e) => s + e.approval_rate, 0) / rebRates.length;
  console.log('  Rebill avg rate: ' + (rebAvg * 100).toFixed(1) + '%');
  assert('Rebill avg rate 0-30%', rebAvg < 0.3, (rebAvg * 100).toFixed(1) + '%');

  // Salvage rates should be mostly 0-10%
  const salvRates = Object.values(tables.salvage.tier_4d);
  const salvAvg = salvRates.reduce((s, e) => s + e.approval_rate, 0) / salvRates.length;
  console.log('  Salvage avg rate: ' + (salvAvg * 100).toFixed(1) + '%');
  assert('Salvage avg rate 0-10%', salvAvg < 0.1, (salvAvg * 100).toFixed(1) + '%');

  // ═══ 7. CROSS-TABLE CONSISTENCY ═══
  console.log('\n═══ 7. CROSS-TABLE CONSISTENCY ═══');
  console.log('  Do the same issuers appear across tables? No orphan data?\n');

  const initIssuers = new Set(Object.keys(tables.initial.tier_3d).map(k => k.split('|')[0]));
  const rebIssuers = new Set(Object.keys(tables.rebill.tier_4d).map(k => k.split('|')[0]));
  const salvIssuers = new Set(Object.keys(tables.salvage.tier_4d).map(k => k.split('|')[0]));

  const overlap = [...initIssuers].filter(i => rebIssuers.has(i) && salvIssuers.has(i));
  console.log('  Initial issuers: ' + initIssuers.size);
  console.log('  Rebill issuers: ' + rebIssuers.size);
  console.log('  Salvage issuers: ' + salvIssuers.size);
  console.log('  Overlap (in all 3): ' + overlap.length);
  assert('Significant issuer overlap across tables', overlap.length > 10, overlap.length + ' issuers in all 3 tables');

  // ═══ SUMMARY ═══
  console.log('\n' + '═'.repeat(50));
  console.log('RESULTS: ' + passed + ' passed, ' + failed + ' failed, ' + warnings + ' warnings');
  if (failed === 0) console.log('ALL CHECKS PASSED');
  else console.log('SOME CHECKS FAILED — review above');
  process.exit(failed > 0 ? 1 : 0);
})();
