/**
 * Quick validation of the lookup engine against built tables.
 * Run on server after tables are built.
 */
const { initDb } = require('../src/db/connection');
const le = require('../src/routing/lookup-engine');

(async () => {
  await initDb();

  const tables = le.loadTables();
  console.log('Tables loaded:');
  for (const [name, data] of Object.entries(tables)) {
    if (!data) { console.log(`  ${name}: NOT FOUND`); continue; }
    const tiers = Object.keys(data).filter(k => k.startsWith('tier_'));
    console.log(`  ${name}: ${tiers.length} tiers — ${tiers.map(t => `${t}: ${Object.keys(data[t]).length}`).join(', ')}`);
  }

  console.log('\n═══ TEST: INITIAL LOOKUP ═══');
  // Test with a known high-volume issuer
  const initResult = le.filterInitialCandidates(
    'SUTTON BANK', 'DEBIT', 1, // prepaid debit from Sutton Bank
    ['KURV', 'NETEVIA', 'PAYSAFE', 'CELERO', 'PRIORITY'],
    null
  );
  console.log('  Input: Sutton Bank PREPAID → 5 candidates');
  console.log('  Result:', JSON.stringify(initResult.log));
  console.log('  Remaining:', initResult.candidates.join(', '));
  if (initResult.excluded.length > 0) {
    console.log('  Excluded:', initResult.excluded.map(e => `${e.processor} (${e.lookup?.tier}: ${e.lookup?.approval_rate})`).join(', '));
  }
  if (initResult.downranked.length > 0) {
    console.log('  Downranked:', initResult.downranked.map(e => `${e.processor} (${e.lookup?.tier}: ${e.lookup?.approval_rate})`).join(', '));
  }

  console.log('\n═══ TEST: CASCADE LOOKUP ═══');
  const cascResult = le.filterCascadeCandidates(
    'SUTTON BANK', 'DEBIT', 1, 'KURV',
    ['NETEVIA', 'PAYSAFE', 'CELERO', 'PRIORITY', 'PAYARC']
  );
  console.log('  Input: Sutton Bank PREPAID, failed on KURV → 5 cascade candidates');
  console.log('  Result:', JSON.stringify(cascResult.log));
  console.log('  Remaining:', cascResult.candidates.join(', '));

  console.log('\n═══ TEST: REBILL LOOKUP (C1) ═══');
  const rebResult = le.filterRebillCandidates(
    'SUTTON BANK', 'DEBIT', 1, 'KURV',
    ['KURV', 'NETEVIA', 'PAYSAFE', 'CELERO'],
    1, // cycle 1
    null
  );
  console.log('  Input: Sutton Bank PREPAID, initial on KURV, cycle 1 → 4 candidates');
  console.log('  Result:', JSON.stringify(rebResult.log));
  console.log('  Remaining:', rebResult.candidates.join(', '));

  console.log('\n═══ TEST: REBILL LOOKUP (C5 — should skip) ═══');
  const rebC5 = le.filterRebillCandidates(
    'SUTTON BANK', 'DEBIT', 1, 'KURV',
    ['KURV', 'NETEVIA', 'PAYSAFE', 'CELERO'],
    5, // C5 — should skip lookup
    null
  );
  console.log('  Input: Same but cycle 5');
  console.log('  Result:', JSON.stringify(rebC5.log));
  console.log('  Skipped:', rebC5.log.skipped || 'no');

  console.log('\n═══ TEST: SALVAGE LOOKUP ═══');
  const salvResult = le.filterSalvageCandidates(
    'DO NOT HONOR', 'SUTTON BANK', 'DEBIT', 1, 'KURV',
    ['NETEVIA', 'PAYSAFE', 'CELERO', 'PRIORITY']
  );
  console.log('  Input: DO NOT HONOR, Sutton Bank PREPAID, failed on KURV → 4 candidates');
  console.log('  Result:', JSON.stringify(salvResult.log));
  console.log('  Remaining:', salvResult.candidates.join(', '));

  console.log('\n═══ TEST: MIN-2 SAFEGUARD ═══');
  // Try with only 2 candidates where both might be excluded
  const safeResult = le.filterCascadeCandidates(
    'SUTTON BANK', 'DEBIT', 1, 'KURV',
    ['NETEVIA', 'PAYSAFE'] // only 2 — if both excluded, safeguard triggers
  );
  console.log('  Input: 2 cascade candidates (safeguard test)');
  console.log('  Result:', JSON.stringify(safeResult.log));
  console.log('  Safeguard triggered:', safeResult.log.safeguard_triggered);

  process.exit(0);
})();
