/**
 * Comprehensive lookup engine tests — v2.
 * Tables: Main Initial (3D), Upsell (4D), Rebill (4D), Salvage (4D+5D).
 */
const le = require('../src/routing/lookup-engine');

let passed = 0;
let failed = 0;

function assert(name, condition, detail) {
  if (condition) { console.log('  ✓ ' + name); passed++; }
  else { console.log('  ✗ ' + name + (detail ? ' — ' + detail : '')); failed++; }
}

const tables = le.loadTables();
console.log('Tables loaded:');
for (const [name, data] of Object.entries(tables)) {
  if (!data) { console.log('  ' + name + ': NOT FOUND'); continue; }
  const tiers = Object.keys(data).filter(k => k.startsWith('tier_'));
  console.log('  ' + name + ': ' + tiers.map(t => t + '=' + Object.keys(data[t]).length).join(', '));
}

// ═══ 1. cardTypeMerged ═══
console.log('\n═══ 1. cardTypeMerged() ═══');
assert('prepaid=1 → PREPAID', le.cardTypeMerged('DEBIT', 1) === 'PREPAID');
assert('prepaid="1" → PREPAID', le.cardTypeMerged('DEBIT', '1') === 'PREPAID');
assert('prepaid=true → PREPAID', le.cardTypeMerged('DEBIT', true) === 'PREPAID');
assert('prepaid=0 CREDIT → CREDIT', le.cardTypeMerged('CREDIT', 0) === 'CREDIT');
assert('prepaid=0 DEBIT → DEBIT', le.cardTypeMerged('DEBIT', 0) === 'DEBIT');
assert('prepaid=0 null → null', le.cardTypeMerged(null, 0) === null);

// ═══ 2. INITIAL (3D) ═══
console.log('\n═══ 2. MAIN INITIAL ═══');
const init3dKeys = Object.keys(tables.initial.tier_3d);
const init2dKeys = Object.keys(tables.initial.tier_2d);

if (init3dKeys.length > 0) {
  const [issuer, ctm, target] = init3dKeys[0].split('|');
  const r = le.queryInitial(issuer, ctm === 'PREPAID' ? 'DEBIT' : ctm, ctm === 'PREPAID' ? 1 : 0, target);
  assert('3D hit returns tier=3D', r && r.tier === '3D');
  assert('Has approval_rate', r && typeof r.approval_rate === 'number');
  assert('Has sample_size >= 20', r && r.sample_size >= 20);
  assert('Has valid action', r && ['allow', 'hard_exclude', 'soft_downrank'].includes(r.action));
}

assert('Unknown → null', le.queryInitial('NONEXISTENT_XYZ', 'CREDIT', 0, 'NONEXISTENT') === null);
assert('Null issuer → null', le.queryInitial(null, null, 0, 'KURV') === null);

const initFilter = le.filterInitialCandidates('SOME_BANK', 'CREDIT', 0, ['KURV'], null);
assert('Single candidate stays', initFilter.candidates.length === 1);

// ═══ 3. UPSELL (4D) ═══
console.log('\n═══ 3. UPSELL ═══');
const ups4dKeys = Object.keys(tables.upsell?.tier_4d || {});
const ups3dKeys = Object.keys(tables.upsell?.tier_3d || {});
const ups2dKeys = Object.keys(tables.upsell?.tier_2d || {});

if (ups4dKeys.length > 0) {
  const [issuer, ctm, initProc, target] = ups4dKeys[0].split('|');
  const r = le.queryUpsell(issuer, ctm === 'PREPAID' ? 'DEBIT' : ctm, ctm === 'PREPAID' ? 1 : 0, initProc, target);
  assert('4D upsell hit', r && r.tier === '4D');
  assert('Has approval_rate', r && typeof r.approval_rate === 'number');
} else {
  console.log('  (no 4D entries — checking fallback)');
}

if (ups3dKeys.length > 0) {
  const [issuer, initProc, target] = ups3dKeys[0].split('|');
  const r = le.queryUpsell(issuer, 'NONEXIST_CT', 0, initProc, target);
  assert('3D fallback hit', r && (r.tier === '3D' || r.tier === '4D'));
}

if (ups2dKeys.length > 0) {
  const [issuer, target] = ups2dKeys[0].split('|');
  const r = le.queryUpsell(issuer, 'NONEXIST_CT', 0, 'NONEXIST_PROC', target);
  assert('2D fallback hit', r && r.tier === '2D');
}

assert('Unknown upsell → null', le.queryUpsell('NONEXISTENT_XYZ', 'CREDIT', 0, 'X', 'Y') === null);

// Test filter
if (ups4dKeys.length > 0) {
  const [issuer, ctm, initProc] = ups4dKeys[0].split('|');
  const targets = [...new Set(ups4dKeys.filter(k => k.startsWith(issuer + '|' + ctm + '|' + initProc + '|')).map(k => k.split('|')[3]))].slice(0, 5);
  if (targets.length >= 2) {
    const result = le.filterUpsellCandidates(issuer, ctm === 'PREPAID' ? 'DEBIT' : ctm, ctm === 'PREPAID' ? 1 : 0, initProc, targets);
    assert('Upsell filter returns candidates', result.candidates.length > 0);
    assert('Upsell filter has log', result.log.table === 'upsell');
  }
}

// ═══ 4. REBILL ═══
console.log('\n═══ 4. REBILL ═══');
const reb4dKeys = Object.keys(tables.rebill.tier_4d);

if (reb4dKeys.length > 0) {
  const [issuer, ctm, initProc, target] = reb4dKeys[0].split('|');
  const isPrepaid = ctm === 'PREPAID' ? 1 : 0;
  const cardType = ctm === 'PREPAID' ? 'DEBIT' : ctm;

  assert('C1 hit', le.queryRebill(issuer, cardType, isPrepaid, initProc, target, 1)?.tier === '4D');
  assert('C2 hit', le.queryRebill(issuer, cardType, isPrepaid, initProc, target, 2) !== null);
  assert('C3 skip', le.queryRebill(issuer, cardType, isPrepaid, initProc, target, 3) === null);
  assert('C5 skip', le.queryRebill(issuer, cardType, isPrepaid, initProc, target, 5) === null);
  assert('C10 skip', le.queryRebill(issuer, cardType, isPrepaid, initProc, target, 10) === null);

  const filterC3 = le.filterRebillCandidates(issuer, cardType, isPrepaid, initProc, [target], 3);
  assert('C3 filter skips', filterC3.log.skipped === 'C3+');
}

// ═══ 5. SALVAGE ═══
console.log('\n═══ 5. SALVAGE ═══');
const salv4dKeys = Object.keys(tables.salvage.tier_4d);
const salv5dKeys = Object.keys(tables.salvage.tier_5d_optional);

if (salv4dKeys.length > 0) {
  const [decline, issuer, failed, target] = salv4dKeys[0].split('|');
  const r = le.querySalvage(decline, issuer, 'CREDIT', 0, failed, target);
  assert('4D salvage hit', r && (r.tier === '4D' || r.tier === '5D' || r.tier === '3D'));
}

if (salv5dKeys.length > 0) {
  const [decline, issuer, ctm, failed, target] = salv5dKeys[0].split('|');
  const r = le.querySalvage(decline, issuer, ctm === 'PREPAID' ? 'DEBIT' : ctm, ctm === 'PREPAID' ? 1 : 0, failed, target);
  assert('5D salvage hit when available', r && r.tier === '5D', 'got tier=' + r?.tier);
}

// ═══ 6. SAFEGUARD ═══
console.log('\n═══ 6. Min-2 Safeguard ═══');

// Find two excluded entries from same context
const salvExcluded = salv4dKeys.filter(k => tables.salvage.tier_4d[k].action === 'hard_exclude');
if (salvExcluded.length >= 2) {
  const [d1, i1, f1, t1] = salvExcluded[0].split('|');
  const sameContext = salvExcluded.find(k => {
    const [d, i, f] = k.split('|');
    return d === d1 && i === i1 && f === f1 && k !== salvExcluded[0];
  });
  if (sameContext) {
    const t2 = sameContext.split('|')[3];
    const result = le.filterSalvageCandidates(d1, i1, 'CREDIT', 0, f1, [t1, t2]);
    assert('Both excluded → safeguard fires', result.log.safeguard_triggered === true);
    assert('Both excluded → candidates restored', result.candidates.length === 2);
  }
}

// ═══ 7. DATA QUALITY ═══
console.log('\n═══ 7. Data Quality ═══');
for (const [tableName, data] of Object.entries(tables)) {
  if (!data) continue;
  for (const tierName of Object.keys(data).filter(k => k.startsWith('tier_'))) {
    const entries = Object.values(data[tierName]);
    if (entries.length === 0) continue;
    const negatives = entries.filter(e => e.approval_rate < 0);
    const overOne = entries.filter(e => e.approval_rate > 1);
    const noSample = entries.filter(e => !e.sample_size || e.sample_size < 1);
    assert(tableName + '/' + tierName + ': no negative rates', negatives.length === 0);
    assert(tableName + '/' + tierName + ': no rates > 1', overOne.length === 0);
    assert(tableName + '/' + tierName + ': all have sample_size', noSample.length === 0);

    // Classification check (using raw rate to avoid rounding edge case)
    let misclassified = 0;
    for (const [key, e] of Object.entries(data[tierName])) {
      const rawRate = e.approved / e.sample_size;
      if (e.sample_size >= 35 && rawRate < 0.05 && e.action !== 'hard_exclude') misclassified++;
      if (e.sample_size >= 20 && e.sample_size < 35 && rawRate < 0.05 && e.action !== 'soft_downrank') misclassified++;
      if (rawRate >= 0.05 && e.action !== 'allow') misclassified++;
    }
    assert(tableName + '/' + tierName + ': actions correct', misclassified === 0, misclassified + ' misclassified');
  }
}

// ═══ 8. RETURN SHAPES ═══
console.log('\n═══ 8. Return Shapes ═══');
for (const [name, fn] of [
  ['initial', () => le.filterInitialCandidates('X', 'CREDIT', 0, ['A', 'B'])],
  ['upsell', () => le.filterUpsellCandidates('X', 'CREDIT', 0, 'Y', ['A', 'B'])],
  ['rebill', () => le.filterRebillCandidates('X', 'CREDIT', 0, 'Y', ['A', 'B'], 1)],
  ['salvage', () => le.filterSalvageCandidates('DNH', 'X', 'CREDIT', 0, 'Y', ['A', 'B'])],
]) {
  const r = fn();
  assert(name + ': has candidates[]', Array.isArray(r.candidates));
  assert(name + ': has excluded[]', Array.isArray(r.excluded));
  assert(name + ': has downranked[]', Array.isArray(r.downranked));
  assert(name + ': has log.table', r.log.table === name);
  assert(name + ': has log.safeguard_triggered', typeof r.log.safeguard_triggered === 'boolean');
}

// ═══ 9. NO CASCADE ═══
console.log('\n═══ 9. Cascade Removed ═══');
assert('No queryCascade export', typeof le.queryCascade === 'undefined');
assert('No filterCascadeCandidates export', typeof le.filterCascadeCandidates === 'undefined');
assert('queryUpsell exists', typeof le.queryUpsell === 'function');
assert('filterUpsellCandidates exists', typeof le.filterUpsellCandidates === 'function');

// ═══ SUMMARY ═══
console.log('\n' + '═'.repeat(50));
console.log('RESULTS: ' + passed + ' passed, ' + failed + ' failed');
if (failed === 0) console.log('ALL TESTS PASSED');
else console.log('SOME TESTS FAILED');
process.exit(failed > 0 ? 1 : 0);
