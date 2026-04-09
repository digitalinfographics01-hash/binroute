/**
 * Full results report — real lookup outcomes across all 4 tables
 * for top issuers and processors.
 */
const le = require('../src/routing/lookup-engine');

const tables = le.loadTables();

// Top issuers by volume (from our data)
const ISSUERS = [
  'JPMORGAN CHASE BANK N.A.',
  'BANCORP BANK',
  'SUTTON BANK',
  'PATHWARD',
  'COMERICA BANK',
  'NAVY FEDERAL CREDIT UNION',
  'FIFTH THIRD BANK, THE',
  'GREEN DOT BANK DBA BONNEVILLE BANK',
  'STRIDE BANK',
  'WELLS FARGO BANK',
];

// Active processors
const PROCESSORS = ['KURV', 'NETEVIA', 'PAYSAFE', 'CELERO', 'PRIORITY', 'PAYARC', 'CLIQ', 'MERCHANT INDUSTRY', 'SIGNAPAY', 'APPS'];

// Common decline reasons
const DECLINES = ['DO NOT HONOR', 'DECLINED', 'INSUFFICIENT FUNDS', 'Issuer Declined', '51 - DECLINED'];

function pad(str, len) { return (str || '').substring(0, len).padEnd(len); }

// ═══════════════════════════════════════════
// TABLE 1: INITIAL
// ═══════════════════════════════════════════
console.log('╔═══════════════════════════════════════════════════════════════════════════════╗');
console.log('║                         TABLE 1: INITIAL LOOKUP (3D)                          ║');
console.log('╚═══════════════════════════════════════════════════════════════════════════════╝\n');

for (const cardInfo of [
  { label: 'CREDIT', ct: 'CREDIT', pp: 0 },
  { label: 'DEBIT', ct: 'DEBIT', pp: 0 },
  { label: 'PREPAID', ct: 'DEBIT', pp: 1 },
]) {
  console.log(`── ${cardInfo.label} ──`);
  console.log(pad('ISSUER', 35) + PROCESSORS.map(p => pad(p, 10)).join(''));
  console.log('-'.repeat(35 + PROCESSORS.length * 10));

  for (const issuer of ISSUERS) {
    let row = pad(issuer, 35);
    for (const proc of PROCESSORS) {
      const r = le.queryInitial(issuer, cardInfo.ct, cardInfo.pp, proc);
      if (!r) { row += pad('—', 10); continue; }
      const symbol = r.action === 'hard_exclude' ? '✗' : r.action === 'soft_downrank' ? '↓' : '';
      row += pad(`${(r.approval_rate * 100).toFixed(1)}%${symbol}`, 10);
    }
    console.log(row);
  }
  console.log();
}

// Filter simulation
console.log('── FILTER SIMULATION: Top 5 issuers with 5 candidates ──\n');
for (const issuer of ISSUERS.slice(0, 5)) {
  for (const cardInfo of [
    { label: 'CREDIT', ct: 'CREDIT', pp: 0 },
    { label: 'PREPAID', ct: 'DEBIT', pp: 1 },
  ]) {
    const result = le.filterInitialCandidates(issuer, cardInfo.ct, cardInfo.pp, PROCESSORS.slice(0, 5));
    const excluded = result.excluded.map(e => e.processor).join(', ') || 'none';
    const downranked = result.downranked.map(e => e.processor).join(', ') || 'none';
    console.log(`  ${pad(issuer, 35)} ${cardInfo.label}: ${result.candidates.length}/${result.log.input_candidates} remain | excluded: ${excluded} | downranked: ${downranked}${result.log.safeguard_triggered ? ' [SAFEGUARD]' : ''}`);
  }
}

// ═══════════════════════════════════════════
// TABLE 2: CASCADE
// ═══════════════════════════════════════════
console.log('\n╔═══════════════════════════════════════════════════════════════════════════════╗');
console.log('║                       TABLE 2: CASCADE LOOKUP (3D+4D)                         ║');
console.log('╚═══════════════════════════════════════════════════════════════════════════════╝\n');

console.log('── FILTER SIMULATION: After decline on each processor, which targets survive? ──\n');
for (const issuer of ISSUERS.slice(0, 5)) {
  console.log(`  ${issuer}:`);
  for (const failedProc of ['KURV', 'NETEVIA', 'PAYSAFE']) {
    const targets = PROCESSORS.filter(p => p !== failedProc);
    const result = le.filterCascadeCandidates(issuer, 'CREDIT', 0, failedProc, targets);
    const remaining = result.candidates.map(p => {
      const lookup = le.queryCascade(issuer, 'CREDIT', 0, failedProc, p);
      return lookup ? `${p}(${(lookup.approval_rate*100).toFixed(1)}%)` : p;
    }).join(', ');
    console.log(`    Failed ${pad(failedProc, 10)}: ${result.log.after_lookup}/${result.log.input_candidates} remain${result.log.safeguard_triggered ? ' [SAFEGUARD]' : ''}`);
    if (result.excluded.length > 0 && result.excluded.length <= 5) {
      console.log(`      Excluded: ${result.excluded.map(e => `${e.processor}(${(e.lookup.approval_rate*100).toFixed(1)}%,${e.lookup.tier})`).join(', ')}`);
    } else if (result.excluded.length > 5) {
      console.log(`      Excluded: ${result.excluded.length} processors`);
    }
  }
  console.log();
}

// ═══════════════════════════════════════════
// TABLE 3: REBILL
// ═══════════════════════════════════════════
console.log('╔═══════════════════════════════════════════════════════════════════════════════╗');
console.log('║                     TABLE 3: REBILL FIRST-ATTEMPT (4D)                        ║');
console.log('╚═══════════════════════════════════════════════════════════════════════════════╝\n');

console.log('── C1 REBILL: Initial proc → which target is best? ──\n');
for (const issuer of ISSUERS.slice(0, 5)) {
  console.log(`  ${issuer}:`);
  for (const initProc of ['KURV', 'NETEVIA', 'PAYSAFE']) {
    const targets = PROCESSORS.slice(0, 6);
    const result = le.filterRebillCandidates(issuer, 'DEBIT', 0, initProc, targets, 1);
    console.log(`    Init ${pad(initProc, 10)} C1: ${result.log.after_lookup}/${result.log.input_candidates} remain`);
    if (result.excluded.length > 0) {
      console.log(`      Excluded: ${result.excluded.map(e => `${e.processor}(${(e.lookup.approval_rate*100).toFixed(1)}%,n=${e.lookup.sample_size},${e.lookup.tier})`).join(', ')}`);
    }
    if (result.downranked.length > 0) {
      console.log(`      Downranked: ${result.downranked.map(e => `${e.processor}(${(e.lookup.approval_rate*100).toFixed(1)}%,n=${e.lookup.sample_size},${e.lookup.tier})`).join(', ')}`);
    }
    // Show best targets
    const scored = targets.map(t => {
      const l = le.queryRebill(issuer, 'DEBIT', 0, initProc, t, 1);
      return { proc: t, rate: l?.approval_rate || null, tier: l?.tier };
    }).filter(s => s.rate !== null).sort((a, b) => b.rate - a.rate);
    if (scored.length > 0) {
      console.log(`      Best: ${scored.slice(0, 3).map(s => `${s.proc} ${(s.rate*100).toFixed(1)}% (${s.tier})`).join(' > ')}`);
    }
  }
  console.log();
}

console.log('── PREPAID C1 REBILL ──\n');
for (const issuer of ISSUERS.slice(0, 3)) {
  for (const initProc of ['KURV', 'NETEVIA']) {
    const targets = PROCESSORS.slice(0, 6);
    const result = le.filterRebillCandidates(issuer, 'DEBIT', 1, initProc, targets, 1);
    console.log(`  ${pad(issuer, 35)} Init ${pad(initProc, 10)}: ${result.log.after_lookup}/${result.log.input_candidates} | excl: ${result.excluded.length} | down: ${result.downranked.length}${result.log.safeguard_triggered ? ' [SAFEGUARD]' : ''}`);
  }
}

// ═══════════════════════════════════════════
// TABLE 4: SALVAGE
// ═══════════════════════════════════════════
console.log('\n╔═══════════════════════════════════════════════════════════════════════════════╗');
console.log('║                      TABLE 4: REBILL SALVAGE (4D+5D)                          ║');
console.log('╚═══════════════════════════════════════════════════════════════════════════════╝\n');

for (const decline of DECLINES) {
  console.log(`── Decline: "${decline}" ──`);
  for (const issuer of ISSUERS.slice(0, 3)) {
    for (const failedProc of ['KURV', 'NETEVIA']) {
      const targets = PROCESSORS.filter(p => p !== failedProc).slice(0, 5);
      const result = le.filterSalvageCandidates(decline, issuer, 'DEBIT', 0, failedProc, targets);
      const details = targets.map(t => {
        const l = le.querySalvage(decline, issuer, 'DEBIT', 0, failedProc, t);
        if (!l) return `${t}: —`;
        return `${t}: ${(l.approval_rate*100).toFixed(1)}% (${l.tier})`;
      });
      console.log(`  ${pad(issuer, 30)} failed ${pad(failedProc, 8)}: ${result.log.after_lookup}/${result.log.input_candidates}${result.log.safeguard_triggered ? ' [SAFEGUARD]' : ''}`);
      if (details.some(d => !d.endsWith('—'))) {
        console.log(`    ${details.join(' | ')}`);
      }
    }
  }
  console.log();
}

// ═══════════════════════════════════════════
// COVERAGE SUMMARY
// ═══════════════════════════════════════════
console.log('╔═══════════════════════════════════════════════════════════════════════════════╗');
console.log('║                           COVERAGE SUMMARY                                    ║');
console.log('╚═══════════════════════════════════════════════════════════════════════════════╝\n');

let initHits = 0, initMiss = 0;
let cascHits = 0, cascMiss = 0;
let rebHits = 0, rebMiss = 0;
let salvHits = 0, salvMiss = 0;

for (const issuer of ISSUERS) {
  for (const proc of PROCESSORS) {
    if (le.queryInitial(issuer, 'CREDIT', 0, proc)) initHits++; else initMiss++;
    if (le.queryInitial(issuer, 'DEBIT', 0, proc)) initHits++; else initMiss++;
    if (le.queryInitial(issuer, 'DEBIT', 1, proc)) initHits++; else initMiss++;

    if (le.queryCascade(issuer, 'CREDIT', 0, 'KURV', proc)) cascHits++; else cascMiss++;
    if (le.queryRebill(issuer, 'DEBIT', 0, 'KURV', proc, 1)) rebHits++; else rebMiss++;
    if (le.querySalvage('DO NOT HONOR', issuer, 'DEBIT', 0, 'KURV', proc)) salvHits++; else salvMiss++;
  }
}

console.log(`  Initial:  ${initHits}/${initHits+initMiss} queries hit (${(100*initHits/(initHits+initMiss)).toFixed(0)}%) for top 10 issuers × 10 procs × 3 card types`);
console.log(`  Cascade:  ${cascHits}/${cascHits+cascMiss} queries hit (${(100*cascHits/(cascHits+cascMiss)).toFixed(0)}%) for top 10 issuers × 10 procs (failed=KURV)`);
console.log(`  Rebill:   ${rebHits}/${rebHits+rebMiss} queries hit (${(100*rebHits/(rebHits+rebMiss)).toFixed(0)}%) for top 10 issuers × 10 procs (init=KURV, C1)`);
console.log(`  Salvage:  ${salvHits}/${salvHits+salvMiss} queries hit (${(100*salvHits/(salvHits+salvMiss)).toFixed(0)}%) for top 10 issuers × 10 procs (DNH, failed=KURV)`);

process.exit(0);
