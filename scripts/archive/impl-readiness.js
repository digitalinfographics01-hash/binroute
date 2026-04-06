const { initDb, querySql, closeDb } = require('../src/db/connection');

(async () => {
  await initDb();

  const banks = querySql(`
    SELECT b.issuer_bank, b.is_prepaid,
      SUM(CASE WHEN o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as init_att,
      SUM(CASE WHEN o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL) AND o.is_cascaded = 0 AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as init_app,
      SUM(CASE WHEN o.derived_product_role = 'main_rebill' AND o.derived_cycle = 1 AND o.derived_attempt = 1 AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as c1_att,
      SUM(CASE WHEN o.derived_product_role = 'main_rebill' AND o.derived_cycle = 1 AND o.derived_attempt = 1 AND o.is_cascaded = 0 AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as c1_app,
      COUNT(DISTINCT o.cc_first_6) as bins
    FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.is_test = 0 AND o.is_internal_test = 0 AND o.cc_first_6 NOT IN ('144444','777777')
    GROUP BY b.issuer_bank, b.is_prepaid
    HAVING c1_att >= 10
    ORDER BY c1_att DESC
  `);

  function getSignals(b) {
    const signals = [];
    const c1Rate = b.c1_att > 0 ? b.c1_app / b.c1_att * 100 : 0;

    // Check processor variance
    const procVar = querySql(`
      SELECT g.processor_name,
        COUNT(*) as att,
        SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
      FROM orders o
      JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
      JOIN bin_lookup bb ON o.cc_first_6 = bb.bin
      WHERE o.derived_product_role = 'main_initial'
        AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
        AND o.is_test = 0 AND o.is_internal_test = 0
        AND bb.issuer_bank = ? AND bb.is_prepaid = ?
        AND g.processor_name IS NOT NULL
      GROUP BY g.processor_name HAVING att >= 10
      ORDER BY app*1.0/att DESC
    `, [b.issuer_bank, b.is_prepaid]);

    const best = procVar[0];
    const worst = procVar[procVar.length - 1];
    const spread = best && worst && procVar.length > 1
      ? Math.round((best.app / best.att - worst.app / worst.att) * 100)
      : 0;

    if (b.c1_app === 0 && b.c1_att >= 10) signals.push('NOT-REBILL-WORTHY');
    if (c1Rate > 0 && c1Rate < 5) signals.push('REBILL-BLOCKER');
    if (spread >= 20) signals.push('HIGH-SPREAD(' + spread + 'pp)');
    else if (spread >= 10) signals.push('OPTIMIZABLE(' + spread + 'pp)');
    if (procVar.some(p => p.att >= 10 && p.app === 0)) signals.push('HAS-BLOCK-RULE');
    if (c1Rate >= 30) signals.push('HIGH-PERFORMER');
    if (c1Rate >= 10 && c1Rate < 30 && spread >= 15) signals.push('LIFT-OPPORTUNITY');
    if (best) signals.push('Best:' + best.processor_name + ' ' + Math.round(best.app / best.att * 100) + '%');

    return { signals, spread, bestProc: best ? best.processor_name : '-' };
  }

  console.log('══════════════════════════════════════════════════════════════════════════════════════');
  console.log('  IMPLEMENTATION READINESS ASSESSMENT');
  console.log('══════════════════════════════════════════════════════════════════════════════════════');

  console.log('\n── TIER 1: CONFIDENT (30+ C1 rebill attempts) — ready to implement ──\n');
  console.log('Bank'.padEnd(40) + 'PP  ' + 'Init Rate'.padEnd(14) + 'C1 Rate'.padEnd(14) + 'Tier'.padEnd(8) + 'Signals');
  console.log('-'.repeat(110));

  const tier1 = banks.filter(b => b.c1_att >= 30);
  for (const b of tier1) {
    const initRate = b.init_att > 0 ? (b.init_app / b.init_att * 100).toFixed(1) : '-';
    const c1Rate = b.c1_att > 0 ? (b.c1_app / b.c1_att * 100).toFixed(1) : '-';
    const tier = parseFloat(c1Rate) < 10 ? 'AGGR' : parseFloat(c1Rate) < 20 ? 'OPT' : 'STD';
    const { signals } = getSignals(b);

    console.log(
      b.issuer_bank.substring(0, 38).padEnd(40) +
      (b.is_prepaid ? 'PP  ' : '    ') +
      (initRate + '%').padEnd(14) +
      (c1Rate + '% (' + b.c1_app + '/' + b.c1_att + ')').padEnd(14) +
      tier.padEnd(8) +
      signals.join(' | ')
    );
  }

  console.log('\n── TIER 2: EARLY SIGNALS (10-29 C1 attempts) — watch these ──\n');
  console.log('Bank'.padEnd(40) + 'PP  ' + 'Init Rate'.padEnd(14) + 'C1 Rate'.padEnd(14) + 'Tier'.padEnd(8) + 'Signals');
  console.log('-'.repeat(110));

  const tier2 = banks.filter(b => b.c1_att >= 10 && b.c1_att < 30);
  for (const b of tier2) {
    const initRate = b.init_att > 0 ? (b.init_app / b.init_att * 100).toFixed(1) : '-';
    const c1Rate = b.c1_att > 0 ? (b.c1_app / b.c1_att * 100).toFixed(1) : '-';
    const tier = parseFloat(c1Rate) < 10 ? 'AGGR' : parseFloat(c1Rate) < 20 ? 'OPT' : 'STD';
    const { signals } = getSignals(b);

    console.log(
      b.issuer_bank.substring(0, 38).padEnd(40) +
      (b.is_prepaid ? 'PP  ' : '    ') +
      (initRate + '%').padEnd(14) +
      (c1Rate + '% (' + b.c1_app + '/' + b.c1_att + ')').padEnd(14) +
      tier.padEnd(8) +
      signals.join(' | ')
    );
  }

  console.log('\n══ SUMMARY ══');
  console.log('Tier 1 (confident):', tier1.length, 'banks');
  console.log('Tier 2 (early signal):', tier2.length, 'banks');
  console.log('  AGGRESSIVE:', banks.filter(b => b.c1_att > 0 && b.c1_app / b.c1_att < 0.10).length);
  console.log('  OPTIMIZE:', banks.filter(b => b.c1_att > 0 && b.c1_app / b.c1_att >= 0.10 && b.c1_app / b.c1_att < 0.20).length);
  console.log('  STANDARD:', banks.filter(b => b.c1_att > 0 && b.c1_app / b.c1_att >= 0.20).length);
  console.log('  NOT REBILL WORTHY:', banks.filter(b => b.c1_app === 0).length);

  closeDb();
})();
