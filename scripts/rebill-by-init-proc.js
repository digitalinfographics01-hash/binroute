const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // All initial_processor values in main rebill attempts
  console.log('=== Rebill attempts by initial_processor ===\n');
  const procs = db.prepare(`
    SELECT initial_processor, COUNT(*) as cnt,
      SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) as approved,
      ROUND(100.0*SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr_pct
    FROM transaction_attempts
    WHERE feature_version >= 3 AND model_target = 'rebill'
      AND derived_product_role LIKE '%main%'
    GROUP BY initial_processor ORDER BY cnt DESC
  `).all();
  console.table(procs);

  // For non-PAYFAC initial processors, show the rebill lookup entries
  const nonPayfac = procs.filter(p => p.initial_processor && p.initial_processor !== 'PAYFAC');

  for (const proc of nonPayfac.slice(0, 8)) {
    console.log(`\n--- Initial Processor: ${proc.initial_processor} (${proc.cnt} rebills, ${proc.appr_pct}% approval) ---`);

    // Top issuer x target combos
    const combos = db.prepare(`
      SELECT issuer_bank, card_type, processor_name as target,
        COUNT(*) as cnt,
        SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) as approved,
        ROUND(100.0*SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr_pct
      FROM transaction_attempts
      WHERE feature_version >= 3 AND model_target = 'rebill'
        AND derived_product_role LIKE '%main%'
        AND initial_processor = ?
      GROUP BY issuer_bank, card_type, processor_name
      HAVING cnt >= 20
      ORDER BY issuer_bank, appr_pct DESC
    `).all(proc.initial_processor);

    // Group by issuer
    const byIssuer = {};
    for (const c of combos) {
      const key = c.issuer_bank || 'UNKNOWN';
      if (!byIssuer[key]) byIssuer[key] = [];
      byIssuer[key].push(c);
    }

    for (const [issuer, entries] of Object.entries(byIssuer).slice(0, 5)) {
      console.log(`  ${issuer}:`);
      for (const e of entries) {
        const flag = e.appr_pct < 5 ? ' ** HARD EXCLUDE' : e.appr_pct < 10 ? ' * SOFT' : '';
        console.log(`    ${e.card_type || 'ALL'} -> ${e.target}: ${e.appr_pct}% (n=${e.cnt})${flag}`);
      }
    }
  }
})();
