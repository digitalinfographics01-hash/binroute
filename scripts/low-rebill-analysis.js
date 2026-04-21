const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // Find all issuer x initial_processor combos where rebill approval < 10%
  console.log('=== Rebill combos below 10% approval (50+ samples) ===\n');
  const lowCombos = db.prepare(`
    SELECT issuer_bank, initial_processor, card_type,
      COUNT(*) as attempts,
      SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) as approved,
      ROUND(100.0*SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr_pct
    FROM transaction_attempts
    WHERE feature_version >= 3 AND model_target = 'rebill'
      AND derived_product_role LIKE '%main%'
    GROUP BY issuer_bank, initial_processor, card_type
    HAVING attempts >= 50 AND appr_pct < 10
    ORDER BY attempts DESC
  `).all();

  console.log(`Found ${lowCombos.length} combos below 10%\n`);
  console.table(lowCombos.slice(0, 20));

  // Total volume in these low combos
  const totalLow = lowCombos.reduce((s, c) => s + c.attempts, 0);
  const totalAll = db.prepare(`SELECT COUNT(*) as cnt FROM transaction_attempts WHERE feature_version >= 3 AND model_target = 'rebill' AND derived_product_role LIKE '%main%'`).get().cnt;
  console.log(`\nLow combos: ${totalLow.toLocaleString()} attempts (${(100*totalLow/totalAll).toFixed(1)}% of all rebills)`);

  // For each low combo, what are the decline reasons?
  console.log('\n\n=== Decline reasons in low-approval combos ===\n');
  for (const combo of lowCombos.slice(0, 10)) {
    console.log(`--- ${combo.issuer_bank} | ${combo.card_type} | init=${combo.initial_processor} (${combo.appr_pct}%, n=${combo.attempts}) ---`);

    const reasons = db.prepare(`
      SELECT ta.decline_reason, drc.decline_class, COUNT(*) as cnt
      FROM transaction_attempts ta
      LEFT JOIN decline_reason_classes drc ON ta.decline_reason = drc.decline_reason
      WHERE ta.feature_version >= 3 AND ta.model_target = 'rebill'
        AND ta.derived_product_role LIKE '%main%'
        AND ta.issuer_bank = ? AND ta.initial_processor = ? AND ta.card_type = ?
        AND ta.outcome = 'declined'
      GROUP BY ta.decline_reason, drc.decline_class
      ORDER BY cnt DESC LIMIT 5
    `).all(combo.issuer_bank, combo.initial_processor, combo.card_type);
    reasons.forEach(r => {
      console.log(`  ${r.decline_class || '?'}: ${r.decline_reason} (${r.cnt})`);
    });

    // Did ANY processor work for this issuer+card combo?
    const bestProc = db.prepare(`
      SELECT processor_name as target,
        COUNT(*) as cnt,
        ROUND(100.0*SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr_pct
      FROM transaction_attempts
      WHERE feature_version >= 3 AND model_target = 'rebill'
        AND derived_product_role LIKE '%main%'
        AND issuer_bank = ? AND initial_processor = ? AND card_type = ?
      GROUP BY processor_name HAVING cnt >= 20
      ORDER BY appr_pct DESC LIMIT 3
    `).all(combo.issuer_bank, combo.initial_processor, combo.card_type);
    if (bestProc.length > 0) {
      console.log(`  Best targets: ${bestProc.map(p => `${p.target} ${p.appr_pct}% (n=${p.cnt})`).join(', ')}`);
    }
    console.log('');
  }

  // What about time-of-day? Do low combos improve at certain hours?
  console.log('\n=== Time-of-day effect on lowest combo (Cap One/PAYFAC) ===');
  const timeEffect = db.prepare(`
    SELECT hour_of_day, COUNT(*) as cnt,
      ROUND(100.0*SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr_pct
    FROM transaction_attempts
    WHERE feature_version >= 3 AND model_target = 'rebill'
      AND derived_product_role LIKE '%main%'
      AND issuer_bank = 'CAPITAL ONE' AND initial_processor = 'PAYFAC'
    GROUP BY hour_of_day ORDER BY hour_of_day
  `).all();
  timeEffect.forEach(t => {
    const bar = '|'.repeat(Math.round(t.appr_pct));
    console.log(`  Hour ${String(t.hour_of_day).padStart(2)}: ${String(t.appr_pct).padStart(5)}% (n=${String(t.cnt).padStart(4)}) ${bar}`);
  });

  // Cycle depth effect — do these combos improve after cycle 1?
  console.log('\n=== Cycle depth effect on Cap One/PAYFAC rebills ===');
  const cycleEffect = db.prepare(`
    SELECT derived_cycle, COUNT(*) as cnt,
      ROUND(100.0*SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr_pct
    FROM transaction_attempts
    WHERE feature_version >= 3 AND model_target = 'rebill'
      AND derived_product_role LIKE '%main%'
      AND issuer_bank = 'CAPITAL ONE' AND initial_processor = 'PAYFAC'
    GROUP BY derived_cycle ORDER BY derived_cycle
  `).all();
  console.table(cycleEffect);
})();
