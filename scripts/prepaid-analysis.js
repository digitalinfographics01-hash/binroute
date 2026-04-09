const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  const q = (sql) => querySql(sql);

  // 1. Prepaid distribution
  const prepaid = q(
    "SELECT is_prepaid, COUNT(*) as attempts, " +
    "SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as approval_pct " +
    "FROM transaction_attempts WHERE feature_version >= 3 AND model_target != 'excluded' GROUP BY is_prepaid"
  );
  console.log('=== PREPAID DISTRIBUTION ===');
  prepaid.forEach(r => console.log('  prepaid=' + r.is_prepaid + ': ' + r.attempts + ' attempts, ' + r.approval_pct + '% (' + r.approved + ' approved)'));

  // 2. Card_type x prepaid
  const combo = q(
    "SELECT card_type, is_prepaid, COUNT(*) as attempts, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as approval_pct " +
    "FROM transaction_attempts WHERE feature_version >= 3 AND model_target != 'excluded' " +
    "GROUP BY card_type, is_prepaid ORDER BY card_type, is_prepaid"
  );
  console.log('\n=== CARD_TYPE x PREPAID ===');
  combo.forEach(r => console.log('  ' + (r.card_type || 'NULL') + ' / prepaid=' + r.is_prepaid + ': ' + r.attempts + ' attempts, ' + r.approval_pct + '%'));

  // 3. Top issuers: prepaid vs non-prepaid gap
  const issuerGap = q(
    "SELECT issuer_bank, " +
    "SUM(CASE WHEN is_prepaid=1 THEN 1 ELSE 0 END) as pp_n, " +
    "ROUND(100.0 * SUM(CASE WHEN is_prepaid=1 AND outcome='approved' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN is_prepaid=1 THEN 1 ELSE 0 END),0), 2) as pp_pct, " +
    "SUM(CASE WHEN is_prepaid=0 THEN 1 ELSE 0 END) as np_n, " +
    "ROUND(100.0 * SUM(CASE WHEN is_prepaid=0 AND outcome='approved' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN is_prepaid=0 THEN 1 ELSE 0 END),0), 2) as np_pct " +
    "FROM transaction_attempts WHERE feature_version >= 3 AND model_target != 'excluded' AND issuer_bank IS NOT NULL " +
    "GROUP BY issuer_bank HAVING pp_n >= 20 AND np_n >= 20 ORDER BY (np_pct - pp_pct) DESC LIMIT 20"
  );
  console.log('\n=== ISSUERS: BIGGEST PREPAID vs NON-PREPAID GAP (both 20+) ===');
  issuerGap.forEach(r => console.log('  ' + r.issuer_bank + ': prepaid ' + r.pp_pct + '% (' + r.pp_n + ') vs non-prepaid ' + r.np_pct + '% (' + r.np_n + ') → gap ' + (r.np_pct - r.pp_pct).toFixed(1) + '%'));

  // 4. By source (tx class) x prepaid
  const byClass = q(
    "SELECT source, is_prepaid, COUNT(*) as attempts, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as approval_pct " +
    "FROM transaction_attempts WHERE feature_version >= 3 AND model_target != 'excluded' " +
    "GROUP BY source, is_prepaid ORDER BY source, is_prepaid"
  );
  console.log('\n=== TX CLASS x PREPAID ===');
  byClass.forEach(r => console.log('  ' + r.source + ' / prepaid=' + r.is_prepaid + ': ' + r.attempts + ' attempts, ' + r.approval_pct + '%'));

  // 5. Processor x prepaid (top processors)
  const procPrepaid = q(
    "SELECT processor_name, is_prepaid, COUNT(*) as attempts, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as approval_pct " +
    "FROM transaction_attempts WHERE feature_version >= 3 AND model_target != 'excluded' AND processor_name IS NOT NULL " +
    "GROUP BY processor_name, is_prepaid HAVING attempts >= 50 ORDER BY processor_name, is_prepaid"
  );
  console.log('\n=== PROCESSOR x PREPAID (50+ attempts) ===');
  procPrepaid.forEach(r => console.log('  ' + r.processor_name + ' / prepaid=' + r.is_prepaid + ': ' + r.attempts + ' attempts, ' + r.approval_pct + '%'));

  process.exit(0);
})();
