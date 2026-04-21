const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  const total = db.prepare("SELECT COUNT(*) as cnt FROM transaction_attempts WHERE feature_version >= 3").get().cnt;

  console.log('=== Top Issuers by Traffic Share ===\n');
  const rows = db.prepare(`
    SELECT issuer_bank, COUNT(*) as cnt,
      ROUND(100.0*COUNT(*) / ${total}, 1) as pct_of_total,
      SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) as approved,
      ROUND(100.0*SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr_pct
    FROM transaction_attempts
    WHERE feature_version >= 3
    GROUP BY issuer_bank ORDER BY cnt DESC LIMIT 20
  `).all();

  console.log(`${'Issuer'.padEnd(45)} ${'Volume'.padStart(8)} ${'Share'.padStart(6)} ${'Appr%'.padStart(6)}`);
  console.log('-'.repeat(70));
  for (const r of rows) {
    console.log(`${(r.issuer_bank || 'NULL').padEnd(45)} ${String(r.cnt).padStart(8)} ${(r.pct_of_total + '%').padStart(6)} ${(r.appr_pct + '%').padStart(6)}`);
  }
  console.log(`${'TOTAL'.padEnd(45)} ${String(total).padStart(8)}`);
})();
