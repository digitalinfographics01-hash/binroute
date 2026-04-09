const { initDb, querySql } = require('../src/db/connection');
(async () => {
  await initDb();
  const q = (sql) => querySql(sql);

  // 1. derived_cycle distribution for order_direct
  const cycles = q(
    "SELECT derived_cycle, COUNT(*) as n, " +
    "ROUND(100.0*SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END)/COUNT(*),2) as rate " +
    "FROM transaction_attempts WHERE feature_version>=3 AND model_target!='excluded' AND source='order_direct' " +
    "GROUP BY derived_cycle ORDER BY n DESC"
  );
  console.log('=== derived_cycle distribution (order_direct) ===');
  cycles.forEach(x => console.log('  cycle=' + x.derived_cycle + ': ' + x.n + ' attempts, ' + x.rate + '% approval'));

  // 2. NULL cycle breakdown
  const nullCycles = q(
    "SELECT derived_product_role, COUNT(*) as n, " +
    "ROUND(100.0*SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END)/COUNT(*),2) as rate " +
    "FROM transaction_attempts WHERE feature_version>=3 AND model_target!='excluded' " +
    "AND source='order_direct' AND derived_cycle IS NULL GROUP BY derived_product_role"
  );
  console.log('\n=== NULL cycle breakdown by product_role ===');
  nullCycles.forEach(x => console.log('  ' + x.derived_product_role + ': ' + x.n + ' attempts, ' + x.rate + '%'));

  // 3. Cycle 0 breakdown
  const cycle0 = q(
    "SELECT derived_product_role, COUNT(*) as n, " +
    "ROUND(100.0*SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END)/COUNT(*),2) as rate " +
    "FROM transaction_attempts WHERE feature_version>=3 AND model_target!='excluded' " +
    "AND source='order_direct' AND derived_cycle=0 GROUP BY derived_product_role"
  );
  console.log('\n=== Cycle 0 breakdown by product_role ===');
  cycle0.forEach(x => console.log('  ' + x.derived_product_role + ': ' + x.n + ' attempts, ' + x.rate + '%'));

  // 4. Fifth Third specifically — where are the 90%+ rates coming from?
  const fifth = q(
    "SELECT derived_cycle, derived_product_role, COUNT(*) as n, " +
    "ROUND(100.0*SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END)/COUNT(*),2) as rate " +
    "FROM transaction_attempts WHERE feature_version>=3 AND model_target!='excluded' " +
    "AND source='order_direct' AND issuer_bank='FIFTH THIRD BANK, THE' " +
    "AND (derived_cycle=0 OR derived_cycle IS NULL) " +
    "GROUP BY derived_cycle, derived_product_role ORDER BY n DESC"
  );
  console.log('\n=== Fifth Third Bank initials breakdown ===');
  fifth.forEach(x => console.log('  cycle=' + x.derived_cycle + ' / ' + x.derived_product_role + ': ' + x.n + ' attempts, ' + x.rate + '%'));

  // 5. Compare: cycle=0 only vs cycle=0 OR NULL
  const compare = q(
    "SELECT " +
    "SUM(CASE WHEN derived_cycle=0 THEN 1 ELSE 0 END) as cycle0_n, " +
    "ROUND(100.0*SUM(CASE WHEN derived_cycle=0 AND outcome='approved' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN derived_cycle=0 THEN 1 ELSE 0 END),0),2) as cycle0_rate, " +
    "SUM(CASE WHEN derived_cycle IS NULL THEN 1 ELSE 0 END) as null_n, " +
    "ROUND(100.0*SUM(CASE WHEN derived_cycle IS NULL AND outcome='approved' THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN derived_cycle IS NULL THEN 1 ELSE 0 END),0),2) as null_rate " +
    "FROM transaction_attempts WHERE feature_version>=3 AND model_target!='excluded' AND source='order_direct'"
  );
  console.log('\n=== Cycle 0 vs NULL comparison ===');
  console.log('  cycle=0: ' + compare[0].cycle0_n + ' attempts, ' + compare[0].cycle0_rate + '% approval');
  console.log('  cycle=NULL: ' + compare[0].null_n + ' attempts, ' + compare[0].null_rate + '% approval');

  process.exit(0);
})();
