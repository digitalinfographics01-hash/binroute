const { initDb, querySql } = require('../src/db/connection');
(async () => {
  await initDb();
  const q = (sql) => querySql(sql);

  const r = q(
    "SELECT COUNT(*) as total, " +
    "SUM(CASE WHEN initial_processor IS NOT NULL THEN 1 ELSE 0 END) as has_init " +
    "FROM transaction_attempts " +
    "WHERE feature_version>=3 AND source='order_direct' AND derived_cycle=0 AND derived_product_role='upsell_initial'"
  );
  console.log('Upsell attempts: total=' + r[0].total + ', has initial_processor=' + r[0].has_init);

  // What does initial_processor look like for upsells?
  const sample = q(
    "SELECT initial_processor, COUNT(*) as n FROM transaction_attempts " +
    "WHERE feature_version>=3 AND source='order_direct' AND derived_cycle=0 AND derived_product_role='upsell_initial' " +
    "GROUP BY initial_processor ORDER BY n DESC LIMIT 10"
  );
  console.log('\nInitial_processor values for upsells:');
  sample.forEach(x => console.log('  ' + (x.initial_processor || 'NULL') + ': ' + x.n));

  // Check: does the upsell order have a parent/ancestor that we could trace the initial proc from?
  const parentCheck = q(
    "SELECT COUNT(*) as total, " +
    "SUM(CASE WHEN last_approved_processor IS NOT NULL THEN 1 ELSE 0 END) as has_last_approved " +
    "FROM transaction_attempts " +
    "WHERE feature_version>=3 AND source='order_direct' AND derived_cycle=0 AND derived_product_role='upsell_initial'"
  );
  console.log('\nHas last_approved_processor: ' + parentCheck[0].has_last_approved + '/' + parentCheck[0].total);

  process.exit(0);
})();
