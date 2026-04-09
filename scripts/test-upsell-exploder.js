const { initDb, runSql, querySql } = require('../src/db/connection');
const { explodeOrdersToAttempts } = require('../src/pipeline/attempt-exploder');

(async () => {
  await initDb();

  // Get excluded gateways
  const excluded = querySql(
    "SELECT gateway_id FROM gateways WHERE client_id = 1 AND exclude_from_analysis = 1"
  ).map(r => r.gateway_id);

  // Find testable upsells
  const ups = querySql(
    "SELECT id, order_id, customer_id, gateway_id FROM orders " +
    "WHERE derived_product_role = 'upsell_initial' " +
    "AND is_test = 0 AND is_internal_test = 0 " +
    "AND product_type_classified IS NOT NULL AND product_type_classified != 'straight_sale' " +
    "AND order_status IN (2,6,7,8) " +
    "AND gateway_id NOT IN (" + excluded.join(',') + ") " +
    "AND client_id = 1 LIMIT 5"
  );

  console.log('Found ' + ups.length + ' testable upsells');

  for (const u of ups) {
    console.log('\n  id=' + u.id + ' order=' + u.order_id + ' cust=' + u.customer_id + ' gw=' + u.gateway_id);

    // Clear and re-explode
    runSql('DELETE FROM transaction_attempts WHERE order_id = ? AND client_id = 1', [u.id]);
    const count = explodeOrdersToAttempts(1, [u.id]);
    console.log('  Exploded: ' + count + ' attempts');

    if (count > 0) {
      const atts = querySql(
        'SELECT attempt_seq, processor_name, initial_processor, derived_product_role, outcome ' +
        'FROM transaction_attempts WHERE order_id = ? AND client_id = 1', [u.id]
      );
      for (const a of atts) {
        const status = a.initial_processor ? '✓' : '✗';
        console.log('  ' + status + ' seq=' + a.attempt_seq +
          ' proc=' + a.processor_name +
          ' initial_proc=' + (a.initial_processor || 'NULL') +
          ' outcome=' + a.outcome);
      }
    }
  }

  process.exit(0);
})();
