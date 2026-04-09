/**
 * Test that upsells now get initial_processor populated.
 * Runs locally — picks a few upsell orders and checks the exploder output.
 */
const { initDb, querySql, queryOneSql } = require('../src/db/connection');

(async () => {
  await initDb();

  // 1. Find upsell orders that have a customer_id with a known main_initial
  const upsells = querySql(
    "SELECT o.id, o.order_id, o.customer_id, o.gateway_id, o.derived_product_role, o.order_status " +
    "FROM orders o " +
    "WHERE o.derived_product_role = 'upsell_initial' " +
    "AND o.customer_id IS NOT NULL AND o.customer_id != '' AND o.customer_id != '0' " +
    "LIMIT 10"
  );

  if (upsells.length === 0) {
    console.log('No upsell orders found in local DB.');
    process.exit(0);
  }

  console.log('=== UPSELL ORDERS ===');
  for (const ups of upsells) {
    // Find the main_initial for this customer
    const mainInit = queryOneSql(
      "SELECT o.id, o.order_id, o.gateway_id, g.processor_name " +
      "FROM orders o " +
      "LEFT JOIN gateways g ON g.client_id = o.client_id AND g.gateway_id = o.gateway_id " +
      "WHERE o.customer_id = ? AND o.derived_product_role = 'main_initial' " +
      "AND o.order_status IN (2, 6, 8) " +
      "ORDER BY o.acquisition_date ASC LIMIT 1",
      [ups.customer_id]
    );

    console.log(`\n  Upsell order ${ups.order_id} (customer ${ups.customer_id}, gw ${ups.gateway_id}):`);
    if (mainInit) {
      console.log(`    Main initial: order ${mainInit.order_id}, gw ${mainInit.gateway_id}, proc: ${mainInit.processor_name}`);
    } else {
      console.log(`    Main initial: NOT FOUND (no approved main_initial for this customer)`);
    }

    // Check current transaction_attempts for this upsell
    const existing = queryOneSql(
      "SELECT initial_processor FROM transaction_attempts WHERE sticky_order_id = ?",
      [ups.order_id]
    );
    if (existing) {
      console.log(`    Current initial_processor in tx_attempts: ${existing.initial_processor || 'NULL'}`);
    } else {
      console.log(`    Not yet in transaction_attempts`);
    }
  }

  // 2. Now test the exploder on one upsell
  console.log('\n=== TESTING EXPLODER ON UPSELL ===');

  // Find a upsell with a known main_initial that has an approved status
  const testUpsell = querySql(
    "SELECT u.id, u.order_id, u.customer_id, u.client_id " +
    "FROM orders u " +
    "WHERE u.derived_product_role = 'upsell_initial' " +
    "AND u.customer_id IN (" +
    "  SELECT m.customer_id FROM orders m " +
    "  WHERE m.derived_product_role = 'main_initial' AND m.order_status IN (2, 6, 8)" +
    ") " +
    "LIMIT 3"
  );

  if (testUpsell.length === 0) {
    console.log('No testable upsell orders (need customer with approved main_initial)');
    process.exit(0);
  }

  const { explodeOrdersToAttempts } = require('../src/pipeline/attempt-exploder');

  for (const ups of testUpsell) {
    console.log(`\n  Exploding upsell order id=${ups.id} (order_id=${ups.order_id}, customer=${ups.customer_id}):`);

    // Delete existing attempt rows for this order so we can re-explode
    const { runSql } = require('../src/db/connection');
    runSql('DELETE FROM transaction_attempts WHERE order_id = ? AND client_id = ?', [ups.id, ups.client_id]);

    // Re-explode
    const count = explodeOrdersToAttempts(ups.client_id, [ups.id]);
    console.log(`    Exploded into ${count} attempt(s)`);

    // Check the result
    const attempts = querySql(
      'SELECT attempt_seq, processor_name, initial_processor, outcome FROM transaction_attempts WHERE order_id = ? AND client_id = ?',
      [ups.id, ups.client_id]
    );
    for (const att of attempts) {
      console.log(`    Attempt ${att.attempt_seq}: proc=${att.processor_name}, initial_proc=${att.initial_processor || 'NULL'}, outcome=${att.outcome}`);
    }

    if (attempts.length > 0 && attempts[0].initial_processor) {
      console.log('    ✓ initial_processor populated!');
    } else if (attempts.length > 0) {
      console.log('    ✗ initial_processor still NULL');
    }
  }

  process.exit(0);
})();
