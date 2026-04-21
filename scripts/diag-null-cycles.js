/**
 * Investigate why some orders have NULL derived_cycle / derived_attempt.
 * For each client:
 *  1. Total nulls
 *  2. Breakdown by tx_type, product_type (joined), order_status, billing_cycle
 *  3. Sample of null orders to spot patterns
 */
const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  for (const cid of [1, 2, 3, 4, 5]) {
    const clientName = querySql('SELECT name FROM clients WHERE id=?', [cid])[0]?.name;
    console.log('='.repeat(72));
    console.log('CLIENT ' + cid + ': ' + clientName);
    console.log('='.repeat(72));

    // Total + null counts
    const counts = querySql(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN derived_cycle IS NULL THEN 1 ELSE 0 END) as null_cycle,
        SUM(CASE WHEN derived_attempt IS NULL THEN 1 ELSE 0 END) as null_attempt,
        SUM(CASE WHEN derived_cycle IS NULL AND derived_attempt IS NULL THEN 1 ELSE 0 END) as null_both
      FROM orders WHERE client_id = ?
    `, [cid])[0];
    const nullPct = (counts.null_cycle / counts.total * 100).toFixed(1);
    console.log('Total: ' + counts.total + ' | NULL derived_cycle: ' + counts.null_cycle + ' (' + nullPct + '%)');
    console.log();

    // Breakdown by tx_type
    console.log('NULL orders by tx_type:');
    const byTxType = querySql(`
      SELECT COALESCE(tx_type, '(null)') as tx_type, COUNT(*) as n
      FROM orders WHERE client_id = ? AND derived_cycle IS NULL
      GROUP BY tx_type ORDER BY n DESC
    `, [cid]);
    byTxType.forEach(r => console.log('  ' + (r.tx_type || '(null)').padEnd(25) + String(r.n).padStart(7)));
    console.log();

    // Breakdown by current derived_product_role
    console.log('NULL orders by derived_product_role:');
    const byRole = querySql(`
      SELECT COALESCE(derived_product_role, '(null)') as role, COUNT(*) as n
      FROM orders WHERE client_id = ? AND derived_cycle IS NULL
      GROUP BY role ORDER BY n DESC
    `, [cid]);
    byRole.forEach(r => console.log('  ' + r.role.padEnd(25) + String(r.n).padStart(7)));
    console.log();

    // Breakdown by product_type (via join)
    console.log('NULL orders by joined product_type:');
    const byProductType = querySql(`
      SELECT COALESCE(pga.product_type, '(no join match)') as ptype, COUNT(*) as n
      FROM orders o
      LEFT JOIN product_group_assignments pga
        ON pga.client_id = o.client_id
        AND CAST(pga.product_id AS TEXT) = JSON_EXTRACT(o.product_ids, '$[0]')
      WHERE o.client_id = ? AND o.derived_cycle IS NULL
      GROUP BY ptype ORDER BY n DESC
    `, [cid]);
    byProductType.forEach(r => console.log('  ' + r.ptype.padEnd(25) + String(r.n).padStart(7)));
    console.log();

    // Internal test breakdown
    const testCounts = querySql(`
      SELECT
        SUM(CASE WHEN COALESCE(is_internal_test, 0) = 1 THEN 1 ELSE 0 END) as internal_test,
        SUM(CASE WHEN COALESCE(is_internal_test, 0) = 0 THEN 1 ELSE 0 END) as not_internal
      FROM orders WHERE client_id = ? AND derived_cycle IS NULL
    `, [cid])[0];
    console.log('NULL orders that are internal_test=1: ' + testCounts.internal_test);
    console.log('NULL orders NOT internal test:        ' + testCounts.not_internal);
    console.log();

    // Sample a few non-test null orders for inspection
    const samples = querySql(`
      SELECT order_id, customer_id, contact_id, JSON_EXTRACT(product_ids, '$[0]') as product_id,
             billing_cycle, retry_attempt, order_status, tx_type, derived_product_role, campaign_id,
             acquisition_date
      FROM orders
      WHERE client_id = ? AND derived_cycle IS NULL AND COALESCE(is_internal_test, 0) = 0
      LIMIT 5
    `, [cid]);
    if (samples.length > 0) {
      console.log('Sample of NULL non-test orders:');
      samples.forEach(s => console.log('  ' + JSON.stringify(s)));
    }
    console.log();
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
