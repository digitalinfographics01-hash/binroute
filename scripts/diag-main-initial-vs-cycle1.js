/**
 * Specific comparison: MAIN sequence only, cycle 0 attempt 1 vs cycle 1 attempt 1
 *
 * Filters to main groups only (excludes upsell, excluded, recovery groups).
 */
const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  console.log('='.repeat(96));
  console.log('MAIN INITIAL (cycle 0 attempt 1) vs MAIN REBILL CYCLE 1 (cycle 1 attempt 1)');
  console.log('  Filter: only orders whose product is in a "main" sequence group');
  console.log('='.repeat(96));
  console.log();

  for (const cid of [1, 2, 3, 4, 5]) {
    const clientName = querySql('SELECT name FROM clients WHERE id=?', [cid])[0]?.name;
    console.log('CLIENT ' + cid + ': ' + clientName);
    console.log('-'.repeat(96));

    // Main initial: cycle 0 attempt 1, in main-sequence group
    const mainInitial = querySql(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
        SUM(CASE WHEN o.order_status = 7 THEN 1 ELSE 0 END) as declined,
        COUNT(DISTINCT o.customer_id) as unique_customers
      FROM orders o
      INNER JOIN product_group_assignments pga
        ON pga.client_id = o.client_id
        AND CAST(pga.product_id AS TEXT) = JSON_EXTRACT(o.product_ids, '$[0]')
      INNER JOIN product_groups pg ON pg.id = pga.product_group_id
      WHERE o.client_id = ?
        AND COALESCE(o.is_internal_test, 0) = 0
        AND o.derived_cycle = 0
        AND o.derived_attempt = 1
        AND pg.product_sequence = 'main'
        AND pga.product_type IN ('initial', 'initial_rebill')
    `, [cid])[0];

    // Main rebill cycle 1 first attempt: cycle 1 attempt 1, in main-sequence group, rebill product
    const mainRebillC1 = querySql(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
        SUM(CASE WHEN o.order_status = 7 THEN 1 ELSE 0 END) as declined,
        COUNT(DISTINCT o.customer_id) as unique_customers
      FROM orders o
      INNER JOIN product_group_assignments pga
        ON pga.client_id = o.client_id
        AND CAST(pga.product_id AS TEXT) = JSON_EXTRACT(o.product_ids, '$[0]')
      INNER JOIN product_groups pg ON pg.id = pga.product_group_id
      WHERE o.client_id = ?
        AND COALESCE(o.is_internal_test, 0) = 0
        AND o.derived_cycle = 1
        AND o.derived_attempt = 1
        AND pg.product_sequence = 'main'
        AND pga.product_type IN ('rebill', 'initial_rebill')
    `, [cid])[0];

    console.log('  MAIN INITIAL (cycle 0 attempt 1):');
    console.log('    Orders:           ' + mainInitial.total);
    console.log('    Approved:         ' + mainInitial.approved + ' (' + (mainInitial.approved/mainInitial.total*100).toFixed(1) + '%)');
    console.log('    Declined:         ' + mainInitial.declined);
    console.log('    Unique customers: ' + mainInitial.unique_customers);
    console.log();
    console.log('  MAIN REBILL CYCLE 1 (cycle 1 attempt 1):');
    console.log('    Orders:           ' + mainRebillC1.total);
    console.log('    Approved:         ' + mainRebillC1.approved + ' (' + (mainRebillC1.total > 0 ? (mainRebillC1.approved/mainRebillC1.total*100).toFixed(1) : 'n/a') + '%)');
    console.log('    Declined:         ' + mainRebillC1.declined);
    console.log('    Unique customers: ' + mainRebillC1.unique_customers);
    console.log();
    console.log('  CONVERSION:');
    console.log('    Approved initials → Cycle 1 attempts (orders): ' + mainInitial.approved + ' → ' + mainRebillC1.total + ' (ratio: ' + (mainRebillC1.total / mainInitial.approved).toFixed(3) + ')');
    console.log('    Approved initials → Cycle 1 attempts (customers): ' + mainInitial.unique_customers + ' → ' + mainRebillC1.unique_customers + ' (ratio: ' + (mainRebillC1.unique_customers / mainInitial.unique_customers).toFixed(3) + ')');
    console.log();
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
