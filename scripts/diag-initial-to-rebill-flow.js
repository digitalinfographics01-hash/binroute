/**
 * Sanity check: approved initials >= cycle 1 first attempts
 *
 * For each client:
 *  - Count approved main_initial orders
 *  - Count cycle 1 first-attempt rebill orders (derived_cycle=1, derived_attempt=1)
 *  - Verify the ratio: cycle_1_attempts / approved_initials should be < 1
 *
 * Also breakdown by cycle to show the funnel.
 */
const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  for (const cid of [1, 2, 3, 4, 5]) {
    const clientName = querySql('SELECT name FROM clients WHERE id=?', [cid])[0]?.name;
    console.log('='.repeat(72));
    console.log('CLIENT ' + cid + ': ' + clientName);
    console.log('='.repeat(72));

    // 1. Approved main_initial orders
    // (using existing derived_product_role since we haven't applied the new classifier yet)
    const approvedInitials = querySql(`
      SELECT COUNT(DISTINCT customer_id) as unique_customers, COUNT(*) as orders
      FROM orders
      WHERE client_id = ?
        AND COALESCE(is_internal_test, 0) = 0
        AND derived_cycle = 0
        AND derived_attempt = 1
        AND order_status IN (2,6,8)
    `, [cid])[0];
    console.log('Approved cycle 0 attempt 1 (became subscribers):');
    console.log('  Orders:           ' + approvedInitials.orders);
    console.log('  Unique customers: ' + approvedInitials.unique_customers);
    console.log();

    // 2. Cycle 1 first-attempt rebills (any status)
    const cycle1FirstAttempts = querySql(`
      SELECT
        COUNT(*) as orders,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved
      FROM orders
      WHERE client_id = ?
        AND COALESCE(is_internal_test, 0) = 0
        AND derived_cycle = 1
        AND derived_attempt = 1
    `, [cid])[0];
    console.log('Cycle 1 first attempt (any status):');
    console.log('  Orders:           ' + cycle1FirstAttempts.orders);
    console.log('  Unique customers: ' + cycle1FirstAttempts.unique_customers);
    console.log('  Approved:         ' + cycle1FirstAttempts.approved);
    console.log();

    const ratio = cycle1FirstAttempts.orders / approvedInitials.orders;
    console.log('Ratio (cycle1_attempts / approved_initials): ' + ratio.toFixed(3));
    if (ratio > 1) {
      console.log('  ⚠ More cycle 1 attempts than approved initials — possibly misclassification');
    } else {
      console.log('  ✓ More approved initials than cycle 1 attempts (expected — recent subscribers haven\'t cycled yet)');
    }
    console.log();

    // 3. Funnel: cycle by cycle (first attempt only)
    console.log('Funnel — first-attempt orders per cycle:');
    const funnel = querySql(`
      SELECT
        derived_cycle as cycle,
        COUNT(*) as first_attempts,
        SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved
      FROM orders
      WHERE client_id = ?
        AND COALESCE(is_internal_test, 0) = 0
        AND derived_attempt = 1
        AND derived_cycle IS NOT NULL
      GROUP BY derived_cycle
      ORDER BY derived_cycle
    `, [cid]);
    console.log('  cycle | first_attempts | approved | appr%');
    funnel.forEach(r => {
      const pct = r.first_attempts > 0 ? (r.approved / r.first_attempts * 100).toFixed(1) + '%' : 'n/a';
      console.log('  ' + String(r.cycle).padStart(5) + ' | ' + String(r.first_attempts).padStart(14) + ' | ' + String(r.approved).padStart(8) + ' | ' + pct);
    });
    console.log();
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
