/**
 * Investigate existing cycle/attempt tracking in the orders table
 * and compute approval rates per cycle to see if higher cycles really
 * have higher approval rates.
 */
const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  // 1. Schema check — what cycle-related columns exist?
  console.log('='.repeat(72));
  console.log('1. Cycle-related columns on orders table');
  console.log('='.repeat(72));
  const cols = querySql('PRAGMA table_info(orders)').filter(c =>
    /cycle|attempt/i.test(c.name)
  );
  cols.forEach(c => console.log('  ' + c.name + ' (' + c.type + ')'));
  console.log();

  // 2. For each client, show distribution of derived_cycle and derived_attempt
  console.log('='.repeat(72));
  console.log('2. Population of derived_cycle / derived_attempt per client');
  console.log('='.repeat(72));
  for (const cid of [1, 2, 3, 4, 5]) {
    const stats = querySql(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN derived_cycle IS NOT NULL THEN 1 ELSE 0 END) as has_cycle,
        SUM(CASE WHEN derived_attempt IS NOT NULL THEN 1 ELSE 0 END) as has_attempt,
        SUM(CASE WHEN billing_cycle IS NOT NULL THEN 1 ELSE 0 END) as has_billing_cycle,
        MAX(derived_cycle) as max_derived_cycle,
        MAX(billing_cycle) as max_billing_cycle
      FROM orders WHERE client_id = ?
    `, [cid])[0];
    console.log('Client ' + cid + ':');
    console.log('  total=' + stats.total + ' has_derived_cycle=' + stats.has_cycle + ' has_derived_attempt=' + stats.has_attempt);
    console.log('  max_derived_cycle=' + stats.max_derived_cycle + ' max_billing_cycle=' + stats.max_billing_cycle);
  }
  console.log();

  // 3. For Optimus (largest), per-cycle approval rate using existing derived_cycle
  console.log('='.repeat(72));
  console.log('3. Per-cycle approval rate using existing derived_cycle (Optimus)');
  console.log('='.repeat(72));
  const optimusByCycle = querySql(`
    SELECT
      COALESCE(derived_cycle, 0) as cycle,
      COUNT(*) as total,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      SUM(CASE WHEN order_status = 7 THEN 1 ELSE 0 END) as declined
    FROM orders
    WHERE client_id = 3
      AND derived_product_role IN ('main_rebill', 'main_initial')
    GROUP BY cycle
    ORDER BY cycle
  `);
  console.log('cycle | total    | approved | declined | appr%');
  optimusByCycle.forEach(r => {
    const denom = r.approved + r.declined;
    const pct = denom > 0 ? ((r.approved / denom) * 100).toFixed(1) + '%' : 'n/a';
    console.log('  ' + String(r.cycle).padStart(3) + ' | ' + String(r.total).padStart(8) + ' | ' + String(r.approved).padStart(8) + ' | ' + String(r.declined).padStart(8) + ' | ' + pct);
  });
  console.log();

  // 4. Same but using billing_cycle for comparison
  console.log('='.repeat(72));
  console.log('4. Per-billing_cycle approval rate (Optimus rebill orders only)');
  console.log('='.repeat(72));
  const optimusByBilling = querySql(`
    SELECT
      COALESCE(billing_cycle, -1) as cycle,
      COUNT(*) as total,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      SUM(CASE WHEN order_status = 7 THEN 1 ELSE 0 END) as declined
    FROM orders
    WHERE client_id = 3
      AND COALESCE(is_internal_test, 0) = 0
    GROUP BY cycle
    ORDER BY cycle
  `);
  console.log('billing_cycle | total    | approved | declined | appr%');
  optimusByBilling.forEach(r => {
    const denom = r.approved + r.declined;
    const pct = denom > 0 ? ((r.approved / denom) * 100).toFixed(1) + '%' : 'n/a';
    console.log('  ' + String(r.cycle).padStart(11) + ' | ' + String(r.total).padStart(8) + ' | ' + String(r.approved).padStart(8) + ' | ' + String(r.declined).padStart(8) + ' | ' + pct);
  });
  console.log();

  // 5. For Optimus, look at the C4+ product specifically — what's its approval rate by attempt?
  console.log('='.repeat(72));
  console.log('5. Approval rate per product for Optimus rebill products (top 20)');
  console.log('='.repeat(72));
  const productRates = querySql(`
    SELECT
      pga.product_id,
      pc.product_name,
      COUNT(o.id) as total,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      SUM(CASE WHEN o.order_status = 7 THEN 1 ELSE 0 END) as declined
    FROM product_group_assignments pga
    LEFT JOIN products_catalog pc ON pc.client_id = pga.client_id AND pc.product_id = pga.product_id
    LEFT JOIN orders o ON o.client_id = pga.client_id
      AND JSON_EXTRACT(o.product_ids, '$[0]') = CAST(pga.product_id AS TEXT)
    WHERE pga.client_id = 3 AND pga.product_type = 'rebill'
    GROUP BY pga.product_id
    HAVING total > 100
    ORDER BY total DESC
    LIMIT 20
  `);
  console.log('product_id | total    | appr% | name');
  productRates.forEach(r => {
    const denom = r.approved + r.declined;
    const pct = denom > 0 ? ((r.approved / denom) * 100).toFixed(1) + '%' : 'n/a';
    console.log('  ' + String(r.product_id).padStart(7) + ' | ' + String(r.total).padStart(8) + ' | ' + pct.padStart(6) + ' | ' + (r.product_name || '?').substring(0, 50));
  });

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
