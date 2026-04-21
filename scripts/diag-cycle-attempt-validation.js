/**
 * Two diagnostics:
 * 1. Sample orders where product_type='rebill' AND derived_cycle=0
 *    (the "upsell rebilling on same product from cycle 0" case)
 * 2. Per-(cycle × attempt) approval rates for Optimus to verify the existing
 *    derived_cycle / derived_attempt fields are correct.
 */
const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  // ────────────────────────────────────────────────────────────
  // 1. Rebill-type products at cycle 0 (the upsell-from-zero case)
  // ────────────────────────────────────────────────────────────
  console.log('='.repeat(72));
  console.log('1. Orders where product.type=rebill AND derived_cycle=0');
  console.log('='.repeat(72));
  console.log('(These should be upsell products being sold for the first time)');
  console.log();

  for (const cid of [1, 3]) {
    const clientName = querySql('SELECT name FROM clients WHERE id=?', [cid])[0]?.name;
    const stats = querySql(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
        SUM(CASE WHEN o.order_status = 7 THEN 1 ELSE 0 END) as declined
      FROM orders o
      INNER JOIN product_group_assignments pga
        ON pga.client_id = o.client_id
        AND CAST(pga.product_id AS TEXT) = JSON_EXTRACT(o.product_ids, '$[0]')
      INNER JOIN product_groups pg ON pg.id = pga.product_group_id
      WHERE o.client_id = ?
        AND pga.product_type = 'rebill'
        AND o.derived_cycle = 0
        AND COALESCE(o.is_internal_test, 0) = 0
    `, [cid])[0];

    const denom = stats.approved + stats.declined;
    const pct = denom > 0 ? ((stats.approved / denom) * 100).toFixed(1) + '%' : 'n/a';
    console.log('Client ' + cid + ' (' + clientName + '):');
    console.log('  Orders with rebill-type product at cycle 0: ' + stats.total);
    console.log('  Approved: ' + stats.approved + ', Declined: ' + stats.declined + ', Rate: ' + pct);

    if (stats.total > 0) {
      // Show top products in this category
      const topProducts = querySql(`
        SELECT
          pga.product_id, pc.product_name, pg.product_sequence,
          COUNT(*) as n,
          SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved
        FROM orders o
        INNER JOIN product_group_assignments pga
          ON pga.client_id = o.client_id
          AND CAST(pga.product_id AS TEXT) = JSON_EXTRACT(o.product_ids, '$[0]')
        INNER JOIN product_groups pg ON pg.id = pga.product_group_id
        LEFT JOIN products_catalog pc ON pc.client_id = pga.client_id AND pc.product_id = pga.product_id
        WHERE o.client_id = ?
          AND pga.product_type = 'rebill'
          AND o.derived_cycle = 0
        GROUP BY pga.product_id
        ORDER BY n DESC
        LIMIT 10
      `, [cid]);
      console.log('  Top products:');
      topProducts.forEach(p => {
        console.log('    pid=' + p.product_id + ' seq=' + p.product_sequence + ' n=' + p.n + ' appr=' + (p.approved/p.n*100).toFixed(1) + '% — ' + (p.product_name || '?').substring(0, 50));
      });
    }
    console.log();
  }

  // ────────────────────────────────────────────────────────────
  // 2. Per-(cycle × attempt) approval rate for Optimus
  // ────────────────────────────────────────────────────────────
  console.log('='.repeat(72));
  console.log('2. Optimus: approval rate per (derived_cycle × derived_attempt)');
  console.log('='.repeat(72));
  console.log('Expected: cycle N attempt 1 (first attempt) >> cycle N attempt 2+ (salvage)');
  console.log();

  const matrix = querySql(`
    SELECT
      derived_cycle as cycle,
      derived_attempt as attempt,
      COUNT(*) as total,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      SUM(CASE WHEN order_status = 7 THEN 1 ELSE 0 END) as declined
    FROM orders
    WHERE client_id = 3
      AND derived_cycle IS NOT NULL
      AND derived_attempt IS NOT NULL
      AND COALESCE(is_internal_test, 0) = 0
    GROUP BY derived_cycle, derived_attempt
    HAVING total >= 50
    ORDER BY derived_cycle, derived_attempt
  `);

  console.log('cycle | attempt | total    | approved | declined | appr%');
  let lastCycle = -1;
  for (const r of matrix) {
    if (r.cycle !== lastCycle) {
      console.log('-'.repeat(60));
      lastCycle = r.cycle;
    }
    const denom = r.approved + r.declined;
    const pct = denom > 0 ? ((r.approved / denom) * 100).toFixed(1) + '%' : 'n/a';
    console.log(
      '  ' + String(r.cycle).padStart(3) +
      ' | ' + String(r.attempt).padStart(7) +
      ' | ' + String(r.total).padStart(8) +
      ' | ' + String(r.approved).padStart(8) +
      ' | ' + String(r.declined).padStart(8) +
      ' | ' + pct
    );
  }

  // ────────────────────────────────────────────────────────────
  // 3. Same for Kytsan as a cross-check
  // ────────────────────────────────────────────────────────────
  console.log();
  console.log('='.repeat(72));
  console.log('3. Kytsan: approval rate per (derived_cycle × derived_attempt) — top values');
  console.log('='.repeat(72));
  const kytsanMatrix = querySql(`
    SELECT
      derived_cycle as cycle,
      derived_attempt as attempt,
      COUNT(*) as total,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved
    FROM orders
    WHERE client_id = 1
      AND derived_cycle IS NOT NULL
      AND derived_attempt IS NOT NULL
      AND COALESCE(is_internal_test, 0) = 0
    GROUP BY derived_cycle, derived_attempt
    HAVING total >= 50
    ORDER BY derived_cycle, derived_attempt
  `);

  console.log('cycle | attempt | total    | appr%');
  lastCycle = -1;
  for (const r of kytsanMatrix) {
    if (r.cycle !== lastCycle) {
      console.log('-'.repeat(48));
      lastCycle = r.cycle;
    }
    const pct = r.total > 0 ? ((r.approved / r.total) * 100).toFixed(1) + '%' : 'n/a';
    console.log('  ' + String(r.cycle).padStart(3) + ' | ' + String(r.attempt).padStart(7) + ' | ' + String(r.total).padStart(8) + ' | ' + pct);
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
