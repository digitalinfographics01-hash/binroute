#!/usr/bin/env node
const path = require('path');
const { initDb, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  // Crown anonymous ViraFlexx Max ME orders — what did they actually get classified as?
  console.log('=== Crown anonymous ViraFlexx Max ME — actual classification ===\n');

  const breakdown = db.prepare(`
    SELECT product_ids, product_type_classified, derived_product_role, tx_type, COUNT(*) as cnt
    FROM orders
    WHERE client_id = 4
      AND (customer_id IS NULL OR customer_id = 0)
      AND product_group_name = 'ViraFlexx Max ME'
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
    GROUP BY product_ids, product_type_classified, derived_product_role, tx_type
    ORDER BY cnt DESC
  `).all();

  for (const r of breakdown) {
    console.log(`  pids=${r.product_ids} → ptc=${r.product_type_classified} role=${r.derived_product_role} tx_type=${r.tx_type} (${r.cnt})`);
  }

  // Check PGA for product 70
  console.log('\n=== PGA for product 70 ===');
  const pga70 = db.prepare(`
    SELECT pga.product_id, pga.product_type, pga.product_group_id, pg.group_name
    FROM product_group_assignments pga
    JOIN product_groups pg ON pg.id = pga.product_group_id
    WHERE pga.client_id = 4 AND pga.product_id = '70'
  `).all();
  for (const r of pga70) console.log(`  pid=70 type=${r.product_type} group=${r.group_name} (group_id=${r.product_group_id})`);

  // So if product 70 has type=initial, how did the order end up as straight_sale?
  // Let's check: maybe the order was first classified with a DIFFERENT product_type
  // and then derived_product_role was set from that

  // Sample a few specific orders
  console.log('\n=== Sample Crown anonymous orders on product 70 ===');
  const samples = db.prepare(`
    SELECT id, order_id, customer_id, product_ids, product_type_classified, derived_product_role,
           tx_type, product_group_id, product_group_name, billing_cycle, is_recurring, retry_attempt
    FROM orders
    WHERE client_id = 4
      AND (customer_id IS NULL OR customer_id = 0)
      AND product_ids LIKE '%"70"%'
      AND order_status = 7 AND is_test = 0 AND is_internal_test = 0
    LIMIT 5
  `).all();
  for (const s of samples) console.log(JSON.stringify(s));

  // Check: does product 70 appear with MULTIPLE product_types in PGA?
  console.log('\n=== All PGA entries for Crown (client 4) product 70 ===');
  const allPga = db.prepare(`
    SELECT * FROM product_group_assignments WHERE client_id = 4 AND product_id = '70'
  `).all();
  console.log(JSON.stringify(allPga, null, 2));

  // CRITICAL CHECK: Is product_type_classified being set to 'straight_sale' even though PGA says 'initial'?
  // Maybe the product was CHANGED at some point, or maybe PGA has TWO entries

  // Check ALL Crown products and their PGA types vs what anonymous orders got classified as
  console.log('\n=== Crown — PGA type vs actual classification on anonymous orders ===');
  const mismatch = db.prepare(`
    SELECT
      o.product_ids,
      pga.product_type as pga_type,
      o.product_type_classified as order_ptc,
      o.derived_product_role as order_role,
      COUNT(*) as cnt
    FROM orders o
    LEFT JOIN product_group_assignments pga ON pga.client_id = o.client_id
      AND pga.product_id = (
        SELECT REPLACE(REPLACE(SUBSTR(o.product_ids, 3, LENGTH(o.product_ids)-4), '"', ''), ' ', '')
      )
    WHERE o.client_id = 4
      AND (o.customer_id IS NULL OR o.customer_id = 0)
      AND o.order_status IN (2,6,7,8) AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.product_group_name IS NOT NULL
    GROUP BY o.product_ids, pga.product_type, o.product_type_classified, o.derived_product_role
    ORDER BY cnt DESC LIMIT 20
  `).all();

  console.log(`\n  ${'PIDs'.padEnd(15)} ${'PGA Type'.padEnd(15)} ${'Order PTC'.padEnd(15)} ${'Role'.padEnd(18)} Count`);
  console.log('  ' + '-'.repeat(75));
  for (const r of mismatch) {
    const match = r.pga_type === r.order_ptc ? '' : ' *** MISMATCH ***';
    console.log(`  ${(r.product_ids||'').padEnd(15)} ${(r.pga_type||'NULL').padEnd(15)} ${(r.order_ptc||'NULL').padEnd(15)} ${(r.order_role||'NULL').padEnd(18)} ${r.cnt}${match}`);
  }
}

main().catch(err => { console.error(err); process.exit(1); });
