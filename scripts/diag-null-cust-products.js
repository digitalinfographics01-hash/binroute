#!/usr/bin/env node
const path = require('path');
const { initDb, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  const clients = db.prepare('SELECT id, name FROM clients ORDER BY id').all();

  for (const client of clients) {
    const C = client.id;
    console.log('\n' + '='.repeat(60));
    console.log(`Client ${C}: ${client.name}`);
    console.log('='.repeat(60));

    // Product group breakdown for NULL customer_id orders
    const products = db.prepare(`
      SELECT
        o.product_group_name,
        o.product_type_classified,
        o.derived_product_role,
        pg.product_sequence,
        pga.product_type as assigned_type,
        COUNT(*) as cnt
      FROM orders o
      LEFT JOIN product_groups pg ON pg.id = o.product_group_id
      LEFT JOIN product_group_assignments pga ON pga.product_group_id = o.product_group_id AND pga.client_id = o.client_id
      WHERE o.client_id = ?
        AND (o.customer_id IS NULL OR o.customer_id = 0)
        AND o.order_status IN (2, 6, 7, 8) AND o.is_test = 0 AND o.is_internal_test = 0
      GROUP BY o.product_group_name, o.product_type_classified, o.derived_product_role, pg.product_sequence, pga.product_type
      ORDER BY cnt DESC
    `).all(C);

    if (products.length === 0) {
      console.log('  No anonymous orders');
      continue;
    }

    console.log(`\n  ${'Product Group'.padEnd(35)} ${'Classified'.padEnd(15)} ${'Role'.padEnd(18)} ${'Seq'.padEnd(8)} ${'Assigned'.padEnd(15)} Count`);
    console.log('  ' + '-'.repeat(110));
    for (const p of products) {
      console.log(`  ${(p.product_group_name || 'NULL').padEnd(35)} ${(p.product_type_classified || 'NULL').padEnd(15)} ${(p.derived_product_role || 'NULL').padEnd(18)} ${(p.product_sequence || 'NULL').padEnd(8)} ${(p.assigned_type || 'NULL').padEnd(15)} ${p.cnt}`);
    }

    // Also show the actual product_ids for NULL product_group orders
    const nullPg = db.prepare(`
      SELECT product_ids, COUNT(*) as cnt
      FROM orders
      WHERE client_id = ?
        AND (customer_id IS NULL OR customer_id = 0)
        AND order_status IN (2, 6, 7, 8) AND is_test = 0 AND is_internal_test = 0
        AND product_group_id IS NULL
      GROUP BY product_ids ORDER BY cnt DESC LIMIT 20
    `).all(C);

    if (nullPg.length > 0) {
      console.log('\n  Orders with NULL product_group — product_ids:');
      for (const p of nullPg) console.log(`    ${p.product_ids}: ${p.cnt}`);
    }
  }
}

main().catch(err => { console.error(err); process.exit(1); });
