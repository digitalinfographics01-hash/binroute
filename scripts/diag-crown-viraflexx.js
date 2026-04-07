#!/usr/bin/env node
const path = require('path');
const { initDb, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  console.log('='.repeat(70));
  console.log('CROWN — ViraFlexx Max ME Product Investigation');
  console.log('='.repeat(70));

  // What product_ids are in the ViraFlexx Max ME group?
  const pgId = db.prepare(`
    SELECT id, group_name, product_sequence FROM product_groups
    WHERE client_id = 4 AND group_name LIKE '%ViraFlexx Max ME%'
  `).all();
  console.log('\nProduct Group:', JSON.stringify(pgId));

  if (pgId.length > 0) {
    const assignments = db.prepare(`
      SELECT pga.product_id, pc.product_name, pga.product_type
      FROM product_group_assignments pga
      JOIN products_catalog pc ON pc.client_id = pga.client_id AND pc.product_id = pga.product_id
      WHERE pga.client_id = 4 AND pga.product_group_id = ?
      ORDER BY CAST(pga.product_id AS INTEGER)
    `).all(pgId[0].id);

    console.log('\nProducts in this group:');
    for (const a of assignments) {
      console.log(`  ${a.product_id}: ${a.product_name} → type: ${a.product_type}`);
    }
  }

  // What product_ids do the anonymous ViraFlexx Max ME orders have?
  console.log('\n--- Anonymous ViraFlexx Max ME orders ---');
  const anonProducts = db.prepare(`
    SELECT product_ids, COUNT(*) as cnt
    FROM orders
    WHERE client_id = 4
      AND (customer_id IS NULL OR customer_id = 0)
      AND product_group_name = 'ViraFlexx Max ME'
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
    GROUP BY product_ids ORDER BY cnt DESC LIMIT 20
  `).all();
  console.log('\nProduct IDs on anonymous ViraFlexx Max ME orders:');
  for (const p of anonProducts) console.log(`  ${p.product_ids}: ${p.cnt} orders`);

  // Now check: what about orders WITH customer_id for ViraFlexx Max ME?
  console.log('\n--- ViraFlexx Max ME with customer_id ---');
  const withCust = db.prepare(`
    SELECT product_type_classified, derived_product_role, COUNT(*) as cnt
    FROM orders
    WHERE client_id = 4
      AND customer_id IS NOT NULL AND customer_id != 0
      AND product_group_name = 'ViraFlexx Max ME'
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
    GROUP BY product_type_classified, derived_product_role ORDER BY cnt DESC
  `).all();
  console.log('\nClassification of ViraFlexx Max ME WITH customer:');
  for (const w of withCust) console.log(`  ${w.product_type_classified} / ${w.derived_product_role}: ${w.cnt}`);

  // Why is it straight_sale? Check the product_type in PGA for the specific product_ids
  console.log('\n\n' + '='.repeat(70));
  console.log('PART 2 EXPLANATION — How classification works for anonymous orders');
  console.log('='.repeat(70));

  console.log(`
The classification pipeline in post-sync.js works like this:

1. Parse product_ids JSON from the order → get first product_id
2. Look up that product_id in product_group_assignments → get product_type
3. product_type becomes product_type_classified

For anonymous orders (customer_id = NULL):
- tx_type is set to 'anonymous_decline' early in the classifier
- BUT product_type_classified still gets set from the product_group_assignments lookup
- derived_product_role is then computed from product_type_classified + product_sequence

So if a product is assigned as type 'straight_sale' in PGA, the anonymous order gets:
  product_type_classified = 'straight_sale'
  derived_product_role = 'straight_sale'

Even though it's really an anonymous initial attempt.

The problem: product_type in PGA describes what the product IS (rebill product, initial product,
straight sale product), not what the ORDER is. An anonymous order using a "rebill product"
is still an anonymous initial attempt — we just can't determine the customer journey.
`);

  // Show the actual flow for a specific Crown anonymous order
  console.log('--- Trace: specific Crown anonymous order ---');
  const sample = db.prepare(`
    SELECT order_id, customer_id, product_ids, billing_cycle, is_recurring,
           tx_type, product_type_classified, derived_product_role, product_group_name, order_total
    FROM orders
    WHERE client_id = 4
      AND (customer_id IS NULL OR customer_id = 0)
      AND product_group_name = 'ViraFlexx Max ME'
      AND order_status = 7
    LIMIT 1
  `).get();

  if (sample) {
    let pid = null;
    try { pid = JSON.parse(sample.product_ids)[0]; } catch(e) {}
    const pga = pid ? db.prepare(
      'SELECT product_type FROM product_group_assignments WHERE client_id = 4 AND product_id = ?'
    ).get(String(pid)) : null;

    console.log(`\nOrder ${sample.order_id}:`);
    console.log(`  customer_id: ${sample.customer_id} (NULL → anonymous)`);
    console.log(`  product_ids: ${sample.product_ids} → first pid: ${pid}`);
    console.log(`  PGA lookup for pid ${pid}: product_type = "${pga ? pga.product_type : 'NOT FOUND'}"`);
    console.log(`  → product_type_classified = "${sample.product_type_classified}"`);
    console.log(`  → derived_product_role = "${sample.derived_product_role}"`);
    console.log(`  → tx_type = "${sample.tx_type}"`);
    console.log(`  `);
    console.log(`  SHOULD BE: product_type_classified = "initial", derived_product_role = "main_initial"`);
    console.log(`  REASON: No customer_id means we can't determine subscription context.`);
    console.log(`          It's an anonymous decline — always an initial attempt.`);
  }
}

main().catch(err => { console.error(err); process.exit(1); });
