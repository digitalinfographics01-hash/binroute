#!/usr/bin/env node
const path = require('path');
const { initDb, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  const r = db.prepare(`
    SELECT derived_product_role, product_type_classified, product_group_name, COUNT(*) as cnt
    FROM orders WHERE client_id = 1 AND customer_id IS NOT NULL AND customer_id != 0
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
      AND derived_product_role IN ('main_initial', 'upsell_initial', 'main_rebill', 'upsell_rebill')
      AND (derived_cycle IS NULL OR derived_attempt IS NULL)
    GROUP BY derived_product_role, product_type_classified, product_group_name ORDER BY cnt DESC
  `).all();
  console.log('Kytsan 1745 NULL cycle — breakdown:');
  for (const x of r) console.log(`  ${x.derived_product_role} / ${x.product_type_classified} / ${x.product_group_name}: ${x.cnt}`);

  const s = db.prepare(`
    SELECT order_id, customer_id, product_group_id, product_group_name,
           product_type_classified, derived_product_role, billing_cycle, is_recurring,
           derived_cycle, derived_attempt
    FROM orders WHERE client_id = 1 AND customer_id IS NOT NULL AND customer_id != 0
      AND derived_product_role = 'main_initial' AND derived_cycle IS NULL
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
    LIMIT 10
  `).all();
  console.log('\nSamples:');
  for (const x of s) console.log(JSON.stringify(x));

  // Are these product 266 orders that just got classified?
  const p266 = db.prepare(`
    SELECT COUNT(*) as cnt FROM orders
    WHERE client_id = 1 AND customer_id IS NOT NULL AND customer_id != 0
      AND product_ids LIKE '%"266"%'
      AND (derived_cycle IS NULL OR derived_attempt IS NULL)
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
  `).get();
  console.log('\nProduct 266 with customer but NULL cycle:', p266.cnt);
}

main().catch(err => { console.error(err); process.exit(1); });
