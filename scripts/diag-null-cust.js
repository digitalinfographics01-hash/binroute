#!/usr/bin/env node
const path = require('path');
const { initDb, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  console.log('=== NULL CUSTOMER_ID ORDERS — FULL SCOPE ===\n');

  // What roles do NULL customer_id orders currently have?
  const roles = db.prepare(`
    SELECT client_id, derived_product_role, product_type_classified, COUNT(*) as cnt
    FROM orders
    WHERE (customer_id IS NULL OR customer_id = 0)
      AND order_status IN (2, 6, 7, 8) AND is_test = 0 AND is_internal_test = 0
    GROUP BY client_id, derived_product_role, product_type_classified
    ORDER BY client_id, cnt DESC
  `).all();

  let currentClient = null;
  for (const r of roles) {
    if (r.client_id !== currentClient) {
      currentClient = r.client_id;
      console.log(`Client ${r.client_id}:`);
    }
    console.log(`  ${r.derived_product_role || 'NULL'} / ${r.product_type_classified || 'NULL'}: ${r.cnt}`);
  }

  // How many need reclassification (not already main_initial)?
  console.log('\n=== NEEDS RECLASSIFICATION ===');
  const needsFix = db.prepare(`
    SELECT client_id, derived_product_role, product_type_classified, COUNT(*) as cnt
    FROM orders
    WHERE (customer_id IS NULL OR customer_id = 0)
      AND order_status IN (2, 6, 7, 8) AND is_test = 0 AND is_internal_test = 0
      AND (derived_product_role != 'main_initial' OR derived_product_role IS NULL
           OR product_type_classified != 'initial' OR product_type_classified IS NULL)
    GROUP BY client_id, derived_product_role, product_type_classified
    ORDER BY client_id, cnt DESC
  `).all();

  let total = 0;
  for (const r of needsFix) {
    console.log(`  Client ${r.client_id}: ${r.derived_product_role || 'NULL'} / ${r.product_type_classified || 'NULL'} → main_initial/initial: ${r.cnt}`);
    total += r.cnt;
  }
  console.log(`\nTotal orders to reclassify: ${total}`);

  // What about transaction_attempts?
  console.log('\n=== TRANSACTION_ATTEMPTS IMPACT ===');
  const taFix = db.prepare(`
    SELECT client_id, model_target, derived_product_role, COUNT(*) as cnt
    FROM transaction_attempts
    WHERE (customer_id IS NULL OR customer_id = 0)
      AND model_target NOT IN ('initial', 'cascade')
    GROUP BY client_id, model_target, derived_product_role
    ORDER BY client_id, cnt DESC
  `).all();
  let taTotal = 0;
  for (const r of taFix) {
    console.log(`  Client ${r.client_id}: ${r.model_target} / ${r.derived_product_role || 'NULL'} → needs fix: ${r.cnt}`);
    taTotal += r.cnt;
  }
  console.log(`\nTotal attempt rows to fix: ${taTotal}`);

  // Also check: are there any approved anonymous orders?
  console.log('\n=== ANONYMOUS ORDER OUTCOMES ===');
  const outcomes = db.prepare(`
    SELECT client_id, order_status, COUNT(*) as cnt
    FROM orders
    WHERE (customer_id IS NULL OR customer_id = 0)
      AND order_status IN (2, 6, 7, 8) AND is_test = 0 AND is_internal_test = 0
    GROUP BY client_id, order_status
    ORDER BY client_id, order_status
  `).all();
  for (const r of outcomes) {
    const status = r.order_status === 7 ? 'declined' : 'approved';
    console.log(`  Client ${r.client_id}: status=${r.order_status} (${status}): ${r.cnt}`);
  }
}

main().catch(err => { console.error(err); process.exit(1); });
