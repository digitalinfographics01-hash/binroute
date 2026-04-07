#!/usr/bin/env node
const path = require('path');
const { initDb, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  console.log('=== ANONYMOUS REBILL ATTEMPTS (357) ===');
  const byClient = db.prepare(`
    SELECT client_id, model_target, COUNT(*) as cnt
    FROM transaction_attempts
    WHERE (customer_id IS NULL OR customer_id = 0) AND model_target = 'rebill'
    GROUP BY client_id ORDER BY client_id
  `).all();
  console.log('\nBy client:');
  for (const r of byClient) console.log(`  Client ${r.client_id}: ${r.cnt}`);

  console.log('\n=== SOURCE ORDERS ===');
  const orderIds = db.prepare(`
    SELECT DISTINCT order_id FROM transaction_attempts
    WHERE (customer_id IS NULL OR customer_id = 0) AND model_target = 'rebill'
  `).all().map(r => r.order_id);
  console.log(`${orderIds.length} distinct orders`);

  // Sample
  const sampleIds = orderIds.slice(0, 15);
  const ph = sampleIds.map(() => '?').join(',');
  const samples = db.prepare(`
    SELECT order_id, customer_id, billing_cycle, is_recurring, tx_type,
           product_type_classified, derived_product_role, product_group_name,
           order_status, is_cascaded, order_total, acquisition_date, client_id
    FROM orders WHERE id IN (${ph})
  `).all(...sampleIds);
  console.log('\nSample source orders:');
  for (const s of samples) console.log(JSON.stringify(s));

  // customer_id check
  const nullCheck = db.prepare(`
    SELECT
      SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_cust,
      SUM(CASE WHEN customer_id = 0 THEN 1 ELSE 0 END) as zero_cust,
      COUNT(*) as total
    FROM orders WHERE id IN (SELECT DISTINCT order_id FROM transaction_attempts
      WHERE (customer_id IS NULL OR customer_id = 0) AND model_target = 'rebill')
  `).get();
  console.log('\nCustomer ID on source orders:', JSON.stringify(nullCheck));

  // tx_type breakdown
  const txTypes = db.prepare(`
    SELECT tx_type, product_type_classified, derived_product_role, COUNT(*) as cnt
    FROM orders WHERE id IN (SELECT DISTINCT order_id FROM transaction_attempts
      WHERE (customer_id IS NULL OR customer_id = 0) AND model_target = 'rebill')
    GROUP BY tx_type, product_type_classified, derived_product_role ORDER BY cnt DESC
  `).all();
  console.log('\ntx_type breakdown:');
  for (const t of txTypes) console.log(`  ${t.tx_type} / ${t.product_type_classified} / ${t.derived_product_role}: ${t.cnt}`);

  // Are they rebill by product_type or by billing_cycle?
  const billingCycle = db.prepare(`
    SELECT billing_cycle, is_recurring, COUNT(*) as cnt
    FROM orders WHERE id IN (SELECT DISTINCT order_id FROM transaction_attempts
      WHERE (customer_id IS NULL OR customer_id = 0) AND model_target = 'rebill')
    GROUP BY billing_cycle, is_recurring ORDER BY cnt DESC
  `).all();
  console.log('\nbilling_cycle / is_recurring:');
  for (const b of billingCycle) console.log(`  cycle=${b.billing_cycle} recurring=${b.is_recurring}: ${b.cnt}`);

  // Excluded anonymous
  console.log('\n=== ANONYMOUS EXCLUDED (1175) ===');
  const exclTypes = db.prepare(`
    SELECT tx_type, product_type_classified, derived_product_role, COUNT(*) as cnt
    FROM orders WHERE id IN (SELECT DISTINCT order_id FROM transaction_attempts
      WHERE (customer_id IS NULL OR customer_id = 0) AND model_target = 'excluded')
    GROUP BY tx_type, product_type_classified, derived_product_role ORDER BY cnt DESC
  `).all();
  for (const t of exclTypes) console.log(`  ${t.tx_type} / ${t.product_type_classified} / ${t.derived_product_role}: ${t.cnt}`);
}

main().catch(err => { console.error(err); process.exit(1); });
