#!/usr/bin/env node
/**
 * Diagnose classification issues — why do orders have NULL derived_cycle/derived_attempt?
 */
const path = require('path');
const { initDb, querySql, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  const clients = querySql('SELECT id, name FROM clients ORDER BY id');

  for (const client of clients) {
    const C = client.id;
    console.log('\n' + '='.repeat(60));
    console.log(`Client ${C}: ${client.name}`);
    console.log('='.repeat(60));

    // 1. NULL cycle/attempt breakdown by role
    const byRole = db.prepare(`
      SELECT derived_product_role, product_type_classified, COUNT(*) as cnt
      FROM orders
      WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND derived_product_role IN ('main_initial', 'upsell_initial', 'main_rebill', 'upsell_rebill')
        AND (derived_cycle IS NULL OR derived_attempt IS NULL)
      GROUP BY derived_product_role, product_type_classified ORDER BY cnt DESC
    `).all(C);
    console.log('\nNULL cycle/attempt by role:');
    let totalNull = 0;
    for (const r of byRole) {
      console.log(`  ${r.derived_product_role} / ${r.product_type_classified}: ${r.cnt}`);
      totalNull += r.cnt;
    }
    console.log(`  TOTAL: ${totalNull}`);

    // 2. Why? Check customer_id and product_group_id
    const causes = db.prepare(`
      SELECT
        SUM(CASE WHEN customer_id IS NULL OR customer_id = 0 THEN 1 ELSE 0 END) as no_customer,
        SUM(CASE WHEN product_group_id IS NULL THEN 1 ELSE 0 END) as no_product_group,
        SUM(CASE WHEN customer_id IS NOT NULL AND customer_id != 0 AND product_group_id IS NOT NULL THEN 1 ELSE 0 END) as has_both,
        COUNT(*) as total
      FROM orders
      WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND derived_product_role IN ('main_initial', 'upsell_initial', 'main_rebill', 'upsell_rebill')
        AND (derived_cycle IS NULL OR derived_attempt IS NULL)
    `).get(C);
    console.log('\nCause analysis:');
    console.log(`  No customer_id: ${causes.no_customer}`);
    console.log(`  No product_group_id: ${causes.no_product_group}`);
    console.log(`  Has both (should have been computed): ${causes.has_both}`);

    // 3. For those that have both customer + product_group, what's going on?
    if (causes.has_both > 0) {
      const samples = db.prepare(`
        SELECT order_id, customer_id, product_group_id, product_type_classified,
               derived_product_role, billing_cycle, is_recurring, derived_cycle, derived_attempt,
               order_status, acquisition_date
        FROM orders
        WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
          AND derived_product_role IN ('main_initial', 'upsell_initial', 'main_rebill', 'upsell_rebill')
          AND (derived_cycle IS NULL OR derived_attempt IS NULL)
          AND customer_id IS NOT NULL AND customer_id != 0
          AND product_group_id IS NOT NULL
        LIMIT 10
      `).all(C);
      console.log(`\nSample rows with both customer+product but NULL cycle (${causes.has_both} total):`);
      for (const s of samples) console.log('  ', JSON.stringify(s));
    }

    // 4. Total product_type_classified = NULL
    const nullPtc = db.prepare(`
      SELECT COUNT(*) as cnt FROM orders
      WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND product_type_classified IS NULL
    `).get(C);
    console.log(`\nNULL product_type_classified: ${nullPtc.cnt}`);

    // 5. NULL derived_product_role
    const nullRole = db.prepare(`
      SELECT product_type_classified, COUNT(*) as cnt FROM orders
      WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND derived_product_role IS NULL AND product_type_classified IS NOT NULL
      GROUP BY product_type_classified
    `).all(C);
    if (nullRole.length > 0) {
      console.log('\nNULL derived_product_role (has product_type):');
      for (const r of nullRole) console.log(`  ${r.product_type_classified}: ${r.cnt}`);
    }

    // 6. Gateways missing processor_name
    const badGws = db.prepare(`
      SELECT DISTINCT g.gateway_id, g.gateway_alias FROM gateways g
      JOIN orders o ON o.client_id = g.client_id AND o.gateway_id = g.gateway_id
      WHERE g.client_id = ? AND g.exclude_from_analysis = 0
        AND (g.processor_name IS NULL OR g.processor_name = '')
        AND o.order_status IN (2,6,7,8) AND o.is_test = 0 AND o.is_internal_test = 0
        AND o.acquisition_date >= date('now', '-180 days')
    `).all(C);
    if (badGws.length > 0) {
      console.log('\nGateways missing processor_name:');
      for (const g of badGws) console.log(`  GW ${g.gateway_id}: ${g.gateway_alias}`);
    }
  }
}

main().catch(err => { console.error(err); process.exit(1); });
