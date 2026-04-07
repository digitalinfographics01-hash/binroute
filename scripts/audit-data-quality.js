#!/usr/bin/env node
/**
 * Comprehensive data quality audit — find EVERYTHING that's wrong before ML training.
 */
const path = require('path');
const { initDb, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  const clients = db.prepare('SELECT id, name FROM clients ORDER BY id').all();
  const issues = [];

  for (const client of clients) {
    const C = client.id;
    console.log('\n' + '='.repeat(70));
    console.log(`CLIENT ${C}: ${client.name}`);
    console.log('='.repeat(70));

    // ---------------------------------------------------------------
    // 1. Orders with NULL product_type_classified
    // ---------------------------------------------------------------
    const nullPtc = db.prepare(`
      SELECT COUNT(*) as cnt FROM orders
      WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND product_type_classified IS NULL
    `).get(C).cnt;
    if (nullPtc > 0) {
      issues.push({ client: C, issue: 'NULL product_type_classified', count: nullPtc });
      console.log(`\n  [1] NULL product_type_classified: ${nullPtc}`);

      // What product_ids?
      const nullPtcProducts = db.prepare(`
        SELECT product_ids, product_group_name, COUNT(*) as cnt FROM orders
        WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
          AND product_type_classified IS NULL
        GROUP BY product_ids, product_group_name ORDER BY cnt DESC LIMIT 10
      `).all(C);
      for (const p of nullPtcProducts) console.log(`      ${p.product_ids} (${p.product_group_name || 'NO GROUP'}): ${p.cnt}`);
    } else {
      console.log(`\n  [1] product_type_classified: all populated ✓`);
    }

    // ---------------------------------------------------------------
    // 2. Orders with NULL derived_product_role (but has ptc)
    // ---------------------------------------------------------------
    const nullRole = db.prepare(`
      SELECT product_type_classified, COUNT(*) as cnt FROM orders
      WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND derived_product_role IS NULL AND product_type_classified IS NOT NULL
      GROUP BY product_type_classified
    `).all(C);
    if (nullRole.length > 0) {
      const total = nullRole.reduce((s, r) => s + r.cnt, 0);
      issues.push({ client: C, issue: 'NULL derived_product_role', count: total });
      console.log(`  [2] NULL derived_product_role: ${total}`);
      for (const r of nullRole) console.log(`      ${r.product_type_classified}: ${r.cnt}`);
    } else {
      console.log(`  [2] derived_product_role: all populated ✓`);
    }

    // ---------------------------------------------------------------
    // 3. PGA type vs product_type_classified mismatch (non-anonymous orders)
    // ---------------------------------------------------------------
    const pgaMismatch = db.prepare(`
      SELECT pga.product_type as pga_type, o.product_type_classified as order_ptc, COUNT(*) as cnt
      FROM orders o
      JOIN product_group_assignments pga ON pga.client_id = o.client_id
        AND pga.product_id = REPLACE(REPLACE(SUBSTR(o.product_ids, 3, LENGTH(o.product_ids)-4), '"', ''), ' ', '')
      WHERE o.client_id = ?
        AND o.customer_id IS NOT NULL AND o.customer_id != 0
        AND o.order_status IN (2,6,7,8) AND o.is_test = 0 AND o.is_internal_test = 0
        AND o.product_type_classified IS NOT NULL
        AND pga.product_type != o.product_type_classified
        AND NOT (pga.product_type = 'initial_rebill' AND o.product_type_classified IN ('initial', 'rebill'))
        AND NOT (pga.product_type IN ('initial', 'rebill') AND o.product_type_classified = 'initial_rebill')
      GROUP BY pga.product_type, o.product_type_classified
      ORDER BY cnt DESC
    `).all(C);
    if (pgaMismatch.length > 0) {
      const total = pgaMismatch.reduce((s, r) => s + r.cnt, 0);
      issues.push({ client: C, issue: 'PGA vs order classification mismatch', count: total });
      console.log(`  [3] PGA type vs order classification mismatch: ${total}`);
      for (const r of pgaMismatch) console.log(`      PGA=${r.pga_type} → order=${r.order_ptc}: ${r.cnt}`);
    } else {
      console.log(`  [3] PGA vs order classification: consistent ✓`);
    }

    // ---------------------------------------------------------------
    // 4. Anonymous orders NOT classified as main_initial that should be
    // ---------------------------------------------------------------
    const anonWrong = db.prepare(`
      SELECT derived_product_role, product_type_classified, COUNT(*) as cnt
      FROM orders
      WHERE client_id = ? AND (customer_id IS NULL OR customer_id = 0)
        AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND product_type_classified IN ('initial', 'initial_rebill')
        AND derived_product_role != 'main_initial'
      GROUP BY derived_product_role, product_type_classified ORDER BY cnt DESC
    `).all(C);
    if (anonWrong.length > 0) {
      const total = anonWrong.reduce((s, r) => s + r.cnt, 0);
      issues.push({ client: C, issue: 'Anonymous initial/initial_rebill not main_initial', count: total });
      console.log(`  [4] Anonymous initial not classified as main_initial: ${total}`);
      for (const r of anonWrong) console.log(`      ${r.product_type_classified} → ${r.derived_product_role}: ${r.cnt}`);
    } else {
      console.log(`  [4] Anonymous initial classification: correct ✓`);
    }

    // ---------------------------------------------------------------
    // 5. Anonymous orders with rebill classification (should be excluded)
    // ---------------------------------------------------------------
    const anonRebill = db.prepare(`
      SELECT product_group_name, COUNT(*) as cnt
      FROM orders
      WHERE client_id = ? AND (customer_id IS NULL OR customer_id = 0)
        AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND product_type_classified = 'rebill'
      GROUP BY product_group_name ORDER BY cnt DESC
    `).all(C);
    if (anonRebill.length > 0) {
      const total = anonRebill.reduce((s, r) => s + r.cnt, 0);
      issues.push({ client: C, issue: 'Anonymous rebill orders (need exclusion)', count: total });
      console.log(`  [5] Anonymous rebill orders: ${total}`);
      for (const r of anonRebill) console.log(`      ${r.product_group_name}: ${r.cnt}`);
    } else {
      console.log(`  [5] Anonymous rebill: none ✓`);
    }

    // ---------------------------------------------------------------
    // 6. Anonymous orders with wrong straight_sale (PGA says initial)
    // ---------------------------------------------------------------
    const anonSsMismatch = db.prepare(`
      SELECT o.product_group_name, pga.product_type as pga_type, COUNT(*) as cnt
      FROM orders o
      JOIN product_group_assignments pga ON pga.client_id = o.client_id
        AND pga.product_id = REPLACE(REPLACE(SUBSTR(o.product_ids, 3, LENGTH(o.product_ids)-4), '"', ''), ' ', '')
      WHERE o.client_id = ?
        AND (o.customer_id IS NULL OR o.customer_id = 0)
        AND o.order_status IN (2,6,7,8) AND o.is_test = 0 AND o.is_internal_test = 0
        AND o.product_type_classified = 'straight_sale'
        AND pga.product_type IN ('initial', 'initial_rebill')
      GROUP BY o.product_group_name, pga.product_type ORDER BY cnt DESC
    `).all(C);
    if (anonSsMismatch.length > 0) {
      const total = anonSsMismatch.reduce((s, r) => s + r.cnt, 0);
      issues.push({ client: C, issue: 'Anonymous straight_sale but PGA says initial', count: total });
      console.log(`  [6] Anonymous straight_sale where PGA says initial: ${total}`);
      for (const r of anonSsMismatch) console.log(`      ${r.product_group_name} (PGA=${r.pga_type}): ${r.cnt}`);
    } else {
      console.log(`  [6] Anonymous straight_sale vs PGA: consistent ✓`);
    }

    // ---------------------------------------------------------------
    // 7. Customer orders with straight_sale but PGA says initial (Crown bug)
    // ---------------------------------------------------------------
    const custSsMismatch = db.prepare(`
      SELECT o.product_group_name, pga.product_type as pga_type, COUNT(*) as cnt
      FROM orders o
      JOIN product_group_assignments pga ON pga.client_id = o.client_id
        AND pga.product_id = REPLACE(REPLACE(SUBSTR(o.product_ids, 3, LENGTH(o.product_ids)-4), '"', ''), ' ', '')
      WHERE o.client_id = ?
        AND o.customer_id IS NOT NULL AND o.customer_id != 0
        AND o.order_status IN (2,6,7,8) AND o.is_test = 0 AND o.is_internal_test = 0
        AND o.product_type_classified = 'straight_sale'
        AND pga.product_type = 'initial'
      GROUP BY o.product_group_name ORDER BY cnt DESC
    `).all(C);
    if (custSsMismatch.length > 0) {
      const total = custSsMismatch.reduce((s, r) => s + r.cnt, 0);
      issues.push({ client: C, issue: 'Customer orders: straight_sale but PGA says initial', count: total });
      console.log(`  [7] Customer straight_sale where PGA says initial: ${total}`);
      for (const r of custSsMismatch) console.log(`      ${r.product_group_name}: ${r.cnt}`);
    } else {
      console.log(`  [7] Customer straight_sale vs PGA: consistent ✓`);
    }

    // ---------------------------------------------------------------
    // 8. Gateways missing processor_name (with recent orders)
    // ---------------------------------------------------------------
    const badGws = db.prepare(`
      SELECT DISTINCT g.gateway_id, g.gateway_alias FROM gateways g
      JOIN orders o ON o.client_id = g.client_id AND o.gateway_id = g.gateway_id
      WHERE g.client_id = ? AND g.exclude_from_analysis = 0
        AND (g.processor_name IS NULL OR g.processor_name = '')
        AND o.order_status IN (2,6,7,8) AND o.is_test = 0 AND o.is_internal_test = 0
        AND o.acquisition_date >= date('now', '-180 days')
    `).all(C);
    if (badGws.length > 0) {
      issues.push({ client: C, issue: 'Gateways missing processor_name', count: badGws.length });
      console.log(`  [8] Gateways missing processor_name: ${badGws.length}`);
      for (const g of badGws) console.log(`      GW ${g.gateway_id}: ${g.gateway_alias}`);
    } else {
      console.log(`  [8] Gateway processor_name: all configured ✓`);
    }

    // ---------------------------------------------------------------
    // 9. Product groups missing product_sequence
    // ---------------------------------------------------------------
    const noSeq = db.prepare(`
      SELECT pg.group_name, COUNT(DISTINCT o.id) as order_cnt
      FROM product_groups pg
      JOIN orders o ON o.client_id = pg.client_id AND o.product_group_id = pg.id
      WHERE pg.client_id = ? AND pg.product_sequence IS NULL
        AND o.order_status IN (2,6,7,8) AND o.is_test = 0 AND o.is_internal_test = 0
      GROUP BY pg.group_name ORDER BY order_cnt DESC
    `).all(C);
    if (noSeq.length > 0) {
      const total = noSeq.reduce((s, r) => s + r.order_cnt, 0);
      issues.push({ client: C, issue: 'Product groups without product_sequence', count: noSeq.length, orders: total });
      console.log(`  [9] Product groups without product_sequence: ${noSeq.length} groups (${total} orders)`);
      for (const g of noSeq) console.log(`      ${g.group_name}: ${g.order_cnt} orders`);
    } else {
      console.log(`  [9] Product sequence: all tagged ✓`);
    }

    // ---------------------------------------------------------------
    // 10. Orders missing product_group_id
    // ---------------------------------------------------------------
    const noGroup = db.prepare(`
      SELECT product_ids, COUNT(*) as cnt FROM orders
      WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND product_type_classified IN ('initial', 'rebill')
        AND product_group_id IS NULL
      GROUP BY product_ids ORDER BY cnt DESC LIMIT 10
    `).all(C);
    if (noGroup.length > 0) {
      const total = noGroup.reduce((s, r) => s + r.cnt, 0);
      issues.push({ client: C, issue: 'Initial/rebill orders missing product_group_id', count: total });
      console.log(`  [10] Initial/rebill missing product_group_id: ${total}`);
      for (const g of noGroup) console.log(`       ${g.product_ids}: ${g.cnt}`);
    } else {
      console.log(`  [10] Product group assignment: complete ✓`);
    }

    // ---------------------------------------------------------------
    // 11. NULL derived_cycle/attempt on non-anonymous classified orders
    // ---------------------------------------------------------------
    const nullCycleWithCust = db.prepare(`
      SELECT derived_product_role, COUNT(*) as cnt FROM orders
      WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND customer_id IS NOT NULL AND customer_id != 0
        AND derived_product_role IN ('main_initial', 'upsell_initial', 'main_rebill', 'upsell_rebill')
        AND (derived_cycle IS NULL OR derived_attempt IS NULL)
      GROUP BY derived_product_role ORDER BY cnt DESC
    `).all(C);
    if (nullCycleWithCust.length > 0) {
      const total = nullCycleWithCust.reduce((s, r) => s + r.cnt, 0);
      issues.push({ client: C, issue: 'Non-anonymous orders NULL cycle/attempt', count: total });
      console.log(`  [11] Non-anonymous with NULL cycle/attempt: ${total}`);
      for (const r of nullCycleWithCust) console.log(`       ${r.derived_product_role}: ${r.cnt}`);
    } else {
      console.log(`  [11] Non-anonymous cycle/attempt: all populated ✓`);
    }

    // ---------------------------------------------------------------
    // 12. Excluded gateways check
    // ---------------------------------------------------------------
    const excludedGws = db.prepare(`
      SELECT g.gateway_id, g.gateway_alias, g.processor_name, COUNT(o.id) as order_cnt
      FROM gateways g
      JOIN orders o ON o.client_id = g.client_id AND o.gateway_id = g.gateway_id
      WHERE g.client_id = ? AND g.exclude_from_analysis = 1
        AND o.order_status IN (2,6,7,8)
      GROUP BY g.gateway_id ORDER BY order_cnt DESC
    `).all(C);
    if (excludedGws.length > 0) {
      console.log(`  [12] Excluded gateways (for reference):`);
      for (const g of excludedGws) console.log(`       GW ${g.gateway_id} ${g.gateway_alias} (${g.processor_name || 'no proc'}): ${g.order_cnt} orders`);
    } else {
      console.log(`  [12] No excluded gateways`);
    }
  }

  // ---------------------------------------------------------------
  // SUMMARY
  // ---------------------------------------------------------------
  console.log('\n' + '='.repeat(70));
  console.log('ISSUE SUMMARY');
  console.log('='.repeat(70));

  if (issues.length === 0) {
    console.log('\nNo issues found — data is clean!');
  } else {
    for (const i of issues) {
      console.log(`  Client ${i.client}: ${i.issue} (${i.count}${i.orders ? ' groups, ' + i.orders + ' orders' : ''})`);
    }
    console.log(`\nTotal issues: ${issues.length}`);
  }
}

main().catch(err => { console.error(err); process.exit(1); });
