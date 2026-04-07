#!/usr/bin/env node
/**
 * Full product audit:
 * 1. Find ALL product_ids in orders that are NOT in products_catalog or product_group_assignments
 * 2. Look up missing products on Sticky.io API
 * 3. Audit NULL customer_id orders — what products do they have and are they classified correctly?
 * 4. Check for product_type mismatches (rebill product assigned to initial orders, etc.)
 */
const path = require('path');
const { initDb, querySql, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));
const StickyClient = require(path.join(__dirname, '..', 'src', 'api', 'sticky-client'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  const clients = db.prepare('SELECT id, name, sticky_base_url, sticky_username, sticky_password FROM clients ORDER BY id').all();

  for (const client of clients) {
    console.log('\n' + '='.repeat(70));
    console.log(`CLIENT ${client.id}: ${client.name}`);
    console.log('='.repeat(70));

    const sticky = new StickyClient({
      baseUrl: client.sticky_base_url,
      username: client.sticky_username,
      password: client.sticky_password
    });

    // ---------------------------------------------------------------
    // PART 1: Find ALL distinct product_ids referenced in orders
    // ---------------------------------------------------------------
    console.log('\n--- PART 1: Product ID inventory ---');

    const allProductIds = db.prepare(`
      SELECT DISTINCT product_ids FROM orders
      WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND product_ids IS NOT NULL AND product_ids != '[]'
    `).all(client.id);

    // Extract unique product IDs from JSON arrays
    const pidSet = new Set();
    for (const row of allProductIds) {
      try {
        const ids = JSON.parse(row.product_ids);
        if (Array.isArray(ids)) ids.forEach(id => pidSet.add(String(id)));
      } catch (e) {}
    }
    const allPids = [...pidSet].sort((a, b) => parseInt(a) - parseInt(b));
    console.log(`Total unique product_ids in orders: ${allPids.length}`);

    // Check which ones are in catalog
    const catalogPids = new Set(
      db.prepare('SELECT product_id FROM products_catalog WHERE client_id = ?').all(client.id)
        .map(r => String(r.product_id))
    );

    // Check which ones are in product_group_assignments
    const pgaPids = new Set(
      db.prepare('SELECT product_id FROM product_group_assignments WHERE client_id = ?').all(client.id)
        .map(r => String(r.product_id))
    );

    const missingFromCatalog = allPids.filter(pid => !catalogPids.has(pid));
    const missingFromPga = allPids.filter(pid => !pgaPids.has(pid));
    const missingFromBoth = allPids.filter(pid => !catalogPids.has(pid) && !pgaPids.has(pid));

    console.log(`In catalog: ${allPids.length - missingFromCatalog.length}/${allPids.length}`);
    console.log(`In product_group_assignments: ${allPids.length - missingFromPga.length}/${allPids.length}`);
    console.log(`Missing from BOTH: ${missingFromBoth.length}`);

    // ---------------------------------------------------------------
    // PART 2: Look up missing products on Sticky.io
    // ---------------------------------------------------------------
    if (missingFromBoth.length > 0) {
      console.log('\n--- PART 2: Missing products (looking up on Sticky.io) ---');

      for (const pid of missingFromBoth) {
        // Count how many orders use this product
        const orderCount = db.prepare(`
          SELECT COUNT(*) as cnt FROM orders
          WHERE client_id = ? AND product_ids LIKE ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        `).get(client.id, `%"${pid}"%`).cnt;

        try {
          const result = await sticky._post('product_index', { product_id: pid });
          if (result.products && result.products[pid]) {
            const p = result.products[pid];
            console.log(`\n  Product ${pid}: "${p.product_name}"`);
            console.log(`    Category: ${p.product_category_name || 'none'}`);
            console.log(`    Price: $${p.product_price}, Trial: ${p.product_is_trial}, Rebill: ${p.product_rebill_product}`);
            console.log(`    Orders using this product: ${orderCount}`);
          } else {
            console.log(`\n  Product ${pid}: NOT FOUND on Sticky.io (${orderCount} orders)`);
          }
        } catch (e) {
          console.log(`\n  Product ${pid}: API ERROR — ${e.message} (${orderCount} orders)`);
        }
      }
    }

    // Products in catalog but NOT in PGA (no group assignment)
    const inCatalogNotPga = allPids.filter(pid => catalogPids.has(pid) && !pgaPids.has(pid));
    if (inCatalogNotPga.length > 0) {
      console.log('\n--- Products in catalog but NO group assignment ---');
      for (const pid of inCatalogNotPga) {
        const cat = db.prepare('SELECT product_name FROM products_catalog WHERE client_id = ? AND product_id = ?').get(client.id, pid);
        const orderCount = db.prepare(`
          SELECT COUNT(*) as cnt FROM orders
          WHERE client_id = ? AND product_ids LIKE ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        `).get(client.id, `%"${pid}"%`).cnt;
        console.log(`  ${pid}: ${cat ? cat.product_name : '???'} (${orderCount} orders)`);
      }
    }

    // ---------------------------------------------------------------
    // PART 3: NULL customer orders — product classification audit
    // ---------------------------------------------------------------
    console.log('\n--- PART 3: NULL customer_id orders — product audit ---');

    const nullCustProducts = db.prepare(`
      SELECT
        o.product_group_name,
        o.product_type_classified,
        o.derived_product_role,
        pg.product_sequence,
        COUNT(*) as cnt,
        COUNT(DISTINCT CASE WHEN o.product_ids IS NOT NULL THEN o.product_ids END) as unique_pids
      FROM orders o
      LEFT JOIN product_groups pg ON pg.id = o.product_group_id
      WHERE o.client_id = ?
        AND (o.customer_id IS NULL OR o.customer_id = 0)
        AND o.order_status IN (2,6,7,8) AND o.is_test = 0 AND o.is_internal_test = 0
      GROUP BY o.product_group_name, o.product_type_classified, o.derived_product_role, pg.product_sequence
      ORDER BY cnt DESC
    `).all(client.id);

    let totalAnon = 0;
    let needsReclass = 0;
    console.log(`\n  ${'Product Group'.padEnd(35)} ${'Classified'.padEnd(15)} ${'Role'.padEnd(18)} ${'Seq'.padEnd(8)} ${'Count'.padEnd(8)} Status`);
    console.log('  ' + '-'.repeat(100));

    for (const p of nullCustProducts) {
      totalAnon += p.cnt;
      const role = p.derived_product_role || 'NULL';
      const classified = p.product_type_classified || 'NULL';
      const group = p.product_group_name || 'NULL';
      const seq = p.product_sequence || 'NULL';

      // Determine if this needs reclassification
      let status = 'OK';
      if (role === 'NULL' || classified === 'NULL') {
        status = 'MISSING — needs classification';
        needsReclass += p.cnt;
      } else if (role !== 'main_initial' && classified !== 'initial') {
        // Anonymous orders should all be main_initial / initial
        if (classified === 'straight_sale' || classified === 'initial_rebill') {
          status = 'RECLASS → main_initial';
          needsReclass += p.cnt;
        } else if (classified === 'rebill') {
          status = 'WRONG — rebill with no customer';
          needsReclass += p.cnt;
        } else if (role === 'upsell_initial') {
          status = 'RECLASS → main_initial (no customer for upsell)';
          needsReclass += p.cnt;
        } else {
          status = 'REVIEW';
          needsReclass += p.cnt;
        }
      }

      console.log(`  ${group.padEnd(35)} ${classified.padEnd(15)} ${role.padEnd(18)} ${seq.padEnd(8)} ${String(p.cnt).padEnd(8)} ${status}`);
    }

    console.log(`\n  Total anonymous orders: ${totalAnon}`);
    console.log(`  Needs reclassification: ${needsReclass}`);
    console.log(`  Already correct (main_initial): ${totalAnon - needsReclass}`);

    // ---------------------------------------------------------------
    // PART 4: Product type consistency check
    // ---------------------------------------------------------------
    console.log('\n--- PART 4: Products with multiple types in PGA ---');
    const multiType = db.prepare(`
      SELECT pga.product_id, pc.product_name, pga.product_type, pg.group_name, pg.product_sequence
      FROM product_group_assignments pga
      JOIN products_catalog pc ON pc.client_id = pga.client_id AND pc.product_id = pga.product_id
      JOIN product_groups pg ON pg.id = pga.product_group_id
      WHERE pga.client_id = ?
      ORDER BY pga.product_id, pga.product_type
    `).all(client.id);

    // Group by product_id
    const byPid = new Map();
    for (const r of multiType) {
      if (!byPid.has(r.product_id)) byPid.set(r.product_id, []);
      byPid.get(r.product_id).push(r);
    }

    // Find products assigned to multiple groups or with multiple types
    let multiCount = 0;
    for (const [pid, rows] of byPid) {
      const types = [...new Set(rows.map(r => r.product_type))];
      const groups = [...new Set(rows.map(r => r.group_name))];
      if (types.length > 1 || groups.length > 1) {
        if (multiCount === 0) console.log('');
        console.log(`  Product ${pid} (${rows[0].product_name}):`);
        for (const r of rows) console.log(`    → Group: ${r.group_name}, Type: ${r.product_type}, Seq: ${r.product_sequence}`);
        multiCount++;
      }
    }
    if (multiCount === 0) console.log('  None — all products have single assignment');
  }
}

main().catch(err => { console.error(err); process.exit(1); });
