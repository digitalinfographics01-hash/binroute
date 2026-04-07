#!/usr/bin/env node
/**
 * Fix all classification issues identified by audit.
 *
 * Fix 1: Kytsan product 266 — add to catalog + PGA as E-XceL Now ME initial
 * Fix 2: Anonymous initial_rebill/upsell orders → main_initial (Kytsan, Prime, Optimus)
 * Fix 3: Anonymous rebill orders → mark for exclusion (Prime, ATB)
 * Fix 4: Crown product 70 — reclassify all orders (PGA was changed from straight_sale to initial)
 * Fix 5: ATB customer orders where PGA=initial but classified as rebill
 * Fix 6: Set derived_cycle=0, derived_attempt=1 on all anonymous orders that have NULL
 *
 * After running: re-run post-sync on Crown (4) and ATB (5) for customer reclassification,
 * then re-backfill transaction_attempts for all clients.
 */
const path = require('path');
const { initDb, querySql, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  console.log('='.repeat(70));
  console.log('CLASSIFICATION FIX SCRIPT');
  console.log('='.repeat(70));

  // ---------------------------------------------------------------
  // FIX 1: Kytsan product 266 — E-XceL Now ME Init
  // ---------------------------------------------------------------
  console.log('\n--- Fix 1: Add Kytsan product 266 to catalog + PGA ---');

  // Find the E-XceL Now ME product group
  const excelGroup = db.prepare(
    `SELECT id, group_name FROM product_groups WHERE client_id = 1 AND group_name LIKE '%E-XceL Now ME%' AND group_name NOT LIKE '%Gummies%' AND group_name NOT LIKE '%Testo%'`
  ).get();

  if (excelGroup) {
    console.log(`  Found group: ${excelGroup.group_name} (id=${excelGroup.id})`);

    // Add to products_catalog if not exists
    const existing = db.prepare('SELECT product_id FROM products_catalog WHERE client_id = 1 AND product_id = ?').get('266');
    if (!existing) {
      db.prepare('INSERT INTO products_catalog (client_id, product_id, product_name) VALUES (?, ?, ?)').run(1, '266', 'E-XceL Now ME Init');
      console.log('  Added to products_catalog');
    }

    // Add to PGA if not exists
    const existingPga = db.prepare('SELECT id FROM product_group_assignments WHERE client_id = 1 AND product_id = ?').get('266');
    if (!existingPga) {
      db.prepare('INSERT INTO product_group_assignments (client_id, product_group_id, product_id, product_type) VALUES (?, ?, ?, ?)').run(1, excelGroup.id, '266', 'initial');
      console.log('  Added to PGA as initial');
    }

    // Now classify the 4,866 orders with product 266
    const fix1 = db.prepare(`
      UPDATE orders SET
        product_type_classified = 'initial',
        derived_product_role = 'main_initial',
        product_group_id = ?,
        product_group_name = ?,
        tx_type = CASE WHEN customer_id IS NULL OR customer_id = 0 THEN 'anonymous_decline' ELSE 'cp_initial' END,
        derived_cycle = CASE WHEN customer_id IS NULL OR customer_id = 0 THEN 0 ELSE derived_cycle END,
        derived_attempt = CASE WHEN customer_id IS NULL OR customer_id = 0 THEN 1 ELSE derived_attempt END
      WHERE client_id = 1
        AND product_ids LIKE '%"266"%'
        AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
        AND (product_type_classified IS NULL OR product_group_id IS NULL)
    `).run(excelGroup.id, excelGroup.group_name);
    console.log(`  Classified ${fix1.changes} orders`);
  } else {
    console.log('  ERROR: E-XceL Now ME group not found!');
  }

  // ---------------------------------------------------------------
  // FIX 2: Anonymous initial_rebill → main_initial (all clients)
  // ---------------------------------------------------------------
  console.log('\n--- Fix 2: Anonymous initial_rebill/upsell_initial → main_initial ---');

  // Anonymous orders with initial_rebill classified as straight_sale
  const fix2a = db.prepare(`
    UPDATE orders SET
      product_type_classified = 'initial',
      derived_product_role = 'main_initial',
      derived_cycle = 0,
      derived_attempt = 1
    WHERE (customer_id IS NULL OR customer_id = 0)
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
      AND product_type_classified = 'initial_rebill'
  `).run();
  console.log(`  initial_rebill → initial: ${fix2a.changes} orders`);

  // Anonymous upsell_initial → main_initial
  const fix2b = db.prepare(`
    UPDATE orders SET
      derived_product_role = 'main_initial',
      derived_cycle = 0,
      derived_attempt = 1
    WHERE (customer_id IS NULL OR customer_id = 0)
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
      AND product_type_classified = 'initial'
      AND derived_product_role = 'upsell_initial'
  `).run();
  console.log(`  upsell_initial → main_initial: ${fix2b.changes} orders`);

  // ---------------------------------------------------------------
  // FIX 3: Anonymous rebill orders → mark for exclusion
  // ---------------------------------------------------------------
  console.log('\n--- Fix 3: Anonymous rebill → anonymous_decline (exclude from training) ---');

  // These have product_type = rebill but no customer — can't be real rebills
  // Keep product_type_classified as 'rebill' for data accuracy but set tx_type properly
  // The exploder will skip these (NULL customer + rebill = excluded)
  const fix3count = db.prepare(`
    SELECT COUNT(*) as cnt FROM orders
    WHERE (customer_id IS NULL OR customer_id = 0)
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
      AND product_type_classified = 'rebill'
  `).get().cnt;
  console.log(`  Anonymous rebill orders to exclude from training: ${fix3count}`);
  console.log(`  (No DB change needed — exploder will handle exclusion)`);

  // ---------------------------------------------------------------
  // FIX 4: Crown product 70 — reclassify ALL orders
  // ---------------------------------------------------------------
  console.log('\n--- Fix 4: Crown product 70 reclassification ---');

  // 4a: Anonymous orders with product 70 → main_initial
  const fix4a = db.prepare(`
    UPDATE orders SET
      product_type_classified = 'initial',
      derived_product_role = 'main_initial',
      derived_cycle = 0,
      derived_attempt = 1
    WHERE client_id = 4
      AND (customer_id IS NULL OR customer_id = 0)
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
      AND product_ids LIKE '%"70"%'
      AND product_type_classified = 'straight_sale'
  `).run();
  console.log(`  Crown anonymous product 70 → main_initial: ${fix4a.changes} orders`);

  // 4b: Customer orders with product 70 — NULL out classification so post-sync re-runs
  const fix4b = db.prepare(`
    UPDATE orders SET
      product_type_classified = NULL,
      derived_product_role = NULL,
      derived_cycle = NULL,
      derived_attempt = NULL,
      tx_type = NULL
    WHERE client_id = 4
      AND customer_id IS NOT NULL AND customer_id != 0
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
      AND product_ids LIKE '%"70"%'
      AND product_type_classified = 'straight_sale'
  `).run();
  console.log(`  Crown customer product 70 → NULL for re-classification: ${fix4b.changes} orders`);

  // ---------------------------------------------------------------
  // FIX 5: ATB customer orders PGA=initial but classified as rebill
  // ---------------------------------------------------------------
  console.log('\n--- Fix 5: ATB customer orders with wrong rebill classification ---');

  // NULL out for re-classification by post-sync
  const fix5 = db.prepare(`
    UPDATE orders SET
      product_type_classified = NULL,
      derived_product_role = NULL,
      derived_cycle = NULL,
      derived_attempt = NULL,
      tx_type = NULL
    WHERE client_id = 5
      AND customer_id IS NOT NULL AND customer_id != 0
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
      AND product_type_classified = 'rebill'
      AND product_ids IN (
        SELECT DISTINCT o.product_ids FROM orders o
        JOIN product_group_assignments pga ON pga.client_id = o.client_id
          AND pga.product_id = REPLACE(REPLACE(SUBSTR(o.product_ids, 3, LENGTH(o.product_ids)-4), '"', ''), ' ', '')
        WHERE o.client_id = 5 AND pga.product_type = 'initial'
          AND o.product_type_classified = 'rebill'
          AND o.customer_id IS NOT NULL AND o.customer_id != 0
      )
  `).run();
  console.log(`  ATB customer orders NULL'd for re-classification: ${fix5.changes} orders`);

  // ---------------------------------------------------------------
  // FIX 6: Set derived_cycle/attempt on remaining anonymous orders
  // ---------------------------------------------------------------
  console.log('\n--- Fix 6: Set cycle=0, attempt=1 on anonymous orders with NULL ---');

  const fix6 = db.prepare(`
    UPDATE orders SET
      derived_cycle = 0,
      derived_attempt = 1
    WHERE (customer_id IS NULL OR customer_id = 0)
      AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
      AND derived_product_role IN ('main_initial', 'upsell_initial', 'main_rebill', 'upsell_rebill')
      AND (derived_cycle IS NULL OR derived_attempt IS NULL)
  `).run();
  console.log(`  Set cycle=0, attempt=1: ${fix6.changes} orders`);

  // ---------------------------------------------------------------
  // SUMMARY
  // ---------------------------------------------------------------
  console.log('\n' + '='.repeat(70));
  console.log('FIXES APPLIED');
  console.log('='.repeat(70));
  console.log(`  Fix 1: Product 266 added + ${fix1 ? fix1.changes : 0} orders classified`);
  console.log(`  Fix 2: ${fix2a.changes + fix2b.changes} anonymous initial_rebill/upsell → main_initial`);
  console.log(`  Fix 3: ${fix3count} anonymous rebills (excluded by exploder)`);
  console.log(`  Fix 4: Crown — ${fix4a.changes} anonymous fixed, ${fix4b.changes} customer orders ready for re-classification`);
  console.log(`  Fix 5: ATB — ${fix5.changes} customer orders ready for re-classification`);
  console.log(`  Fix 6: ${fix6.changes} anonymous orders got cycle=0/attempt=1`);

  console.log('\n--- NEXT STEPS ---');
  console.log('  1. Run post-sync on Crown (client 4) to reclassify the NULL\'d customer orders');
  console.log('  2. Run post-sync on ATB (client 5) to reclassify the NULL\'d customer orders');
  console.log('  3. Re-run audit to verify all clean');
  console.log('  4. Re-backfill transaction_attempts for all clients (--reset)');
}

main().catch(err => { console.error(err); process.exit(1); });
