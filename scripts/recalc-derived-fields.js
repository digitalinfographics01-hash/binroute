/**
 * Recalculate derived_cycle, derived_attempt, processing_gateway_id
 * for ALL orders in a single pass per customer+product_group.
 *
 * CAN run while the server is running (better-sqlite3 uses file-based access).
 *
 * derived_cycle:
 *   - initial/initial_rebill products → cycle 0
 *   - rebill products → starts at 1, increments after each approved rebill
 *   - declined rebills stay at the same cycle
 *   - straight_sale → cycle 0
 *   - anonymous/unclassified → NULL
 *
 * derived_attempt:
 *   - attempt counter within a cycle, resets after approval
 *   - first attempt = 1, each subsequent failed attempt increments
 *
 * processing_gateway_id:
 *   - is_cascaded=1 AND original_gateway_id exists → original_gateway_id
 *   - otherwise → gateway_id
 */
const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = path.join(__dirname, '..', 'data', 'binroute.db');

(() => {
  const db = new Database(DB_PATH);
  db.pragma('busy_timeout = 10000');

  // Verify order count
  const before = db.prepare('SELECT COUNT(*) as cnt FROM orders').get();
  console.log('Orders in DB:', before.cnt);

  // ── Step 0: Ensure columns exist ──────────────────────────────
  const addCol = (name, type) => {
    try { db.exec(`ALTER TABLE orders ADD COLUMN ${name} ${type}`); console.log('Added column:', name); }
    catch { console.log('Column exists:', name); }
  };
  addCol('derived_attempt', 'INTEGER');
  addCol('processing_gateway_id', 'INTEGER');
  // derived_cycle already exists from classify.js

  // ── Step 1: processing_gateway_id (all orders, simple logic) ──
  console.log('\n=== PROCESSING_GATEWAY_ID ===');
  const pgResult = db.prepare(`UPDATE orders SET processing_gateway_id =
    CASE
      WHEN is_cascaded = 1 AND original_gateway_id IS NOT NULL THEN original_gateway_id
      WHEN is_cascaded = 1 AND original_gateway_id IS NULL THEN NULL
      ELSE gateway_id
    END`).run();
  console.log('Updated processing_gateway_id:', pgResult.changes);

  // ── Step 2: Load orders for cycle/attempt calculation ─────────
  console.log('\n=== DERIVED_CYCLE + DERIVED_ATTEMPT ===');
  const orders = db.prepare(`
    SELECT id, order_id, customer_id, product_group_id, product_type_classified,
           order_status, acquisition_date, is_cascaded
    FROM orders
    WHERE is_test = 0 AND is_internal_test = 0
    ORDER BY customer_id, product_group_id, acquisition_date ASC, order_id ASC
  `).all();
  console.log('Clean orders loaded:', orders.length);

  // ── Step 3: Group by customer+product_group, compute in one pass ──
  db.exec('BEGIN TRANSACTION');

  let updated = 0;
  let nullCycle = 0;
  let i = 0;

  while (i < orders.length) {
    const o = orders[i];

    // Skip orders without customer or product group
    if (!o.customer_id || !o.product_group_id) {
      db.prepare('UPDATE orders SET derived_cycle = NULL, derived_attempt = NULL WHERE id = ?').run(o.id);
      nullCycle++;
      i++;
      updated++;
      continue;
    }

    // Collect all orders for this customer+product_group
    const custId = o.customer_id;
    const pgId = o.product_group_id;
    const group = [];
    while (i < orders.length &&
           orders[i].customer_id === custId &&
           orders[i].product_group_id === pgId) {
      group.push(orders[i]);
      i++;
    }

    // Process this customer+product_group journey
    let currentCycle = 0;    // starts at 0 for initial
    let attemptInCycle = 0;  // resets after each approval
    let initialApproved = false;

    for (const row of group) {
      const ptype = row.product_type_classified;
      const isApproved = [2, 6, 8].includes(parseInt(row.order_status));
      let derivedCycle = null;
      let derivedAttempt = null;

      if (ptype === 'straight_sale') {
        derivedCycle = 0;
        derivedAttempt = 1;
      } else if (ptype === 'initial' || ptype === 'initial_rebill') {
        // Initial purchase phase — all at cycle 0
        derivedCycle = 0;
        attemptInCycle++;
        derivedAttempt = attemptInCycle;

        if (isApproved) {
          // Initial approved — next rebill will be cycle 1
          initialApproved = true;
          currentCycle = 1;
          attemptInCycle = 0;
        }
      } else if (ptype === 'rebill') {
        // Rebill — cycle starts at 1
        if (!initialApproved) {
          // Edge case: rebill product before any initial was approved
          // Shouldn't happen normally, but data may be messy
          derivedCycle = currentCycle || 1;
        } else {
          derivedCycle = currentCycle;
        }
        attemptInCycle++;
        derivedAttempt = attemptInCycle;

        if (isApproved) {
          // This rebill cycle is done — advance to next
          currentCycle++;
          attemptInCycle = 0;
        }
      } else {
        // Unknown product type — leave null
        derivedCycle = null;
        derivedAttempt = null;
      }

      db.prepare('UPDATE orders SET derived_cycle = ?, derived_attempt = ? WHERE id = ?')
        .run(derivedCycle, derivedAttempt, row.id);
      updated++;
    }
  }

  db.exec('COMMIT');
  console.log('Updated cycle/attempt:', updated, 'orders');
  console.log('NULL (no customer/group):', nullCycle);

  // ── Step 4: Reporting ──────────────────────────────────────────
  // Verify order count unchanged
  const after = db.prepare('SELECT COUNT(*) as cnt FROM orders').get();
  console.log('\nOrders after:', after.cnt);

  // 1. derived_cycle distribution for rebills
  console.log('\n=== REBILL DERIVED_CYCLE DISTRIBUTION ===');
  const cycles = db.prepare(`
    SELECT derived_cycle, COUNT(*) as count,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved
    FROM orders
    WHERE product_type_classified = 'rebill' AND is_test = 0 AND is_internal_test = 0
    GROUP BY derived_cycle ORDER BY derived_cycle ASC LIMIT 20
  `).all();
  console.table(cycles);

  // 2. derived_attempt distribution
  console.log('\n=== DERIVED_ATTEMPT DISTRIBUTION (all orders) ===');
  const attempts = db.prepare(`
    SELECT derived_attempt, COUNT(*) as count,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      ROUND(100.0 * SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) as rate
    FROM orders
    WHERE is_test = 0 AND is_internal_test = 0 AND derived_attempt IS NOT NULL
    GROUP BY derived_attempt ORDER BY derived_attempt ASC LIMIT 15
  `).all();
  console.table(attempts);

  // 3. Cycle 0 rebill check (should be 0 now!)
  console.log('\n=== REBILLS WITH CYCLE 0 (should be minimal) ===');
  const r3 = db.prepare(`
    SELECT COUNT(*) as count FROM orders
    WHERE product_type_classified = 'rebill' AND derived_cycle = 0
    AND is_test = 0 AND is_internal_test = 0
  `).get();
  console.log('Rebills with cycle 0:', r3.count);

  // 4. NULL checks
  console.log('\n=== NULL CHECKS ===');
  const nulls = db.prepare(`
    SELECT
      SUM(CASE WHEN derived_cycle IS NULL THEN 1 ELSE 0 END) as null_cycle,
      SUM(CASE WHEN derived_attempt IS NULL THEN 1 ELSE 0 END) as null_attempt,
      SUM(CASE WHEN processing_gateway_id IS NULL THEN 1 ELSE 0 END) as null_pgw
    FROM orders WHERE is_test = 0 AND is_internal_test = 0
  `).get();
  console.log('NULL derived_cycle:', nulls.null_cycle);
  console.log('NULL derived_attempt:', nulls.null_attempt);
  console.log('NULL processing_gateway_id:', nulls.null_pgw);

  // 5. processing_gateway_id vs gateway_id — cascade correction count
  console.log('\n=== CASCADE CORRECTION ===');
  const r5 = db.prepare(`
    SELECT COUNT(*) as count FROM orders
    WHERE processing_gateway_id != gateway_id
    AND is_test = 0 AND is_internal_test = 0
  `).get();
  console.log('Orders where processing_gateway_id != gateway_id:', r5.count);

  // 6. Sample customer journey
  console.log('\n=== SAMPLE CUSTOMER JOURNEY ===');
  const r6 = db.prepare(`
    SELECT customer_id FROM orders
    WHERE product_type_classified = 'rebill' AND derived_cycle >= 2
    AND is_test = 0 AND is_internal_test = 0
    LIMIT 1
  `).get();
  if (r6) {
    const sampleCust = r6.customer_id;
    console.log('Customer:', sampleCust);
    const journey = db.prepare(`
      SELECT order_id, acquisition_date, product_type_classified, product_group_name,
             derived_cycle, derived_attempt, order_status, gateway_id, is_cascaded,
             processing_gateway_id
      FROM orders
      WHERE customer_id = ? AND is_test = 0
      ORDER BY acquisition_date, order_id
    `).all(sampleCust);
    console.table(journey);
  } else {
    console.log('No customer with cycle >= 2 found for sample.');
  }

  db.pragma('wal_checkpoint(TRUNCATE)');
  console.log('\nWAL checkpoint done.');
  db.close();
  console.log('Done.');
})();
