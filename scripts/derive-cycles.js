/**
 * Calculate derived_cycle for all clean orders.
 * Only UPDATEs derived_cycle — no reimport, no deletes.
 */
const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = path.join(__dirname, '..', 'data', 'binroute.db');

(() => {
  const db = new Database(DB_PATH);
  db.pragma('busy_timeout = 10000');

  // Verify order count
  const before = db.prepare('SELECT COUNT(*) as cnt FROM orders').get();
  console.log('Orders before:', before.cnt);

  // Load all clean orders sorted by order_id (chronological)
  const orders = db.prepare(`
    SELECT id, order_id, customer_id, product_group_id, product_type_classified,
           order_status, acquisition_date
    FROM orders
    WHERE is_test = 0 AND is_internal_test = 0
    ORDER BY order_id ASC
  `).all();
  console.log('Clean orders loaded:', orders.length);

  // Track approved counts per customer+product_group
  // Key: "customer_id:product_group_id" → count of approved orders seen so far
  const approvedCounts = {};

  function getKey(customerId, groupId) {
    return `${customerId}:${groupId}`;
  }

  // Calculate derived_cycle for each order
  console.log('Calculating derived_cycles...');
  db.exec('BEGIN TRANSACTION');

  let updated = 0;
  for (const o of orders) {
    let derivedCycle = null;

    if (o.customer_id && o.product_group_id) {
      const key = getKey(o.customer_id, o.product_group_id);
      const priorApproved = approvedCounts[key] || 0;

      if (o.product_type_classified === 'initial' || o.product_type_classified === 'initial_rebill') {
        derivedCycle = priorApproved; // 0 for first order
      } else if (o.product_type_classified === 'rebill') {
        derivedCycle = priorApproved + 1; // 1 for first rebill
      } else if (o.product_type_classified === 'straight_sale') {
        derivedCycle = 0; // straight sales are always cycle 0
      }

      // After processing, if this order was approved, increment the counter
      const isApproved = [2, 6, 8].includes(parseInt(o.order_status));
      if (isApproved) {
        approvedCounts[key] = priorApproved + 1;
      }
    } else if (!o.customer_id) {
      // Anonymous decline — no cycle
      derivedCycle = null;
    } else {
      // No product group — unclassified
      derivedCycle = null;
    }

    db.prepare('UPDATE orders SET derived_cycle = ? WHERE id = ?').run(derivedCycle, o.id);
    updated++;
    if (updated % 5000 === 0) console.log('  updated:', updated);
  }

  db.exec('COMMIT');
  console.log('Updated:', updated, 'orders');

  // Verify order count
  const after = db.prepare('SELECT COUNT(*) as cnt FROM orders').get();
  console.log('Orders after:', after.cnt);

  // === REPORTING ===

  // 1. derived_cycle distribution for tp_rebill
  console.log('\n=== 1. TP_REBILL DERIVED_CYCLE DISTRIBUTION ===');
  const cycles = db.prepare(`
    SELECT derived_cycle, COUNT(*) as count
    FROM orders WHERE tx_type = 'tp_rebill' AND is_test = 0 AND is_internal_test = 0
    GROUP BY derived_cycle ORDER BY derived_cycle ASC LIMIT 20
  `).all();
  console.table(cycles);

  // 2. Customer 74362 history
  console.log('\n=== 2. CUSTOMER 74362 FULL HISTORY ===');
  const hist = db.prepare(`
    SELECT order_id, acquisition_date, tx_type, product_group_name,
      product_type_classified, derived_cycle, order_status, gateway_id, is_cascaded
    FROM orders WHERE customer_id = 74362 AND is_test = 0
    ORDER BY acquisition_date, order_id
  `).all();
  console.table(hist);

  // 3. NULL check
  console.log('\n=== 3. NULL DERIVED_CYCLE CHECK ===');
  const r3 = db.prepare(`
    SELECT COUNT(*) as null_cycles FROM orders
    WHERE tx_type = 'tp_rebill' AND derived_cycle IS NULL
    AND is_test = 0 AND is_internal_test = 0
  `).get();
  console.log('tp_rebill with NULL derived_cycle:', r3.null_cycles);

  const nulls = db.prepare(`
    SELECT tx_type, COUNT(*) as null_count FROM orders
    WHERE derived_cycle IS NULL AND is_test = 0 AND is_internal_test = 0
    GROUP BY tx_type ORDER BY null_count DESC
  `).all();
  console.log('NULL derived_cycle by tx_type:');
  console.table(nulls);

  db.pragma('wal_checkpoint(TRUNCATE)');
  console.log('\nWAL checkpoint done.');
  db.close();
  console.log('Done.');
})();
