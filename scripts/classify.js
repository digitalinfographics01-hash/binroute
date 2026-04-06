/**
 * Full transaction classification pass.
 * Only ALTER TABLE + UPDATE — no reimport, no data deletion.
 */
const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = path.join(__dirname, '..', 'data', 'binroute.db');

(() => {
  const db = new Database(DB_PATH);
  db.pragma('busy_timeout = 10000');

  // Verify order count before
  const before = db.prepare('SELECT COUNT(*) as cnt FROM orders').get();
  console.log('Orders before:', before.cnt);

  // Step 0: Add columns via ALTER TABLE
  const cols = [
    ['derived_cycle', 'INTEGER'],
    ['product_group_id', 'INTEGER'],
    ['product_group_name', 'TEXT'],
    ['product_type_classified', 'TEXT'],
    ['upsell_parent_order_id', 'TEXT'],
    ['upsell_position', 'INTEGER'],
  ];
  for (const [name, type] of cols) {
    try { db.exec(`ALTER TABLE orders ADD COLUMN ${name} ${type}`); console.log('Added:', name); }
    catch { console.log('Exists:', name); }
  }

  // Build product lookup: product_id -> { product_type, product_group_id, group_name }
  const pgaRows = db.prepare(`
    SELECT pga.product_id, pga.product_type, pga.product_group_id, pg.group_name
    FROM product_group_assignments pga
    JOIN product_groups pg ON pga.product_group_id = pg.id
    WHERE pga.client_id = 1
  `).all();
  const productMap = {};
  for (const r of pgaRows) {
    productMap[String(r.product_id)] = {
      product_type: r.product_type,
      product_group_id: r.product_group_id,
      group_name: r.group_name,
    };
  }
  console.log('Product map entries:', Object.keys(productMap).length);

  // Load ALL orders
  const orders = db.prepare(`
    SELECT id, order_id, customer_id, is_test, billing_cycle, is_recurring,
           retry_attempt, is_cascaded, product_ids, order_status, acquisition_date
    FROM orders WHERE client_id = 1
    ORDER BY order_id
  `).all();
  console.log('Orders loaded:', orders.length);

  // Enrich each order with product info
  for (const o of orders) {
    let pid = null;
    if (o.product_ids) {
      try {
        const ids = JSON.parse(o.product_ids);
        if (Array.isArray(ids) && ids.length > 0) pid = String(ids[0]);
      } catch {}
    }
    o._pid = pid;
    o._pi = pid ? (productMap[pid] || null) : null;
    o._date = o.acquisition_date ? o.acquisition_date.split(' ')[0] : null;
    o._billingCycle = parseInt(o.billing_cycle) || 0;
    o._isRecurring = o.is_recurring === 1 || o.is_recurring === '1';
    o._retryAttempt = parseInt(o.retry_attempt) || 0;
    o._status = parseInt(o.order_status) || 0;
    o._isApproved = [2, 6, 8].includes(o._status);
    o._isDeclined = o._status === 7;
  }

  // Build customer index: customer_id -> orders sorted by order_id
  const custIdx = {};
  for (const o of orders) {
    if (o.customer_id) {
      if (!custIdx[o.customer_id]) custIdx[o.customer_id] = [];
      custIdx[o.customer_id].push(o);
    }
  }
  console.log('Customer index built:', Object.keys(custIdx).length, 'customers');

  // Classify
  console.log('Classifying...');

  db.exec('BEGIN TRANSACTION');

  let count = 0;
  for (const o of orders) {
    let tx_type = null;
    let derived_cycle = null;
    let product_group_id = o._pi ? o._pi.product_group_id : null;
    let product_group_name = o._pi ? o._pi.group_name : null;
    let product_type_classified = o._pi ? o._pi.product_type : null;
    let upsell_parent_order_id = null;
    let upsell_position = null;

    const pi = o._pi;

    // STEP 1: Anonymous decline
    if (o.customer_id === null || o.customer_id === 0) {
      tx_type = 'anonymous_decline';
    }
    // STEP 2: Test order
    else if (o.is_test === 1) {
      tx_type = 'test_order';
    }
    // STEP 3: No product group
    else if (!pi) {
      tx_type = 'unclassified';
    }
    // STEP 4: Straight sale
    else if (pi.product_type === 'straight_sale') {
      tx_type = 'straight_sale';
    }
    // STEP 5: Sticky COF rebill (cycle > 0, recurring)
    else if (o._billingCycle > 0 && o._isRecurring) {
      tx_type = 'sticky_cof_rebill';
      derived_cycle = o._billingCycle;
    }
    // STEP 6: Third party rebill (cycle > 0, not recurring)
    else if (o._billingCycle > 0 && !o._isRecurring) {
      tx_type = 'tp_rebill';
      derived_cycle = o._billingCycle;
    }
    // STEPS 7-12: billing_cycle = 0
    else {
      let matched = false;

      // STEP 7: Upsell detection
      if (o.customer_id && o._date && pi.product_type !== 'rebill') {
        const custOrders = custIdx[o.customer_id] || [];
        const sameDay = custOrders.filter(co =>
          co._date === o._date &&
          co._billingCycle === 0 &&
          co.is_test !== 1
        ).sort((a, b) => a.order_id - b.order_id);

        if (sameDay.length > 1) {
          const anchor = sameDay[0];
          if (o.order_id !== anchor.order_id &&
              o._pid !== anchor._pid) {
            tx_type = 'upsell';
            upsell_parent_order_id = String(anchor.order_id);
            // Position among upsells (non-anchor, different product)
            const upsells = sameDay.filter(co =>
              co.order_id !== anchor.order_id &&
              co._pid !== anchor._pid
            );
            upsell_position = upsells.findIndex(u => u.order_id === o.order_id) + 1;
            matched = true;
          }
        }
      }

      if (!matched) {
        // STEP 8: Sticky COF rebill (cycle 0, recurring, initial_rebill/rebill product)
        if (o._isRecurring && (pi.product_type === 'initial_rebill' || pi.product_type === 'rebill')) {
          const custOrders = custIdx[o.customer_id] || [];
          const priorApproved = custOrders.some(co =>
            co.order_id < o.order_id &&
            co._pi && co._pi.product_group_id === pi.product_group_id &&
            co._isApproved
          );
          tx_type = priorApproved ? 'sticky_cof_rebill' : 'cp_initial';
          matched = true;
        }
        // STEP 9: Third party rebill (cycle 0, not recurring, rebill product)
        else if (!o._isRecurring && pi.product_type === 'rebill') {
          tx_type = 'tp_rebill';
          matched = true;
        }
        // STEP 10: Initial salvage
        else if (o._retryAttempt === 0 && !o._isRecurring &&
                 (pi.product_type === 'initial' || pi.product_type === 'initial_rebill')) {
          const custOrders = custIdx[o.customer_id] || [];
          const priorDeclined = custOrders.some(co =>
            co.order_id < o.order_id &&
            co._pi && co._pi.product_group_id === pi.product_group_id &&
            co._isDeclined
          );
          tx_type = priorDeclined ? 'initial_salvage' : 'cp_initial';
          matched = true;
        }
        // STEP 11: Retry
        else if (o._retryAttempt > 0) {
          tx_type = o._billingCycle === 0 ? 'initial_retry' : 'rebill_retry';
          matched = true;
        }
      }

      // STEP 12: Default
      if (!tx_type) {
        tx_type = 'cp_initial';
      }
    }

    // Update
    db.prepare(`UPDATE orders SET
      tx_type = ?, derived_cycle = ?, product_group_id = ?,
      product_group_name = ?, product_type_classified = ?,
      upsell_parent_order_id = ?, upsell_position = ?
      WHERE id = ?`).run(tx_type, derived_cycle, product_group_id, product_group_name,
       product_type_classified, upsell_parent_order_id, upsell_position, o.id);

    count++;
    if (count % 5000 === 0) console.log('  classified:', count);
  }

  db.exec('COMMIT');
  console.log('Classification complete:', count, 'orders');

  // Verify order count after
  const after = db.prepare('SELECT COUNT(*) as cnt FROM orders').get();
  console.log('Orders after:', after.cnt);

  // === REPORTING ===

  // 1. Count per tx_type
  console.log('\n=== 1. COUNT PER TX_TYPE ===');
  const types = db.prepare('SELECT tx_type, COUNT(*) as count FROM orders WHERE is_test = 0 GROUP BY tx_type ORDER BY count DESC').all();
  console.table(types);

  // 2. Validation
  console.log('\n=== 2. VALIDATION ===');
  const total = db.prepare('SELECT COUNT(*) as total FROM orders WHERE is_test = 0').get().total;
  const sum = types.reduce((s, t) => s + t.count, 0);
  console.log('Total non-test orders:', total);
  console.log('Sum of tx_type counts:', sum);
  console.log('Match:', total === sum ? 'YES' : 'NO — MISMATCH!');

  // 3. Sample per tx_type
  console.log('\n=== 3. SAMPLES PER TX_TYPE ===');
  for (const t of types) {
    const samples = db.prepare(`SELECT order_id, tx_type, product_type_classified, product_group_name,
      billing_cycle, is_recurring, customer_id, order_status
      FROM orders WHERE is_test = 0 AND tx_type = ? LIMIT 3`).all(t.tx_type);
    console.log('\n--- ' + t.tx_type + ' (' + t.count + ') ---');
    console.table(samples);
  }

  // 4. Upsell positions
  console.log('\n=== 4. UPSELL POSITIONS ===');
  const ups = db.prepare("SELECT upsell_position, COUNT(*) as count FROM orders WHERE tx_type = 'upsell' GROUP BY upsell_position ORDER BY upsell_position").all();
  if (ups.length > 0) console.table(ups);
  else console.log('No upsells detected');

  db.pragma('wal_checkpoint(TRUNCATE)');
  console.log('\nWAL checkpoint done.');
  db.close();
  console.log('Done.');
})();
