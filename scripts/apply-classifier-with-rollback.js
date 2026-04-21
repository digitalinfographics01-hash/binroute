/**
 * Apply the new classifier output to derived_product_role for clients 1-5,
 * with full rollback safety:
 *
 *   1. Create a backup table containing (order_pk, client_id, old_role, new_role)
 *      for every order whose role changed.
 *   2. In a SINGLE transaction:
 *        - INSERT all backup rows
 *        - UPDATE orders.derived_product_role = new_role for each changed order
 *   3. If anything fails, the transaction rolls back automatically.
 *   4. Backup table is named with a timestamp so it can be queried/used to
 *      restore later if needed.
 *
 * To rollback later:
 *   UPDATE orders SET derived_product_role = (
 *     SELECT old_role FROM orders_role_backup_<ts> WHERE order_pk = orders.id
 *   ) WHERE id IN (SELECT order_pk FROM orders_role_backup_<ts>);
 */
const { initDb, querySql, runSql, transaction, saveDb } = require('../src/db/connection');

const REPROCESSING_WINDOW_DAYS = 90;
const CLIENT_IDS = [1, 2, 3, 4, 5];

const UNRECOVERABLE_REASONS = new Set([
  'cvv2 mismatch', 'invalid credit card number', 'invalid card number',
  '14  -  invalid card number', '14 - invalid card number', 'invalid cc',
  'c2  -  cvv mismatch/ missing', 'c2 - cvv mismatch/ missing',
  'expired card', 'account closed',
]);
function isUnrecoverable(r) { return r && UNRECOVERABLE_REASONS.has(r.toLowerCase().trim()); }
function getPrimaryProductId(s) { try { const a = JSON.parse(s||'[]'); return a.length>0?String(a[0]):null; } catch { return null; } }
function daysBetween(d1, d2) { const t1=new Date(d1).getTime(), t2=new Date(d2).getTime(); if (isNaN(t1)||isNaN(t2)) return Infinity; return Math.abs(t2-t1)/86400000; }
const APPROVED_STATUSES = new Set([2, 6, 8, '2', '6', '8']);
function isApproved(s) { return APPROVED_STATUSES.has(s); }
function isDeclined(s) { return s === 7 || s === '7'; }

(async () => {
  await initDb();

  const ts = Date.now();
  const backupTable = 'orders_role_backup_' + ts;

  // Load product map (all 5 clients in one shot)
  console.log('Loading product map...');
  const productMap = new Map();
  querySql(`
    SELECT pga.client_id, pga.product_id, pga.product_type, pg.product_sequence, pg.group_name
    FROM product_group_assignments pga
    LEFT JOIN product_groups pg ON pg.id = pga.product_group_id
    WHERE pga.client_id IN (1,2,3,4,5)
  `).forEach(a => {
    productMap.set(a.client_id + ':' + a.product_id, {
      type: a.product_type, sequence: a.product_sequence, group_name: a.group_name,
    });
  });
  console.log('  ' + productMap.size + ' product assignments');
  console.log();

  // Per-client classification
  const allChanges = []; // { order_pk, client_id, old_role, new_role }
  const perClientStats = {};

  for (const cid of CLIENT_IDS) {
    const tStart = Date.now();
    console.log('Classifying client ' + cid + '...');
    const orders = querySql(`
      SELECT id, client_id, customer_id, contact_id, email_address, product_ids,
             billing_cycle, is_cascaded, order_status, acquisition_date, campaign_id,
             decline_reason, decline_category, derived_cycle, derived_attempt,
             COALESCE(derived_product_role, '(null)') as current_role
      FROM orders
      WHERE client_id = ? AND COALESCE(is_internal_test, 0) = 0
      ORDER BY acquisition_date ASC, id ASC
    `, [cid]);

    const declineByCustomer = new Map();
    const declineByContact = new Map();
    const declineByEmail = new Map();

    function recordDecline(o, pid, info) {
      if (o.decline_category === 'crm_routing_rule') return;
      if (isUnrecoverable(o.decline_reason)) return;
      const cycle = parseInt(o.billing_cycle || 0, 10);
      const isInitial = info.type === 'initial' || (info.type === 'initial_rebill' && cycle === 0);
      if (!isInitial) return;
      const record = { date: o.acquisition_date };
      if (o.customer_id != null && String(o.customer_id) !== '0' && String(o.customer_id) !== '') {
        const k = String(o.customer_id);
        if (!declineByCustomer.has(k)) declineByCustomer.set(k, new Map());
        declineByCustomer.get(k).set(pid, record);
      }
      if (o.contact_id != null && String(o.contact_id) !== '0' && String(o.contact_id) !== '') {
        const k = String(o.contact_id);
        if (!declineByContact.has(k)) declineByContact.set(k, new Map());
        declineByContact.get(k).set(pid, record);
      }
      if (o.email_address && o.email_address.trim() !== '') {
        const k = o.email_address.trim().toLowerCase();
        if (!declineByEmail.has(k)) declineByEmail.set(k, new Map());
        declineByEmail.get(k).set(pid, record);
      }
    }
    function lookupPriorDecline(o, pid) {
      if (o.customer_id != null && String(o.customer_id) !== '0' && String(o.customer_id) !== '') {
        const m = declineByCustomer.get(String(o.customer_id));
        if (m) { const r = m.get(pid); if (r) return r; }
      }
      if (o.contact_id != null && String(o.contact_id) !== '0' && String(o.contact_id) !== '') {
        const m = declineByContact.get(String(o.contact_id));
        if (m) { const r = m.get(pid); if (r) return r; }
      }
      if (o.email_address && o.email_address.trim() !== '') {
        const m = declineByEmail.get(o.email_address.trim().toLowerCase());
        if (m) { const r = m.get(pid); if (r) return r; }
      }
      return null;
    }

    function classify(o) {
      const pid = getPrimaryProductId(o.product_ids);
      if (!pid) return 'unclassified_no_product';
      const info = productMap.get(o.client_id + ':' + pid);
      if (!info) return 'unclassified_unknown_product';
      if (!info.type) return 'unclassified_null_type';
      if (!info.sequence) return 'unclassified_null_sequence';
      const seq = info.sequence;
      if (seq === 'excluded') return 'excluded';
      if (seq === 'recovery') return 'rebill_recovery';
      if (info.type === 'straight_sale') return 'upsell_ots';

      if (o.derived_cycle !== null && o.derived_cycle !== undefined &&
          o.derived_attempt !== null && o.derived_attempt !== undefined) {
        const dCycle = parseInt(o.derived_cycle, 10);
        const dAttempt = parseInt(o.derived_attempt, 10);
        if (dCycle === 0) {
          if (dAttempt === 1) return seq + '_initial';
          return seq === 'main' ? 'initial_reprocessing' : 'upsell_reprocessing';
        } else {
          if (dAttempt === 1) return seq + '_rebill';
          return seq === 'main' ? 'main_rebill_salvage' : 'upsell_rebill_salvage';
        }
      }

      const cycle = parseInt(o.billing_cycle || 0, 10);
      const type = info.type;
      let phase;
      if (type === 'initial' || (type === 'initial_rebill' && cycle === 0)) phase = 'initial';
      else if (type === 'rebill' || (type === 'initial_rebill' && cycle >= 1)) phase = 'rebill';
      else return 'unclassified_unknown_type';
      const baseRole = seq + '_' + phase;
      if (phase === 'initial' && !(o.is_cascaded === 1 || o.is_cascaded === '1')) {
        const prior = lookupPriorDecline(o, pid);
        if (prior) {
          const dd = daysBetween(o.acquisition_date, prior.date);
          if (dd <= REPROCESSING_WINDOW_DAYS && new Date(prior.date) < new Date(o.acquisition_date)) {
            return seq === 'main' ? 'initial_reprocessing' : 'upsell_reprocessing';
          }
        }
      }
      return baseRole;
    }

    let processed = 0;
    let changed = 0;
    const counts = {};
    for (const o of orders) {
      const newRole = classify(o);
      counts[newRole] = (counts[newRole] || 0) + 1;
      processed++;
      if (newRole !== o.current_role) {
        changed++;
        allChanges.push({
          order_pk: o.id,
          client_id: o.client_id,
          old_role: o.current_role === '(null)' ? null : o.current_role,
          new_role: newRole,
        });
      }
      // Update history AFTER classifying
      const pid = getPrimaryProductId(o.product_ids);
      if (pid && isDeclined(o.order_status)) {
        const info = productMap.get(o.client_id + ':' + pid);
        if (info) recordDecline(o, pid, info);
      }
    }

    perClientStats[cid] = { total: orders.length, processed, changed, counts };
    console.log('  ' + orders.length + ' orders | ' + changed + ' to change | took ' + (Date.now() - tStart) + 'ms');
  }

  console.log();
  console.log('='.repeat(72));
  console.log('PRE-APPLY SUMMARY');
  console.log('='.repeat(72));
  console.log('Total changes to apply: ' + allChanges.length);
  for (const cid of CLIENT_IDS) {
    const s = perClientStats[cid];
    console.log('  Client ' + cid + ': ' + s.changed + ' / ' + s.total + ' (' + (s.changed/s.total*100).toFixed(1) + '%)');
  }
  console.log();

  // Create backup table and apply changes in a single transaction
  console.log('Creating backup table: ' + backupTable);
  runSql(`
    CREATE TABLE ${backupTable} (
      order_pk INTEGER PRIMARY KEY,
      client_id INTEGER NOT NULL,
      old_role TEXT,
      new_role TEXT NOT NULL,
      backed_up_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
  saveDb();
  console.log();

  console.log('Applying changes in a single transaction...');
  const tApply = Date.now();
  let backupCount = 0;
  let updateCount = 0;
  try {
    transaction(() => {
      for (const c of allChanges) {
        runSql(
          `INSERT INTO ${backupTable} (order_pk, client_id, old_role, new_role) VALUES (?, ?, ?, ?)`,
          [c.order_pk, c.client_id, c.old_role, c.new_role]
        );
        backupCount++;
        runSql(
          'UPDATE orders SET derived_product_role = ? WHERE id = ?',
          [c.new_role, c.order_pk]
        );
        updateCount++;
      }
    });
    saveDb();
  } catch (err) {
    console.error('TRANSACTION FAILED — rolling back:', err.message);
    runSql('DROP TABLE IF EXISTS ' + backupTable);
    saveDb();
    process.exit(1);
  }
  console.log('  Backed up: ' + backupCount + ' rows');
  console.log('  Updated:   ' + updateCount + ' orders');
  console.log('  Time:      ' + (Date.now() - tApply) + 'ms');
  console.log();

  // Verify by spot-checking a few rows
  console.log('Spot-check (5 random changes):');
  const samples = querySql(`SELECT * FROM ${backupTable} ORDER BY RANDOM() LIMIT 5`);
  for (const s of samples) {
    const current = querySql('SELECT derived_product_role FROM orders WHERE id = ?', [s.order_pk])[0];
    const ok = current.derived_product_role === s.new_role ? '✓' : '✗ MISMATCH';
    console.log('  ' + s.order_pk + ': ' + s.old_role + ' → ' + s.new_role + ' (current: ' + current.derived_product_role + ') ' + ok);
  }
  console.log();

  console.log('='.repeat(72));
  console.log('APPLY COMPLETE');
  console.log('='.repeat(72));
  console.log('Backup table: ' + backupTable + ' (' + backupCount + ' rows)');
  console.log();
  console.log('To rollback:');
  console.log('  UPDATE orders SET derived_product_role = (');
  console.log('    SELECT old_role FROM ' + backupTable + ' WHERE order_pk = orders.id');
  console.log('  ) WHERE id IN (SELECT order_pk FROM ' + backupTable + ');');
  console.log();

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
