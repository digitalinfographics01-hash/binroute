/**
 * Three validation tests for the subscription classifier:
 *  1. Role distribution sanity per client
 *  3. Approval rate per role (in expected range?)
 *  6. Crown/ATB null product_type check
 *
 * Uses the same classifier logic as classify-subscription.js v4.
 */
const { initDb, querySql } = require('../src/db/connection');

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

// Expected approval-rate ranges per role for sanity check
// Split rebills now: main_rebill = first attempt of cycle (high), main_rebill_salvage = retries (low)
const EXPECTED_RANGES = {
  main_initial:        [0.15, 0.60],
  main_rebill:         [0.30, 0.85],   // first-attempt rebills (cycle 1 attempt 1)
  main_rebill_salvage: [0.03, 0.30],   // retries of declined rebills
  upsell_initial:      [0.20, 0.85],
  upsell_rebill:       [0.30, 0.85],   // first-attempt upsell rebills
  upsell_rebill_salvage:[0.03, 0.30],  // retries of declined upsell rebills
  upsell_ots:          [0.30, 0.95],
  initial_reprocessing:[0.03, 0.40],
  upsell_reprocessing: [0.03, 0.50],
  rebill_recovery:     [0.10, 0.85],
  excluded:            [0,    1],
};

(async () => {
  await initDb();

  // ────────────────────────────────────────────────────────────
  // TEST 6: Crown/ATB null product_types
  // ────────────────────────────────────────────────────────────
  console.log('='.repeat(72));
  console.log('TEST 6: Null product_type check (clients 4 and 5)');
  console.log('='.repeat(72));
  for (const cid of [4, 5]) {
    const nulls = querySql(`
      SELECT pga.product_id, pga.product_group_id, pg.group_name, pc.product_name,
             (SELECT COUNT(*) FROM orders o
              WHERE o.client_id = ?
                AND JSON_EXTRACT(o.product_ids, '$[0]') = CAST(pga.product_id AS TEXT)) as order_count
      FROM product_group_assignments pga
      LEFT JOIN product_groups pg ON pg.id = pga.product_group_id
      LEFT JOIN products_catalog pc ON pc.client_id = pga.client_id AND pc.product_id = pga.product_id
      WHERE pga.client_id = ? AND pga.product_type IS NULL
      ORDER BY order_count DESC
    `, [cid, cid]);

    const clientName = querySql('SELECT name FROM clients WHERE id=?', [cid])[0]?.name;
    console.log();
    console.log('Client ' + cid + ' (' + clientName + '): ' + nulls.length + ' products with null product_type');
    if (nulls.length > 0) {
      const totalOrders = nulls.reduce((s, r) => s + (r.order_count || 0), 0);
      console.log('  Affected orders: ' + totalOrders);
      console.log('  Top 10:');
      nulls.slice(0, 10).forEach(r => {
        console.log('    pid=' + String(r.product_id).padStart(5) + '  orders=' + String(r.order_count || 0).padStart(5) + '  group=' + (r.group_name || '?').substring(0, 30) + '  name=' + (r.product_name || '(none)').substring(0, 40));
      });
    }
  }
  console.log();

  // ────────────────────────────────────────────────────────────
  // Load product map (with null product_type included so we can detect)
  // ────────────────────────────────────────────────────────────
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

  // ────────────────────────────────────────────────────────────
  // Classifier (same as classify-subscription.js v4)
  // ────────────────────────────────────────────────────────────
  function classifyClient(cid) {
    const orders = querySql(`
      SELECT id, client_id, order_id, customer_id, contact_id, email_address, product_ids,
             billing_cycle, is_cascaded, order_status, acquisition_date, campaign_id,
             decline_reason, decline_category, derived_cycle, derived_attempt
      FROM orders
      WHERE client_id = ? AND COALESCE(is_internal_test, 0) = 0
      ORDER BY acquisition_date ASC, id ASC
    `, [cid]);

    const declineByCustomer = new Map();
    const declineByContact = new Map();
    const declineByEmail = new Map();
    const lastRebillByCustomer = new Map();
    const lastRebillByContact = new Map();
    const lastRebillByEmail = new Map();

    function recordRebillAttempt(o, pid, info) {
      if (o.decline_category === 'crm_routing_rule') return;
      if (isDeclined(o.order_status) && isUnrecoverable(o.decline_reason)) return;
      const cycle = parseInt(o.billing_cycle || 0, 10);
      const isRebillPhase = info.type === 'rebill' || (info.type === 'initial_rebill' && cycle >= 1);
      if (!isRebillPhase) return;
      const record = { date: o.acquisition_date, declined: isDeclined(o.order_status), approved: isApproved(o.order_status) };
      if (o.customer_id != null && String(o.customer_id) !== '0' && String(o.customer_id) !== '') {
        const k = String(o.customer_id);
        if (!lastRebillByCustomer.has(k)) lastRebillByCustomer.set(k, new Map());
        lastRebillByCustomer.get(k).set(pid, record);
      }
      if (o.contact_id != null && String(o.contact_id) !== '0' && String(o.contact_id) !== '') {
        const k = String(o.contact_id);
        if (!lastRebillByContact.has(k)) lastRebillByContact.set(k, new Map());
        lastRebillByContact.get(k).set(pid, record);
      }
      if (o.email_address && o.email_address.trim() !== '') {
        const k = o.email_address.trim().toLowerCase();
        if (!lastRebillByEmail.has(k)) lastRebillByEmail.set(k, new Map());
        lastRebillByEmail.get(k).set(pid, record);
      }
    }

    function lookupLastRebill(o, pid) {
      if (o.customer_id != null && String(o.customer_id) !== '0' && String(o.customer_id) !== '') {
        const m = lastRebillByCustomer.get(String(o.customer_id));
        if (m) { const r = m.get(pid); if (r) return r; }
      }
      if (o.contact_id != null && String(o.contact_id) !== '0' && String(o.contact_id) !== '') {
        const m = lastRebillByContact.get(String(o.contact_id));
        if (m) { const r = m.get(pid); if (r) return r; }
      }
      if (o.email_address && o.email_address.trim() !== '') {
        const m = lastRebillByEmail.get(o.email_address.trim().toLowerCase());
        if (m) { const r = m.get(pid); if (r) return r; }
      }
      return null;
    }

    function recordDecline(o, pid, info) {
      if (o.decline_category === 'crm_routing_rule') return;
      if (isUnrecoverable(o.decline_reason)) return;
      const cycle = parseInt(o.billing_cycle || 0, 10);
      const isInitial = info.type === 'initial' || (info.type === 'initial_rebill' && cycle === 0);
      if (!isInitial) return;
      const record = { date: o.acquisition_date, order_id: o.order_id, campaign_id: o.campaign_id };
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

      // PRIMARY: derived_cycle/attempt when populated
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

      // FALLBACK: history-based for nulls
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
      if (phase === 'rebill' && !(o.is_cascaded === 1 || o.is_cascaded === '1')) {
        const lastRebill = lookupLastRebill(o, pid);
        if (lastRebill && lastRebill.declined && new Date(lastRebill.date) < new Date(o.acquisition_date)) {
          return seq === 'main' ? 'main_rebill_salvage' : 'upsell_rebill_salvage';
        }
      }
      return baseRole;
    }

    const counts = {};
    const approvedCounts = {};
    const declinedCounts = {};

    for (const o of orders) {
      const role = classify(o);
      counts[role] = (counts[role] || 0) + 1;
      if (isApproved(o.order_status)) approvedCounts[role] = (approvedCounts[role] || 0) + 1;
      if (isDeclined(o.order_status)) declinedCounts[role] = (declinedCounts[role] || 0) + 1;

      // Update history AFTER classifying current order
      const pidH = getPrimaryProductId(o.product_ids);
      if (pidH) {
        const infoH = productMap.get(o.client_id + ':' + pidH);
        if (infoH) {
          if (isDeclined(o.order_status)) recordDecline(o, pidH, infoH);
          recordRebillAttempt(o, pidH, infoH);
        }
      }
    }
    return { total: orders.length, counts, approvedCounts, declinedCounts };
  }

  // ────────────────────────────────────────────────────────────
  // TEST 1 + TEST 3: Run classifier per client and report
  // ────────────────────────────────────────────────────────────
  console.log('='.repeat(72));
  console.log('TEST 1 & 3: Role distribution + approval rate per role');
  console.log('='.repeat(72));

  const clientNames = {};
  querySql('SELECT id, name FROM clients WHERE id IN (1,2,3,4,5)').forEach(r => clientNames[r.id] = r.name);

  const flags = [];

  for (const cid of CLIENT_IDS) {
    console.log();
    console.log('CLIENT ' + cid + ': ' + (clientNames[cid] || ''));
    console.log('-'.repeat(72));
    const r = classifyClient(cid);
    console.log('Role'.padEnd(28) + 'Count'.padStart(10) + '  %'.padStart(8) + '  Appr%'.padStart(9) + '  ApprRange');
    Object.entries(r.counts).sort((a, b) => b[1] - a[1]).forEach(([role, n]) => {
      const pct = ((n / r.total) * 100).toFixed(1);
      const appr = (r.approvedCounts[role] || 0) + (r.declinedCounts[role] || 0);
      const apprRate = appr > 0 ? ((r.approvedCounts[role] || 0) / appr) : null;
      const apprStr = apprRate !== null ? (apprRate * 100).toFixed(1) + '%' : '   -  ';
      const range = EXPECTED_RANGES[role];
      let rangeStr = '';
      let flag = '';
      if (range && apprRate !== null) {
        rangeStr = '[' + (range[0]*100).toFixed(0) + '-' + (range[1]*100).toFixed(0) + '%]';
        if (apprRate < range[0] || apprRate > range[1]) {
          flag = ' ⚠';
          flags.push({
            client: cid, role,
            apprRate: (apprRate * 100).toFixed(1),
            range: '[' + (range[0]*100).toFixed(0) + '-' + (range[1]*100).toFixed(0) + '%]',
            n
          });
        }
      } else if (role.startsWith('unclassified')) {
        flag = ' ⚠ unclassified';
        flags.push({ client: cid, role, n, issue: 'unclassified' });
      }
      console.log(
        role.padEnd(28) +
        String(n).padStart(10) +
        ('(' + pct + '%)').padStart(8) +
        '  ' + apprStr.padStart(7) +
        '  ' + rangeStr.padEnd(11) +
        flag
      );
    });

    // Distribution sanity checks
    const c = r.counts;
    const mainInitial = c.main_initial || 0;
    const mainRebill = c.main_rebill || 0;
    const upsellInitial = c.upsell_initial || 0;
    const upsellRebill = c.upsell_rebill || 0;
    const reprocessing = (c.initial_reprocessing || 0) + (c.upsell_reprocessing || 0);
    const excluded = c.excluded || 0;

    console.log();
    if (mainInitial > mainRebill * 1.5) {
      console.log('  ⚠ main_initial > 1.5× main_rebill — possibly under-counting rebills');
      flags.push({ client: cid, issue: 'main_initial > main_rebill' });
    }
    if (reprocessing / r.total > 0.20) {
      console.log('  ⚠ reprocessing > 20% of orders — possibly over-counting');
      flags.push({ client: cid, issue: 'reprocessing > 20%' });
    }
    if (excluded / r.total > 0.05) {
      console.log('  ⚠ excluded > 5% — possibly group misclassification');
      flags.push({ client: cid, issue: 'excluded > 5%' });
    }
  }

  console.log();
  console.log('='.repeat(72));
  console.log('SUMMARY OF FLAGS');
  console.log('='.repeat(72));
  if (flags.length === 0) {
    console.log('✓ No flags raised. Classifier output looks healthy.');
  } else {
    console.log('Issues to investigate:');
    flags.forEach(f => console.log('  ' + JSON.stringify(f)));
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
