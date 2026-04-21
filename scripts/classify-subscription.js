/**
 * Subscription classifier v3 — with contact_id and email fallback for
 * anonymous reprocessing detection.
 *
 * Base matrix:
 *   excluded sequence                      → excluded
 *   recovery sequence                      → rebill_recovery
 *   straight_sale + main/upsell            → upsell_ots
 *   initial / initial_rebill+cycle=0 + main    → main_initial
 *   initial / initial_rebill+cycle=0 + upsell  → upsell_initial
 *   rebill  / initial_rebill+cycle≥1 + main    → main_rebill
 *   rebill  / initial_rebill+cycle≥1 + upsell  → upsell_rebill
 *
 * Reprocessing modifier:
 *   IF base_role in (main_initial, upsell_initial)
 *   AND is_cascaded = 0
 *   AND a prior INITIAL-phase declined order exists for the SAME product_id within 90 days
 *   AND the prior decline can be matched via customer_id, contact_id, or email
 *   THEN role becomes initial_reprocessing / upsell_reprocessing
 *
 * Dry run: no DB writes.
 */
const { initDb, querySql } = require('../src/db/connection');

const REPROCESSING_WINDOW_DAYS = 90;
const CLIENT_IDS = [1, 2, 3, 4, 5];

// Truly unrecoverable decline reasons (Bucket 1) — never trigger reprocessing detection
// because no processor change OR timing change can fix them. Customer must provide new
// input or new card for any retry to be a "fresh" attempt, not a continuation.
const UNRECOVERABLE_REASONS = new Set([
  'cvv2 mismatch',
  'invalid credit card number',
  'invalid card number',
  '14  -  invalid card number',
  '14 - invalid card number',
  'invalid cc',
  'c2  -  cvv mismatch/ missing',
  'c2 - cvv mismatch/ missing',
  'expired card',
  'account closed',
]);

function isUnrecoverable(declineReason) {
  if (!declineReason) return false;
  return UNRECOVERABLE_REASONS.has(declineReason.toLowerCase().trim());
}

function getPrimaryProductId(productIdsJson) {
  try {
    const arr = JSON.parse(productIdsJson || '[]');
    return arr.length > 0 ? String(arr[0]) : null;
  } catch { return null; }
}

function daysBetween(d1, d2) {
  const t1 = new Date(d1).getTime();
  const t2 = new Date(d2).getTime();
  if (isNaN(t1) || isNaN(t2)) return Infinity;
  return Math.abs(t2 - t1) / 86400000;
}

(async () => {
  await initDb();

  console.log('Loading product map...');
  const productMap = new Map();
  querySql(`
    SELECT pga.client_id, pga.product_id, pga.product_type, pga.product_group_id,
           pg.product_sequence, pg.group_name
    FROM product_group_assignments pga
    LEFT JOIN product_groups pg ON pg.id = pga.product_group_id
    WHERE pga.client_id IN (1,2,3,4,5)
  `).forEach(a => {
    productMap.set(a.client_id + ':' + a.product_id, {
      type: a.product_type,
      group_id: a.product_group_id,
      sequence: a.product_sequence,
      group_name: a.group_name,
    });
  });
  console.log('  ' + productMap.size + ' product assignments');
  console.log();

  const clientNames = {};
  querySql('SELECT id, name FROM clients WHERE id IN (1,2,3,4,5)').forEach(r => clientNames[r.id] = r.name);

  for (const cid of CLIENT_IDS) {
    console.log('='.repeat(72));
    console.log('CLIENT ' + cid + ': ' + (clientNames[cid] || 'unknown'));
    console.log('='.repeat(72));

    const tStart = Date.now();
    console.log('Loading orders...');
    const orders = querySql(`
      SELECT id, client_id, order_id, customer_id, contact_id, email_address, product_ids,
             billing_cycle, is_cascaded, order_status, acquisition_date, campaign_id,
             decline_reason, decline_category,
             COALESCE(derived_product_role, '(null)') as current_role
      FROM orders
      WHERE client_id = ?
        AND COALESCE(is_internal_test, 0) = 0
      ORDER BY acquisition_date ASC, id ASC
    `, [cid]);
    console.log('  ' + orders.length + ' orders in ' + (Date.now() - tStart) + 'ms');

    // Three decline indexes for matching
    const declineByCustomer = new Map();
    const declineByContact = new Map();
    const declineByEmail = new Map();

    // Initial-phase decline history (used for reprocessing detection)
    function recordDecline(o, pid, info) {
      // Filter system routing declines (not real declines, just CRM rules)
      if (o.decline_category === 'crm_routing_rule') return;
      // Filter truly unrecoverable declines (Bucket 1: human input errors / hard state)
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

    // Rebill-phase last-attempt tracker (used for rebill salvage detection).
    // Tracks the MOST RECENT rebill attempt per (customer, product) regardless of status.
    // The classifier checks: if last prior rebill was DECLINED → current is a salvage continuation
    // (same cycle, retried). If prior was APPROVED or no prior → current is a new cycle's first attempt.
    // No time window: a cycle stays active in salvage until approved.
    const lastRebillByCustomer = new Map();
    const lastRebillByContact = new Map();
    const lastRebillByEmail = new Map();

    function recordRebillAttempt(o, pid, info) {
      // Only track real rebill attempts. Skip:
      //  - system declines (CRM routing — not actually attempted)
      //  - Bucket 1 unrecoverable declines (cycle is effectively dead, no salvage will follow)
      if (o.decline_category === 'crm_routing_rule') return;
      if (isDeclined(o.order_status) && isUnrecoverable(o.decline_reason)) return;

      const cycle = parseInt(o.billing_cycle || 0, 10);
      const isRebillPhase = info.type === 'rebill' || (info.type === 'initial_rebill' && cycle >= 1);
      if (!isRebillPhase) return;

      const record = {
        date: o.acquisition_date,
        order_id: o.order_id,
        campaign_id: o.campaign_id,
        approved: isApproved(o.order_status),
        declined: isDeclined(o.order_status),
      };

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

    // Need isApproved/isDeclined helpers in this scope
    const APPROVED_STATUSES = new Set([2, 6, 8, '2', '6', '8']);
    function isApproved(s) { return APPROVED_STATUSES.has(s); }
    function isDeclined(s) { return s === 7 || s === '7'; }

    function classify(o) {
      const pid = getPrimaryProductId(o.product_ids);
      if (!pid) return 'unclassified_no_product';

      const info = productMap.get(o.client_id + ':' + pid);
      if (!info) return 'unclassified_unknown_product';
      if (!info.type) return 'unclassified_null_type';
      if (!info.sequence) return 'unclassified_null_sequence';

      const seq = info.sequence;

      // Group-level overrides (always take precedence)
      if (seq === 'excluded') return 'excluded';
      if (seq === 'recovery') return 'rebill_recovery';
      if (info.type === 'straight_sale') return 'upsell_ots';

      // PRIMARY PATH: use existing derived_cycle / derived_attempt when populated
      // (98.6% agreement with history-based detection on Optimus, and gives us
      // proper per-cycle granularity for the 70-86% of orders that have it)
      if (o.derived_cycle !== null && o.derived_cycle !== undefined &&
          o.derived_attempt !== null && o.derived_attempt !== undefined) {
        const dCycle = parseInt(o.derived_cycle, 10);
        const dAttempt = parseInt(o.derived_attempt, 10);

        if (dCycle === 0) {
          // Initial phase
          if (dAttempt === 1) return seq + '_initial';
          return seq === 'main' ? 'initial_reprocessing' : 'upsell_reprocessing';
        } else {
          // Rebill phase (cycle >= 1)
          if (dAttempt === 1) return seq + '_rebill';
          return seq === 'main' ? 'main_rebill_salvage' : 'upsell_rebill_salvage';
        }
      }

      // FALLBACK PATH: derived_cycle/attempt are null (anonymous declines + edge cases)
      // Use product.type + billing_cycle to determine phase, then history-based
      // detection for first vs reprocessing/salvage.
      const cycle = parseInt(o.billing_cycle || 0, 10);
      const type = info.type;

      let phase;
      if (type === 'initial' || (type === 'initial_rebill' && cycle === 0)) phase = 'initial';
      else if (type === 'rebill' || (type === 'initial_rebill' && cycle >= 1)) phase = 'rebill';
      else return 'unclassified_unknown_type';

      const baseRole = seq + '_' + phase;

      // Reprocessing modifier (initial phase, 90-day window, prior decline required)
      if (phase === 'initial' && !(o.is_cascaded === 1 || o.is_cascaded === '1')) {
        const prior = lookupPriorDecline(o, pid);
        if (prior) {
          const dd = daysBetween(o.acquisition_date, prior.date);
          if (dd <= REPROCESSING_WINDOW_DAYS && new Date(prior.date) < new Date(o.acquisition_date)) {
            return seq === 'main' ? 'initial_reprocessing' : 'upsell_reprocessing';
          }
        }
      }

      // Rebill salvage modifier (no time window — cycle stays active until approved)
      if (phase === 'rebill' && !(o.is_cascaded === 1 || o.is_cascaded === '1')) {
        const lastRebill = lookupLastRebill(o, pid);
        if (lastRebill && lastRebill.declined && new Date(lastRebill.date) < new Date(o.acquisition_date)) {
          // Last attempt for this customer + product was declined → still in same cycle, salvage
          return seq === 'main' ? 'main_rebill_salvage' : 'upsell_rebill_salvage';
        }
      }

      return baseRole;
    }

    const counts = {};
    const diffs = {};
    const diffSamples = {};
    let processed = 0;
    let reprocessingCount = 0;

    const tClassify = Date.now();
    for (const o of orders) {
      const newRole = classify(o);
      counts[newRole] = (counts[newRole] || 0) + 1;
      if (newRole === 'initial_reprocessing' || newRole === 'upsell_reprocessing') {
        reprocessingCount++;
      }
      processed++;

      if (newRole !== o.current_role) {
        const key = o.current_role + ' → ' + newRole;
        diffs[key] = (diffs[key] || 0) + 1;
        if (!diffSamples[key]) diffSamples[key] = [];
        if (diffSamples[key].length < 3) diffSamples[key].push(o.order_id);
      }

      // Update history AFTER classifying current order
      const pidForHistory = getPrimaryProductId(o.product_ids);
      if (pidForHistory) {
        const histInfo = productMap.get(o.client_id + ':' + pidForHistory);
        if (histInfo) {
          // Initial-phase decline history (for reprocessing detection) — only declined orders
          if (o.order_status === 7 || o.order_status === '7') {
            recordDecline(o, pidForHistory, histInfo);
          }
          // Rebill-phase last-attempt tracker (for rebill salvage detection) — both approved and declined
          recordRebillAttempt(o, pidForHistory, histInfo);
        }
      }
    }
    console.log('  Classified ' + processed + ' in ' + (Date.now() - tClassify) + 'ms');
    console.log();

    console.log('New role distribution:');
    Object.entries(counts).sort((a, b) => b[1] - a[1]).forEach(([role, n]) => {
      const pct = ((n / processed) * 100).toFixed(1);
      console.log('  ' + role.padEnd(32) + ' ' + String(n).padStart(8) + '  (' + pct + '%)');
    });
    console.log();
    console.log('Reprocessing detected: ' + reprocessingCount + ' orders');
    console.log();

    const sortedDiffs = Object.entries(diffs).sort((a, b) => b[1] - a[1]);
    const totalDiff = sortedDiffs.reduce((s, [, n]) => s + n, 0);
    console.log('Diffs vs existing: ' + totalDiff + ' / ' + processed + ' (' + (totalDiff/processed*100).toFixed(1) + '%)');
    sortedDiffs.slice(0, 12).forEach(([key, n]) => {
      console.log('  ' + key.padEnd(60) + ' ' + String(n).padStart(7));
    });
    if (sortedDiffs.length > 12) console.log('  ... and ' + (sortedDiffs.length - 12) + ' more');
    console.log();
  }

  console.log('='.repeat(72));
  console.log('Done — dry run. No DB writes.');
  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
