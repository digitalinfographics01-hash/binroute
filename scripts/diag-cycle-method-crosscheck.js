/**
 * Cross-check the existing derived_cycle/derived_attempt against my
 * history-based reprocessing/salvage detection on Optimus.
 *
 * For each order with derived_cycle/derived_attempt populated:
 *  - Compute what my history-based detection would say
 *  - Compare to (cycle, attempt) interpretation
 *
 * Report agreement / disagreement counts so we know if the two methods
 * align on the orders where both can fire.
 */
const { initDb, querySql } = require('../src/db/connection');

const REPROCESSING_WINDOW_DAYS = 90;
const CLIENT_ID = 3; // Optimus

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

  console.log('Loading product map...');
  const productMap = new Map();
  querySql(`
    SELECT pga.product_id, pga.product_type, pg.product_sequence
    FROM product_group_assignments pga
    LEFT JOIN product_groups pg ON pg.id = pga.product_group_id
    WHERE pga.client_id = 1
  `).forEach(a => productMap.set(String(a.product_id), { type: a.product_type, sequence: a.product_sequence }));

  console.log('Loading Optimus orders...');
  const orders = querySql(`
    SELECT id, order_id, customer_id, contact_id, email_address, product_ids,
           billing_cycle, is_cascaded, order_status, acquisition_date, campaign_id,
           decline_reason, decline_category, derived_cycle, derived_attempt
    FROM orders
    WHERE client_id = 3 AND COALESCE(is_internal_test, 0) = 0
    ORDER BY acquisition_date ASC, id ASC
  `);
  console.log('  ' + orders.length + ' orders');
  console.log();

  // Build decline history (initial-phase only)
  const declineByCustomer = new Map();
  const declineByContact = new Map();
  const declineByEmail = new Map();
  // Build last-rebill tracker (rebill phase)
  const lastRebillByCustomer = new Map();
  const lastRebillByContact = new Map();
  const lastRebillByEmail = new Map();

  // Reload Optimus's product map
  productMap.clear();
  querySql(`
    SELECT pga.product_id, pga.product_type, pg.product_sequence
    FROM product_group_assignments pga
    LEFT JOIN product_groups pg ON pg.id = pga.product_group_id
    WHERE pga.client_id = 3
  `).forEach(a => productMap.set(String(a.product_id), { type: a.product_type, sequence: a.product_sequence }));

  function recordInitialDecline(o, pid, info) {
    if (o.decline_category === 'crm_routing_rule') return;
    if (isUnrecoverable(o.decline_reason)) return;
    const cycle = parseInt(o.billing_cycle || 0, 10);
    if (info.type === 'rebill' || (info.type === 'initial_rebill' && cycle >= 1)) return;
    if (info.type !== 'initial' && info.type !== 'initial_rebill') return;
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

  function lookupPriorInitialDecline(o, pid) {
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

  function recordRebillAttempt(o, pid, info) {
    if (o.decline_category === 'crm_routing_rule') return;
    if (isDeclined(o.order_status) && isUnrecoverable(o.decline_reason)) return;
    const cycle = parseInt(o.billing_cycle || 0, 10);
    if (!(info.type === 'rebill' || (info.type === 'initial_rebill' && cycle >= 1))) return;
    const record = { date: o.acquisition_date, declined: isDeclined(o.order_status) };
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

  // For each order with derived_cycle/attempt populated, compare:
  // - Existing classification: derived from (cycle, attempt) tuple
  // - History-based classification: derived from prior-decline lookup
  const agreement = {
    initial_first_match:    0,  // existing: first, mine: first
    initial_first_disagree: 0,  // existing: first, mine: reprocessing
    initial_repro_match:    0,  // existing: reprocessing, mine: reprocessing
    initial_repro_disagree: 0,  // existing: reprocessing, mine: first
    rebill_first_match:     0,
    rebill_first_disagree:  0,
    rebill_salvage_match:   0,
    rebill_salvage_disagree:0,
    skipped_no_product:     0,
  };

  for (const o of orders) {
    if (o.derived_cycle === null || o.derived_attempt === null) continue;
    const pid = getPrimaryProductId(o.product_ids);
    if (!pid) { agreement.skipped_no_product++; continue; }
    const info = productMap.get(pid);
    if (!info || !info.sequence || info.sequence === 'excluded' || info.sequence === 'recovery') continue;
    if (info.type === 'straight_sale') continue; // OTS — no cycle/attempt distinction

    const cycle = o.derived_cycle;
    const attempt = o.derived_attempt;

    if (cycle === 0) {
      // Initial phase
      if (attempt === 1) {
        // Existing says: first attempt
        // Mine: check if prior decline exists
        const prior = lookupPriorInitialDecline(o, pid);
        if (prior && new Date(prior.date) < new Date(o.acquisition_date)) {
          agreement.initial_first_disagree++;
        } else {
          agreement.initial_first_match++;
        }
      } else if (attempt >= 2) {
        // Existing says: reprocessing
        // Mine: check if prior decline exists
        const prior = lookupPriorInitialDecline(o, pid);
        if (prior && new Date(prior.date) < new Date(o.acquisition_date)) {
          agreement.initial_repro_match++;
        } else {
          agreement.initial_repro_disagree++;
        }
      }
    } else {
      // Rebill phase (cycle >= 1)
      if (attempt === 1) {
        // Existing says: first attempt of this cycle
        // Mine: check if last rebill was approved (or no prior)
        const last = lookupLastRebill(o, pid);
        if (last && last.declined) {
          agreement.rebill_first_disagree++;
        } else {
          agreement.rebill_first_match++;
        }
      } else if (attempt >= 2) {
        // Existing says: salvage of this cycle
        // Mine: check if last rebill was declined
        const last = lookupLastRebill(o, pid);
        if (last && last.declined) {
          agreement.rebill_salvage_match++;
        } else {
          agreement.rebill_salvage_disagree++;
        }
      }
    }

    // Update history AFTER comparison
    if (info.type === 'initial' || (info.type === 'initial_rebill' && parseInt(o.billing_cycle||0,10) === 0)) {
      if (isDeclined(o.order_status)) recordInitialDecline(o, pid, info);
    }
    recordRebillAttempt(o, pid, info);
  }

  console.log('='.repeat(72));
  console.log('CROSS-CHECK RESULTS (Optimus, where derived_cycle/attempt are populated)');
  console.log('='.repeat(72));
  console.log();
  console.log('Initial phase (derived_cycle=0):');
  const i_total = agreement.initial_first_match + agreement.initial_first_disagree + agreement.initial_repro_match + agreement.initial_repro_disagree;
  console.log('  attempt=1 (existing says first):');
  console.log('    Match (mine also first):     ' + agreement.initial_first_match);
  console.log('    Disagree (mine says repro):  ' + agreement.initial_first_disagree);
  console.log('  attempt>=2 (existing says reprocessing):');
  console.log('    Match (mine also repro):     ' + agreement.initial_repro_match);
  console.log('    Disagree (mine says first):  ' + agreement.initial_repro_disagree);
  console.log('  Total initial: ' + i_total);
  const initialAgree = agreement.initial_first_match + agreement.initial_repro_match;
  console.log('  Agreement: ' + initialAgree + ' / ' + i_total + ' (' + (initialAgree/i_total*100).toFixed(1) + '%)');
  console.log();
  console.log('Rebill phase (derived_cycle>=1):');
  const r_total = agreement.rebill_first_match + agreement.rebill_first_disagree + agreement.rebill_salvage_match + agreement.rebill_salvage_disagree;
  console.log('  attempt=1 (existing says first attempt of cycle):');
  console.log('    Match (mine also first):       ' + agreement.rebill_first_match);
  console.log('    Disagree (mine says salvage):  ' + agreement.rebill_first_disagree);
  console.log('  attempt>=2 (existing says salvage):');
  console.log('    Match (mine also salvage):     ' + agreement.rebill_salvage_match);
  console.log('    Disagree (mine says first):    ' + agreement.rebill_salvage_disagree);
  console.log('  Total rebill: ' + r_total);
  const rebillAgree = agreement.rebill_first_match + agreement.rebill_salvage_match;
  console.log('  Agreement: ' + rebillAgree + ' / ' + r_total + ' (' + (rebillAgree/r_total*100).toFixed(1) + '%)');
  console.log();
  console.log('Overall agreement: ' + (initialAgree + rebillAgree) + ' / ' + (i_total + r_total) +
              ' (' + ((initialAgree + rebillAgree)/(i_total + r_total)*100).toFixed(1) + '%)');

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
