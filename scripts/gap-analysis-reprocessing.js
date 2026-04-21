/**
 * Gap analysis v2 — with contact_id and email fallback matching.
 *
 * Builds three decline indexes:
 *   - declineByCustomer: customer_id → product_id → record
 *   - declineByContact:  contact_id → product_id → record
 *   - declineByEmail:    email → product_id → record
 *
 * Only records INITIAL-phase declines (not rebill declines, which would
 * be misleading for the reprocessing rule).
 *
 * For each order in a Kytsan reprocessing campaign, attempt to match a prior
 * decline via the three indexes in priority order.
 */
const { initDb, querySql } = require('../src/db/connection');

const CLIENT_ID = 1;
const REPROCESSING_CAMPAIGNS = [310, 277, 315, 312, 316, 317, 313, 278, 311, 318, 279, 314, 338, 319, 281];
const WINDOW_DAYS = 90;

const UNRECOVERABLE_REASONS = new Set([
  'cvv2 mismatch', 'invalid credit card number', 'invalid card number',
  '14  -  invalid card number', '14 - invalid card number', 'invalid cc',
  'c2  -  cvv mismatch/ missing', 'c2 - cvv mismatch/ missing',
  'expired card', 'account closed',
]);
function isUnrecoverable(r) { return r && UNRECOVERABLE_REASONS.has(r.toLowerCase().trim()); }

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
    SELECT pga.product_id, pga.product_type, pg.product_sequence, pg.group_name
    FROM product_group_assignments pga
    LEFT JOIN product_groups pg ON pg.id = pga.product_group_id
    WHERE pga.client_id = 1
  `).forEach(a => {
    productMap.set(String(a.product_id), {
      type: a.product_type,
      sequence: a.product_sequence,
      group_name: a.group_name,
    });
  });
  console.log('  ' + productMap.size + ' products');
  console.log();

  console.log('Loading orders chronologically...');
  const orders = querySql(`
    SELECT id, order_id, customer_id, contact_id, email_address, product_ids,
           billing_cycle, is_cascaded, order_status, acquisition_date, campaign_id,
           decline_reason, decline_category
    FROM orders
    WHERE client_id = 1
      AND COALESCE(is_internal_test, 0) = 0
    ORDER BY acquisition_date ASC, id ASC
  `);
  console.log('  ' + orders.length + ' orders');
  console.log();

  // Three indexes
  const declineByCustomer = new Map();
  const declineByContact = new Map();
  const declineByEmail = new Map();

  function recordDecline(o, pid, info) {
    // Filter system routing declines
    if (o.decline_category === 'crm_routing_rule') return;
    // Filter Bucket 1 unrecoverable
    if (isUnrecoverable(o.decline_reason)) return;
    // Only initial-phase declines
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
      if (m) { const r = m.get(pid); if (r) return { record: r, via: 'customer_id' }; }
    }
    if (o.contact_id != null && String(o.contact_id) !== '0' && String(o.contact_id) !== '') {
      const m = declineByContact.get(String(o.contact_id));
      if (m) { const r = m.get(pid); if (r) return { record: r, via: 'contact_id' }; }
    }
    if (o.email_address && o.email_address.trim() !== '') {
      const m = declineByEmail.get(o.email_address.trim().toLowerCase());
      if (m) { const r = m.get(pid); if (r) return { record: r, via: 'email' }; }
    }
    return null;
  }

  // Build dict in chronological order, gathering reprocessing-campaign orders for analysis
  const inReprocessingCampaigns = [];
  for (const o of orders) {
    const pid = getPrimaryProductId(o.product_ids);
    if (REPROCESSING_CAMPAIGNS.includes(o.campaign_id)) {
      inReprocessingCampaigns.push({ ...o, _pid: pid });
    }
    // Update decline history AFTER (so current order can't match itself)
    if ((o.order_status === 7 || o.order_status === '7') && pid) {
      const info = productMap.get(pid);
      if (info) recordDecline(o, pid, info);
    }
  }

  console.log('Decline indexes:');
  console.log('  by customer_id: ' + declineByCustomer.size + ' unique customers');
  console.log('  by contact_id:  ' + declineByContact.size + ' unique contacts');
  console.log('  by email:       ' + declineByEmail.size + ' unique emails');
  console.log();

  // Now analyze every order in a reprocessing campaign
  // We need to use the dict AS IT WAS at the moment of each order, but since
  // we process in chrono order, the dict was already updated incrementally.
  // For the analysis we need to know "what was in the dict before this order
  // was processed" — so we re-walk and rebuild.
  declineByCustomer.clear();
  declineByContact.clear();
  declineByEmail.clear();

  const results = {
    flagged_reprocessing: 0,
    base_not_initial: 0,
    missed_no_prior_decline: 0,
    missed_too_old: 0,
  };
  const matchVia = { customer_id: 0, contact_id: 0, email: 0 };
  const perCampaign = {};
  const missSamples = { missed_no_prior_decline: [], missed_too_old: [] };
  REPROCESSING_CAMPAIGNS.forEach(c => perCampaign[c] = { total: 0, flagged: 0, missed: 0, base_skip: 0 });

  for (const o of orders) {
    const pid = getPrimaryProductId(o.product_ids);
    const isReprocCamp = REPROCESSING_CAMPAIGNS.includes(o.campaign_id);

    if (isReprocCamp) {
      perCampaign[o.campaign_id].total++;

      const info = pid ? productMap.get(pid) : null;
      if (!info || !info.sequence) {
        results.base_not_initial++;
        perCampaign[o.campaign_id].base_skip++;
      } else if (info.sequence === 'excluded' || info.sequence === 'recovery') {
        results.base_not_initial++;
        perCampaign[o.campaign_id].base_skip++;
      } else if (info.type === 'straight_sale') {
        results.base_not_initial++;
        perCampaign[o.campaign_id].base_skip++;
      } else {
        const cycle = parseInt(o.billing_cycle || 0, 10);
        const isInitial = info.type === 'initial' || (info.type === 'initial_rebill' && cycle === 0);
        if (!isInitial) {
          results.base_not_initial++;
          perCampaign[o.campaign_id].base_skip++;
        } else if (o.is_cascaded === 1 || o.is_cascaded === '1') {
          results.base_not_initial++;
          perCampaign[o.campaign_id].base_skip++;
        } else {
          // This is the candidate. Try to find a prior decline.
          const found = lookupPriorDecline(o, pid);
          if (found) {
            const dd = daysBetween(o.acquisition_date, found.record.date);
            if (dd <= WINDOW_DAYS && new Date(found.record.date) < new Date(o.acquisition_date)) {
              results.flagged_reprocessing++;
              matchVia[found.via]++;
              perCampaign[o.campaign_id].flagged++;
            } else {
              results.missed_too_old++;
              perCampaign[o.campaign_id].missed++;
              if (missSamples.missed_too_old.length < 5) {
                missSamples.missed_too_old.push({ order: o.order_id, days: Math.round(dd) });
              }
            }
          } else {
            results.missed_no_prior_decline++;
            perCampaign[o.campaign_id].missed++;
            if (missSamples.missed_no_prior_decline.length < 5) {
              missSamples.missed_no_prior_decline.push({
                order: o.order_id,
                customer_id: o.customer_id,
                contact_id: o.contact_id,
                product_id: pid,
                campaign: o.campaign_id,
              });
            }
          }
        }
      }
    }

    // Update decline history AFTER processing this order
    if ((o.order_status === 7 || o.order_status === '7') && pid) {
      const info = productMap.get(pid);
      if (info) recordDecline(o, pid, info);
    }
  }

  // Report
  const total = results.flagged_reprocessing + results.base_not_initial + results.missed_no_prior_decline + results.missed_too_old;
  console.log('='.repeat(70));
  console.log('GAP ANALYSIS V2: Kytsan reprocessing campaigns with contact_id matching');
  console.log('='.repeat(70));
  console.log('Total orders in reprocessing campaigns: ' + total);
  console.log();
  const candidates = total - results.base_not_initial;
  console.log('Base classifier skipped (rebill/straight_sale/cascaded/excluded): ' + results.base_not_initial);
  console.log('Reprocessing candidates (initial phase): ' + candidates);
  console.log();
  console.log('  ✓ Flagged as reprocessing:    ' + results.flagged_reprocessing + ' (' + (results.flagged_reprocessing/candidates*100).toFixed(1) + '% of candidates)');
  console.log('  ✗ No prior decline found:     ' + results.missed_no_prior_decline + ' (' + (results.missed_no_prior_decline/candidates*100).toFixed(1) + '%)');
  console.log('  ✗ Prior decline too old:      ' + results.missed_too_old + ' (' + (results.missed_too_old/candidates*100).toFixed(1) + '%)');
  console.log();
  console.log('Match via:');
  console.log('  customer_id: ' + matchVia.customer_id);
  console.log('  contact_id:  ' + matchVia.contact_id + '  (the new fix)');
  console.log('  email:       ' + matchVia.email);
  console.log();

  console.log('Per-campaign:');
  console.log('CID  | Total | Skip | Flagged | Missed | Detection rate (of candidates)');
  Object.entries(perCampaign).sort((a, b) => b[1].total - a[1].total).forEach(([cid, s]) => {
    const cands = s.total - s.base_skip;
    const rate = cands > 0 ? (s.flagged / cands * 100).toFixed(1) : 'n/a';
    console.log(
      String(cid).padStart(4) + ' | ' +
      String(s.total).padStart(5) + ' | ' +
      String(s.base_skip).padStart(4) + ' | ' +
      String(s.flagged).padStart(7) + ' | ' +
      String(s.missed).padStart(6) + ' | ' +
      rate + '%'
    );
  });
  console.log();
  if (missSamples.missed_no_prior_decline.length > 0) {
    console.log('Sample remaining no-prior-decline misses:');
    missSamples.missed_no_prior_decline.forEach(s => console.log('  ' + JSON.stringify(s)));
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
