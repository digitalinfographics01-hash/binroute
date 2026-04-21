/**
 * Inspect Kytsan orders flagged as reprocessing that are NOT in the 15
 * official reprocessing campaigns. Group by which campaign they landed in,
 * show samples with prior-decline context.
 */
const { initDb, querySql } = require('../src/db/connection');

const CLIENT_ID = 1;
const REPROCESSING_CAMPAIGNS = [310, 277, 315, 312, 316, 317, 313, 278, 311, 318, 279, 314, 338, 319, 281];
const WINDOW_DAYS = 90;

function getPrimaryProductId(s) {
  try { const a = JSON.parse(s || '[]'); return a.length > 0 ? String(a[0]) : null; } catch { return null; }
}

function daysBetween(d1, d2) {
  const t1 = new Date(d1).getTime();
  const t2 = new Date(d2).getTime();
  if (isNaN(t1) || isNaN(t2)) return Infinity;
  return Math.abs(t2 - t1) / 86400000;
}

(async () => {
  await initDb();

  // Product map
  const productMap = new Map();
  querySql(`
    SELECT pga.product_id, pga.product_type, pg.product_sequence, pg.group_name
    FROM product_group_assignments pga
    LEFT JOIN product_groups pg ON pg.id = pga.product_group_id
    WHERE pga.client_id = 1
  `).forEach(a => productMap.set(String(a.product_id), { type: a.product_type, sequence: a.product_sequence, group_name: a.group_name }));

  // Campaign name map
  const campaignNames = {};
  querySql('SELECT campaign_id, campaign_name, COALESCE(campaign_type, \'\') as type FROM campaigns WHERE client_id=1').forEach(r => {
    campaignNames[r.campaign_id] = { name: r.campaign_name, type: r.type };
  });

  // Load all orders chronologically
  const orders = querySql(`
    SELECT id, order_id, customer_id, contact_id, email_address, product_ids,
           billing_cycle, is_cascaded, order_status, acquisition_date, campaign_id
    FROM orders
    WHERE client_id = 1 AND COALESCE(is_internal_test, 0) = 0
    ORDER BY acquisition_date ASC, id ASC
  `);
  console.log('Loaded ' + orders.length + ' Kytsan orders');

  // Build incremental decline maps + classify orders
  const declineByCustomer = new Map();
  const declineByContact = new Map();
  const declineByEmail = new Map();

  function recordDecline(o, pid, info) {
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

  // Walk orders, identify "extra" reprocessings (flagged but not in official camp)
  const extras = []; // each = { order, prior, via, campaign_name }
  for (const o of orders) {
    const pid = getPrimaryProductId(o.product_ids);
    if (pid) {
      const info = productMap.get(pid);
      if (info && info.type !== 'straight_sale' && info.sequence !== 'excluded' && info.sequence !== 'recovery') {
        const cycle = parseInt(o.billing_cycle || 0, 10);
        const isInitialPhase = info.type === 'initial' || (info.type === 'initial_rebill' && cycle === 0);
        if (isInitialPhase && !(o.is_cascaded === 1 || o.is_cascaded === '1')) {
          const found = lookupPriorDecline(o, pid);
          if (found) {
            const dd = daysBetween(o.acquisition_date, found.record.date);
            if (dd <= WINDOW_DAYS && new Date(found.record.date) < new Date(o.acquisition_date)) {
              // This is a flagged reprocessing
              if (!REPROCESSING_CAMPAIGNS.includes(o.campaign_id)) {
                extras.push({
                  order: o,
                  prior: found.record,
                  via: found.via,
                  product_id: pid,
                  group_name: info.group_name,
                  days_diff: Math.round(dd),
                });
              }
            }
          }
        }
      }
    }
    // Update history
    if ((o.order_status === 7 || o.order_status === '7') && pid) {
      const info = productMap.get(pid);
      if (info) recordDecline(o, pid, info);
    }
  }

  console.log('Extra reprocessings (flagged but NOT in official reprocessing campaigns): ' + extras.length);
  console.log();

  // Group by current campaign
  const byCampaign = {};
  for (const e of extras) {
    const cid = e.order.campaign_id;
    if (!byCampaign[cid]) {
      byCampaign[cid] = {
        campaign_id: cid,
        name: campaignNames[cid]?.name || '(unknown)',
        type: campaignNames[cid]?.type || '(none)',
        count: 0,
        viaCustomer: 0,
        viaContact: 0,
        viaEmail: 0,
        priorCampaigns: {},
        samples: [],
      };
    }
    const b = byCampaign[cid];
    b.count++;
    if (e.via === 'customer_id') b.viaCustomer++;
    else if (e.via === 'contact_id') b.viaContact++;
    else b.viaEmail++;
    const pCid = e.prior.campaign_id;
    b.priorCampaigns[pCid] = (b.priorCampaigns[pCid] || 0) + 1;
    if (b.samples.length < 5) b.samples.push(e);
  }

  console.log('='.repeat(80));
  console.log('GROUPED BY CURRENT CAMPAIGN (where the reprocessing is being classified)');
  console.log('='.repeat(80));
  console.log();
  Object.values(byCampaign).sort((a, b) => b.count - a.count).slice(0, 25).forEach(b => {
    console.log('Campaign ' + b.campaign_id + ' [' + b.type + '] — ' + b.count + ' extra reprocessings');
    console.log('  Name: ' + (b.name || '(no name)'));
    console.log('  Match via: customer=' + b.viaCustomer + ', contact=' + b.viaContact + ', email=' + b.viaEmail);
    const topPriors = Object.entries(b.priorCampaigns).sort((a, b) => b[1] - a[1]).slice(0, 5);
    console.log('  Prior decline campaigns: ' + topPriors.map(([c, n]) => {
      const cn = campaignNames[c]?.name?.substring(0, 30) || '(unknown)';
      return c + '(' + cn + ')=' + n;
    }).join(', '));
    console.log('  Samples:');
    b.samples.forEach(s => {
      console.log('    order ' + s.order.order_id + ' product ' + s.product_id + ' [' + s.group_name + ']');
      console.log('      via: ' + s.via + '  (customer=' + (s.order.customer_id || 'null') + ', contact=' + (s.order.contact_id || 'null') + ')');
      console.log('      prior decline: order ' + s.prior.order_id + ' on campaign ' + s.prior.campaign_id +
                  ' (' + (campaignNames[s.prior.campaign_id]?.name?.substring(0, 30) || '?') + ')' +
                  ' — ' + s.days_diff + ' days ago');
    });
    console.log();
  });

  // Stat: of all extras, what % match between SAME current and prior campaign?
  let sameCamp = 0;
  let differentCamp = 0;
  for (const e of extras) {
    if (e.order.campaign_id === e.prior.campaign_id) sameCamp++;
    else differentCamp++;
  }
  console.log('='.repeat(80));
  console.log('Pattern analysis');
  console.log('='.repeat(80));
  console.log('Reprocessing where current campaign == prior decline campaign: ' + sameCamp + ' (' + (sameCamp/extras.length*100).toFixed(1) + '%)');
  console.log('Reprocessing where current campaign != prior decline campaign: ' + differentCamp + ' (' + (differentCamp/extras.length*100).toFixed(1) + '%)');

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
