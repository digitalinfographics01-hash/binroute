/**
 * Verify two theories about Kytsan's reprocessing campaigns:
 * 1. Rebill-product orders inside reprocessing campaigns really are cycle>=1 rebills
 *    (confirming they're the "rebill tail" of reprocessed subscriptions, not misses)
 * 2. Anonymous orders in reprocessing campaigns have a contact_id we can match on
 */
const { initDb, querySql } = require('../src/db/connection');

const REPROCESSING_CAMPAIGNS = [310, 277, 315, 312, 316, 317, 313, 278, 311, 318, 279, 314, 338, 319, 281];

(async () => {
  await initDb();

  const placeholders = REPROCESSING_CAMPAIGNS.map(() => '?').join(',');

  console.log('=== Theory 1: rebill-product orders have cycle >= 1 ===');
  const rebillPhaseDist = querySql(`
    SELECT o.billing_cycle, COUNT(*) as n
    FROM orders o
    JOIN product_group_assignments pga ON pga.client_id = o.client_id
      AND CAST(pga.product_id AS TEXT) = JSON_EXTRACT(o.product_ids, '$[0]')
    WHERE o.client_id = 1
      AND o.campaign_id IN (${placeholders})
      AND pga.product_type = 'rebill'
    GROUP BY o.billing_cycle
    ORDER BY o.billing_cycle
  `, REPROCESSING_CAMPAIGNS);

  console.log('Billing cycle distribution for rebill-type products in reprocessing campaigns:');
  rebillPhaseDist.forEach(r => {
    console.log('  cycle ' + r.billing_cycle + ': ' + r.n + ' orders');
  });
  console.log();

  console.log('=== Theory 2: anonymous orders — do they have contact_id? ===');
  const anonAnalysis = querySql(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN customer_id IS NULL OR customer_id = 0 THEN 1 ELSE 0 END) as no_customer_id,
      SUM(CASE WHEN (customer_id IS NULL OR customer_id = 0) AND (contact_id IS NULL OR contact_id = 0) THEN 1 ELSE 0 END) as no_either,
      SUM(CASE WHEN (customer_id IS NULL OR customer_id = 0) AND contact_id IS NOT NULL AND contact_id > 0 THEN 1 ELSE 0 END) as anon_but_has_contact,
      SUM(CASE WHEN is_anonymous_decline = 1 THEN 1 ELSE 0 END) as flagged_anon_decline,
      SUM(CASE WHEN email_address IS NOT NULL AND email_address != '' THEN 1 ELSE 0 END) as has_email
    FROM orders
    WHERE client_id = 1
      AND campaign_id IN (${placeholders})
  `, REPROCESSING_CAMPAIGNS)[0];

  console.log('Total orders in reprocessing campaigns:          ' + anonAnalysis.total);
  console.log('Missing customer_id (anonymous):                 ' + anonAnalysis.no_customer_id);
  console.log('  of those, with contact_id:                     ' + anonAnalysis.anon_but_has_contact);
  console.log('  of those, with NO contact_id either:           ' + anonAnalysis.no_either);
  console.log('Marked is_anonymous_decline=1:                   ' + anonAnalysis.flagged_anon_decline);
  console.log('With email address:                              ' + anonAnalysis.has_email);
  console.log();

  console.log('=== Can we match anonymous declines to reprocessing via contact_id? ===');
  // For each anonymous order in a reprocessing campaign, check if that contact_id
  // has a prior declined order in a non-reprocessing campaign
  const sampleAnon = querySql(`
    SELECT o.order_id, o.contact_id, o.campaign_id, JSON_EXTRACT(o.product_ids, '$[0]') as product_id,
           o.acquisition_date
    FROM orders o
    WHERE o.client_id = 1
      AND o.campaign_id IN (${placeholders})
      AND (o.customer_id IS NULL OR o.customer_id = 0)
      AND o.contact_id IS NOT NULL AND o.contact_id > 0
    LIMIT 20
  `, REPROCESSING_CAMPAIGNS);

  console.log('Sample of ' + sampleAnon.length + ' anonymous-with-contact_id orders:');
  let matchCount = 0;
  for (const o of sampleAnon) {
    // Does this contact have a prior declined order for the same product?
    const priorDecline = querySql(`
      SELECT order_id, campaign_id, acquisition_date, order_status
      FROM orders
      WHERE client_id = 1
        AND contact_id = ?
        AND JSON_EXTRACT(product_ids, '$[0]') = ?
        AND order_status = 7
        AND acquisition_date < ?
      ORDER BY acquisition_date DESC
      LIMIT 1
    `, [o.contact_id, o.product_id, o.acquisition_date]);

    if (priorDecline.length > 0) {
      matchCount++;
      const p = priorDecline[0];
      console.log('  ' + o.order_id + ' (contact ' + o.contact_id + ', prod ' + o.product_id + ') → prior decline ' + p.order_id + ' in campaign ' + p.campaign_id);
    }
  }
  console.log();
  console.log('Contact-id matching found prior declines for ' + matchCount + ' of ' + sampleAnon.length + ' samples');

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
