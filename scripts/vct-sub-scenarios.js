const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // Only look at customers whose first order is within our data range
  // so we see the FULL lifecycle from day 1

  // ========================================
  // SCENARIO 1: Customer approved on store (first OTP order approved)
  // ========================================
  console.log('=== SCENARIO 1: First store order APPROVED ===\n');

  const approvedStats = db.prepare(`
    SELECT
      COUNT(DISTINCT customer_id) as customers,
      ROUND(AVG(sub_count), 2) as avg_subs,
      ROUND(AVG(total_orders), 1) as avg_orders,
      SUM(CASE WHEN sub_count = 1 THEN 1 ELSE 0 END) as with_1_sub,
      SUM(CASE WHEN sub_count = 2 THEN 1 ELSE 0 END) as with_2_subs,
      SUM(CASE WHEN sub_count = 3 THEN 1 ELSE 0 END) as with_3_subs,
      SUM(CASE WHEN sub_count = 4 THEN 1 ELSE 0 END) as with_4_subs,
      SUM(CASE WHEN sub_count >= 5 THEN 1 ELSE 0 END) as with_5plus_subs
    FROM (
      SELECT customer_id,
        COUNT(DISTINCT subscription_id) as sub_count,
        COUNT(*) as total_orders
      FROM orders WHERE client_id = 6 AND customer_id IN (
        SELECT customer_id FROM orders WHERE client_id = 6
        AND billing_model_name = 'One Time Purchase' AND order_status IN (2,6,8)
        GROUP BY customer_id
        HAVING MIN(acquisition_date) >= '2026-01-01'
      )
      GROUP BY customer_id
    )
  `).get();
  console.log('Subscription distribution (approved store customers, Jan 2026+):');
  console.table(approvedStats);

  // What campaigns/subscriptions do approved customers get?
  const approvedCampaigns = db.prepare(`
    SELECT campaign_id, billing_model_name as model,
      COUNT(DISTINCT customer_id) as customers,
      COUNT(*) as orders,
      SUM(CASE WHEN billing_cycle = 0 THEN 1 ELSE 0 END) as cycle0,
      SUM(CASE WHEN billing_cycle > 0 THEN 1 ELSE 0 END) as rebills,
      ROUND(AVG(order_total), 2) as avg_total
    FROM orders WHERE client_id = 6 AND customer_id IN (
      SELECT customer_id FROM orders WHERE client_id = 6
      AND billing_model_name = 'One Time Purchase' AND order_status IN (2,6,8)
      GROUP BY customer_id
      HAVING MIN(acquisition_date) >= '2026-01-01'
    )
    GROUP BY campaign_id, billing_model_name
    ORDER BY customers DESC
    LIMIT 20
  `).all();
  console.log('\nCampaign breakdown for approved customers:');
  console.table(approvedCampaigns);

  // Trace 5 approved customers to see exact subscription pattern
  console.log('\n--- Approved customer traces (Jan 2026+) ---\n');
  const approvedSamples = db.prepare(`
    SELECT customer_id FROM orders WHERE client_id = 6
    AND billing_model_name = 'One Time Purchase' AND order_status IN (2,6,8)
    GROUP BY customer_id
    HAVING MIN(acquisition_date) >= '2026-02-01' AND MIN(acquisition_date) < '2026-02-15'
    ORDER BY RANDOM() LIMIT 5
  `).all();

  for (const {customer_id} of approvedSamples) {
    const orders = db.prepare(`
      SELECT order_id, campaign_id as camp, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids as products,
             retry_attempt as retry, subscription_id as sub_id,
             acquisition_date as acq_date
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);
    const subs = new Set(orders.map(o => o.sub_id)).size;
    const firstDate = new Date(orders[0].acq_date);
    console.log(`Customer ${customer_id} — ${orders.length} orders, ${subs} subscriptions:`);
    orders.forEach((o, i) => {
      const daysSince = Math.round((new Date(o.acq_date) - firstDate) / 86400000);
      const sl = [2,6,8].includes(o.status) ? 'OK' : 'DECL';
      console.log(`  [${String(i).padStart(2)}] Day ${String(daysSince).padStart(3)} | camp ${String(o.camp).padStart(3)} | ${(o.model||'null').padEnd(22)} | cyc ${String(o.cycle).padStart(2)} ret ${o.retry} | $${String(o.total).padStart(6)} | ${sl.padEnd(4)} | sub:${o.sub_id ? o.sub_id.substring(0,8) : 'null'} | ${o.products}`);
    });
    console.log('');
  }

  // ========================================
  // SCENARIO 2: Customer declined on ALL store orders
  // ========================================
  console.log('\n\n=== SCENARIO 2: ALL store orders DECLINED ===\n');

  const declinedStats = db.prepare(`
    SELECT
      COUNT(DISTINCT customer_id) as customers,
      ROUND(AVG(sub_count), 2) as avg_subs,
      ROUND(AVG(total_orders), 1) as avg_orders,
      SUM(CASE WHEN sub_count = 1 THEN 1 ELSE 0 END) as with_1_sub,
      SUM(CASE WHEN sub_count = 2 THEN 1 ELSE 0 END) as with_2_subs,
      SUM(CASE WHEN sub_count = 3 THEN 1 ELSE 0 END) as with_3_subs,
      SUM(CASE WHEN sub_count = 4 THEN 1 ELSE 0 END) as with_4_subs,
      SUM(CASE WHEN sub_count >= 5 THEN 1 ELSE 0 END) as with_5plus_subs
    FROM (
      SELECT customer_id,
        COUNT(DISTINCT subscription_id) as sub_count,
        COUNT(*) as total_orders
      FROM orders WHERE client_id = 6 AND customer_id IN (
        SELECT customer_id FROM orders WHERE client_id = 6
        AND billing_model_name = 'One Time Purchase'
        GROUP BY customer_id
        HAVING MIN(acquisition_date) >= '2026-01-01'
        AND SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) = 0
      )
      GROUP BY customer_id
    )
  `).get();
  console.log('Subscription distribution (all-declined store customers, Jan 2026+):');
  console.table(declinedStats);

  const declinedCampaigns = db.prepare(`
    SELECT campaign_id, billing_model_name as model,
      COUNT(DISTINCT customer_id) as customers,
      COUNT(*) as orders,
      SUM(CASE WHEN billing_cycle = 0 THEN 1 ELSE 0 END) as cycle0,
      SUM(CASE WHEN billing_cycle > 0 THEN 1 ELSE 0 END) as rebills,
      ROUND(AVG(order_total), 2) as avg_total
    FROM orders WHERE client_id = 6 AND customer_id IN (
      SELECT customer_id FROM orders WHERE client_id = 6
      AND billing_model_name = 'One Time Purchase'
      GROUP BY customer_id
      HAVING MIN(acquisition_date) >= '2026-01-01'
      AND SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) = 0
    )
    GROUP BY campaign_id, billing_model_name
    ORDER BY customers DESC
    LIMIT 20
  `).all();
  console.log('\nCampaign breakdown for all-declined customers:');
  console.table(declinedCampaigns);

  // Trace 5 all-declined customers
  console.log('\n--- All-declined customer traces (Jan 2026+) ---\n');
  const declinedSamples = db.prepare(`
    SELECT customer_id FROM orders WHERE client_id = 6
    AND billing_model_name = 'One Time Purchase'
    GROUP BY customer_id
    HAVING MIN(acquisition_date) >= '2026-02-01' AND MIN(acquisition_date) < '2026-02-15'
    AND SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) = 0
    ORDER BY RANDOM() LIMIT 5
  `).all();

  for (const {customer_id} of declinedSamples) {
    const orders = db.prepare(`
      SELECT order_id, campaign_id as camp, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids as products,
             retry_attempt as retry, subscription_id as sub_id,
             acquisition_date as acq_date
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);
    const subs = new Set(orders.map(o => o.sub_id)).size;
    const firstDate = new Date(orders[0].acq_date);
    console.log(`Customer ${customer_id} — ${orders.length} orders, ${subs} subscriptions:`);
    orders.forEach((o, i) => {
      const daysSince = Math.round((new Date(o.acq_date) - firstDate) / 86400000);
      const sl = [2,6,8].includes(o.status) ? 'OK' : 'DECL';
      console.log(`  [${String(i).padStart(2)}] Day ${String(daysSince).padStart(3)} | camp ${String(o.camp).padStart(3)} | ${(o.model||'null').padEnd(22)} | cyc ${String(o.cycle).padStart(2)} ret ${o.retry} | $${String(o.total).padStart(6)} | ${sl.padEnd(4)} | sub:${o.sub_id ? o.sub_id.substring(0,8) : 'null'} | ${o.products}`);
    });
    console.log('');
  }

  // ========================================
  // SCENARIO 3: Customer declined then approved on retry (same day)
  // ========================================
  console.log('\n\n=== SCENARIO 3: First order declined, later order approved (same day or cascade) ===\n');

  const mixedStats = db.prepare(`
    SELECT
      COUNT(DISTINCT customer_id) as customers,
      ROUND(AVG(sub_count), 2) as avg_subs,
      ROUND(AVG(total_orders), 1) as avg_orders,
      SUM(CASE WHEN sub_count = 1 THEN 1 ELSE 0 END) as with_1_sub,
      SUM(CASE WHEN sub_count = 2 THEN 1 ELSE 0 END) as with_2_subs,
      SUM(CASE WHEN sub_count = 3 THEN 1 ELSE 0 END) as with_3_subs,
      SUM(CASE WHEN sub_count = 4 THEN 1 ELSE 0 END) as with_4_subs,
      SUM(CASE WHEN sub_count >= 5 THEN 1 ELSE 0 END) as with_5plus_subs
    FROM (
      SELECT customer_id,
        COUNT(DISTINCT subscription_id) as sub_count,
        COUNT(*) as total_orders
      FROM orders WHERE client_id = 6 AND customer_id IN (
        SELECT customer_id FROM orders WHERE client_id = 6
        AND billing_model_name = 'One Time Purchase'
        GROUP BY customer_id
        HAVING MIN(acquisition_date) >= '2026-01-01'
        AND SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) > 0
        AND MIN(order_id) IN (SELECT order_id FROM orders WHERE client_id = 6 AND order_status = 7)
      )
      GROUP BY customer_id
    )
  `).get();
  console.log('Subscription distribution (first declined then approved, Jan 2026+):');
  console.table(mixedStats);

  // Trace 5 mixed customers
  console.log('\n--- First-declined-then-approved traces ---\n');
  const mixedSamples = db.prepare(`
    SELECT customer_id FROM orders WHERE client_id = 6
    AND billing_model_name = 'One Time Purchase'
    GROUP BY customer_id
    HAVING MIN(acquisition_date) >= '2026-02-01' AND MIN(acquisition_date) < '2026-02-15'
    AND SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) > 0
    AND MIN(order_id) IN (SELECT order_id FROM orders WHERE client_id = 6 AND order_status = 7)
    ORDER BY RANDOM() LIMIT 5
  `).all();

  for (const {customer_id} of mixedSamples) {
    const orders = db.prepare(`
      SELECT order_id, campaign_id as camp, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids as products,
             retry_attempt as retry, subscription_id as sub_id,
             acquisition_date as acq_date
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);
    const subs = new Set(orders.map(o => o.sub_id)).size;
    const firstDate = new Date(orders[0].acq_date);
    console.log(`Customer ${customer_id} — ${orders.length} orders, ${subs} subscriptions:`);
    orders.forEach((o, i) => {
      const daysSince = Math.round((new Date(o.acq_date) - firstDate) / 86400000);
      const sl = [2,6,8].includes(o.status) ? 'OK' : 'DECL';
      console.log(`  [${String(i).padStart(2)}] Day ${String(daysSince).padStart(3)} | camp ${String(o.camp).padStart(3)} | ${(o.model||'null').padEnd(22)} | cyc ${String(o.cycle).padStart(2)} ret ${o.retry} | $${String(o.total).padStart(6)} | ${sl.padEnd(4)} | sub:${o.sub_id ? o.sub_id.substring(0,8) : 'null'} | ${o.products}`);
    });
    console.log('');
  }
})();
