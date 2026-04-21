const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // ========================================
  // PART 1: Campaign 37 only customers (3130)
  // ========================================
  console.log('=== PART 1: Customers who ONLY have campaign 37 orders ===\n');

  const camp37only = db.prepare(`
    SELECT customer_id FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
    GROUP BY customer_id
    HAVING COUNT(DISTINCT campaign_id) = 1
    AND MIN(campaign_id) = 37
    LIMIT 10
  `).all();

  console.log(`Sample of campaign-37-only customers:\n`);
  for (const {customer_id} of camp37only.slice(0, 5)) {
    console.log(`--- Customer ${customer_id} ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id as camp, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids as products,
             is_cascaded as casc, retry_attempt as retry, gateway_id as gw,
             decline_reason, acquisition_date as acq_date
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);
    orders.forEach((o, i) => {
      const sl = [2,6,8].includes(o.status) ? 'OK' : 'DECL';
      console.log(`  [${i}] order ${o.order_id} | camp ${o.camp} | ${(o.model||'null').padEnd(22)} | cyc ${o.cycle} ret ${o.retry} | $${String(o.total).padStart(6)} | ${sl} | gw ${o.gw} | casc ${o.casc} | ${o.products} | ${o.decline_reason || ''}`);
    });
    console.log('');
  }

  // Stats on camp37-only customers
  const camp37onlyStats = db.prepare(`
    SELECT
      COUNT(DISTINCT customer_id) as customers,
      COUNT(*) as total_orders,
      SUM(CASE WHEN billing_cycle = 0 THEN 1 ELSE 0 END) as cycle0,
      SUM(CASE WHEN billing_cycle > 0 THEN 1 ELSE 0 END) as rebills,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      ROUND(AVG(order_total), 2) as avg_total,
      MAX(billing_cycle) as max_cycle
    FROM orders WHERE client_id = 6 AND customer_id IN (
      SELECT customer_id FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
      GROUP BY customer_id HAVING COUNT(DISTINCT campaign_id) = 1 AND MIN(campaign_id) = 37
    )
  `).get();
  console.log('Campaign 37 only — stats:');
  console.table(camp37onlyStats);

  // ========================================
  // PART 2: Campaign 37 with other campaigns
  // ========================================
  console.log('\n\n=== PART 2: Customers who have campaign 37 AND other campaigns ===\n');

  // What other campaigns do camp37 customers have?
  const camp37OtherCamps = db.prepare(`
    SELECT campaign_id, COUNT(DISTINCT customer_id) as customers, COUNT(*) as orders
    FROM orders WHERE client_id = 6 AND customer_id IN (
      SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND campaign_id = 37
    ) AND campaign_id != 37
    GROUP BY campaign_id ORDER BY customers DESC LIMIT 15
  `).all();
  console.log('Other campaigns these customers are on:');
  console.table(camp37OtherCamps);

  // Trace camp37 customers who also have store orders
  console.log('\nSample: camp37 + store orders');
  const camp37withStore = db.prepare(`
    SELECT DISTINCT o1.customer_id
    FROM orders o1
    JOIN orders o2 ON o1.customer_id = o2.customer_id AND o2.client_id = 6 AND o2.billing_model_name = 'One Time Purchase'
    WHERE o1.client_id = 6 AND o1.campaign_id = 37
    ORDER BY RANDOM() LIMIT 5
  `).all();

  for (const {customer_id} of camp37withStore) {
    console.log(`\n--- Customer ${customer_id} ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id as camp, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids as products,
             is_cascaded as casc, retry_attempt as retry, gateway_id as gw,
             decline_reason, acquisition_date as acq_date
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);
    const firstDate = new Date(orders[0].acq_date);
    orders.forEach((o, i) => {
      const daysSince = Math.round((new Date(o.acq_date) - firstDate) / 86400000);
      const sl = [2,6,8].includes(o.status) ? 'OK' : 'DECL';
      console.log(`  [${String(i).padStart(2)}] Day ${String(daysSince).padStart(3)} | order ${o.order_id} | camp ${String(o.camp).padStart(3)} | ${(o.model||'null').padEnd(22)} | cyc ${String(o.cycle).padStart(2)} ret ${o.retry} | $${String(o.total).padStart(6)} | ${sl.padEnd(4)} | gw ${String(o.gw).padStart(3)} | casc ${o.casc} | ${o.products} | ${o.decline_reason || ''}`);
    });
  }

  // ========================================
  // PART 3: Retry/price reduction pattern
  // ========================================
  console.log('\n\n=== PART 3: Retry price reduction pattern ===\n');

  // Within a single subscription, how do retry_attempts work?
  const retryPattern = db.prepare(`
    SELECT billing_cycle, retry_attempt,
      COUNT(*) as orders,
      ROUND(AVG(order_total), 2) as avg_total,
      ROUND(MIN(order_total), 2) as min_total,
      ROUND(MAX(order_total), 2) as max_total,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      ROUND(100.0 * SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) / COUNT(*), 1) as appr_pct
    FROM orders WHERE client_id = 6 AND billing_model_name = '30 Days Subscription'
    AND campaign_id = 2
    GROUP BY billing_cycle, retry_attempt
    ORDER BY billing_cycle, retry_attempt
    LIMIT 30
  `).all();
  console.log('Campaign 2 — cycle x retry x price:');
  console.table(retryPattern);

  // Same for campaign 1
  const retryPattern1 = db.prepare(`
    SELECT billing_cycle, retry_attempt,
      COUNT(*) as orders,
      ROUND(AVG(order_total), 2) as avg_total,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      ROUND(100.0 * SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) / COUNT(*), 1) as appr_pct
    FROM orders WHERE client_id = 6 AND billing_model_name = '30 Days Subscription'
    AND campaign_id = 1
    GROUP BY billing_cycle, retry_attempt
    ORDER BY billing_cycle, retry_attempt
    LIMIT 30
  `).all();
  console.log('\nCampaign 1 — cycle x retry x price:');
  console.table(retryPattern1);

  // Same for campaign 37
  const retryPattern37 = db.prepare(`
    SELECT billing_cycle, retry_attempt,
      COUNT(*) as orders,
      ROUND(AVG(order_total), 2) as avg_total,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      ROUND(100.0 * SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) / COUNT(*), 1) as appr_pct
    FROM orders WHERE client_id = 6 AND billing_model_name = '30 Days Subscription'
    AND campaign_id = 37
    GROUP BY billing_cycle, retry_attempt
    ORDER BY billing_cycle, retry_attempt
    LIMIT 30
  `).all();
  console.log('\nCampaign 37 — cycle x retry x price:');
  console.table(retryPattern37);

  // ========================================
  // PART 4: Do declined initial customers get camp37?
  // ========================================
  console.log('\n\n=== PART 4: Declined initial → what happens next? ===\n');

  // Customers whose ONLY store order was declined — what subscriptions do they get?
  const declinedOnly = db.prepare(`
    SELECT customer_id
    FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
    AND billing_model_name = 'One Time Purchase'
    GROUP BY customer_id
    HAVING SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) = 0
    AND COUNT(*) <= 5
    AND MIN(acquisition_date) >= '2026-01-01'
    ORDER BY RANDOM() LIMIT 5
  `).all();

  console.log('Customers whose ALL store orders were declined:\n');
  for (const {customer_id} of declinedOnly) {
    console.log(`--- Customer ${customer_id} ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id as camp, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids as products,
             is_cascaded as casc, retry_attempt as retry, gateway_id as gw,
             decline_reason, acquisition_date as acq_date
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);
    const firstDate = new Date(orders[0].acq_date);
    orders.forEach((o, i) => {
      const daysSince = Math.round((new Date(o.acq_date) - firstDate) / 86400000);
      const sl = [2,6,8].includes(o.status) ? 'OK' : 'DECL';
      console.log(`  [${String(i).padStart(2)}] Day ${String(daysSince).padStart(3)} | order ${o.order_id} | camp ${String(o.camp).padStart(3)} | ${(o.model||'null').padEnd(22)} | cyc ${String(o.cycle).padStart(2)} ret ${o.retry} | $${String(o.total).padStart(6)} | ${sl.padEnd(4)} | gw ${String(o.gw).padStart(3)} | ${o.products} | ${o.decline_reason || ''}`);
    });
    console.log('');
  }

  // How many declined-only store customers get camp37 vs camp2?
  const declinedFunnel = db.prepare(`
    SELECT
      COUNT(DISTINCT customer_id) as total_all_declined_store,
      COUNT(DISTINCT CASE WHEN customer_id IN (
        SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND campaign_id = 37
      ) THEN customer_id END) as got_camp37,
      COUNT(DISTINCT CASE WHEN customer_id IN (
        SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND campaign_id = 2 AND billing_cycle = 0 AND order_total = 0
      ) THEN customer_id END) as got_camp2_trigger,
      COUNT(DISTINCT CASE WHEN customer_id IN (
        SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND campaign_id NOT IN (37, 2)
        AND billing_model_name = '30 Days Subscription'
      ) THEN customer_id END) as got_other_sub
    FROM (
      SELECT customer_id
      FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
      AND billing_model_name = 'One Time Purchase'
      GROUP BY customer_id
      HAVING SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) = 0
    )
  `).get();
  console.log('All-declined store customers — subscription funnel:');
  console.table(declinedFunnel);

  // Same for approved store customers
  const approvedFunnel = db.prepare(`
    SELECT
      COUNT(DISTINCT customer_id) as total_approved_store,
      COUNT(DISTINCT CASE WHEN customer_id IN (
        SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND campaign_id = 37
      ) THEN customer_id END) as got_camp37,
      COUNT(DISTINCT CASE WHEN customer_id IN (
        SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND campaign_id = 2 AND billing_cycle = 0 AND order_total = 0
      ) THEN customer_id END) as got_camp2_trigger,
      COUNT(DISTINCT CASE WHEN customer_id IN (
        SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND campaign_id NOT IN (37, 2)
        AND billing_model_name = '30 Days Subscription'
      ) THEN customer_id END) as got_other_sub
    FROM (
      SELECT customer_id
      FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
      AND billing_model_name = 'One Time Purchase'
      GROUP BY customer_id
      HAVING SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) > 0
    )
  `).get();
  console.log('\nApproved store customers — subscription funnel:');
  console.table(approvedFunnel);
})();
