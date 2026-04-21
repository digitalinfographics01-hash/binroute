const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // Find customers whose FIRST order is within our data range (recent 3 months)
  // and trace their full lifecycle

  // First: customers whose first order was APPROVED (store purchase)
  console.log('=== APPROVED Day 1 Customers (first order approved, recent 3 months) ===\n');
  const approvedDay1 = db.prepare(`
    SELECT customer_id, MIN(order_id) as first_order_id
    FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
    GROUP BY customer_id
    HAVING MIN(acquisition_date) >= '2026-01-01'
      AND MIN(order_id) IN (
        SELECT order_id FROM orders WHERE client_id = 6 AND order_status IN (2,6,8)
      )
    ORDER BY RANDOM() LIMIT 5
  `).all();

  for (const {customer_id} of approvedDay1) {
    console.log(`\n--- Customer ${customer_id} (Day 1 APPROVED) ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids as products,
             is_cascaded as casc, retry_attempt as retry, gateway_id as gw,
             subscription_id as sub_id, decline_reason,
             acquisition_date as acq_date
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);

    // Show timeline with days since first order
    const firstDate = new Date(orders[0].acq_date);
    orders.forEach((o, i) => {
      const daysSince = Math.round((new Date(o.acq_date) - firstDate) / 86400000);
      const statusLabel = [2,6,8].includes(o.status) ? 'OK' : 'DECL';
      console.log(`  [${String(i).padStart(2)}] Day ${String(daysSince).padStart(3)} | order ${o.order_id} | camp ${String(o.campaign_id).padStart(3)} | ${(o.model||'null').padEnd(22)} | cyc ${String(o.cycle).padStart(2)} ret ${o.retry} | $${String(o.total).padStart(6)} | ${statusLabel.padEnd(4)} | gw ${String(o.gw).padStart(3)} | casc ${o.casc} | products ${o.products} | ${o.decline_reason || ''}`);
    });
  }

  // Second: customers whose first order was DECLINED
  console.log('\n\n=== DECLINED Day 1 Customers (first order declined, recent 3 months) ===\n');
  const declinedDay1 = db.prepare(`
    SELECT customer_id, MIN(order_id) as first_order_id
    FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
    GROUP BY customer_id
    HAVING MIN(acquisition_date) >= '2026-01-01'
      AND MIN(order_id) IN (
        SELECT order_id FROM orders WHERE client_id = 6 AND order_status = 7
      )
    ORDER BY RANDOM() LIMIT 5
  `).all();

  for (const {customer_id} of declinedDay1) {
    console.log(`\n--- Customer ${customer_id} (Day 1 DECLINED) ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids as products,
             is_cascaded as casc, retry_attempt as retry, gateway_id as gw,
             subscription_id as sub_id, decline_reason,
             acquisition_date as acq_date
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);

    const firstDate = new Date(orders[0].acq_date);
    orders.forEach((o, i) => {
      const daysSince = Math.round((new Date(o.acq_date) - firstDate) / 86400000);
      const statusLabel = [2,6,8].includes(o.status) ? 'OK' : 'DECL';
      console.log(`  [${String(i).padStart(2)}] Day ${String(daysSince).padStart(3)} | order ${o.order_id} | camp ${String(o.campaign_id).padStart(3)} | ${(o.model||'null').padEnd(22)} | cyc ${String(o.cycle).padStart(2)} ret ${o.retry} | $${String(o.total).padStart(6)} | ${statusLabel.padEnd(4)} | gw ${String(o.gw).padStart(3)} | casc ${o.casc} | products ${o.products} | ${o.decline_reason || ''}`);
    });
  }

  // Third: what % of customers get a campaign 37 subscription (Initial Declines SS)?
  console.log('\n\n=== Campaign 37 (Initial Declines SS) — who gets it? ===');

  // Customers with campaign 37 — was their first store order approved or declined?
  const camp37customers = db.prepare(`
    SELECT
      COUNT(DISTINCT c37.customer_id) as total_camp37_customers,
      COUNT(DISTINCT CASE WHEN first_status IN (2,6,8) THEN c37.customer_id END) as first_order_approved,
      COUNT(DISTINCT CASE WHEN first_status = 7 THEN c37.customer_id END) as first_order_declined,
      COUNT(DISTINCT CASE WHEN first_status IS NULL THEN c37.customer_id END) as no_other_orders
    FROM (SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND campaign_id = 37) c37
    LEFT JOIN (
      SELECT customer_id, order_status as first_status
      FROM orders WHERE client_id = 6 AND campaign_id != 37
      AND order_id IN (
        SELECT MIN(order_id) FROM orders WHERE client_id = 6 AND campaign_id != 37
        GROUP BY customer_id
      )
    ) first_other ON c37.customer_id = first_other.customer_id
  `).get();
  console.table(camp37customers);

  // Same for campaign 2 ($0 trigger)
  console.log('\n=== Campaign 2 ($0 trigger) — who gets it? ===');
  const camp2customers = db.prepare(`
    SELECT
      COUNT(DISTINCT c2.customer_id) as total_camp2_trigger_customers,
      COUNT(DISTINCT CASE WHEN first_status IN (2,6,8) THEN c2.customer_id END) as first_order_approved,
      COUNT(DISTINCT CASE WHEN first_status = 7 THEN c2.customer_id END) as first_order_declined
    FROM (SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND campaign_id = 2 AND billing_cycle = 0 AND order_total = 0) c2
    LEFT JOIN (
      SELECT customer_id, order_status as first_status
      FROM orders WHERE client_id = 6 AND campaign_id NOT IN (2, 37)
      AND order_id IN (
        SELECT MIN(order_id) FROM orders WHERE client_id = 6 AND campaign_id NOT IN (2, 37)
        GROUP BY customer_id
      )
    ) first_other ON c2.customer_id = first_other.customer_id
  `).get();
  console.table(camp2customers);

  // Time from first order to campaign 37 trigger
  console.log('\n=== Time gap: first order → campaign 37 ===');
  const camp37gap = db.prepare(`
    SELECT
      ROUND(AVG(julianday(c37_date) - julianday(first_date)), 1) as avg_days,
      ROUND(MIN(julianday(c37_date) - julianday(first_date)), 1) as min_days,
      ROUND(MAX(julianday(c37_date) - julianday(first_date)), 1) as max_days,
      COUNT(*) as pairs
    FROM (
      SELECT customer_id, MIN(acquisition_date) as c37_date
      FROM orders WHERE client_id = 6 AND campaign_id = 37 AND billing_cycle = 0
      GROUP BY customer_id
    ) c37
    JOIN (
      SELECT customer_id, MIN(acquisition_date) as first_date
      FROM orders WHERE client_id = 6 AND campaign_id != 37
      GROUP BY customer_id
    ) first ON c37.customer_id = first.customer_id
  `).get();
  console.table(camp37gap);
})();
