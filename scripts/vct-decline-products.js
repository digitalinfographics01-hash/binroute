const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // Product 1033 = "Decline Upsell" ($34.95) — used on $0 triggers
  // Product 546 = "Decline Rerun - Copy" ($28.97)
  // Product 683 = "Decline - Rerun" ($28.97)
  // Product 6695 = "Initial Declines SS" ($29.99)
  // Product 1094 = "Rewards (decline runner)" ($29.99)
  // Product 1113 = "Decline Defense" ($34.95)
  // Product 1165 = "ODECLINE" ($29.99)

  const declineProductIds = ['1033', '546', '683', '6695', '1094', '1113', '1165'];

  console.log('=== Orders by decline product ===');
  for (const pid of declineProductIds) {
    const stats = db.prepare(`
      SELECT COUNT(*) as orders,
        SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
        SUM(CASE WHEN billing_cycle = 0 THEN 1 ELSE 0 END) as cycle0,
        SUM(CASE WHEN billing_cycle > 0 THEN 1 ELSE 0 END) as rebills,
        ROUND(AVG(order_total), 2) as avg_total,
        GROUP_CONCAT(DISTINCT campaign_id) as campaigns
      FROM orders WHERE client_id = 6 AND product_ids LIKE '%"${pid}"%'
    `).get();
    console.log(`\nProduct ${pid}: ${stats.orders} orders, ${stats.approved} approved (${(100*stats.approved/stats.orders).toFixed(1)}%), avg $${stats.avg_total}`);
    console.log(`  Cycle 0: ${stats.cycle0}, Rebills: ${stats.rebills}, Campaigns: ${stats.campaigns}`);
  }

  // Trace customers who have "Decline Upsell" (1033) orders
  console.log('\n\n=== Customer lifecycle with Decline Upsell (product 1033) ===');
  const custWith1033 = db.prepare(`
    SELECT DISTINCT customer_id FROM orders
    WHERE client_id = 6 AND product_ids LIKE '%"1033"%' AND customer_id IS NOT NULL
    ORDER BY RANDOM() LIMIT 3
  `).all();

  for (const {customer_id} of custWith1033) {
    console.log(`\n--- Customer ${customer_id} ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids, is_cascaded,
             retry_attempt, subscription_id as sub_id, gateway_id,
             acquisition_date as acq_date, decline_reason
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);
    console.table(orders);
  }

  // Trace customers who have "Decline Rerun" (546) orders
  console.log('\n\n=== Customer lifecycle with Decline Rerun (product 546) ===');
  const custWith546 = db.prepare(`
    SELECT DISTINCT customer_id FROM orders
    WHERE client_id = 6 AND product_ids LIKE '%"546"%' AND customer_id IS NOT NULL
    ORDER BY RANDOM() LIMIT 3
  `).all();

  for (const {customer_id} of custWith546) {
    console.log(`\n--- Customer ${customer_id} ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids, is_cascaded,
             retry_attempt, subscription_id as sub_id, gateway_id,
             acquisition_date as acq_date, decline_reason
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);
    console.table(orders);
  }

  // Trace customers with "Initial Declines SS" (6695) — campaign 37
  console.log('\n\n=== Customer lifecycle with Initial Declines SS (product 6695) ===');
  const custWith6695 = db.prepare(`
    SELECT DISTINCT customer_id FROM orders
    WHERE client_id = 6 AND product_ids LIKE '%"6695"%' AND customer_id IS NOT NULL
    ORDER BY RANDOM() LIMIT 3
  `).all();

  for (const {customer_id} of custWith6695) {
    console.log(`\n--- Customer ${customer_id} ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, product_ids, is_cascaded,
             retry_attempt, subscription_id as sub_id, gateway_id,
             acquisition_date as acq_date, decline_reason
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY order_id
    `).all(customer_id);
    console.table(orders);
  }
})();
