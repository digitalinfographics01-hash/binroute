const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // Sample customers who have both a store order and a $0 trigger
  console.log('=== Customer flow: store order → $0 trigger → rebills ===');
  const customers = db.prepare(`
    SELECT customer_id, COUNT(*) as total_orders,
      SUM(CASE WHEN billing_model_name = 'One Time Purchase' THEN 1 ELSE 0 END) as otp_orders,
      SUM(CASE WHEN billing_model_name = '30 Days Subscription' AND billing_cycle = 0 AND order_total = 0 THEN 1 ELSE 0 END) as triggers,
      SUM(CASE WHEN billing_model_name = '30 Days Subscription' AND billing_cycle > 0 THEN 1 ELSE 0 END) as rebills
    FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
    GROUP BY customer_id
    HAVING otp_orders > 0 AND triggers > 0 AND rebills > 0
    LIMIT 10
  `).all();
  console.table(customers);

  // Trace a few of these customers
  for (const cust of customers.slice(0, 3)) {
    console.log(`\n--- Customer ${cust.customer_id} (${cust.total_orders} orders) ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id, billing_model_name, billing_cycle, order_total,
             order_status, acquisition_date, subscription_id
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY acquisition_date
    `).all(cust.customer_id);
    console.table(orders);
  }

  // How many customers have BOTH store + trigger?
  console.log('\n=== Customer overlap stats ===');
  const stats = db.prepare(`
    SELECT
      COUNT(DISTINCT customer_id) as total_customers,
      COUNT(DISTINCT CASE WHEN billing_model_name = 'One Time Purchase' THEN customer_id END) as has_store_order,
      COUNT(DISTINCT CASE WHEN billing_model_name = '30 Days Subscription' AND billing_cycle = 0 AND order_total = 0 THEN customer_id END) as has_trigger,
      COUNT(DISTINCT CASE WHEN billing_model_name = '30 Days Subscription' AND billing_cycle > 0 THEN customer_id END) as has_rebills
    FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
  `).get();
  console.table(stats);

  // How many trigger customers also have a store order?
  const overlap = db.prepare(`
    SELECT COUNT(DISTINCT t.customer_id) as trigger_with_store
    FROM (SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND billing_model_name = '30 Days Subscription' AND billing_cycle = 0 AND order_total = 0) t
    JOIN (SELECT DISTINCT customer_id FROM orders WHERE client_id = 6 AND billing_model_name = 'One Time Purchase') s
    ON t.customer_id = s.customer_id
  `).get();
  console.log('Trigger customers who also have a store order:', overlap.trigger_with_store);

  // Time gap between store order and trigger
  console.log('\n=== Time gap: store order → trigger ===');
  const gaps = db.prepare(`
    SELECT
      ROUND(AVG(julianday(t.trigger_date) - julianday(s.store_date)), 1) as avg_days,
      ROUND(MIN(julianday(t.trigger_date) - julianday(s.store_date)), 1) as min_days,
      ROUND(MAX(julianday(t.trigger_date) - julianday(s.store_date)), 1) as max_days,
      COUNT(*) as pairs
    FROM (
      SELECT customer_id, MIN(acquisition_date) as trigger_date
      FROM orders WHERE client_id = 6 AND billing_model_name = '30 Days Subscription' AND billing_cycle = 0 AND order_total = 0
      GROUP BY customer_id
    ) t
    JOIN (
      SELECT customer_id, MIN(acquisition_date) as store_date
      FROM orders WHERE client_id = 6 AND billing_model_name = 'One Time Purchase'
      GROUP BY customer_id
    ) s ON t.customer_id = s.customer_id
  `).get();
  console.table(gaps);
})();
