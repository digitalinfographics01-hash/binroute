const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // How many distinct subscriptions per customer?
  console.log('=== Subscriptions per customer ===');
  const subCounts = db.prepare(`
    SELECT sub_count, COUNT(*) as customers FROM (
      SELECT customer_id, COUNT(DISTINCT subscription_id) as sub_count
      FROM orders WHERE client_id = 6 AND subscription_id IS NOT NULL
      GROUP BY customer_id
    ) GROUP BY sub_count ORDER BY sub_count
  `).all();
  console.table(subCounts);

  // Customers with multiple subs — how many store orders do they have?
  console.log('\n=== Multi-sub customers: store orders vs subscriptions ===');
  const multiSub = db.prepare(`
    SELECT customer_id,
      COUNT(DISTINCT subscription_id) as subs,
      SUM(CASE WHEN billing_model_name = 'One Time Purchase' THEN 1 ELSE 0 END) as store_orders,
      SUM(CASE WHEN billing_model_name = '30 Days Subscription' AND billing_cycle = 0 AND order_total = 0 THEN 1 ELSE 0 END) as triggers,
      SUM(CASE WHEN billing_model_name = '30 Days Subscription' AND billing_cycle > 0 THEN 1 ELSE 0 END) as rebills,
      COUNT(*) as total
    FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
    GROUP BY customer_id
    HAVING subs > 1
    ORDER BY subs DESC
    LIMIT 15
  `).all();
  console.table(multiSub);

  // Trace a customer with 2-3 subs to see the flow
  const sample = db.prepare(`
    SELECT customer_id, COUNT(DISTINCT subscription_id) as subs
    FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
    GROUP BY customer_id HAVING subs BETWEEN 2 AND 3
    ORDER BY RANDOM() LIMIT 3
  `).all();

  for (const cust of sample) {
    console.log(`\n--- Customer ${cust.customer_id} (${cust.subs} subscriptions) ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, subscription_id as sub_id,
             acquisition_date as acq_date
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY acquisition_date, billing_cycle
    `).all(cust.customer_id);
    console.table(orders);
  }

  // Key question: do customers with multiple subs share the same store order?
  console.log('\n=== Multi-sub customers: 1 store order linking to multiple subs ===');
  const linking = db.prepare(`
    SELECT
      COUNT(*) as total_multi_sub_customers,
      SUM(CASE WHEN store_orders = 1 THEN 1 ELSE 0 END) as one_store_order,
      SUM(CASE WHEN store_orders > 1 THEN 1 ELSE 0 END) as multiple_store_orders,
      SUM(CASE WHEN store_orders = 0 THEN 1 ELSE 0 END) as no_store_order
    FROM (
      SELECT customer_id,
        COUNT(DISTINCT subscription_id) as subs,
        SUM(CASE WHEN billing_model_name = 'One Time Purchase' THEN 1 ELSE 0 END) as store_orders
      FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
      GROUP BY customer_id HAVING subs > 1
    )
  `).get();
  console.table(linking);
})();
