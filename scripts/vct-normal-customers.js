const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // Distribution: how many store orders per customer?
  console.log('=== Store orders per customer ===');
  const storeDist = db.prepare(`
    SELECT store_orders, COUNT(*) as customers FROM (
      SELECT customer_id,
        SUM(CASE WHEN billing_model_name = 'One Time Purchase' THEN 1 ELSE 0 END) as store_orders
      FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
      GROUP BY customer_id
    ) GROUP BY store_orders ORDER BY store_orders LIMIT 20
  `).all();
  console.table(storeDist);

  // Customers with reasonable order counts (1-5 subs, 1-3 store orders)
  console.log('\n=== Normal customers (1-3 store orders, 1-3 subs) ===');
  const normal = db.prepare(`
    SELECT customer_id,
      COUNT(DISTINCT subscription_id) as subs,
      SUM(CASE WHEN billing_model_name = 'One Time Purchase' THEN 1 ELSE 0 END) as store_orders,
      SUM(CASE WHEN billing_model_name = '30 Days Subscription' AND billing_cycle = 0 AND order_total = 0 THEN 1 ELSE 0 END) as triggers,
      SUM(CASE WHEN billing_model_name = '30 Days Subscription' AND billing_cycle > 0 THEN 1 ELSE 0 END) as rebills,
      COUNT(*) as total
    FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
    GROUP BY customer_id
    HAVING store_orders BETWEEN 1 AND 3 AND subs BETWEEN 1 AND 3
    ORDER BY RANDOM() LIMIT 5
  `).all();
  console.table(normal);

  // Trace 3 normal customers
  for (const cust of normal.slice(0, 3)) {
    console.log(`\n--- Customer ${cust.customer_id} (${cust.store_orders} store, ${cust.triggers} triggers, ${cust.rebills} rebills) ---`);
    const orders = db.prepare(`
      SELECT order_id, campaign_id, billing_model_name as model, billing_cycle as cycle,
             order_total as total, order_status as status, subscription_id as sub_id,
             acquisition_date as acq_date
      FROM orders WHERE client_id = 6 AND customer_id = ?
      ORDER BY acquisition_date, order_id
    `).all(cust.customer_id);
    console.table(orders);
  }

  // How many customers have >10 store orders? Likely test accounts
  console.log('\n=== Potential test accounts (>10 store orders) ===');
  const testAccounts = db.prepare(`
    SELECT COUNT(*) as customers,
      SUM(total) as total_orders
    FROM (
      SELECT customer_id,
        SUM(CASE WHEN billing_model_name = 'One Time Purchase' THEN 1 ELSE 0 END) as store_orders,
        COUNT(*) as total
      FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
      GROUP BY customer_id
      HAVING store_orders > 10
    )
  `).get();
  console.log(testAccounts);

  // vs normal customers
  const normalAccounts = db.prepare(`
    SELECT COUNT(*) as customers,
      SUM(total) as total_orders
    FROM (
      SELECT customer_id,
        SUM(CASE WHEN billing_model_name = 'One Time Purchase' THEN 1 ELSE 0 END) as store_orders,
        COUNT(*) as total
      FROM orders WHERE client_id = 6 AND customer_id IS NOT NULL
      GROUP BY customer_id
      HAVING store_orders BETWEEN 1 AND 10
    )
  `).get();
  console.log('Normal (1-10 store orders):', normalAccounts);
})();
