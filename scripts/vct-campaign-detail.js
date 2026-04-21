const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // All campaigns with order stats
  const campaigns = db.prepare(`
    SELECT
      o.campaign_id,
      c.campaign_name,
      COUNT(*) as orders,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
      ROUND(100.0 * SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) / COUNT(*), 1) as approval_pct,
      ROUND(AVG(o.order_total),2) as avg_total,
      ROUND(MIN(o.order_total),2) as min_total,
      ROUND(MAX(o.order_total),2) as max_total,
      SUM(CASE WHEN o.billing_cycle = 0 THEN 1 ELSE 0 END) as cycle0,
      SUM(CASE WHEN o.billing_cycle > 0 THEN 1 ELSE 0 END) as rebills,
      MAX(o.billing_cycle) as max_cycle,
      GROUP_CONCAT(DISTINCT o.billing_model_name) as billing_models,
      GROUP_CONCAT(DISTINCT o.offer_id) as offer_ids,
      MIN(o.acquisition_date) as first_order,
      MAX(o.acquisition_date) as last_order
    FROM orders o
    LEFT JOIN campaigns c ON o.client_id = c.client_id AND o.campaign_id = c.campaign_id
    WHERE o.client_id = 6
    GROUP BY o.campaign_id
    ORDER BY orders DESC
  `).all();

  for (const camp of campaigns) {
    console.log(`\n========== Campaign ${camp.campaign_id} ==========`);
    console.log(`Name: ${camp.campaign_name || '(no name synced)'}`);
    console.log(`Orders: ${camp.orders} (${camp.cycle0} initial, ${camp.rebills} rebills, max cycle ${camp.max_cycle})`);
    console.log(`Approval: ${camp.approval_pct}% (${camp.approved}/${camp.orders})`);
    console.log(`Amount: avg $${camp.avg_total}, range $${camp.min_total}-$${camp.max_total}`);
    console.log(`Billing models: ${camp.billing_models}`);
    console.log(`Offer IDs: ${camp.offer_ids}`);
    console.log(`Active: ${camp.first_order.substring(0,10)} to ${camp.last_order.substring(0,10)}`);

    // Products for this campaign
    const products = db.prepare(`
      SELECT DISTINCT product_ids, product_group_name, product_type_classified, offer_name
      FROM orders
      WHERE client_id = 6 AND campaign_id = ?
      LIMIT 5
    `).all(camp.campaign_id);
    if (products.length > 0) {
      console.log('Products:');
      products.forEach(p => {
        console.log(`  IDs: ${p.product_ids} | Group: ${p.product_group_name} | Type: ${p.product_type_classified} | Offer: ${p.offer_name}`);
      });
    }
  }
})();
