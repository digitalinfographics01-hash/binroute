const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // Check if products table exists
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='products'").all();
  if (tables.length > 0) {
    const productCount = db.prepare("SELECT COUNT(*) as cnt FROM products WHERE client_id = 6").get();
    console.log('Products in DB for client 6:', productCount.cnt);
  } else {
    console.log('No products table exists. Checking products_json from orders instead...');
  }

  // Look at distinct product_ids from orders
  console.log('\n=== Distinct product IDs from subscription orders (30 Days Sub) ===');
  const subProducts = db.prepare(`
    SELECT DISTINCT product_ids
    FROM orders WHERE client_id = 6 AND billing_model_name = '30 Days Subscription' AND billing_cycle = 0
    LIMIT 30
  `).all();
  console.table(subProducts);

  // Look at products_json for subscription triggers
  console.log('\n=== Products JSON from $0 triggers (sample) ===');
  const triggerProducts = db.prepare(`
    SELECT order_id, product_ids, products_json, campaign_id, order_total
    FROM orders WHERE client_id = 6 AND billing_model_name = '30 Days Subscription' AND billing_cycle = 0 AND order_total = 0
    LIMIT 5
  `).all();
  for (const o of triggerProducts) {
    console.log(`\nOrder ${o.order_id} (campaign ${o.campaign_id}, $${o.order_total}):`);
    console.log('  product_ids:', o.product_ids);
    if (o.products_json) {
      try {
        const products = JSON.parse(o.products_json);
        products.forEach(p => {
          console.log(`  Product ${p.product_id}: ${p.name || p.product_name || 'NO NAME'} — $${p.price || p.product_price || '?'}`);
        });
      } catch(e) {
        console.log('  (could not parse products_json)');
      }
    } else {
      console.log('  (no products_json)');
    }
  }

  // Look at products_json from store orders
  console.log('\n=== Products JSON from store orders (sample) ===');
  const storeProducts = db.prepare(`
    SELECT order_id, product_ids, products_json, campaign_id, order_total
    FROM orders WHERE client_id = 6 AND billing_model_name = 'One Time Purchase'
    ORDER BY RANDOM() LIMIT 5
  `).all();
  for (const o of storeProducts) {
    console.log(`\nOrder ${o.order_id} (campaign ${o.campaign_id}, $${o.order_total}):`);
    console.log('  product_ids:', o.product_ids);
    if (o.products_json) {
      try {
        const products = JSON.parse(o.products_json);
        products.forEach(p => {
          console.log(`  Product ${p.product_id}: ${p.name || p.product_name || 'NO NAME'} — $${p.price || p.product_price || '?'}`);
        });
      } catch(e) {
        console.log('  (could not parse products_json)');
      }
    } else {
      console.log('  (no products_json)');
    }
  }

  // Look at products_json from rebill orders
  console.log('\n=== Products JSON from rebills (sample) ===');
  const rebillProducts = db.prepare(`
    SELECT order_id, product_ids, products_json, campaign_id, order_total, billing_cycle
    FROM orders WHERE client_id = 6 AND billing_model_name = '30 Days Subscription' AND billing_cycle > 0
    ORDER BY RANDOM() LIMIT 5
  `).all();
  for (const o of rebillProducts) {
    console.log(`\nOrder ${o.order_id} (campaign ${o.campaign_id}, $${o.order_total}, cycle ${o.billing_cycle}):`);
    console.log('  product_ids:', o.product_ids);
    if (o.products_json) {
      try {
        const products = JSON.parse(o.products_json);
        products.forEach(p => {
          console.log(`  Product ${p.product_id}: ${p.name || p.product_name || 'NO NAME'} — $${p.price || p.product_price || '?'}`);
        });
      } catch(e) {
        console.log('  (could not parse products_json)');
      }
    } else {
      console.log('  (no products_json)');
    }
  }

  // Search for products with "reward" or "decline" in name
  console.log('\n=== Products with reward/decline in name ===');
  const rewardProducts = db.prepare(`
    SELECT DISTINCT products_json FROM orders
    WHERE client_id = 6 AND products_json LIKE '%reward%'
    LIMIT 10
  `).all();
  console.log('With "reward":', rewardProducts.length);
  rewardProducts.forEach(r => {
    try { JSON.parse(r.products_json).forEach(p => console.log(`  ${p.product_id}: ${p.name || p.product_name}`)); } catch {}
  });

  const declineProducts = db.prepare(`
    SELECT DISTINCT products_json FROM orders
    WHERE client_id = 6 AND products_json LIKE '%decline%'
    LIMIT 10
  `).all();
  console.log('\nWith "decline":', declineProducts.length);
  declineProducts.forEach(r => {
    try { JSON.parse(r.products_json).forEach(p => console.log(`  ${p.product_id}: ${p.name || p.product_name}`)); } catch {}
  });

  // Also check for "sub" or "subscription" or "trial" or "recur" in product names
  const subNames = db.prepare(`
    SELECT DISTINCT products_json FROM orders
    WHERE client_id = 6 AND (products_json LIKE '%subscri%' OR products_json LIKE '%trial%' OR products_json LIKE '%recur%' OR products_json LIKE '%rebill%')
    LIMIT 10
  `).all();
  console.log('\nWith "subscri/trial/recur/rebill":', subNames.length);
  subNames.forEach(r => {
    try { JSON.parse(r.products_json).forEach(p => console.log(`  ${p.product_id}: ${p.name || p.product_name}`)); } catch {}
  });
})();
