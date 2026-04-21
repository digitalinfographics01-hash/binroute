const Database = require('better-sqlite3');
const path = require('path');
const db = new Database(path.join(__dirname, '..', 'data', 'binroute.db'), { readonly: true });

const known = new Set([29,73,41,64,77,13,86,88,100,101,102,98,79,82,84, 37,49,7,12, 3,11,60,4, 1,2,72,38,46,9,76,85,80,78,103,141]);

const camps = db.prepare('SELECT DISTINCT campaign_id FROM orders WHERE client_id=6 ORDER BY campaign_id').all().map(r => r.campaign_id);
const unknown = camps.filter(c => !known.has(c));

console.log(`Checking ${unknown.length} unknown campaigns for product names...\n`);

for (const cid of unknown) {
  const stats = db.prepare(`
    SELECT COUNT(*) as orders, ROUND(AVG(order_total),2) as avg_total,
      SUM(CASE WHEN billing_cycle=0 THEN 1 ELSE 0 END) as c0,
      SUM(CASE WHEN billing_cycle>0 THEN 1 ELSE 0 END) as rebills,
      GROUP_CONCAT(DISTINCT billing_model_name) as models
    FROM orders WHERE client_id=6 AND campaign_id=?
  `).get(cid);

  const products = db.prepare(`
    SELECT main_product_id, COUNT(*) as n
    FROM orders WHERE client_id=6 AND campaign_id=? AND main_product_id IS NOT NULL
    GROUP BY main_product_id ORDER BY n DESC LIMIT 5
  `).all(cid);

  // Get product name from products_json
  const sampleOrder = db.prepare(`
    SELECT products_json FROM orders
    WHERE client_id=6 AND campaign_id=? AND products_json IS NOT NULL LIMIT 1
  `).get(cid);

  let productName = '';
  if (sampleOrder && sampleOrder.products_json) {
    try {
      const prods = JSON.parse(sampleOrder.products_json);
      if (Array.isArray(prods) && prods.length > 0) {
        productName = prods[0].name || prods[0].product_name || '';
      }
    } catch (e) {}
  }

  const pids = products.map(p => `${p.main_product_id}(${p.n})`).join(', ');
  console.log(
    `Camp ${String(cid).padStart(3)} | ${String(stats.orders).padStart(5)} ord | avg$${String(stats.avg_total).padStart(7)} | c0:${String(stats.c0).padStart(5)} reb:${String(stats.rebills).padStart(5)} | ${(stats.models || '').substring(0, 30).padEnd(30)} | pids: ${pids.substring(0, 40).padEnd(40)} | ${productName.substring(0, 60)}`
  );
}

db.close();
