const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  console.log('=== Sub trigger (cycle=0, 30 Day Sub) by campaign ===');
  const triggers = db.prepare("SELECT campaign_id, COUNT(*) as cnt, ROUND(AVG(order_total),2) as avg_total, SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved FROM orders WHERE client_id = 6 AND billing_model_name = '30 Days Subscription' AND billing_cycle = 0 GROUP BY campaign_id ORDER BY cnt DESC LIMIT 15").all();
  console.table(triggers);

  console.log('\n=== One Time Purchase by campaign ===');
  const otp = db.prepare("SELECT campaign_id, COUNT(*) as cnt, ROUND(AVG(order_total),2) as avg_total, SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved FROM orders WHERE client_id = 6 AND billing_model_name = 'One Time Purchase' AND billing_cycle = 0 GROUP BY campaign_id ORDER BY cnt DESC LIMIT 15").all();
  console.table(otp);

  console.log('\n=== Price points for sub triggers ===');
  const prices = db.prepare("SELECT ROUND(order_total, 2) as price, COUNT(*) as cnt FROM orders WHERE client_id = 6 AND billing_model_name = '30 Days Subscription' AND billing_cycle = 0 GROUP BY price ORDER BY cnt DESC LIMIT 10").all();
  console.table(prices);

  console.log('\n=== Pure initial-only campaigns (<=10 rebills) ===');
  const pureInitial = db.prepare("SELECT campaign_id, COUNT(*) as orders, ROUND(AVG(order_total),2) as avg_total, SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved FROM orders WHERE client_id = 6 AND campaign_id IN (SELECT campaign_id FROM orders WHERE client_id = 6 GROUP BY campaign_id HAVING SUM(CASE WHEN billing_cycle > 0 THEN 1 ELSE 0 END) <= 10) GROUP BY campaign_id ORDER BY orders DESC LIMIT 15").all();
  console.table(pureInitial);

  console.log('\n=== NULL billing model ===');
  const nullBm = db.prepare("SELECT campaign_id, billing_cycle, COUNT(*) as cnt, ROUND(AVG(order_total),2) as avg_total FROM orders WHERE client_id = 6 AND billing_model_name IS NULL GROUP BY campaign_id, billing_cycle ORDER BY cnt DESC LIMIT 10").all();
  console.table(nullBm);

  console.log('\n=== Offer breakdown ===');
  const offers = db.prepare("SELECT offer_id, offer_name, COUNT(*) as cnt, ROUND(AVG(order_total),2) as avg_total, SUM(CASE WHEN billing_cycle = 0 THEN 1 ELSE 0 END) as cycle0, SUM(CASE WHEN billing_cycle > 0 THEN 1 ELSE 0 END) as rebills FROM orders WHERE client_id = 6 GROUP BY offer_id, offer_name ORDER BY cnt DESC LIMIT 15").all();
  console.table(offers);
})();
