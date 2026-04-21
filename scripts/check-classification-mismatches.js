const Database = require('better-sqlite3');
const path = require('path');
const db = new Database(path.join(__dirname, '..', 'data', 'binroute.db'), { readonly: true });

const UPSELL_CAMPS = new Set([29,73,41,64,77,13,86,88,100,101,102,98,79,82,84,75,90,92,95,97,104,106,108,122,136,138,140,142,144,146,148,152,154,156,158,162,164,166,168,170,172,174,176]);
const DECLINE_SALVAGE_CAMPS = new Set([37,49,7,12]);
const MEMBERSHIP_CAMPS = new Set([3,11,60,4]);
const upsellList = [...UPSELL_CAMPS].join(',');
const salvageList = [...DECLINE_SALVAGE_CAMPS].join(',');
const memberList = [...MEMBERSHIP_CAMPS].join(',');

console.log('=== 1. LOW PRICE (<$15) ON NON-UPSELL CAMPS — potential missed upsells ===\n');
const lowPrice = db.prepare(`
  SELECT campaign_id, main_product_id, COUNT(*) as n, ROUND(AVG(order_total),2) as avg_total
  FROM orders
  WHERE client_id=6 AND is_test_cc=0 AND customer_id IS NOT NULL
    AND billing_cycle=0 AND order_total > 0 AND order_total < 15
    AND campaign_id NOT IN (${upsellList})
    AND campaign_id NOT IN (${salvageList})
    AND campaign_id NOT IN (${memberList})
  GROUP BY campaign_id, main_product_id
  HAVING n >= 3
  ORDER BY n DESC
  LIMIT 20
`).all();
lowPrice.forEach(r => console.log(`  Camp ${r.campaign_id} | pid ${r.main_product_id} | avg$${r.avg_total} | ${r.n} orders`));

console.log('\n=== 2. UPSELL CAMPS WITH REBILLS — are these upsell_rebill or misclassified? ===\n');
const upsellRebills = db.prepare(`
  SELECT campaign_id, COUNT(*) as n, ROUND(AVG(order_total),2) as avg_total
  FROM orders
  WHERE client_id=6 AND is_test_cc=0
    AND campaign_id IN (${upsellList})
    AND billing_cycle > 0
  GROUP BY campaign_id
  HAVING n >= 2
  ORDER BY n DESC
`).all();
console.log(`Total: ${upsellRebills.reduce((s,r)=>s+r.n,0)} rebill orders across ${upsellRebills.length} upsell campaigns`);
upsellRebills.forEach(r => console.log(`  Camp ${r.campaign_id} | ${r.n} rebills | avg$${r.avg_total}`));

console.log('\n=== 3. $0 ORDERS ON UNEXPECTED CAMPS — are these subscription_triggers? ===\n');
const zeroOrders = db.prepare(`
  SELECT campaign_id, billing_cycle, COUNT(*) as n
  FROM orders
  WHERE client_id=6 AND is_test_cc=0 AND order_total=0
    AND campaign_id NOT IN (2, ${salvageList}, ${memberList})
  GROUP BY campaign_id, billing_cycle
  HAVING n >= 3
  ORDER BY n DESC
  LIMIT 15
`).all();
zeroOrders.forEach(r => console.log(`  Camp ${r.campaign_id} | cycle ${r.billing_cycle} | ${r.n} orders at $0`));

console.log('\n=== 4. SALVAGE CAMPS (37,49,7,12) — any with $0 (would be subscription_trigger)? ===\n');
const salvageZero = db.prepare(`
  SELECT campaign_id, COUNT(*) as n
  FROM orders
  WHERE client_id=6 AND is_test_cc=0 AND order_total=0
    AND campaign_id IN (${salvageList})
  GROUP BY campaign_id
`).all();
salvageZero.forEach(r => console.log(`  Camp ${r.campaign_id} | ${r.n} orders at $0`));
if (salvageZero.length === 0) console.log('  None');

console.log('\n=== 5. NON-UPSELL CAMPS selling "Ultimate Protection" product IDs ===\n');
// Get all product IDs that appear on upsell camps
const upsellPids = db.prepare(`
  SELECT DISTINCT main_product_id FROM orders
  WHERE client_id=6 AND campaign_id IN (${upsellList}) AND main_product_id IS NOT NULL
`).all().map(r => r.main_product_id);
const pidSet = new Set(upsellPids);

const nonUpsellWithUpsellProd = db.prepare(`
  SELECT campaign_id, main_product_id, COUNT(*) as n, ROUND(AVG(order_total),2) as avg_total
  FROM orders
  WHERE client_id=6 AND is_test_cc=0
    AND campaign_id NOT IN (${upsellList})
    AND main_product_id IN (${upsellPids.join(',')})
  GROUP BY campaign_id, main_product_id
  HAVING n >= 2
  ORDER BY n DESC
  LIMIT 15
`).all();
nonUpsellWithUpsellProd.forEach(r => console.log(`  Camp ${r.campaign_id} | pid ${r.main_product_id} | avg$${r.avg_total} | ${r.n} orders — UPSELL PRODUCT ON NON-UPSELL CAMP`));

console.log('\n=== 6. SUMMARY: is_test_cc breakdown ===\n');
console.log(db.prepare('SELECT is_test_cc, COUNT(*) as n FROM orders WHERE client_id=6 GROUP BY is_test_cc').all());

db.close();
