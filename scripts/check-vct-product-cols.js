const Database = require('better-sqlite3');
const db = new Database('/opt/binroute/data/binroute.db', { readonly: true });

const r = db.prepare(`
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN products_json IS NOT NULL AND products_json <> '' AND products_json <> '[]' THEN 1 ELSE 0 END) AS with_pj_nonempty,
    SUM(CASE WHEN products_json IS NOT NULL AND products_json <> '' THEN 1 ELSE 0 END) AS with_pj,
    SUM(CASE WHEN product_ids IS NOT NULL AND product_ids <> '' THEN 1 ELSE 0 END) AS with_pids,
    SUM(CASE WHEN main_product_id IS NOT NULL THEN 1 ELSE 0 END) AS with_main_pid,
    SUM(CASE WHEN product_group_name IS NOT NULL AND product_group_name <> '' THEN 1 ELSE 0 END) AS with_group_name
  FROM orders WHERE client_id = 6
`).get();
console.log('client_id=6 product column coverage:');
console.log(r);

console.log('\n--- Sample rows where main_product_id is set ---');
db.prepare(`
  SELECT order_id, main_product_id, upsell_product_id, product_ids,
         CASE WHEN products_json IS NULL THEN '(null)'
              WHEN products_json = '' THEN '(empty)'
              WHEN products_json = '[]' THEN '[] empty array'
              ELSE substr(products_json, 1, 300) END AS pj_preview,
         product_group_name
  FROM orders
  WHERE client_id = 6 AND main_product_id IS NOT NULL
  LIMIT 3
`).all().forEach(row => console.log(JSON.stringify(row, null, 2)));

console.log('\n--- Distinct main_product_id counts (top 20) ---');
db.prepare(`
  SELECT main_product_id, COUNT(*) AS cnt
  FROM orders
  WHERE client_id = 6 AND main_product_id IS NOT NULL
  GROUP BY main_product_id
  ORDER BY cnt DESC
  LIMIT 20
`).all().forEach(r => console.log(`  id=${r.main_product_id}  orders=${r.cnt}`));

// check if any other product-name column exists
console.log('\n--- Searching all columns for ANY product name text ---');
const cols = db.prepare(`PRAGMA table_info(orders)`).all();
console.log('text-ish columns:');
cols.filter(c => c.type === 'TEXT').forEach(c => console.log('  ' + c.name));
