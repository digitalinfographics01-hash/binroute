// Extract distinct (product_id, product_name) pairs from VCT orders' products_json
// and populate products_catalog for client_id=6.
// Also computes COGS match coverage (weighted by order count).

const Database = require('better-sqlite3');

const DB_PATH = '/opt/binroute/data/binroute.db';
const CLIENT_ID = 6;

function normName(s) {
  if (s == null) return null;
  return String(s).trim().toLowerCase().replace(/\s+/g, ' ');
}

// Try to pull {id, name} pairs from a products_json blob. Handles common shapes:
//   - array of objects: [{product_id, name|product_name|productName, ...}, ...]
//   - object keyed by id: {"8790": {name, ...}, ...}
//   - nested with "products": { products: [...] }
function extractProducts(jsonStr) {
  if (!jsonStr || jsonStr === '[]' || jsonStr === '{}') return [];
  let parsed;
  try { parsed = JSON.parse(jsonStr); } catch { return []; }

  const out = [];
  const visit = (obj) => {
    if (obj == null) return;
    if (Array.isArray(obj)) {
      for (const item of obj) visit(item);
      return;
    }
    if (typeof obj !== 'object') return;
    const id = obj.product_id ?? obj.productId ?? obj.id;
    const name = obj.product_name ?? obj.productName ?? obj.name ?? obj.title;
    if (id != null && name) {
      const pid = typeof id === 'string' ? id : String(id);
      out.push({ product_id: pid, product_name: String(name).trim() });
    }
    // keyed-by-id shape
    for (const [k, v] of Object.entries(obj)) {
      if (v && typeof v === 'object' && !Array.isArray(v)) {
        const numK = Number(k);
        if (Number.isFinite(numK) && numK > 0) {
          const n = v.product_name ?? v.productName ?? v.name ?? v.title;
          if (n) out.push({ product_id: String(numK), product_name: String(n).trim() });
        } else if (k === 'products' || k === 'items' || k === 'lines') {
          visit(v);
        }
      }
    }
  };
  visit(parsed);
  return out;
}

(async () => {
  const db = new Database(DB_PATH);
  const startedAt = new Date().toISOString();

  // Make sure products_catalog exists (it does per earlier check)
  const cols = db.prepare(`PRAGMA table_info(products_catalog)`).all();
  if (!cols.length) {
    db.exec(`CREATE TABLE products_catalog (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      client_id INTEGER,
      product_id TEXT,
      product_name TEXT,
      last_synced DATETIME
    )`);
  }
  // unique index on (client_id, product_id) for upsert
  try {
    db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_pcat_client_pid ON products_catalog(client_id, product_id)`);
  } catch (e) {
    console.log('Index create note:', e.message);
  }

  // 1. Collect {id->name, count} from all orders with products_json
  console.log(`Scanning VCT orders with products_json...`);
  const t0 = Date.now();
  const stmt = db.prepare(`
    SELECT products_json, main_product_id, upsell_product_id
    FROM orders
    WHERE client_id = ? AND products_json IS NOT NULL AND products_json != '' AND products_json != '[]'
  `);

  const agg = new Map();  // product_id -> { name, count, lastSeenName }
  let ordersScanned = 0, ordersWithProducts = 0, totalProductRows = 0;

  // Iterate as a cursor to avoid holding all rows in memory
  const iter = stmt.iterate(CLIENT_ID);
  for (const row of iter) {
    ordersScanned++;
    const pairs = extractProducts(row.products_json);
    if (pairs.length) ordersWithProducts++;
    for (const p of pairs) {
      totalProductRows++;
      const existing = agg.get(p.product_id);
      if (existing) {
        existing.count++;
        // Prefer longer/non-truncated name
        if (p.product_name && p.product_name.length > (existing.name?.length || 0)) {
          existing.name = p.product_name;
        }
      } else {
        agg.set(p.product_id, { name: p.product_name, count: 1 });
      }
    }
    if (ordersScanned % 200000 === 0) {
      console.log(`  scanned ${ordersScanned} orders, products: ${agg.size} distinct ids, ${((Date.now()-t0)/1000).toFixed(1)}s`);
    }
  }
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`\nScan complete in ${elapsed}s`);
  console.log(`  orders scanned: ${ordersScanned}`);
  console.log(`  orders with parseable products: ${ordersWithProducts}`);
  console.log(`  total product rows extracted: ${totalProductRows}`);
  console.log(`  distinct product_ids: ${agg.size}`);

  if (agg.size === 0) {
    console.log('\nNo products extracted — products_json may use an unexpected shape. Dumping 1 sample:');
    const sample = db.prepare(`SELECT substr(products_json, 1, 1200) AS pj FROM orders WHERE client_id=? AND products_json IS NOT NULL AND products_json != '' AND products_json != '[]' LIMIT 1`).get(CLIENT_ID);
    console.log(sample?.pj);
    return;
  }

  // 2. Upsert into products_catalog
  console.log(`\nUpserting into products_catalog (client_id=${CLIENT_ID})...`);
  const upsert = db.prepare(`
    INSERT INTO products_catalog (client_id, product_id, product_name, last_synced)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(client_id, product_id) DO UPDATE SET
      product_name = excluded.product_name,
      last_synced  = excluded.last_synced
  `);
  const tx = db.transaction(() => {
    for (const [pid, info] of agg) upsert.run(CLIENT_ID, pid, info.name, startedAt);
  });
  tx();
  console.log(`  upserted ${agg.size} products`);

  // 3. COGS match coverage
  console.log(`\n=== COGS match coverage ===`);
  const cogsMap = new Map();
  db.prepare(`SELECT product_name_norm, cogs FROM product_cogs`).all()
    .forEach(r => cogsMap.set(r.product_name_norm, r.cogs));

  let matched = 0, totalOrders = 0, matchedOrders = 0;
  const sampleMiss = [];
  const sampleHit = [];
  const sorted = [...agg.entries()].sort((a, b) => b[1].count - a[1].count);
  for (const [pid, info] of sorted) {
    totalOrders += info.count;
    const n = normName(info.name);
    if (cogsMap.has(n)) {
      matched++;
      matchedOrders += info.count;
      if (sampleHit.length < 5) sampleHit.push({ pid, name: info.name, cogs: cogsMap.get(n), orders: info.count });
    } else if (sampleMiss.length < 15) {
      sampleMiss.push({ pid, name: info.name, orders: info.count });
    }
  }
  const pctProducts = ((matched / agg.size) * 100).toFixed(1);
  const pctOrders = ((matchedOrders / totalOrders) * 100).toFixed(1);
  console.log(`Distinct products:  ${matched} / ${agg.size} matched  (${pctProducts}%)`);
  console.log(`Order-weighted:     ${matchedOrders.toLocaleString()} / ${totalOrders.toLocaleString()} product-rows  (${pctOrders}%)`);

  console.log(`\nTop HITS (by order count):`);
  sampleHit.forEach(s => console.log(`  [${s.orders}x]  id=${s.pid}  $${s.cogs}  ${s.name}`));
  console.log(`\nTop 15 MISSES (highest volume unmatched):`);
  sampleMiss.forEach(s => console.log(`  [${s.orders}x]  id=${s.pid}  ${s.name}`));
})().catch(e => { console.error('FAIL:', e.message); console.error(e.stack); process.exit(1); });
