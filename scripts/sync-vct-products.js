// Sync VCT (client_id=6) product catalog from Sticky's product_index API.
// Reuses src/api/sticky-client.js productIndex() — same logic as POST /api/products/:id/sync route.

const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = '/opt/binroute/data/binroute.db';
const CLIENT_ID = 6;

(async () => {
  const db = new Database(DB_PATH);
  const client = db.prepare('SELECT id, name, sticky_base_url, sticky_username, sticky_password, sticky_domain FROM clients WHERE id = ?').get(CLIENT_ID);
  if (!client) throw new Error('VCT client not found');
  console.log(`Client: id=${client.id} name=${client.name} base_url=${client.sticky_base_url ? 'set' : 'MISSING'} creds=${client.sticky_username ? 'set' : 'MISSING'}`);
  if (!client.sticky_base_url || !client.sticky_username) {
    console.error('Sticky creds missing — cannot sync');
    process.exit(1);
  }

  const StickyClient = require('/opt/binroute/src/api/sticky-client');
  const sticky = new StickyClient({
    baseUrl: client.sticky_base_url,
    username: client.sticky_username,
    password: client.sticky_password,
  });

  console.log('Calling product_index on Sticky...');
  const t0 = Date.now();
  const data = await sticky.productIndex();
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`response_code=${data?.response_code} in ${elapsed}s`);
  if (!data || data.response_code !== '100') {
    console.error('Sticky response not 100:', JSON.stringify(data).slice(0, 500));
    process.exit(1);
  }

  const products = data.products || [];
  console.log(`Received ${products.length} products`);

  const existing = db.prepare('SELECT product_id FROM products_catalog WHERE client_id = ?').all(CLIENT_ID);
  const existingSet = new Set(existing.map(r => String(r.product_id)));

  const insert = db.prepare(`INSERT INTO products_catalog (client_id, product_id, product_name, last_synced) VALUES (?, ?, ?, datetime('now'))`);
  const update = db.prepare(`UPDATE products_catalog SET product_name = ?, last_synced = datetime('now') WHERE client_id = ? AND product_id = ?`);

  let inserted = 0, updated = 0, skipped = 0;
  const tx = db.transaction(() => {
    for (const p of products) {
      if (!p.product_id) { skipped++; continue; }
      const pid = String(p.product_id);
      const name = p.product_name || null;
      if (existingSet.has(pid)) {
        update.run(name, CLIENT_ID, pid);
        updated++;
      } else {
        insert.run(CLIENT_ID, pid, name);
        inserted++;
      }
    }
  });
  tx();

  // update sync_state
  const ss = db.prepare("SELECT 1 FROM sync_state WHERE client_id = ? AND sync_type = 'product_sync'").get(CLIENT_ID);
  if (ss) {
    db.prepare("UPDATE sync_state SET last_sync_at = datetime('now'), records_synced = ?, status = 'complete' WHERE client_id = ? AND sync_type = 'product_sync'").run(products.length, CLIENT_ID);
  } else {
    db.prepare("INSERT INTO sync_state (client_id, sync_type, last_sync_at, records_synced, status) VALUES (?, 'product_sync', datetime('now'), ?, 'complete')").run(CLIENT_ID, products.length);
  }

  console.log(`Inserted: ${inserted} | Updated: ${updated} | Skipped: ${skipped}`);

  // COGS match coverage
  const cogs = db.prepare(`SELECT product_name_norm, cogs FROM product_cogs`).all();
  const cogsMap = new Map();
  cogs.forEach(r => cogsMap.set(r.product_name_norm, r.cogs));
  function norm(s) { return s == null ? null : String(s).trim().toLowerCase().replace(/\s+/g, ' '); }

  const catalog = db.prepare(`SELECT product_id, product_name FROM products_catalog WHERE client_id = ?`).all(CLIENT_ID);
  let match = 0, noname = 0;
  const miss = [];
  for (const p of catalog) {
    if (!p.product_name) { noname++; continue; }
    const n = norm(p.product_name);
    if (cogsMap.has(n)) match++;
    else if (miss.length < 15) miss.push(p);
  }
  console.log(`\n=== Coverage ===`);
  console.log(`Catalog total:     ${catalog.length}`);
  console.log(`  without name:    ${noname}`);
  console.log(`  matched to COGS: ${match} (${((match/catalog.length)*100).toFixed(1)}%)`);
  console.log(`  unmatched:       ${catalog.length - match - noname}`);
  console.log(`\nSample unmatched:`);
  miss.forEach(p => console.log(`  id=${p.product_id}  ${p.product_name}`));
})().catch(e => { console.error('FAIL:', e.message); console.error(e.stack); process.exit(1); });
