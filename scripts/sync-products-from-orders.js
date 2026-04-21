/**
 * Seed products_catalog from order product IDs, then fetch names via Sticky API.
 * Only fetches products that actually appear in orders — not the full 42K catalog.
 */
const { initDb, runSql, querySql } = require('../src/db/connection');
const StickyClient = require('../src/api/sticky-client');
const fs = require('fs');

const CLIENT_ID = 6;
const THROTTLE_MS = 600;
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  await initDb();

  const client = querySql('SELECT * FROM clients WHERE id = ?', [CLIENT_ID])[0];
  const sticky = new StickyClient({
    baseUrl: client.sticky_base_url,
    username: client.sticky_username,
    password: client.sticky_password,
  });

  // Step 1: Get all distinct product IDs from orders not yet in catalog
  console.log('Finding product IDs in orders not yet in catalog...');
  const missing = querySql(`
    SELECT DISTINCT main_product_id as pid FROM orders
    WHERE client_id = ? AND main_product_id IS NOT NULL
      AND CAST(main_product_id AS TEXT) NOT IN (SELECT product_id FROM products_catalog WHERE client_id = ?)
    UNION
    SELECT DISTINCT upsell_product_id as pid FROM orders
    WHERE client_id = ? AND upsell_product_id IS NOT NULL
      AND CAST(upsell_product_id AS TEXT) NOT IN (SELECT product_id FROM products_catalog WHERE client_id = ?)
  `, [CLIENT_ID, CLIENT_ID, CLIENT_ID, CLIENT_ID]).map(r => r.pid);

  console.log(`${missing.length} product IDs need lookup`);

  // Step 2: Fetch each product via v2 API
  let found = 0, notFound = 0, errors = 0;
  const insert = runSql;

  for (let i = 0; i < missing.length; i++) {
    const pid = missing[i];
    try {
      await sleep(THROTTLE_MS);
      const resp = await sticky._post_v2_get(`products/${pid}`);

      if (resp && resp.data) {
        const p = resp.data;
        runSql(`INSERT INTO products_catalog (client_id, product_id, product_name, last_synced)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT(client_id, product_id) DO UPDATE SET product_name=excluded.product_name, last_synced=excluded.last_synced`,
          [CLIENT_ID, String(pid), p.name || null]);
        found++;
      } else {
        // Insert with null name so we don't retry
        runSql(`INSERT OR IGNORE INTO products_catalog (client_id, product_id, product_name, last_synced)
                VALUES (?, ?, NULL, datetime('now'))`, [CLIENT_ID, String(pid)]);
        notFound++;
      }
    } catch (e) {
      if (e.message && e.message.includes('404')) {
        runSql(`INSERT OR IGNORE INTO products_catalog (client_id, product_id, product_name, last_synced)
                VALUES (?, ?, NULL, datetime('now'))`, [CLIENT_ID, String(pid)]);
        notFound++;
      } else {
        errors++;
        if (errors <= 5) console.log(`  Error on product ${pid}: ${e.message}`);
      }
    }

    if ((i + 1) % 100 === 0) {
      console.log(`Progress: ${i + 1}/${missing.length} | found=${found} notFound=${notFound} errors=${errors}`);
    }
  }

  console.log(`\nDone! ${missing.length} products. found=${found} notFound=${notFound} errors=${errors}`);

  // Coverage check
  const total = querySql('SELECT COUNT(DISTINCT main_product_id) as n FROM orders WHERE client_id=? AND main_product_id IS NOT NULL', [CLIENT_ID])[0].n;
  const covered = querySql('SELECT COUNT(*) as n FROM products_catalog WHERE client_id=? AND product_name IS NOT NULL', [CLIENT_ID])[0].n;
  console.log(`Catalog coverage: ${covered} named products / ${total} distinct IDs in orders (${(covered/total*100).toFixed(1)}%)`);
})().catch(e => { console.error('FAIL:', e); process.exit(1); });
