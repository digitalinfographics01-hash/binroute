const Database = require("better-sqlite3");

async function run() {
  const db = new Database("./data/binroute.db");
  db.pragma('busy_timeout = 10000');

  console.log("=== MAPPING PRODUCT GROUPS TO ORDERS ===");

  // Build lookup: product_id → { product_group_id, product_type, group_name }
  const assignments = db.prepare(`
    SELECT pga.product_id, pga.product_group_id, pga.product_type, pg.group_name
    FROM product_group_assignments pga
    LEFT JOIN product_groups pg ON pga.product_group_id = pg.id AND pga.client_id = pg.client_id
  `).all();

  const lookup = new Map();
  for (const r of assignments) {
    lookup.set(String(r.product_id), { pgId: r.product_group_id, ptype: r.product_type, pgName: r.group_name });
  }
  console.log("Product assignments loaded:", lookup.size);

  // Get all orders with NULL product_group_id
  const nullOrders = db.prepare(`
    SELECT id, product_ids FROM orders
    WHERE product_group_id IS NULL AND is_test = 0 AND product_ids IS NOT NULL AND product_ids != ''
  `).all();

  let mapped = 0, unmapped = 0;
  const unmappedPids = new Set();

  const updateStmt = db.prepare("UPDATE orders SET product_group_id = ?, product_group_name = ?, product_type_classified = ? WHERE id = ?");

  const runMapping = db.transaction(() => {
    for (const row of nullOrders) {
      const { id, product_ids: productIdsRaw } = row;
      // Parse product_ids: format is '["242"]' or '["242","243"]'
      let pids;
      try {
        pids = JSON.parse(productIdsRaw);
      } catch {
        pids = [productIdsRaw.replace(/[^0-9]/g, '')];
      }

      // Use first product_id for group mapping
      const pid = String(pids[0] || '');
      const match = lookup.get(pid);

      if (match) {
        updateStmt.run(match.pgId, match.pgName || '', match.ptype || '', id);
        mapped++;
      } else {
        unmapped++;
        unmappedPids.add(pid);
      }
    }
  });
  runMapping();

  console.log("Mapped:", mapped);
  console.log("Unmapped:", unmapped);
  if (unmappedPids.size > 0) {
    console.log("Unmapped product_ids:", [...unmappedPids].slice(0, 20).join(", "));
  }

  // Now tag offers on newly mapped orders
  console.log();
  console.log("=== TAGGING OFFERS ON NEWLY MAPPED ===");
  db.exec(`UPDATE orders SET offer_name = 'Skin' WHERE offer_name IS NULL AND (
    product_group_name LIKE '%Eternal Lumi%' OR product_group_name LIKE '%Derma Lumiere%'
    OR product_group_name LIKE '%Derma La Fleur%' OR product_group_name LIKE '%Glo Vous Derm%'
  )`);
  const skin = db.prepare("SELECT changes()").get()["changes()"];
  console.log("  Skin:", skin);

  db.exec(`UPDATE orders SET offer_name = 'Male Enhancement' WHERE offer_name IS NULL AND (
    product_group_name LIKE '%E-XceL%' OR product_group_name LIKE '%Excel%'
    OR product_group_name LIKE '%ViraFlexx%' OR product_group_name LIKE '%SS shipping%'
  )`);
  const male = db.prepare("SELECT changes()").get()["changes()"];
  console.log("  Male Enhancement:", male);

  db.exec("UPDATE orders SET offer_name = 'Immunis' WHERE offer_name IS NULL AND product_group_name LIKE '%Erecovery%'");
  const imm = db.prepare("SELECT changes()").get()["changes()"];
  console.log("  Immunis:", imm);

  console.log("  Saved.");

  // Final check
  console.log();
  console.log("=== FINAL STATE ===");
  const nullPg = db.prepare("SELECT COUNT(*) as cnt FROM orders WHERE product_group_id IS NULL AND is_test=0 AND is_internal_test=0").get().cnt;
  const nullOffer = db.prepare("SELECT COUNT(*) as cnt FROM orders WHERE offer_name IS NULL AND is_test=0 AND is_internal_test=0").get().cnt;
  console.log("  NULL product_group_id:", nullPg);
  console.log("  NULL offer_name:", nullOffer);

  const offers = db.prepare("SELECT COALESCE(offer_name,'UNTAGGED') as o, COUNT(*) as c FROM orders WHERE is_test=0 AND is_internal_test=0 GROUP BY offer_name ORDER BY c DESC").all();
  for (const r of offers) console.log("  " + String(r.o).padEnd(20) + r.c);

  db.pragma('wal_checkpoint(TRUNCATE)');
  console.log("  WAL checkpoint done.");
  db.close();
}
run().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
