const Database = require("better-sqlite3");

async function run() {
  const db = new Database("./data/binroute.db");

  // Add offer_name column if not exists
  const cols = db.prepare("PRAGMA table_info(orders)").all();
  const hasOffer = cols.some(r => r.name === "offer_name");
  if (!hasOffer) {
    db.exec("ALTER TABLE orders ADD COLUMN offer_name TEXT DEFAULT NULL");
    console.log("Added offer_name column to orders");
  } else {
    console.log("offer_name column already exists");
  }

  // Skin offers
  db.exec(`UPDATE orders SET offer_name = 'Skin' WHERE offer_name IS NULL AND (
    product_group_name LIKE '%Eternal Lumi%'
    OR product_group_name LIKE '%Derma Lumiere%'
    OR product_group_name LIKE '%Derma La Fleur%'
    OR product_group_name LIKE '%Glo Vous Derm%'
  )`);
  const skinCount = db.prepare("SELECT changes()").get()["changes()"];
  console.log("Tagged Skin:", skinCount);

  // Male Enhancement offers (E-XceL, ViraFlexx, Erecovery, SS shipping)
  db.exec(`UPDATE orders SET offer_name = 'Male Enhancement' WHERE offer_name IS NULL AND (
    product_group_name LIKE '%E-XceL%'
    OR product_group_name LIKE '%Excel%'
    OR product_group_name LIKE '%ViraFlexx%'
    OR product_group_name LIKE '%Erecovery%'
    OR product_group_name LIKE '%SS shipping%'
  )`);
  const maleCount = db.prepare("SELECT changes()").get()["changes()"];
  console.log("Tagged Male Enhancement:", maleCount);

  console.log("Saved.");

  // Verify
  console.log();
  console.log("=== Results ===");
  const q = db.prepare(`SELECT COALESCE(offer_name, 'UNTAGGED') as offer, COUNT(*) as cnt
    FROM orders WHERE is_test = 0 AND is_internal_test = 0 GROUP BY offer_name ORDER BY cnt DESC`).all();
  for (const r of q) console.log("  " + String(r.offer).padEnd(20) + " " + r.cnt);

  // Show untagged
  const untagged = db.prepare(`SELECT DISTINCT product_group_name, COUNT(*) as cnt
    FROM orders WHERE offer_name IS NULL AND is_test = 0 AND is_internal_test = 0
    GROUP BY product_group_name ORDER BY cnt DESC`).all();
  if (untagged.length > 0) {
    console.log();
    console.log("Untagged products:");
    for (const r of untagged) console.log("  " + (r.product_group_name||"NULL").padEnd(45) + " " + r.cnt);
  } else {
    console.log("\nAll orders tagged. Zero untagged.");
  }

  // Show per-product breakdown
  console.log();
  console.log("=== Per-product tagging ===");
  const q2 = db.prepare(`SELECT product_group_name, offer_name, COUNT(*) as cnt
    FROM orders WHERE is_test = 0 AND is_internal_test = 0
    GROUP BY product_group_name, offer_name ORDER BY offer_name, cnt DESC`).all();
  for (const r of q2) console.log("  " + (r.offer_name||"UNTAGGED").padEnd(18) + " | " + (r.product_group_name||"NULL").padEnd(45) + " | " + r.cnt);

  db.close();
}
run();
