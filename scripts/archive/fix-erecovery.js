const Database = require("better-sqlite3");

async function run() {
  const db = new Database("./data/binroute.db");

  db.exec("UPDATE orders SET offer_name = 'Immunis' WHERE product_group_name LIKE '%Erecovery%'");
  const cnt = db.prepare("SELECT changes()").get()["changes()"];
  console.log("Retagged Erecovery → Immunis:", cnt);

  const q = db.prepare(`SELECT COALESCE(offer_name, 'UNTAGGED') as offer, COUNT(*) as cnt
    FROM orders WHERE is_test = 0 AND is_internal_test = 0 GROUP BY offer_name ORDER BY cnt DESC`).all();
  for (const r of q) console.log("  " + String(r.offer).padEnd(20) + " " + r.cnt);

  db.close();
}
run();
