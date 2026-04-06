const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== Campaign 314 — Full product breakdown ===");
  const q1 = querySql(`
    SELECT o.product_group_id, o.product_group_name, o.tx_type, COUNT(*) as cnt,
      MIN(o.acquisition_date) as first_seen
    FROM orders o
    WHERE o.campaign_id = 314 AND o.is_test = 0 AND o.is_internal_test = 0
    GROUP BY o.product_group_id, o.product_group_name, o.tx_type
    ORDER BY o.product_group_name, cnt DESC
  `);
  console.log("  pg_id | product                              | tx_type              | count | first_seen");
  console.log("  " + "-".repeat(100));
  for (const r of q1) {
    console.log("  " + String(r.product_group_id).padStart(5) + " | " +
      (r.product_group_name||"").substring(0,37).padEnd(37) + "| " +
      (r.tx_type||"").padEnd(21) + "| " +
      String(r.cnt).padStart(5) + " | " +
      (r.first_seen||"").substring(0,10));
  }

  console.log();
  console.log("=== Campaign 314 — customer overlap ===");
  const q2 = querySql(`
    SELECT COUNT(DISTINCT customer_id) as total_customers,
      COUNT(DISTINCT CASE WHEN product_group_name LIKE '%Eternal Lumi%' OR product_group_name LIKE '%Derma%' THEN customer_id END) as derma_cust,
      COUNT(DISTINCT CASE WHEN product_group_name LIKE '%E-XceL%' OR product_group_name LIKE '%Excel%' THEN customer_id END) as excel_cust
    FROM orders WHERE campaign_id = 314 AND is_test = 0 AND customer_id IS NOT NULL
  `);
  if (q2.length) {
    console.log("  Total customers:", q2[0].total_customers);
    console.log("  Derma customers:", q2[0].derma_cust);
    console.log("  E-XceL customers:", q2[0].excel_cust);
  }

  console.log();
  console.log("=== products_catalog table status ===");
  try {
    const q3 = querySql("SELECT COUNT(*) as cnt FROM products_catalog");
    console.log("  products_catalog rows:", q3[0].cnt);
    const cols = querySql("PRAGMA table_info(products_catalog)");
    console.log("  columns:", cols.map(c => c.name).join(", "));
  } catch(e) {
    console.log("  " + (e.message.includes("no such table") ? "TABLE DOES NOT EXIST" : e.message));
  }

  console.log();
  console.log("=== All product_group_id → product_group_name mappings ===");
  const q5 = querySql(`
    SELECT product_group_id, product_group_name, COUNT(*) as cnt
    FROM orders WHERE is_test = 0 AND is_internal_test = 0 AND product_group_id IS NOT NULL
    GROUP BY product_group_id ORDER BY cnt DESC
  `);
  for (const r of q5) console.log("  " + String(r.product_group_id).padStart(4) + " | " + (r.product_group_name||"NULL").padEnd(45) + "| " + r.cnt);

  process.exit(0);
}
run();
