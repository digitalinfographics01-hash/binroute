const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== 1. Date range of orders ===");
  const q1 = querySql("SELECT MIN(acquisition_date) as mn, MAX(acquisition_date) as mx, COUNT(*) as total FROM orders");
  console.log("  Min:", q1[0].mn);
  console.log("  Max:", q1[0].mx);
  console.log("  Total:", q1[0].total);

  const q1b = querySql("SELECT MIN(acquisition_date) as mn, MAX(acquisition_date) as mx, COUNT(*) as total FROM orders WHERE is_test = 0 AND is_internal_test = 0");
  console.log("  Non-test: min=" + q1b[0].mn + " max=" + q1b[0].mx + " total=" + q1b[0].total);

  // Monthly breakdown
  const q1c = querySql("SELECT strftime('%Y-%m', acquisition_date) as month, COUNT(*) as cnt FROM orders WHERE is_test = 0 GROUP BY month ORDER BY month");
  console.log("  Monthly:");
  for (const r of q1c) console.log("    " + r.month + ": " + r.cnt);

  console.log();
  console.log("=== 2. Eternal Lumi Deluxe orders ===");
  const q2 = querySql(`
    SELECT campaign_id, product_group_name, COUNT(*) as orders,
      MIN(acquisition_date) as first_order, MAX(acquisition_date) as last_order
    FROM orders
    WHERE product_group_name LIKE '%Deluxe%'
      OR product_group_name LIKE '%ELD%'
      OR product_group_name LIKE '%Eternal Lumi%'
    GROUP BY campaign_id, product_group_name
    ORDER BY orders DESC
  `);
  console.log("  campaign | product                              | orders | first        | last");
  console.log("  " + "-".repeat(95));
  for (const r of q2) {
    console.log("  " + String(r.campaign_id).padStart(8) + " | " +
      (r.product_group_name||"").substring(0,37).padEnd(37) + "| " +
      String(r.orders).padStart(6) + " | " +
      (r.first_order||"").substring(0,10) + " | " +
      (r.last_order||"").substring(0,10));
  }

  console.log();
  console.log("=== 3. Campaigns in DB ===");
  const q3 = querySql("SELECT DISTINCT campaign_id FROM orders ORDER BY campaign_id");
  console.log("  Unique campaign_ids in DB:", q3.length);
  console.log("  IDs:", q3.map(r => r.campaign_id).join(", "));

  // Check campaigns table if exists
  try {
    const q4 = querySql("SELECT campaign_id, campaign_name FROM campaigns ORDER BY campaign_id");
    console.log();
    console.log("  Campaigns table rows:", q4.length);
    if (q4.length > 0 && q4.length <= 60) {
      for (const r of q4) console.log("    " + String(r.campaign_id).padStart(4) + " | " + (r.campaign_name||"").substring(0,50));
    }
  } catch(e) {
    console.log("  Campaigns table: " + (e.message.includes("no such table") ? "does not exist" : e.message));
  }

  process.exit(0);
}
run();
