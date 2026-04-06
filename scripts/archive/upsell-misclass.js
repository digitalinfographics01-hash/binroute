const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== UPSELL MISCLASSIFICATION DIAGNOSTIC ===");
  console.log("Definition: same customer, different product_group, within 30 min of a cp_initial");
  console.log();

  // 1. Total count
  const q1 = querySql(`
    SELECT COUNT(*) as cnt FROM orders o1
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL AND o1.product_group_id IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM orders o2
      WHERE o2.customer_id = o1.customer_id
      AND o2.tx_type = 'cp_initial'
      AND o2.product_group_id != o1.product_group_id
      AND o2.product_group_id IS NOT NULL
      AND o2.acquisition_date <= o1.acquisition_date
      AND (julianday(o1.acquisition_date) - julianday(o2.acquisition_date)) * 1440 <= 30
      AND (julianday(o1.acquisition_date) - julianday(o2.acquisition_date)) * 1440 > 0
      AND o2.order_id != o1.order_id
      AND o2.is_test=0
    )
  `);
  console.log("1. Total misclassified upsells (30-min window):", q1[0].cnt);

  // Count with NULL product_group
  const q1b = querySql(`
    SELECT COUNT(*) as cnt FROM orders o1
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL AND o1.product_group_id IS NULL
  `);
  console.log("   cp_initial with NULL product_group_id:", q1b[0].cnt, "(cannot evaluate)");

  // 2. Sample 10
  console.log();
  console.log("2. Sample 10:");
  const q2 = querySql(`
    SELECT o1.order_id as oid, o1.tx_type as tx, o1.customer_id as cust,
      o1.product_group_id as pgid, o1.product_group_name as pg,
      o1.acquisition_date as dt,
      o2.order_id as anchor_oid, o2.product_group_id as anchor_pgid,
      o2.product_group_name as anchor_pg,
      ROUND((julianday(o1.acquisition_date) - julianday(o2.acquisition_date)) * 1440, 1) as min_after
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.tx_type = 'cp_initial'
      AND o2.product_group_id != o1.product_group_id
      AND o2.product_group_id IS NOT NULL
      AND o2.acquisition_date <= o1.acquisition_date
      AND (julianday(o1.acquisition_date) - julianday(o2.acquisition_date)) * 1440 <= 30
      AND (julianday(o1.acquisition_date) - julianday(o2.acquisition_date)) * 1440 > 0
      AND o2.order_id != o1.order_id AND o2.is_test=0
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL AND o1.product_group_id IS NOT NULL
    GROUP BY o1.order_id
    ORDER BY o1.acquisition_date DESC
    LIMIT 10
  `);
  console.log("  oid    | tx         | cust  | pgid | product                     | anchor_oid | anchor_pg                   | min_after");
  console.log("  " + "-".repeat(120));
  for (const r of q2) {
    console.log("  " + String(r.oid).padStart(6) + " | " + (r.tx||"").padEnd(10) + " | " + String(r.cust).padStart(5) + " | " +
      String(r.pgid).padStart(4) + " | " + (r.pg||"").substring(0,27).padEnd(27) + " | " +
      String(r.anchor_oid).padStart(10) + " | " + (r.anchor_pg||"").substring(0,27).padEnd(27) + " | " +
      r.min_after + " min");
  }

  // 4. Product groups appearing as upsells
  console.log();
  console.log("4. Most common upsell product groups:");
  const q4 = querySql(`
    SELECT o1.product_group_name, COUNT(DISTINCT o1.order_id) as cnt
    FROM orders o1
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL AND o1.product_group_id IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM orders o2
      WHERE o2.customer_id = o1.customer_id
      AND o2.tx_type = 'cp_initial'
      AND o2.product_group_id != o1.product_group_id
      AND o2.product_group_id IS NOT NULL
      AND o2.acquisition_date <= o1.acquisition_date
      AND (julianday(o1.acquisition_date) - julianday(o2.acquisition_date)) * 1440 <= 30
      AND (julianday(o1.acquisition_date) - julianday(o2.acquisition_date)) * 1440 > 0
      AND o2.order_id != o1.order_id AND o2.is_test=0
    )
    GROUP BY o1.product_group_name ORDER BY cnt DESC
  `);
  for (const r of q4) console.log("  " + (r.product_group_name||"NULL").padEnd(45) + r.cnt);

  process.exit(0);
}
run();
