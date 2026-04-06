const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== CAMPAIGN-BASED RETRY DETECTION ===");
  console.log("Filter: billing_cycle=0, is_recurring=0, ptype in (initial, initial_rebill)");
  console.log("        customer_id NOT NULL, prior DECLINED order same customer + same product_id");
  console.log();

  // Note: product_ids is stored as JSON string like '["243"]' — need to handle that
  // Check what product_ids looks like
  const sample = querySql("SELECT product_ids FROM orders WHERE product_ids IS NOT NULL AND product_ids != '' LIMIT 3");
  console.log("product_ids format sample:", sample.map(r => r.product_ids).join(" | "));
  console.log();

  // 1 & 2: Same vs different campaign_id
  console.log("=== 1 & 2: Same vs different campaign_id ===");
  const q1 = querySql(`
    SELECT
      CASE WHEN o1.campaign_id = o2.campaign_id THEN 'same_campaign' ELSE 'diff_campaign' END as group_type,
      COUNT(*) as cnt,
      ROUND(AVG(julianday(o1.acquisition_date) - julianday(o2.acquisition_date)), 2) as avg_days,
      COUNT(CASE WHEN o1.order_status IN (2,6,8) THEN 1 END) as approved,
      ROUND(COUNT(CASE WHEN o1.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.product_ids = o1.product_ids
      AND o2.order_status = 7
      AND o2.acquisition_date < o1.acquisition_date
      AND o2.order_id != o1.order_id
      AND o2.is_test = 0
    WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
    AND o1.product_type_classified IN ('initial', 'initial_rebill')
    AND o1.customer_id IS NOT NULL
    AND o1.is_test = 0 AND o1.is_internal_test = 0
    GROUP BY group_type
  `);
  console.log("  group          | count  | avg days | approved | rate");
  console.log("  " + "-".repeat(55));
  for (const r of q1) console.log("  " + r.group_type.padEnd(17) + "| " + String(r.cnt).padStart(6) + " | " + String(r.avg_days).padStart(8) + " | " + String(r.approved).padStart(8) + " | " + (r.rate||0) + "%");

  // 3: Same vs different product_id between retry and prior decline
  console.log();
  console.log("=== 3: Product ID match ===");
  const q3 = querySql(`
    SELECT
      CASE WHEN o1.product_ids = o2.product_ids THEN 'same_product' ELSE 'diff_product' END as match,
      CASE WHEN o1.campaign_id = o2.campaign_id THEN 'same_camp' ELSE 'diff_camp' END as camp_match,
      COUNT(*) as cnt
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.order_status = 7
      AND o2.acquisition_date < o1.acquisition_date
      AND o2.order_id != o1.order_id
      AND o2.is_test = 0
      AND o2.product_type_classified IN ('initial', 'initial_rebill')
    WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
    AND o1.product_type_classified IN ('initial', 'initial_rebill')
    AND o1.customer_id IS NOT NULL
    AND o1.is_test = 0 AND o1.is_internal_test = 0
    AND julianday(o1.acquisition_date) - julianday(o2.acquisition_date) <= 30
    GROUP BY match, camp_match
    ORDER BY cnt DESC
  `);
  console.log("  product_match | campaign_match | count");
  console.log("  " + "-".repeat(45));
  for (const r of q3) console.log("  " + r.match.padEnd(16) + "| " + r.camp_match.padEnd(15) + "| " + r.cnt);

  // 4: Samples — same campaign
  console.log();
  console.log("=== 4a: Sample 5 — SAME campaign retries ===");
  const q4a = querySql(`
    SELECT o1.order_id as oid, o1.product_ids as pid, o1.campaign_id as camp,
      o1.order_status as status,
      o2.order_id as prior_oid, o2.campaign_id as prior_camp, o2.product_ids as prior_pid,
      ROUND(julianday(o1.acquisition_date) - julianday(o2.acquisition_date), 2) as days
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.product_ids = o1.product_ids
      AND o2.campaign_id = o1.campaign_id
      AND o2.order_status = 7
      AND o2.acquisition_date < o1.acquisition_date
      AND o2.order_id != o1.order_id AND o2.is_test = 0
    WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
    AND o1.product_type_classified IN ('initial', 'initial_rebill')
    AND o1.customer_id IS NOT NULL AND o1.is_test = 0
    LIMIT 5
  `);
  console.log("  oid    | product_ids | camp | status | prior_oid | prior_camp | prior_pid   | days");
  console.log("  " + "-".repeat(90));
  for (const r of q4a) console.log("  " + String(r.oid).padStart(6) + " | " + (r.pid||"").padEnd(11) + " | " + String(r.camp).padStart(4) + " | " + String(r.status).padStart(6) + " | " + String(r.prior_oid).padStart(9) + " | " + String(r.prior_camp).padStart(10) + " | " + (r.prior_pid||"").padEnd(11) + " | " + r.days);

  // 4b: Samples — different campaign
  console.log();
  console.log("=== 4b: Sample 5 — DIFFERENT campaign retries ===");
  const q4b = querySql(`
    SELECT o1.order_id as oid, o1.product_ids as pid, o1.campaign_id as camp,
      o1.order_status as status,
      o2.order_id as prior_oid, o2.campaign_id as prior_camp, o2.product_ids as prior_pid,
      ROUND(julianday(o1.acquisition_date) - julianday(o2.acquisition_date), 2) as days
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.product_ids = o1.product_ids
      AND o2.campaign_id != o1.campaign_id
      AND o2.order_status = 7
      AND o2.acquisition_date < o1.acquisition_date
      AND o2.order_id != o1.order_id AND o2.is_test = 0
    WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
    AND o1.product_type_classified IN ('initial', 'initial_rebill')
    AND o1.customer_id IS NOT NULL AND o1.is_test = 0
    LIMIT 5
  `);
  console.log("  oid    | product_ids | camp | status | prior_oid | prior_camp | prior_pid   | days");
  console.log("  " + "-".repeat(90));
  for (const r of q4b) console.log("  " + String(r.oid).padStart(6) + " | " + (r.pid||"").padEnd(11) + " | " + String(r.camp).padStart(4) + " | " + String(r.status).padStart(6) + " | " + String(r.prior_oid).padStart(9) + " | " + String(r.prior_camp).padStart(10) + " | " + (r.prior_pid||"").padEnd(11) + " | " + r.days);

  process.exit(0);
}
run();
