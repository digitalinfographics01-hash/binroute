const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== 356 DIFFERENT PRODUCT + DIFFERENT CAMPAIGN ORDERS ===");
  console.log();

  // 1. Time gap distribution
  console.log("=== 1. Time gap from prior declined order ===");
  const q1 = querySql(`
    SELECT
      CASE
        WHEN gap_min <= 5 THEN '0-5 min'
        WHEN gap_min <= 30 THEN '5-30 min'
        WHEN gap_min <= 60 THEN '30-60 min'
        WHEN gap_min <= 1440 THEN '1-24 hrs'
        ELSE '24+ hrs'
      END as bucket,
      COUNT(*) as cnt
    FROM (
      SELECT o1.order_id,
        MIN((julianday(o1.acquisition_date) - julianday(o2.acquisition_date)) * 1440) as gap_min
      FROM orders o1
      JOIN orders o2 ON o2.customer_id = o1.customer_id
        AND o2.product_ids != o1.product_ids
        AND o2.campaign_id != o1.campaign_id
        AND o2.order_status = 7
        AND o2.acquisition_date < o1.acquisition_date
        AND o2.order_id != o1.order_id AND o2.is_test = 0
        AND o2.product_type_classified IN ('initial','initial_rebill')
        AND julianday(o1.acquisition_date) - julianday(o2.acquisition_date) <= 30
      WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
      AND o1.product_type_classified IN ('initial','initial_rebill')
      AND o1.customer_id IS NOT NULL
      AND o1.is_test = 0 AND o1.is_internal_test = 0
      GROUP BY o1.order_id
    )
    GROUP BY bucket
    ORDER BY MIN(gap_min)
  `);
  console.log("  time_gap       | count");
  console.log("  " + "-".repeat(25));
  for (const r of q1) console.log("  " + r.bucket.padEnd(17) + "| " + r.cnt);

  // 2. Current tx_type
  console.log();
  console.log("=== 2. Current tx_type ===");
  const q2 = querySql(`
    SELECT o1.tx_type, COUNT(DISTINCT o1.order_id) as cnt
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.product_ids != o1.product_ids
      AND o2.campaign_id != o1.campaign_id
      AND o2.order_status = 7
      AND o2.acquisition_date < o1.acquisition_date
      AND o2.order_id != o1.order_id AND o2.is_test = 0
      AND o2.product_type_classified IN ('initial','initial_rebill')
      AND julianday(o1.acquisition_date) - julianday(o2.acquisition_date) <= 30
    WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
    AND o1.product_type_classified IN ('initial','initial_rebill')
    AND o1.customer_id IS NOT NULL
    AND o1.is_test = 0 AND o1.is_internal_test = 0
    GROUP BY o1.tx_type ORDER BY cnt DESC
  `);
  for (const r of q2) console.log("  " + (r.tx_type||"NULL").padEnd(25) + r.cnt);

  // 3. Product group comparison
  console.log();
  console.log("=== 3. Product group comparison ===");
  const q3 = querySql(`
    SELECT
      CASE
        WHEN o1.product_group_id IS NULL OR o2.product_group_id IS NULL THEN 'NULL product_group'
        WHEN o1.product_group_id = o2.product_group_id THEN 'Same product_group'
        ELSE 'Different product_group'
      END as match,
      COUNT(DISTINCT o1.order_id) as cnt
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.product_ids != o1.product_ids
      AND o2.campaign_id != o1.campaign_id
      AND o2.order_status = 7
      AND o2.acquisition_date < o1.acquisition_date
      AND o2.order_id != o1.order_id AND o2.is_test = 0
      AND o2.product_type_classified IN ('initial','initial_rebill')
      AND julianday(o1.acquisition_date) - julianday(o2.acquisition_date) <= 30
    WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
    AND o1.product_type_classified IN ('initial','initial_rebill')
    AND o1.customer_id IS NOT NULL
    AND o1.is_test = 0 AND o1.is_internal_test = 0
    GROUP BY match ORDER BY cnt DESC
  `);
  for (const r of q3) console.log("  " + r.match.padEnd(25) + r.cnt);

  // 4. Samples
  console.log();
  console.log("=== 4. Sample 5 ===");
  const q4 = querySql(`
    SELECT o1.order_id as oid, o1.product_ids as pid, o1.product_group_id as pgid,
      o1.product_group_name as pg, o1.campaign_id as camp,
      o2.order_id as prior_oid, o2.product_ids as prior_pid,
      o2.product_group_id as prior_pgid, o2.product_group_name as prior_pg,
      ROUND((julianday(o1.acquisition_date) - julianday(o2.acquisition_date)) * 1440, 1) as gap_min
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.product_ids != o1.product_ids
      AND o2.campaign_id != o1.campaign_id
      AND o2.order_status = 7
      AND o2.acquisition_date < o1.acquisition_date
      AND o2.order_id != o1.order_id AND o2.is_test = 0
      AND o2.product_type_classified IN ('initial','initial_rebill')
      AND julianday(o1.acquisition_date) - julianday(o2.acquisition_date) <= 30
    WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
    AND o1.product_type_classified IN ('initial','initial_rebill')
    AND o1.customer_id IS NOT NULL
    AND o1.is_test = 0 AND o1.is_internal_test = 0
    ORDER BY gap_min ASC
    LIMIT 5
  `);
  console.log("  oid    | pid       | pgid | product                 | camp | prior_oid | prior_pid | prior_pg                | gap_min");
  console.log("  " + "-".repeat(120));
  for (const r of q4) {
    console.log("  " + String(r.oid).padStart(6) + " | " + (r.pid||"").padEnd(9) + " | " + String(r.pgid||"null").padStart(4) + " | " +
      (r.pg||"NULL").substring(0,23).padEnd(23) + " | " + String(r.camp).padStart(4) + " | " +
      String(r.prior_oid).padStart(9) + " | " + (r.prior_pid||"").padEnd(9) + " | " +
      (r.prior_pg||"NULL").substring(0,23).padEnd(23) + " | " + r.gap_min + " min");
  }

  // Also: under-30-min count explicitly
  console.log();
  const under30 = querySql(`
    SELECT COUNT(DISTINCT o1.order_id) as cnt FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.product_ids != o1.product_ids AND o2.campaign_id != o1.campaign_id
      AND o2.order_status = 7 AND o2.acquisition_date < o1.acquisition_date
      AND o2.order_id != o1.order_id AND o2.is_test = 0
      AND o2.product_type_classified IN ('initial','initial_rebill')
      AND (julianday(o1.acquisition_date) - julianday(o2.acquisition_date)) * 1440 <= 30
    WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
    AND o1.product_type_classified IN ('initial','initial_rebill')
    AND o1.customer_id IS NOT NULL AND o1.is_test = 0 AND o1.is_internal_test = 0
    AND o1.product_group_id IS NOT NULL AND o2.product_group_id IS NOT NULL
    AND o1.product_group_id != o2.product_group_id
  `);
  console.log("  Under 30 min + different product_group (= upsells): " + under30[0].cnt);

  process.exit(0);
}
run();
