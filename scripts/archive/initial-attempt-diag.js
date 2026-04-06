const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== DERIVED INITIAL ATTEMPT ANALYSIS ===");
  console.log("For billing_cycle=0, is_recurring=0, ptype in (initial, initial_rebill), customer IS NOT NULL");
  console.log();

  // 1. Attempt distribution
  console.log("=== 1. Attempt distribution ===");
  const q1 = querySql(`
    SELECT
      attempt,
      COUNT(*) as cnt,
      COUNT(CASE WHEN status_approved = 1 THEN 1 END) as approved,
      ROUND(COUNT(CASE WHEN status_approved = 1 THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM (
      SELECT o1.order_id,
        o1.order_status,
        CASE WHEN o1.order_status IN (2,6,8) THEN 1 ELSE 0 END as status_approved,
        (SELECT COUNT(*) FROM orders o2
         WHERE o2.customer_id = o1.customer_id
         AND o2.product_ids = o1.product_ids
         AND o2.acquisition_date < o1.acquisition_date
         AND o2.is_test = 0
         AND o2.billing_cycle = 0
         AND o2.product_type_classified IN ('initial','initial_rebill')
        ) + 1 as attempt
      FROM orders o1
      WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
      AND o1.product_type_classified IN ('initial','initial_rebill')
      AND o1.customer_id IS NOT NULL
      AND o1.is_test = 0 AND o1.is_internal_test = 0
    )
    GROUP BY attempt ORDER BY attempt
  `);
  console.log("  attempt | count  | approved | rate");
  console.log("  " + "-".repeat(42));
  for (const r of q1) console.log("  " + String(r.attempt).padStart(7) + " | " + String(r.cnt).padStart(6) + " | " + String(r.approved).padStart(8) + " | " + (r.rate||0) + "%");

  // 2. Same vs different campaign by attempt
  console.log();
  console.log("=== 2. Same vs different campaign by attempt ===");
  const q2 = querySql(`
    SELECT attempt,
      SUM(CASE WHEN camp_match = 1 THEN 1 ELSE 0 END) as same_camp,
      SUM(CASE WHEN camp_match = 0 THEN 1 ELSE 0 END) as diff_camp
    FROM (
      SELECT o1.order_id,
        (SELECT COUNT(*) FROM orders o2
         WHERE o2.customer_id = o1.customer_id AND o2.product_ids = o1.product_ids
         AND o2.acquisition_date < o1.acquisition_date AND o2.is_test = 0
         AND o2.billing_cycle = 0 AND o2.product_type_classified IN ('initial','initial_rebill')
        ) + 1 as attempt,
        CASE WHEN EXISTS (
          SELECT 1 FROM orders o3
          WHERE o3.customer_id = o1.customer_id AND o3.product_ids = o1.product_ids
          AND o3.campaign_id = o1.campaign_id
          AND o3.acquisition_date < o1.acquisition_date AND o3.is_test = 0
          AND o3.billing_cycle = 0 AND o3.product_type_classified IN ('initial','initial_rebill')
        ) THEN 1 ELSE 0 END as camp_match
      FROM orders o1
      WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
      AND o1.product_type_classified IN ('initial','initial_rebill')
      AND o1.customer_id IS NOT NULL
      AND o1.is_test = 0 AND o1.is_internal_test = 0
    )
    WHERE attempt >= 2
    GROUP BY attempt ORDER BY attempt
  `);
  console.log("  attempt | same_camp | diff_camp | %diff");
  console.log("  " + "-".repeat(45));
  for (const r of q2) {
    const total = r.same_camp + r.diff_camp;
    const pctDiff = total > 0 ? Math.round(r.diff_camp / total * 100) : 0;
    console.log("  " + String(r.attempt).padStart(7) + " | " + String(r.same_camp).padStart(9) + " | " + String(r.diff_camp).padStart(9) + " | " + pctDiff + "%");
  }

  // 3. Approval rate by attempt (already in q1, reformat)
  console.log();
  console.log("=== 3. Approval rate trend ===");
  for (const r of q1) {
    const bar = "#".repeat(Math.round((r.rate || 0) / 2));
    console.log("  Att " + String(r.attempt).padStart(2) + ": " + String(r.rate||0).padStart(5) + "% " + bar + " (" + r.cnt + " orders)");
  }

  // 4. Time gap between attempts
  console.log();
  console.log("=== 4. Avg days since prior attempt ===");
  const q4 = querySql(`
    SELECT attempt, ROUND(AVG(days_gap), 2) as avg_days, ROUND(MIN(days_gap), 2) as min_days, ROUND(MAX(days_gap), 2) as max_days
    FROM (
      SELECT o1.order_id,
        (SELECT COUNT(*) FROM orders o2
         WHERE o2.customer_id = o1.customer_id AND o2.product_ids = o1.product_ids
         AND o2.acquisition_date < o1.acquisition_date AND o2.is_test = 0
         AND o2.billing_cycle = 0 AND o2.product_type_classified IN ('initial','initial_rebill')
        ) + 1 as attempt,
        (SELECT MIN(julianday(o1.acquisition_date) - julianday(o3.acquisition_date))
         FROM orders o3
         WHERE o3.customer_id = o1.customer_id AND o3.product_ids = o1.product_ids
         AND o3.acquisition_date < o1.acquisition_date AND o3.is_test = 0
         AND o3.billing_cycle = 0 AND o3.product_type_classified IN ('initial','initial_rebill')
         AND julianday(o1.acquisition_date) - julianday(o3.acquisition_date) > 0
        ) as days_gap
      FROM orders o1
      WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
      AND o1.product_type_classified IN ('initial','initial_rebill')
      AND o1.customer_id IS NOT NULL
      AND o1.is_test = 0 AND o1.is_internal_test = 0
    )
    WHERE attempt >= 2 AND days_gap IS NOT NULL
    GROUP BY attempt ORDER BY attempt
  `);
  console.log("  attempt | avg_days | min_days | max_days");
  console.log("  " + "-".repeat(45));
  for (const r of q4) console.log("  " + String(r.attempt).padStart(7) + " | " + String(r.avg_days).padStart(8) + " | " + String(r.min_days).padStart(8) + " | " + String(r.max_days).padStart(8));

  process.exit(0);
}
run();
