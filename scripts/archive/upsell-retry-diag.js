const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== 1. cp_initial retry_attempt distribution ===");
  const q1 = querySql(`
    SELECT retry_attempt, COUNT(*) as cnt,
      COUNT(CASE WHEN order_status IN (2,6,8) THEN 1 END) as approved,
      ROUND(COUNT(CASE WHEN order_status IN (2,6,8) THEN 1 END)*100.0/
        NULLIF(COUNT(CASE WHEN order_status IN (2,6,7,8) THEN 1 END),0),1) as rate
    FROM orders WHERE tx_type = 'cp_initial' AND is_test=0 AND is_internal_test=0
    GROUP BY retry_attempt ORDER BY retry_attempt
  `);
  console.log("  retry_attempt | count  | approved | rate");
  console.log("  " + "-".repeat(45));
  for (const r of q1) console.log("  " + String(r.retry_attempt).padStart(14) + " | " + String(r.cnt).padStart(6) + " | " + String(r.approved).padStart(8) + " | " + (r.rate||0) + "%");

  const retryGt0 = q1.filter(r => r.retry_attempt > 0).reduce((s,r) => s + r.cnt, 0);
  console.log("  cp_initial with retry_attempt > 0: " + retryGt0);

  console.log();
  console.log("=== 2. cp_initial with prior DECLINED cp_initial (same customer+product, 7 days) ===");
  const q2 = querySql(`
    SELECT COUNT(*) as cnt FROM orders o1
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL AND o1.product_group_id IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM orders o2
      WHERE o2.customer_id = o1.customer_id
      AND o2.product_group_id = o1.product_group_id
      AND o2.tx_type = 'cp_initial'
      AND o2.order_status = 7
      AND o2.acquisition_date < o1.acquisition_date
      AND julianday(o1.acquisition_date) - julianday(o2.acquisition_date) <= 7
      AND o2.order_id != o1.order_id
      AND o2.is_test=0
    )
  `);
  console.log("  Count: " + q2[0].cnt);

  // Sample
  const q2b = querySql(`
    SELECT o1.order_id as oid, o1.retry_attempt, o1.order_status, o1.attempt_number,
      o2.order_id as prior_oid, o2.order_status as prior_status,
      ROUND(julianday(o1.acquisition_date) - julianday(o2.acquisition_date), 2) as days_gap
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.product_group_id = o1.product_group_id
      AND o2.tx_type = 'cp_initial' AND o2.order_status = 7
      AND o2.acquisition_date < o1.acquisition_date
      AND julianday(o1.acquisition_date) - julianday(o2.acquisition_date) <= 7
      AND o2.order_id != o1.order_id AND o2.is_test=0
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL AND o1.product_group_id IS NOT NULL
    LIMIT 5
  `);
  if (q2b.length) {
    console.log("  Sample:");
    for (const r of q2b) console.log("    oid=" + r.oid + " retry=" + r.retry_attempt + " status=" + r.order_status + " att=" + r.attempt_number + " | prior_oid=" + r.prior_oid + " prior_status=" + r.prior_status + " gap=" + r.days_gap + "d");
  }

  console.log();
  console.log("=== 3. tx_type for retry_attempt > 0 ===");
  const q3 = querySql(`
    SELECT tx_type, COUNT(*) as cnt
    FROM orders WHERE retry_attempt > 0 AND is_test=0 AND is_internal_test=0
    GROUP BY tx_type ORDER BY cnt DESC
  `);
  console.log("  tx_type                | count");
  console.log("  " + "-".repeat(35));
  for (const r of q3) console.log("  " + (r.tx_type||"NULL").padEnd(25) + "| " + r.cnt);

  // Also check: is there an initial_salvage or initial_retry tx_type?
  console.log();
  console.log("  Does 'initial_retry' or 'initial_salvage' tx_type exist?");
  const q4 = querySql("SELECT DISTINCT tx_type FROM orders WHERE tx_type LIKE '%initial%' AND is_test=0");
  for (const r of q4) console.log("    " + r.tx_type);

  // Check attempt_number distribution for cp_initial
  console.log();
  console.log("=== BONUS: attempt_number for cp_initial ===");
  const q5 = querySql(`
    SELECT attempt_number, COUNT(*) as cnt
    FROM orders WHERE tx_type = 'cp_initial' AND is_test=0 AND is_internal_test=0
    GROUP BY attempt_number ORDER BY attempt_number
  `);
  for (const r of q5) console.log("  attempt " + (r.attempt_number == null ? "NULL" : r.attempt_number) + ": " + r.cnt);

  process.exit(0);
}
run();
