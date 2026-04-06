const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("═══════════════════════════════════════════════");
  console.log("ISSUE B — 1,321 upsells classified as cp_initial");
  console.log("═══════════════════════════════════════════════");

  // Check product_group_id on these orders
  const qb1 = querySql(`
    SELECT
      SUM(CASE WHEN o1.product_group_id IS NULL THEN 1 ELSE 0 END) as null_pg,
      SUM(CASE WHEN o1.product_group_id IS NOT NULL THEN 1 ELSE 0 END) as has_pg
    FROM orders o1
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM orders o2
      WHERE o2.customer_id = o1.customer_id
      AND o2.campaign_id != o1.campaign_id
      AND o2.order_status IN (2,6,8)
      AND ABS(julianday(o2.acquisition_date) - julianday(o1.acquisition_date)) < 1
      AND o2.order_id != o1.order_id AND o2.is_test=0
    )
  `);
  console.log("  NULL product_group_id:", qb1[0].null_pg);
  console.log("  Has product_group_id:", qb1[0].has_pg);

  // What are the anchor orders?
  console.log();
  console.log("  Sample 5 misclassified orders + their anchor:");
  const qb2 = querySql(`
    SELECT o1.order_id as mis_oid, o1.tx_type as mis_tx, o1.campaign_id as mis_camp,
      o1.product_group_name as mis_pg, o1.product_group_id as mis_pgid,
      o1.acquisition_date as mis_date, o1.order_status as mis_status,
      o2.order_id as anchor_oid, o2.tx_type as anchor_tx, o2.campaign_id as anchor_camp,
      o2.product_group_name as anchor_pg, o2.order_status as anchor_status,
      o2.acquisition_date as anchor_date
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.campaign_id != o1.campaign_id
      AND o2.order_status IN (2,6,8)
      AND ABS(julianday(o2.acquisition_date) - julianday(o1.acquisition_date)) < 1
      AND o2.order_id != o1.order_id AND o2.is_test=0
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL
    LIMIT 5
  `);
  for (const r of qb2) {
    console.log("    Misclassified: oid=" + r.mis_oid + " tx=" + r.mis_tx + " camp=" + r.mis_camp +
      " pg=" + (r.mis_pg||"NULL").substring(0,25) + " pgid=" + r.mis_pgid + " status=" + r.mis_status);
    console.log("    Anchor:        oid=" + r.anchor_oid + " tx=" + r.anchor_tx + " camp=" + r.anchor_camp +
      " pg=" + (r.anchor_pg||"NULL").substring(0,25) + " status=" + r.anchor_status);
    console.log("    Time diff: " + (new Date(r.mis_date) - new Date(r.anchor_date)) / 1000 + "s");
    console.log();
  }

  // Check: are these the same product or different product?
  const qb3 = querySql(`
    SELECT
      SUM(CASE WHEN o1.product_group_id = o2.product_group_id THEN 1 ELSE 0 END) as same_pg,
      SUM(CASE WHEN o1.product_group_id != o2.product_group_id THEN 1 ELSE 0 END) as diff_pg,
      SUM(CASE WHEN o1.product_group_id IS NULL OR o2.product_group_id IS NULL THEN 1 ELSE 0 END) as null_pg
    FROM orders o1
    JOIN orders o2 ON o2.customer_id = o1.customer_id
      AND o2.campaign_id != o1.campaign_id
      AND o2.order_status IN (2,6,8)
      AND ABS(julianday(o2.acquisition_date) - julianday(o1.acquisition_date)) < 1
      AND o2.order_id != o1.order_id AND o2.is_test=0
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL
  `);
  console.log("  Product group relationship to anchor:");
  console.log("    Same product_group: " + qb3[0].same_pg);
  console.log("    Different product_group: " + qb3[0].diff_pg + " ← these are genuine upsells");
  console.log("    NULL product_group: " + qb3[0].null_pg);

  // What are the approved/declined status of the misclassified?
  const qb4 = querySql(`
    SELECT o1.order_status, COUNT(*) as cnt
    FROM orders o1
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM orders o2
      WHERE o2.customer_id = o1.customer_id
      AND o2.campaign_id != o1.campaign_id
      AND o2.order_status IN (2,6,8)
      AND ABS(julianday(o2.acquisition_date) - julianday(o1.acquisition_date)) < 1
      AND o2.order_id != o1.order_id AND o2.is_test=0
    )
    GROUP BY o1.order_status ORDER BY cnt DESC
  `);
  console.log();
  console.log("  Status of misclassified orders:");
  for (const r of qb4) console.log("    status " + r.order_status + ": " + r.cnt);

  console.log();
  console.log("═══════════════════════════════════════════════");
  console.log("ISSUE E — 352 is_recurring=1 not sticky_cof_rebill");
  console.log("═══════════════════════════════════════════════");

  const qe1 = querySql(`
    SELECT tx_type, billing_cycle, COUNT(*) as cnt
    FROM orders WHERE is_recurring = 1 AND tx_type != 'sticky_cof_rebill'
    AND is_test=0 AND is_internal_test=0
    GROUP BY tx_type, billing_cycle ORDER BY cnt DESC
  `);
  console.log("  By tx_type + billing_cycle:");
  for (const r of qe1) console.log("    " + r.tx_type.padEnd(20) + " cycle=" + r.billing_cycle + " → " + r.cnt);

  // Sample 5
  console.log();
  console.log("  Sample 5:");
  const qe2 = querySql(`
    SELECT order_id, tx_type, billing_cycle, is_recurring, customer_id, campaign_id,
      product_group_name, product_type_classified, order_status, derived_cycle, attempt_number
    FROM orders WHERE is_recurring = 1 AND tx_type != 'sticky_cof_rebill'
    AND is_test=0 AND is_internal_test=0
    LIMIT 5
  `);
  for (const r of qe2) {
    console.log("    oid=" + r.order_id + " tx=" + r.tx_type + " cycle=" + r.billing_cycle +
      " recurring=" + r.is_recurring + " cust=" + r.customer_id + " camp=" + r.campaign_id +
      " ptype=" + r.product_type_classified + " status=" + r.order_status +
      " dcycle=" + r.derived_cycle + " att=" + r.attempt_number);
    console.log("      pg=" + (r.product_group_name || "NULL"));
  }

  // Check: do they have prior approved orders?
  const qe3 = querySql(`
    SELECT
      SUM(CASE WHEN EXISTS (
        SELECT 1 FROM orders o2 WHERE o2.customer_id = o1.customer_id
        AND o2.product_group_id = o1.product_group_id
        AND o2.order_status IN (2,6,8)
        AND o2.acquisition_date < o1.acquisition_date AND o2.is_test=0
      ) THEN 1 ELSE 0 END) as has_prior,
      SUM(CASE WHEN NOT EXISTS (
        SELECT 1 FROM orders o2 WHERE o2.customer_id = o1.customer_id
        AND o2.product_group_id = o1.product_group_id
        AND o2.order_status IN (2,6,8)
        AND o2.acquisition_date < o1.acquisition_date AND o2.is_test=0
      ) THEN 1 ELSE 0 END) as no_prior
    FROM orders o1
    WHERE o1.is_recurring = 1 AND o1.tx_type != 'sticky_cof_rebill'
    AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL AND o1.product_group_id IS NOT NULL
  `);
  console.log();
  console.log("  Prior approved orders (same customer+product):");
  console.log("    Has prior: " + qe3[0].has_prior);
  console.log("    No prior: " + qe3[0].no_prior);

  // Product type breakdown
  const qe4 = querySql(`
    SELECT product_type_classified, COUNT(*) as cnt
    FROM orders WHERE is_recurring = 1 AND tx_type != 'sticky_cof_rebill'
    AND is_test=0 AND is_internal_test=0
    GROUP BY product_type_classified ORDER BY cnt DESC
  `);
  console.log();
  console.log("  Product type:");
  for (const r of qe4) console.log("    " + (r.product_type_classified || "NULL").padEnd(20) + r.cnt);

  // Campaign breakdown
  const qe5 = querySql(`
    SELECT campaign_id, product_group_name, COUNT(*) as cnt
    FROM orders WHERE is_recurring = 1 AND tx_type != 'sticky_cof_rebill'
    AND is_test=0 AND is_internal_test=0
    GROUP BY campaign_id ORDER BY cnt DESC LIMIT 5
  `);
  console.log();
  console.log("  Top campaigns:");
  for (const r of qe5) console.log("    camp " + r.campaign_id + " (" + (r.product_group_name||"").substring(0,30) + "): " + r.cnt);

  process.exit(0);
}
run();
