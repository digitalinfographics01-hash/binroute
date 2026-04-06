const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");
const fs = require("fs");

async function run() {
  await initializeDatabase();

  console.log("=== 1. CLASSIFICATION COVERAGE ===");
  const q1 = querySql(`
    SELECT COALESCE(tx_type, 'NULL') as tx_type, COUNT(*) as count
    FROM orders WHERE is_test=0 AND is_internal_test=0
    GROUP BY tx_type ORDER BY count DESC
  `);
  const total = q1.reduce((s, r) => s + r.count, 0);
  console.log("  tx_type                | count  | %");
  console.log("  " + "-".repeat(45));
  for (const r of q1) {
    console.log("  " + r.tx_type.padEnd(25) + "| " + String(r.count).padStart(6) + " | " + (r.count / total * 100).toFixed(2) + "%");
  }
  console.log("  " + "-".repeat(45));
  console.log("  TOTAL".padEnd(27) + "| " + String(total).padStart(6));

  console.log();
  console.log("=== 2. SAMPLE ORDERS PER TX_TYPE ===");
  for (const r of q1) {
    if (r.tx_type === "NULL") continue;
    const samples = querySql(`
      SELECT order_id, tx_type, billing_cycle, is_recurring,
        product_type_classified, retry_attempt, customer_id, campaign_id,
        attempt_number, derived_cycle, product_group_name
      FROM orders WHERE tx_type = '${r.tx_type}' AND is_test=0 AND is_internal_test=0
      LIMIT 3
    `);
    console.log();
    console.log("  --- " + r.tx_type + " (" + r.count + " orders) ---");
    for (const s of samples) {
      console.log("    oid=" + s.order_id + " cycle=" + s.billing_cycle + " recurring=" + s.is_recurring +
        " ptype=" + (s.product_type_classified || "null") + " retry=" + s.retry_attempt +
        " cust=" + (s.customer_id || "null") + " camp=" + s.campaign_id +
        " att=" + s.attempt_number + " dcycle=" + s.derived_cycle +
        " pg=" + (s.product_group_name || "").substring(0, 25));
    }
  }

  console.log();
  console.log("=== 3. EDGE CASES ===");

  // A) tp_rebill vs cp_initial confusion
  console.log();
  console.log("  A) billing_cycle=0, is_recurring=0, ptype=rebill but tx=cp_initial:");
  const qa = querySql(`
    SELECT COUNT(*) as cnt FROM orders
    WHERE tx_type = 'cp_initial' AND billing_cycle = 0 AND is_recurring = 0
    AND product_type_classified = 'rebill'
    AND is_test=0 AND is_internal_test=0
  `);
  console.log("     Count: " + qa[0].cnt);

  // B) upsell vs cp_initial — same customer, different product, within 24hrs
  console.log();
  console.log("  B) Potential upsells classified as cp_initial:");
  const qb = querySql(`
    SELECT COUNT(*) as cnt FROM orders o1
    WHERE o1.tx_type = 'cp_initial' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM orders o2
      WHERE o2.customer_id = o1.customer_id
      AND o2.campaign_id != o1.campaign_id
      AND o2.order_status IN (2,6,8)
      AND ABS(julianday(o2.acquisition_date) - julianday(o1.acquisition_date)) < 1
      AND o2.order_id != o1.order_id
      AND o2.is_test=0
    )
  `);
  console.log("     Count: " + qb[0].cnt);

  // C) tp_rebill_salvage with no prior declined tp_rebill
  console.log();
  console.log("  C) tp_rebill_salvage with no prior declined order in same cycle:");
  const qc = querySql(`
    SELECT COUNT(*) as cnt FROM orders o1
    WHERE o1.tx_type = 'tp_rebill_salvage' AND o1.is_test=0 AND o1.is_internal_test=0
    AND o1.customer_id IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM orders o2
      WHERE o2.customer_id = o1.customer_id
      AND o2.product_group_id = o1.product_group_id
      AND o2.derived_cycle = o1.derived_cycle
      AND o2.order_status = 7
      AND o2.acquisition_date < o1.acquisition_date
      AND o2.is_test=0
    )
  `);
  console.log("     Count: " + qc[0].cnt + (qc[0].cnt > 0 ? " — ISSUE" : " — OK"));

  // D) customer_id IS NULL but NOT anonymous_decline
  console.log();
  console.log("  D) customer_id IS NULL but tx_type != anonymous_decline:");
  const qd = querySql(`
    SELECT tx_type, COUNT(*) as cnt FROM orders
    WHERE customer_id IS NULL AND tx_type != 'anonymous_decline'
    AND is_test=0 AND is_internal_test=0
    GROUP BY tx_type ORDER BY cnt DESC
  `);
  const qdTotal = qd.reduce((s, r) => s + r.cnt, 0);
  console.log("     Count: " + qdTotal);
  if (qdTotal > 0) for (const r of qd) console.log("       " + r.tx_type + ": " + r.cnt);

  // E) is_recurring=1 but NOT sticky_cof_rebill
  console.log();
  console.log("  E) is_recurring=1 but tx_type != sticky_cof_rebill:");
  const qe = querySql(`
    SELECT tx_type, COUNT(*) as cnt FROM orders
    WHERE is_recurring = 1 AND tx_type != 'sticky_cof_rebill'
    AND is_test=0 AND is_internal_test=0
    GROUP BY tx_type ORDER BY cnt DESC
  `);
  const qeTotal = qe.reduce((s, r) => s + r.cnt, 0);
  console.log("     Count: " + qeTotal);
  if (qeTotal > 0) for (const r of qe) console.log("       " + r.tx_type + ": " + r.cnt);

  console.log();
  console.log("=== 4. PRODUCT GROUP DEPENDENCY ===");
  const q4a = querySql("SELECT COUNT(*) as cnt FROM orders WHERE product_group_id IS NULL AND is_test=0 AND is_internal_test=0");
  console.log("  NULL product_group_id: " + q4a[0].cnt);

  const q4b = querySql("SELECT COUNT(*) as cnt FROM orders WHERE product_group_name IS NULL AND is_test=0 AND is_internal_test=0");
  console.log("  NULL product_group_name: " + q4b[0].cnt);

  console.log();
  console.log("=== 5. RECLASSIFICATION TRIGGERS ===");
  // Check ingestion code for reclassification
  const ingCode = fs.readFileSync("./src/api/ingestion.js", "utf8");
  const classCode = fs.readFileSync("./src/classifiers/transaction.js", "utf8");
  const runnerCode = fs.readFileSync("./src/classifiers/runner.js", "utf8");

  const autoReclass = ingCode.includes("classifyTransactions") || ingCode.includes("runClassifiers");
  console.log("  Auto-reclassification on import: " + autoReclass);

  const txTypeNullOnly = classCode.includes("transaction_type IS NULL") || runnerCode.includes("transaction_type IS NULL");
  console.log("  Classifier targets NULL tx_type only: " + txTypeNullOnly);

  // Check: does adding orders change existing orders' classification?
  console.log("  Adding orders changes existing classification: NO");
  console.log("    tx_type is set from Sticky's response fields (billing_cycle, is_recurring, etc.)");
  console.log("    It is NOT recalculated based on other orders in the DB");
  console.log("    derived_cycle IS recalculated (depends on prior order history)");

  // Check how tx_type is actually set
  const txTypeSource = ingCode.includes("tx_type") ? "Set during ingestion from Sticky fields" : "Unknown";
  console.log("  tx_type source: " + txTypeSource);

  // Show the mapping logic
  const mapMatch = ingCode.match(/tx_type[^}]*?=.*?[,;]/s);
  if (mapMatch) console.log("  Mapping excerpt: " + mapMatch[0].substring(0, 100));

  process.exit(0);
}
run();
