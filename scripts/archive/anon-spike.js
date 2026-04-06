const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== 1. Campaigns: Jan vs Feb vs Mar anonymous declines ===");
  const q1 = querySql(`
    SELECT o.campaign_id,
      MIN(o.acquisition_date) as first_order,
      SUM(CASE WHEN strftime('%Y-%m', o.acquisition_date) = '2026-01' AND o.customer_id IS NULL THEN 1 ELSE 0 END) as jan_anon,
      SUM(CASE WHEN strftime('%Y-%m', o.acquisition_date) = '2026-01' THEN 1 ELSE 0 END) as jan_total,
      SUM(CASE WHEN strftime('%Y-%m', o.acquisition_date) = '2026-02' AND o.customer_id IS NULL THEN 1 ELSE 0 END) as feb_anon,
      SUM(CASE WHEN strftime('%Y-%m', o.acquisition_date) = '2026-02' THEN 1 ELSE 0 END) as feb_total,
      SUM(CASE WHEN strftime('%Y-%m', o.acquisition_date) = '2026-03' AND o.customer_id IS NULL THEN 1 ELSE 0 END) as mar_anon,
      SUM(CASE WHEN strftime('%Y-%m', o.acquisition_date) = '2026-03' THEN 1 ELSE 0 END) as mar_total
    FROM orders o
    WHERE o.is_test = 0 AND o.is_internal_test = 0
    GROUP BY o.campaign_id
    HAVING SUM(CASE WHEN o.customer_id IS NULL THEN 1 ELSE 0 END) > 0
    ORDER BY feb_anon DESC
  `);
  console.log("  campaign | first_order          | jan anon/total | feb anon/total | mar anon/total");
  console.log("  " + "-".repeat(95));
  for (const r of q1) {
    console.log("  " + String(r.campaign_id).padStart(8) + " | " + (r.first_order||"").substring(0,19).padEnd(19) + " | " +
      String(r.jan_anon).padStart(4) + "/" + String(r.jan_total).padStart(5) + "       | " +
      String(r.feb_anon).padStart(4) + "/" + String(r.feb_total).padStart(5) + "       | " +
      String(r.mar_anon).padStart(4) + "/" + String(r.mar_total).padStart(5));
  }

  console.log();
  console.log("=== 2. Cliq (GW172) anonymous rate by month ===");
  const q2 = querySql(`
    SELECT strftime('%Y-%m', o.acquisition_date) as month,
      COUNT(*) as total,
      SUM(CASE WHEN o.customer_id IS NULL THEN 1 ELSE 0 END) as anon,
      ROUND(SUM(CASE WHEN o.customer_id IS NULL THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0),1) as pct
    FROM orders o
    WHERE o.gateway_id = 172 AND o.is_test = 0 AND o.is_internal_test = 0
    GROUP BY month ORDER BY month
  `);
  console.log("  month   | total  | anon   | % anon");
  console.log("  " + "-".repeat(40));
  for (const r of q2) console.log("  " + r.month + "  | " + String(r.total).padStart(6) + " | " + String(r.anon).padStart(6) + " | " + r.pct + "%");

  console.log();
  console.log("=== 3. Sutton Bank prepaid anonymous: decline reason by month ===");
  const q3 = querySql(`
    SELECT o.decline_reason, strftime('%Y-%m', o.acquisition_date) as month, COUNT(*) as cnt
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.customer_id IS NULL AND o.is_test = 0 AND o.is_internal_test = 0
    AND b.issuer_bank = 'SUTTON BANK' AND b.is_prepaid = 1
    GROUP BY o.decline_reason, month
    HAVING COUNT(*) >= 5
    ORDER BY o.decline_reason, month
  `);
  let lastReason = "";
  console.log("  decline_reason                    | month   | count");
  console.log("  " + "-".repeat(55));
  for (const r of q3) {
    if (r.decline_reason !== lastReason) { if (lastReason) console.log(""); lastReason = r.decline_reason; }
    console.log("  " + (r.decline_reason||"").substring(0,36).padEnd(36) + "| " + r.month + "  | " + r.cnt);
  }

  console.log();
  console.log("=== 4. Top 5 campaigns by anon count (Feb+Mar) ===");
  const q4 = querySql(`
    SELECT o.campaign_id,
      COUNT(*) as anon_count,
      COUNT(DISTINCT o.cc_first_6) as unique_bins,
      COUNT(DISTINCT o.gateway_id) as gateways
    FROM orders o
    WHERE o.customer_id IS NULL AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.acquisition_date >= '2026-02-01'
    GROUP BY o.campaign_id
    ORDER BY anon_count DESC
    LIMIT 5
  `);
  console.log("  campaign | anon count | unique BINs | gateways");
  console.log("  " + "-".repeat(55));
  for (const r of q4) console.log("  " + String(r.campaign_id).padStart(8) + " | " + String(r.anon_count).padStart(10) + " | " + String(r.unique_bins).padStart(11) + " | " + String(r.gateways).padStart(8));

  console.log();
  console.log("  Campaign → Product groups:");
  const q5 = querySql(`
    SELECT DISTINCT o.campaign_id, o.product_group_name
    FROM orders o
    WHERE o.customer_id IS NULL AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.acquisition_date >= '2026-02-01'
    AND o.campaign_id IN (
      SELECT campaign_id FROM orders WHERE customer_id IS NULL AND is_test = 0 AND acquisition_date >= '2026-02-01'
      GROUP BY campaign_id ORDER BY COUNT(*) DESC LIMIT 5
    )
    ORDER BY o.campaign_id
  `);
  for (const r of q5) console.log("  " + String(r.campaign_id).padStart(8) + " -> " + (r.product_group_name || "NULL"));

  process.exit(0);
}
run();
