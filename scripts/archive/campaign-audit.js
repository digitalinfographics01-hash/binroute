const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== 1. ALL campaigns with tx_type breakdown ===");
  const q1 = querySql(`
    SELECT o.campaign_id,
      COUNT(*) as total,
      COUNT(CASE WHEN o.tx_type = 'cp_initial' THEN 1 END) as initials,
      COUNT(CASE WHEN o.tx_type IN ('tp_rebill','tp_rebill_salvage') THEN 1 END) as rebills,
      COUNT(CASE WHEN o.tx_type IN ('upsell','upsell_cascade') THEN 1 END) as upsells,
      COUNT(CASE WHEN o.tx_type = 'anonymous_decline' THEN 1 END) as anon,
      COUNT(CASE WHEN o.tx_type = 'initial_salvage' THEN 1 END) as init_salv,
      COUNT(CASE WHEN o.tx_type = 'straight_sale' THEN 1 END) as ss,
      MIN(o.acquisition_date) as earliest,
      MAX(o.acquisition_date) as latest,
      GROUP_CONCAT(DISTINCT o.product_group_name) as products
    FROM orders o WHERE o.is_test = 0 AND o.is_internal_test = 0
    GROUP BY o.campaign_id
    ORDER BY total DESC
  `);
  console.log("  camp | total  | init | rebill | upsell | anon  | salv | ss   | earliest            | products");
  console.log("  " + "-".repeat(130));
  for (const r of q1) {
    console.log("  " + String(r.campaign_id).padStart(4) + " | " +
      String(r.total).padStart(6) + " | " + String(r.initials).padStart(4) + " | " +
      String(r.rebills).padStart(6) + " | " + String(r.upsells).padStart(6) + " | " +
      String(r.anon).padStart(5) + " | " + String(r.init_salv).padStart(4) + " | " +
      String(r.ss).padStart(4) + " | " +
      (r.earliest||"").substring(0,10).padEnd(10) + " | " +
      (r.products||"").substring(0,50));
  }

  console.log();
  console.log("=== 2. Derma campaign identification ===");
  console.log("Current mapping uses: product_group_name LIKE '%eternal lumi%' OR '%lumi%'");
  const q2 = querySql(`
    SELECT DISTINCT campaign_id, product_group_name, COUNT(*) as cnt
    FROM orders WHERE is_test = 0 AND is_internal_test = 0
    AND (product_group_name LIKE '%Eternal Lumi%' OR product_group_name LIKE '%Derma%')
    GROUP BY campaign_id, product_group_name
    ORDER BY cnt DESC
  `);
  console.log("  Derma campaigns found:");
  for (const r of q2) console.log("  " + String(r.campaign_id).padStart(6) + " | " + (r.product_group_name||"").padEnd(40) + "| " + r.cnt);

  console.log();
  console.log("=== 3. cp_initial orders — which campaigns? ===");
  const q3 = querySql(`
    SELECT campaign_id, COUNT(*) as cnt, MIN(acquisition_date) as first_seen,
      GROUP_CONCAT(DISTINCT product_group_name) as products
    FROM orders WHERE tx_type = 'cp_initial' AND is_test = 0 AND is_internal_test = 0
    GROUP BY campaign_id ORDER BY cnt DESC
  `);
  console.log("  campaign | count | first_seen | products");
  console.log("  " + "-".repeat(80));
  for (const r of q3) console.log("  " + String(r.campaign_id).padStart(8) + " | " + String(r.cnt).padStart(5) + " | " + (r.first_seen||"").substring(0,10) + " | " + (r.products||"").substring(0,45));

  console.log();
  console.log("=== 4. upsell orders — which campaigns? ===");
  const q4 = querySql(`
    SELECT campaign_id, COUNT(*) as cnt, GROUP_CONCAT(DISTINCT product_group_name) as products
    FROM orders WHERE tx_type IN ('upsell','upsell_cascade') AND is_test = 0 AND is_internal_test = 0
    GROUP BY campaign_id ORDER BY cnt DESC
  `);
  console.log("  campaign | count | products");
  console.log("  " + "-".repeat(80));
  for (const r of q4) console.log("  " + String(r.campaign_id).padStart(8) + " | " + String(r.cnt).padStart(5) + " | " + (r.products||"").substring(0,55));

  console.log();
  console.log("=== 5. Derma product names × tx_type ===");
  const q5 = querySql(`
    SELECT product_group_name, tx_type, COUNT(*) as cnt
    FROM orders WHERE is_test = 0 AND is_internal_test = 0
    AND (product_group_name LIKE '%Eternal Lumi%' OR product_group_name LIKE '%Derma%')
    GROUP BY product_group_name, tx_type
    ORDER BY product_group_name, cnt DESC
  `);
  let lastProd = "";
  console.log("  product                                | tx_type                | count");
  console.log("  " + "-".repeat(80));
  for (const r of q5) {
    if (r.product_group_name !== lastProd) { if (lastProd) console.log(""); lastProd = r.product_group_name; }
    console.log("  " + (r.product_group_name||"").substring(0,41).padEnd(41) + "| " + (r.tx_type||"").padEnd(23) + "| " + r.cnt);
  }

  // Key question: where are the Derma initials?
  console.log();
  console.log("=== KEY: Derma initial orders ===");
  console.log("Searching for Eternal Lumi in cp_initial...");
  const q6 = querySql(`
    SELECT product_group_name, COUNT(*) as cnt
    FROM orders WHERE tx_type = 'cp_initial' AND is_test = 0 AND is_internal_test = 0
    GROUP BY product_group_name ORDER BY cnt DESC
  `);
  for (const r of q6) {
    const isDerma = (r.product_group_name||"").toLowerCase().includes("lumi") || (r.product_group_name||"").toLowerCase().includes("derma");
    console.log("  " + (isDerma ? ">>> " : "    ") + (r.product_group_name||"NULL").padEnd(45) + r.cnt);
  }

  process.exit(0);
}
run();
