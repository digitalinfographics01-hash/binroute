const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  // Identify offers
  console.log("=== CAMPAIGN → OFFER MAPPING ===");
  const camps = querySql(`
    SELECT campaign_id, product_group_name, COUNT(*) as orders
    FROM orders WHERE is_test = 0 AND is_internal_test = 0
    GROUP BY campaign_id, product_group_name
    ORDER BY orders DESC
  `);

  const derma = new Set();
  const excel = new Set();
  const other = new Set();

  console.log("  campaign | product                              | orders | offer");
  console.log("  " + "-".repeat(80));
  for (const r of camps) {
    const pg = (r.product_group_name || "").toLowerCase();
    let offer;
    if (pg.includes("eternal lumi") || pg.includes("lumi")) {
      offer = "DERMA"; derma.add(r.campaign_id);
    } else if (pg.includes("e-xcel") || pg.includes("excel")) {
      offer = "EXCEL"; excel.add(r.campaign_id);
    } else if (pg.includes("viraflexx") || pg.includes("erecovery")) {
      offer = "OTHER"; other.add(r.campaign_id);
    } else {
      offer = "OTHER"; other.add(r.campaign_id);
    }
    if (r.orders >= 20) {
      console.log("  " + String(r.campaign_id).padStart(8) + " | " + (r.product_group_name||"").substring(0,37).padEnd(37) + "| " + String(r.orders).padStart(6) + " | " + offer);
    }
  }

  const dermaIds = [...derma].join(",");
  const excelIds = [...excel].join(",");
  console.log("\n  Derma campaigns:", derma.size, "(" + dermaIds.substring(0,50) + ")");
  console.log("  E-XceL campaigns:", excel.size, "(" + excelIds.substring(0,50) + ")");

  function offerFilter(ids) {
    return "o.campaign_id IN (" + ids + ")";
  }

  for (const [name, ids] of [["DERMA LUMIERE", dermaIds], ["E-XCEL NOW ME", excelIds]]) {
    if (!ids) continue;
    const f = offerFilter(ids);

    console.log("\n\n════════════════════════════════════════════");
    console.log("  " + name);
    console.log("════════════════════════════════════════════");

    // 1. TX type distribution
    console.log("\n--- 1. TX TYPE DISTRIBUTION ---");
    const q1 = querySql(`
      SELECT o.tx_type, COUNT(*) as cnt,
        COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app,
        ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/
          NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as rate
      FROM orders o WHERE o.is_test = 0 AND o.is_internal_test = 0 AND ${f}
      GROUP BY o.tx_type ORDER BY cnt DESC
    `);
    console.log("  tx_type                | count  | approved | rate");
    console.log("  " + "-".repeat(60));
    for (const r of q1) console.log("  " + (r.tx_type||"NULL").padEnd(25) + "| " + String(r.cnt).padStart(6) + " | " + String(r.app).padStart(8) + " | " + (r.rate||0) + "%");

    // 2. Top BIN groups
    console.log("\n--- 2. TOP 10 BIN GROUPS ---");
    const q2 = querySql(`
      SELECT COALESCE(b.issuer_bank,'Unknown') as bank, COALESCE(b.card_brand,'?') as brand, COALESCE(b.is_prepaid,0) as prep,
        COUNT(*) as att,
        ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/
          NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as rate
      FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
      WHERE o.is_test = 0 AND o.is_internal_test = 0 AND ${f}
      GROUP BY bank, brand ORDER BY att DESC LIMIT 10
    `);
    console.log("  issuer                                 | brand      | prep | att    | rate");
    console.log("  " + "-".repeat(80));
    for (const r of q2) console.log("  " + r.bank.substring(0,41).padEnd(41) + "| " + r.brand.padEnd(11) + "| " + String(r.prep).padEnd(5) + "| " + String(r.att).padStart(6) + " | " + (r.rate||0) + "%");

    // 3. Gateway performance
    console.log("\n--- 3. GATEWAY PERFORMANCE ---");
    const q3 = querySql(`
      SELECT o.gateway_id, g.gateway_alias, g.gateway_active,
        COUNT(*) as att,
        ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/
          NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as rate
      FROM orders o JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
      WHERE o.is_test = 0 AND o.is_internal_test = 0 AND ${f}
      GROUP BY o.gateway_id ORDER BY att DESC LIMIT 10
    `);
    console.log("  gw  | alias                              | active | att    | rate");
    console.log("  " + "-".repeat(75));
    for (const r of q3) console.log("  " + String(r.gateway_id).padStart(3) + " | " + (r.gateway_alias||"").substring(0,35).padEnd(35) + "| " + String(r.gateway_active).padEnd(7) + "| " + String(r.att).padStart(6) + " | " + (r.rate||0) + "%");

    // 4. Price point (rebills)
    console.log("\n--- 4. PRICE POINTS (rebills only) ---");
    const q4 = querySql(`
      SELECT CASE WHEN o.order_total <= 25 THEN '$0-25' WHEN o.order_total <= 50 THEN '$26-50'
        WHEN o.order_total <= 75 THEN '$51-75' WHEN o.order_total <= 100 THEN '$76-100' ELSE '$100+' END as bkt,
        COUNT(*) as att,
        ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/
          NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as rate
      FROM orders o WHERE o.is_test = 0 AND o.is_internal_test = 0 AND ${f}
      AND o.tx_type IN ('tp_rebill','tp_rebill_salvage') AND o.order_total > 0
      GROUP BY bkt ORDER BY bkt
    `);
    console.log("  bucket   | att    | rate");
    console.log("  " + "-".repeat(30));
    for (const r of q4) console.log("  " + r.bkt.padEnd(10) + "| " + String(r.att).padStart(6) + " | " + (r.rate||0) + "%");

    // 5. Decline category
    console.log("\n--- 5. DECLINE CATEGORIES ---");
    const q5 = querySql(`
      SELECT COALESCE(o.decline_category,'NULL') as cat, COUNT(*) as cnt
      FROM orders o WHERE o.is_test = 0 AND o.is_internal_test = 0 AND ${f} AND o.order_status = 7
      GROUP BY cat ORDER BY cnt DESC
    `);
    const decTotal = q5.reduce((s,r) => s + r.cnt, 0);
    console.log("  category         | count  | %");
    console.log("  " + "-".repeat(35));
    for (const r of q5) console.log("  " + r.cat.padEnd(19) + "| " + String(r.cnt).padStart(6) + " | " + (decTotal>0?(r.cnt/decTotal*100).toFixed(1):"0") + "%");

    // 6. Average order value
    console.log("\n--- 6. AVG ORDER VALUE ---");
    const q6 = querySql(`
      SELECT CASE WHEN o.tx_type IN ('cp_initial','initial_salvage','straight_sale') THEN 'Initials'
        WHEN o.tx_type IN ('tp_rebill','tp_rebill_salvage') THEN 'Rebills'
        WHEN o.tx_type IN ('upsell','upsell_cascade') THEN 'Upsells'
        ELSE 'Other' END as grp,
        ROUND(AVG(CASE WHEN o.order_total > 0 AND o.order_status IN (2,6,8) THEN o.order_total END),2) as avg_approved,
        ROUND(AVG(CASE WHEN o.order_total > 0 THEN o.order_total END),2) as avg_all,
        COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as approved
      FROM orders o WHERE o.is_test = 0 AND o.is_internal_test = 0 AND ${f}
      GROUP BY grp ORDER BY grp
    `);
    console.log("  type     | avg (approved) | avg (all) | approved orders");
    console.log("  " + "-".repeat(55));
    for (const r of q6) console.log("  " + r.grp.padEnd(10) + "| $" + String(r.avg_approved||0).padStart(7) + "       | $" + String(r.avg_all||0).padStart(7) + "  | " + r.approved);
  }

  process.exit(0);
}
run();
