const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== MISMATCH 1: BR-001 BIN 403163 Initials ===");
  console.log("Engine: +52pp, $380/mo | Raw: +15.7pp, $115/mo");
  console.log();

  // Per gateway_id (not processor)
  const q1 = querySql(
    `SELECT o.gateway_id, g.processor_name, COUNT(*) as att,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app,
      ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders o JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
    WHERE o.cc_first_6 = '403163'
    AND o.tx_type IN ('cp_initial','initial_salvage','straight_sale')
    AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_status IN (2,6,7,8)
    AND g.gateway_active = 1 AND COALESCE(g.exclude_from_analysis,0) != 1
    GROUP BY o.gateway_id ORDER BY rate DESC`
  );
  console.log("Per gateway_id (active only):");
  for (const r of q1) console.log("  GW " + r.gateway_id + " " + (r.processor_name||"?").padEnd(10) + r.att + " att, " + r.app + " app, " + r.rate + "%");

  // The 100% rate — is a small-sample gateway?
  const best = q1[0];
  const current = q1.reduce((a,b) => b.att > a.att ? b : a, q1[0]);
  console.log();
  console.log("Best: GW " + best.gateway_id + " " + best.rate + "% (" + best.att + " att)");
  console.log("Current (most vol): GW " + current.gateway_id + " " + current.rate + "% (" + current.att + " att)");
  console.log("Issue: engine picks GW " + best.gateway_id + " at " + best.rate + "% but it only has " + best.att + " attempts!");

  console.log();
  console.log("=== MISMATCH 2: Discover rebill gateway ===");
  console.log("Engine: Paysafe 50% vs Cliq 21.4%");
  console.log();

  // Per gateway at $76-100
  const q2a = querySql(
    `SELECT o.gateway_id, g.processor_name, COUNT(*) as att,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app,
      ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
    WHERE b.issuer_bank = 'DISCOVER ISSUER'
    AND o.tx_type = 'tp_rebill' AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.order_status IN (2,6,7,8) AND o.order_total >= 76 AND o.order_total <= 100
    AND g.gateway_active = 1 AND COALESCE(g.exclude_from_analysis,0) != 1
    GROUP BY o.gateway_id ORDER BY rate DESC`
  );
  console.log("At $76-100 per gateway:");
  for (const r of q2a) console.log("  GW " + r.gateway_id + " " + (r.processor_name||"?").padEnd(10) + r.att + " att, " + r.app + " app, " + r.rate + "%");

  // Where does 50% come from? Check $51-75
  const q2b = querySql(
    `SELECT o.gateway_id, g.processor_name, COUNT(*) as att,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app,
      ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
    WHERE b.issuer_bank = 'DISCOVER ISSUER'
    AND o.tx_type = 'tp_rebill' AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.order_status IN (2,6,7,8) AND o.order_total >= 51 AND o.order_total <= 75
    AND g.gateway_active = 1 AND COALESCE(g.exclude_from_analysis,0) != 1
    GROUP BY o.gateway_id HAVING COUNT(*) >= 2 ORDER BY rate DESC`
  );
  console.log("\nAt $51-75 per gateway:");
  for (const r of q2b) console.log("  GW " + r.gateway_id + " " + (r.processor_name||"?").padEnd(10) + r.att + " att, " + r.app + " app, " + r.rate + "%");

  // Check ALL buckets combined
  const q2c = querySql(
    `SELECT o.gateway_id, g.processor_name, COUNT(*) as att,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app,
      ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
    WHERE b.issuer_bank = 'DISCOVER ISSUER'
    AND o.tx_type = 'tp_rebill' AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.order_status IN (2,6,7,8)
    AND g.gateway_active = 1 AND COALESCE(g.exclude_from_analysis,0) != 1
    GROUP BY o.gateway_id HAVING COUNT(*) >= 5 ORDER BY rate DESC`
  );
  console.log("\nALL prices combined per gateway:");
  for (const r of q2c) console.log("  GW " + r.gateway_id + " " + (r.processor_name||"?").padEnd(10) + r.att + " att, " + r.app + " app, " + r.rate + "%");

  console.log();
  console.log("=== MISMATCH 3: Discover LTV ===");
  console.log("Engine: $73 vs $27 | Manual: $34 vs $22");
  console.log();

  // Check with and without exclude filter
  const q3a = querySql(
    `SELECT CASE WHEN o.order_total BETWEEN 76 AND 100 THEN '76-100' WHEN o.order_total BETWEEN 51 AND 75 THEN '51-75' END as bkt,
      COUNT(*) as att, ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE b.issuer_bank = 'DISCOVER ISSUER' AND o.tx_type = 'tp_rebill'
    AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_status IN (2,6,7,8) AND o.order_total BETWEEN 51 AND 100
    GROUP BY bkt`
  );
  console.log("WITHOUT gateway filter:");
  for (const r of q3a) console.log("  " + r.bkt + ": " + r.att + " att, " + r.rate + "%");

  const q3b = querySql(
    `SELECT CASE WHEN o.order_total BETWEEN 76 AND 100 THEN '76-100' WHEN o.order_total BETWEEN 51 AND 75 THEN '51-75' END as bkt,
      COUNT(*) as att, ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
    WHERE b.issuer_bank = 'DISCOVER ISSUER' AND o.tx_type = 'tp_rebill'
    AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_status IN (2,6,7,8)
    AND COALESCE(g.exclude_from_analysis,0) != 1
    AND o.order_total BETWEEN 51 AND 100
    GROUP BY bkt`
  );
  console.log("WITH exclude filter:");
  for (const r of q3b) console.log("  " + r.bkt + ": " + r.att + " att, " + r.rate + "%");

  // Check engine rebill query — does it join gateways?
  console.log();
  console.log("=== Root cause check: does flow-optix rebill query join gateways? ===");
  console.log("Reading flow-optix.js line ~449...");
  const fs = require("fs");
  const code = fs.readFileSync("./src/analytics/flow-optix.js", "utf8");
  const rebillQueryMatch = code.match(/Step 2:.*?rebillRows = querySql\(`([\s\S]*?)`/);
  if (rebillQueryMatch) {
    console.log("Rebill query excerpt:");
    console.log(rebillQueryMatch[1].substring(0, 400));
  }

  process.exit(0);
}
run();
