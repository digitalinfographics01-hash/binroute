const Database = require("better-sqlite3");

async function run() {
  const db = new Database("./data/binroute.db");

  function compLtv(rate, price, cycles) {
    let ret = 100, rev = 0;
    for (let i = 0; i < cycles; i++) { ret *= rate/100; rev += ret * price; }
    return Math.round(rev/100 * 100)/100;
  }

  console.log("════════════════════════════════════════════════════════");
  console.log("RAW SQL VERIFICATION — independent of engine code");
  console.log("════════════════════════════════════════════════════════");

  // 1. BR-001 Initials
  console.log("\n=== 1. BR-001: BIN 403163 Initials ===");
  console.log("Engine: Paysafe vs Cliq, +52pp, $380/mo");
  const q1 = db.prepare(`SELECT g.processor_name, COUNT(*) as att, COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app, ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate, ROUND(AVG(CASE WHEN o.order_total > 0 THEN o.order_total END),2) as avg FROM orders o JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1 WHERE o.cc_first_6 = '403163' AND o.tx_type IN ('cp_initial','initial_salvage','straight_sale') AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_status IN (2,6,7,8) AND g.gateway_active = 1 AND COALESCE(g.exclude_from_analysis,0) != 1 GROUP BY g.processor_name HAVING COUNT(*) >= 20 ORDER BY rate DESC`).all();
  for (const r of q1) console.log("  " + (r.processor_name||"?") + ": " + r.att + " att, " + r.app + " app, " + r.rate + "%, avg $" + r.avg);
  const cliq = q1.find(r => r.processor_name === "Cliq");
  const paysafe = q1.find(r => r.processor_name === "Paysafe");
  if (cliq && paysafe) {
    const lift = paysafe.rate - cliq.rate;
    const mo = Math.round(cliq.att * 30 / 90);
    const rev = Math.round(mo * (lift/100) * cliq.avg * 100) / 100;
    console.log("  Manual: lift=" + lift.toFixed(1) + "pp, mo_att=" + mo + ", avg=$" + cliq.avg + " → $" + rev + "/mo");
    console.log("  Engine: $380.44/mo → " + (Math.abs(rev - 380.44) < 50 ? "CLOSE ✓" : "MISMATCH ✗ diff=$" + Math.abs(rev-380.44).toFixed(0)));
  }

  // 2. Discover rebill gateway
  console.log("\n=== 2. Discover rebills at $76-100, active gateways ===");
  console.log("Engine: Paysafe 50% vs Cliq 21.4%");
  const q2 = db.prepare(`SELECT g.processor_name, COUNT(*) as att, COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app, ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1 WHERE b.issuer_bank = 'DISCOVER ISSUER' AND o.tx_type = 'tp_rebill' AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_status IN (2,6,7,8) AND o.order_total >= 76 AND o.order_total <= 100 AND g.gateway_active = 1 AND COALESCE(g.exclude_from_analysis,0) != 1 GROUP BY g.processor_name HAVING COUNT(*) >= 5 ORDER BY rate DESC`).all();
  for (const r of q2) {
    const match = (r.processor_name==="Paysafe" && r.rate==50) || (r.processor_name==="Cliq" && Math.abs(r.rate-21.4)<1);
    console.log("  " + r.processor_name + ": " + r.att + " att, " + r.rate + "% → " + (match ? "MATCH ✓" : "CHECK"));
  }

  // 3. Discover LTV
  console.log("\n=== 3. Discover LTV compounding ===");
  console.log("Engine: $51-75 LTV $73 vs $76-100 LTV $27");
  const q3 = db.prepare(`SELECT CASE WHEN o.order_total BETWEEN 76 AND 100 THEN '76-100' WHEN o.order_total BETWEEN 51 AND 75 THEN '51-75' END as bkt, COUNT(*) as att, ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1 WHERE b.issuer_bank = 'DISCOVER ISSUER' AND o.tx_type = 'tp_rebill' AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_status IN (2,6,7,8) AND COALESCE(g.exclude_from_analysis,0) != 1 AND o.order_total BETWEEN 51 AND 100 GROUP BY bkt`).all();
  let rate76=0, rate51=0;
  for (const r of q3) {
    console.log("  " + r.bkt + ": " + r.att + " att, " + r.rate + "% approval");
    if (r.bkt==='76-100') rate76=r.rate; if (r.bkt==='51-75') rate51=r.rate;
  }
  const ltv76 = compLtv(rate76, 88, 5);
  const ltv51 = compLtv(rate51, 63, 5);
  console.log("  Manual: $76-100=$" + ltv76 + " | $51-75=$" + ltv51);
  console.log("  Engine: $76-100=$27 | $51-75=$73");
  console.log("  " + (Math.abs(ltv76-27)<3 ? "✓" : "✗ diff=" + (ltv76-27).toFixed(1)) + " | " + (Math.abs(ltv51-73)<3 ? "✓" : "✗ diff=" + (ltv51-73).toFixed(1)));

  // 4. Eligibility: Sutton Bank insuf
  console.log("\n=== 4. Sutton Bank insufficient funds recovery ===");
  console.log("Engine: 2.3% → ALLOW via timing");
  const q4 = db.prepare(`SELECT COUNT(*) as att, COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END) as rec, ROUND(COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate FROM orders d LEFT JOIN bin_lookup b ON d.cc_first_6 = b.bin LEFT JOIN orders r ON r.customer_id = d.customer_id AND r.product_group_id = d.product_group_id AND r.derived_cycle = d.derived_cycle AND r.attempt_number > d.attempt_number AND r.order_status IN (2,6,8) AND r.is_test = 0 WHERE d.client_id = 1 AND d.tx_type IN ('tp_rebill','tp_rebill_salvage') AND d.is_test = 0 AND d.is_internal_test = 0 AND d.order_status = 7 AND d.decline_reason = 'Insufficient funds' AND b.issuer_bank = 'SUTTON BANK' AND b.card_type = 'DEBIT'`).get();
  if (q4) { console.log("  Raw: " + q4.att + " att, " + q4.rec + " rec, " + q4.rate + "% → " + (q4.rate==2.3?"MATCH ✓":"DIFF ✗")); }

  // 5. DNH Citibank
  console.log("\n=== 5. Citibank Credit DNH ===");
  console.log("Engine: 4.2% → ALLOW");
  const q5 = db.prepare(`SELECT COUNT(*) as att, COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END) as rec, ROUND(COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate FROM orders d LEFT JOIN bin_lookup b ON d.cc_first_6 = b.bin LEFT JOIN orders r ON r.customer_id = d.customer_id AND r.product_group_id = d.product_group_id AND r.derived_cycle = d.derived_cycle AND r.attempt_number > d.attempt_number AND r.order_status IN (2,6,8) AND r.is_test = 0 WHERE d.client_id = 1 AND d.tx_type IN ('tp_rebill','tp_rebill_salvage') AND d.is_test = 0 AND d.is_internal_test = 0 AND d.order_status = 7 AND d.decline_reason = 'Do Not Honor' AND b.issuer_bank = 'CITIBANK N.A.' AND b.card_type = 'CREDIT'`).get();
  if (q5) { console.log("  Raw: " + q5.att + " att, " + q5.rec + " rec, " + q5.rate + "% → " + (q5.rate==4.2?"MATCH ✓":"DIFF ✗")); }

  // 6. Prepaid split
  console.log("\n=== 6. Sutton Bank VISA prepaid split ===");
  console.log("Engine: prepaid card = 1118 att, 5 BINs");
  const q6 = db.prepare(`SELECT b.is_prepaid, COUNT(*) as att, COUNT(DISTINCT o.cc_first_6) as bins FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin WHERE b.issuer_bank = 'SUTTON BANK' AND b.card_brand = 'VISA' AND o.tx_type IN ('tp_rebill','tp_rebill_salvage') AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_status IN (2,6,7,8) GROUP BY b.is_prepaid`).all();
  for (const r of q6) {
    console.log("  prepaid=" + r.is_prepaid + ": " + r.att + " att, " + r.bins + " bins");
    if (r.is_prepaid===1) console.log("  Engine says 1118 att, 5 bins → " + (Math.abs(r.att-1118)<20 ? "CLOSE ✓" : "DIFF ✗ actual=" + r.att));
  }

  // 7. DNH override: Discover Credit
  console.log("\n=== 7. DNH override: Discover Credit ===");
  console.log("Engine: 2.7% → ALLOW via override");
  const q7 = db.prepare(`SELECT COUNT(*) as att, COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END) as rec, ROUND(COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate FROM orders d LEFT JOIN bin_lookup b ON d.cc_first_6 = b.bin LEFT JOIN orders r ON r.customer_id = d.customer_id AND r.product_group_id = d.product_group_id AND r.derived_cycle = d.derived_cycle AND r.attempt_number > d.attempt_number AND r.order_status IN (2,6,8) AND r.is_test = 0 WHERE d.client_id = 1 AND d.tx_type IN ('tp_rebill','tp_rebill_salvage') AND d.is_test = 0 AND d.is_internal_test = 0 AND d.order_status = 7 AND d.decline_reason = 'Do Not Honor' AND b.issuer_bank = 'DISCOVER ISSUER' AND b.card_type = 'CREDIT'`).get();
  if (q7) { console.log("  Raw: " + q7.att + " att, " + q7.rec + " rec, " + q7.rate + "% → " + (q7.rate==2.7?"MATCH ✓":"DIFF ✗")); }

  console.log("\n════════════════════════════════════════════════════════");
  db.close();
}
run();
