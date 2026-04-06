const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");
const { daysAgoFilter } = require("../src/analytics/engine");

async function run() {
  await initializeDatabase();
  const df = daysAgoFilter(90);

  function compLtv(rate, price, cycles) {
    let ret = 100, rev = 0;
    for (let i = 0; i < cycles; i++) { ret *= rate/100; rev += ret * price; }
    return Math.round(rev/100 * 100)/100;
  }

  console.log("════════════════════════════════════════════");
  console.log("VERIFICATION WITH 90-DAY FILTER (matching engine)");
  console.log("════════════════════════════════════════════");

  // 1. Discover gateway at $76-100
  console.log("\n=== Discover gateway at $76-100 (90-day) ===");
  const q1 = querySql(`
    SELECT o.gateway_id, g.processor_name, COUNT(*) as att,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app,
      ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
    WHERE b.issuer_bank = 'DISCOVER ISSUER' AND b.is_prepaid = 0
    AND o.tx_type = 'tp_rebill' AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.order_status IN (2,6,7,8) AND o.order_total >= 76 AND o.order_total <= 100
    AND g.gateway_active = 1 AND COALESCE(g.exclude_from_analysis,0) != 1
    AND ${df}
    GROUP BY o.gateway_id ORDER BY rate DESC
  `);
  for (const r of q1) console.log("  GW " + r.gateway_id + " " + (r.processor_name||"?").padEnd(10) + r.att + " att, " + r.app + " app, " + r.rate + "%");

  // 2. Discover bucket rates
  console.log("\n=== Discover bucket rates (90-day) ===");
  const q2 = querySql(`
    SELECT CASE WHEN o.order_total <= 25 THEN '$0-25' WHEN o.order_total <= 50 THEN '$26-50'
      WHEN o.order_total <= 75 THEN '$51-75' WHEN o.order_total <= 100 THEN '$76-100' ELSE '$100+' END as bkt,
      COUNT(*) as att, COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app,
      ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
    WHERE b.issuer_bank = 'DISCOVER ISSUER' AND b.is_prepaid = 0
    AND o.tx_type = 'tp_rebill' AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.order_status IN (2,6,7,8) AND o.order_total > 0
    AND COALESCE(g.exclude_from_analysis,0) != 1 AND ${df}
    GROUP BY bkt ORDER BY bkt
  `);
  for (const r of q2) {
    const ltv = compLtv(r.rate, r.bkt === '$76-100' ? 88 : r.bkt === '$51-75' ? 63 : r.bkt === '$26-50' ? 38 : 13, 5);
    console.log("  " + r.bkt + ": " + r.att + " att, " + r.app + " app, " + r.rate + "% → LTV $" + ltv);
  }
  console.log("  Engine says: $51-75=55%, LTV $73 | $76-100=23.3%, LTV $27");

  // 3. BR-001 BIN 403163
  console.log("\n=== BR-001: BIN 403163 (90-day) ===");
  const q3 = querySql(`
    SELECT o.gateway_id, g.processor_name, COUNT(*) as att,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app,
      ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate,
      ROUND(AVG(CASE WHEN o.order_total > 0 THEN o.order_total END),2) as avg
    FROM orders o JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
    WHERE o.cc_first_6 = '403163'
    AND o.tx_type IN ('cp_initial','initial_salvage','straight_sale')
    AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_status IN (2,6,7,8)
    AND g.gateway_active = 1 AND COALESCE(g.exclude_from_analysis,0) != 1
    AND ${df}
    GROUP BY o.gateway_id HAVING COUNT(*) >= 20 ORDER BY rate DESC
  `);
  for (const r of q3) console.log("  GW " + r.gateway_id + " " + (r.processor_name||"?").padEnd(10) + r.att + " att, " + r.app + " app, " + r.rate + "%, avg $" + r.avg);
  if (q3.length >= 2) {
    const best = q3[0];
    const current = q3.reduce((a,b) => b.att > a.att ? b : a, q3[0]);
    const lift = best.rate - current.rate;
    const mo = Math.round(current.att * 30 / 90);
    const rev = Math.round(mo * (lift/100) * (current.avg||0) * 100) / 100;
    console.log("  Best: GW " + best.gateway_id + " " + best.rate + "% | Current: GW " + current.gateway_id + " " + current.rate + "%");
    console.log("  Lift: " + lift.toFixed(1) + "pp | Mo att: " + mo + " | Rev: $" + rev + "/mo");
    console.log("  Engine: $380/mo");
  }

  // 4. Sutton Bank prepaid count
  console.log("\n=== Sutton Bank VISA prepaid (90-day) ===");
  const q4 = querySql(`
    SELECT o.tx_type, COUNT(*) as att
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
    WHERE b.issuer_bank = 'SUTTON BANK' AND b.card_brand = 'VISA' AND b.is_prepaid = 1
    AND o.tx_type IN ('tp_rebill','tp_rebill_salvage')
    AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_status IN (2,6,7,8) AND o.order_total > 0
    AND COALESCE(g.exclude_from_analysis,0) != 1 AND ${df}
    GROUP BY o.tx_type
  `);
  let total = 0;
  for (const r of q4) { console.log("  " + r.tx_type + ": " + r.att); total += r.att; }
  console.log("  Total: " + total + " (engine says 1118)");

  console.log("\n════════════════════════════════════════════");
  process.exit(0);
}
run();
