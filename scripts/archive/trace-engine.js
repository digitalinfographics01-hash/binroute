/**
 * Trace exactly what the flow-optix engine computes for Discover
 * by injecting logging into the data flow.
 */
const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");
const { daysAgoFilter, CLEAN_FILTER } = require("../src/analytics/engine");

async function run() {
  await initializeDatabase();

  // Run the EXACT same query the engine uses for rebillRows
  const days = 90;
  const rebillRows = querySql(`
    SELECT o.cc_first_6 AS bin, o.gateway_id, o.order_total, o.order_status,
      b.issuer_bank, b.card_brand, b.card_type, b.card_level, b.is_prepaid,
      g.gateway_alias, g.processor_name
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = 1 AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.tx_type = 'tp_rebill'
      AND o.order_status IN (2,6,7,8) AND o.order_total > 0
      AND COALESCE(g.exclude_from_analysis, 0) != 1
      AND ${daysAgoFilter(days)}
  `);

  console.log("Total rebillRows:", rebillRows.length);

  // Filter to Discover only
  const discoverRows = rebillRows.filter(r => r.issuer_bank === "DISCOVER ISSUER" && r.card_brand === "DISCOVER" && !r.is_prepaid);
  console.log("Discover non-prepaid rebillRows:", discoverRows.length);

  // Bucket them exactly like the engine does
  const BUCKETS = [
    { label: '$0-25',   min: 0,   max: 25 },
    { label: '$26-50',  min: 26,  max: 50 },
    { label: '$51-75',  min: 51,  max: 75 },
    { label: '$76-100', min: 76,  max: 100 },
    { label: '$100+',   min: 101, max: 999999 },
  ];

  function getBucket(total) {
    for (const b of BUCKETS) {
      if (total >= b.min && total <= b.max) return b.label;
    }
    return '$100+';
  }

  const buckets = {};
  for (const b of BUCKETS) buckets[b.label] = { att: 0, app: 0 };

  for (const row of discoverRows) {
    const bkt = getBucket(row.order_total);
    buckets[bkt].att++;
    if ([2, 6, 8].includes(row.order_status)) buckets[bkt].app++;
  }

  console.log("\nBucket rates (engine logic, Discover non-prepaid):");
  for (const [label, data] of Object.entries(buckets)) {
    const rate = data.att > 0 ? (data.app / data.att * 100).toFixed(1) : "0";
    console.log("  " + label + ": " + data.att + " att, " + data.app + " app, " + rate + "%");
  }

  // Now check: what if is_prepaid is null/undefined for some rows?
  const prepaidVals = {};
  for (const r of discoverRows) {
    const key = String(r.is_prepaid);
    prepaidVals[key] = (prepaidVals[key] || 0) + 1;
  }
  console.log("\nis_prepaid values in Discover rebillRows:", JSON.stringify(prepaidVals));

  // Check: what does buildGroupKey produce?
  // Level 2: issuer|brand|prepaid
  // If is_prepaid is null, the key becomes "DISCOVER ISSUER|DISCOVER|0" (falsy → "0")
  // If is_prepaid is 1, key = "DISCOVER ISSUER|DISCOVER|1"
  const groups = {};
  for (const r of rebillRows) {
    if (r.issuer_bank !== "DISCOVER ISSUER") continue;
    const prep = r.is_prepaid ? "1" : "0";
    const key = r.issuer_bank + "|" + (r.card_brand || "Unknown") + "|" + prep;
    if (!groups[key]) groups[key] = { att: 0, app: 0 };
    groups[key].att++;
    if ([2, 6, 8].includes(r.order_status)) groups[key].app++;
  }
  console.log("\nDiscover group keys in rebillRows:");
  for (const [k, v] of Object.entries(groups)) {
    console.log("  " + k + ": " + v.att + " att, " + v.app + " app");
  }

  // Check daysAgoFilter impact
  const allTimeRows = querySql(`
    SELECT COUNT(*) as cnt FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = 1 AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.tx_type = 'tp_rebill'
      AND o.order_status IN (2,6,7,8) AND o.order_total > 0
      AND COALESCE(g.exclude_from_analysis, 0) != 1
      AND b.issuer_bank = 'DISCOVER ISSUER'
  `);
  console.log("\nDiscover all-time:", allTimeRows[0].cnt, "vs 90-day:", discoverRows.length);

  // What is daysAgoFilter actually producing?
  console.log("\ndaysAgoFilter(90):", daysAgoFilter(90));

  // Check the actual date range of our data
  const dateRange = querySql("SELECT MIN(acquisition_date) as mn, MAX(acquisition_date) as mx FROM orders WHERE is_test = 0 AND order_status IN (2,6,7,8)");
  console.log("Data date range:", dateRange[0].mn, "to", dateRange[0].mx);

  process.exit(0);
}
run();
