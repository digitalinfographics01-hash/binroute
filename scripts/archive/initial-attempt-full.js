const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  // Pre-check: verify DB is reasonably complete
  const total = querySql("SELECT COUNT(*) as c FROM orders WHERE is_test=0")[0].c;
  console.log("Total orders in DB:", total);
  if (total < 40000) {
    console.log("WARNING: DB appears incomplete (< 40K). Import may still be running.");
    console.log("Proceeding anyway...");
  }
  console.log();

  // Build a temp view of derived_initial_attempt + campaign signal
  // This is expensive so we do it once and reuse
  console.log("Building derived_initial_attempt data (this takes a while)...");
  const t0 = Date.now();

  const orders = querySql(`
    SELECT o1.order_id, o1.customer_id, o1.product_ids, o1.campaign_id,
      o1.order_status, o1.tx_type, o1.acquisition_date,
      o1.product_group_id, o1.product_group_name
    FROM orders o1
    WHERE o1.billing_cycle = 0 AND o1.is_recurring = 0
    AND o1.product_type_classified IN ('initial','initial_rebill')
    AND o1.customer_id IS NOT NULL
    AND o1.is_test = 0 AND o1.is_internal_test = 0
    ORDER BY o1.customer_id, o1.product_ids, o1.acquisition_date
  `);

  // Group by customer+product_ids and assign attempt numbers
  const enriched = [];
  let lastKey = null;
  let attempt = 0;
  let priorOrders = [];

  for (const o of orders) {
    const key = o.customer_id + "|" + o.product_ids;
    if (key !== lastKey) {
      lastKey = key;
      attempt = 1;
      priorOrders = [];
    } else {
      attempt++;
    }

    const priorDecline = priorOrders.length > 0 ? priorOrders[priorOrders.length - 1] : null;
    const sameCampaign = priorDecline ? priorDecline.campaign_id === o.campaign_id : null;
    const hoursSincePrior = priorDecline
      ? (new Date(o.acquisition_date) - new Date(priorDecline.acquisition_date)) / 3600000
      : null;

    enriched.push({
      ...o,
      attempt,
      sameCampaign,
      hoursSincePrior,
      isApproved: [2, 6, 8].includes(o.order_status),
    });

    priorOrders.push(o);
  }

  console.log("Built " + enriched.length + " enriched orders in " + Math.round((Date.now() - t0) / 1000) + "s");
  console.log();

  // 1. Attempt distribution with campaign signal
  console.log("=== 1. Attempt distribution with campaign signal ===");
  const att1 = {};
  for (const o of enriched) {
    const a = Math.min(o.attempt, 6); // bucket 6+
    const label = a >= 6 ? "6+" : String(a);
    if (!att1[label]) att1[label] = { sameCnt: 0, diffCnt: 0, sameApp: 0, diffApp: 0 };
    if (o.attempt === 1) {
      att1[label].sameCnt++; // attempt 1 has no prior
      if (o.isApproved) att1[label].sameApp++;
    } else if (o.sameCampaign) {
      att1[label].sameCnt++;
      if (o.isApproved) att1[label].sameApp++;
    } else {
      att1[label].diffCnt++;
      if (o.isApproved) att1[label].diffApp++;
    }
  }
  console.log("  attempt | same_camp | diff_camp | same_app% | diff_app%");
  console.log("  " + "-".repeat(55));
  for (const [a, d] of Object.entries(att1).sort((a,b) => a[0].localeCompare(b[0]))) {
    const sr = d.sameCnt > 0 ? (d.sameApp / d.sameCnt * 100).toFixed(1) : "—";
    const dr = d.diffCnt > 0 ? (d.diffApp / d.diffCnt * 100).toFixed(1) : "—";
    console.log("  " + a.padStart(7) + " | " + String(d.sameCnt).padStart(9) + " | " + String(d.diffCnt).padStart(9) + " | " + sr.padStart(9) + " | " + dr.padStart(9));
  }

  // 2. cp_initial_retry candidates (attempt > 1, same campaign)
  console.log();
  console.log("=== 2. cp_initial_retry candidates (att > 1, same campaign) ===");
  const retries = enriched.filter(o => o.attempt > 1 && o.sameCampaign);
  const retryByAtt = {};
  for (const o of retries) {
    const a = Math.min(o.attempt, 6);
    const label = a >= 6 ? "6+" : String(a);
    if (!retryByAtt[label]) retryByAtt[label] = { cnt: 0, app: 0, hours: [] };
    retryByAtt[label].cnt++;
    if (o.isApproved) retryByAtt[label].app++;
    if (o.hoursSincePrior != null) retryByAtt[label].hours.push(o.hoursSincePrior);
  }
  console.log("  attempt | count | approval | avg_hrs_since_prior");
  console.log("  " + "-".repeat(50));
  for (const [a, d] of Object.entries(retryByAtt).sort((a,b) => a[0].localeCompare(b[0]))) {
    const rate = d.cnt > 0 ? (d.app / d.cnt * 100).toFixed(1) : "0";
    const avgH = d.hours.length > 0 ? (d.hours.reduce((s,h) => s + h, 0) / d.hours.length).toFixed(1) : "—";
    console.log("  " + a.padStart(7) + " | " + String(d.cnt).padStart(5) + " | " + (rate + "%").padStart(8) + " | " + avgH);
  }

  // 3. initial_salvage candidates (attempt > 1, different campaign)
  console.log();
  console.log("=== 3. initial_salvage candidates (att > 1, diff campaign) ===");
  const salvages = enriched.filter(o => o.attempt > 1 && !o.sameCampaign);
  const salvByAtt = {};
  for (const o of salvages) {
    const a = Math.min(o.attempt, 6);
    const label = a >= 6 ? "6+" : String(a);
    if (!salvByAtt[label]) salvByAtt[label] = { cnt: 0, app: 0, hours: [] };
    salvByAtt[label].cnt++;
    if (o.isApproved) salvByAtt[label].app++;
    if (o.hoursSincePrior != null) salvByAtt[label].hours.push(o.hoursSincePrior);
  }
  console.log("  attempt | count | approval | avg_hrs_since_prior");
  console.log("  " + "-".repeat(50));
  for (const [a, d] of Object.entries(salvByAtt).sort((a,b) => a[0].localeCompare(b[0]))) {
    const rate = d.cnt > 0 ? (d.app / d.cnt * 100).toFixed(1) : "0";
    const avgH = d.hours.length > 0 ? (d.hours.reduce((s,h) => s + h, 0) / d.hours.length).toFixed(1) : "—";
    console.log("  " + a.padStart(7) + " | " + String(d.cnt).padStart(5) + " | " + (rate + "%").padStart(8) + " | " + avgH);
  }

  // 4. Reclassification counts
  console.log();
  console.log("=== 4. Reclassification impact ===");
  const cpInitRetry = enriched.filter(o => o.tx_type === "cp_initial" && o.attempt > 1 && o.sameCampaign);
  const cpInitSalvage = enriched.filter(o => o.tx_type === "cp_initial" && o.attempt > 1 && !o.sameCampaign);
  console.log("  Current cp_initial that would become cp_initial_retry:", cpInitRetry.length);
  console.log("  Current cp_initial that would become initial_salvage:", cpInitSalvage.length);

  // 5. Current vs new initial_salvage
  console.log();
  console.log("=== 5. initial_salvage counts ===");
  const currentIS = enriched.filter(o => o.tx_type === "initial_salvage").length;
  const newIS = enriched.filter(o => o.attempt > 1 && !o.sameCampaign).length;
  console.log("  Current initial_salvage:", currentIS);
  console.log("  New initial_salvage (after reclass):", newIS);

  // 6. Time gap for cp_initial_retry
  console.log();
  console.log("=== 6. Time gap for same-campaign retries ===");
  const buckets = { "0-6 hrs": 0, "6-12 hrs": 0, "12-24 hrs": 0, "1-3 days": 0, "3+ days": 0 };
  for (const o of retries) {
    const h = o.hoursSincePrior || 0;
    if (h <= 6) buckets["0-6 hrs"]++;
    else if (h <= 12) buckets["6-12 hrs"]++;
    else if (h <= 24) buckets["12-24 hrs"]++;
    else if (h <= 72) buckets["1-3 days"]++;
    else buckets["3+ days"]++;
  }
  console.log("  bucket     | count");
  console.log("  " + "-".repeat(25));
  for (const [b, c] of Object.entries(buckets)) console.log("  " + b.padEnd(13) + "| " + c);

  process.exit(0);
}
run();
