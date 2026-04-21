const {initDb, querySql} = require('../src/db/connection');
(async () => {
  await initDb();
  var f = "client_id=6 AND is_test=0 AND COALESCE(is_internal_test,0)=0";

  // 1. Reprocessing samples — same customer, same campaign, >30 min gap
  console.log("=== INITIAL_REPROCESSING SAMPLES ===");
  // Find customers with cycle-0 orders >30min apart on same main campaign
  var reproc = querySql(
    "SELECT a.customer_id, a.campaign_id, a.order_id as o1, b.order_id as o2, " +
    "ROUND((julianday(b.acquisition_date) - julianday(a.acquisition_date)) * 24, 1) as hours_gap, " +
    "a.order_total as t1, b.order_total as t2, a.order_status as s1, b.order_status as s2 " +
    "FROM orders a JOIN orders b ON a.client_id=b.client_id AND a.customer_id=b.customer_id " +
    "AND a.campaign_id=b.campaign_id AND a.order_id < b.order_id " +
    "AND a.billing_cycle=0 AND b.billing_cycle=0 AND a.order_total > 0 AND b.order_total > 0 " +
    "AND (julianday(b.acquisition_date) - julianday(a.acquisition_date)) * 24 > 0.5 " +
    "AND (julianday(b.acquisition_date) - julianday(a.acquisition_date)) * 24 < 2400 " +
    "WHERE a.client_id=6 AND a.is_test=0 AND COALESCE(a.is_internal_test,0)=0 AND a.customer_id IS NOT NULL " +
    "AND a.campaign_id NOT IN (29,73,41,64,77,13,86,88,100,101,102,98,79,82,84,37,49,7,12,3,11,60,4) " +
    "ORDER BY RANDOM() LIMIT 8"
  );

  for (var r of reproc) {
    console.log("\nCustomer " + r.customer_id + " camp " + r.campaign_id + " (gap: " + r.hours_gap + " hours):");
    var orders = querySql(
      "SELECT order_id, campaign_id as camp, billing_cycle as cyc, ROUND(order_total,2) as total, " +
      "order_status as st, is_cascaded as casc, gateway_id as gw, acquisition_date as date " +
      "FROM orders WHERE client_id=6 AND customer_id=? ORDER BY order_id LIMIT 15",
      [r.customer_id]
    );
    orders.forEach(function(o) {
      var sl = [2,6,8].indexOf(o.st) >= 0 ? "OK  " : "DECL";
      var marker = (o.order_id == r.o1 || o.order_id == r.o2) ? " <<<" : "";
      console.log("  " + o.order_id + " | camp " + String(o.camp).padStart(3) + " | cyc " + String(o.cyc).padStart(2) + " | $" + String(o.total).padStart(7) + " | " + sl + " | gw " + String(o.gw).padStart(3) + " | casc " + o.casc + " | " + o.date + marker);
    });
  }

  // 2. Time gap distribution for reprocessing
  console.log("\n\n=== REPROCESSING TIME GAP DISTRIBUTION ===");
  var gaps = querySql(
    "SELECT CASE " +
    "  WHEN hours < 1 THEN '<1h' " +
    "  WHEN hours < 6 THEN '1-6h' " +
    "  WHEN hours < 24 THEN '6-24h' " +
    "  WHEN hours < 168 THEN '1-7d' " +
    "  WHEN hours < 720 THEN '7-30d' " +
    "  ELSE '>30d' END as gap_bucket, COUNT(*) as cnt " +
    "FROM (SELECT (julianday(b.acquisition_date) - julianday(a.acquisition_date)) * 24 as hours " +
    "  FROM orders a JOIN orders b ON a.client_id=b.client_id AND a.customer_id=b.customer_id " +
    "  AND a.campaign_id=b.campaign_id AND a.billing_cycle=0 AND b.billing_cycle=0 " +
    "  AND a.order_total > 0 AND b.order_total > 0 AND a.order_id < b.order_id " +
    "  AND (julianday(b.acquisition_date) - julianday(a.acquisition_date)) * 24 > 0.5 " +
    "  WHERE a.client_id=6 AND a.is_test=0 AND COALESCE(a.is_internal_test,0)=0 AND a.customer_id IS NOT NULL " +
    "  AND a.campaign_id NOT IN (29,73,41,64,77,13,86,88,100,101,102,98,79,82,84,37,49,7,12,3,11,60,4)" +
    ") GROUP BY gap_bucket ORDER BY MIN(hours)"
  );
  console.table(gaps);

  // 3. Camps 100,101,102,98 real-dollar orders — are they upsells?
  console.log("\n=== CAMPS 100,101,102,98 REAL-DOLLAR SAMPLES ===");
  var shippingCamps = [100, 101, 102, 98];
  for (var c of shippingCamps) {
    var sample = querySql(
      "SELECT customer_id FROM orders WHERE " + f + " AND campaign_id=? AND billing_cycle=0 AND order_total > 0 AND customer_id IS NOT NULL LIMIT 1", [c]
    );
    if (!sample.length) { console.log("Camp " + c + ": no real-dollar cycle-0"); continue; }
    console.log("\nCamp " + c + " — Customer " + sample[0].customer_id + ":");
    var orders = querySql(
      "SELECT order_id, campaign_id as camp, billing_cycle as cyc, ROUND(order_total,2) as total, " +
      "order_status as st, acquisition_date as date " +
      "FROM orders WHERE client_id=6 AND customer_id=? ORDER BY order_id LIMIT 10",
      [sample[0].customer_id]
    );
    orders.forEach(function(o) {
      var sl = [2,6,8].indexOf(o.st) >= 0 ? "OK  " : "DECL";
      console.log("  " + o.order_id + " | camp " + String(o.camp).padStart(3) + " | cyc " + String(o.cyc).padStart(2) + " | $" + String(o.total).padStart(7) + " | " + sl + " | " + o.date);
    });
  }

  // 4. Camp 84 — what is it?
  console.log("\n=== CAMP 84 DEEP LOOK ===");
  var c84sample = querySql(
    "SELECT customer_id FROM orders WHERE " + f + " AND campaign_id=84 AND customer_id IS NOT NULL LIMIT 2"
  );
  for (var c of c84sample) {
    console.log("\nCamp 84 — Customer " + c.customer_id + ":");
    var orders = querySql(
      "SELECT order_id, campaign_id as camp, billing_cycle as cyc, ROUND(order_total,2) as total, " +
      "order_status as st, acquisition_date as date " +
      "FROM orders WHERE client_id=6 AND customer_id=? ORDER BY order_id LIMIT 10",
      [c.customer_id]
    );
    orders.forEach(function(o) {
      var sl = [2,6,8].indexOf(o.st) >= 0 ? "OK  " : "DECL";
      console.log("  " + o.order_id + " | camp " + String(o.camp).padStart(3) + " | cyc " + String(o.cyc).padStart(2) + " | $" + String(o.total).padStart(7) + " | " + sl + " | " + o.date);
    });
  }

  // 5. Anonymous declines — any approved ones? (sanity check)
  console.log("\n=== ANONYMOUS DECLINE SANITY CHECK ===");
  var anonCheck = querySql(
    "SELECT order_status, COUNT(*) as orders FROM orders WHERE " + f + " AND customer_id IS NULL GROUP BY order_status"
  );
  console.table(anonCheck);

})().catch(e => { console.error('FAIL:', e); process.exit(1); });
