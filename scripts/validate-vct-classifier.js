const {initDb, querySql} = require('../src/db/connection');
(async () => {
  await initDb();
  var f = "client_id=6 AND is_test=0 AND COALESCE(is_internal_test,0)=0";

  // 1. Upsell camps we haven't confirmed
  console.log("=== UNCONFIRMED UPSELL CAMPS ===");
  var upsellCamps = [77, 88, 100, 101, 102, 98, 79, 82, 84];
  for (var c of upsellCamps) {
    var r = querySql(
      "SELECT pc.product_name, COUNT(*) as orders, ROUND(AVG(o.order_total),2) as avg FROM orders o " +
      "LEFT JOIN products_catalog pc ON pc.client_id=o.client_id AND pc.product_id=CAST(o.main_product_id AS TEXT) " +
      "WHERE o." + f + " AND o.campaign_id=? AND o.billing_cycle=0 GROUP BY pc.product_name ORDER BY orders DESC LIMIT 2", [c]
    );
    var line = "Camp " + c + ": ";
    r.forEach(function(p) { line += p.orders + "x $" + p.avg + " [" + (p.product_name || "NULL") + "] "; });
    console.log(line);
  }

  // 2. retry_attempt population check
  console.log("\n=== RETRY_ATTEMPT POPULATION ===");
  var retryCheck = querySql(
    "SELECT CASE WHEN retry_attempt IS NULL THEN 'NULL' WHEN retry_attempt = 0 THEN '0' ELSE 'gt0' END as retry_val, " +
    "COUNT(*) as orders FROM orders WHERE " + f + " GROUP BY retry_val"
  );
  console.table(retryCheck);

  // 3. Reprocessing check — customers with >1 cycle-0 on same MAIN campaign
  console.log("\n=== MAIN INITIAL: POTENTIAL REPROCESSING ===");
  var reprocess = querySql(
    "SELECT COUNT(*) as customers, SUM(orders) as total_orders, SUM(orders - 1) as extra_attempts FROM (" +
    "  SELECT customer_id, campaign_id, COUNT(*) as orders FROM orders " +
    "  WHERE " + f + " AND customer_id IS NOT NULL AND billing_cycle=0 AND order_total > 0 " +
    "  AND campaign_id NOT IN (29,73,41,64,77,13,86,88,100,101,102,98,79,82,84,37,49,7,12,3,11,60,4) " +
    "  GROUP BY customer_id, campaign_id HAVING COUNT(*) > 1" +
    ")"
  );
  console.log("Customers with multiple cycle-0 on same main campaign:");
  console.table(reprocess);

  // 4. Sample reprocessing candidates
  console.log("\n=== SAMPLE REPROCESSING CANDIDATES ===");
  var candidates = querySql(
    "SELECT customer_id, campaign_id, COUNT(*) as c0_orders FROM orders " +
    "WHERE " + f + " AND customer_id IS NOT NULL AND billing_cycle=0 AND order_total > 0 " +
    "AND campaign_id NOT IN (29,73,41,64,77,13,86,88,100,101,102,98,79,82,84,37,49,7,12,3,11,60,4) " +
    "GROUP BY customer_id, campaign_id HAVING COUNT(*) >= 3 ORDER BY RANDOM() LIMIT 5"
  );
  for (var c of candidates) {
    console.log("\nCustomer " + c.customer_id + " camp " + c.campaign_id + " (" + c.c0_orders + " cycle-0 orders):");
    var orders = querySql(
      "SELECT order_id, ROUND(order_total,2) as total, order_status as st, is_cascaded as casc, " +
      "gateway_id as gw, acquisition_date as date FROM orders " +
      "WHERE client_id=6 AND customer_id=? AND campaign_id=? AND billing_cycle=0 ORDER BY order_id LIMIT 10",
      [c.customer_id, c.campaign_id]
    );
    orders.forEach(function(o) {
      var sl = [2,6,8].indexOf(o.st) >= 0 ? "OK  " : "DECL";
      console.log("  " + o.order_id + " | $" + String(o.total).padStart(7) + " | " + sl + " | gw " + String(o.gw).padStart(3) + " | casc " + o.casc + " | " + o.date);
    });
  }

  // 5. Cascade check on main initials
  console.log("\n=== MAIN INITIAL: CASCADE CHECK ===");
  var cascCheck = querySql(
    "SELECT is_cascaded, COUNT(*) as orders FROM orders WHERE " + f + " AND customer_id IS NOT NULL " +
    "AND billing_cycle=0 AND order_total > 0 " +
    "AND campaign_id NOT IN (29,73,41,64,77,13,86,88,100,101,102,98,79,82,84,37,49,7,12,3,11,60,4) " +
    "GROUP BY is_cascaded"
  );
  console.table(cascCheck);

  // 6. Are cascaded main_initial orders actually cascade retries (not true initials)?
  console.log("\n=== CASCADED MAIN INITIALS — SAMPLE ===");
  var cascSample = querySql(
    "SELECT customer_id FROM orders WHERE " + f + " AND customer_id IS NOT NULL " +
    "AND billing_cycle=0 AND order_total > 0 AND is_cascaded=1 " +
    "AND campaign_id NOT IN (29,73,41,64,77,13,86,88,100,101,102,98,79,82,84,37,49,7,12,3,11,60,4) " +
    "ORDER BY RANDOM() LIMIT 3"
  );
  for (var c of cascSample) {
    console.log("\nCustomer " + c.customer_id + " (cascaded initial):");
    var orders = querySql(
      "SELECT order_id, campaign_id as camp, billing_cycle as cyc, ROUND(order_total,2) as total, " +
      "order_status as st, is_cascaded as casc, gateway_id as gw, acquisition_date as date " +
      "FROM orders WHERE client_id=6 AND customer_id=? AND billing_cycle=0 ORDER BY order_id LIMIT 10",
      [c.customer_id]
    );
    orders.forEach(function(o) {
      var sl = [2,6,8].indexOf(o.st) >= 0 ? "OK  " : "DECL";
      console.log("  " + o.order_id + " | camp " + String(o.camp).padStart(3) + " | $" + String(o.total).padStart(7) + " | " + sl + " | gw " + String(o.gw).padStart(3) + " | casc " + o.casc + " | " + o.date);
    });
  }
})();
