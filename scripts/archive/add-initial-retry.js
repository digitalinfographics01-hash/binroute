/**
 * Add cp_initial_retry tx_type and derived_initial_attempt field.
 * Run AFTER import is complete.
 *
 * 1. Add derived_initial_attempt column
 * 2. Calculate derived_initial_attempt for all orders
 * 3. Reclassify cp_initial → cp_initial_retry (same campaign, attempt > 1)
 * 4. Reclassify initial_salvage → cp_initial_retry (same campaign, attempt > 1)
 * 5. Show before/after
 */
const Database = require("better-sqlite3");

async function run() {
  const db = new Database("./data/binroute.db");

  // Pre-check
  const total = db.prepare("SELECT COUNT(*) as cnt FROM orders WHERE is_test = 0").get().cnt;
  console.log("Total orders:", total);

  // Show before distribution
  console.log();
  console.log("=== BEFORE ===");
  const before = db.prepare("SELECT COALESCE(tx_type,'NULL') as t, COUNT(*) as c FROM orders WHERE is_test=0 AND is_internal_test=0 GROUP BY tx_type ORDER BY c DESC").all();
  for (const r of before) console.log("  " + String(r.t).padEnd(25) + r.c);

  // CHANGE 2: Add derived_initial_attempt column
  console.log();
  console.log("=== Adding derived_initial_attempt column ===");
  const cols = db.prepare("PRAGMA table_info(orders)").all();
  const hasCol = cols.some(r => r.name === "derived_initial_attempt");
  if (!hasCol) {
    db.exec("ALTER TABLE orders ADD COLUMN derived_initial_attempt INTEGER DEFAULT NULL");
    console.log("  Column added.");
  } else {
    console.log("  Column already exists.");
  }

  // Calculate derived_initial_attempt for all initial orders
  console.log("  Calculating...");

  // Get all initial orders sorted for sequential processing
  const initials = db.prepare(`
    SELECT id, customer_id, product_ids, campaign_id, acquisition_date
    FROM orders
    WHERE billing_cycle = 0 AND is_recurring = 0
    AND product_type_classified IN ('initial','initial_rebill')
    AND is_test = 0 AND is_internal_test = 0
    ORDER BY customer_id, product_ids, acquisition_date
  `).all();

  let updated = 0;
  if (initials.length > 0) {
    let lastKey = null;
    let attempt = 0;

    for (const row of initials) {
      const { id, customer_id: custId, product_ids: prodIds, campaign_id: campId, acquisition_date: acqDate } = row;
      const key = (custId || "null") + "|" + (prodIds || "null");

      if (key !== lastKey) {
        lastKey = key;
        attempt = 1;
      } else {
        attempt++;
      }

      db.prepare("UPDATE orders SET derived_initial_attempt = " + attempt + " WHERE id = " + id).run();
      updated++;
    }
  }
  console.log("  Updated:", updated, "orders");

  // Set NULL customer → attempt 1
  db.exec("UPDATE orders SET derived_initial_attempt = 1 WHERE derived_initial_attempt IS NULL AND customer_id IS NULL AND billing_cycle = 0 AND product_type_classified IN ('initial','initial_rebill') AND is_test = 0");
  const anonSet = db.prepare("SELECT changes()").get()["changes()"];
  console.log("  Anonymous → attempt 1:", anonSet);

  // CHANGE 1: Reclassify to cp_initial_retry
  console.log();
  console.log("=== Reclassifying to cp_initial_retry ===");

  // Build lookup of prior campaign per customer+product+attempt
  // For each order with attempt > 1, check if same campaign as immediately prior attempt
  const retryCandiates = db.prepare(`
    SELECT o1.id, o1.order_id, o1.tx_type, o1.customer_id, o1.product_ids, o1.campaign_id, o1.acquisition_date
    FROM orders o1
    WHERE o1.derived_initial_attempt > 1
    AND o1.customer_id IS NOT NULL
    AND o1.is_test = 0 AND o1.is_internal_test = 0
    AND o1.tx_type IN ('cp_initial', 'initial_salvage')
    ORDER BY o1.customer_id, o1.product_ids, o1.acquisition_date
  `).all();

  let reclass = 0;
  if (retryCandiates.length > 0) {
    for (const row of retryCandiates) {
      const { id, order_id: orderId, tx_type: txType, customer_id: custId, product_ids: prodIds, campaign_id: campId, acquisition_date: acqDate } = row;

      // Find the immediately prior order for same customer + product
      const prior = db.prepare(`
        SELECT campaign_id FROM orders
        WHERE customer_id = ${custId} AND product_ids = '${(prodIds||"").replace(/'/g, "''")}'
        AND acquisition_date < '${acqDate}'
        AND billing_cycle = 0 AND product_type_classified IN ('initial','initial_rebill')
        AND is_test = 0
        ORDER BY acquisition_date DESC LIMIT 1
      `).get();

      if (prior) {
        if (prior.campaign_id === campId) {
          // Same campaign → cp_initial_retry
          db.prepare("UPDATE orders SET tx_type = 'cp_initial_retry' WHERE id = " + id).run();
          reclass++;
        }
      }
    }
  }
  console.log("  Reclassified to cp_initial_retry:", reclass);

  console.log("  Saved.");

  // Show after distribution
  console.log();
  console.log("=== AFTER ===");
  const after = db.prepare("SELECT COALESCE(tx_type,'NULL') as t, COUNT(*) as c FROM orders WHERE is_test=0 AND is_internal_test=0 GROUP BY tx_type ORDER BY c DESC").all();
  for (const r of after) console.log("  " + String(r.t).padEnd(25) + r.c);

  // Show derived_initial_attempt distribution
  console.log();
  console.log("=== derived_initial_attempt distribution ===");
  const attDist = db.prepare(`
    SELECT derived_initial_attempt, COUNT(*) as c
    FROM orders WHERE derived_initial_attempt IS NOT NULL AND is_test=0
    GROUP BY derived_initial_attempt ORDER BY derived_initial_attempt
  `).all();
  for (const r of attDist) console.log("  attempt " + r.derived_initial_attempt + ": " + r.c);

  const nullAtt = db.prepare("SELECT COUNT(*) as cnt FROM orders WHERE derived_initial_attempt IS NULL AND is_test=0").get().cnt;
  console.log("  NULL: " + nullAtt + " (non-initial orders — expected)");

  // cp_initial_retry threshold check
  console.log();
  console.log("=== NETWORK THRESHOLD CHECK ===");
  const retryCount = db.prepare("SELECT COUNT(*) as cnt FROM orders WHERE tx_type = 'cp_initial_retry' AND is_test=0").get().cnt;
  console.log("  cp_initial_retry count:", retryCount);
  console.log("  Threshold: 300");
  console.log("  Status:", retryCount >= 300 ? "ACTIVE — analysis enabled" : "PENDING — " + retryCount + " of 300 (" + Math.round(retryCount/300*100) + "%)");

  db.close();
}
run().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
