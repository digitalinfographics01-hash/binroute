/**
 * Full post-import pipeline:
 * 1. Repull overflow chunks
 * 2. Backfill Jan-Mar 2026 gaps
 * 3. Offer tagging
 * 4. Derived cycle recalc
 * 5. Decline category classification
 * 6. Prepaid backfill
 * 7. Reconciliation counts
 *
 * NOTE: Cascade chain backfill is now handled during import (chunked-import.js).
 *
 * Usage: node scripts/post-import-all.js [clientId]
 */
const fs = require("fs");
const { initializeDatabase } = require("../src/db/schema");
const { querySql, runSql, saveDb, closeDb, transaction, checkpointWal } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  // ═══════════════════════════════════════
  // STEP 1: Show current state
  // ═══════════════════════════════════════
  console.log("=== CURRENT STATE ===");
  const counts = querySql("SELECT strftime('%Y-%m', acquisition_date) as m, COUNT(*) as c FROM orders WHERE is_test=0 GROUP BY m ORDER BY m");
  for (const r of counts) console.log("  " + r.m + ": " + r.c);
  console.log("  Total:", counts.reduce((s,r) => s + r.c, 0));

  // ═══════════════════════════════════════
  // STEP 3: Offer tagging
  // ═══════════════════════════════════════
  // ═══════════════════════════════════════
  // STEP 2: Ensure columns exist
  // ═══════════════════════════════════════
  console.log();
  console.log("=== STEP 2: ENSURE COLUMNS ===");
  const cols = querySql("PRAGMA table_info(orders)");
  const colNames = cols.map(c => c.name);
  if (!colNames.includes("offer_name")) {
    runSql("ALTER TABLE orders ADD COLUMN offer_name TEXT DEFAULT NULL");
    console.log("  Added offer_name column");
  }
  if (!colNames.includes("requires_bank_change")) {
    runSql("ALTER TABLE orders ADD COLUMN requires_bank_change INTEGER DEFAULT 0");
    console.log("  Added requires_bank_change column");
  }
  saveDb();

  console.log();
  console.log("=== STEP 3: OFFER TAGGING ===");

  runSql(`UPDATE orders SET offer_name = 'Skin' WHERE offer_name IS NULL AND (
    product_group_name LIKE '%Eternal Lumi%' OR product_group_name LIKE '%Derma Lumiere%'
    OR product_group_name LIKE '%Derma La Fleur%' OR product_group_name LIKE '%Glo Vous Derm%'
  )`);
  const skinTagged = querySql("SELECT changes() as c")[0].c;
  console.log("  Skin tagged:", skinTagged);

  runSql(`UPDATE orders SET offer_name = 'Male Enhancement' WHERE offer_name IS NULL AND (
    product_group_name LIKE '%E-XceL%' OR product_group_name LIKE '%Excel%'
    OR product_group_name LIKE '%ViraFlexx%' OR product_group_name LIKE '%SS shipping%'
  )`);
  const maleTagged = querySql("SELECT changes() as c")[0].c;
  console.log("  Male Enhancement tagged:", maleTagged);

  runSql("UPDATE orders SET offer_name = 'Immunis' WHERE offer_name IS NULL AND product_group_name LIKE '%Erecovery%'");
  const immunisTagged = querySql("SELECT changes() as c")[0].c;
  console.log("  Immunis tagged:", immunisTagged);

  const untagged = querySql("SELECT COUNT(*) as c FROM orders WHERE offer_name IS NULL AND is_test=0 AND is_internal_test=0")[0].c;
  console.log("  Still untagged:", untagged);

  saveDb();

  // ═══════════════════════════════════════
  // STEP 4: Derived cycle recalc
  // ═══════════════════════════════════════
  console.log();
  console.log("=== STEP 4: DERIVED CYCLE RECALC ===");
  const nullCycles = querySql("SELECT COUNT(*) as c FROM orders WHERE derived_cycle IS NULL AND is_test=0")[0].c;
  console.log("  NULL derived_cycle:", nullCycles);

  if (nullCycles > 0) {
    // Anonymous (no customer) → cycle 0
    runSql("UPDATE orders SET derived_cycle = 0 WHERE derived_cycle IS NULL AND is_test = 0 AND customer_id IS NULL");
    const anonFixed = querySql("SELECT changes() as c")[0].c;
    console.log("  Anonymous → cycle 0:", anonFixed);

    // With customer — single-pass calculation (same algorithm as recalc-derived-fields.js)
    const remaining = querySql(`
      SELECT id, customer_id, product_group_id, product_type_classified, acquisition_date, order_status
      FROM orders
      WHERE derived_cycle IS NULL AND is_test = 0 AND customer_id IS NOT NULL
      ORDER BY customer_id, product_group_id, acquisition_date ASC, id ASC
    `);
    console.log("  Remaining with customer_id:", remaining.length);

    // Group by customer+product_group in memory, compute cycles in one pass
    const approvedCounts = {};
    const updates = [];

    for (const r of remaining) {
      const key = `${r.customer_id}:${r.product_group_id || 0}`;
      const priorApproved = approvedCounts[key] || 0;
      const ptype = r.product_type_classified || "";

      let cycle;
      if (ptype === "rebill" || ptype === "initial_rebill") {
        cycle = priorApproved > 0 ? priorApproved : 1;
      } else {
        cycle = priorApproved;
      }

      updates.push({ id: r.id, cycle });

      // Track approved orders for subsequent cycle calculations
      const status = parseInt(r.order_status);
      if ([2, 6, 8].includes(status)) {
        approvedCounts[key] = priorApproved + 1;
      }
    }

    // Batch all updates in a single transaction
    transaction(() => {
      for (let j = 0; j < updates.length; j++) {
        runSql("UPDATE orders SET derived_cycle = ? WHERE id = ?", [updates[j].cycle, updates[j].id]);
        if ((j + 1) % 5000 === 0) console.log("    progress:", j + 1, "/", updates.length);
      }
    });
    console.log("  Calculated:", updates.length);
    saveDb();
  }

  const stillNull = querySql("SELECT COUNT(*) as c FROM orders WHERE derived_cycle IS NULL AND is_test=0")[0].c;
  console.log("  Remaining NULL:", stillNull);

  // ═══════════════════════════════════════
  // STEP 5: Decline category classification
  // ═══════════════════════════════════════
  console.log();
  console.log("=== STEP 5: DECLINE CATEGORY ===");
  const nullDecline = querySql("SELECT COUNT(*) as c FROM orders WHERE decline_category IS NULL AND order_status = 7 AND is_test=0 AND is_internal_test=0")[0].c;
  console.log("  NULL decline_category:", nullDecline);

  if (nullDecline > 0) {
    // Load classifier
    delete require.cache[require.resolve("../src/classifiers/decline")];
    const { classifyDecline } = require("../src/classifiers/decline");

    const unclassified = querySql("SELECT DISTINCT decline_reason FROM orders WHERE decline_category IS NULL AND order_status = 7 AND is_test=0 AND decline_reason IS NOT NULL AND decline_reason != ''");
    let classified = 0;
    transaction(() => {
      for (const r of unclassified) {
        const cat = classifyDecline(r.decline_reason);
        if (cat) {
          runSql("UPDATE orders SET decline_category = ? WHERE decline_reason = ? AND decline_category IS NULL AND order_status = 7", [cat, r.decline_reason]);
          classified++;
        }
      }
    });
    console.log("  Classified:", classified, "unique reasons");
    saveDb();
  }

  const stillNullDec = querySql("SELECT COUNT(*) as c FROM orders WHERE decline_category IS NULL AND order_status = 7 AND is_test=0 AND is_internal_test=0")[0].c;
  console.log("  Remaining NULL:", stillNullDec);

  // ═══════════════════════════════════════
  // STEP 6: is_prepaid backfill
  // ═══════════════════════════════════════
  console.log();
  console.log("=== STEP 6: IS_PREPAID BACKFILL ===");
  runSql("UPDATE bin_lookup SET is_prepaid = 1 WHERE card_level LIKE '%PREPAID%' AND is_prepaid = 0");
  const prepaidFixed = querySql("SELECT changes() as c")[0].c;
  console.log("  Prepaid backfilled:", prepaidFixed);
  saveDb();

  // NOTE: Cascade chain backfill is now handled during import (chunked-import.js).
  // Use scripts/backfill-cascade-chain.js for one-time backfill of historical data.

  // ═══════════════════════════════════════
  // STEP 7: Final reconciliation
  // ═══════════════════════════════════════
  console.log();
  console.log("=== FINAL RECONCILIATION ===");
  const final = querySql("SELECT strftime('%Y-%m', acquisition_date) as m, COUNT(*) as c FROM orders WHERE is_test=0 AND is_internal_test=0 GROUP BY m ORDER BY m");
  console.log("  month   | orders");
  console.log("  " + "-".repeat(20));
  for (const r of final) console.log("  " + r.m + "  | " + r.c);
  console.log("  Total:", final.reduce((s,r) => s + r.c, 0));

  // Offer breakdown
  console.log();
  const offers = querySql("SELECT COALESCE(offer_name,'UNTAGGED') as o, COUNT(*) as c FROM orders WHERE is_test=0 AND is_internal_test=0 GROUP BY offer_name ORDER BY c DESC");
  console.log("  By offer:");
  for (const r of offers) console.log("    " + r.o.padEnd(20) + r.c);

  // Null checks
  console.log();
  console.log("  Null checks:");
  console.log("    derived_cycle NULL:", querySql("SELECT COUNT(*) as c FROM orders WHERE derived_cycle IS NULL AND is_test=0")[0].c);
  console.log("    decline_category NULL (declined):", querySql("SELECT COUNT(*) as c FROM orders WHERE decline_category IS NULL AND order_status=7 AND is_test=0 AND is_internal_test=0")[0].c);
  console.log("    offer_name NULL:", querySql("SELECT COUNT(*) as c FROM orders WHERE offer_name IS NULL AND is_test=0 AND is_internal_test=0")[0].c);
  console.log("    product_group_id NULL:", querySql("SELECT COUNT(*) as c FROM orders WHERE product_group_id IS NULL AND is_test=0 AND is_internal_test=0")[0].c);

  saveDb();
  console.log("\n=== WAL CHECKPOINT ===");
  checkpointWal();
  console.log("  WAL truncated.");
  closeDb();
}
run().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
