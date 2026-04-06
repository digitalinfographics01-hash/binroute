const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

async function run() {
  await initializeDatabase();

  console.log("=== 1. Eligibility lookup impact: Sep-Oct inclusion ===");
  console.log();

  // Current 90-day window (roughly Jan-Mar 2026)
  const q90 = querySql(`
    SELECT d.decline_reason, b.issuer_bank, b.card_type,
      COUNT(*) as att, COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END) as rec,
      ROUND(COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders d LEFT JOIN bin_lookup b ON d.cc_first_6 = b.bin
    LEFT JOIN orders r ON r.customer_id = d.customer_id AND r.product_group_id = d.product_group_id
      AND r.derived_cycle = d.derived_cycle AND r.attempt_number > d.attempt_number
      AND r.order_status IN (2,6,8) AND r.is_test = 0
    WHERE d.client_id = 1 AND d.tx_type IN ('tp_rebill','tp_rebill_salvage')
    AND d.is_test = 0 AND d.is_internal_test = 0 AND d.order_status = 7
    AND d.decline_reason IS NOT NULL AND d.decline_reason != ''
    AND d.acquisition_date >= date('now', '-90 days')
    GROUP BY d.decline_reason, b.issuer_bank, b.card_type
    HAVING COUNT(*) >= 15
  `);

  // Full 180-day window (includes Sep-Oct)
  const q180 = querySql(`
    SELECT d.decline_reason, b.issuer_bank, b.card_type,
      COUNT(*) as att, COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END) as rec,
      ROUND(COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END)*100.0/NULLIF(COUNT(*),0),1) as rate
    FROM orders d LEFT JOIN bin_lookup b ON d.cc_first_6 = b.bin
    LEFT JOIN orders r ON r.customer_id = d.customer_id AND r.product_group_id = d.product_group_id
      AND r.derived_cycle = d.derived_cycle AND r.attempt_number > d.attempt_number
      AND r.order_status IN (2,6,8) AND r.is_test = 0
    WHERE d.client_id = 1 AND d.tx_type IN ('tp_rebill','tp_rebill_salvage')
    AND d.is_test = 0 AND d.is_internal_test = 0 AND d.order_status = 7
    AND d.decline_reason IS NOT NULL AND d.decline_reason != ''
    GROUP BY d.decline_reason, b.issuer_bank, b.card_type
    HAVING COUNT(*) >= 15
  `);

  console.log("  90-day entries (15+ att):", q90.length);
  console.log("  180-day entries (15+ att):", q180.length);
  console.log("  New entries from Sep-Oct:", q180.length - q90.length);

  // Check for classification flips
  const map90 = new Map();
  for (const r of q90) map90.set(r.decline_reason + "|" + r.issuer_bank + "|" + r.card_type, r.rate);

  let flips = 0, newEntries = 0;
  for (const r of q180) {
    const key = r.decline_reason + "|" + r.issuer_bank + "|" + r.card_type;
    const rate90 = map90.get(key);
    if (rate90 === undefined) {
      newEntries++;
    } else {
      const was = rate90 >= 4 ? "ALLOW" : "BLOCK";
      const now = r.rate >= 4 ? "ALLOW" : "BLOCK";
      if (was !== now) {
        flips++;
        console.log("  FLIP: " + r.decline_reason.substring(0,25) + " | " + (r.issuer_bank||"").substring(0,25) + " | 90d=" + rate90 + "% → 180d=" + r.rate + "%");
      }
    }
  }
  console.log("  Classification flips:", flips);
  console.log("  New entries (below 15 in 90d, above in 180d):", newEntries);

  console.log();
  console.log("=== 2. BIN groups gaining 30+ attempts ===");
  const qBin90 = querySql(`
    SELECT COALESCE(b.issuer_bank,'Unknown') || '|' || COALESCE(b.card_brand,'') as grp, COUNT(*) as att
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.tx_type IN ('tp_rebill','tp_rebill_salvage') AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.order_status IN (2,6,7,8) AND o.acquisition_date >= date('now', '-90 days')
    GROUP BY grp
  `);
  const qBin180 = querySql(`
    SELECT COALESCE(b.issuer_bank,'Unknown') || '|' || COALESCE(b.card_brand,'') as grp, COUNT(*) as att
    FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.tx_type IN ('tp_rebill','tp_rebill_salvage') AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.order_status IN (2,6,7,8)
    GROUP BY grp
  `);

  const binMap90 = new Map();
  for (const r of qBin90) binMap90.set(r.grp, r.att);

  let gained30 = 0;
  const gainedGroups = [];
  for (const r of qBin180) {
    const att90 = binMap90.get(r.grp) || 0;
    const gain = r.att - att90;
    if (gain >= 30) {
      gained30++;
      gainedGroups.push({ grp: r.grp, att90, att180: r.att, gain });
    }
  }
  console.log("  BIN groups gaining 30+ attempts: " + gained30);
  gainedGroups.sort((a, b) => b.gain - a.gain);
  for (const g of gainedGroups.slice(0, 10)) {
    console.log("    " + g.grp.replace("|", " · ").substring(0, 45).padEnd(45) + " 90d=" + g.att90 + " → 180d=" + g.att180 + " (+" + g.gain + ")");
  }

  console.log();
  console.log("=== 3. November 2025 ===");
  const qNov = querySql("SELECT COUNT(*) as cnt, MIN(acquisition_date) as mn, MAX(acquisition_date) as mx FROM orders WHERE acquisition_date >= '2025-11-01' AND acquisition_date < '2025-12-01'");
  console.log("  Orders in Nov 2025:", qNov[0].cnt);
  if (qNov[0].cnt > 0) console.log("  Range:", qNov[0].mn, "to", qNov[0].mx);

  // Check what's around the gap
  const qGap = querySql(`
    SELECT date(acquisition_date) as day, COUNT(*) as cnt
    FROM orders WHERE acquisition_date >= '2025-10-01' AND acquisition_date < '2025-12-15'
    GROUP BY day ORDER BY day
  `);
  console.log("  Daily orders Oct-Dec 2025:");
  let lastDay = "";
  for (const r of qGap) {
    // Show gaps
    if (lastDay && r.day > lastDay) {
      const gap = Math.round((new Date(r.day) - new Date(lastDay)) / 86400000);
      if (gap > 2) console.log("    ... " + (gap-1) + " days with no orders ...");
    }
    console.log("    " + r.day + ": " + r.cnt);
    lastDay = r.day;
  }

  console.log();
  console.log("=== 4. Sep-Oct cycle distribution ===");
  const qCycle = querySql(`
    SELECT derived_cycle, COUNT(*) as cnt
    FROM orders WHERE acquisition_date < '2025-11-01' AND is_test = 0
    GROUP BY derived_cycle ORDER BY derived_cycle
  `);
  console.log("  derived_cycle | count");
  console.log("  " + "-".repeat(25));
  for (const r of qCycle) console.log("  " + String(r.derived_cycle).padStart(13) + " | " + r.cnt);

  // Also: tx_type for Sep-Oct
  const qTx = querySql(`
    SELECT tx_type, COUNT(*) as cnt
    FROM orders WHERE acquisition_date < '2025-11-01' AND is_test = 0
    GROUP BY tx_type ORDER BY cnt DESC
  `);
  console.log();
  console.log("  Sep-Oct tx_type:");
  for (const r of qTx) console.log("    " + (r.tx_type||"NULL").padEnd(25) + r.cnt);

  process.exit(0);
}
run();
