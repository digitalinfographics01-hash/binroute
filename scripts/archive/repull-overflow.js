/**
 * Re-pull overflow chunks that were skipped during the main import.
 * Uses 6-hour windows to stay under the 500-order limit.
 * Safe to run — INSERT OR IGNORE prevents duplicates.
 *
 * Usage: node scripts/repull-overflow.js
 */
const { initializeDatabase } = require("../src/db/schema");
const { querySql, saveDb, closeDb } = require("../src/db/connection");
const StickyClient = require("../src/api/sticky-client");
const DataIngestion = require("../src/api/ingestion");

function formatDate(d) {
  return String(d.getMonth() + 1).padStart(2, "0") + "/" +
    String(d.getDate()).padStart(2, "0") + "/" + d.getFullYear();
}
function formatTime(d) {
  return String(d.getHours()).padStart(2, "0") + ":" +
    String(d.getMinutes()).padStart(2, "0") + ":" +
    String(d.getSeconds()).padStart(2, "0");
}

async function run() {
  await initializeDatabase();

  const row = querySql("SELECT * FROM clients WHERE id = 1")[0];
  const client = new StickyClient({
    baseUrl: row.sticky_base_url,
    username: row.sticky_username,
    password: row.sticky_password,
  });
  const ingestion = new DataIngestion(1);
  ingestion.init();

  // Overflow chunks from the main import
  const overflowDates = [
    { date: "2025-11-29", label: "Nov 29" },
    { date: "2025-11-30", label: "Nov 30" },
    { date: "2025-12-03", label: "Dec 3", half: "PM" },
    { date: "2025-12-07", label: "Dec 7", half: "PM" },
    { date: "2025-12-08", label: "Dec 8", half: "PM" },
    { date: "2025-12-09", label: "Dec 9", half: "PM" },
    { date: "2025-12-10", label: "Dec 10", half: "PM" },
  ];

  console.log("=== RE-PULLING OVERFLOW CHUNKS (6-hour windows) ===");
  console.log("Chunks to re-pull:", overflowDates.length);
  console.log();

  let totalImported = 0;

  for (const ov of overflowDates) {
    const baseDate = new Date(ov.date + "T00:00:00");

    // Determine time ranges — 6-hour windows
    let windows;
    if (ov.half === "PM") {
      // Original was 12:00-23:59, split into 12-17:59 and 18-23:59
      windows = [
        { start: new Date(ov.date + "T12:00:00"), end: new Date(ov.date + "T17:59:59"), label: ov.label + " 12-18" },
        { start: new Date(ov.date + "T18:00:00"), end: new Date(ov.date + "T23:59:59"), label: ov.label + " 18-24" },
      ];
    } else {
      // Full day — split into 4 × 6-hour windows
      windows = [
        { start: new Date(ov.date + "T00:00:00"), end: new Date(ov.date + "T05:59:59"), label: ov.label + " 00-06" },
        { start: new Date(ov.date + "T06:00:00"), end: new Date(ov.date + "T11:59:59"), label: ov.label + " 06-12" },
        { start: new Date(ov.date + "T12:00:00"), end: new Date(ov.date + "T17:59:59"), label: ov.label + " 12-18" },
        { start: new Date(ov.date + "T18:00:00"), end: new Date(ov.date + "T23:59:59"), label: ov.label + " 18-24" },
      ];
    }

    for (const w of windows) {
      process.stdout.write("  " + w.label + "... ");
      const t = Date.now();
      try {
        const data = await client._post("order_find", {
          campaign_id: "all",
          start_date: formatDate(w.start),
          end_date: formatDate(w.end),
          start_time: formatTime(w.start),
          end_time: formatTime(w.end),
          date_type: "create",
          criteria: "all",
          search_type: "all",
          return_type: "order_view",
          resultsPerPage: 500,
          page: 1,
        });
        const ms = Date.now() - t;
        const orders = client.parseOrdersFromResponse(data);

        if (orders.length === 0) {
          console.log("0 orders, " + Math.round(ms / 1000) + "s");
          continue;
        }

        if (orders.length >= 450) {
          console.log("STILL OVERFLOW (" + orders.length + ") — need 3-hour windows for " + w.label);
        }

        const before = ingestion._getDBCount();
        ingestion._saveOrderBatchToDB(orders);
        const after = ingestion._getDBCount();
        const inserted = after - before;
        totalImported += inserted;

        console.log(orders.length + " pulled, " + inserted + " new, " + (orders.length - inserted) + " skip, " + Math.round(ms / 1000) + "s");
      } catch (e) {
        console.log("ERROR: " + e.message.substring(0, 60));
      }
    }

    saveDb();
  }

  saveDb();
  console.log();
  console.log("=== OVERFLOW RE-PULL COMPLETE ===");
  console.log("Total new orders imported:", totalImported);

  // Show final counts
  const counts = querySql(`
    SELECT strftime('%Y-%m', acquisition_date) as month, COUNT(*) as cnt
    FROM orders WHERE is_test = 0 AND acquisition_date < '2026-01-01'
    GROUP BY month ORDER BY month
  `);
  console.log();
  console.log("Final pre-2026 counts:");
  for (const r of counts) console.log("  " + r.month + ": " + r.cnt);
  console.log("  Total:", counts.reduce((s, r) => s + r.cnt, 0));

  closeDb();
}

run().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
