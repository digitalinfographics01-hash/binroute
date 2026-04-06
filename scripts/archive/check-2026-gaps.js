const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");
const StickyClient = require("../src/api/sticky-client");

async function run() {
  await initializeDatabase();
  const row = querySql("SELECT * FROM clients WHERE id = 1")[0];
  const client = new StickyClient({
    baseUrl: row.sticky_base_url,
    username: row.sticky_username,
    password: row.sticky_password,
  });

  console.log("=== JAN-MAR 2026 GAP CHECK ===");
  console.log();

  const months = [
    { label: "Jan 2026", start: "01/01/2026", end: "01/31/2026", dbFilter: "2026-01" },
    { label: "Feb 2026", start: "02/01/2026", end: "02/28/2026", dbFilter: "2026-02" },
    { label: "Mar 2026", start: "03/01/2026", end: "03/28/2026", dbFilter: "2026-03" },
  ];

  for (const m of months) {
    // DB count
    const dbCount = querySql(
      "SELECT COUNT(*) as cnt FROM orders WHERE is_test=0 AND is_internal_test=0 AND strftime('%Y-%m', acquisition_date) = '" + m.dbFilter + "'"
    )[0].cnt;

    // Sticky count
    process.stdout.write(m.label + "... ");
    const t = Date.now();
    try {
      const resp = await client.orderFindAll(m.start, m.end, 1, 1);
      const ms = Date.now() - t;
      const stickyCount = resp.total_orders || 0;
      const missing = Math.max(0, stickyCount - dbCount);
      const pct = stickyCount > 0 ? Math.round(dbCount / stickyCount * 100) : 100;

      console.log("Sticky: " + stickyCount + " | DB: " + dbCount + " | Missing: " + missing + " (" + pct + "% coverage) | " + Math.round(ms/1000) + "s");
    } catch (e) {
      console.log("ERROR: " + e.message.substring(0, 60));
    }
  }

  // Also check last import date
  console.log();
  const lastOrder = querySql("SELECT MAX(acquisition_date) as mx FROM orders WHERE is_test=0");
  console.log("Latest order in DB: " + lastOrder[0].mx);
  console.log("Today: 2026-03-28");
  console.log("Days since last order: " + Math.round((new Date("2026-03-28") - new Date(lastOrder[0].mx)) / 86400000));

  process.exit(0);
}
run().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
