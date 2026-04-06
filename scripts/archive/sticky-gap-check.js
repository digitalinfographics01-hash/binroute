const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");
const StickyClient = require("../src/api/sticky-client");

async function run() {
  await initializeDatabase();

  const clients = querySql("SELECT * FROM clients WHERE id = 1");
  if (!clients.length) { console.log("No client found"); process.exit(1); }
  const row = clients[0];

  const client = new StickyClient({
    baseUrl: row.sticky_base_url,
    username: row.sticky_username,
    password: row.sticky_password,
  });

  console.log("=== STICKY API GAP CHECK ===");
  console.log("Sticky URL:", row.sticky_base_url);
  console.log();

  // DB counts
  const dbOct = querySql("SELECT COUNT(*) as cnt FROM orders WHERE acquisition_date >= '2025-10-01' AND acquisition_date < '2025-11-01'");
  const dbNov = querySql("SELECT COUNT(*) as cnt FROM orders WHERE acquisition_date >= '2025-11-01' AND acquisition_date < '2025-12-01'");
  const dbDec = querySql("SELECT COUNT(*) as cnt FROM orders WHERE acquisition_date >= '2025-12-01' AND acquisition_date < '2026-01-01'");

  console.log("DB counts:");
  console.log("  Oct 2025:", dbOct[0].cnt);
  console.log("  Nov 2025:", dbNov[0].cnt);
  console.log("  Dec 2025:", dbDec[0].cnt);
  console.log();

  // Query Sticky API for October
  console.log("=== Querying Sticky API ===");

  try {
    console.log("October 2025...");
    const t1 = Date.now();
    const octResp = await client.orderFindAll("10/01/2025", "10/31/2025", 1, 1);
    const t1ms = Date.now() - t1;
    const octTotal = octResp.total_orders || 0;
    console.log("  Sticky Oct total:", octTotal, "(" + t1ms + "ms)");
    console.log("  DB Oct total:", dbOct[0].cnt);
    console.log("  Missing:", Math.max(0, octTotal - dbOct[0].cnt));
  } catch (e) {
    console.log("  Oct ERROR:", e.message.substring(0, 200));
  }

  console.log();

  try {
    console.log("November 2025...");
    const t2 = Date.now();
    const novResp = await client.orderFindAll("11/01/2025", "11/30/2025", 1, 1);
    const t2ms = Date.now() - t2;
    const novTotal = novResp.total_orders || 0;
    console.log("  Sticky Nov total:", novTotal, "(" + t2ms + "ms)");
    console.log("  DB Nov total:", dbNov[0].cnt);
    console.log("  Missing:", Math.max(0, novTotal - dbNov[0].cnt));
  } catch (e) {
    console.log("  Nov ERROR:", e.message.substring(0, 200));
  }

  console.log();

  try {
    console.log("December 2025...");
    const t3 = Date.now();
    const decResp = await client.orderFindAll("12/01/2025", "12/31/2025", 1, 1);
    const t3ms = Date.now() - t3;
    const decTotal = decResp.total_orders || 0;
    console.log("  Sticky Dec total:", decTotal, "(" + t3ms + "ms)");
    console.log("  DB Dec total:", dbDec[0].cnt);
    console.log("  Missing:", Math.max(0, decTotal - dbDec[0].cnt));
  } catch (e) {
    console.log("  Dec ERROR:", e.message.substring(0, 200));
  }

  // Also check Sep for completeness
  console.log();
  const dbSep = querySql("SELECT COUNT(*) as cnt FROM orders WHERE acquisition_date >= '2025-09-01' AND acquisition_date < '2025-10-01'");
  try {
    console.log("September 2025...");
    const t4 = Date.now();
    const sepResp = await client.orderFindAll("09/01/2025", "09/30/2025", 1, 1);
    const t4ms = Date.now() - t4;
    const sepTotal = sepResp.total_orders || 0;
    console.log("  Sticky Sep total:", sepTotal, "(" + t4ms + "ms)");
    console.log("  DB Sep total:", dbSep[0].cnt);
    console.log("  Missing:", Math.max(0, sepTotal - dbSep[0].cnt));
  } catch (e) {
    console.log("  Sep ERROR:", e.message.substring(0, 200));
  }

  console.log();
  console.log("=== Summary ===");
  console.log("  API response times give us import speed baseline.");
  console.log("  Any 'Missing' > 0 means orders in Sticky not in our DB.");

  process.exit(0);
}

run().catch(e => {
  console.error("Fatal:", e.message);
  process.exit(1);
});
