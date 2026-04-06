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

  console.log("=== 1. Pagination verification — December 2025 ===");
  console.log();

  // Page 1
  console.log("Pulling page 1 (500/page)...");
  const t1 = Date.now();
  const resp1 = await client.orderFindAll("12/01/2025", "12/31/2025", 1, 500);
  const ms1 = Date.now() - t1;
  const orders1 = client.parseOrdersFromResponse(resp1);
  const ids1 = orders1.map(o => o.order_id || o.orders_id);
  console.log("  Time: " + ms1 + "ms");
  console.log("  Orders returned: " + orders1.length);
  console.log("  total_orders in response: " + (resp1.total_orders || "not provided"));
  console.log("  First 5 order_ids: " + ids1.slice(0, 5).join(", "));
  console.log("  Last 5 order_ids: " + ids1.slice(-5).join(", "));

  // Page 2
  console.log();
  console.log("Pulling page 2 (500/page)...");
  const t2 = Date.now();
  const resp2 = await client.orderFindAll("12/01/2025", "12/31/2025", 2, 500);
  const ms2 = Date.now() - t2;
  const orders2 = client.parseOrdersFromResponse(resp2);
  const ids2 = orders2.map(o => o.order_id || o.orders_id);
  console.log("  Time: " + ms2 + "ms");
  console.log("  Orders returned: " + orders2.length);
  console.log("  First 5 order_ids: " + ids2.slice(0, 5).join(", "));
  console.log("  Last 5 order_ids: " + ids2.slice(-5).join(", "));

  // Check overlap
  const set1 = new Set(ids1);
  const overlap = ids2.filter(id => set1.has(id));
  console.log();
  console.log("  Overlap between page 1 and 2: " + overlap.length + " orders");
  console.log("  Unique in page 1: " + ids1.length);
  console.log("  Unique in page 2: " + ids2.length);
  console.log("  PAGINATION WORKING: " + (overlap.length === 0 ? "YES — zero overlap" : "NO — " + overlap.length + " duplicates!"));

  // Calculate total pages for all missing months
  console.log();
  console.log("=== 2. Total pages calculation ===");
  const totalOrders = resp1.total_orders || 24717;
  const decPages = Math.ceil(totalOrders / 500);
  console.log("  December: " + totalOrders + " orders / 500 = " + decPages + " pages");

  // We already know the other months from the gap check
  console.log("  September: 6,508 / 500 = " + Math.ceil(6508/500) + " pages");
  console.log("  October: 1,518 / 500 = " + Math.ceil(1518/500) + " pages");
  console.log("  November: 2,998 / 500 = " + Math.ceil(2998/500) + " pages");

  const totalPages = Math.ceil(6508/500) + Math.ceil(1518/500) + Math.ceil(2998/500) + decPages;
  console.log("  TOTAL PAGES: " + totalPages);
  const avgMs = Math.round((ms1 + ms2) / 2);
  console.log("  Avg time per page: " + avgMs + "ms (" + Math.round(avgMs/1000) + "s)");
  console.log("  Estimated total time: " + Math.round(totalPages * avgMs / 60000) + " minutes (" + (totalPages * avgMs / 3600000).toFixed(1) + " hours)");

  // 3. Check for checkpoint/resume mechanism
  console.log();
  console.log("=== 3. Resume/checkpoint mechanism ===");
  const fs = require("fs");
  const ingestionCode = fs.readFileSync("./src/api/ingestion.js", "utf8");
  const hasCheckpoint = ingestionCode.includes("checkpoint") || ingestionCode.includes("resumeFrom") || ingestionCode.includes("lastPage");
  const hasInsertIgnore = ingestionCode.includes("INSERT OR IGNORE") || ingestionCode.includes("ON CONFLICT");
  console.log("  Checkpoint/resume in ingestion.js: " + hasCheckpoint);
  console.log("  INSERT OR IGNORE (duplicate safe): " + hasInsertIgnore);

  // Check if checkpoint.json exists
  const cpExists = fs.existsSync("./checkpoint.json");
  console.log("  checkpoint.json exists: " + cpExists);
  if (cpExists) {
    const cp = JSON.parse(fs.readFileSync("./checkpoint.json", "utf8"));
    console.log("  checkpoint.json contents: " + JSON.stringify(cp).substring(0, 200));
  }

  // Check if DB saves happen per-page or at end
  const hasSavePerPage = ingestionCode.includes("saveDb()");
  const saveCount = (ingestionCode.match(/saveDb\(\)/g) || []).length;
  console.log("  saveDb() calls in ingestion.js: " + saveCount);
  console.log("  Saves per page (data durability): " + (saveCount > 1 ? "YES — multiple save points" : "CHECK — may only save at end"));

  process.exit(0);
}
run().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
