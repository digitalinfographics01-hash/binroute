const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");
const axios = require("axios");

async function run() {
  await initializeDatabase();
  const row = querySql("SELECT * FROM clients WHERE id = 1")[0];
  const baseUrl = `https://${row.sticky_base_url.replace(/\/$/, "")}/api/v1/order_find`;
  const auth = { username: row.sticky_username, password: row.sticky_password };

  function makeParams(pageSize) {
    const p = new URLSearchParams();
    p.append("campaign_id", "all");
    p.append("start_date", "03/20/2026");
    p.append("end_date", "03/24/2026");
    p.append("date_type", "create");
    p.append("criteria", "all");
    p.append("search_type", "all");
    p.append("return_type", "order_view");
    p.append("resultsPerPage", String(pageSize));
    p.append("page", "1");
    return p.toString();
  }

  // TEST 2 — Rate limit headers from 5 consecutive calls
  console.log("=== TEST 2: Rate limit headers (5 consecutive calls) ===");
  for (let i = 1; i <= 5; i++) {
    const t = Date.now();
    try {
      const resp = await axios.post(baseUrl, makeParams(1), {
        auth, headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: 120000
      });
      const ms = Date.now() - t;
      const h = resp.headers;
      const totalOrders = resp.data.total_orders || "?";
      console.log("  Call " + i + ": " + ms + "ms | total=" + totalOrders +
        " | RateLimit=" + (h["x-ratelimit-limit"] || "none") +
        " | Remaining=" + (h["x-ratelimit-remaining"] || "none") +
        " | Reset=" + (h["x-ratelimit-reset"] || "none") +
        " | Status=" + resp.status);
    } catch (e) {
      const ms = Date.now() - t;
      console.log("  Call " + i + ": " + ms + "ms | ERROR " + (e.response?.status || e.code || e.message));
    }
  }

  // TEST 4 — Page size optimization
  console.log();
  console.log("=== TEST 4: Page size optimization ===");
  for (const ps of [100, 200, 500]) {
    const t = Date.now();
    try {
      const resp = await axios.post(baseUrl, makeParams(ps), {
        auth, headers: { "Content-Type": "application/x-www-form-urlencoded" }, timeout: 120000
      });
      const ms = Date.now() - t;
      const data = resp.data;
      let orderCount = 0;
      if (data.data && typeof data.data === "object") {
        orderCount = Array.isArray(data.data) ? data.data.length : Object.keys(data.data).length;
      }
      const ordersPerMin = Math.round(orderCount / (ms / 60000));
      console.log("  " + ps + "/page: " + ms + "ms | " + orderCount + " orders returned | " + ordersPerMin + " orders/min");
    } catch (e) {
      console.log("  " + ps + "/page: ERROR " + (e.response?.status || e.message));
    }
  }

  // TEST 5 — Estimate
  console.log();
  console.log("=== TEST 5: Import estimate ===");
  console.log("  Missing orders: 30,271");
  console.log("  Current config: 500/page, 35ms throttle (28 req/sec max)");
  console.log("  But each request takes 60-100s, so throttle is irrelevant");
  console.log("  Real bottleneck: Sticky API response time");
  console.log("  Pages needed: " + Math.ceil(30271 / 500) + " (at 500/page)");
  console.log("  Estimate below based on measured response times...");

  process.exit(0);
}
run().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
