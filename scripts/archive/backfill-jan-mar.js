/**
 * Backfill Jan-Mar 2026 gaps + new orders Mar 25-28.
 * Uses date-chunked approach with auto-split.
 * Run AFTER Sep-Dec import is complete.
 */
const fs = require("fs");
const path = require("path");
const { initializeDatabase } = require("../src/db/schema");
const { querySql, saveDb, closeDb } = require("../src/db/connection");
const StickyClient = require("../src/api/sticky-client");
const DataIngestion = require("../src/api/ingestion");

const MAX_PER_CHUNK = 450;

function formatDate(d) {
  return String(d.getMonth() + 1).padStart(2, "0") + "/" +
    String(d.getDate()).padStart(2, "0") + "/" + d.getFullYear();
}
function formatTime(d) {
  return String(d.getHours()).padStart(2, "0") + ":" +
    String(d.getMinutes()).padStart(2, "0") + ":" +
    String(d.getSeconds()).padStart(2, "0");
}
function addHours(d, h) { return new Date(d.getTime() + h * 3600000); }
function addDays(d, n) { return new Date(d.getTime() + n * 86400000); }

async function importChunk(client, ingestion, chunk) {
  const startDate = formatDate(chunk.start);
  const endDate = formatDate(chunk.end);
  const startTime = formatTime(chunk.start);
  const endTime = formatTime(chunk.end);

  const t = Date.now();
  let data;
  try {
    data = await client._post("order_find", {
      campaign_id: "all", start_date: startDate, end_date: endDate,
      start_time: startTime, end_time: endTime,
      date_type: "create", criteria: "all", search_type: "all",
      return_type: "order_view", resultsPerPage: 500, page: 1,
    });
  } catch (e) {
    return { error: e.message, ms: Date.now() - t, pulled: 0, inserted: 0, skipped: 0 };
  }
  const ms = Date.now() - t;
  const orders = client.parseOrdersFromResponse(data);
  if (orders.length === 0) return { ms, pulled: 0, inserted: 0, skipped: 0 };

  // Auto-split on overflow
  if (orders.length >= MAX_PER_CHUNK) {
    const chunkMs = chunk.end.getTime() - chunk.start.getTime();
    if (chunkMs < 3600000) {
      const before = ingestion._getDBCount();
      ingestion._saveOrderBatchToDB(orders);
      const after = ingestion._getDBCount();
      return { ms, pulled: orders.length, inserted: after - before, skipped: orders.length - (after - before), note: "MAXED" };
    }
    const mid = new Date(chunk.start.getTime() + Math.floor(chunkMs / 2));
    const r1 = await importChunk(client, ingestion, { start: chunk.start, end: new Date(mid.getTime() - 1000), label: chunk.label + " [1/2]" });
    const r2 = await importChunk(client, ingestion, { start: mid, end: chunk.end, label: chunk.label + " [2/2]" });
    return { ms: (r1.ms||0)+(r2.ms||0), pulled: (r1.pulled||0)+(r2.pulled||0), inserted: (r1.inserted||0)+(r2.inserted||0), skipped: (r1.skipped||0)+(r2.skipped||0), note: "SPLIT → " + (r1.pulled||0) + "+" + (r2.pulled||0) };
  }

  const before = ingestion._getDBCount();
  ingestion._saveOrderBatchToDB(orders);
  const after = ingestion._getDBCount();
  return { ms, pulled: orders.length, inserted: after - before, skipped: orders.length - (after - before) };
}

async function run() {
  await initializeDatabase();
  const row = querySql("SELECT * FROM clients WHERE id = 1")[0];
  const client = new StickyClient({ baseUrl: row.sticky_base_url, username: row.sticky_username, password: row.sticky_password });
  const ingestion = new DataIngestion(1);
  ingestion.init();

  // Build chunks: Jan 1-day, Feb 12-hour (busier), Mar 12-hour
  const chunks = [];
  // Jan: 1-day chunks
  let d = new Date("2026-01-01T00:00:00");
  while (d < new Date("2026-02-01T00:00:00")) {
    const next = addDays(d, 1);
    chunks.push({ start: new Date(d), end: new Date(next.getTime() - 1000), label: "Jan " + d.getDate() });
    d = next;
  }
  // Feb: 12-hour chunks
  d = new Date("2026-02-01T00:00:00");
  while (d < new Date("2026-03-01T00:00:00")) {
    const next = addHours(d, 12);
    const half = d.getHours() < 12 ? "AM" : "PM";
    chunks.push({ start: new Date(d), end: new Date(next.getTime() - 1000), label: "Feb " + d.getDate() + " " + half });
    d = next;
  }
  // Mar: 12-hour chunks through Mar 28
  d = new Date("2026-03-01T00:00:00");
  while (d < new Date("2026-03-29T00:00:00")) {
    const next = addHours(d, 12);
    const half = d.getHours() < 12 ? "AM" : "PM";
    chunks.push({ start: new Date(d), end: new Date(next.getTime() - 1000), label: "Mar " + d.getDate() + " " + half });
    d = next;
  }

  console.log("=== JAN-MAR 2026 BACKFILL ===");
  console.log("Total chunks:", chunks.length);
  console.log();

  let totalImported = 0, totalSkipped = 0;
  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    process.stdout.write("[" + (i+1) + "/" + chunks.length + "] " + chunk.label + "... ");
    const result = await importChunk(client, ingestion, chunk);
    if (result.error) { console.log("ERROR: " + result.error.substring(0, 60)); continue; }
    const note = result.note ? " [" + result.note + "]" : "";
    console.log(result.pulled + " pulled, " + result.inserted + " new, " + result.skipped + " skip, " + Math.round(result.ms / 1000) + "s" + note);
    totalImported += result.inserted;
    totalSkipped += result.skipped;
    if ((i + 1) % 5 === 0) saveDb();
  }

  saveDb();
  console.log();
  console.log("=== COMPLETE ===");
  console.log("Imported:", totalImported, "| Skipped:", totalSkipped);

  const counts = querySql("SELECT strftime('%Y-%m', acquisition_date) as m, COUNT(*) as c FROM orders WHERE is_test=0 GROUP BY m ORDER BY m");
  for (const r of counts) console.log("  " + r.m + ": " + r.c);
  console.log("  Total:", counts.reduce((s,r) => s + r.c, 0));

  closeDb();
}

run().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
