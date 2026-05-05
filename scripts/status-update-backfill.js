#!/usr/bin/env node
/**
 * Status Update Backfill — fetches updated order IDs 1-day-at-a-time,
 * then batch order_view 50/call with 5 concurrent.
 *
 * Usage:
 *   node scripts/status-update-backfill.js --days=10
 *   node scripts/status-update-backfill.js --days=30
 */

const path = require('path');
const Database = require('better-sqlite3');
const { createDbHelpers } = require('../src/db/connection-factory');
const { initStagingSchema } = require('../src/db/staging-schema');
const DataIngestion = require('../src/api/ingestion');

const DB_PATH = path.join(__dirname, '..', 'data', 'binroute.db');
const STAGING_DIR = path.join(__dirname, '..', 'data', 'staging');
const fs = require('fs');

const BATCH_SIZE = 50;
const VIEW_CONCURRENCY = 5;

function parseArgs() {
  const days = parseInt((process.argv.find(a => a.startsWith('--days=')) || '--days=10').split('=')[1], 10);
  return { days };
}

function formatDate(d) {
  return `${String(d.getMonth() + 1).padStart(2, '0')}/${String(d.getDate()).padStart(2, '0')}/${d.getFullYear()}`;
}

function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d;
}

async function main() {
  const { days } = parseArgs();
  const startTime = Date.now();

  console.log(`=== STATUS UPDATE BACKFILL: last ${days} days ===`);

  // Read credentials from main DB
  const mainDb = new Database(DB_PATH, { readonly: true });
  const creds = mainDb.prepare('SELECT sticky_base_url, sticky_username, sticky_password FROM clients WHERE id = 6').get();
  mainDb.close();

  if (!creds) throw new Error('Client 6 not found');

  // Create staging DB
  if (!fs.existsSync(STAGING_DIR)) fs.mkdirSync(STAGING_DIR, { recursive: true });
  const stagingPath = path.join(STAGING_DIR, `client-6-backfill-${Date.now()}.db`);
  const stagingHelpers = createDbHelpers(stagingPath, { cacheSizeMB: 32 });
  initStagingSchema(stagingHelpers);

  // Build ingestion instance pointing at staging DB
  const ingestion = new DataIngestion(6, { dbHelpers: stagingHelpers });
  ingestion.initWithCredentials(creds);

  // ─── Step 1: Collect all updated order IDs, 1 day at a time ───
  console.log(`\nStep 1: Fetching updated order IDs (1 day at a time)...`);
  const allIds = new Set();

  for (let i = 0; i < days; i++) {
    const start = formatDate(daysAgo(i + 1));
    const end = formatDate(daysAgo(i));

    try {
      let page = 1;
      let dayIds = 0;
      while (true) {
        const result = await ingestion.client._post('order_find_updated', {
          start_date: start,
          end_date: end,
          campaign_id: 'all',
          results_per_page: 5000,
          page,
        }, 5, { timeout: 300000 });

        if (result.response_code !== '100' || !result.order_id) break;

        const ids = Array.isArray(result.order_id) ? result.order_id : [result.order_id];
        for (const id of ids) allIds.add(id);
        dayIds += ids.length;

        if (dayIds >= parseInt(result.total_orders, 10)) break;
        page++;
      }
      console.log(`  ${start} → ${end}: ${dayIds} updated IDs (unique total: ${allIds.size})`);
    } catch (err) {
      console.error(`  ${start} → ${end}: FAILED — ${err.message}`);
    }
  }

  if (allIds.size === 0) {
    console.log('No updated orders found. Done.');
    return;
  }

  console.log(`\nStep 1 complete: ${allIds.size} unique order IDs to refresh`);

  // ─── Step 2: Batch order_view — 50/call, 5 concurrent ───
  console.log(`\nStep 2: Fetching full orders (batch=${BATCH_SIZE}, concurrency=${VIEW_CONCURRENCY})...`);

  const idList = [...allIds];
  const batches = [];
  for (let i = 0; i < idList.length; i += BATCH_SIZE) {
    batches.push(idList.slice(i, i + BATCH_SIZE));
  }

  let fetched = 0, saved = 0, errors = 0;

  for (let bi = 0; bi < batches.length; bi += VIEW_CONCURRENCY) {
    const concurrent = batches.slice(bi, bi + VIEW_CONCURRENCY);

    const results = await Promise.all(concurrent.map(async (batch) => {
      const idStr = batch.join(',');
      try {
        const resp = await ingestion.client._post('order_view', { order_id: idStr });
        if (!resp || resp.response_code !== '100') return [];

        if (resp.data && typeof resp.data === 'object' && !Array.isArray(resp.data)) {
          return Object.values(resp.data).filter(o => o && o.order_id);
        }
        if (resp.order_id) return [resp];
        return [];
      } catch (e) {
        errors += batch.length;
        return [];
      }
    }));

    for (const orders of results) {
      for (const raw of orders) {
        fetched++;
        try {
          const order = ingestion.client.normalizeOrder(raw);
          ingestion._insertOrderSafe(order);
          saved++;
        } catch (e) {
          errors++;
        }
      }
    }

    if (fetched % 2000 < BATCH_SIZE * VIEW_CONCURRENCY) {
      ingestion._saveDb();
      const pct = ((fetched / idList.length) * 100).toFixed(1);
      console.log(`  Progress: ${fetched}/${idList.length} (${pct}%) — ${saved} saved, ${errors} errors`);
    }
  }

  ingestion._saveDb();
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\nStep 2 complete: ${fetched} fetched, ${saved} saved, ${errors} errors`);

  // ─── Step 3: Run staging post-sync ───
  console.log(`\nStep 3: Classifying orders...`);
  try {
    const { runStagingPostSync } = require('../src/pipeline/staging-post-sync');
    const psResult = runStagingPostSync(stagingHelpers, 6);
    console.log(`  Classified: ${psResult.classified}, Cascades: ${psResult.cascadesParsed}`);
  } catch (err) {
    console.error(`  Post-sync failed: ${err.message}`);
  }

  console.log(`\nStaging DB: ${stagingPath}`);
  console.log(`Total time: ${elapsed}s`);
  console.log(`\nTo merge: node -e "const {initDb}=require('./src/db/connection'); const {mergeStagingToMain}=require('./src/db/staging-merge'); (async()=>{await initDb(); console.log(JSON.stringify(mergeStagingToMain('${stagingPath}',6)));})()"`);
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
