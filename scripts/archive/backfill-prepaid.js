#!/usr/bin/env node
/**
 * One-time backfill: update prepaid + prepaid_match on existing orders
 * by re-pulling from Sticky.io API via order_find with return_type=order_view.
 *
 * READ + UPDATE only — never inserts new orders, never deletes.
 *
 * Usage:
 *   node scripts/backfill-prepaid.js
 *   node scripts/backfill-prepaid.js --dry-run     (preview without writing)
 */
const path = require('path');

// Bootstrap DB + client
const { initDb, closeDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));
const { querySql, runSql, saveDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const DataIngestion = require(path.join(__dirname, '..', 'src', 'api', 'ingestion'));

const DRY_RUN = process.argv.includes('--dry-run');
const CLIENT_ID = 1;
const DAYS_BACK = 180;
const RESULTS_PER_PAGE = 500;

function formatDate(d) {
  return `${String(d.getMonth() + 1).padStart(2, '0')}/${String(d.getDate()).padStart(2, '0')}/${d.getFullYear()}`;
}

async function main() {
  await initializeDatabase();
  console.log('Database ready.');

  // Show before state
  const before = querySql(
    "SELECT prepaid, COUNT(*) as count, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM orders WHERE is_test = 0), 1) as pct " +
    "FROM orders WHERE is_test = 0 GROUP BY prepaid"
  );
  console.log('\n=== BEFORE backfill ===');
  console.table(before);

  if (DRY_RUN) {
    console.log('\n[DRY RUN] Would pull last', DAYS_BACK, 'days from Sticky API and update prepaid fields.');
    console.log('[DRY RUN] No changes made.');
    closeDb();
    return;
  }

  // Init Sticky client
  const ingestion = new DataIngestion(CLIENT_ID);
  ingestion.init();

  const endDate = formatDate(new Date());
  const start = new Date();
  start.setDate(start.getDate() - DAYS_BACK);
  const startDate = formatDate(start);

  console.log(`\nPulling orders from ${startDate} to ${endDate}...`);

  // Build set of existing order_ids for fast lookup
  const existingIds = new Set(
    querySql('SELECT order_id FROM orders WHERE client_id = ?', [CLIENT_ID])
      .map(r => r.order_id)
  );
  console.log(`Existing orders in DB: ${existingIds.size}`);

  let page = 1;
  let totalUpdated = 0;
  let totalSkipped = 0;
  let totalFromApi = 0;

  while (true) {
    console.log(`  Fetching page ${page}...`);
    const data = await ingestion.client.orderFindAll(startDate, endDate, page, RESULTS_PER_PAGE);

    if (!data || data.response_code !== '100') {
      if (page === 1) console.log('  API returned no data. response_code:', data?.response_code);
      break;
    }

    const orders = ingestion.client.parseOrdersFromResponse(data);
    if (orders.length === 0) break;

    totalFromApi += orders.length;

    for (const raw of orders) {
      const orderId = parseInt(raw.order_id, 10);
      if (!existingIds.has(orderId)) {
        totalSkipped++;
        continue;
      }

      const prepaid = raw.prepaid || '0';
      const prepaidMatch = raw.prepaid_match || 'No';

      runSql(
        'UPDATE orders SET prepaid = ?, prepaid_match = ? WHERE client_id = ? AND order_id = ?',
        [prepaid, prepaidMatch, CLIENT_ID, orderId]
      );
      totalUpdated++;

      if (totalUpdated % 5000 === 0) {
        saveDb();
        console.log(`  Updated ${totalUpdated} of ${existingIds.size} orders...`);
      }
    }

    // Check if more pages
    const totalResults = parseInt(data.total_orders || data.totalResults || '0', 10);
    if (page * RESULTS_PER_PAGE >= totalResults || orders.length < RESULTS_PER_PAGE) {
      break;
    }
    page++;
  }

  saveDb();

  console.log(`\nBackfill complete.`);
  console.log(`  API orders fetched: ${totalFromApi}`);
  console.log(`  Orders updated: ${totalUpdated}`);
  console.log(`  Skipped (not in DB): ${totalSkipped}`);

  // Show after state
  const after = querySql(
    "SELECT prepaid, COUNT(*) as count, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM orders WHERE is_test = 0), 1) as pct " +
    "FROM orders WHERE is_test = 0 GROUP BY prepaid"
  );
  console.log('\n=== AFTER backfill ===');
  console.table(after);

  // Cross-check against bin_lookup
  const crossCheck = querySql(
    "SELECT " +
    "  SUM(CASE WHEN o.prepaid = '1' AND b.is_prepaid = 1 THEN 1 ELSE 0 END) as both_prepaid, " +
    "  SUM(CASE WHEN o.prepaid = '0' AND b.is_prepaid = 0 THEN 1 ELSE 0 END) as both_not, " +
    "  SUM(CASE WHEN o.prepaid = '1' AND b.is_prepaid = 0 THEN 1 ELSE 0 END) as sticky_only, " +
    "  SUM(CASE WHEN o.prepaid = '0' AND b.is_prepaid = 1 THEN 1 ELSE 0 END) as bin_only " +
    "FROM orders o " +
    "JOIN bin_lookup b ON o.cc_first_6 = b.bin " +
    "WHERE o.is_test = 0 AND o.is_internal_test = 0"
  );
  console.log('\n=== Cross-check: Sticky prepaid vs bin_lookup is_prepaid ===');
  console.table(crossCheck);

  closeDb();
}

main().catch(err => {
  console.error('Backfill failed:', err);
  closeDb();
  process.exit(1);
});
