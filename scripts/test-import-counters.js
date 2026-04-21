/**
 * Test the patched counters by re-fetching Apr 8 and comparing
 * reported totalFetched to apiTotal ground truth.
 *
 * Expected after patch:
 *   apiTotal ≈ 29124
 *   totalFetched ≈ 29124 (distinct), NOT 45512 (phantom)
 *   coverage ≈ 100%
 *
 * DB is untouched — INSERT OR IGNORE no-ops on existing rows.
 */
const { initDb, querySql } = require('../src/db/connection');
const DataIngestion = require('../src/api/ingestion');

(async () => {
  await initDb();
  const ing = new DataIngestion(6);
  ing.init();

  // Manually init the run-scoped Set that pullTransactions normally sets.
  ing._runSeenOrderIds = new Set();

  // Baseline DB count before test
  const before = querySql('SELECT COUNT(*) n FROM orders WHERE client_id=6')[0].n;
  console.log('DB row count BEFORE test:', before);
  console.log();

  let chunkLog = [];
  const onChunk = async (orders) => {
    if (orders.length === 0) return 0;
    const saved = ing._saveOrderBatchToDB(orders);
    return saved;
  };

  const log = (msg) => { chunkLog.push(msg); console.log(msg); };

  console.log('Re-fetching 04/08/2026 with patched counters...');
  console.log();
  const startTs = Date.now();

  const result = await ing._fetchAndVerifyDay('04/08/2026', log, {
    chunkTarget: 400,
    onChunk,
  });

  const elapsed = ((Date.now() - startTs) / 1000).toFixed(1);

  const after = querySql('SELECT COUNT(*) n FROM orders WHERE client_id=6')[0].n;

  console.log();
  console.log('='.repeat(60));
  console.log('RESULTS');
  console.log('='.repeat(60));
  console.log('Elapsed:      ' + elapsed + 's');
  console.log('apiTotal:     ' + result.apiTotal);
  console.log('totalFetched: ' + result.totalFetched + '  (was 45512 before patch)');
  console.log('totalSaved:   ' + result.totalSaved);
  console.log('apiCalls:     ' + result.apiCalls);
  console.log('chunksSaved:  ' + result.chunksSaved);
  console.log('chunksFailed: ' + result.chunksFailed);
  console.log();
  console.log('Coverage: ' + ((result.totalFetched / result.apiTotal) * 100).toFixed(2) + '%');
  console.log();
  console.log('DB row count AFTER test:  ' + after);
  console.log('DB row delta:             ' + (after - before) + '  (should be ~0 since INSERT OR IGNORE)');
  console.log();
  console.log('Run-scoped seen Set size: ' + ing._runSeenOrderIds.size);
  console.log();
  if (result.totalFetched >= result.apiTotal * 0.98 && result.totalFetched <= result.apiTotal * 1.02) {
    console.log('PASS: totalFetched is within ±2% of apiTotal (patched correctly)');
  } else if (result.totalFetched > result.apiTotal * 1.1) {
    console.log('FAIL: totalFetched still inflated — patch not working');
  } else {
    console.log('PARTIAL: coverage below 98%, but not inflated');
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
