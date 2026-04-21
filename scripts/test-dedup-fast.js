/**
 * FAST test of the phantom-count fix.
 *
 * Directly simulates the cap-hit scenario we proved in Test 2:
 *   - Query parent window (19:38-19:56) → 500 of 609 orders
 *   - Query half A (19:38-19:47) → 315 (all in parent)
 *   - Query half B (19:48-19:56) → 294 (109 new)
 *
 * Feeds each batch through _saveOrderBatchToDB.
 *
 * BEFORE patch: stats.orders would increment by 500+315+294 = 1109
 * AFTER patch:  stats.orders should increment by exactly 609 (distinct)
 */
const { initDb, querySql } = require('../src/db/connection');
const DataIngestion = require('../src/api/ingestion');

async function queryWindow(client, start, end) {
  const data = await client._post('order_find', {
    campaign_id: 'all',
    start_date: '04/08/2026',
    end_date: '04/08/2026',
    start_time: start,
    end_time: end,
    date_type: 'create',
    criteria: 'all',
    search_type: 'all',
    return_type: 'order_view',
    results_per_page: 500,
    page: 1,
  });
  if (!data || data.response_code !== '100') return { orders: [], total: 0 };
  if (data.data && typeof data.data === 'object') {
    const arr = Array.isArray(data.data) ? data.data : Object.values(data.data);
    return { orders: arr, total: parseInt(data.total_orders || 0, 10) };
  }
  const numKeys = Object.keys(data).filter(k => /^\d+$/.test(k));
  const orders = numKeys.sort((a, b) => parseInt(a) - parseInt(b)).map(k => data[k]);
  return { orders, total: parseInt(data.total_orders || 0, 10) };
}

(async () => {
  await initDb();
  const ing = new DataIngestion(6);
  ing.init();

  // Init the run-scoped Set (pullTransactions normally does this)
  ing._runSeenOrderIds = new Set();

  const beforeDbCount = querySql('SELECT COUNT(*) n FROM orders WHERE client_id=6')[0].n;
  const beforeStats = { orders: ing.stats.orders, errors: ing.stats.errors };

  console.log('BEFORE:');
  console.log('  DB rows:       ' + beforeDbCount);
  console.log('  stats.orders:  ' + beforeStats.orders);
  console.log();

  console.log('Fetching 3 overlapping windows (parent + halves)...');
  const parent = await queryWindow(ing.client, '19:38:00', '19:56:59');
  const halfA = await queryWindow(ing.client, '19:38:00', '19:47:59');
  const halfB = await queryWindow(ing.client, '19:48:00', '19:56:59');

  console.log('  Parent: total=' + parent.total + ', returned=' + parent.orders.length);
  console.log('  Half A: total=' + halfA.total + ', returned=' + halfA.orders.length);
  console.log('  Half B: total=' + halfB.total + ', returned=' + halfB.orders.length);
  console.log();

  const naiveSum = parent.orders.length + halfA.orders.length + halfB.orders.length;
  const trueUnique = new Set([
    ...parent.orders.map(o => o.order_id),
    ...halfA.orders.map(o => o.order_id),
    ...halfB.orders.map(o => o.order_id),
  ]).size;
  console.log('Naive (buggy) sum: ' + naiveSum);
  console.log('True unique union: ' + trueUnique);
  console.log('Phantom amount:    ' + (naiveSum - trueUnique));
  console.log();

  console.log('Feeding all 3 through _saveOrderBatchToDB...');
  const s1 = ing._saveOrderBatchToDB(parent.orders);
  console.log('  parent pass: saved=' + s1);
  const s2 = ing._saveOrderBatchToDB(halfA.orders);
  console.log('  halfA pass:  saved=' + s2 + '  (expected 0 — all already in parent 500)');
  const s3 = ing._saveOrderBatchToDB(halfB.orders);
  console.log('  halfB pass:  saved=' + s3 + '  (expected ~109 — the new recovered tail)');
  console.log();

  const afterDbCount = querySql('SELECT COUNT(*) n FROM orders WHERE client_id=6')[0].n;
  const afterStats = { orders: ing.stats.orders, errors: ing.stats.errors };

  console.log('AFTER:');
  console.log('  DB rows:       ' + afterDbCount + '  (delta: ' + (afterDbCount - beforeDbCount) + ')');
  console.log('  stats.orders:  ' + afterStats.orders + '  (delta: ' + (afterStats.orders - beforeStats.orders) + ')');
  console.log('  seen.size:     ' + ing._runSeenOrderIds.size);
  console.log();

  const statsDelta = afterStats.orders - beforeStats.orders;
  console.log('='.repeat(60));
  console.log('VERDICT');
  console.log('='.repeat(60));
  console.log('Naive (buggy) sum:        ' + naiveSum);
  console.log('True unique:              ' + trueUnique);
  console.log('Patched counter reported: ' + statsDelta);
  console.log();
  if (statsDelta === trueUnique) {
    console.log('PASS — counter matches true unique, phantom eliminated');
  } else if (statsDelta === naiveSum) {
    console.log('FAIL — counter still phantom-counts');
  } else {
    console.log('PARTIAL — unexpected delta, inspect');
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
