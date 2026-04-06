const { initializeDatabase } = require('./src/db/schema');
const { querySql, queryOneSql, runSql, saveDb, closeDb } = require('./src/db/connection');
const StickyClient = require('./src/api/sticky-client');

async function run() {
  await initializeDatabase();
  const row = queryOneSql('SELECT * FROM clients WHERE id = 1');
  const client = new StickyClient({ baseUrl: row.sticky_base_url, username: row.sticky_username, password: row.sticky_password });

  // Get all order IDs
  const orders = querySql('SELECT order_id FROM orders WHERE client_id = 1');
  console.log(`Backfilling ${orders.length} orders with is_test, recurring_date, auth_id, transaction_id...`);

  const BATCH = 50;
  let testCount = 0;
  let recurringCount = 0;
  let processed = 0;

  for (let i = 0; i < orders.length; i += BATCH) {
    const batchIds = orders.slice(i, i + BATCH).map(o => o.order_id);

    try {
      const data = await client.orderView(batchIds.join(','));
      if (!data || data.response_code !== '100') continue;

      // Parse orders — batch order_view returns orders in data.data keyed by order_id
      let rawOrders = [];
      if (data.data && typeof data.data === 'object') {
        rawOrders = Object.values(data.data);
      } else if (data.order_id && typeof data.order_id === 'string' && !data.order_id.includes(',')) {
        rawOrders = [data]; // single order at top level
      }

      for (const raw of rawOrders) {
        if (!raw.order_id) continue;
        const orderId = parseInt(raw.order_id, 10);
        const isTestCc = raw.is_test_cc === '1';
        const noAuth = raw.auth_id === 'Not Available' || !raw.auth_id;
        const noTxn = raw.transaction_id === 'Not Available' || !raw.transaction_id;
        const isTest = (isTestCc || (noAuth && noTxn)) ? 1 : 0;
        const recurringDate = (raw.recurring_date && raw.recurring_date !== '0000-00-00') ? raw.recurring_date : null;

        runSql('UPDATE orders SET is_test = ?, recurring_date = ?, auth_id = ?, transaction_id = ? WHERE client_id = 1 AND order_id = ?',
          [isTest, recurringDate, raw.auth_id || null, raw.transaction_id || null, orderId]);

        if (isTest) testCount++;
        if (recurringDate) recurringCount++;
      }
    } catch (err) {
      console.log(`  Batch error: ${err.message}`);
    }

    processed += batchIds.length;
    if (processed % 500 === 0 || processed >= orders.length) {
      saveDb();
      console.log(`  ${processed}/${orders.length} — tests: ${testCount}, with recurring_date: ${recurringCount}`);
    }
  }

  saveDb();

  // Populate upcoming_rebills
  console.log('\nPopulating upcoming_rebills...');
  runSql('DELETE FROM upcoming_rebills WHERE client_id = 1');
  runSql(`INSERT INTO upcoming_rebills (client_id, order_id, customer_id, cc_first_6, cc_type, gateway_id, gateway_descriptor, campaign_id, recurring_date, billing_cycle, tx_type)
    SELECT client_id, order_id, customer_id, cc_first_6, cc_type, gateway_id, gateway_descriptor, campaign_id, recurring_date, billing_cycle, tx_type
    FROM orders WHERE client_id = 1 AND recurring_date > date('now') AND is_test = 0`);
  saveDb();
  const rebillCount = queryOneSql('SELECT COUNT(*) as cnt FROM upcoming_rebills WHERE client_id = 1').cnt;
  console.log(`Upcoming rebills: ${rebillCount}`);

  // Final stats
  console.log('\n=== RESULTS ===');
  console.log('Test orders:', testCount);
  console.log('Orders with recurring_date:', recurringCount);

  const total = queryOneSql('SELECT COUNT(*) as cnt FROM orders WHERE client_id = 1 AND is_test = 0').cnt;
  const approved = queryOneSql('SELECT COUNT(*) as cnt FROM orders WHERE client_id = 1 AND is_test = 0 AND order_status IN (2,6,8)').cnt;
  const calcBase = queryOneSql('SELECT COUNT(*) as cnt FROM orders WHERE client_id = 1 AND is_test = 0 AND order_status IN (2,6,7,8)').cnt;
  console.log('Non-test orders:', total);
  console.log('Approved (excl test):', approved);
  console.log('Calc base (excl test):', calcBase);
  console.log('NEW APPROVAL RATE:', (100 * approved / calcBase).toFixed(2) + '%');

  // Status breakdown excl tests
  console.log('\nStatus breakdown (excl test):');
  querySql('SELECT order_status, COUNT(*) as cnt FROM orders WHERE client_id = 1 AND is_test = 0 GROUP BY order_status ORDER BY cnt DESC')
    .forEach(r => console.log('  Status ' + r.order_status + ': ' + r.cnt));

  // Upcoming rebills by gateway
  console.log('\nUpcoming rebills by gateway:');
  querySql('SELECT gateway_id, gateway_descriptor, COUNT(*) as cnt FROM upcoming_rebills WHERE client_id = 1 GROUP BY gateway_id ORDER BY cnt DESC')
    .forEach(r => console.log('  GW ' + r.gateway_id + ' (' + r.gateway_descriptor + '): ' + r.cnt));

  closeDb();
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
