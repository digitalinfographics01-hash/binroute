/**
 * Diagnostic script: test all Sticky.io order discovery methods
 * to find which parameter combinations return data.
 */
const { initializeDatabase } = require('./src/db/schema');
const { queryOneSql, closeDb } = require('./src/db/connection');
const StickyClient = require('./src/api/sticky-client');

async function run() {
  await initializeDatabase();
  const row = queryOneSql('SELECT * FROM clients WHERE id = 1');
  if (!row) { console.log('No client found'); return; }

  const client = new StickyClient({
    baseUrl: row.sticky_base_url,
    username: row.sticky_username,
    password: row.sticky_password,
  });

  const today = new Date();
  const ago30 = new Date(); ago30.setDate(ago30.getDate() - 30);
  const ago7 = new Date(); ago7.setDate(ago7.getDate() - 7);

  const fmt = (d) => `${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getDate()).padStart(2,'0')}/${d.getFullYear()}`;
  const fmtIso = (d) => `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;

  console.log('=== STICKY.IO ORDER DISCOVERY DIAGNOSTIC ===\n');

  // Test 1: order_find WITHOUT campaign_id (various param combos)
  const tests = [
    { name: 'order_find: date only', method: 'order_find', params: { start_date: fmt(ago30), end_date: fmt(today) } },
    { name: 'order_find: date + criteria=all', method: 'order_find', params: { start_date: fmt(ago30), end_date: fmt(today), criteria: 'all' } },
    { name: 'order_find: date + campaign_id=all', method: 'order_find', params: { start_date: fmt(ago30), end_date: fmt(today), campaign_id: 'all' } },
    { name: 'order_find: date + campaign_id empty', method: 'order_find', params: { start_date: fmt(ago30), end_date: fmt(today), campaign_id: '' } },
    { name: 'order_find: known campaign_id=1', method: 'order_find', params: { campaign_id: 1, start_date: fmt(ago30), end_date: fmt(today), criteria: 'all' } },
    { name: 'order_find: known campaign_id=1 + resultsPerPage', method: 'order_find', params: { campaign_id: 1, start_date: fmt(ago30), end_date: fmt(today), criteria: 'all', resultsPerPage: 5, page: 1 } },
  ];

  // Test 2: order_find_updated variations
  tests.push(
    { name: 'order_find_updated: 30d MM/DD/YYYY', method: 'order_find_updated', params: { start_date: fmt(ago30), end_date: fmt(today) } },
    { name: 'order_find_updated: 7d MM/DD/YYYY', method: 'order_find_updated', params: { start_date: fmt(ago7), end_date: fmt(today) } },
    { name: 'order_find_updated: 30d + resultsPerPage', method: 'order_find_updated', params: { start_date: fmt(ago30), end_date: fmt(today), resultsPerPage: 5, page: 1 } },
    { name: 'order_find_updated: ISO dates', method: 'order_find_updated', params: { start_date: fmtIso(ago30), end_date: fmtIso(today) } },
    { name: 'order_find_updated: with campaign_id=1', method: 'order_find_updated', params: { start_date: fmt(ago30), end_date: fmt(today), campaign_id: 1 } },
  );

  // Test 3: other endpoints
  tests.push(
    { name: 'transaction_find: date only', method: 'transaction_find', params: { start_date: fmt(ago7), end_date: fmt(today) } },
    { name: 'order_search: date only', method: 'order_search', params: { start_date: fmt(ago7), end_date: fmt(today) } },
  );

  for (const test of tests) {
    console.log(`--- ${test.name} ---`);
    console.log(`  params: ${JSON.stringify(test.params)}`);
    try {
      const data = await client._post(test.method, test.params);
      const code = data?.response_code;
      const keys = data ? Object.keys(data) : [];
      const hasOrders = data?.order_id || data?.data || data?.orders;
      const totalField = data?.totalResults || data?.total_orders || data?.total_count || data?.orderCount;
      console.log(`  response_code: ${code}`);
      console.log(`  keys: ${keys.join(', ')}`);
      if (hasOrders) {
        const sample = typeof hasOrders === 'string' ? hasOrders.substring(0, 200) : JSON.stringify(hasOrders).substring(0, 200);
        console.log(`  HAS DATA! Sample: ${sample}`);
        console.log(`  total: ${totalField}`);
      } else {
        console.log(`  No order data in response`);
      }
    } catch (err) {
      console.log(`  ERROR: ${err.message}`);
    }
    console.log('');
  }

  // Test 4: Check campaign_find_active to see total campaigns
  console.log('--- campaign_find_active ---');
  try {
    const data = await client._post('campaign_find_active');
    console.log(`  response_code: ${data?.response_code}`);
    if (data?.campaign_id) {
      const ids = typeof data.campaign_id === 'string' ? data.campaign_id.split(',') :
        Array.isArray(data.campaign_id) ? data.campaign_id : [data.campaign_id];
      const numIds = ids.map(id => parseInt(String(id).trim(), 10)).filter(Boolean);
      console.log(`  Active campaigns: ${numIds.length}`);
      console.log(`  ID range: ${Math.min(...numIds)} to ${Math.max(...numIds)}`);
      console.log(`  All IDs: ${numIds.sort((a,b) => a-b).join(', ')}`);
    }
  } catch (err) {
    console.log(`  ERROR: ${err.message}`);
  }

  closeDb();
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
