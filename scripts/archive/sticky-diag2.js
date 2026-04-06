/**
 * Diagnostic 2: Dump campaign_find_active and test order_find with various campaign IDs
 */
const { initializeDatabase } = require('./src/db/schema');
const { queryOneSql, querySql, closeDb } = require('./src/db/connection');
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
  const ago90 = new Date(); ago90.setDate(ago90.getDate() - 90);
  const ago180 = new Date(); ago180.setDate(ago180.getDate() - 180);
  const fmt = (d) => `${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getDate()).padStart(2,'0')}/${d.getFullYear()}`;

  // 1. Full dump of campaign_find_active
  console.log('=== 1. FULL campaign_find_active RESPONSE ===');
  try {
    const data = await client._post('campaign_find_active');
    console.log(JSON.stringify(data, null, 2));
  } catch (err) {
    console.log('ERROR:', err.message);
  }

  // 2. Check campaigns in DB
  console.log('\n=== 2. CAMPAIGNS IN DB ===');
  const dbCampaigns = querySql('SELECT campaign_id, campaign_name FROM campaigns WHERE client_id = 1 ORDER BY campaign_id');
  console.log(`Total in DB: ${dbCampaigns.length}`);
  for (const c of dbCampaigns) {
    console.log(`  Campaign ${c.campaign_id}: ${c.campaign_name}`);
  }

  // 3. Test order_find with each DB campaign (90-day window)
  console.log('\n=== 3. order_find PER CAMPAIGN (90-day window) ===');
  const start90 = fmt(ago90);
  const end = fmt(today);
  console.log(`Date range: ${start90} to ${end}`);

  for (const c of dbCampaigns) {
    try {
      const data = await client._post('order_find', {
        campaign_id: c.campaign_id,
        start_date: start90,
        end_date: end,
        criteria: 'all',
        resultsPerPage: 5,
        page: 1,
      });
      const total = data?.total_orders || data?.totalResults || 0;
      const hasOrders = data?.response_code === '100';
      console.log(`  Campaign ${c.campaign_id}: code=${data?.response_code}, total_orders=${total}${hasOrders ? ' *** HAS ORDERS ***' : ''}`);
    } catch (err) {
      console.log(`  Campaign ${c.campaign_id}: ERROR ${err.message}`);
    }
  }

  // 4. Try scanning higher campaign IDs (100-300) that may not be in DB
  console.log('\n=== 4. SCANNING CAMPAIGN IDs 100-300 (checking if they exist) ===');
  const existingIds = new Set(dbCampaigns.map(c => c.campaign_id));
  let found = 0;

  for (let id = 100; id <= 300; id++) {
    if (existingIds.has(id)) continue;
    try {
      const data = await client._post('campaign_view', { campaign_id: id });
      if (data && data.response_code === '100') {
        found++;
        console.log(`  Campaign ${id}: EXISTS! Name="${data.campaign_name}", gateway_id=${data.gateway_id}`);
        // Also test order_find on this campaign
        const orderData = await client._post('order_find', {
          campaign_id: id,
          start_date: fmt(ago180),
          end_date: end,
          criteria: 'all',
          resultsPerPage: 5,
          page: 1,
        });
        const total = orderData?.total_orders || orderData?.totalResults || 0;
        console.log(`    -> order_find: code=${orderData?.response_code}, total_orders=${total}`);
      }
    } catch {
      // Skip errors (campaign doesn't exist)
    }
  }
  console.log(`Found ${found} campaigns in range 100-300 NOT in DB`);

  // 5. Check total orders in DB
  console.log('\n=== 5. ORDERS IN DB ===');
  const orderCount = queryOneSql('SELECT COUNT(*) as cnt FROM orders WHERE client_id = 1');
  const orderByCampaign = querySql('SELECT campaign_id, COUNT(*) as cnt FROM orders WHERE client_id = 1 GROUP BY campaign_id ORDER BY campaign_id');
  console.log(`Total orders: ${orderCount?.cnt || 0}`);
  for (const o of orderByCampaign) {
    console.log(`  Campaign ${o.campaign_id}: ${o.cnt} orders`);
  }

  closeDb();
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
