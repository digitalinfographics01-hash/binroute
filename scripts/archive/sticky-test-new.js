const { initializeDatabase } = require('./src/db/schema');
const { queryOneSql, closeDb } = require('./src/db/connection');
const axios = require('axios');

async function run() {
  await initializeDatabase();
  const row = queryOneSql('SELECT * FROM clients WHERE id = 1');
  if (!row) { console.log('No client found'); return; }

  const baseUrl = row.sticky_base_url.replace(/\/$/, '');
  const auth = { username: row.sticky_username, password: row.sticky_password };

  // 7 days ago
  const ago7 = new Date(); ago7.setDate(ago7.getDate() - 7);
  const today = new Date();
  const fmt = (d) => `${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getDate()).padStart(2,'0')}/${d.getFullYear()}`;

  const params = {
    campaign_id: 'all',
    start_date: fmt(ago7),
    end_date: fmt(today),
    date_type: 'create',
    criteria: 'all',
    search_type: 'all',
    return_type: 'order_view',
    resultsPerPage: '200',
    page: '1',
  };

  console.log('=== ORDER_FIND TEST: campaign_id="all", return_type="order_view", last 7 days ===');
  console.log('Params:', JSON.stringify(params, null, 2));
  console.log('Date range:', fmt(ago7), 'to', fmt(today));
  console.log('');

  const formData = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    formData.append(key, String(value));
  }

  try {
    const response = await axios.post(
      `https://${baseUrl}/api/v1/order_find`,
      formData.toString(),
      {
        auth,
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        timeout: 60000,
      }
    );

    const data = response.data;

    console.log('1. RESPONSE CODE:', data.response_code);
    console.log('2. TOTAL ORDERS:', data.total_orders || data.totalResults || data.total_count || 'N/A');
    console.log('3. TOP-LEVEL KEYS:', Object.keys(data).join(', '));
    console.log('');

    // Find the order data - could be in data.data, data.order_id, or directly as numbered keys
    let orders = [];
    if (data.data && typeof data.data === 'object') {
      orders = Array.isArray(data.data) ? data.data : Object.values(data.data);
    } else {
      // Check for numbered keys like "0", "1", "2"
      const numericKeys = Object.keys(data).filter(k => /^\d+$/.test(k));
      if (numericKeys.length > 0) {
        orders = numericKeys.map(k => data[k]);
      }
    }

    console.log('4. ORDERS IN RESPONSE:', orders.length);
    console.log('');

    if (orders.length > 0) {
      // Show first 2 complete order records
      for (let i = 0; i < Math.min(2, orders.length); i++) {
        console.log(`=== ORDER ${i + 1} RAW JSON ===`);
        console.log(JSON.stringify(orders[i], null, 2));
        console.log('');
      }

      // List every field name from first order
      console.log('=== ALL FIELD NAMES (from order 1) ===');
      const fields = Object.keys(orders[0]);
      fields.forEach(f => console.log('  ' + f));
      console.log(`\nTotal fields: ${fields.length}`);

      // Check cascade fields
      const cascadeFields = fields.filter(f =>
        f.toLowerCase().includes('cascade') ||
        f.toLowerCase().includes('retry') ||
        f.toLowerCase().includes('reroute') ||
        f.toLowerCase().includes('original') ||
        f.toLowerCase().includes('billing_cycle')
      );
      console.log('\nCASCADE / BILLING_CYCLE FIELDS:', cascadeFields.join(', ') || 'NONE');
    } else {
      // Maybe order data is at top level with order_id
      console.log('No orders array found. Dumping first 3000 chars of response:');
      console.log(JSON.stringify(data, null, 2).substring(0, 3000));
    }
  } catch (err) {
    console.log('ERROR:', err.message);
    if (err.response) {
      console.log('Response status:', err.response.status);
      console.log('Response data:', JSON.stringify(err.response.data));
    }
  }

  closeDb();
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
