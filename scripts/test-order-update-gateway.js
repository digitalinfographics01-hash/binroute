/**
 * Test: Can order_update change the gateway on an existing order?
 *
 * Tests order_update with forceGatewayId on a Kytsan order.
 * If this works, we can set the gateway for the next natural rebill
 * without forcing an immediate bill via order_force_bill.
 */

const axios = require('axios');
const { initDb, queryOneSql, querySql } = require('../src/db/connection');

// Pick a recurring approved order to test on
const TEST_ORDER_ID = process.argv[2];
const TARGET_GATEWAY = process.argv[3];

if (!TEST_ORDER_ID || !TARGET_GATEWAY) {
  console.log('Usage: node scripts/test-order-update-gateway.js <order_id> <target_gateway_id>');
  console.log('Example: node scripts/test-order-update-gateway.js 647896 193');
  process.exit(1);
}

async function apiCall(baseUrl, auth, method, body, contentType = 'application/json') {
  const url = `https://${baseUrl}/api/v1/${method}`;
  const config = { auth, timeout: 30000 };

  if (contentType === 'application/json') {
    config.headers = { 'Content-Type': 'application/json' };
    return axios.post(url, JSON.stringify(body), config);
  } else {
    config.headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
    const params = new URLSearchParams(body);
    return axios.post(url, params.toString(), config);
  }
}

async function viewOrder(baseUrl, auth, orderId) {
  const resp = await apiCall(baseUrl, auth, 'order_view', { order_id: String(orderId) }, 'form');
  const data = resp.data;
  return {
    order_id: data.order_id,
    gateway_id: data.gateway_id,
    order_status: data.order_status,
    is_recurring: data.is_recurring,
    recurring_date: data.recurring_date,
    preserve_force_gateway: data.preserve_force_gateway,
  };
}

async function main() {
  console.log('='.repeat(70));
  console.log('Test: order_update with forceGatewayId');
  console.log('='.repeat(70));

  initDb();
  const client = queryOneSql('SELECT * FROM clients WHERE id = 1');
  const baseUrl = client.sticky_base_url;
  const auth = { username: client.sticky_username, password: client.sticky_password };

  // Load gateway aliases
  const gateways = {};
  querySql('SELECT gateway_id, gateway_alias FROM gateways WHERE client_id = 1').forEach(g => {
    gateways[g.gateway_id] = g.gateway_alias;
  });

  console.log(`\nOrder: ${TEST_ORDER_ID}`);
  console.log(`Target gateway: ${TARGET_GATEWAY} (${gateways[TARGET_GATEWAY] || 'unknown'})`);

  // Step 1: View order BEFORE
  console.log('\n--- BEFORE ---');
  const before = await viewOrder(baseUrl, auth, TEST_ORDER_ID);
  console.log(`  Gateway: ${before.gateway_id} (${gateways[before.gateway_id] || '?'})`);
  console.log(`  Status: ${before.order_status}`);
  console.log(`  Recurring: ${before.is_recurring}, Next rebill: ${before.recurring_date}`);
  console.log(`  preserve_force_gateway: ${before.preserve_force_gateway}`);

  // Step 2: Try order_update with different param combos
  const tests = [
    { label: 'JSON + forceGatewayId', contentType: 'application/json', body: { order_id: TEST_ORDER_ID, forceGatewayId: TARGET_GATEWAY, preserve_force_gateway: '1' } },
    { label: 'JSON + gateway_id', contentType: 'application/json', body: { order_id: TEST_ORDER_ID, gateway_id: TARGET_GATEWAY } },
    { label: 'Form + forceGatewayId', contentType: 'form', body: { order_id: TEST_ORDER_ID, forceGatewayId: TARGET_GATEWAY, preserve_force_gateway: '1' } },
    { label: 'Form + gateway_id', contentType: 'form', body: { order_id: TEST_ORDER_ID, gateway_id: TARGET_GATEWAY } },
  ];

  for (const test of tests) {
    console.log(`\n--- TEST: ${test.label} ---`);
    try {
      const ct = test.contentType === 'form' ? 'application/x-www-form-urlencoded' : 'application/json';
      const resp = await apiCall(baseUrl, auth, 'order_update', test.body, ct);
      const data = resp.data;
      console.log(`  Response code: ${data.response_code}`);
      console.log(`  Raw: ${JSON.stringify(data).substring(0, 300)}`);

      if (data.response_code === '100' || data.response_code === 100) {
        // Check if gateway actually changed
        console.log('  >> SUCCESS response — checking order...');
        await new Promise(r => setTimeout(r, 1000));
        const after = await viewOrder(baseUrl, auth, TEST_ORDER_ID);
        console.log(`  Gateway now: ${after.gateway_id} (${gateways[after.gateway_id] || '?'})`);
        console.log(`  preserve_force_gateway: ${after.preserve_force_gateway}`);
        console.log(`  CHANGED: ${String(after.gateway_id) !== String(before.gateway_id) ? 'YES' : 'NO'}`);
      }
    } catch (err) {
      const errData = err.response?.data;
      const code = errData?.response_code || err.response?.status || err.code;
      const msg = errData?.error_message || err.message;
      console.log(`  Error: ${code} — ${msg}`);
      if (errData) console.log(`  Raw: ${JSON.stringify(errData).substring(0, 300)}`);
    }
    await new Promise(r => setTimeout(r, 1500));
  }

  // Step 3: View order AFTER all tests
  console.log('\n--- FINAL STATE ---');
  const final = await viewOrder(baseUrl, auth, TEST_ORDER_ID);
  console.log(`  Gateway: ${final.gateway_id} (${gateways[final.gateway_id] || '?'})`);
  console.log(`  preserve_force_gateway: ${final.preserve_force_gateway}`);
  console.log(`  Changed from ${before.gateway_id} → ${final.gateway_id}: ${String(final.gateway_id) !== String(before.gateway_id) ? 'YES' : 'NO'}`);

  console.log('\nDone!');
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
