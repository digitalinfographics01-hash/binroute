/**
 * BinRoute AI Reprocess Test
 *
 * Reprocesses 4 declined orders using AI-recommended gateways.
 * Uses order_reprocess with JSON body + forceGatewayId.
 */

const axios = require('axios');
const { initDb, querySql, queryOneSql } = require('../src/db/connection');

// Orders to reprocess with AI-recommended gateways
const REPROCESS_ORDERS = [
  { order_id: 647079, forceGatewayId: 194, confidence: 48.5, bin: '470793', decline: 'Pick up card - SF' },
  { order_id: 647083, forceGatewayId: 188, confidence: 46.5, bin: '411776', decline: 'Pick up card - SF' },
  { order_id: 647067, forceGatewayId: 187, confidence: 41.6, bin: '434256', decline: 'Issuer Declined' },
  { order_id: 647084, forceGatewayId: 194, confidence: 31.7, bin: '545958', decline: 'Do Not Honor' },
];

async function main() {
  console.log('='.repeat(80));
  console.log('BinRoute AI — Reprocess Test');
  console.log('='.repeat(80));

  initDb();
  const client = queryOneSql('SELECT * FROM clients WHERE id = 1');
  const baseUrl = client.sticky_base_url;
  const auth = { username: client.sticky_username, password: client.sticky_password };

  // Load gateway aliases for display
  const gateways = {};
  querySql('SELECT gateway_id, gateway_alias FROM gateways WHERE client_id = 1').forEach(g => {
    gateways[g.gateway_id] = g.gateway_alias;
  });

  console.log(`\n  API: ${baseUrl}`);
  console.log(`  Orders to reprocess: ${REPROCESS_ORDERS.length}`);
  console.log(`  Total exposure: $${(REPROCESS_ORDERS.length * 9.97).toFixed(2)}`);

  const results = [];

  for (const order of REPROCESS_ORDERS) {
    console.log(`\n${'─'.repeat(80)}`);
    console.log(`  Order ${order.order_id} | BIN ${order.bin} | Last decline: ${order.decline}`);
    console.log(`  AI recommends: [${order.forceGatewayId}] ${gateways[order.forceGatewayId] || order.forceGatewayId} (${order.confidence}%)`);
    console.log(`  Calling order_reprocess...`);

    try {
      const url = `https://${baseUrl}/api/v1/order_reprocess`;
      const body = {
        order_id: String(order.order_id),
        forceGatewayId: String(order.forceGatewayId),
        preserve_force_gateway: '1',
      };

      const response = await axios.post(url, JSON.stringify(body), {
        auth,
        headers: { 'Content-Type': 'application/json' },
        timeout: 120000,
      });

      const data = response.data;
      console.log(`  RAW RESPONSE: ${JSON.stringify(data)}`);
      const code = data.response_code;
      const newOrderId = data.new_order_id || data.order_id || '?';
      const gwUsed = data.gateway_id || '?';
      const status = data.order_status || data.status || '?';
      const declineReason = data.decline_reason || data.error_message || '';

      const approved = code === '100' || status == 2;

      console.log(`  Response code: ${code}`);
      console.log(`  New order ID: ${newOrderId}`);
      console.log(`  Gateway used: ${gwUsed} (requested: ${order.forceGatewayId})`);
      console.log(`  Status: ${approved ? 'APPROVED ✓' : 'DECLINED ✗'}`);
      if (declineReason) console.log(`  Decline reason: ${declineReason}`);
      console.log(`  Gateway respected: ${String(gwUsed) === String(order.forceGatewayId) ? 'YES' : 'NO — routed to ' + gwUsed}`);

      results.push({
        order_id: order.order_id,
        bin: order.bin,
        ai_gw: order.forceGatewayId,
        ai_confidence: order.confidence,
        actual_gw: gwUsed,
        gw_respected: String(gwUsed) === String(order.forceGatewayId),
        approved,
        new_order_id: newOrderId,
        decline_reason: declineReason || null,
        response_code: code,
        raw: JSON.stringify(data).substring(0, 200),
      });

    } catch (err) {
      const errData = err.response?.data;
      const errCode = errData?.response_code || err.response?.status || err.code;
      const errMsg = errData?.error_message || errData?.decline_reason || err.message;

      console.log(`  ERROR: ${errCode} — ${errMsg}`);
      if (errData) console.log(`  Raw: ${JSON.stringify(errData).substring(0, 200)}`);

      results.push({
        order_id: order.order_id,
        bin: order.bin,
        ai_gw: order.forceGatewayId,
        ai_confidence: order.confidence,
        actual_gw: null,
        gw_respected: false,
        approved: false,
        new_order_id: null,
        decline_reason: errMsg,
        response_code: errCode,
        raw: JSON.stringify(errData || {}).substring(0, 200),
      });
    }

    // Small delay between calls
    await new Promise(r => setTimeout(r, 2000));
  }

  // Summary table
  console.log(`\n${'='.repeat(80)}`);
  console.log('RESULTS SUMMARY');
  console.log('='.repeat(80));

  const approved = results.filter(r => r.approved);
  const declined = results.filter(r => !r.approved);
  const gwRespected = results.filter(r => r.gw_respected);

  console.log(`\n  Order    BIN     AI GW  Conf   Actual GW  GW OK  Result      Decline Reason`);
  console.log('  ' + '─'.repeat(85));

  for (const r of results) {
    const gwOk = r.gw_respected ? 'YES' : 'NO';
    const result = r.approved ? 'APPROVED' : 'DECLINED';
    console.log(`  ${String(r.order_id).padEnd(8)} ${r.bin}  ${String(r.ai_gw).padStart(5)}  ${r.ai_confidence.toFixed(1)}%  ${String(r.actual_gw || '?').padStart(9)} ${gwOk.padStart(5)}  ${result.padStart(10)}  ${r.decline_reason || ''}`);
  }

  console.log(`\n  Approved: ${approved.length}/${results.length}`);
  console.log(`  Gateway respected: ${gwRespected.length}/${results.length}`);

  if (approved.length > 0) {
    console.log(`\n  *** AI RECOVERED ${approved.length} ORDER(S) — $${(approved.length * 9.97).toFixed(2)} REVENUE ***`);
  }

  console.log('\nDone!');
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
