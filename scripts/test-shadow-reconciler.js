#!/usr/bin/env node
/**
 * test-shadow-reconciler.js — end-to-end test of Phase 3 reconciliation.
 *
 * Inserts a fake shadow_decisions row and a fake orders row carrying the
 * "BinRoute_shadow: id=<uuid>" marker, then runs reconcileShadowDecisions and
 * asserts that actual_* columns fill in correctly.
 *
 * Tests:
 *   1. Marker in employee_notes — happy path
 *   2. Marker in system_notes — field fallback
 *   3. Marker in custom_fields — field fallback
 *   4. Idempotency — re-running doesn't mutate a reconciled row
 *   5. Orphan — marker present but no shadow row → skipped, no error
 *   6. shadow-report helpers return sensible values
 *
 * Leaves no residue: drops the fake rows after each scenario.
 */

const crypto = require('crypto');
const { initDb, querySql, queryOneSql, runSql, saveDb, closeDb } = require('../src/db/connection');
const { reconcileShadowDecisions } = require('../src/pipeline/post-sync');
const {
  getShadowSummary,
  getReasonBreakdown,
  getConcentration,
  getShadowByIssuer,
  getRecentDecisions,
} = require('../src/analytics/shadow-report');

const CLIENT_ID = 1;

// Colours for output — plain ANSI, no external dep.
const G = s => `\x1b[32m${s}\x1b[0m`;
const R = s => `\x1b[31m${s}\x1b[0m`;
const B = s => `\x1b[36m${s}\x1b[0m`;

let failures = 0;
function assertEq(actual, expected, label) {
  const ok = actual === expected
    || (actual != null && expected != null && String(actual) === String(expected));
  if (ok) {
    console.log(`  ${G('✓')} ${label} ${B(`(${actual})`)}`);
  } else {
    console.log(`  ${R('✗')} ${label}  expected=${expected}  actual=${actual}`);
    failures++;
  }
}

function insertShadow(shadowId, recGw, bin = '403163') {
  runSql(
    `INSERT INTO shadow_decisions (
       shadow_id, client_id, bin, amount, sales_type,
       recommended_gateway_id, recommended_processor, confidence, reason,
       candidate_pool_json, lookup_has_data, lookup_best_rate,
       daemon_timed_out, daemon_latency_ms, latency_ms_server,
       feature_snapshot_json, model_version,
       request_received_at,
       would_have_approved
     ) VALUES (?, ?, ?, 49.95, 'INITIALS',
               ?, 'Paysafe', 0.24, 'lookup_ai_hybrid',
               '[]', 1, 0.256,
               0, 28, 35,
               '{}', 'test_model.pkl@2026-04-16',
               CURRENT_TIMESTAMP,
               0.256)`,
    [shadowId, CLIENT_ID, bin, recGw]
  );
}

/** Insert a fake order carrying a shadow marker in one of the notes fields. */
function insertFakeOrder({ stickyOrderId, gatewayId, orderStatus, shadowId, field }) {
  const marker = `BinRoute_shadow: id=${shadowId} rec_gw=${gatewayId} rec_proc=Paysafe conf=0.24 reason=lookup_ai_hybrid lat=35`;
  const col = field; // employee_notes | system_notes | custom_fields
  // Build dynamic SQL safely — whitelist the column.
  if (!['employee_notes', 'system_notes', 'custom_fields'].includes(col)) {
    throw new Error(`invalid notes field: ${col}`);
  }
  runSql(
    `INSERT INTO orders (client_id, order_id, gateway_id, order_status, is_test, is_internal_test, ${col})
     VALUES (?, ?, ?, ?, 0, 0, ?)`,
    [CLIENT_ID, stickyOrderId, gatewayId, orderStatus, marker]
  );
  const row = queryOneSql(
    `SELECT id FROM orders WHERE client_id = ? AND order_id = ?`,
    [CLIENT_ID, stickyOrderId]
  );
  return row.id;
}

function cleanup(shadowIds, orderInternalIds) {
  for (const sid of shadowIds) {
    runSql('DELETE FROM shadow_decisions WHERE shadow_id = ?', [sid]);
  }
  for (const oid of orderInternalIds) {
    runSql('DELETE FROM orders WHERE id = ?', [oid]);
  }
  saveDb();
}

async function main() {
  await initDb();
  console.log(B('=== Phase 3 reconciler smoke test ==='));

  const createdShadowIds = [];
  const createdOrderIds = [];

  try {
    // ---- Scenario 1: marker in employee_notes, status=2 approved, AI pick != actual ----
    console.log(B('\n[1] employee_notes, approved, gw mismatch'));
    const s1 = crypto.randomUUID();
    createdShadowIds.push(s1);
    insertShadow(s1, /*recGw=*/190);                 // we recommended 190 (Paysafe)
    const o1 = insertFakeOrder({
      stickyOrderId: 9000001,
      gatewayId: 172,                                // Beast actually picked 172 (Cliq)
      orderStatus: 2,                                // approved
      shadowId: s1,
      field: 'employee_notes',
    });
    createdOrderIds.push(o1);

    let r = reconcileShadowDecisions(CLIENT_ID);
    console.log(`  stats: ${JSON.stringify(r)}`);
    const row1 = queryOneSql('SELECT * FROM shadow_decisions WHERE shadow_id = ?', [s1]);
    assertEq(row1.actual_order_id, o1, 'actual_order_id');
    assertEq(row1.actual_sticky_order_id, 9000001, 'actual_sticky_order_id');
    assertEq(row1.actual_gateway_id, 172, 'actual_gateway_id');
    assertEq(row1.actual_outcome, 'approved', 'actual_outcome');
    assertEq(row1.would_match, 0, 'would_match (190 != 172)');
    assertEq(row1.reconciled_at != null, true, 'reconciled_at set');

    // ---- Scenario 2: marker in system_notes, status=7 declined, match ----
    console.log(B('\n[2] system_notes, declined, gw match'));
    const s2 = crypto.randomUUID();
    createdShadowIds.push(s2);
    insertShadow(s2, /*recGw=*/190);
    const o2 = insertFakeOrder({
      stickyOrderId: 9000002,
      gatewayId: 190,                                // matches our rec
      orderStatus: 7,                                // declined
      shadowId: s2,
      field: 'system_notes',
    });
    createdOrderIds.push(o2);

    r = reconcileShadowDecisions(CLIENT_ID);
    const row2 = queryOneSql('SELECT * FROM shadow_decisions WHERE shadow_id = ?', [s2]);
    assertEq(row2.actual_gateway_id, 190, 'actual_gateway_id');
    assertEq(row2.actual_outcome, 'declined', 'actual_outcome');
    assertEq(row2.would_match, 1, 'would_match=1');

    // ---- Scenario 3: marker in custom_fields ----
    console.log(B('\n[3] custom_fields path'));
    const s3 = crypto.randomUUID();
    createdShadowIds.push(s3);
    insertShadow(s3, /*recGw=*/190);
    const o3 = insertFakeOrder({
      stickyOrderId: 9000003,
      gatewayId: 187,
      orderStatus: 6,                                // approved (alt code)
      shadowId: s3,
      field: 'custom_fields',
    });
    createdOrderIds.push(o3);

    r = reconcileShadowDecisions(CLIENT_ID);
    const row3 = queryOneSql('SELECT * FROM shadow_decisions WHERE shadow_id = ?', [s3]);
    assertEq(row3.actual_outcome, 'approved', 'status 6 → approved');
    assertEq(row3.actual_gateway_id, 187, 'actual_gateway_id from custom_fields path');

    // ---- Scenario 4: idempotency — re-running must not mutate ----
    console.log(B('\n[4] idempotency — re-run'));
    const before = queryOneSql('SELECT reconciled_at FROM shadow_decisions WHERE shadow_id = ?', [s1]);
    r = reconcileShadowDecisions(CLIENT_ID);
    const after = queryOneSql('SELECT reconciled_at FROM shadow_decisions WHERE shadow_id = ?', [s1]);
    assertEq(before.reconciled_at, after.reconciled_at, 'reconciled_at unchanged');
    assertEq(r.skipped_reconciled >= 3, true, `skipped_reconciled >= 3 (got ${r.skipped_reconciled})`);
    assertEq(r.matched, 0, 'matched=0 on second run');

    // ---- Scenario 5: orphan — marker without shadow row ----
    console.log(B('\n[5] orphan marker'));
    const orphanUuid = crypto.randomUUID();           // no shadow row for this
    const oOrphan = insertFakeOrder({
      stickyOrderId: 9000099,
      gatewayId: 190,
      orderStatus: 2,
      shadowId: orphanUuid,
      field: 'employee_notes',
    });
    createdOrderIds.push(oOrphan);
    r = reconcileShadowDecisions(CLIENT_ID);
    assertEq(r.skipped_orphan >= 1, true, `skipped_orphan >= 1 (got ${r.skipped_orphan})`);
    assertEq(r.matched, 0, 'orphan did not cause a bogus match');

    // ---- Scenario 6: shadow-report summary ----
    console.log(B('\n[6] shadow-report helpers'));
    const sum = getShadowSummary(CLIENT_ID);
    console.log(`  getShadowSummary: total=${sum.total} reconciled=${sum.reconciled} reconciled_pct=${(sum.reconciled_pct*100).toFixed(1)}%`);
    console.log(`    would_match_pct=${(sum.would_match_pct*100).toFixed(1)}%  lift_pp=${sum.lift_pp != null ? (sum.lift_pp*100).toFixed(2)+'pp' : 'n/a'}`);
    console.log(`    latency_ms_server p50=${sum.latency_ms_server.p50} p95=${sum.latency_ms_server.p95}`);
    assertEq(sum.total >= 3, true, 'summary total includes our fakes');
    assertEq(sum.reconciled >= 3, true, 'summary reconciled >= 3');

    const reasons = getReasonBreakdown(CLIENT_ID);
    console.log(`  getReasonBreakdown: ${reasons.slice(0, 3).map(r => r.reason+'='+r.n).join(', ')}`);
    assertEq(reasons.length > 0, true, 'reasons non-empty');

    const conc = getConcentration(CLIENT_ID);
    console.log(`  getConcentration (top 3): ${conc.slice(0,3).map(c => `gw${c.gateway_id}:${(c.share*100).toFixed(1)}%`).join(' ')}`);
    assertEq(conc.length > 0, true, 'concentration non-empty');

    const issuer = getShadowByIssuer(CLIENT_ID);
    console.log(`  getShadowByIssuer: ${issuer.length} buckets`);
    assertEq(issuer.length > 0, true, 'by-issuer non-empty');

    const recent = getRecentDecisions(CLIENT_ID, 5);
    console.log(`  getRecentDecisions: ${recent.length} rows, newest reason=${recent[0]?.reason}`);
    assertEq(recent[0]?.candidate_pool != null || recent[0]?.candidate_pool_json !== undefined, true, 'candidate_pool parsed');
  } finally {
    console.log(B('\n--- Cleanup ---'));
    cleanup(createdShadowIds, createdOrderIds);
    console.log(`  Deleted ${createdShadowIds.length} shadow rows, ${createdOrderIds.length} order rows`);
    closeDb();
  }

  console.log(B('\n=== Summary ==='));
  if (failures === 0) {
    console.log(G(`All assertions passed.`));
    process.exit(0);
  } else {
    console.log(R(`${failures} assertion failure(s).`));
    process.exit(1);
  }
}

main().catch(err => {
  console.error(R('FATAL:'), err);
  try { closeDb(); } catch {}
  process.exit(2);
});
