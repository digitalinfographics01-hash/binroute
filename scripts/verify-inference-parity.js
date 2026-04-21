#!/usr/bin/env node
/**
 * P4.1 — Inference smoke test.
 *
 * Sends scripted POST /api/route requests for 5 known BINs and validates:
 *   1. Response arrives with confidence non-null, non-zero
 *   2. candidate_pool_json entries have ai_score when daemon_timed_out=0
 *   3. candidate_pool_json entries have rank_position, spread_vs_best, spread_vs_avg
 *   4. Latency < 200ms soft budget
 *   5. P3 columns present in the shadow_decisions row
 *
 * Usage:
 *   node scripts/verify-inference-parity.js [base_url] [api_key] [client_id]
 *
 * Defaults: http://127.0.0.1:3000, reads from env BINROUTE_API_KEY / BINROUTE_CLIENT_ID
 */

const axios = require('axios');
const { initDb, querySql, queryOneSql, closeDb } = require('../src/db/connection');

const BASE_URL = process.argv[2] || process.env.BINROUTE_URL || 'http://127.0.0.1:3000';
const API_KEY = process.argv[3] || process.env.BINROUTE_API_KEY;
const CLIENT_ID = parseInt(process.argv[4] || process.env.BINROUTE_CLIENT_ID || '1', 10);

// 5 test BINs: mix of high-volume, low-volume, and likely-unknown.
const TEST_BINS = [
  { bin: '411111', label: 'Visa classic (high volume)', amount: 49.95 },
  { bin: '545454', label: 'MC mid-range', amount: 79.00 },
  { bin: '374245', label: 'Amex (lower volume)', amount: 129.00 },
  { bin: '601100', label: 'Discover', amount: 39.95 },
  { bin: '999999', label: 'Unknown BIN (no lookup data)', amount: 59.00 },
];

let passed = 0;
let failed = 0;
const issues = [];

function assert(condition, msg) {
  if (condition) {
    passed++;
  } else {
    failed++;
    issues.push(msg);
    console.log(`  FAIL: ${msg}`);
  }
}

async function testBin(entry) {
  const { bin, label, amount } = entry;
  console.log(`\n--- ${label} (BIN ${bin}) ---`);

  const headers = { 'Content-Type': 'application/json' };
  if (API_KEY) {
    headers['x-api-key'] = API_KEY;
    headers['x-client-id'] = String(CLIENT_ID);
  }

  const start = Date.now();
  let resp;
  try {
    resp = await axios.post(`${BASE_URL}/api/route`, {
      bin, amount, sales_type: 'INITIALS',
    }, { headers, timeout: 5000 });
  } catch (err) {
    const status = err.response ? err.response.status : 'NETWORK_ERROR';
    assert(false, `${bin}: request failed (${status}: ${err.message})`);
    return;
  }
  const latency = Date.now() - start;
  const data = resp.data;

  console.log(`  shadow_id: ${data.shadow_id}`);
  console.log(`  gateway_id: ${data.gateway_id}, processor: ${data.processor}`);
  console.log(`  confidence: ${data.confidence}, reason: ${data.reason}`);
  console.log(`  latency: ${latency}ms`);

  // Basic response checks
  assert(data.shadow_id != null, `${bin}: shadow_id is null`);
  assert(data.reason != null, `${bin}: reason is null`);
  assert(latency < 200, `${bin}: latency ${latency}ms exceeds 200ms soft budget`);

  // Confidence check (skip for no_candidates / unknown BIN)
  if (data.reason !== 'no_candidates') {
    assert(data.confidence != null && data.confidence !== 0, `${bin}: confidence is null or zero`);
  }

  // Check the shadow_decisions row in DB for P3 columns
  if (data.shadow_id) {
    const row = queryOneSql(
      'SELECT * FROM shadow_decisions WHERE shadow_id = ?',
      [data.shadow_id]
    );
    if (row) {
      // P3 transaction context columns
      assert(row.confidence_tier != null, `${bin}: confidence_tier not populated`);
      assert(row.hour_of_day != null, `${bin}: hour_of_day not populated`);
      assert(row.day_of_week != null, `${bin}: day_of_week not populated`);

      // P3 decision metadata (may be null for edge cases, but should exist as column)
      assert('best_lookup_rate' in row, `${bin}: best_lookup_rate column missing`);
      assert('chosen_lookup_rate' in row, `${bin}: chosen_lookup_rate column missing`);
      assert('regret' in row, `${bin}: regret column missing`);
      assert('expected_approval' in row, `${bin}: expected_approval column missing`);
      assert('would_have_approved_binary' in row, `${bin}: would_have_approved_binary column missing`);
      assert('ai_disagreed_with_lookup' in row, `${bin}: ai_disagreed_with_lookup column missing`);

      // candidate_pool_json checks
      if (row.candidate_pool_json) {
        const pool = JSON.parse(row.candidate_pool_json);
        if (pool.length > 0) {
          const first = pool[0];
          assert('rank_position' in first, `${bin}: pool missing rank_position`);
          assert('spread_vs_best' in first, `${bin}: pool missing spread_vs_best`);
          assert('spread_vs_avg' in first, `${bin}: pool missing spread_vs_avg`);

          // If daemon didn't time out, all pool entries should have ai_score
          if (row.daemon_timed_out === 0 && pool.length >= 2) {
            const eligible = pool.filter(e => e.lookup_action !== 'hard_exclude');
            const withScore = eligible.filter(e => e.ai_score != null);
            assert(
              withScore.length === eligible.length,
              `${bin}: ${eligible.length - withScore.length} eligible pool entries missing ai_score (daemon didn't timeout)`
            );
          }
        }
      }
    } else {
      assert(false, `${bin}: shadow_decisions row not found for ${data.shadow_id}`);
    }
  }
}

async function main() {
  if (!API_KEY) {
    console.log('WARN: No API key provided. Set BINROUTE_API_KEY env var or pass as argv[3].');
    console.log('      Will attempt requests without auth (may 401).\n');
  }

  await initDb();

  // Verify P3 schema columns exist
  console.log('=== Schema column check ===');
  const cols = querySql("PRAGMA table_info(shadow_decisions)").map(r => r.name);
  const p3Cols = [
    'merchant_vertical', 'issuer_bank', 'card_type', 'card_brand', 'is_prepaid',
    'hour_of_day', 'day_of_week', 'amount_vs_bin_avg',
    'lookup_best_gateway_id', 'ai_disagreed_with_lookup', 'confidence_tier',
    'ai_score_spread', 'lookup_score_spread', 'best_lookup_rate', 'chosen_lookup_rate',
    'regret', 'actual_outcome_binary', 'would_have_approved_binary', 'expected_approval',
  ];
  for (const col of p3Cols) {
    assert(cols.includes(col), `shadow_decisions missing column: ${col}`);
  }
  console.log(`  ${p3Cols.length} P3 columns checked.\n`);

  // Run BIN tests
  console.log('=== Inference smoke tests ===');
  for (const entry of TEST_BINS) {
    await testBin(entry);
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`  Passed: ${passed}`);
  console.log(`  Failed: ${failed}`);
  if (issues.length > 0) {
    console.log('\n  Issues:');
    for (const i of issues) console.log(`    - ${i}`);
  }

  closeDb();
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => {
  console.error('Fatal:', err);
  try { closeDb(); } catch {}
  process.exit(1);
});
