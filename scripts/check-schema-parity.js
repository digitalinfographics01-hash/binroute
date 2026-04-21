#!/usr/bin/env node
/**
 * P4.3 — Schema validation script.
 *
 * Validates that all P3 columns exist on the shadow_decisions table and that
 * the new P3 indexes are present. Useful as a pre-deploy check: did the
 * migration run?
 *
 * Usage:
 *   node scripts/check-schema-parity.js
 */

const { initDb, querySql, queryOneSql, closeDb } = require('../src/db/connection');

const EXPECTED_SHADOW_COLUMNS = [
  // Original columns (should already exist)
  'shadow_id', 'client_id', 'bin', 'amount', 'product_id', 'email_hash', 'sales_type',
  'recommended_gateway_id', 'recommended_processor', 'confidence', 'reason',
  'candidate_pool_json', 'lookup_has_data', 'lookup_best_rate',
  'daemon_timed_out', 'daemon_latency_ms', 'feature_snapshot_json', 'model_version',
  'request_received_at', 'response_sent_at', 'latency_ms_server',
  'latency_ms_client', 'arrived_before_submit', 'submit_timing_at',
  'actual_order_id', 'actual_sticky_order_id', 'actual_gateway_id', 'actual_processor',
  'actual_outcome', 'would_match', 'would_have_approved', 'reconciled_at',
  'ai_model_version', 'ai_score', 'ai_recommended_gateway_id', 'ai_scored_at',
  // P3 new columns
  'merchant_vertical', 'issuer_bank', 'card_type', 'card_brand', 'is_prepaid',
  'hour_of_day', 'day_of_week', 'amount_vs_bin_avg',
  'lookup_best_gateway_id', 'ai_disagreed_with_lookup', 'confidence_tier',
  'ai_score_spread', 'lookup_score_spread', 'best_lookup_rate', 'chosen_lookup_rate',
  'regret', 'actual_outcome_binary', 'would_have_approved_binary', 'expected_approval',
];

const EXPECTED_INDEXES = [
  'idx_shadow_client_time',
  'idx_shadow_unreconciled',
  'idx_shadow_sticky_order',
  'idx_shadow_client_tier',
  'idx_shadow_issuer_cardtype',
  'idx_shadow_disagreement',
];

const EXPECTED_OTHER_COLUMNS = [
  { table: 'clients', column: 'merchant_vertical' },
  { table: 'gateways', column: 'is_exploration' },
  { table: 'api_keys', column: 'api_key_hash' },
];

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.log(`  FAIL: ${msg}`);
  }
}

async function main() {
  await initDb();

  // Check shadow_decisions columns
  console.log('=== shadow_decisions columns ===');
  const shadowCols = querySql("PRAGMA table_info(shadow_decisions)").map(r => r.name);
  for (const col of EXPECTED_SHADOW_COLUMNS) {
    assert(shadowCols.includes(col), `shadow_decisions missing column: ${col}`);
  }
  console.log(`  ${EXPECTED_SHADOW_COLUMNS.length} columns checked (${shadowCols.length} total in table).`);

  // Check indexes
  console.log('\n=== Indexes ===');
  const indexes = querySql("SELECT name FROM sqlite_master WHERE type='index'").map(r => r.name);
  for (const idx of EXPECTED_INDEXES) {
    assert(indexes.includes(idx), `Missing index: ${idx}`);
  }
  console.log(`  ${EXPECTED_INDEXES.length} indexes checked.`);

  // Check other table columns
  console.log('\n=== Other table columns ===');
  for (const { table, column } of EXPECTED_OTHER_COLUMNS) {
    const cols = querySql(`PRAGMA table_info(${table})`).map(r => r.name);
    assert(cols.includes(column), `${table} missing column: ${column}`);
  }
  console.log(`  ${EXPECTED_OTHER_COLUMNS.length} columns checked.`);

  // Check a recent shadow row if any exist
  console.log('\n=== Recent shadow row spot-check ===');
  const recentRow = queryOneSql(
    'SELECT * FROM shadow_decisions ORDER BY request_received_at DESC LIMIT 1'
  );
  if (recentRow) {
    console.log(`  Found row: shadow_id=${recentRow.shadow_id}`);
    console.log(`  confidence_tier=${recentRow.confidence_tier}`);
    console.log(`  issuer_bank=${recentRow.issuer_bank}`);
    console.log(`  expected_approval=${recentRow.expected_approval}`);
    // Check P3 columns are present (may be null if no shadow data yet)
    assert('confidence_tier' in recentRow, 'Recent row missing confidence_tier');
    assert('expected_approval' in recentRow, 'Recent row missing expected_approval');
    assert('regret' in recentRow, 'Recent row missing regret');
  } else {
    console.log('  No shadow rows yet — schema check only (OK for pre-launch).');
  }

  // Summary
  console.log(`\n=== Result: ${passed} passed, ${failed} failed ===`);
  closeDb();
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => {
  console.error('Fatal:', err);
  try { closeDb(); } catch {}
  process.exit(1);
});
