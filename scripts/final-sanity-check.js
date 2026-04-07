#!/usr/bin/env node
/**
 * Final sanity check before ML training — verify data makes logical sense.
 */
const path = require('path');
const { initDb, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  let allGood = true;
  const fail = (msg) => { console.log('  FAIL: ' + msg); allGood = false; };
  const pass = (msg) => { console.log('  PASS: ' + msg); };

  console.log('='.repeat(70));
  console.log('FINAL SANITY CHECK — Is data ML-ready?');
  console.log('='.repeat(70));

  // 1. No NULL features on training rows
  console.log('\n--- 1. NULL feature check (training rows only, excludes "excluded") ---');
  const nulls = db.prepare(`
    SELECT
      SUM(CASE WHEN processor_name IS NULL THEN 1 ELSE 0 END) as null_proc,
      SUM(CASE WHEN outcome IS NULL THEN 1 ELSE 0 END) as null_outcome,
      SUM(CASE WHEN model_target IS NULL THEN 1 ELSE 0 END) as null_mt,
      SUM(CASE WHEN mid_velocity_daily IS NULL THEN 1 ELSE 0 END) as null_vel_d,
      SUM(CASE WHEN mid_velocity_weekly IS NULL THEN 1 ELSE 0 END) as null_vel_w,
      SUM(CASE WHEN customer_history_on_proc IS NULL THEN 1 ELSE 0 END) as null_cust_hist,
      SUM(CASE WHEN bin_velocity_weekly IS NULL THEN 1 ELSE 0 END) as null_bin_vel,
      SUM(CASE WHEN hour_of_day IS NULL THEN 1 ELSE 0 END) as null_hour,
      SUM(CASE WHEN feature_version < 3 THEN 1 ELSE 0 END) as not_v3,
      COUNT(*) as total
    FROM transaction_attempts
    WHERE model_target != 'excluded'
  `).get();

  for (const [key, val] of Object.entries(nulls)) {
    if (key === 'total') continue;
    if (val > 0) fail(`${key}: ${val} rows`);
    else pass(`${key}: 0`);
  }

  // 2. Excluded gateways should NOT be in INITIAL model (cascade/rebill/salvage can reference them)
  console.log('\n--- 2. Excluded gateways not in initial model ---');
  const excludedInInitial = db.prepare(`
    SELECT ta.client_id, ta.gateway_id, g.gateway_alias, COUNT(*) as cnt
    FROM transaction_attempts ta
    JOIN gateways g ON g.client_id = ta.client_id AND g.gateway_id = ta.gateway_id
    WHERE g.exclude_from_analysis = 1 AND ta.model_target = 'initial'
    GROUP BY ta.client_id, ta.gateway_id
    ORDER BY cnt DESC LIMIT 10
  `).all();
  if (excludedInInitial.length > 0) {
    fail(`${excludedInInitial.length} excluded gateways found in initial model`);
    for (const r of excludedInInitial) console.log(`    Client ${r.client_id} GW ${r.gateway_id} (${r.gateway_alias}): ${r.cnt}`);
  } else {
    pass('No excluded gateways in initial model');
  }

  // Info: excluded gateways in cascade/rebill (expected — real historical data)
  const excludedInOther = db.prepare(`
    SELECT ta.model_target, COUNT(*) as cnt
    FROM transaction_attempts ta
    JOIN gateways g ON g.client_id = ta.client_id AND g.gateway_id = ta.gateway_id
    WHERE g.exclude_from_analysis = 1 AND ta.model_target NOT IN ('initial', 'excluded')
    GROUP BY ta.model_target ORDER BY ta.model_target
  `).all();
  if (excludedInOther.length > 0) {
    for (const r of excludedInOther) pass(`Excluded GWs in ${r.model_target}: ${r.cnt} (expected — historical data)`);
  }

  // 3. model_target assignment correctness
  console.log('\n--- 3. model_target vs derived_product_role consistency ---');

  // Initials should be main_initial or upsell_initial at cascade_position=0
  const badInitial = db.prepare(`
    SELECT COUNT(*) as cnt FROM transaction_attempts
    WHERE model_target = 'initial'
      AND (derived_product_role NOT IN ('main_initial', 'upsell_initial') OR cascade_position != 0)
  `).get().cnt;
  if (badInitial > 0) fail(`${badInitial} initial rows with wrong role/position`);
  else pass('initial model_target: all correct');

  // Cascade should always have cascade_position > 0
  const badCascade = db.prepare(`
    SELECT COUNT(*) as cnt FROM transaction_attempts
    WHERE model_target = 'cascade' AND cascade_position = 0
  `).get().cnt;
  if (badCascade > 0) fail(`${badCascade} cascade rows with position=0`);
  else pass('cascade model_target: all position > 0');

  // Rebill should be main_rebill/upsell_rebill with attempt=1, position=0
  const badRebill = db.prepare(`
    SELECT COUNT(*) as cnt FROM transaction_attempts
    WHERE model_target = 'rebill'
      AND (derived_product_role NOT IN ('main_rebill', 'upsell_rebill') OR derived_attempt != 1 OR cascade_position != 0)
  `).get().cnt;
  if (badRebill > 0) fail(`${badRebill} rebill rows with wrong role/attempt/position`);
  else pass('rebill model_target: all correct');

  // Salvage should be main_rebill/upsell_rebill with attempt>=2, position=0
  const badSalvage = db.prepare(`
    SELECT COUNT(*) as cnt FROM transaction_attempts
    WHERE model_target = 'rebill_salvage'
      AND (derived_product_role NOT IN ('main_rebill', 'upsell_rebill') OR derived_attempt < 2 OR cascade_position != 0)
  `).get().cnt;
  if (badSalvage > 0) fail(`${badSalvage} salvage rows with wrong role/attempt/position`);
  else pass('rebill_salvage model_target: all correct');

  // 4. Cascade chain integrity
  console.log('\n--- 4. Cascade chain integrity ---');

  // Cascade rows should have initial_declined_processor
  const cascNoInit = db.prepare(`
    SELECT COUNT(*) as cnt FROM transaction_attempts
    WHERE model_target = 'cascade' AND initial_declined_processor IS NULL
  `).get().cnt;
  if (cascNoInit > 0) fail(`${cascNoInit} cascade rows missing initial_declined_processor`);
  else pass('All cascade rows have initial_declined_processor');

  // Cascade rows should have processors_tried_before
  const cascNoProcs = db.prepare(`
    SELECT COUNT(*) as cnt FROM transaction_attempts
    WHERE model_target = 'cascade' AND processors_tried_before IS NULL
  `).get().cnt;
  if (cascNoProcs > 0) fail(`${cascNoProcs} cascade rows missing processors_tried_before`);
  else pass('All cascade rows have processors_tried_before');

  // attempt_seq=1 should never be cascade
  const seq1Cascade = db.prepare(`
    SELECT COUNT(*) as cnt FROM transaction_attempts
    WHERE attempt_seq = 1 AND model_target = 'cascade'
  `).get().cnt;
  if (seq1Cascade > 0) fail(`${seq1Cascade} attempt_seq=1 rows have model_target=cascade`);
  else pass('No attempt_seq=1 with cascade model_target');

  // 5. Approval rate sanity
  console.log('\n--- 5. Approval rate sanity per model ---');
  const rates = db.prepare(`
    SELECT model_target,
      COUNT(*) as total,
      SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) as approved,
      ROUND(100.0 * SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) / COUNT(*), 1) as rate
    FROM transaction_attempts
    WHERE model_target != 'excluded'
    GROUP BY model_target ORDER BY model_target
  `).all();
  for (const r of rates) {
    const sane = (r.model_target === 'initial' && r.rate > 20 && r.rate < 60) ||
                 (r.model_target === 'cascade' && r.rate < 10) ||
                 (r.model_target === 'rebill' && r.rate > 10 && r.rate < 50) ||
                 (r.model_target === 'rebill_salvage' && r.rate < 15);
    if (sane) pass(`${r.model_target}: ${r.rate}% (${r.approved}/${r.total})`);
    else fail(`${r.model_target}: ${r.rate}% — unexpected range (${r.approved}/${r.total})`);
  }

  // 6. Relationship features on correct rows
  console.log('\n--- 6. Relationship features coverage ---');

  // Rebill rows should have decent last_approved_processor coverage
  const rebillLap = db.prepare(`
    SELECT
      SUM(CASE WHEN last_approved_processor IS NOT NULL THEN 1 ELSE 0 END) as has_lap,
      COUNT(*) as total
    FROM transaction_attempts WHERE model_target = 'rebill'
  `).get();
  const lapPct = (100 * rebillLap.has_lap / rebillLap.total).toFixed(1);
  if (parseFloat(lapPct) > 50) pass(`Rebill last_approved_processor: ${lapPct}% (${rebillLap.has_lap}/${rebillLap.total})`);
  else fail(`Rebill last_approved_processor: only ${lapPct}%`);

  // Salvage rows should have high parent_declined_processor coverage
  const salvPdp = db.prepare(`
    SELECT
      SUM(CASE WHEN parent_declined_processor IS NOT NULL THEN 1 ELSE 0 END) as has_pdp,
      COUNT(*) as total
    FROM transaction_attempts WHERE model_target = 'rebill_salvage'
  `).get();
  const pdpPct = (100 * salvPdp.has_pdp / salvPdp.total).toFixed(1);
  if (parseFloat(pdpPct) > 80) pass(`Salvage parent_declined_processor: ${pdpPct}% (${salvPdp.has_pdp}/${salvPdp.total})`);
  else fail(`Salvage parent_declined_processor: only ${pdpPct}%`);

  // Initial rows should NOT have last_approved_processor
  const initLap = db.prepare(`
    SELECT COUNT(*) as cnt FROM transaction_attempts
    WHERE model_target = 'initial' AND last_approved_processor IS NOT NULL
  `).get().cnt;
  if (initLap === 0) pass('Initial rows have no last_approved_processor (correct)');
  else fail(`${initLap} initial rows have last_approved_processor (should be NULL)`);

  // 7. No duplicate orders in same model
  console.log('\n--- 7. Duplicate check ---');
  const dupes = db.prepare(`
    SELECT COUNT(*) as cnt FROM (
      SELECT client_id, sticky_order_id, attempt_seq, COUNT(*) as c
      FROM transaction_attempts
      GROUP BY client_id, sticky_order_id, attempt_seq
      HAVING c > 1
    )
  `).get().cnt;
  if (dupes > 0) fail(`${dupes} duplicate (client, order, seq) combinations`);
  else pass('No duplicates');

  // 8. Anonymous orders check — should be initial or excluded, never rebill/salvage
  console.log('\n--- 8. Anonymous order model assignment ---');
  const anonRebill = db.prepare(`
    SELECT model_target, COUNT(*) as cnt FROM transaction_attempts
    WHERE (customer_id IS NULL OR customer_id = 0)
      AND model_target IN ('rebill', 'rebill_salvage')
    GROUP BY model_target
  `).all();
  if (anonRebill.length > 0) {
    for (const r of anonRebill) fail(`${r.cnt} anonymous rows with model_target=${r.model_target}`);
  } else {
    pass('No anonymous rows in rebill/salvage models');
  }

  // 9. Training data volume per model
  console.log('\n--- 9. Training data volume ---');
  const volumes = db.prepare(`
    SELECT model_target,
      COUNT(*) as total,
      SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) as pos,
      SUM(CASE WHEN outcome = 'declined' THEN 1 ELSE 0 END) as neg
    FROM transaction_attempts
    WHERE model_target != 'excluded'
    GROUP BY model_target ORDER BY model_target
  `).all();
  for (const v of volumes) {
    const ratio = v.neg > 0 ? (v.neg / v.pos).toFixed(1) : 'INF';
    const sufficient = v.total > 1000 && v.pos > 100;
    if (sufficient) pass(`${v.model_target}: ${v.total.toLocaleString()} rows (${v.pos} pos / ${v.neg} neg, ratio ${ratio}:1)`);
    else fail(`${v.model_target}: ${v.total.toLocaleString()} rows — insufficient volume`);
  }

  // 10. Cross-client consistency
  console.log('\n--- 10. Cross-client consistency ---');
  const perClient = db.prepare(`
    SELECT client_id, model_target,
      COUNT(*) as total,
      ROUND(100.0 * SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) / COUNT(*), 1) as rate
    FROM transaction_attempts
    WHERE model_target IN ('initial', 'rebill')
    GROUP BY client_id, model_target ORDER BY model_target, client_id
  `).all();
  console.log('  Per-client approval rates:');
  for (const r of perClient) {
    console.log(`    Client ${r.client_id} ${r.model_target}: ${r.rate}% (${r.total.toLocaleString()} rows)`);
  }

  // Summary
  console.log('\n' + '='.repeat(70));
  if (allGood) {
    console.log('ALL CHECKS PASSED — Data is ML-ready');
  } else {
    console.log('SOME CHECKS FAILED — Review issues above');
  }
  console.log('='.repeat(70));
}

main().catch(err => { console.error(err); process.exit(1); });
