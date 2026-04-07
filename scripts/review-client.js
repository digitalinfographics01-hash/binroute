#!/usr/bin/env node
/**
 * Review transaction_attempts data quality for a client.
 * Usage: node scripts/review-client.js --client=1
 */
const path = require('path');
const { initDb, getDb } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();
  const db = getDb();

  const args = process.argv.slice(2);
  let clientId = 1;
  for (const arg of args) {
    if (arg.startsWith('--client=')) clientId = parseInt(arg.split('=')[1]);
  }

  const C = clientId;
  const hr = '='.repeat(60);

  // 1. CASCADE BREAKDOWN
  console.log(hr);
  console.log('CASCADE BREAKDOWN');
  console.log(hr);
  const cs = db.prepare(`
    SELECT
      SUM(CASE WHEN is_cascaded = 0 THEN 1 ELSE 0 END) as non_cascaded,
      SUM(CASE WHEN is_cascaded = 1 AND cascade_chain IS NOT NULL AND cascade_chain != '[]' THEN 1 ELSE 0 END) as with_chain,
      SUM(CASE WHEN is_cascaded = 1 AND (cascade_chain IS NULL OR cascade_chain = '[]') THEN 1 ELSE 0 END) as no_chain
    FROM orders
    WHERE client_id = ? AND order_status IN (2,6,7,8) AND is_test = 0 AND is_internal_test = 0
      AND product_type_classified IS NOT NULL AND product_type_classified != 'straight_sale'
  `).get(C);
  console.log('Non-cascaded orders:', cs.non_cascaded);
  console.log('Cascaded WITH chain:', cs.with_chain);
  console.log('Cascaded NO chain:', cs.no_chain);
  console.log('Total:', cs.non_cascaded + cs.with_chain + cs.no_chain);

  // 2. SOURCE DISTRIBUTION
  console.log('\n' + hr);
  console.log('SOURCE DISTRIBUTION');
  console.log(hr);
  const sources = db.prepare('SELECT source, COUNT(*) as cnt FROM transaction_attempts WHERE client_id = ? GROUP BY source ORDER BY cnt DESC').all(C);
  for (const s of sources) console.log(`  ${s.source}: ${s.cnt.toLocaleString()}`);

  // 3. ATTEMPT SEQUENCE
  console.log('\n' + hr);
  console.log('ATTEMPT SEQUENCE');
  console.log(hr);
  const seqs = db.prepare(`
    SELECT attempt_seq, COUNT(*) as cnt,
      SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) as approved
    FROM transaction_attempts WHERE client_id = ? GROUP BY attempt_seq ORDER BY attempt_seq
  `).all(C);
  for (const s of seqs) {
    const rate = s.cnt > 0 ? ((s.approved / s.cnt) * 100).toFixed(1) : 0;
    console.log(`  seq ${s.attempt_seq}: ${s.cnt.toLocaleString()} (${s.approved} approved, ${rate}%)`);
  }

  // 4. MODEL TARGET
  console.log('\n' + hr);
  console.log('MODEL TARGET BREAKDOWN');
  console.log(hr);
  const models = db.prepare(`
    SELECT model_target, COUNT(*) as cnt,
      SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) as approved,
      SUM(CASE WHEN outcome = 'declined' THEN 1 ELSE 0 END) as declined
    FROM transaction_attempts WHERE client_id = ? GROUP BY model_target ORDER BY model_target
  `).all(C);
  for (const m of models) {
    const rate = m.cnt > 0 ? ((m.approved / m.cnt) * 100).toFixed(1) : 0;
    console.log(`  ${m.model_target}: ${m.cnt.toLocaleString()} total (${m.approved} app / ${m.declined} dec = ${rate}%)`);
  }

  // 5. RELATIONSHIP FEATURES
  console.log('\n' + hr);
  console.log('RELATIONSHIP FEATURES COVERAGE');
  console.log(hr);

  const rebRel = db.prepare(`SELECT
    SUM(CASE WHEN initial_processor IS NOT NULL THEN 1 ELSE 0 END) as has_init,
    SUM(CASE WHEN last_approved_processor IS NOT NULL THEN 1 ELSE 0 END) as has_last,
    COUNT(*) as total
    FROM transaction_attempts WHERE client_id = ? AND model_target = 'rebill'`).get(C);
  console.log(`  Rebill: initial_processor ${rebRel.has_init}/${rebRel.total}, last_approved ${rebRel.has_last}/${rebRel.total}`);

  const salvRel = db.prepare(`SELECT
    SUM(CASE WHEN initial_processor IS NOT NULL THEN 1 ELSE 0 END) as has_init,
    SUM(CASE WHEN parent_declined_processor IS NOT NULL THEN 1 ELSE 0 END) as has_parent,
    SUM(CASE WHEN prev_decline_reason IS NOT NULL THEN 1 ELSE 0 END) as has_prev,
    COUNT(*) as total
    FROM transaction_attempts WHERE client_id = ? AND model_target = 'rebill_salvage'`).get(C);
  console.log(`  Salvage: initial_processor ${salvRel.has_init}/${salvRel.total}, parent_declined ${salvRel.has_parent}/${salvRel.total}, prev_decline ${salvRel.has_prev}/${salvRel.total}`);

  // 6. CASCADE CONTEXT
  console.log('\n' + hr);
  console.log('CASCADE CONTEXT FEATURES');
  console.log(hr);
  const casc = db.prepare(`SELECT
    SUM(CASE WHEN initial_declined_processor IS NOT NULL THEN 1 ELSE 0 END) as has_init_dec,
    SUM(CASE WHEN cascade_final_outcome IS NOT NULL THEN 1 ELSE 0 END) as has_final,
    SUM(CASE WHEN cascade_approved_processor IS NOT NULL THEN 1 ELSE 0 END) as has_appr_proc,
    SUM(CASE WHEN processors_tried_before IS NOT NULL THEN 1 ELSE 0 END) as has_procs_tried,
    SUM(had_nsf) as nsf, SUM(had_do_not_honor) as dnh, SUM(had_pickup) as pickup,
    COUNT(*) as total
    FROM transaction_attempts WHERE client_id = ? AND model_target = 'cascade'`).get(C);
  console.log(`  Cascade rows: ${casc.total}`);
  console.log(`  initial_declined_processor: ${casc.has_init_dec}/${casc.total}`);
  console.log(`  cascade_final_outcome: ${casc.has_final}/${casc.total}`);
  console.log(`  cascade_approved_processor: ${casc.has_appr_proc}/${casc.total}`);
  console.log(`  processors_tried_before: ${casc.has_procs_tried}/${casc.total}`);
  console.log(`  Flags — NSF: ${casc.nsf}, Do Not Honor: ${casc.dnh}, Pick Up: ${casc.pickup}`);

  // 7. VELOCITY
  console.log('\n' + hr);
  console.log('VELOCITY FEATURES');
  console.log(hr);
  const vel = db.prepare(`SELECT
    MIN(mid_velocity_daily) as min_d, ROUND(AVG(mid_velocity_daily),1) as avg_d, MAX(mid_velocity_daily) as max_d,
    MIN(mid_velocity_weekly) as min_w, ROUND(AVG(mid_velocity_weekly),1) as avg_w, MAX(mid_velocity_weekly) as max_w,
    MIN(customer_history_on_proc) as min_c, ROUND(AVG(customer_history_on_proc),1) as avg_c, MAX(customer_history_on_proc) as max_c,
    MIN(bin_velocity_weekly) as min_b, ROUND(AVG(bin_velocity_weekly),1) as avg_b, MAX(bin_velocity_weekly) as max_b
    FROM transaction_attempts WHERE client_id = ?`).get(C);
  console.log(`  mid_velocity_daily:  min=${vel.min_d}  avg=${vel.avg_d}  max=${vel.max_d}`);
  console.log(`  mid_velocity_weekly: min=${vel.min_w}  avg=${vel.avg_w}  max=${vel.max_w}`);
  console.log(`  customer_history:    min=${vel.min_c}  avg=${vel.avg_c}  max=${vel.max_c}`);
  console.log(`  bin_velocity_weekly: min=${vel.min_b}  avg=${vel.avg_b}  max=${vel.max_b}`);

  // 8. SUBSCRIPTION
  console.log('\n' + hr);
  console.log('SUBSCRIPTION FEATURES (rebill + salvage)');
  console.log(hr);
  const sub = db.prepare(`SELECT
    ROUND(AVG(consecutive_approvals),1) as avg_ca, MAX(consecutive_approvals) as max_ca,
    ROUND(AVG(days_since_last_charge),1) as avg_dslc, ROUND(MAX(days_since_last_charge),1) as max_dslc,
    ROUND(AVG(days_since_initial),1) as avg_dsi, ROUND(MAX(days_since_initial),1) as max_dsi,
    ROUND(AVG(lifetime_charges),1) as avg_lc, MAX(lifetime_charges) as max_lc,
    ROUND(AVG(lifetime_revenue),2) as avg_lr, ROUND(MAX(lifetime_revenue),2) as max_lr,
    ROUND(AVG(initial_amount),2) as avg_ia,
    ROUND(AVG(amount_ratio),2) as avg_ar,
    ROUND(AVG(prior_declines_in_cycle),1) as avg_pd, MAX(prior_declines_in_cycle) as max_pd,
    COUNT(*) as total
    FROM transaction_attempts WHERE client_id = ? AND model_target IN ('rebill', 'rebill_salvage')`).get(C);
  console.log(`  Rows: ${sub.total}`);
  console.log(`  consecutive_approvals:   avg=${sub.avg_ca}  max=${sub.max_ca}`);
  console.log(`  days_since_last_charge:  avg=${sub.avg_dslc}  max=${sub.max_dslc}`);
  console.log(`  days_since_initial:      avg=${sub.avg_dsi}  max=${sub.max_dsi}`);
  console.log(`  lifetime_charges:        avg=${sub.avg_lc}  max=${sub.max_lc}`);
  console.log(`  lifetime_revenue:        avg=$${sub.avg_lr}  max=$${sub.max_lr}`);
  console.log(`  initial_amount:          avg=$${sub.avg_ia}`);
  console.log(`  amount_ratio:            avg=${sub.avg_ar}`);
  console.log(`  prior_declines_in_cycle: avg=${sub.avg_pd}  max=${sub.max_pd}`);

  // 9. SAMPLE ROWS
  console.log('\n' + hr);
  console.log('SAMPLE CASCADE ROWS');
  console.log(hr);
  const cascSamples = db.prepare(`
    SELECT sticky_order_id, attempt_seq, processor_name, outcome, decline_reason,
      cascade_position, initial_declined_processor, cascade_final_outcome, cascade_approved_processor,
      had_nsf, had_do_not_honor, mid_age_days
    FROM transaction_attempts WHERE client_id = ? AND source = 'chain' AND cascade_position > 0 LIMIT 5
  `).all(C);
  for (const s of cascSamples) console.log(JSON.stringify(s));

  console.log('\nSAMPLE REBILL ROWS');
  const rebSamples = db.prepare(`
    SELECT sticky_order_id, processor_name, outcome, model_target,
      initial_processor, last_approved_processor, consecutive_approvals,
      days_since_last_charge, lifetime_charges, mid_velocity_weekly
    FROM transaction_attempts WHERE client_id = ? AND model_target = 'rebill' AND last_approved_processor IS NOT NULL LIMIT 5
  `).all(C);
  for (const s of rebSamples) console.log(JSON.stringify(s));

  // 10. NULL CHECK
  console.log('\n' + hr);
  console.log('NULL CHECK (should all be 0)');
  console.log(hr);
  const nulls = db.prepare(`SELECT
    SUM(CASE WHEN feature_version < 3 THEN 1 ELSE 0 END) as not_v3,
    SUM(CASE WHEN mid_velocity_daily IS NULL THEN 1 ELSE 0 END) as null_vel_d,
    SUM(CASE WHEN mid_velocity_weekly IS NULL THEN 1 ELSE 0 END) as null_vel_w,
    SUM(CASE WHEN customer_history_on_proc IS NULL THEN 1 ELSE 0 END) as null_cust,
    SUM(CASE WHEN bin_velocity_weekly IS NULL THEN 1 ELSE 0 END) as null_bin,
    SUM(CASE WHEN processor_name IS NULL THEN 1 ELSE 0 END) as null_proc,
    SUM(CASE WHEN outcome IS NULL THEN 1 ELSE 0 END) as null_outcome,
    SUM(CASE WHEN model_target IS NULL THEN 1 ELSE 0 END) as null_mt
    FROM transaction_attempts WHERE client_id = ?`).get(C);
  console.log(`  not_v3: ${nulls.not_v3}`);
  console.log(`  null velocity_daily: ${nulls.null_vel_d}`);
  console.log(`  null velocity_weekly: ${nulls.null_vel_w}`);
  console.log(`  null customer_history: ${nulls.null_cust}`);
  console.log(`  null bin_velocity: ${nulls.null_bin}`);
  console.log(`  null processor_name: ${nulls.null_proc}`);
  console.log(`  null outcome: ${nulls.null_outcome}`);
  console.log(`  null model_target: ${nulls.null_mt}`);
}

main().catch(err => { console.error(err); process.exit(1); });
