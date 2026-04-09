const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  const q = (sql) => querySql(sql);

  // card_type_merged = PREPAID if is_prepaid=1, else card_type
  const CTM = "CASE WHEN is_prepaid=1 THEN 'PREPAID' ELSE card_type END";

  console.log('╔══════════════════════════════════════════════════════════════╗');
  console.log('║         LOOKUP TABLE SPARSITY ANALYSIS — ALL 4 MODELS       ║');
  console.log('╚══════════════════════════════════════════════════════════════╝\n');

  // ═══════════════════════════════════════════
  // TABLE 1: INITIAL (3D)
  // ═══════════════════════════════════════════
  console.log('═══ TABLE 1: INITIAL (3D) ═══');
  console.log('Dimensions: issuer_bank × card_type_merged × target_processor\n');

  const init3d = q(
    "SELECT issuer_bank, " + CTM + " as ctm, processor_name as target, " +
    "COUNT(*) as n, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as rate " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND source='order_direct' " +
    "AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL " +
    "GROUP BY issuer_bank, ctm, processor_name"
  );
  const init3d_35 = init3d.filter(r => r.n >= 35);
  const init3d_20 = init3d.filter(r => r.n >= 20);
  const init3d_total = init3d.reduce((s, r) => s + r.n, 0);
  const init3d_covered35 = init3d_35.reduce((s, r) => s + r.n, 0);
  const init3d_covered20 = init3d_20.reduce((s, r) => s + r.n, 0);
  const init3d_actionable = init3d_35.filter(r => r.rate < 5);
  console.log('  Total attempts (order_direct): ' + init3d_total);
  console.log('  Unique 3D combos: ' + init3d.length);
  console.log('  Combos with 35+ rows: ' + init3d_35.length + ' (' + (100*init3d_covered35/init3d_total).toFixed(1) + '% of attempts)');
  console.log('  Combos with 20+ rows: ' + init3d_20.length + ' (' + (100*init3d_covered20/init3d_total).toFixed(1) + '% of attempts)');
  console.log('  Actionable (35+ AND <5% approval): ' + init3d_actionable.length + ' hard excludes');
  console.log('  Actionable (20-34 AND <5% approval): ' + init3d_20.filter(r => r.n < 35 && r.rate < 5).length + ' soft down-ranks');

  // Fallback 2D: issuer × target
  const init2d = q(
    "SELECT issuer_bank, processor_name as target, " +
    "COUNT(*) as n, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as rate " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND source='order_direct' " +
    "AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL " +
    "GROUP BY issuer_bank, processor_name"
  );
  const init2d_35 = init2d.filter(r => r.n >= 35);
  const init2d_covered35 = init2d_35.reduce((s, r) => s + r.n, 0);
  console.log('  Fallback 2D (issuer × target): ' + init2d_35.length + ' combos with 35+ (' + (100*init2d_covered35/init3d_total).toFixed(1) + '% coverage)');

  // ═══════════════════════════════════════════
  // TABLE 2: CASCADE (4D)
  // ═══════════════════════════════════════════
  console.log('\n═══ TABLE 2: CASCADE (4D) ═══');
  console.log('Dimensions: issuer_bank × card_type_merged × initial_declined_processor × target_processor\n');

  const casc4d = q(
    "SELECT issuer_bank, " + CTM + " as ctm, initial_declined_processor as failed, processor_name as target, " +
    "COUNT(*) as n, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as rate " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND source='chain' " +
    "AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_declined_processor IS NOT NULL " +
    "GROUP BY issuer_bank, ctm, failed, processor_name"
  );
  const casc4d_35 = casc4d.filter(r => r.n >= 35);
  const casc4d_20 = casc4d.filter(r => r.n >= 20);
  const casc4d_total = casc4d.reduce((s, r) => s + r.n, 0);
  const casc4d_covered35 = casc4d_35.reduce((s, r) => s + r.n, 0);
  const casc4d_covered20 = casc4d_20.reduce((s, r) => s + r.n, 0);
  console.log('  Total attempts (chain): ' + casc4d_total);
  console.log('  Unique 4D combos: ' + casc4d.length);
  console.log('  Combos with 35+ rows: ' + casc4d_35.length + ' (' + (100*casc4d_covered35/casc4d_total).toFixed(1) + '% of attempts)');
  console.log('  Combos with 20+ rows: ' + casc4d_20.length + ' (' + (100*casc4d_covered20/casc4d_total).toFixed(1) + '% of attempts)');
  console.log('  Actionable (35+ AND <5% approval): ' + casc4d_35.filter(r => r.rate < 5).length + ' hard excludes');

  // Fallback 3D: issuer × failed_proc × target (no card_type)
  const casc3d = q(
    "SELECT issuer_bank, initial_declined_processor as failed, processor_name as target, " +
    "COUNT(*) as n, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as rate " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND source='chain' " +
    "AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_declined_processor IS NOT NULL " +
    "GROUP BY issuer_bank, failed, processor_name"
  );
  const casc3d_35 = casc3d.filter(r => r.n >= 35);
  const casc3d_covered35 = casc3d_35.reduce((s, r) => s + r.n, 0);
  console.log('  Fallback 3D (issuer × failed × target): ' + casc3d_35.length + ' combos with 35+ (' + (100*casc3d_covered35/casc4d_total).toFixed(1) + '% coverage)');

  // What if cascade is just 3D instead of 4D?
  console.log('\n  >> IF CASCADE WERE 3D INSTEAD OF 4D:');
  console.log('     3D combos with 35+: ' + casc3d_35.length + ' vs 4D: ' + casc4d_35.length);
  console.log('     3D coverage: ' + (100*casc3d_covered35/casc4d_total).toFixed(1) + '% vs 4D: ' + (100*casc4d_covered35/casc4d_total).toFixed(1) + '%');

  // ═══════════════════════════════════════════
  // TABLE 3: REBILL FIRST-ATTEMPT (4D)
  // ═══════════════════════════════════════════
  console.log('\n═══ TABLE 3: REBILL FIRST-ATTEMPT (4D) ═══');
  console.log('Dimensions: issuer_bank × card_type_merged × initial_processor × target_processor\n');

  // Rebill = order_direct with derived_cycle >= 1 — but source might not have cycle info
  // Let's use the rebill source directly
  const reb4d = q(
    "SELECT issuer_bank, " + CTM + " as ctm, initial_processor, processor_name as target, " +
    "COUNT(*) as n, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as rate " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND source='order_direct' " +
    "AND derived_cycle >= 1 AND derived_attempt = 1 " +
    "AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL " +
    "GROUP BY issuer_bank, ctm, initial_processor, processor_name"
  );
  const reb4d_35 = reb4d.filter(r => r.n >= 35);
  const reb4d_20 = reb4d.filter(r => r.n >= 20);
  const reb4d_total = reb4d.reduce((s, r) => s + r.n, 0);
  const reb4d_covered35 = reb4d_35.reduce((s, r) => s + r.n, 0);
  const reb4d_covered20 = reb4d_20.reduce((s, r) => s + r.n, 0);
  console.log('  Total rebill first-attempts: ' + reb4d_total);
  console.log('  Unique 4D combos: ' + reb4d.length);
  console.log('  Combos with 35+ rows: ' + reb4d_35.length + ' (' + (100*reb4d_covered35/reb4d_total).toFixed(1) + '% of attempts)');
  console.log('  Combos with 20+ rows: ' + reb4d_20.length + ' (' + (100*reb4d_covered20/reb4d_total).toFixed(1) + '% of attempts)');
  console.log('  Actionable (35+ AND <5% approval): ' + reb4d_35.filter(r => r.rate < 5).length + ' hard excludes');

  // C1-C2 only
  const reb4d_c12 = q(
    "SELECT issuer_bank, " + CTM + " as ctm, initial_processor, processor_name as target, " +
    "COUNT(*) as n, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as rate " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND source='order_direct' " +
    "AND derived_cycle IN (1,2) AND derived_attempt = 1 " +
    "AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL " +
    "GROUP BY issuer_bank, ctm, initial_processor, processor_name"
  );
  const reb4d_c12_35 = reb4d_c12.filter(r => r.n >= 35);
  const reb4d_c12_20 = reb4d_c12.filter(r => r.n >= 20);
  const reb4d_c12_total = reb4d_c12.reduce((s, r) => s + r.n, 0);
  console.log('  C1-C2 only: ' + reb4d_c12_total + ' attempts, ' + reb4d_c12_35.length + ' combos 35+, ' + reb4d_c12_20.length + ' combos 20+');

  // Fallback 3D
  const reb3d = q(
    "SELECT issuer_bank, initial_processor, processor_name as target, " +
    "COUNT(*) as n " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND source='order_direct' " +
    "AND derived_cycle >= 1 AND derived_attempt = 1 " +
    "AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL AND initial_processor IS NOT NULL " +
    "GROUP BY issuer_bank, initial_processor, processor_name"
  );
  const reb3d_35 = reb3d.filter(r => r.n >= 35);
  const reb3d_covered35 = reb3d_35.reduce((s, r) => s + r.n, 0);
  console.log('  Fallback 3D (issuer × initial_proc × target): ' + reb3d_35.length + ' combos with 35+ (' + (100*reb3d_covered35/reb4d_total).toFixed(1) + '% coverage)');

  // ═══════════════════════════════════════════
  // TABLE 4: REBILL SALVAGE (5D)
  // ═══════════════════════════════════════════
  console.log('\n═══ TABLE 4: REBILL SALVAGE (5D) ═══');
  console.log('Dimensions: decline_reason × issuer_bank × card_type_merged × failed_processor × target_processor\n');

  const salv5d = q(
    "SELECT decline_reason, issuer_bank, " + CTM + " as ctm, " +
    "parent_declined_processor as failed, processor_name as target, " +
    "COUNT(*) as n, " +
    "ROUND(100.0 * SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) / COUNT(*), 2) as rate " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND source='order_direct' " +
    "AND derived_cycle >= 1 AND derived_attempt > 1 " +
    "AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL " +
    "AND decline_reason IS NOT NULL AND parent_declined_processor IS NOT NULL " +
    "GROUP BY decline_reason, issuer_bank, ctm, failed, processor_name"
  );
  const salv5d_35 = salv5d.filter(r => r.n >= 35);
  const salv5d_20 = salv5d.filter(r => r.n >= 20);
  const salv5d_total = salv5d.reduce((s, r) => s + r.n, 0);
  const salv5d_covered35 = salv5d_35.reduce((s, r) => s + r.n, 0);
  const salv5d_covered20 = salv5d_20.reduce((s, r) => s + r.n, 0);
  console.log('  Total salvage attempts: ' + salv5d_total);
  console.log('  Unique 5D combos: ' + salv5d.length);
  console.log('  Combos with 35+ rows: ' + salv5d_35.length + ' (' + (salv5d_total > 0 ? (100*salv5d_covered35/salv5d_total).toFixed(1) : 0) + '% of attempts)');
  console.log('  Combos with 20+ rows: ' + salv5d_20.length + ' (' + (salv5d_total > 0 ? (100*salv5d_covered20/salv5d_total).toFixed(1) : 0) + '% of attempts)');

  // Fallback 4D (old structure: decline × issuer × failed × target — no card_type)
  const salv4d = q(
    "SELECT decline_reason, issuer_bank, " +
    "parent_declined_processor as failed, processor_name as target, " +
    "COUNT(*) as n " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND source='order_direct' " +
    "AND derived_cycle >= 1 AND derived_attempt > 1 " +
    "AND issuer_bank IS NOT NULL AND processor_name IS NOT NULL " +
    "AND decline_reason IS NOT NULL AND parent_declined_processor IS NOT NULL " +
    "GROUP BY decline_reason, issuer_bank, failed, processor_name"
  );
  const salv4d_35 = salv4d.filter(r => r.n >= 35);
  const salv4d_covered35 = salv4d_35.reduce((s, r) => s + r.n, 0);
  console.log('  Fallback 4D (decline × issuer × failed × target): ' + salv4d_35.length + ' combos with 35+ (' + (salv5d_total > 0 ? (100*salv4d_covered35/salv5d_total).toFixed(1) : 0) + '% coverage)');

  // ═══════════════════════════════════════════
  // SUMMARY
  // ═══════════════════════════════════════════
  console.log('\n═══ SUMMARY ═══');
  console.log('Table 1 (Initial 3D):    ' + init3d_35.length + ' combos 35+, ' + (100*init3d_covered35/init3d_total).toFixed(1) + '% coverage');
  console.log('Table 2 (Cascade 4D):    ' + casc4d_35.length + ' combos 35+, ' + (100*casc4d_covered35/casc4d_total).toFixed(1) + '% coverage');
  console.log('Table 3 (Rebill 4D):     ' + reb4d_35.length + ' combos 35+, ' + (100*reb4d_covered35/reb4d_total).toFixed(1) + '% coverage');
  console.log('Table 4 (Salvage 5D):    ' + salv5d_35.length + ' combos 35+, ' + (salv5d_total > 0 ? (100*salv5d_covered35/salv5d_total).toFixed(1) : 0) + '% coverage');

  process.exit(0);
})();
