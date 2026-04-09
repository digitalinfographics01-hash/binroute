/**
 * Human-readable lookup table report.
 * Shows real scenarios with plain English explanations.
 */
const le = require('../src/routing/lookup-engine');
const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();
  le.loadTables();

  const q = (sql) => querySql(sql);

  // Get real top issuers by volume
  const topIssuers = q(
    "SELECT issuer_bank, COUNT(*) as n FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND issuer_bank IS NOT NULL " +
    "GROUP BY issuer_bank ORDER BY n DESC LIMIT 8"
  ).map(r => r.issuer_bank);

  // Get real processors
  const topProcs = q(
    "SELECT processor_name, COUNT(*) as n FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' AND processor_name IS NOT NULL AND processor_name != 'UNKNOWN' " +
    "GROUP BY processor_name ORDER BY n DESC LIMIT 8"
  ).map(r => r.processor_name);

  console.log('Using top issuers: ' + topIssuers.join(', '));
  console.log('Using top processors: ' + topProcs.join(', '));

  // ═══════════════════════════════════════════
  // TABLE 1: MAIN INITIAL
  // ═══════════════════════════════════════════
  console.log('\n' + '═'.repeat(80));
  console.log('TABLE 1: MAIN INITIAL — "Where should we route this brand new customer?"');
  console.log('═'.repeat(80));
  console.log('Dimensions: issuer × card_type (CREDIT/DEBIT/PREPAID) × target processor\n');

  // Pick 3 issuers, show routing recommendation
  for (const issuer of topIssuers.slice(0, 4)) {
    console.log(`\n  ─── ${issuer} ───`);
    for (const cardInfo of [
      { label: 'CREDIT', ct: 'CREDIT', pp: 0 },
      { label: 'DEBIT', ct: 'DEBIT', pp: 0 },
      { label: 'PREPAID', ct: 'DEBIT', pp: 1 },
    ]) {
      const scored = topProcs.map(proc => {
        const r = le.queryInitial(issuer, cardInfo.ct, cardInfo.pp, proc);
        return { proc, rate: r?.approval_rate, tier: r?.tier, action: r?.action, n: r?.sample_size };
      }).filter(s => s.rate !== null).sort((a, b) => b.rate - a.rate);

      if (scored.length === 0) { console.log(`    ${cardInfo.label}: No lookup data`); continue; }

      const best = scored[0];
      const worst = scored[scored.length - 1];
      const excluded = scored.filter(s => s.action === 'hard_exclude');

      console.log(`    ${cardInfo.label}:`);
      console.log(`      Best:  ${best.proc} at ${(best.rate * 100).toFixed(1)}% approval (${best.n} samples, ${best.tier})`);
      console.log(`      Worst: ${worst.proc} at ${(worst.rate * 100).toFixed(1)}% approval (${worst.n} samples, ${worst.tier})`);
      console.log(`      Gap: ${((best.rate - worst.rate) * 100).toFixed(1)} percentage points`);
      if (excluded.length > 0) {
        console.log(`      BLOCKED: ${excluded.map(e => e.proc + ' (' + (e.rate*100).toFixed(1) + '%)').join(', ')}`);
      }

      // Verify against raw DB
      const dbBest = q(
        "SELECT COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved " +
        "FROM transaction_attempts " +
        "WHERE feature_version >= 3 AND model_target != 'excluded' " +
        "AND source='order_direct' AND derived_cycle=0 AND derived_product_role='main_initial' " +
        "AND issuer_bank = '" + issuer.replace(/'/g, "''") + "' " +
        "AND CASE WHEN is_prepaid=1 THEN 'PREPAID' ELSE card_type END = '" + cardInfo.label + "' " +
        "AND processor_name = '" + best.proc.replace(/'/g, "''") + "'"
      );
      if (dbBest[0].n > 0) {
        const dbRate = (100 * dbBest[0].approved / dbBest[0].n).toFixed(1);
        const match = dbRate === (best.rate * 100).toFixed(1) ? '✓' : '✗ MISMATCH';
        console.log(`      DB verify: ${dbBest[0].approved}/${dbBest[0].n} = ${dbRate}% ${match}`);
      }
    }
  }

  // ═══════════════════════════════════════════
  // TABLE 2: UPSELL
  // ═══════════════════════════════════════════
  console.log('\n' + '═'.repeat(80));
  console.log('TABLE 2: UPSELL — "Customer approved on proc X, where should the upsell go?"');
  console.log('═'.repeat(80));
  console.log('Dimensions: issuer × card_type × initial_processor × target processor\n');

  // Find real upsell combos from the data
  const upsellCombos = q(
    "SELECT issuer_bank, CASE WHEN is_prepaid=1 THEN 'PREPAID' ELSE card_type END as ctm, " +
    "initial_processor, processor_name as target, " +
    "COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved " +
    "FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' " +
    "AND source='order_direct' AND derived_cycle=0 AND derived_product_role='upsell_initial' " +
    "AND issuer_bank IS NOT NULL AND initial_processor IS NOT NULL " +
    "GROUP BY issuer_bank, ctm, initial_processor, target " +
    "HAVING n >= 20 ORDER BY n DESC LIMIT 15"
  );

  console.log('  Top upsell combos with 20+ attempts:\n');
  for (const combo of upsellCombos) {
    const rate = (100 * combo.approved / combo.n).toFixed(1);
    const lookup = le.queryUpsell(combo.issuer_bank, combo.ctm === 'PREPAID' ? 'DEBIT' : combo.ctm, combo.ctm === 'PREPAID' ? 1 : 0, combo.initial_processor, combo.target);
    const lookupRate = lookup ? (lookup.approval_rate * 100).toFixed(1) : 'N/A';
    const match = lookupRate === rate ? '✓' : (lookup ? '✗' : '—');

    console.log(`  ${combo.issuer_bank} | ${combo.ctm} | init=${combo.initial_processor} → ${combo.target}`);
    console.log(`    DB: ${combo.approved}/${combo.n} = ${rate}% | Lookup: ${lookupRate}% (${lookup?.tier || 'miss'}) ${match}`);
  }

  // ═══════════════════════════════════════════
  // TABLE 3: REBILL
  // ═══════════════════════════════════════════
  console.log('\n' + '═'.repeat(80));
  console.log('TABLE 3: REBILL — "Customer on cycle 1-2, initial was on proc X, where to rebill?"');
  console.log('═'.repeat(80));
  console.log('Dimensions: issuer × card_type × initial_processor × target (C1-C2 only)\n');

  for (const issuer of topIssuers.slice(0, 3)) {
    console.log(`  ─── ${issuer} ───`);
    // Find what initial processors customers of this issuer used
    const initProcs = q(
      "SELECT initial_processor, COUNT(*) as n FROM transaction_attempts " +
      "WHERE feature_version >= 3 AND model_target != 'excluded' " +
      "AND source='order_direct' AND derived_cycle IN (1,2) AND derived_attempt=1 " +
      "AND issuer_bank = '" + issuer.replace(/'/g, "''") + "' " +
      "AND initial_processor IS NOT NULL " +
      "GROUP BY initial_processor ORDER BY n DESC LIMIT 3"
    );

    for (const ip of initProcs) {
      console.log(`\n    Initial processor: ${ip.initial_processor} (${ip.n} rebill attempts)`);

      const scored = topProcs.map(proc => {
        const r = le.queryRebill(issuer, 'DEBIT', 0, ip.initial_processor, proc, 1);
        return { proc, rate: r?.approval_rate, tier: r?.tier, action: r?.action, n: r?.sample_size };
      }).filter(s => s.rate !== null).sort((a, b) => b.rate - a.rate);

      if (scored.length === 0) { console.log('      No lookup data'); continue; }

      const best = scored[0];
      const worst = scored[scored.length - 1];
      const excluded = scored.filter(s => s.action === 'hard_exclude');
      const downranked = scored.filter(s => s.action === 'soft_downrank');

      console.log(`      BEST:  ${best.proc} → ${(best.rate * 100).toFixed(1)}% (n=${best.n}, ${best.tier})`);
      console.log(`      WORST: ${worst.proc} → ${(worst.rate * 100).toFixed(1)}% (n=${worst.n}, ${worst.tier})`);
      if (excluded.length > 0) console.log(`      HARD EXCLUDE: ${excluded.map(e => e.proc + ' ' + (e.rate*100).toFixed(1) + '% (n=' + e.n + ')').join(', ')}`);
      if (downranked.length > 0) console.log(`      SOFT DOWN-RANK: ${downranked.map(e => e.proc + ' ' + (e.rate*100).toFixed(1) + '% (n=' + e.n + ')').join(', ')}`);

      // Plain English summary
      const spread = ((best.rate - worst.rate) * 100).toFixed(0);
      console.log(`      → Routing to ${best.proc} instead of ${worst.proc} gives +${spread}pp approval lift`);
    }
  }

  // ═══════════════════════════════════════════
  // TABLE 4: SALVAGE
  // ═══════════════════════════════════════════
  console.log('\n' + '═'.repeat(80));
  console.log('TABLE 4: SALVAGE — "Rebill declined for reason X, where to retry?"');
  console.log('═'.repeat(80));
  console.log('Dimensions: decline_reason × issuer × failed_processor → target\n');

  const topDeclines = q(
    "SELECT decline_reason, COUNT(*) as n FROM transaction_attempts " +
    "WHERE feature_version >= 3 AND model_target != 'excluded' " +
    "AND source='order_direct' AND derived_cycle >= 1 AND derived_attempt > 1 " +
    "AND decline_reason IS NOT NULL " +
    "GROUP BY decline_reason ORDER BY n DESC LIMIT 5"
  );

  for (const dec of topDeclines.slice(0, 3)) {
    console.log(`\n  ─── Decline: "${dec.decline_reason}" (${dec.n} salvage attempts) ───`);

    for (const issuer of topIssuers.slice(0, 2)) {
      for (const failedProc of topProcs.slice(0, 2)) {
        const targets = topProcs.filter(p => p !== failedProc).slice(0, 5);
        const result = le.filterSalvageCandidates(dec.decline_reason, issuer, 'DEBIT', 0, failedProc, targets);

        const scored = targets.map(t => {
          const r = le.querySalvage(dec.decline_reason, issuer, 'DEBIT', 0, failedProc, t);
          return { proc: t, rate: r?.approval_rate, tier: r?.tier, action: r?.action };
        }).filter(s => s.rate !== null);

        if (scored.length === 0) continue;

        const bestTarget = scored.sort((a, b) => b.rate - a.rate)[0];
        const blocked = scored.filter(s => s.action === 'hard_exclude');

        console.log(`\n    ${issuer} | failed on ${failedProc}:`);
        console.log(`      ${result.log.after_lookup}/${result.log.input_candidates} targets survive${result.log.safeguard_triggered ? ' [SAFEGUARD — all restored]' : ''}`);
        if (bestTarget) {
          console.log(`      Best retry: ${bestTarget.proc} at ${(bestTarget.rate * 100).toFixed(1)}% recovery (${bestTarget.tier})`);
        }
        if (blocked.length > 0) {
          console.log(`      Don't retry on: ${blocked.map(b => b.proc + ' ' + (b.rate*100).toFixed(1) + '%').join(', ')}`);
        }

        // Verify best target against DB
        if (bestTarget) {
          const dbCheck = q(
            "SELECT COUNT(*) as n, SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END) as approved " +
            "FROM transaction_attempts " +
            "WHERE feature_version >= 3 AND model_target != 'excluded' " +
            "AND source='order_direct' AND derived_cycle >= 1 AND derived_attempt > 1 " +
            "AND issuer_bank = '" + issuer.replace(/'/g, "''") + "' " +
            "AND parent_declined_processor = '" + failedProc.replace(/'/g, "''") + "' " +
            "AND processor_name = '" + bestTarget.proc.replace(/'/g, "''") + "'"
          );
          if (dbCheck[0].n > 0) {
            const dbRate = (100 * dbCheck[0].approved / dbCheck[0].n).toFixed(1);
            console.log(`      DB verify: ${dbCheck[0].approved}/${dbCheck[0].n} = ${dbRate}%`);
          }
        }
      }
    }
  }

  // ═══════════════════════════════════════════
  // OVERALL STATS
  // ═══════════════════════════════════════════
  console.log('\n' + '═'.repeat(80));
  console.log('OVERALL STATS');
  console.log('═'.repeat(80));

  const tables = le.loadTables();
  for (const [name, data] of Object.entries(tables)) {
    if (!data) continue;
    const tiers = Object.keys(data).filter(k => k.startsWith('tier_'));
    let totalEntries = 0, totalHard = 0, totalSoft = 0, totalAllow = 0;
    for (const t of tiers) {
      const entries = Object.values(data[t]);
      totalEntries += entries.length;
      totalHard += entries.filter(e => e.action === 'hard_exclude').length;
      totalSoft += entries.filter(e => e.action === 'soft_downrank').length;
      totalAllow += entries.filter(e => e.action === 'allow').length;
    }
    console.log(`\n  ${name}: ${totalEntries} total entries across ${tiers.length} tiers`);
    console.log(`    Hard exclude: ${totalHard} (${(100*totalHard/totalEntries).toFixed(0)}%)`);
    console.log(`    Soft down-rank: ${totalSoft} (${(100*totalSoft/totalEntries).toFixed(0)}%)`);
    console.log(`    Allow: ${totalAllow} (${(100*totalAllow/totalEntries).toFixed(0)}%)`);
  }

  process.exit(0);
})();
