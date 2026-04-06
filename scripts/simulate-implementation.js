#!/usr/bin/env node
/**
 * Implementation Tracker Simulator
 *
 * Simulates the full implementation lifecycle without waiting for real data.
 * Creates mock implementations and advances them through checkpoints.
 *
 * Usage:
 *   node scripts/simulate-implementation.js create   — Create test implementations
 *   node scripts/simulate-implementation.js evaluate  — Run one evaluation cycle
 *   node scripts/simulate-implementation.js advance   — Fast-forward: create + multiple eval cycles
 *   node scripts/simulate-implementation.js status    — Show current implementation statuses
 *   node scripts/simulate-implementation.js clean     — Remove all test implementations
 */

const { initDb, querySql, runSql, saveDb, closeDb, execSql } = require('../src/db/connection');
const {
  markPlaybookImplemented,
  evaluatePlaybookImplementations,
  getImplementationDashboard,
} = require('../src/engine/playbook-implementation');

const cmd = process.argv[2] || 'status';

async function main() {
  await initDb();

  // Run schema to ensure tables exist
  const { initializeDatabase } = require('../src/db/schema');
  await initializeDatabase();

  switch (cmd) {
    case 'create':  await createTestImplementations(); break;
    case 'evaluate': await runEvaluation(); break;
    case 'advance':  await advanceAll(); break;
    case 'status':   await showStatus(); break;
    case 'clean':    await cleanUp(); break;
    default:
      console.log('Usage: node scripts/simulate-implementation.js [create|evaluate|advance|status|clean]');
  }

  closeDb();
}

async function createTestImplementations() {
  // Get first client
  const clients = querySql('SELECT id, name FROM clients LIMIT 1');
  if (clients.length === 0) {
    console.log('No clients found. Add a client first.');
    return;
  }
  const clientId = clients[0].id;
  console.log(`Using client: ${clients[0].name} (ID: ${clientId})`);

  // Find banks with data
  const banks = querySql(`
    SELECT b.issuer_bank, b.is_prepaid, COUNT(*) as cnt
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role = 'main_initial'
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.acquisition_date >= date('now', '-60 days')
    GROUP BY b.issuer_bank, b.is_prepaid
    HAVING cnt >= 30
    ORDER BY cnt DESC
    LIMIT 3
  `, [clientId]);

  if (banks.length === 0) {
    console.log('No banks with sufficient data found.');
    return;
  }

  // Find top processors for each bank
  for (const bank of banks) {
    const procs = querySql(`
      SELECT g.processor_name, COUNT(*) as cnt
      FROM orders o
      JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
      JOIN bin_lookup b ON o.cc_first_6 = b.bin
      WHERE o.client_id = ? AND o.derived_product_role = 'main_initial'
        AND b.issuer_bank = ? AND b.is_prepaid = ?
        AND o.is_test = 0 AND o.is_internal_test = 0
        AND o.acquisition_date >= date('now', '-60 days')
      GROUP BY g.processor_name
      ORDER BY cnt DESC
      LIMIT 2
    `, [clientId, bank.issuer_bank, bank.is_prepaid]);

    if (procs.length < 1) continue;

    // Get gateway for the top processor
    const gw = querySql(`
      SELECT gateway_id FROM gateways
      WHERE client_id = ? AND processor_name = ? AND lifecycle_state != 'closed'
      LIMIT 1
    `, [clientId, procs[0].processor_name])[0];

    if (!gw) continue;

    console.log(`\nCreating initial_routing implementation:`);
    console.log(`  Bank: ${bank.issuer_bank} (${bank.is_prepaid ? 'Prepaid' : 'Non-Prepaid'})`);
    console.log(`  Processor: ${procs[0].processor_name} (${procs[0].cnt} attempts)`);
    console.log(`  Gateway: ${gw.gateway_id}`);

    try {
      const result = markPlaybookImplemented(clientId, {
        issuer_bank: bank.issuer_bank,
        is_prepaid: bank.is_prepaid,
        rule_type: 'initial_routing',
        recommended_processor: procs[0].processor_name,
        actual_processor: procs[0].processor_name,
        actual_gateway_ids: JSON.stringify([gw.gateway_id]),
      });

      console.log(`  Created implementation #${result.id}`);
      console.log(`  Baseline: ${result.baseline.approval_rate}% (${result.baseline.attempts} att)`);
      if (result.superseded.length > 0) {
        console.log(`  Superseded: ${result.superseded.join(', ')}`);
      }
    } catch (err) {
      console.error(`  Error: ${err.message}`);
    }
  }

  // Also create a cascade implementation for the first bank
  if (banks.length > 0) {
    const bank = banks[0];
    const cascProc = querySql(`
      SELECT g.processor_name, g.gateway_id
      FROM orders o
      JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
      JOIN bin_lookup b ON o.cc_first_6 = b.bin
      WHERE o.client_id = ? AND o.is_cascaded = 1
        AND b.issuer_bank = ? AND b.is_prepaid = ?
        AND o.acquisition_date >= date('now', '-60 days')
      GROUP BY g.processor_name
      ORDER BY COUNT(*) DESC
      LIMIT 1
    `, [clientId, bank.issuer_bank, bank.is_prepaid])[0];

    if (cascProc) {
      console.log(`\nCreating cascade implementation:`);
      console.log(`  Bank: ${bank.issuer_bank}`);
      console.log(`  Cascade target: ${cascProc.processor_name}`);

      try {
        const result = markPlaybookImplemented(clientId, {
          issuer_bank: bank.issuer_bank,
          is_prepaid: bank.is_prepaid,
          rule_type: 'cascade',
          recommended_processor: cascProc.processor_name,
          actual_processor: cascProc.processor_name,
          actual_gateway_ids: JSON.stringify([cascProc.gateway_id]),
        });
        console.log(`  Created implementation #${result.id}`);
        console.log(`  Baseline recovery: ${result.baseline.approval_rate}% (${result.baseline.attempts} cascade att)`);
      } catch (err) {
        console.error(`  Error: ${err.message}`);
      }
    }
  }

  // Create a rebill implementation for the first bank
  if (banks.length > 0) {
    const bank = banks[0];
    const rebProc = querySql(`
      SELECT g.processor_name, g.gateway_id, COUNT(*) as cnt
      FROM orders o
      JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
      JOIN bin_lookup b ON o.cc_first_6 = b.bin
      WHERE o.client_id = ? AND o.derived_product_role IN ('main_rebill','upsell_rebill')
        AND o.derived_cycle = 1 AND o.derived_attempt = 1
        AND b.issuer_bank = ? AND b.is_prepaid = ?
        AND o.acquisition_date >= date('now', '-60 days')
      GROUP BY g.processor_name
      ORDER BY cnt DESC
      LIMIT 1
    `, [clientId, bank.issuer_bank, bank.is_prepaid])[0];

    if (rebProc) {
      console.log(`\nCreating rebill_routing implementation:`);
      console.log(`  Bank: ${bank.issuer_bank}`);
      console.log(`  Processor: ${rebProc.processor_name} (${rebProc.cnt} C1 rebills)`);

      try {
        const result = markPlaybookImplemented(clientId, {
          issuer_bank: bank.issuer_bank,
          is_prepaid: bank.is_prepaid,
          rule_type: 'rebill_routing',
          recommended_processor: rebProc.processor_name,
          actual_processor: rebProc.processor_name,
          actual_gateway_ids: JSON.stringify([rebProc.gateway_id]),
        });
        console.log(`  Created implementation #${result.id}`);
        console.log(`  Baseline C1: ${result.baseline.approval_rate}% (${result.baseline.attempts} att)`);
      } catch (err) {
        console.error(`  Error: ${err.message}`);
      }
    }
  }

  console.log('\nDone. Run "node scripts/simulate-implementation.js status" to see results.');
}

async function runEvaluation() {
  console.log('Running evaluation cycle...');

  // To simulate passage of time, temporarily backdate implementations
  // that are in 'waiting' status so they transition
  const waiting = querySql("SELECT id FROM playbook_implementations WHERE status = 'waiting'");
  if (waiting.length > 0) {
    console.log(`Backdating ${waiting.length} waiting implementations to trigger collecting...`);
    for (const w of waiting) {
      runSql(`
        UPDATE playbook_implementations
        SET implemented_at = datetime('now', '-2 days'),
            collecting_start_date = date('now', '-2 days')
        WHERE id = ?
      `, [w.id]);
    }
    saveDb();
  }

  const result = evaluatePlaybookImplementations();
  console.log(`Evaluation complete: ${result.evaluated} checkpoints recorded, ${result.transitioned} status transitions`);
}

async function advanceAll() {
  console.log('=== Fast-forward: creating implementations and running evaluation cycles ===\n');

  // Step 1: Create if none exist
  const existing = querySql("SELECT COUNT(*) as cnt FROM playbook_implementations WHERE status NOT IN ('superseded','archived')");
  if (existing[0].cnt === 0) {
    console.log('No active implementations. Creating test ones...\n');
    await createTestImplementations();
  }

  // Step 2: Backdate all active implementations to simulate 14 days passing
  console.log('\nBackdating all active implementations by 14 days...');
  runSql(`
    UPDATE playbook_implementations
    SET implemented_at = datetime('now', '-14 days'),
        collecting_start_date = date('now', '-14 days')
    WHERE status IN ('waiting', 'collecting', 'evaluating')
  `);
  saveDb();

  // Step 3: Run 3 evaluation cycles
  for (let i = 1; i <= 3; i++) {
    console.log(`\nEvaluation cycle ${i}/3...`);
    const result = evaluatePlaybookImplementations();
    console.log(`  ${result.evaluated} checkpoints, ${result.transitioned} transitions`);
  }

  console.log('\n=== Done ===');
  await showStatus();
}

async function showStatus() {
  const clients = querySql('SELECT id, name FROM clients');
  if (clients.length === 0) {
    console.log('No clients.');
    return;
  }

  for (const client of clients) {
    const data = getImplementationDashboard(client.id);
    if (data.implementations.length === 0) continue;

    console.log(`\n${'='.repeat(60)}`);
    console.log(`Client: ${client.name} (ID: ${client.id})`);
    console.log(`${'='.repeat(60)}`);
    console.log(`Summary: ${data.summary.confirmed} confirmed, ${data.summary.collecting + data.summary.evaluating + data.summary.waiting} tracking, ${data.summary.regression} regressed`);
    console.log(`Total confirmed lift: +${data.summary.total_confirmed_lift_pp}pp`);
    console.log(`Est revenue impact: $${data.summary.est_monthly_revenue_impact}/mo`);

    console.log(`\nImplementations:`);
    console.log(`${'─'.repeat(60)}`);

    for (const impl of data.implementations) {
      const status = impl.status.toUpperCase().padEnd(12);
      const bank = `${impl.issuer_bank}${impl.is_prepaid ? ' (PP)' : ''}`.padEnd(30);
      const rule = impl.rule_type.padEnd(16);
      const baseline = `baseline:${impl.baseline_rate}%`.padEnd(16);
      const current = impl.current_rate != null ? `current:${impl.current_rate}%` : 'current:—';
      const lift = impl.lift_pp != null ? ` lift:${impl.lift_pp >= 0 ? '+' : ''}${impl.lift_pp.toFixed(1)}pp` : '';
      const sample = `(${impl.sample_progress})`;

      console.log(`  [${status}] ${bank} ${rule} ${baseline} ${current}${lift} ${sample}`);

      if (impl.verdict_reason) {
        console.log(`             Verdict: ${impl.verdict_reason}`);
      }
    }

    // Show checkpoints for most recent impl
    if (data.implementations.length > 0) {
      const firstImpl = data.implementations[0];
      const checkpoints = querySql(`
        SELECT checkpoint_day, post_attempts, post_approval_rate, lift_pp, meets_minimum_sample, status_at_checkpoint
        FROM implementation_checkpoints
        WHERE implementation_id = ?
        ORDER BY checkpoint_day
      `, [firstImpl.id]);

      if (checkpoints.length > 0) {
        console.log(`\n  Checkpoints for #${firstImpl.id} (${firstImpl.issuer_bank} / ${firstImpl.rule_type}):`);
        for (const cp of checkpoints) {
          const sample = cp.meets_minimum_sample ? 'OK' : '..';
          console.log(`    Day ${String(cp.checkpoint_day).padStart(3)}: ${String(cp.post_attempts).padStart(5)} att | ${String(cp.post_approval_rate || '—').padStart(6)}% | lift ${(cp.lift_pp >= 0 ? '+' : '') + cp.lift_pp.toFixed(1)}pp | sample: ${sample} | ${cp.status_at_checkpoint}`);
        }
      }
    }
  }
}

async function cleanUp() {
  console.log('Cleaning up all test implementations...');

  const counts = querySql('SELECT COUNT(*) as cnt FROM playbook_implementations')[0];
  const cpCounts = querySql('SELECT COUNT(*) as cnt FROM implementation_checkpoints')[0];
  const fbCounts = querySql('SELECT COUNT(*) as cnt FROM implementation_network_feedback')[0];

  runSql('DELETE FROM implementation_network_feedback');
  runSql('DELETE FROM implementation_checkpoints');
  runSql('DELETE FROM playbook_implementations');
  saveDb();

  console.log(`Deleted: ${counts.cnt} implementations, ${cpCounts.cnt} checkpoints, ${fbCounts.cnt} feedback records`);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
