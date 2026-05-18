#!/usr/bin/env node
/**
 * Manage experiment lifecycle: activate, pause, complete, list.
 *
 * Usage:
 *   node scripts/manage-experiment.js list
 *   node scripts/manage-experiment.js activate <experiment_id>
 *   node scripts/manage-experiment.js pause <experiment_id>
 *   node scripts/manage-experiment.js complete <experiment_id>
 */
const path = require('path');
const { initDb, runSql, querySql, queryOneSql } = require(path.join(__dirname, '..', 'src', 'db', 'connection'));
const { initializeDatabase } = require(path.join(__dirname, '..', 'src', 'db', 'schema'));

async function main() {
  await initDb();
  await initializeDatabase();

  const [action, idStr] = process.argv.slice(2);

  if (!action || (action !== 'list' && !idStr)) {
    console.error('Usage: node scripts/manage-experiment.js <list|activate|pause|complete> [experiment_id]');
    process.exit(1);
  }

  if (action === 'list') {
    const rows = querySql('SELECT * FROM experiments ORDER BY id DESC');
    if (rows.length === 0) {
      console.log('No experiments found.');
    } else {
      for (const r of rows) {
        console.log(`  [${r.id}] ${r.name} — status=${r.status}, client=${r.client_id}, traffic=${r.traffic_pct}%, treatment=${r.treatment_pct}%`);
      }
    }
    return;
  }

  const id = parseInt(idStr, 10);
  const exp = queryOneSql('SELECT * FROM experiments WHERE id = ?', [id]);
  if (!exp) {
    console.error(`Experiment ${id} not found.`);
    process.exit(1);
  }

  switch (action) {
    case 'activate': {
      // Pause any other active experiment for this client
      const paused = runSql(
        `UPDATE experiments SET status = 'paused', paused_at = CURRENT_TIMESTAMP
         WHERE client_id = ? AND status = 'active'`,
        [exp.client_id]
      );
      if (paused.changes > 0) {
        console.log(`  Paused ${paused.changes} other active experiment(s) for client ${exp.client_id}`);
      }
      runSql(
        `UPDATE experiments SET status = 'active', started_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [id]
      );
      console.log(`Experiment ${id} activated.`);
      console.log(`  Name: ${exp.name}`);
      console.log(`  Traffic: ${exp.traffic_pct}%, Treatment: ${exp.treatment_pct}%`);
      console.log(`  Expected split: ${(100 - exp.traffic_pct).toFixed(1)}% none, ${(exp.traffic_pct * (1 - exp.treatment_pct / 100)).toFixed(1)}% control, ${(exp.traffic_pct * exp.treatment_pct / 100).toFixed(1)}% treatment`);
      break;
    }
    case 'pause': {
      runSql(
        `UPDATE experiments SET status = 'paused', paused_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [id]
      );
      console.log(`Experiment ${id} paused.`);
      break;
    }
    case 'complete': {
      runSql(
        `UPDATE experiments SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [id]
      );
      console.log(`Experiment ${id} completed.`);
      break;
    }
    default:
      console.error(`Unknown action: ${action}. Use: list, activate, pause, complete`);
      process.exit(1);
  }
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
