/**
 * Apply the product_sequence classifications to null-sequence groups
 * for clients 1-5. Single transaction, atomic, reports before/after.
 *
 * Safe to run while VCT import is active — only touches product_groups
 * rows for clients 1-5, no conflict with client 6 writes.
 */
const { initDb, querySql, runSql, transaction, saveDb } = require('../src/db/connection');

const EXCLUDED_IDS = [5, 6, 49, 67, 85, 126, 132, 140, 141, 142, 146, 63, 226, 178, 179];
const RECOVERY_IDS = [37, 127, 224, 177];
const UPSELL_IDS   = [84, 225];

(async () => {
  await initDb();

  // Snapshot: show current state of the target groups
  const allIds = [...EXCLUDED_IDS, ...RECOVERY_IDS, ...UPSELL_IDS];
  console.log('BEFORE state of the 21 target groups:');
  console.log('ID   | Client | Current seq      | Group name');
  console.log('-----+--------+------------------+----------------------------------------');
  const before = querySql(
    `SELECT id, client_id, COALESCE(product_sequence, '(null)') as seq, group_name
     FROM product_groups WHERE id IN (${allIds.map(() => '?').join(',')}) ORDER BY id`,
    allIds
  );
  before.forEach(r => {
    console.log(
      String(r.id).padStart(4) + ' | ' +
      String(r.client_id).padStart(6) + ' | ' +
      (r.seq || '').padEnd(16) + ' | ' +
      (r.group_name || '(no name)').substring(0, 50)
    );
  });
  console.log();

  // Apply updates in a single transaction
  let excludedApplied = 0;
  let recoveryApplied = 0;
  let upsellApplied = 0;

  try {
    transaction(() => {
      // EXCLUDED
      for (const id of EXCLUDED_IDS) {
        runSql(
          "UPDATE product_groups SET product_sequence='excluded', updated_at=datetime('now') WHERE id = ?",
          [id]
        );
        excludedApplied++;
      }
      // RECOVERY
      for (const id of RECOVERY_IDS) {
        runSql(
          "UPDATE product_groups SET product_sequence='recovery', updated_at=datetime('now') WHERE id = ?",
          [id]
        );
        recoveryApplied++;
      }
      // UPSELL
      for (const id of UPSELL_IDS) {
        runSql(
          "UPDATE product_groups SET product_sequence='upsell', updated_at=datetime('now') WHERE id = ?",
          [id]
        );
        upsellApplied++;
      }
    });
    saveDb();
  } catch (err) {
    console.error('Transaction failed:', err.message);
    process.exit(1);
  }

  console.log('Applied:');
  console.log('  excluded: ' + excludedApplied);
  console.log('  recovery: ' + recoveryApplied);
  console.log('  upsell:   ' + upsellApplied);
  console.log('  total:    ' + (excludedApplied + recoveryApplied + upsellApplied));
  console.log();

  // Verify: re-read the same groups
  console.log('AFTER state:');
  console.log('ID   | Client | New seq          | Group name');
  console.log('-----+--------+------------------+----------------------------------------');
  const after = querySql(
    `SELECT id, client_id, COALESCE(product_sequence, '(null)') as seq, group_name
     FROM product_groups WHERE id IN (${allIds.map(() => '?').join(',')}) ORDER BY id`,
    allIds
  );
  after.forEach(r => {
    console.log(
      String(r.id).padStart(4) + ' | ' +
      String(r.client_id).padStart(6) + ' | ' +
      (r.seq || '').padEnd(16) + ' | ' +
      (r.group_name || '(no name)').substring(0, 50)
    );
  });
  console.log();

  // Final sanity check: count remaining null-sequence groups for clients 1-5
  const remainingNulls = querySql(
    "SELECT client_id, COUNT(*) n FROM product_groups WHERE client_id IN (1,2,3,4,5) AND product_sequence IS NULL GROUP BY client_id"
  );
  if (remainingNulls.length === 0) {
    console.log('✓ No remaining null-sequence groups for clients 1-5');
  } else {
    console.log('Remaining null-sequence groups:');
    remainingNulls.forEach(r => console.log('  client ' + r.client_id + ': ' + r.n));
  }

  // Full sequence distribution
  console.log();
  console.log('Final sequence distribution (clients 1-5):');
  querySql(
    `SELECT client_id, COALESCE(product_sequence, '(null)') as seq, COUNT(*) n
     FROM product_groups WHERE client_id IN (1,2,3,4,5)
     GROUP BY client_id, seq ORDER BY client_id, n DESC`
  ).forEach(r => {
    console.log('  client ' + r.client_id + ': ' + r.seq.padEnd(12) + ' = ' + r.n);
  });

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
