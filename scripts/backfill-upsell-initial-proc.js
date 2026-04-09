/**
 * Backfill initial_processor for upsell attempts.
 * Uses the same initialProcMap logic as the exploder.
 * Much faster than re-exploding — just UPDATE existing rows.
 */
const { initDb, runSql, querySql, transaction } = require('../src/db/connection');

(async () => {
  await initDb();

  const clients = querySql('SELECT DISTINCT client_id FROM transaction_attempts WHERE feature_version >= 3');

  let totalUpdated = 0;

  for (const { client_id } of clients) {
    console.log(`\nClient ${client_id}:`);

    // Build initial processor map: customer_id → processor that approved their first main_initial
    // Same logic as attempt-exploder._buildInitialProcessorMap
    const initProcs = querySql(
      "SELECT o.customer_id, g.processor_name " +
      "FROM orders o " +
      "JOIN gateways g ON g.client_id = o.client_id AND g.gateway_id = o.gateway_id " +
      "WHERE o.client_id = ? AND o.derived_product_role = 'main_initial' " +
      "AND o.order_status IN (2, 6, 8) AND g.processor_name IS NOT NULL " +
      "ORDER BY o.acquisition_date ASC",
      [client_id]
    );

    const procMap = new Map();
    for (const row of initProcs) {
      if (!procMap.has(row.customer_id)) {
        procMap.set(row.customer_id, row.processor_name);
      }
    }
    console.log('  Initial proc map: ' + procMap.size + ' customers');

    // Find upsell attempts that need backfill
    const upsells = querySql(
      "SELECT id, customer_id FROM transaction_attempts " +
      "WHERE client_id = ? AND derived_product_role = 'upsell_initial' " +
      "AND (initial_processor IS NULL OR initial_processor = '') " +
      "AND feature_version >= 3",
      [client_id]
    );
    console.log('  Upsells needing backfill: ' + upsells.length);

    let updated = 0;
    transaction(() => {
      for (const att of upsells) {
        const proc = procMap.get(att.customer_id);
        if (proc) {
          runSql('UPDATE transaction_attempts SET initial_processor = ? WHERE id = ?', [proc, att.id]);
          updated++;
        }
      }
    });

    console.log('  Updated: ' + updated + ' (matched ' + (upsells.length > 0 ? (100*updated/upsells.length).toFixed(1) : 0) + '%)');
    totalUpdated += updated;
  }

  console.log('\n=== Total updated: ' + totalUpdated + ' ===');
  process.exit(0);
})();
