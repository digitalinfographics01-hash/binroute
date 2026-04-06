const { initDb, querySql, runSql, saveDb, closeDb, transaction } = require('../db/connection');
const { initializeDatabase } = require('../db/schema');
const { classifyDecline, classifyDeclineBatch, classifyWithAI } = require('./decline');
const { classifyTransactions } = require('./transaction');

/**
 * Run all classifiers on unclassified data for a client.
 */
async function runClassifiers(clientId, options = {}) {
  console.log(`[Classifier] Running classifiers for client ${clientId}...`);

  // 1. Classify decline reasons
  const declineStats = classifyDeclineReasons(clientId);
  console.log(`[Classifier] Decline classification: ${declineStats.classified} classified, ${declineStats.unclassified} unclassified`);

  // 2. If AI key provided, classify unknowns
  if (options.anthropicApiKey && declineStats.unknownReasons.length > 0) {
    console.log(`[Classifier] Running AI classification on ${declineStats.unknownReasons.length} unknown reasons...`);
    const aiResults = await classifyWithAI(declineStats.unknownReasons, options.anthropicApiKey);
    let aiClassified = 0;
    transaction(() => {
      for (const [reason, category] of Object.entries(aiResults)) {
        runSql(
          'UPDATE orders SET decline_category = ? WHERE client_id = ? AND decline_reason = ? AND decline_category IS NULL',
          [category, clientId, reason]
        );
        aiClassified++;
      }
    });
    console.log(`[Classifier] AI classified ${aiClassified} decline reasons.`);
  }

  // 3. Classify transaction types
  const txStats = classifyTransactions(clientId);
  console.log(`[Classifier] Transaction classification: ${txStats.classified} classified`);

  return { declineStats, txStats };
}

/**
 * Classify all orders with a decline reason but no category.
 */
function classifyDeclineReasons(clientId) {
  const orders = querySql(`
    SELECT DISTINCT decline_reason
    FROM orders
    WHERE client_id = ? AND order_status = 7
      AND decline_reason IS NOT NULL AND decline_reason != ''
      AND decline_category IS NULL
  `, [clientId]);

  const reasons = orders.map(o => o.decline_reason);
  const { results, unclassified } = classifyDeclineBatch(reasons);

  let classified = 0;
  transaction(() => {
    for (const [reason, category] of Object.entries(results)) {
      if (category) {
        runSql(
          'UPDATE orders SET decline_category = ? WHERE client_id = ? AND decline_reason = ? AND decline_category IS NULL',
          [category, clientId, reason]
        );
        classified++;
      }
    }
  });

  return {
    total: reasons.length,
    classified,
    unclassified: unclassified.length,
    unknownReasons: unclassified,
  };
}

// CLI
if (require.main === module) {
  const clientId = parseInt(process.argv[2], 10) || 1;
  const apiKey = process.argv[3] || process.env.ANTHROPIC_API_KEY;

  initializeDatabase()
    .then(() => runClassifiers(clientId, { anthropicApiKey: apiKey }))
    .then(results => {
      console.log('[Classifier] Done.', JSON.stringify(results, null, 2));
      closeDb();
    })
    .catch(err => {
      console.error('[Classifier] Error:', err.message);
      closeDb();
      process.exit(1);
    });
}

module.exports = { runClassifiers, classifyDeclineReasons };
