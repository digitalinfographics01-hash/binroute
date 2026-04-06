const cron = require('node-cron');
const { querySql } = require('../db/connection');
const DataIngestion = require('../api/ingestion');
const { runClassifiers } = require('../classifiers/runner');
const { buildPerformanceMatrix } = require('../engine/performance');
const { detectOptimizationWindows, detectMidDegradation } = require('../engine/optimizer');
const { checkWaitingImplementations, evaluateImplementations } = require('../engine/implementation');
const { evaluatePlaybookImplementations } = require('../engine/playbook-implementation');
const { recomputeAllAnalytics } = require('../analytics/engine');
const { runPostSyncPipeline } = require('../pipeline/post-sync');

/**
 * Schedule all recurring jobs.
 */
function startScheduler() {
  console.log('[Scheduler] Starting scheduled jobs...');

  // Daily full transaction pull at 6:00 AM
  cron.schedule('0 6 * * *', async () => {
    console.log('[Scheduler] Running daily full pull...');
    const clients = querySql('SELECT id FROM clients');

    for (const { id } of clients) {
      try {
        const ingestion = new DataIngestion(id);
        ingestion.init();

        const endDate = formatDate(new Date());
        const startDate = formatDate(daysAgo(180));

        await ingestion.syncGateways();
        // Date-based order pull — no campaign dependency
        await ingestion.pullTransactions(startDate, endDate);

        // Run analysis pipeline
        await runClassifiers(id);
        buildPerformanceMatrix(id);
        detectOptimizationWindows(id);
        detectMidDegradation(id);
        checkWaitingImplementations();
        evaluateImplementations();

        console.log(`[Scheduler] Daily pull complete for client ${id}.`, ingestion.getStats());

        // Post-sync pipeline: classify → derive → recompute
        try {
          runPostSyncPipeline(id);
        } catch (err) {
          console.error(`[Scheduler] Post-sync pipeline failed for client ${id}:`, err.message);
        }
        recomputeAllAnalytics(id).catch(err =>
          console.error(`[Scheduler] Analytics recompute failed for client ${id}:`, err.message)
        );
        try {
          const pbResult = evaluatePlaybookImplementations();
          if (pbResult.evaluated > 0 || pbResult.transitioned > 0) {
            console.log(`[Scheduler] Playbook implementations: ${pbResult.evaluated} evaluated, ${pbResult.transitioned} transitioned`);
          }
        } catch (err) {
          console.error(`[Scheduler] Playbook implementation eval failed:`, err.message);
        }
      } catch (err) {
        console.error(`[Scheduler] Daily pull failed for client ${id}:`, err.message);
      }
    }
  });

  // Hourly MID status check
  cron.schedule('0 * * * *', async () => {
    console.log('[Scheduler] Running hourly MID status check...');
    const clients = querySql('SELECT id FROM clients');

    for (const { id } of clients) {
      try {
        const ingestion = new DataIngestion(id);
        ingestion.init();
        await ingestion.checkMidStatus();
      } catch (err) {
        console.error(`[Scheduler] MID check failed for client ${id}:`, err.message);
      }
    }
  });

  // Every 6 hours: check implementations
  cron.schedule('0 */6 * * *', () => {
    console.log('[Scheduler] Checking implementation statuses...');
    try {
      checkWaitingImplementations();
      evaluateImplementations();
      const pbResult = evaluatePlaybookImplementations();
      if (pbResult.evaluated > 0 || pbResult.transitioned > 0) {
        console.log(`[Scheduler] Playbook implementations: ${pbResult.evaluated} evaluated, ${pbResult.transitioned} transitioned`);
      }
    } catch (err) {
      console.error('[Scheduler] Implementation check failed:', err.message);
    }
  });

  // Weekly AI retrain: Sunday 7:00 AM (after daily sync completes)
  cron.schedule('0 7 * * 0', () => {
    console.log('[Scheduler] Running weekly AI retrain...');
    try {
      const { runRetrain } = require('../ml/retrain-runner');
      const result = runRetrain();
      console.log(`[Scheduler] AI retrain complete. Velocity: ${result.velocityUpdated}, Subscription: ${result.subscriptionUpdated}`);
    } catch (err) {
      console.error('[Scheduler] AI retrain failed:', err.message);
    }
  });

  console.log('[Scheduler] Jobs scheduled:');
  console.log('  - Daily full pull: 6:00 AM');
  console.log('  - Hourly MID check: every hour');
  console.log('  - Implementation check: every 6 hours');
  console.log('  - Weekly AI retrain: Sunday 7:00 AM');
}

function daysAgo(n) {
  const d = new Date(); d.setDate(d.getDate() - n); return d;
}
function formatDate(d) {
  return `${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getDate()).padStart(2,'0')}/${d.getFullYear()}`;
}

module.exports = { startScheduler };
