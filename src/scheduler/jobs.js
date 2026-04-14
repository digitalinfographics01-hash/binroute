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

  // Daily sync — PAUSED until new approach is tested
  // To re-enable: change false to '0 6 * * *'
  // cron.schedule('0 6 * * *', async () => {
  if (false) { (async () => {
    console.log('[Scheduler] Running daily sync...');
    const clients = querySql('SELECT id FROM clients');

    for (const { id } of clients) {
      try {
        const ingestion = new DataIngestion(id);
        ingestion.init();

        const endDate = formatDate(new Date());

        // Step 1: Pull new orders (last 3 days by created date)
        const newOrdersStart = formatDate(daysAgo(3));
        await ingestion.syncGateways();
        console.log(`[Scheduler] Client ${id}: pulling new orders ${newOrdersStart} to ${endDate}`);
        await ingestion.pullTransactions(newOrdersStart, endDate);

        // Step 2: Pull status updates (chargebacks, refunds, voids on ANY order)
        const updatesStart = formatDate(daysAgo(2));
        console.log(`[Scheduler] Client ${id}: pulling status updates ${updatesStart} to ${endDate}`);
        await ingestion.pullStatusUpdates(updatesStart, endDate);

        // Run analysis pipeline
        await runClassifiers(id);
        buildPerformanceMatrix(id);
        detectOptimizationWindows(id);
        detectMidDegradation(id);
        checkWaitingImplementations();
        evaluateImplementations();

        console.log(`[Scheduler] Daily sync complete for client ${id}.`, ingestion.getStats());

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
  })(); }

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
  console.log('  - Daily sync: PAUSED (testing new approach)');
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
