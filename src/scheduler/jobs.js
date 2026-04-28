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
const { runVctPostSyncPipeline } = require('../pipeline/post-sync-vct');

/**
 * Schedule all recurring jobs.
 */
function startScheduler() {
  console.log('[Scheduler] Starting scheduled jobs...');

  // Daily sync — Kytsan + Prime Commerce (small clients, fast import + full pipeline)
  cron.schedule('0 6 * * *', async () => {
    console.log('[Scheduler] Running daily sync (Kytsan, Prime)...');

    for (const id of [1, 2]) {
      try {
        const ingestion = new DataIngestion(id);
        ingestion.init();

        const endDate = formatDate(new Date());
        const newOrdersStart = formatDate(daysAgo(7));
        await ingestion.syncGateways();
        console.log(`[Scheduler] Client ${id}: pulling new orders ${newOrdersStart} to ${endDate}`);
        await ingestion.pullTransactions(newOrdersStart, endDate);

        const syncVerification = ingestion.verifySyncWindow(newOrdersStart, endDate);
        if (syncVerification.gaps.length > 0) {
          console.error(`[Scheduler] Client ${id} VERIFICATION FAILED — gaps detected:`);
          for (const gap of syncVerification.gaps) {
            console.error(`  ${gap.day}: API=${gap.apiTotal} DB=${gap.dbCount} (${gap.coverage}%)`);
          }
        } else {
          console.log(`[Scheduler] Client ${id}: all ${syncVerification.daysChecked} days verified (DB counts match API)`);
        }

        const updatesStart = formatDate(daysAgo(2));
        console.log(`[Scheduler] Client ${id}: pulling status updates ${updatesStart} to ${endDate}`);
        await ingestion.pullStatusUpdates(updatesStart, endDate);

        console.log(`[Scheduler] Daily sync complete for client ${id}.`, ingestion.getStats());

        // Analysis pipeline
        try {
          await runClassifiers(id);
          buildPerformanceMatrix(id);
          detectOptimizationWindows(id);
          detectMidDegradation(id);
          checkWaitingImplementations();
          evaluateImplementations();
        } catch (err) {
          console.error(`[Scheduler] Analysis pipeline failed for client ${id}:`, err.message);
        }

        // Post-sync pipeline: classify → derive → reconcile → alerts → recompute
        try {
          await runPostSyncPipeline(id);
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
    console.log('[Scheduler] Kytsan + Prime daily sync complete.');
  });

  // Daily sync — VCT (high-volume, separate pipeline so it can't block Kytsan/Prime)
  cron.schedule('15 6 * * *', async () => {
    console.log('[Scheduler] Running daily sync (VCT)...');
    try {
      const ingestion = new DataIngestion(6);
      ingestion.init();

      const endDate = formatDate(new Date());
      const newOrdersStart = formatDate(daysAgo(3));
      await ingestion.syncGateways();
      console.log(`[Scheduler] VCT: pulling new orders ${newOrdersStart} to ${endDate}`);
      await ingestion.pullTransactions(newOrdersStart, endDate);

      const syncVerification = ingestion.verifySyncWindow(newOrdersStart, endDate);
      if (syncVerification.gaps.length > 0) {
        console.error('[Scheduler] VCT VERIFICATION FAILED — gaps detected:');
        for (const gap of syncVerification.gaps) {
          console.error(`  ${gap.day}: API=${gap.apiTotal} DB=${gap.dbCount} (${gap.coverage}%)`);
        }
      } else {
        console.log(`[Scheduler] VCT: all ${syncVerification.daysChecked} days verified (DB counts match API)`);
      }

      const updatesStart = formatDate(daysAgo(2));
      console.log(`[Scheduler] VCT: pulling status updates ${updatesStart} to ${endDate}`);
      await ingestion.pullStatusUpdates(updatesStart, endDate);

      console.log('[Scheduler] VCT daily sync complete.', ingestion.getStats());

      try {
        const result = runVctPostSyncPipeline(6);
        console.log(`[Scheduler] VCT post-sync: ${result.classified} classified, ${result.cascadeParsed} cascades parsed`);
      } catch (err) {
        console.error(`[Scheduler] VCT post-sync pipeline failed:`, err.message);
      }
    } catch (err) {
      console.error('[Scheduler] VCT daily pull failed:', err.message);
    }
    console.log('[Scheduler] VCT daily sync complete.');
  });

  // Hourly MID status check (at :30 to avoid colliding with daily sync at :00)
  cron.schedule('30 * * * *', async () => {
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
  console.log('  - Daily sync: clients 1,2,6 at 6:00 AM UTC (VCT=3d, others=7d)');
  console.log('  - Hourly MID check: every hour at :30');
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
