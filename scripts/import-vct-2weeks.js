/**
 * Import Very Cool Thing (client 6) — 2 weeks of orders.
 *
 * Uses per-chunk DB writes (streaming mode) for memory safety.
 * Configurable concurrency: start with 1 for testing, scale to 3.
 *
 * Usage:
 *   node scripts/import-vct-2weeks.js              — 2 weeks, dayConcurrency=1 (test)
 *   node scripts/import-vct-2weeks.js --concurrent=3   — 2 weeks, dayConcurrency=3
 *   node scripts/import-vct-2weeks.js --days=1          — 1 day only (quick test)
 */
const { initDb } = require('../src/db/connection');
const DataIngestion = require('../src/api/ingestion');

const CLIENT_ID = 6;

// Parse CLI args
const concurrentArg = process.argv.find(a => a.startsWith('--concurrent='));
const daysArg = process.argv.find(a => a.startsWith('--days='));
const dayConcurrency = concurrentArg ? parseInt(concurrentArg.split('=')[1], 10) : 1;
const daysBack = daysArg ? parseInt(daysArg.split('=')[1], 10) : 14;

(async () => {
  await initDb();
  const ing = new DataIngestion(CLIENT_ID);
  ing.init();

  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - daysBack);

  const fmt = (d) => `${String(d.getMonth() + 1).padStart(2, '0')}/${String(d.getDate()).padStart(2, '0')}/${d.getFullYear()}`;
  const startDate = fmt(start);
  const endDate = fmt(end);

  console.log(`Importing client ${CLIENT_ID} (Very Cool Thing): ${startDate} to ${endDate}`);
  console.log(`Options: dayConcurrency=${dayConcurrency}, chunkTarget=400, daysBack=${daysBack}`);
  console.log();

  await ing.pullTransactions(startDate, endDate, {
    dayConcurrency,
    chunkTarget: 400,
  });

  console.log('\nDone.');
  process.exit(0);
})().catch(e => {
  console.error('FATAL:', e.message, e.stack);
  process.exit(1);
});
