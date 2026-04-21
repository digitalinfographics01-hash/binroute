const { initDb, checkpointWal } = require('../src/db/connection');
const DataIngestion = require('../src/api/ingestion');
const fs = require('fs');
const path = require('path');

const CLIENT_ID = 6;
const DAYS_BACK = 120;

async function main() {
  await initDb();

  // Clear old checkpoint for fresh start
  const cpPath = path.join(__dirname, '..', `checkpoint_client${CLIENT_ID}.json`);
  if (fs.existsSync(cpPath)) {
    fs.unlinkSync(cpPath);
    console.log('Cleared old checkpoint');
  }

  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - DAYS_BACK);

  const fmt = d => `${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getDate()).padStart(2,'0')}/${d.getFullYear()}`;
  const startDate = fmt(start);
  const endDate = fmt(end);

  console.log(`VCT full reimport: ${startDate} to ${endDate} (${DAYS_BACK} days)`);
  console.log('dayConcurrency=3, chunkTarget=400, newest first');

  const ingestion = new DataIngestion(CLIENT_ID);
  ingestion.init();

  // WAL checkpoint every 5 minutes to prevent buildup
  const walInterval = setInterval(() => {
    try { checkpointWal(); } catch (e) { /* ignore */ }
  }, 5 * 60 * 1000);

  const t = Date.now();
  await ingestion.pullTransactions(startDate, endDate, { dayConcurrency: 3, chunkTarget: 400 });
  const elapsed = ((Date.now() - t) / 1000 / 60).toFixed(1);

  clearInterval(walInterval);

  // Final WAL checkpoint
  try { checkpointWal(); } catch (e) { /* ignore */ }

  console.log(`\nDone in ${elapsed} minutes`);
  console.log('Stats:', JSON.stringify(ingestion.getStats(), null, 2));
  process.exit(0);
}

main().catch(err => { console.error(err); process.exit(1); });
