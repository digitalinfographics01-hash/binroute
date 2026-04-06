/**
 * One-off: checkpoint the bloated WAL file back to the main DB.
 * WARNING: Do NOT run while imports are active — TRUNCATE requires exclusive WAL access.
 */
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const DB_PATH = path.join(__dirname, '..', 'data', 'binroute.db');
const WAL_PATH = DB_PATH + '-wal';

// Show before size
try {
  const walSize = fs.statSync(WAL_PATH).size;
  console.log('WAL before:', (walSize / 1024 / 1024).toFixed(1) + ' MB');
} catch {
  console.log('No WAL file found.');
  process.exit(0);
}

const db = new Database(DB_PATH);
db.pragma('wal_checkpoint(TRUNCATE)');
db.close();

// Show after size
try {
  const walSize = fs.statSync(WAL_PATH).size;
  console.log('WAL after:', (walSize / 1024 / 1024).toFixed(1) + ' MB');
} catch {
  console.log('WAL file removed.');
}

console.log('Done.');
