const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

const DB_PATH = path.join(__dirname, '..', '..', 'data', 'binroute.db');

let db = null;

/**
 * better-sqlite3 wrapper — replaces sql.js.
 * Reads from disk natively (no 200MB memory load), synchronous API.
 * Data is persisted automatically (no manual save needed).
 */

async function initDb() {
  if (db) return db;

  // Ensure data directory exists
  const dir = path.dirname(DB_PATH);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  db = new Database(DB_PATH);

  // Enable WAL mode for better concurrent read performance
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');   // safe with WAL, major write speedup
  db.pragma('cache_size = -64000');     // 64 MB page cache (default is 2 MB)
  db.pragma('temp_store = MEMORY');     // temp tables in memory
  db.pragma('foreign_keys = ON');
  db.pragma('busy_timeout = 10000');   // wait up to 10s for write lock (parallel imports)

  return db;
}

/**
 * Get the database instance (must call initDb first).
 */
function getDb() {
  if (!db) throw new Error('Database not initialized. Call await initDb() first.');
  return db;
}

/**
 * Save database to disk.
 * No-op with better-sqlite3 — writes go directly to disk.
 */
function saveDb() {
  // better-sqlite3 persists automatically; no manual save needed
}

/**
 * Close the database.
 */
function closeDb() {
  if (db) {
    try { db.pragma('wal_checkpoint(PASSIVE)'); } catch {}
    db.close();
    db = null;
  }
}

/**
 * Checkpoint the WAL file (passive mode) — safe during concurrent access.
 * Transfers as many frames as possible without blocking other processes.
 */
function checkpointWal() {
  if (db) db.pragma('wal_checkpoint(PASSIVE)');
}

/**
 * Full WAL checkpoint (truncate mode) — reclaims disk space.
 * WARNING: Requires exclusive access. Do NOT run while imports are active.
 */
function checkpointWalFull() {
  if (db) db.pragma('wal_checkpoint(TRUNCATE)');
}

/**
 * Helper: run a SQL statement that modifies data (INSERT/UPDATE/DELETE).
 */
function runSql(sql, params = []) {
  db.prepare(sql).run(...params);
}

/**
 * Helper: run a SQL query and return all rows as objects.
 */
function querySql(sql, params = []) {
  return db.prepare(sql).all(...params);
}

/**
 * Helper: run a SQL query and return the first row as an object, or null.
 */
function queryOneSql(sql, params = []) {
  const row = db.prepare(sql).get(...params);
  return row || null;
}

/**
 * Helper: execute multiple SQL statements (for schema init).
 */
function execSql(sql) {
  db.exec(sql);
}

/**
 * Run a function inside a transaction. Auto-commits on success.
 */
function transaction(fn) {
  const wrapped = db.transaction(fn);
  return wrapped();
}

module.exports = {
  initDb,
  getDb,
  saveDb,
  closeDb,
  checkpointWal,
  checkpointWalFull,
  runSql,
  querySql,
  queryOneSql,
  execSql,
  transaction,
  DB_PATH,
};
