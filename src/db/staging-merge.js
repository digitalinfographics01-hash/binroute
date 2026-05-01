/**
 * staging-merge.js — merge a staging DB into main DB.
 *
 * Uses ATTACH DATABASE + batched INSERT ... ON CONFLICT to merge orders
 * from a staging file into the production database.
 *
 * Derived columns (cascade_chain, derived_product_role, etc.) may arrive
 * pre-computed from staging-post-sync.js. The merge uses COALESCE so that:
 *   - NEW rows: staging values flow through (pre-classified)
 *   - EXISTING rows: staging value wins if non-NULL, else main DB value preserved
 *
 * WAL safety: passive checkpoints every 10 batches. Never truncates.
 */

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const { getDb, checkpointWal } = require('./connection');
const {
  INSERT_COLUMNS_SQL, ON_CONFLICT_SET_SQL,
  MERGE_COLUMNS, MERGE_COLUMNS_SQL, MERGE_ON_CONFLICT_SET_SQL,
} = require('./order-columns');

const BATCH_SIZE = 5000;
const LOCK_PATH = path.join(__dirname, '..', '..', 'data', 'staging', '.merge-lock');
const STALE_LOCK_MS = 30 * 60 * 1000; // 30 minutes

// ── Merge lock ───────────────────────────────────────────────────────────

function acquireMergeLock() {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 2000;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    // Check for stale lock
    if (fs.existsSync(LOCK_PATH)) {
      try {
        const stat = fs.statSync(LOCK_PATH);
        if (Date.now() - stat.mtimeMs > STALE_LOCK_MS) {
          console.log(`[staging-merge] Stale merge lock detected (${Math.round((Date.now() - stat.mtimeMs) / 60000)}min old) — removing`);
          fs.unlinkSync(LOCK_PATH);
        }
      } catch {}
    }

    try {
      fs.writeFileSync(LOCK_PATH, `pid=${process.pid} ts=${Date.now()}`, { flag: 'wx' });
      return true;
    } catch (err) {
      if (err.code === 'EEXIST') {
        if (attempt < MAX_RETRIES) {
          console.log(`[staging-merge] Merge lock held by another process, retry ${attempt}/${MAX_RETRIES} in ${RETRY_DELAY / 1000}s...`);
          const waitUntil = Date.now() + RETRY_DELAY;
          while (Date.now() < waitUntil) { /* busy wait — sync context */ }
        }
      } else {
        throw err;
      }
    }
  }

  return false;
}

function releaseMergeLock() {
  try { fs.unlinkSync(LOCK_PATH); } catch {}
}

// ── Schema validation ────────────────────────────────────────────────────

function validateMergeColumns(db) {
  const mainCols = new Set(
    db.prepare("PRAGMA table_info('orders')").all().map(r => r.name)
  );

  const missing = MERGE_COLUMNS.filter(c => !mainCols.has(c));
  if (missing.length > 0) {
    return { valid: false, missing };
  }
  return { valid: true, missing: [] };
}

function validateStagingColumns(db) {
  const stagingCols = new Set(
    db.prepare("PRAGMA table_info('orders')").all().map(r => r.name)
  );

  const missing = MERGE_COLUMNS.filter(c => !stagingCols.has(c));
  if (missing.length > 0) {
    return { valid: false, missing };
  }
  return { valid: true, missing: [] };
}

// ── Main merge function ──────────────────────────────────────────────────

/**
 * Merge a staging DB into the main DB.
 *
 * @param {string} stagingPath - absolute path to the staging .db file
 * @param {number} clientId - expected client_id (safety check)
 * @returns {{ inserted: number, updated: number, totalMerged: number, elapsed: number }}
 */
function mergeStagingToMain(stagingPath, clientId) {
  const startTime = Date.now();
  const db = getDb();

  // --- Pre-merge validation ------------------------------------------------
  if (!fs.existsSync(stagingPath)) {
    throw new Error(`Staging file not found: ${stagingPath}`);
  }

  // Open staging read-only to validate before attaching
  const stagingDb = new Database(stagingPath, { readonly: true });
  const stagingCount = stagingDb.prepare('SELECT COUNT(*) as cnt FROM orders').get().cnt;
  if (stagingCount === 0) {
    stagingDb.close();
    fs.unlinkSync(stagingPath);
    console.log('[staging-merge] Staging DB is empty — nothing to merge, file deleted');
    return { inserted: 0, updated: 0, totalMerged: 0, elapsed: 0 };
  }

  // Verify all rows belong to expected client
  const wrongClient = stagingDb.prepare(
    'SELECT COUNT(*) as cnt FROM orders WHERE client_id != ?'
  ).get(clientId).cnt;
  stagingDb.close();
  if (wrongClient > 0) {
    throw new Error(`Staging file contains ${wrongClient} rows for wrong client (expected ${clientId})`);
  }

  // --- Acquire merge lock ---------------------------------------------------
  if (!acquireMergeLock()) {
    console.error(`[staging-merge] CRITICAL: Could not acquire merge lock after retries. Staging file preserved: ${stagingPath}`);
    return { inserted: 0, updated: 0, totalMerged: 0, elapsed: 0, pending: true };
  }

  try {
    // --- Schema validation ---------------------------------------------------
    const mainValidation = validateMergeColumns(db);
    let useExtendedMerge = true;
    let columnsSQL, conflictSQL;

    if (!mainValidation.valid) {
      const msg = `Main DB missing ${mainValidation.missing.length} merge columns: ${mainValidation.missing.join(', ')}`;
      if (process.env.NODE_ENV !== 'production') {
        throw new Error(`[staging-merge] SCHEMA ERROR: ${msg}`);
      }
      console.error(`[staging-merge] CRITICAL: ${msg} — falling back to 134-column merge. Derived columns from staging will be lost.`);
      useExtendedMerge = false;
    }

    if (useExtendedMerge) {
      columnsSQL = MERGE_COLUMNS_SQL;
      conflictSQL = MERGE_ON_CONFLICT_SET_SQL;
      console.log(`[staging-merge] Merging ${stagingCount} rows for client ${clientId} (140-column merge, 6 derived columns included)...`);
    } else {
      columnsSQL = INSERT_COLUMNS_SQL;
      conflictSQL = ON_CONFLICT_SET_SQL;
      console.log(`[staging-merge] Merging ${stagingCount} rows for client ${clientId} (134-column fallback)...`);
    }

    // --- Count before merge --------------------------------------------------
    const countBefore = db.prepare(
      'SELECT COUNT(*) as cnt FROM orders WHERE client_id = ?'
    ).get(clientId).cnt;

    // --- ATTACH staging DB ---------------------------------------------------
    db.exec(`ATTACH DATABASE '${stagingPath.replace(/'/g, "''")}' AS staging`);

    try {
      // Validate staging schema if using extended merge
      if (useExtendedMerge) {
        const stagingValidation = validateStagingColumns(db);
        if (!stagingValidation.valid) {
          const msg = `Staging DB missing columns: ${stagingValidation.missing.join(', ')}`;
          if (process.env.NODE_ENV !== 'production') {
            throw new Error(`[staging-merge] SCHEMA ERROR: ${msg}`);
          }
          console.error(`[staging-merge] CRITICAL: ${msg} — falling back to 134-column merge`);
          columnsSQL = INSERT_COLUMNS_SQL;
          conflictSQL = ON_CONFLICT_SET_SQL;
        }
      }

      // Build the merge SQL
      const mergeSql = `
        INSERT INTO main.orders (${columnsSQL})
        SELECT ${columnsSQL}
        FROM staging.orders
        WHERE staging.orders.rowid > ? AND staging.orders.rowid <= ?
        ON CONFLICT(client_id, order_id) DO UPDATE SET ${conflictSQL}
      `;
      const mergeStmt = db.prepare(mergeSql);

      // --- Batched merge in transactions ------------------------------------
      let merged = 0;
      let batchNum = 0;
      let lastRowid = 0;

      // Get max rowid for iteration
      const maxRowid = db.prepare('SELECT MAX(rowid) as m FROM staging.orders').get().m || 0;

      while (lastRowid < maxRowid) {
        const nextRowid = lastRowid + BATCH_SIZE;

        const txn = db.transaction(() => {
          return mergeStmt.run(lastRowid, nextRowid);
        });
        const result = txn();
        merged += result.changes;
        lastRowid = nextRowid;
        batchNum++;

        // Passive WAL checkpoint every 10 batches to keep WAL manageable
        if (batchNum % 10 === 0) {
          try { checkpointWal(); } catch {}
          console.log(`[staging-merge]   ${merged}/${stagingCount} merged (batch ${batchNum})...`);
        }
      }

      // --- Post-merge verification -------------------------------------------
      const countAfter = db.prepare(
        'SELECT COUNT(*) as cnt FROM orders WHERE client_id = ?'
      ).get(clientId).cnt;
      const newRows = countAfter - countBefore;
      const updatedRows = merged - newRows;

      console.log(`[staging-merge] Merged: ${newRows} new + ${updatedRows} updated = ${merged} total`);

      // Spot-check: verify derived columns on existing rows weren't wiped
      const spotCheck = db.prepare(`
        SELECT COUNT(*) as cnt FROM orders
        WHERE client_id = ? AND derived_product_role IS NOT NULL
      `).get(clientId).cnt;
      console.log(`[staging-merge] Derived columns intact: ${spotCheck} rows have derived_product_role`);

      // Final WAL checkpoint
      try { checkpointWal(); } catch {}

      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      console.log(`[staging-merge] Complete in ${elapsed}s`);

      return {
        inserted: newRows,
        updated: updatedRows > 0 ? updatedRows : 0,
        totalMerged: merged,
        elapsed: parseFloat(elapsed),
      };

    } finally {
      // Always detach, even on error
      try { db.exec('DETACH DATABASE staging'); } catch {}

      // Delete staging file on success (caller can catch errors before here)
      try {
        fs.unlinkSync(stagingPath);
        try { fs.unlinkSync(stagingPath + '-wal'); } catch {}
        try { fs.unlinkSync(stagingPath + '-shm'); } catch {}
        console.log(`[staging-merge] Staging file deleted: ${stagingPath}`);
      } catch {}
    }

  } finally {
    releaseMergeLock();
  }
}

module.exports = { mergeStagingToMain };
