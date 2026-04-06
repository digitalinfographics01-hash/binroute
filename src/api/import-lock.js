/**
 * Per-client import lock — prevents concurrent imports on the same client.
 * Shared between HTTP routes and scheduler.
 */

const activeImports = new Map(); // clientId -> { startedAt, type, ingestion }

function acquireImportLock(clientId, type = 'full') {
  if (activeImports.has(clientId)) {
    const existing = activeImports.get(clientId);
    throw new Error(`Import already running for client ${clientId} (${existing.type}, started ${existing.startedAt})`);
  }
  const lock = { startedAt: new Date().toISOString(), type };
  activeImports.set(clientId, lock);
  return lock;
}

function releaseImportLock(clientId) {
  activeImports.delete(clientId);
}

function getImportStatus(clientId) {
  const lock = activeImports.get(clientId);
  if (!lock) return { active: false };
  return {
    active: true,
    type: lock.type,
    started_at: lock.startedAt,
    elapsed_seconds: Math.round((Date.now() - new Date(lock.startedAt).getTime()) / 1000),
    progress: lock.ingestion?.progress || null,
  };
}

function setIngestionRef(clientId, ingestion) {
  const lock = activeImports.get(clientId);
  if (lock) lock.ingestion = ingestion;
}

module.exports = { acquireImportLock, releaseImportLock, getImportStatus, setIngestionRef };
