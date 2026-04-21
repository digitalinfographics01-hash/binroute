/**
 * API-key authentication middleware for /api/route.
 *
 * Mounted BEFORE the session-auth middleware in server.js. The checkout-facing
 * routing endpoint cannot use session cookies — it's called from Kytsan's PHP
 * checkout extension, which holds no session. Instead we authenticate with two
 * headers (same shape Beast Insights uses):
 *
 *   x-client-id   — numeric client_id
 *   x-api-key     — plaintext token, bcrypt-compared against api_keys.api_key_hash
 *
 * On success: attaches req.apiClientId = <id> and calls next().
 * On failure: responds 401 with a minimal error body.
 *
 * We cache the hashes per client_id in memory for a short TTL so a typical
 * request does one Map lookup + one bcrypt compare — no DB hit on the hot path.
 */

const bcrypt = require('bcryptjs');
const { querySql, runSql } = require('../db/connection');

const CACHE_TTL_MS = 60 * 1000;
const cache = new Map(); // client_id -> { rows: [{id, api_key_hash}], expiresAt }

function loadKeysForClient(clientId) {
  const hit = cache.get(clientId);
  const now = Date.now();
  if (hit && hit.expiresAt > now) return hit.rows;

  const rows = querySql(
    'SELECT id, api_key_hash FROM api_keys WHERE client_id = ? AND revoked_at IS NULL',
    [clientId]
  );
  cache.set(clientId, { rows, expiresAt: now + CACHE_TTL_MS });
  return rows;
}

function invalidateCache(clientId) {
  if (clientId == null) cache.clear();
  else cache.delete(clientId);
}

function apiKeyAuth(req, res, next) {
  const clientIdRaw = req.headers['x-client-id'];
  const apiKey = req.headers['x-api-key'];

  if (!clientIdRaw || !apiKey) {
    return res.status(401).json({ error: 'Missing x-client-id or x-api-key header' });
  }

  const clientId = parseInt(clientIdRaw, 10);
  if (!Number.isInteger(clientId) || clientId <= 0) {
    return res.status(401).json({ error: 'Invalid x-client-id' });
  }

  let rows;
  try {
    rows = loadKeysForClient(clientId);
  } catch (err) {
    return res.status(500).json({ error: 'Auth backend unavailable' });
  }

  if (!rows || rows.length === 0) {
    return res.status(401).json({ error: 'No active API keys for this client' });
  }

  let matchedId = null;
  for (const row of rows) {
    // bcrypt.compareSync is sub-ms for cost 10; per-request <1ms even with
    // a handful of active keys per client.
    if (bcrypt.compareSync(apiKey, row.api_key_hash)) {
      matchedId = row.id;
      break;
    }
  }

  if (matchedId == null) {
    return res.status(401).json({ error: 'Invalid API key' });
  }

  // Stamp last_used_at asynchronously — fire & forget, do not block the request.
  try {
    runSql('UPDATE api_keys SET last_used_at = CURRENT_TIMESTAMP WHERE id = ?', [matchedId]);
  } catch (_) {
    // Non-fatal; the request is already authenticated.
  }

  req.apiClientId = clientId;
  req.apiKeyId = matchedId;
  next();
}

module.exports = apiKeyAuth;
module.exports.invalidateCache = invalidateCache;
