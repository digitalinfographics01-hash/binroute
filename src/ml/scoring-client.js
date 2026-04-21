/**
 * Scoring client — thin axios wrapper around the Python scoring daemon.
 *
 * The daemon runs on 127.0.0.1:5001 (same host as the Node server) and loads
 * data/models/five_model_initial.pkl once at boot. We POST feature vectors and
 * candidate pools; it returns per-candidate AI scores.
 *
 * 30ms hard timeout. Any timeout / network error / non-2xx is treated as a
 * soft failure — callers fall back to lookup-only pick. We never throw from
 * scoreInitialCandidates(); the routing endpoint must always return a decision.
 */

const axios = require('axios');

const DAEMON_URL = process.env.SCORING_DAEMON_URL || 'http://127.0.0.1:5001';
// 60ms default timeout — measured warm p95 on Windows dev ~25ms, tail ~40ms.
// Linux prod should be tighter; tune via env var once we have real Kytsan data.
const TIMEOUT_MS = parseInt(process.env.SCORING_DAEMON_TIMEOUT_MS || '60', 10);

const client = axios.create({
  baseURL: DAEMON_URL,
  timeout: TIMEOUT_MS,
  headers: { 'Content-Type': 'application/json' },
});

/**
 * Score each candidate for an initial transaction.
 *
 * @param {Object}   payload
 * @param {Object}   payload.bin_features   — shared bin/customer/time features
 * @param {Object[]} payload.candidates     — one entry per eligible gateway
 * @returns {Promise<{
 *    ok: boolean,
 *    scores?: Array<{gateway_id: number, score: number}>,
 *    model_version?: string,
 *    latency_ms?: number,
 *    timed_out?: boolean,
 *    error?: string
 * }>}
 */
async function scoreInitialCandidates(payload) {
  const start = Date.now();
  try {
    const resp = await client.post('/score', payload);
    if (!resp.data || !Array.isArray(resp.data.scores)) {
      return { ok: false, error: 'malformed_daemon_response', latency_ms: Date.now() - start };
    }
    return {
      ok: true,
      scores: resp.data.scores,
      model_version: resp.data.model_version || null,
      latency_ms: Date.now() - start,
    };
  } catch (err) {
    const timedOut = err.code === 'ECONNABORTED' || /timeout/i.test(err.message || '');
    return {
      ok: false,
      timed_out: timedOut,
      error: timedOut ? 'timeout' : (err.message || 'daemon_unreachable'),
      latency_ms: Date.now() - start,
    };
  }
}

async function health() {
  try {
    const resp = await client.get('/health', { timeout: 500 });
    return { ok: true, data: resp.data };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

module.exports = { scoreInitialCandidates, health };
