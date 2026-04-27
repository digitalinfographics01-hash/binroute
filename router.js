/**
 * BinRoute AI Router — independent process for shadow routing decisions.
 *
 * Serves ONLY:
 *   POST /api/route          — shadow routing (API-key auth)
 *   POST /api/route/submit-timing — checkout timing beacon
 *   GET  /health             — health check
 *
 * Runs as its own PM2 process (binroute-router) on port 3002 so that
 * restarting the data platform (binroute, port 3001) never interrupts
 * live checkout routing.
 *
 * Dependencies: DB (WAL-safe read + shadow_decisions INSERT), scoring
 * daemon (port 5001), lookup tables, API-key auth. Zero dependency on
 * scheduler, analytics, classifiers, session auth, or UI.
 */

const express = require('express');
const { initDb, closeDb } = require('./src/db/connection');
const { initializeDatabase } = require('./src/db/schema');

const app = express();
const PORT = process.env.ROUTER_PORT || 3002;

app.use(express.json());

// Health check — PM2 / Nginx can poll this
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'binroute-router', port: PORT });
});

// Shadow routing — API-key auth, no session needed
app.use(
  '/api/route',
  require('./src/middleware/api-key-auth'),
  require('./src/routes/route')
);

// Start
async function start() {
  await initializeDatabase();
  console.log('[Router] Database ready.');

  app.listen(PORT, () => {
    console.log(`[Router] BinRoute AI Router running on http://localhost:${PORT}`);
  });
}

start().catch(err => {
  console.error('[Router] Failed to start:', err);
  closeDb();
  process.exit(1);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n[Router] Shutting down...');
  closeDb();
  process.exit(0);
});
process.on('SIGTERM', () => {
  console.log('[Router] Shutting down...');
  closeDb();
  process.exit(0);
});
