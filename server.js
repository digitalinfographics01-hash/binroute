const express = require('express');
const path = require('path');
const { initDb, closeDb } = require('./src/db/connection');
const { initializeDatabase } = require('./src/db/schema');
const { startScheduler } = require('./src/scheduler/jobs');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// CORS for development
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE');
  next();
});

// API Routes
app.use('/api/dashboard', require('./src/routes/dashboard'));
app.use('/api/config', require('./src/routes/config'));
app.use('/api/recommendations', require('./src/routes/recommendations'));
app.use('/api/lifecycle', require('./src/routes/lifecycle'));
app.use('/api/network', require('./src/routes/network-analysis'));
app.use('/api/actions', require('./src/routes/actions'));
app.use('/api/products', require('./src/routes/products'));
app.use('/api/analytics', require('./src/routes/analytics'));
app.use('/api/bins', require('./src/routes/bins'));
app.use('/api/master-bins', require('./src/routes/master-bins'));
app.use('/api/implementations', require('./src/routes/playbook-implementations'));

// Dynamic template download — generates CSV from actual gateway data
app.get('/api/templates/mid-config/:clientId', (req, res) => {
  const { querySql } = require('./src/db/connection');
  const clientId = parseInt(req.params.clientId, 10);

  const gateways = querySql(
    `SELECT gateway_id, gateway_alias, gateway_descriptor, lifecycle_state,
            processor_name, bank_name, mcc_code, mcc_label, acquiring_bin
     FROM gateways WHERE client_id = ? ORDER BY gateway_id`,
    [clientId]
  );

  const header = 'gateway_id,gateway_alias,current_status,processor_name,bank_name,mcc_code,mcc_label,acquiring_bin';
  const rows = gateways.map(gw => {
    const escapeCsv = (v) => {
      if (v == null) return '';
      const s = String(v);
      return s.includes(',') || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
    };
    return [
      gw.gateway_id,
      escapeCsv(gw.gateway_alias),
      gw.lifecycle_state,
      escapeCsv(gw.processor_name || ''),
      escapeCsv(gw.bank_name || ''),
      escapeCsv(gw.mcc_code || ''),
      escapeCsv(gw.mcc_label || ''),
      escapeCsv(gw.acquiring_bin || ''),
    ].join(',');
  });

  const csv = [header, ...rows].join('\n');
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', `attachment; filename="mid-config-${clientId}.csv"`);
  res.send(csv);
});

// Legacy static template fallback
app.get('/api/templates/mid-config', (req, res) => {
  res.download(path.join(__dirname, 'templates', 'mid-config-template.csv'));
});

// SPA fallback
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start
async function start() {
  await initializeDatabase();
  console.log('Database ready.');

  startScheduler();

  app.listen(PORT, () => {
    console.log(`BinRoute running on http://localhost:${PORT}`);
  });
}

start().catch(err => {
  console.error('Failed to start:', err);
  closeDb();
  process.exit(1);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down...');
  closeDb();
  process.exit(0);
});
process.on('SIGTERM', () => {
  closeDb();
  process.exit(0);
});
