const express = require('express');
const path = require('path');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const crypto = require('crypto');
const { initDb, closeDb } = require('./src/db/connection');
const { initializeDatabase } = require('./src/db/schema');
const { startScheduler } = require('./src/scheduler/jobs');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Session middleware
app.use(session({
  secret: process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex'),
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    maxAge: 24 * 60 * 60 * 1000, // 24 hours
  },
}));

// Auth routes (before auth middleware)
app.post('/api/auth/login', (req, res) => {
  const { username, password } = req.body;
  const { querySql } = require('./src/db/connection');

  const user = querySql('SELECT * FROM users WHERE username = ?', [username])[0];
  if (!user || !bcrypt.compareSync(password, user.password_hash)) {
    return res.json({ success: false, message: 'Invalid username or password' });
  }

  req.session.userId = user.id;
  req.session.username = user.username;
  req.session.role = user.role || 'admin';

  // Load assigned clients for non-admin users
  if (user.role && user.role !== 'admin') {
    const rows = querySql('SELECT client_id FROM user_clients WHERE user_id = ?', [user.id]);
    req.session.clientIds = rows.map(r => r.client_id);
  } else {
    req.session.clientIds = null; // null = all clients
  }

  res.json({ success: true });
});

app.post('/api/auth/logout', (req, res) => {
  req.session.destroy();
  res.json({ success: true });
});

app.get('/api/auth/check', (req, res) => {
  if (req.session.userId) {
    res.json({
      authenticated: true,
      username: req.session.username,
      role: req.session.role || 'admin',
      clientIds: req.session.clientIds || null,
    });
  } else {
    res.json({ authenticated: false });
  }
});

// Login page (public)
app.get('/login', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

// Auth middleware — protect everything except login
app.use((req, res, next) => {
  // Allow login page assets
  if (req.path === '/login' || req.path.startsWith('/api/auth/')) {
    return next();
  }
  // Check session
  if (!req.session.userId) {
    // API requests get 401, page requests redirect to login
    if (req.path.startsWith('/api/')) {
      return res.status(401).json({ error: 'Not authenticated' });
    }
    return res.redirect('/login');
  }
  next();
});

// Attach user context to every authenticated request
app.use((req, res, next) => {
  req.userRole = req.session.role || 'admin';
  req.userClientIds = req.session.clientIds || null; // null = all (admin)
  next();
});

// RBAC helpers
function requireRole(...roles) {
  return (req, res, next) => {
    if (!roles.includes(req.userRole)) {
      return res.status(403).json({ error: 'Forbidden: insufficient permissions' });
    }
    next();
  };
}

function requireClientAccess(req, res, next) {
  if (req.userRole === 'admin') return next();
  const clientId = parseInt(req.params.clientId || req.params[0], 10);
  if (!clientId) return next(); // non-client-scoped route
  if (!req.userClientIds || !req.userClientIds.includes(clientId)) {
    return res.status(403).json({ error: 'Forbidden: no access to this client' });
  }
  next();
}

// Static files (after auth middleware)
app.use(express.static(path.join(__dirname, 'public')));

// CORS for development
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE');
  next();
});

// API Routes — admin-only routes
app.use('/api/master-bins', requireRole('admin'), require('./src/routes/master-bins'));
app.use('/api/network', requireRole('admin'), require('./src/routes/network-analysis'));
app.use('/api/actions', requireRole('admin'), require('./src/routes/actions'));
app.use('/api/users', requireRole('admin'), require('./src/routes/users'));

// API Routes — client-scoped routes (check client access)
app.use('/api/dashboard', requireClientAccess, require('./src/routes/dashboard'));
app.use('/api/config', require('./src/routes/config'));
app.use('/api/recommendations', requireClientAccess, require('./src/routes/recommendations'));
app.use('/api/lifecycle', requireClientAccess, require('./src/routes/lifecycle'));
app.use('/api/products', requireClientAccess, require('./src/routes/products'));
app.use('/api/analytics', requireClientAccess, require('./src/routes/analytics'));
app.use('/api/bins', requireClientAccess, require('./src/routes/bins'));
app.use('/api/implementations', requireClientAccess, require('./src/routes/playbook-implementations'));

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

  // Preload analytics cache from DB into memory so GET requests work immediately
  try {
    const { querySql } = require('./src/db/connection');
    const rows = querySql('SELECT client_id, output_type, cache_key, result_json FROM analytics_cache WHERE result_json IS NOT NULL');
    if (rows.length > 0) {
      const { preloadCache } = require('./src/analytics/engine');
      let loaded = 0;
      for (const row of rows) {
        try {
          const data = JSON.parse(row.result_json);
          preloadCache(row.client_id, row.output_type, row.cache_key, data);
          loaded++;
        } catch (e) { /* skip bad entries */ }
      }
      console.log(`[Cache] Preloaded ${loaded} analytics entries from DB.`);
    }
  } catch (e) {
    console.log('[Cache] Preload skipped:', e.message);
  }

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
