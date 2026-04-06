const express = require('express');
const bcrypt = require('bcryptjs');
const { querySql, queryOneSql, runSql, saveDb, transaction } = require('../db/connection');
const router = express.Router();

// GET /api/users — list all users with assigned clients
router.get('/', (req, res) => {
  const users = querySql(`
    SELECT u.id, u.username, u.role, u.created_at,
           GROUP_CONCAT(uc.client_id) as client_ids
    FROM users u
    LEFT JOIN user_clients uc ON uc.user_id = u.id
    GROUP BY u.id
    ORDER BY u.username
  `);
  const clients = querySql('SELECT id, name FROM clients ORDER BY name');
  res.json({
    users: users.map(u => ({
      ...u,
      client_ids: u.client_ids ? u.client_ids.split(',').map(Number) : [],
    })),
    clients,
  });
});

// POST /api/users — create user
router.post('/', (req, res) => {
  const { username, password, role, client_ids } = req.body;

  if (!username || !password) {
    return res.status(400).json({ error: 'Username and password required' });
  }
  if (!['admin', 'manager', 'client'].includes(role)) {
    return res.status(400).json({ error: 'Role must be admin, manager, or client' });
  }
  if (role !== 'admin' && (!client_ids || client_ids.length === 0)) {
    return res.status(400).json({ error: 'Non-admin users must have at least one client assigned' });
  }
  if (role === 'client' && client_ids && client_ids.length > 1) {
    return res.status(400).json({ error: 'Client users can only have one client assigned' });
  }

  const existing = queryOneSql('SELECT id FROM users WHERE username = ?', [username]);
  if (existing) {
    return res.status(400).json({ error: 'Username already exists' });
  }

  const hash = bcrypt.hashSync(password, 10);

  transaction(() => {
    runSql('INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)', [username, hash, role]);
    const user = queryOneSql('SELECT id FROM users WHERE username = ?', [username]);
    if (role !== 'admin' && client_ids) {
      for (const cid of client_ids) {
        runSql('INSERT INTO user_clients (user_id, client_id) VALUES (?, ?)', [user.id, cid]);
      }
    }
  });
  saveDb();

  res.json({ success: true });
});

// PUT /api/users/:id — update user
router.put('/:id', (req, res) => {
  const userId = parseInt(req.params.id, 10);
  const { username, password, role, client_ids } = req.body;

  const user = queryOneSql('SELECT * FROM users WHERE id = ?', [userId]);
  if (!user) return res.status(404).json({ error: 'User not found' });

  if (role && !['admin', 'manager', 'client'].includes(role)) {
    return res.status(400).json({ error: 'Role must be admin, manager, or client' });
  }

  const newRole = role || user.role;
  if (newRole !== 'admin' && (!client_ids || client_ids.length === 0)) {
    return res.status(400).json({ error: 'Non-admin users must have at least one client assigned' });
  }
  if (newRole === 'client' && client_ids && client_ids.length > 1) {
    return res.status(400).json({ error: 'Client users can only have one client assigned' });
  }

  transaction(() => {
    if (username && username !== user.username) {
      runSql('UPDATE users SET username = ? WHERE id = ?', [username, userId]);
    }
    if (password) {
      const hash = bcrypt.hashSync(password, 10);
      runSql('UPDATE users SET password_hash = ? WHERE id = ?', [hash, userId]);
    }
    if (role) {
      runSql('UPDATE users SET role = ? WHERE id = ?', [role, userId]);
    }

    // Update client assignments
    if (client_ids !== undefined) {
      runSql('DELETE FROM user_clients WHERE user_id = ?', [userId]);
      if (newRole !== 'admin' && client_ids) {
        for (const cid of client_ids) {
          runSql('INSERT INTO user_clients (user_id, client_id) VALUES (?, ?)', [userId, cid]);
        }
      }
    }
  });
  saveDb();

  res.json({ success: true });
});

// DELETE /api/users/:id — delete user
router.delete('/:id', (req, res) => {
  const userId = parseInt(req.params.id, 10);

  // Prevent self-delete
  if (userId === req.session.userId) {
    return res.status(400).json({ error: 'Cannot delete your own account' });
  }

  const user = queryOneSql('SELECT id FROM users WHERE id = ?', [userId]);
  if (!user) return res.status(404).json({ error: 'User not found' });

  transaction(() => {
    runSql('DELETE FROM user_clients WHERE user_id = ?', [userId]);
    runSql('DELETE FROM users WHERE id = ?', [userId]);
  });
  saveDb();

  res.json({ success: true });
});

module.exports = router;
