const express = require('express');
const { querySql, queryOneSql, runSql, saveDb, transaction } = require('../db/connection');
const router = express.Router();

// ── GET /api/products/:clientId — all products with assignments + order counts ──

router.get('/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  // Get all products from catalog
  const products = querySql(
    `SELECT pc.product_id, pc.product_name, pc.last_synced,
            pga.product_group_id, pga.product_type,
            pg.group_name, pg.product_sequence
     FROM products_catalog pc
     LEFT JOIN product_group_assignments pga
       ON pga.client_id = pc.client_id AND pga.product_id = pc.product_id
     LEFT JOIN product_groups pg
       ON pg.id = pga.product_group_id AND pg.client_id = pc.client_id
     WHERE pc.client_id = ?
     ORDER BY pc.product_id`,
    [clientId]
  );

  // Get order counts per product_id from orders table
  const counts = querySql(
    `SELECT p.value as product_id, COUNT(*) as order_count
     FROM orders o, json_each(o.product_ids) p
     WHERE o.client_id = ? AND o.is_test = 0
     GROUP BY p.value`,
    [clientId]
  );
  const countMap = {};
  for (const c of counts) countMap[c.product_id] = c.order_count;

  // Merge counts into products
  const result = products.map(p => ({
    ...p,
    order_count: countMap[String(p.product_id)] || 0,
  }));

  // Sort by order_count DESC
  result.sort((a, b) => b.order_count - a.order_count);

  res.json(result);
});

// ── GET /api/products/:clientId/unassigned-count ──

router.get('/:clientId/unassigned-count', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const row = queryOneSql(
    `SELECT COUNT(*) as cnt FROM products_catalog pc
     WHERE pc.client_id = ?
       AND NOT EXISTS (
         SELECT 1 FROM product_group_assignments pga
         WHERE pga.client_id = pc.client_id AND pga.product_id = pc.product_id
       )`,
    [clientId]
  );
  res.json({ count: row?.cnt || 0 });
});

// ── POST /api/products/:clientId/seed — pre-populate from existing order data ──

router.post('/:clientId/seed', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  // Find all distinct product_ids from orders
  const products = querySql(
    `SELECT DISTINCT p.value as product_id, COUNT(*) as order_count
     FROM orders o, json_each(o.product_ids) p
     WHERE o.client_id = ? AND o.is_test = 0
     GROUP BY p.value
     ORDER BY order_count DESC`,
    [clientId]
  );

  let inserted = 0;
  transaction(() => {
    for (const p of products) {
      const exists = queryOneSql(
        'SELECT 1 FROM products_catalog WHERE client_id = ? AND product_id = ?',
        [clientId, String(p.product_id)]
      );
      if (!exists) {
        runSql(
          `INSERT INTO products_catalog (client_id, product_id, product_name)
           VALUES (?, ?, ?)`,
          [clientId, String(p.product_id), 'Unknown - sync to get name']
        );
        inserted++;
      }
    }
  });

  res.json({ success: true, found: products.length, inserted });
});

// ── POST /api/products/:clientId/sync — pull from Sticky product_index ──

router.post('/:clientId/sync', async (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const client = queryOneSql('SELECT * FROM clients WHERE id = ?', [clientId]);
  if (!client) return res.status(404).json({ error: 'Client not found' });

  try {
    const StickyClient = require('../api/sticky-client');
    const sticky = new StickyClient({
      baseUrl: client.sticky_base_url,
      username: client.sticky_username,
      password: client.sticky_password,
    });

    const data = await sticky.productIndex();

    if (!data || data.response_code !== '100') {
      return res.status(502).json({ error: `Sticky API error: ${data?.response_code || 'no response'}` });
    }

    const products = data.products || [];

    let updated = 0;
    transaction(() => {
      for (const p of products) {
        if (!p.product_id) continue;
        const exists = queryOneSql(
          'SELECT 1 FROM products_catalog WHERE client_id = ? AND product_id = ?',
          [clientId, String(p.product_id)]
        );
        if (exists) {
          runSql(
            `UPDATE products_catalog SET product_name = ?, last_synced = datetime('now')
             WHERE client_id = ? AND product_id = ?`,
            [p.product_name || null, clientId, String(p.product_id)]
          );
        } else {
          runSql(
            `INSERT INTO products_catalog (client_id, product_id, product_name, last_synced)
             VALUES (?, ?, ?, datetime('now'))`,
            [clientId, String(p.product_id), p.product_name || null]
          );
        }
        updated++;
      }
    });

    // Update sync state
    const syncExists = queryOneSql(
      "SELECT 1 FROM sync_state WHERE client_id = ? AND sync_type = 'product_sync'",
      [clientId]
    );
    if (syncExists) {
      runSql(
        "UPDATE sync_state SET last_sync_at = datetime('now'), records_synced = ?, status = 'complete' WHERE client_id = ? AND sync_type = 'product_sync'",
        [updated, clientId]
      );
    } else {
      runSql(
        "INSERT INTO sync_state (client_id, sync_type, last_sync_at, records_synced, status) VALUES (?, 'product_sync', datetime('now'), ?, 'complete')",
        [clientId, updated]
      );
    }
    saveDb();

    res.json({ success: true, synced: updated });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── PUT /api/products/:clientId/:productId — update group + type ──

router.put('/:clientId/:productId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const productId = String(req.params.productId);
  const { product_group, product_type } = req.body;

  // Validate: product_type requires product_group
  if (product_type && (!product_group || product_group.trim() === '')) {
    return res.status(400).json({ error: 'Please enter a group name first' });
  }

  const groupName = product_group ? product_group.trim() : null;

  try {
    transaction(() => {
      if (!groupName) {
        // Remove assignment if group cleared
        runSql(
          'DELETE FROM product_group_assignments WHERE client_id = ? AND product_id = ?',
          [clientId, productId]
        );
      } else {
        // Find or create group
        let group = queryOneSql(
          'SELECT id FROM product_groups WHERE client_id = ? AND group_name = ?',
          [clientId, groupName]
        );
        if (!group) {
          runSql(
            'INSERT INTO product_groups (client_id, group_name) VALUES (?, ?)',
            [clientId, groupName]
          );
          group = queryOneSql(
            'SELECT id FROM product_groups WHERE client_id = ? AND group_name = ?',
            [clientId, groupName]
          );
        }

        // Upsert assignment
        const existing = queryOneSql(
          'SELECT id FROM product_group_assignments WHERE client_id = ? AND product_id = ?',
          [clientId, productId]
        );
        if (existing) {
          runSql(
            `UPDATE product_group_assignments SET product_group_id = ?, product_type = ?
             WHERE client_id = ? AND product_id = ?`,
            [group.id, product_type || null, clientId, productId]
          );
        } else {
          runSql(
            `INSERT INTO product_group_assignments (client_id, product_id, product_group_id, product_type)
             VALUES (?, ?, ?, ?)`,
            [clientId, productId, group.id, product_type || null]
          );
        }

        // Update group timestamp
        runSql(
          "UPDATE product_groups SET updated_at = datetime('now') WHERE id = ?",
          [group.id]
        );
      }
    });

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── PUT /api/products/:clientId/group/:groupId/sequence — update product_sequence ──

router.put('/:clientId/group/:groupId/sequence', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const groupId = parseInt(req.params.groupId, 10);
  const { product_sequence } = req.body;

  if (!['main', 'upsell', null].includes(product_sequence)) {
    return res.status(400).json({ error: 'product_sequence must be "main", "upsell", or null' });
  }

  const { getDb } = require('../db/connection');
  const db = getDb();
  const stmt = db.prepare("UPDATE product_groups SET product_sequence = ?, updated_at = datetime('now') WHERE id = ? AND client_id = ?");
  stmt.run([product_sequence, groupId, clientId]);
  stmt.free();
  saveDb();
  res.json({ success: true });
});

// ── GET /api/products/:clientId/groups — all product groups with sequence ──

router.get('/:clientId/groups', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const groups = querySql(
    'SELECT id, group_name, product_sequence, created_at, updated_at FROM product_groups WHERE client_id = ? ORDER BY group_name',
    [clientId]
  );
  res.json(groups);
});

module.exports = router;
