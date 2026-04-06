const express = require('express');
const { querySql, queryOneSql, runSql, saveDb, transaction } = require('../db/connection');
const router = express.Router();

// ── Clients ──

// GET /api/config/clients
router.get('/clients', (req, res) => {
  const clients = querySql('SELECT id, name, sticky_base_url, alert_threshold, analysis_window_days, created_at FROM clients ORDER BY name');
  res.json(clients);
});

// POST /api/config/clients
router.post('/clients', (req, res) => {
  const { name, sticky_base_url, sticky_username, sticky_password, alert_threshold } = req.body;
  if (!name || !sticky_base_url || !sticky_username || !sticky_password) {
    return res.status(400).json({ error: 'Missing required fields' });
  }
  runSql(
    'INSERT INTO clients (name, sticky_base_url, sticky_username, sticky_password, alert_threshold) VALUES (?, ?, ?, ?, ?)',
    [name, sticky_base_url, sticky_username, sticky_password, alert_threshold || 5.0]
  );
  saveDb();
  const client = querySql('SELECT * FROM clients WHERE name = ?', [name])[0];
  res.json(client);
});

// ── Gateways / MIDs ──

// GET /api/config/gateways/:clientId
router.get('/gateways/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const gateways = querySql(
    'SELECT * FROM gateways WHERE client_id = ? ORDER BY gateway_id',
    [clientId]
  );
  res.json(gateways);
});

// PUT /api/config/gateways/:clientId/:gatewayId — update manual fields
router.put('/gateways/:clientId/:gatewayId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const gatewayId = parseInt(req.params.gatewayId, 10);
  const { processor_name, bank_name, mcc_code, mcc_label, acquiring_bin, lifecycle_state } = req.body;

  const existing = querySql(
    'SELECT * FROM gateways WHERE client_id = ? AND gateway_id = ?',
    [clientId, gatewayId]
  );
  if (existing.length === 0) return res.status(404).json({ error: 'Gateway not found' });

  const old = existing[0];
  const fields = { processor_name, bank_name, mcc_code, mcc_label, acquiring_bin, lifecycle_state };

  transaction(() => {
    for (const [field, value] of Object.entries(fields)) {
      if (value !== undefined && value !== old[field]) {
        runSql(`UPDATE gateways SET ${field} = ?, updated_at = datetime('now') WHERE client_id = ? AND gateway_id = ?`,
          [value, clientId, gatewayId]);

        // Log change
        runSql(
          'INSERT INTO change_log (client_id, entity_type, entity_id, action, field_name, old_value, new_value, changed_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
          [clientId, 'gateway', gatewayId, 'update', field, old[field], value, req.body.changed_by || 'user']
        );
      }
    }
  });

  const updated = querySql('SELECT * FROM gateways WHERE client_id = ? AND gateway_id = ?', [clientId, gatewayId])[0];
  res.json(updated);
});

// PUT /api/config/gateways/:clientId/:gatewayId/exclude — toggle exclude from analysis
router.put('/gateways/:clientId/:gatewayId/exclude', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const gatewayId = parseInt(req.params.gatewayId, 10);
  const { exclude_from_analysis } = req.body;

  if (exclude_from_analysis !== 0 && exclude_from_analysis !== 1) {
    return res.status(400).json({ error: 'exclude_from_analysis must be 0 or 1' });
  }

  const existing = querySql(
    'SELECT * FROM gateways WHERE client_id = ? AND gateway_id = ?',
    [clientId, gatewayId]
  );
  if (existing.length === 0) return res.status(404).json({ error: 'Gateway not found' });

  const old = existing[0];
  transaction(() => {
    runSql(
      `UPDATE gateways SET exclude_from_analysis = ?, updated_at = datetime('now') WHERE client_id = ? AND gateway_id = ?`,
      [exclude_from_analysis, clientId, gatewayId]
    );
    runSql(
      'INSERT INTO change_log (client_id, entity_type, entity_id, action, field_name, old_value, new_value, changed_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      [clientId, 'gateway', gatewayId, 'update', 'exclude_from_analysis', String(old.exclude_from_analysis || 0), String(exclude_from_analysis), req.body.changed_by || 'user']
    );
  });
  saveDb();

  const updated = querySql('SELECT * FROM gateways WHERE client_id = ? AND gateway_id = ?', [clientId, gatewayId])[0];
  res.json(updated);
});

// DELETE /api/config/gateways/:clientId/below/:minId — remove gateways below a threshold
router.delete('/gateways/:clientId/below/:minId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const minId = parseInt(req.params.minId, 10);
  try {
    const before = queryOneSql('SELECT COUNT(*) as cnt FROM gateways WHERE client_id = ?', [clientId]);
    runSql('DELETE FROM gateways WHERE client_id = ? AND gateway_id < ?', [clientId, minId]);
    saveDb();
    const after = queryOneSql('SELECT COUNT(*) as cnt FROM gateways WHERE client_id = ?', [clientId]);
    res.json({ success: true, before: before.cnt, after: after.cnt, removed: before.cnt - after.cnt });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/config/gateways/:clientId/bulk — CSV/JSON bulk upload
router.post('/gateways/:clientId/bulk', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const { gateways } = req.body;
  if (!Array.isArray(gateways)) return res.status(400).json({ error: 'Expected gateways array' });

  let updated = 0;
  transaction(() => {
    for (const gw of gateways) {
      if (!gw.gateway_id) continue;
      runSql(`
        UPDATE gateways SET
          processor_name = COALESCE(?, processor_name),
          bank_name = COALESCE(?, bank_name),
          mcc_code = COALESCE(?, mcc_code),
          mcc_label = COALESCE(?, mcc_label),
          acquiring_bin = COALESCE(?, acquiring_bin),
          updated_at = datetime('now')
        WHERE client_id = ? AND gateway_id = ?
      `, [gw.processor_name, gw.bank_name, gw.mcc_code, gw.mcc_label, gw.acquiring_bin, clientId, gw.gateway_id]);
      updated++;
    }
  });

  res.json({ updated });
});

// GET /api/config/gateways/:clientId/incomplete — count of incomplete MIDs
router.get('/gateways/:clientId/incomplete', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const rows = querySql(`
    SELECT gateway_id, gateway_descriptor, gateway_alias,
      processor_name, bank_name, mcc_code, mcc_label
    FROM gateways
    WHERE client_id = ? AND lifecycle_state != 'closed'
      AND (processor_name IS NULL OR bank_name IS NULL OR mcc_code IS NULL)
    ORDER BY gateway_id
  `, [clientId]);
  res.json({ count: rows.length, gateways: rows });
});

// ── TX Type Rules ──

// GET /api/config/tx-rules/:clientId
router.get('/tx-rules/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  res.json(querySql('SELECT * FROM tx_type_rules WHERE client_id = ? ORDER BY campaign_id, product_id', [clientId]));
});

// POST /api/config/tx-rules/:clientId
router.post('/tx-rules/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const { campaign_id, product_id, assigned_type, is_cp_simulation, notes } = req.body;
  if (!assigned_type) return res.status(400).json({ error: 'assigned_type required' });

  runSql(
    'INSERT INTO tx_type_rules (client_id, campaign_id, product_id, assigned_type, is_cp_simulation, notes) VALUES (?, ?, ?, ?, ?, ?)',
    [clientId, campaign_id || null, product_id || null, assigned_type, is_cp_simulation ? 1 : 0, notes || null]
  );
  saveDb();
  res.json({ success: true });
});

// DELETE /api/config/tx-rules/:ruleId
router.delete('/tx-rules/:ruleId', (req, res) => {
  runSql('DELETE FROM tx_type_rules WHERE id = ?', [parseInt(req.params.ruleId, 10)]);
  saveDb();
  res.json({ success: true });
});

// ── Cycle Groups ──

// GET /api/config/cycle-groups/:clientId
router.get('/cycle-groups/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  res.json(querySql('SELECT * FROM cycle_groups WHERE client_id = ? ORDER BY min_cycle', [clientId]));
});

// POST /api/config/cycle-groups/:clientId
router.post('/cycle-groups/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const { group_name, min_cycle, max_cycle } = req.body;
  if (!group_name || min_cycle === undefined) return res.status(400).json({ error: 'group_name and min_cycle required' });

  runSql('INSERT INTO cycle_groups (client_id, group_name, min_cycle, max_cycle) VALUES (?, ?, ?, ?)',
    [clientId, group_name, min_cycle, max_cycle || null]);
  saveDb();
  res.json({ success: true });
});

// ── Change Log ──

// GET /api/config/changelog/:clientId
router.get('/changelog/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const limit = parseInt(req.query.limit, 10) || 50;
  res.json(querySql(
    'SELECT * FROM change_log WHERE client_id = ? ORDER BY created_at DESC LIMIT ?',
    [clientId, limit]
  ));
});

// ── Campaigns ──

// GET /api/config/campaigns/:clientId
router.get('/campaigns/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  res.json(querySql('SELECT * FROM campaigns WHERE client_id = ? ORDER BY campaign_id', [clientId]));
});

// ── Client Settings ──

// PUT /api/config/clients/:clientId — update client settings
router.put('/clients/:clientId', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);
  const client = querySql('SELECT * FROM clients WHERE id = ?', [clientId])[0];
  if (!client) return res.status(404).json({ error: 'Client not found' });

  const { uses_cascade, sticky_domain, alert_threshold } = req.body;
  const updates = [];
  const params = [];

  if (uses_cascade !== undefined) {
    updates.push('uses_cascade = ?');
    params.push(uses_cascade ? 1 : 0);
  }
  if (sticky_domain !== undefined) {
    updates.push('sticky_domain = ?');
    params.push(sticky_domain);
  }
  if (alert_threshold !== undefined) {
    updates.push('alert_threshold = ?');
    params.push(alert_threshold);
  }

  if (updates.length > 0) {
    updates.push("updated_at = datetime('now')");
    runSql(`UPDATE clients SET ${updates.join(', ')} WHERE id = ?`, [...params, clientId]);
    saveDb();
  }

  const updated = querySql('SELECT id, name, sticky_base_url, uses_cascade, sticky_domain, alert_threshold FROM clients WHERE id = ?', [clientId])[0];
  res.json(updated);
});

// ── Onboarding Status ──

// GET /api/config/clients/:clientId/onboarding-status
// Returns checklist of onboarding steps with completion status
router.get('/clients/:clientId/onboarding-status', (req, res) => {
  const clientId = parseInt(req.params.clientId, 10);

  const client = querySql('SELECT id, name, uses_cascade FROM clients WHERE id = ?', [clientId])[0];
  if (!client) return res.status(404).json({ error: 'Client not found' });

  // 1. Client added — always true if we got here
  const clientAdded = true;

  // 2. Gateways synced
  const gwCount = querySql(
    'SELECT COUNT(*) as cnt FROM gateways WHERE client_id = ?', [clientId]
  )[0].cnt;
  const gatewaysSynced = gwCount > 0;

  // 3. Products synced
  const prodCount = querySql(
    'SELECT COUNT(*) as cnt FROM products_catalog WHERE client_id = ?', [clientId]
  )[0].cnt;
  const productsSynced = prodCount > 0;

  // 4. Product groups created + all products assigned
  const groupCount = querySql(
    'SELECT COUNT(*) as cnt FROM product_groups WHERE client_id = ?', [clientId]
  )[0].cnt;
  const unassignedCount = querySql(
    `SELECT COUNT(*) as cnt FROM products_catalog pc
     WHERE pc.client_id = ?
       AND NOT EXISTS (
         SELECT 1 FROM product_group_assignments pga
         WHERE pga.client_id = pc.client_id AND pga.product_id = pc.product_id
       )`,
    [clientId]
  )[0].cnt;
  const productsGrouped = groupCount > 0 && unassignedCount === 0 && prodCount > 0;

  // 5. Product sequence tagged (main vs upsell on all groups)
  const untaggedSequence = querySql(
    `SELECT COUNT(*) as cnt FROM product_groups
     WHERE client_id = ? AND (product_sequence IS NULL OR product_sequence = '')`,
    [clientId]
  )[0].cnt;
  const sequenceTagged = groupCount > 0 && untaggedSequence === 0;

  // 6. Campaign types tagged
  const totalCampaigns = querySql(
    'SELECT COUNT(*) as cnt FROM campaigns WHERE client_id = ?', [clientId]
  )[0].cnt;
  const untaggedCampaigns = querySql(
    `SELECT COUNT(*) as cnt FROM campaigns
     WHERE client_id = ? AND (campaign_type IS NULL OR campaign_type = '')`,
    [clientId]
  )[0].cnt;
  const campaignsTagged = totalCampaigns > 0 && untaggedCampaigns === 0;

  // 7. MID config uploaded (processor + bank on active gateways)
  const activeGateways = querySql(
    "SELECT COUNT(*) as cnt FROM gateways WHERE client_id = ? AND lifecycle_state != 'closed'",
    [clientId]
  )[0].cnt;
  const incompleteMids = querySql(
    `SELECT COUNT(*) as cnt FROM gateways
     WHERE client_id = ? AND lifecycle_state != 'closed'
       AND (processor_name IS NULL OR bank_name IS NULL)`,
    [clientId]
  )[0].cnt;
  const midConfigured = activeGateways > 0 && incompleteMids === 0;

  // 8. Orders synced (has order data)
  const orderCount = querySql(
    'SELECT COUNT(*) as cnt FROM orders WHERE client_id = ? AND is_test = 0',
    [clientId]
  )[0].cnt;
  const ordersSynced = orderCount > 0;

  // 9. Post-sync pipeline run (check if derived fields exist)
  const derivedCount = querySql(
    `SELECT COUNT(*) as cnt FROM orders
     WHERE client_id = ? AND is_test = 0
       AND derived_product_role IS NOT NULL`,
    [clientId]
  )[0].cnt;
  const pipelineRun = orderCount > 0 && derivedCount > 0;

  // 10. Classification verified — 0 unclassified among classifiable orders
  // Orders with product_group_id should have derived_product_role
  const unclassified = querySql(
    `SELECT COUNT(*) as cnt FROM orders
     WHERE client_id = ? AND is_test = 0 AND is_internal_test = 0
       AND order_status IN (2, 6, 7, 8)
       AND derived_product_role IS NULL`,
    [clientId]
  )[0].cnt;
  const classificationVerified = orderCount > 0 && derivedCount > 0 && unclassified === 0;

  // 11. Cascade detection — auto-detect from order data
  const cascadedOrders = querySql(
    'SELECT COUNT(*) as cnt FROM orders WHERE client_id = ? AND is_cascaded = 1',
    [clientId]
  )[0].cnt;

  // Auto-detect: if we have orders and some are cascaded, this client cascades
  let usesCascade = client.uses_cascade;
  if (orderCount > 0 && usesCascade === null) {
    // Auto-set based on data
    usesCascade = cascadedOrders > 0 ? 1 : 0;
    // Persist the auto-detected value
    runSql('UPDATE clients SET uses_cascade = ? WHERE id = ?', [usesCascade, clientId]);
    saveDb();
  }

  const cascadeConfigured = orderCount === 0 ? false : true; // can't know until we have orders

  // 12. Cascade CSV imported (only relevant if client cascades)
  let cascadeImported = true; // default true if no cascade
  let cascadeDetail = 'n/a';
  if (usesCascade === 1) {
    const withOrigGw = querySql(
      'SELECT COUNT(*) as cnt FROM orders WHERE client_id = ? AND is_cascaded = 1 AND original_gateway_id IS NOT NULL',
      [clientId]
    )[0].cnt;
    cascadeImported = cascadedOrders > 0 && withOrigGw > 0;
    cascadeDetail = `${withOrigGw}/${cascadedOrders} attributed`;
  } else if (usesCascade === 0) {
    cascadeDetail = 'no cascade detected';
  } else {
    cascadeDetail = 'sync orders first to detect';
  }

  // Overall readiness
  const steps = [
    { key: 'client_added',            label: 'Client added',                       done: clientAdded },
    { key: 'gateways_synced',         label: 'Gateways synced',                    done: gatewaysSynced,         detail: `${gwCount} gateways` },
    { key: 'products_synced',         label: 'Products synced',                    done: productsSynced,         detail: `${prodCount} products` },
    { key: 'products_grouped',        label: 'Product groups assigned',            done: productsGrouped,        detail: unassignedCount > 0 ? `${unassignedCount} unassigned` : `${groupCount} groups` },
    { key: 'sequence_tagged',         label: 'Product sequence tagged (main/upsell)', done: sequenceTagged,      detail: untaggedSequence > 0 ? `${untaggedSequence} untagged` : 'all tagged' },
    { key: 'campaigns_tagged',        label: 'Campaign types tagged',              done: campaignsTagged,        detail: untaggedCampaigns > 0 ? `${untaggedCampaigns}/${totalCampaigns} untagged` : `${totalCampaigns} campaigns` },
    { key: 'mid_configured',          label: 'MID config (processor/bank)',         done: midConfigured,          detail: incompleteMids > 0 ? `${incompleteMids} incomplete` : `${activeGateways} configured` },
    { key: 'orders_synced',           label: 'Order data synced',                  done: ordersSynced,           detail: orderCount > 0 ? `${orderCount.toLocaleString()} orders` : 'no orders' },
    { key: 'cascade_detected',       label: 'Cascade detection',                  done: cascadeConfigured,      detail: cascadeDetail },
    { key: 'pipeline_run',            label: 'Post-sync pipeline run',             done: pipelineRun,            detail: derivedCount > 0 ? `${derivedCount.toLocaleString()} classified` : 'not run' },
    { key: 'classification_verified', label: 'Classification verified (0 gaps)',   done: classificationVerified, detail: unclassified > 0 ? `${unclassified} unclassified` : 'verified' },
  ];

  // Add cascade import step only if client uses cascade
  if (usesCascade === 1) {
    // Insert after cascade_detected
    const cascadeIdx = steps.findIndex(s => s.key === 'cascade_detected');
    steps.splice(cascadeIdx + 1, 0, {
      key: 'cascade_imported', label: 'Cascade CSV data imported', done: cascadeImported, detail: cascadeDetail,
    });
  }

  const completedCount = steps.filter(s => s.done).length;
  const analyticsReady = classificationVerified;

  res.json({
    client: client.name,
    completedCount,
    totalSteps: steps.length,
    pct: Math.round(completedCount / steps.length * 100),
    analyticsReady,
    steps,
  });
});

module.exports = router;
