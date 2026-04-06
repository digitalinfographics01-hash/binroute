const express = require('express');
const { querySql, runSql, saveDb } = require('../db/connection');
const router = express.Router();

// ── Unmatched BINs (cross-client) ──

// GET /api/master-bins/unmatched
router.get('/unmatched', (req, res) => {
  const rows = querySql(`
    SELECT
      o.cc_first_6 as bin,
      COUNT(*) as order_count,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as approved,
      ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) * 100.0 /
        NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as approval_rate,
      o.cc_type as card_network,
      b.issuer_bank,
      b.card_type,
      b.card_level,
      b.is_prepaid,
      CASE WHEN b.bin IS NULL THEN 'no_entry' ELSE 'missing_fields' END as gap_type,
      GROUP_CONCAT(DISTINCT c.name) as client_names
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN clients c ON o.client_id = c.id
    WHERE o.is_test = 0 AND o.is_internal_test = 0
    AND (b.bin IS NULL OR b.issuer_bank IS NULL OR b.card_type IS NULL OR b.card_level IS NULL)
    GROUP BY o.cc_first_6
    ORDER BY order_count DESC
  `);
  res.json(rows);
});

// ── All BINs (cross-client) ──

// GET /api/master-bins/all
router.get('/all', (req, res) => {
  const rows = querySql(`
    SELECT
      b.bin, b.issuer_bank, b.card_brand, b.card_type, b.card_level,
      b.is_prepaid, b.source, b.last_updated,
      COUNT(o.id) as order_count,
      ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) * 100.0 /
        NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as approval_rate,
      COUNT(DISTINCT o.client_id) as client_count
    FROM bin_lookup b
    LEFT JOIN orders o ON o.cc_first_6 = b.bin AND o.is_test = 0 AND o.is_internal_test = 0
    GROUP BY b.bin
    ORDER BY order_count DESC
  `);
  res.json(rows);
});

// ── Upsert BIN (no client required) ──

// PUT /api/master-bins/:bin
router.put('/:bin', (req, res) => {
  const bin = req.params.bin;
  const { issuer_bank, card_type, card_level, card_brand } = req.body;
  const is_prepaid = req.body.is_prepaid ? 1 : 0;

  const existing = querySql('SELECT * FROM bin_lookup WHERE bin = ?', [bin]);

  if (existing.length > 0) {
    runSql(
      "UPDATE bin_lookup SET issuer_bank=?, card_type=?, card_level=?, is_prepaid=?, source='manual_entry', last_updated=CURRENT_TIMESTAMP WHERE bin=?",
      [issuer_bank, card_type, card_level, is_prepaid, bin]
    );
  } else {
    // Resolve card_brand from any client's orders if not provided
    let resolvedBrand = card_brand || null;
    if (!resolvedBrand) {
      const orderRow = querySql(
        'SELECT cc_type FROM orders WHERE cc_first_6 = ? LIMIT 1',
        [bin]
      );
      if (orderRow.length > 0) {
        resolvedBrand = orderRow[0].cc_type;
      }
    }

    runSql(
      "INSERT INTO bin_lookup (bin, issuer_bank, card_brand, card_type, card_level, is_prepaid, source) VALUES (?, ?, ?, ?, ?, ?, 'manual_entry')",
      [bin, issuer_bank, resolvedBrand, card_type, card_level, is_prepaid]
    );
  }

  saveDb();
  const updated = querySql('SELECT * FROM bin_lookup WHERE bin = ?', [bin])[0];
  res.json(updated);
});

// ── Unmatched Count (sidebar badge) ──

// GET /api/master-bins/unmatched-count
router.get('/unmatched-count', (req, res) => {
  const row = querySql(`
    SELECT COUNT(DISTINCT o.cc_first_6) as count, COUNT(*) as order_count
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.is_test = 0 AND o.is_internal_test = 0
    AND (b.bin IS NULL OR b.issuer_bank IS NULL OR b.card_type IS NULL OR b.card_level IS NULL)
  `);
  res.json(row[0]);
});

module.exports = router;
