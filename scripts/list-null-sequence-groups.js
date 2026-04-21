/**
 * List all product groups with NULL product_sequence for clients 1-5,
 * so the user can classify them as main or upsell.
 */
const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  const clientNames = {};
  querySql('SELECT id, name FROM clients WHERE id IN (1,2,3,4,5)').forEach(r => clientNames[r.id] = r.name);

  for (const cid of [1, 2, 3, 4, 5]) {
    const groups = querySql(`
      SELECT pg.id as group_id, pg.group_name, pg.client_id,
             COUNT(DISTINCT pga.product_id) as product_count,
             GROUP_CONCAT(DISTINCT pga.product_type) as product_types
      FROM product_groups pg
      LEFT JOIN product_group_assignments pga ON pga.product_group_id = pg.id
      WHERE pg.client_id = ? AND pg.product_sequence IS NULL
      GROUP BY pg.id
      ORDER BY product_count DESC
    `, [cid]);

    if (groups.length === 0) continue;

    console.log('='.repeat(80));
    console.log('CLIENT ' + cid + ': ' + (clientNames[cid] || '') + ' — ' + groups.length + ' null-sequence groups');
    console.log('='.repeat(80));

    for (const g of groups) {
      console.log();
      console.log('Group ID ' + g.group_id + ': ' + (g.group_name || '(no name)'));
      console.log('  Products: ' + g.product_count + ' | Types: ' + (g.product_types || '(none)'));

      // Get sample products and their order counts
      const products = querySql(`
        SELECT pc.product_id, pc.product_name, pga.product_type,
               (SELECT COUNT(*) FROM orders o WHERE o.client_id = ? AND o.product_ids LIKE '%"' || pc.product_id || '"%') as order_count
        FROM product_group_assignments pga
        JOIN products_catalog pc ON pc.client_id = pga.client_id AND pc.product_id = pga.product_id
        WHERE pga.product_group_id = ?
        ORDER BY order_count DESC
        LIMIT 10
      `, [cid, g.group_id]);

      console.log('  Products in group:');
      products.forEach(p => {
        const name = (p.product_name || '(no name)').substring(0, 60);
        console.log('    [' + String(p.product_id).padStart(5) + '] ' + String(p.order_count).padStart(6) + ' orders  ' + (p.product_type || '(no type)').padEnd(15) + '  ' + name);
      });
    }
    console.log();
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
