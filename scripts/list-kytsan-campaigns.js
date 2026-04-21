/**
 * Dump all Kytsan (client 1) campaigns with current campaign_type,
 * grouped by current tag, sorted by order volume.
 */
const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  const camps = querySql(`
    SELECT
      c.campaign_id,
      COALESCE(c.campaign_type, '(untagged)') as type,
      c.campaign_name,
      COUNT(o.id) as orders,
      SUM(CASE WHEN o.billing_cycle=0 THEN 1 ELSE 0 END) as initials,
      SUM(CASE WHEN o.billing_cycle=0 AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as init_approved,
      SUM(CASE WHEN o.billing_cycle >= 1 THEN 1 ELSE 0 END) as rebills
    FROM campaigns c
    LEFT JOIN orders o ON o.client_id=c.client_id AND o.campaign_id=c.campaign_id
    WHERE c.client_id = 1
    GROUP BY c.campaign_id
    ORDER BY type, orders DESC
  `);

  // Group by type
  const groups = {};
  for (const c of camps) {
    if (!groups[c.type]) groups[c.type] = [];
    groups[c.type].push(c);
  }

  const order = ['reprocessing', 'recovery', 'rebill', '(untagged)'];
  const typeOrder = [...order, ...Object.keys(groups).filter(t => !order.includes(t))];

  for (const type of typeOrder) {
    if (!groups[type] || groups[type].length === 0) continue;
    console.log('='.repeat(100));
    console.log(type.toUpperCase() + ' — ' + groups[type].length + ' campaigns');
    console.log('='.repeat(100));
    console.log('  ID   | Orders  | Inits   | Appr%  | Rebills | Name');
    console.log('  -----+---------+---------+--------+---------+' + '-'.repeat(60));
    for (const c of groups[type]) {
      const appr = c.initials > 0 ? ((c.init_approved / c.initials) * 100).toFixed(1) + '%' : '  n/a';
      const name = (c.campaign_name || '(no name)').substring(0, 60);
      console.log(
        '  ' + String(c.campaign_id).padStart(4) +
        ' | ' + String(c.orders).padStart(7) +
        ' | ' + String(c.initials).padStart(7) +
        ' | ' + String(appr).padStart(6) +
        ' | ' + String(c.rebills).padStart(7) +
        ' | ' + name
      );
    }
    console.log();
  }

  console.log('='.repeat(100));
  console.log('TOTALS: ' + camps.length + ' campaigns');
  Object.entries(groups).forEach(([t, cs]) => {
    const totalOrders = cs.reduce((s, c) => s + c.orders, 0);
    console.log('  ' + t + ': ' + cs.length + ' campaigns, ' + totalOrders + ' orders');
  });

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
