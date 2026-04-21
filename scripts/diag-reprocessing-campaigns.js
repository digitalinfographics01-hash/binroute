/**
 * Diagnostic: find reprocessing campaigns in supplements clients.
 *
 * Flags campaigns that look like they might be dedicated salvage/reprocessing:
 * - Name contains reprocess/retry/salvage/save/v2/alt
 * - Low approval rate (<30% suggests salvage)
 * - Shares products with another campaign from same client
 */
const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  // 1. Check existing campaign_type values
  console.log('='.repeat(70));
  console.log('Existing campaign_type distribution');
  console.log('='.repeat(70));
  const typeDist = querySql(`
    SELECT client_id, COALESCE(campaign_type, '(null)') as ct, COUNT(*) as n
    FROM campaigns WHERE client_id IN (1,2,3)
    GROUP BY client_id, ct
    ORDER BY client_id, n DESC
  `);
  typeDist.forEach(r => console.log('  client ' + r.client_id + ': ' + r.ct + ' = ' + r.n));
  console.log();

  // 2. Campaigns per client with order volume and approval rates
  for (const cid of [1, 2, 3]) {
    const clientName = querySql('SELECT name FROM clients WHERE id=?', [cid])[0]?.name || 'Unknown';
    console.log('='.repeat(70));
    console.log('CLIENT ' + cid + ': ' + clientName);
    console.log('='.repeat(70));

    // All campaigns with order stats (joined via campaign_id)
    const camps = querySql(`
      SELECT
        c.campaign_id,
        c.campaign_name,
        COALESCE(c.campaign_type, '') as campaign_type,
        COUNT(o.id) as orders,
        SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved,
        SUM(CASE WHEN o.billing_cycle=0 THEN 1 ELSE 0 END) as initials,
        SUM(CASE WHEN o.billing_cycle=0 AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as initial_approved
      FROM campaigns c
      LEFT JOIN orders o ON o.client_id=c.client_id AND o.campaign_id=c.campaign_id
      WHERE c.client_id=?
      GROUP BY c.campaign_id
      ORDER BY orders DESC
    `, [cid]);

    console.log('Total campaigns: ' + camps.length);
    console.log('Campaigns with orders: ' + camps.filter(c => c.orders > 0).length);
    console.log();

    // Show top 20 by volume + any suspicious-name campaigns
    console.log('Top campaigns by volume (showing columns: id | name | orders | initials | init_approval%):');
    const topByVol = camps.filter(c => c.orders > 0).slice(0, 25);
    topByVol.forEach(c => {
      const initAppr = c.initials > 0 ? ((c.initial_approved / c.initials) * 100).toFixed(1) : 'n/a';
      const nameShort = (c.campaign_name || '').substring(0, 50);
      console.log('  ' + String(c.campaign_id).padStart(5) + ' | ' +
                  nameShort.padEnd(52) + ' | ' +
                  String(c.orders).padStart(7) + ' | ' +
                  String(c.initials).padStart(6) + ' | ' +
                  String(initAppr).padStart(6) + '%' +
                  (c.campaign_type ? ' [' + c.campaign_type + ']' : ''));
    });
    console.log();

    // Keyword search
    const keywords = ['reprocess', 'retry', 'salvage', 'save', 'recover', 'v2', 'alt', 'backup', 'decline'];
    const suspicious = camps.filter(c => {
      if (!c.campaign_name) return false;
      const name = c.campaign_name.toLowerCase();
      return keywords.some(k => name.includes(k));
    });
    if (suspicious.length > 0) {
      console.log('>>> Name-based suspects (contains reprocess/retry/salvage/etc):');
      suspicious.forEach(c => {
        const initAppr = c.initials > 0 ? ((c.initial_approved / c.initials) * 100).toFixed(1) : 'n/a';
        console.log('  ' + c.campaign_id + ' | ' + c.campaign_name + ' | orders=' + c.orders + ' | init=' + c.initials + ' | init_approval=' + initAppr + '%');
      });
      console.log();
    }

    // Approval rate outliers (low approval on initials → likely salvage)
    const lowApproval = camps.filter(c => c.initials >= 500 && c.initial_approved / c.initials < 0.30);
    if (lowApproval.length > 0) {
      console.log('>>> Low-initial-approval campaigns (>= 500 initials, <30% approval) — possible salvage:');
      lowApproval.slice(0, 10).forEach(c => {
        const initAppr = ((c.initial_approved / c.initials) * 100).toFixed(1);
        console.log('  ' + c.campaign_id + ' | ' + (c.campaign_name || '(no name)') + ' | init=' + c.initials + ' | approval=' + initAppr + '%');
      });
      console.log();
    }

    // Product overlap analysis — which campaigns share product_ids?
    // Pull main product_id from each campaign's orders
    const prodByCamp = querySql(`
      SELECT
        campaign_id,
        product_ids,
        COUNT(*) as n
      FROM orders
      WHERE client_id=? AND product_ids IS NOT NULL
      GROUP BY campaign_id, product_ids
      ORDER BY campaign_id, n DESC
    `, [cid]);

    // Find campaigns where the same product_ids appears under multiple campaign_ids
    const prodToCamps = {};
    prodByCamp.forEach(r => {
      if (!prodToCamps[r.product_ids]) prodToCamps[r.product_ids] = [];
      prodToCamps[r.product_ids].push({ campaign_id: r.campaign_id, n: r.n });
    });
    const shared = Object.entries(prodToCamps)
      .filter(([p, cs]) => cs.length > 1)
      .map(([p, cs]) => ({ product_ids: p, campaigns: cs }))
      .sort((a, b) => b.campaigns.reduce((s, c) => s + c.n, 0) - a.campaigns.reduce((s, c) => s + c.n, 0));

    if (shared.length > 0) {
      console.log('>>> Products used in MULTIPLE campaigns (potential main + reprocessing split):');
      shared.slice(0, 8).forEach(s => {
        const prodShort = s.product_ids.substring(0, 40);
        console.log('  products=' + prodShort + ' → ' + s.campaigns.length + ' campaigns: ' +
                    s.campaigns.map(c => '[' + c.campaign_id + ':' + c.n + ']').join(' '));
      });
      console.log();
    }
  }

  // 3. Cross-check: how many orders currently classified as 'main_initial' come from suspected reprocessing campaigns?
  console.log('='.repeat(70));
  console.log('SUMMARY: potentially misclassified orders');
  console.log('='.repeat(70));
  for (const cid of [1, 2, 3]) {
    const totalInitials = querySql(`
      SELECT COUNT(*) n FROM orders
      WHERE client_id=? AND derived_product_role='main_initial'
    `, [cid])[0].n;
    console.log('Client ' + cid + ': ' + totalInitials + ' orders currently classified as main_initial');
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
