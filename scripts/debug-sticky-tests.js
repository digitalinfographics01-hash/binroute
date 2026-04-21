/**
 * Debug script: 4 tests to understand Sticky API behavior for VCT imports.
 *
 * 1. Pagination — does page=2 return orders 501+?
 * 2. Split overlap — do split sub-windows return same or different orders than parent?
 * 3. DB reconcile — previous VCT import artifacts
 * 4. Midnight boundary — timezone semantics on date_created
 */
const { initDb, querySql } = require('../src/db/connection');
const DataIngestion = require('../src/api/ingestion');

const CLIENT_ID = 6;
const DAY = '04/08/2026';

async function directQuery(client, params) {
  return await client._post('order_find', {
    campaign_id: 'all',
    start_date: params.startDate || DAY,
    end_date: params.endDate || DAY,
    start_time: params.startTime || '',
    end_time: params.endTime || '',
    date_type: 'create',
    criteria: 'all',
    search_type: 'all',
    return_type: 'order_view',
    results_per_page: params.rpp || 500,
    page: params.page || 1,
  });
}

function extractOrders(data) {
  if (!data || data.response_code !== '100') return [];
  if (data.data && typeof data.data === 'object') {
    return Array.isArray(data.data) ? data.data : Object.values(data.data);
  }
  const numericKeys = Object.keys(data).filter(k => /^\d+$/.test(k));
  if (numericKeys.length > 0) {
    return numericKeys.sort((a, b) => parseInt(a) - parseInt(b)).map(k => data[k]);
  }
  return [];
}

function getIds(orders) {
  return orders.map(o => o.order_id).filter(Boolean);
}

(async () => {
  await initDb();
  const ing = new DataIngestion(CLIENT_ID);
  ing.init();
  const client = ing.client;

  console.log('='.repeat(70));
  console.log('TEST 1 — Pagination (Apr 8 full day)');
  console.log('='.repeat(70));

  const p1 = await directQuery(client, { page: 1 });
  const p1Orders = extractOrders(p1);
  const p1Ids = new Set(getIds(p1Orders));
  console.log('Page 1: total_orders=' + p1.total_orders + ', returned=' + p1Orders.length);

  const p2 = await directQuery(client, { page: 2 });
  const p2Orders = extractOrders(p2);
  const p2Ids = new Set(getIds(p2Orders));
  console.log('Page 2: total_orders=' + p2.total_orders + ', returned=' + p2Orders.length);

  const p3 = await directQuery(client, { page: 3 });
  const p3Orders = extractOrders(p3);
  const p3Ids = new Set(getIds(p3Orders));
  console.log('Page 3: total_orders=' + p3.total_orders + ', returned=' + p3Orders.length);

  let p1p2Overlap = 0, p2p3Overlap = 0;
  for (const id of p2Ids) if (p1Ids.has(id)) p1p2Overlap++;
  for (const id of p3Ids) if (p2Ids.has(id)) p2p3Overlap++;
  console.log();
  console.log('Page1 ∩ Page2 overlap: ' + p1p2Overlap + ' / ' + p2Ids.size);
  console.log('Page2 ∩ Page3 overlap: ' + p2p3Overlap + ' / ' + p3Ids.size);
  console.log();
  if (p1Ids.size > 0 && p1p2Overlap === p1Ids.size && p2Ids.size === p1Ids.size) {
    console.log('VERDICT: Pagination NOT honored — page=2 returns SAME rows as page=1');
  } else if (p2Ids.size > 0 && p1p2Overlap === 0) {
    console.log('VERDICT: Pagination WORKS — page=2 returns DIFFERENT rows');
  } else {
    console.log('VERDICT: Mixed — partial overlap, inspect manually');
  }

  console.log();
  console.log('='.repeat(70));
  console.log('TEST 2 — Split window overlap');
  console.log('='.repeat(70));
  // Pick chunk 63/73 (0-idx 62): startMin=62*19=1178 (19:38), endMin=1196 (19:56)
  // midMin = floor((1178+1196)/2) = 1187 = 19:47
  const parentStart = '19:38:00', parentEnd = '19:56:59';
  const halfAStart = '19:38:00', halfAEnd = '19:47:59';
  const halfBStart = '19:48:00', halfBEnd = '19:56:59';
  console.log('Parent: ' + parentStart + ' - ' + parentEnd);
  console.log('Half A: ' + halfAStart + ' - ' + halfAEnd);
  console.log('Half B: ' + halfBStart + ' - ' + halfBEnd);
  console.log();

  const parent = await directQuery(client, { startTime: parentStart, endTime: parentEnd });
  const parentOrders = extractOrders(parent);
  const parentIds = new Set(getIds(parentOrders));
  console.log('Parent: total_orders=' + parent.total_orders + ', returned=' + parentOrders.length);

  const halfA = await directQuery(client, { startTime: halfAStart, endTime: halfAEnd });
  const halfAOrders = extractOrders(halfA);
  const halfAIds = new Set(getIds(halfAOrders));
  console.log('Half A: total_orders=' + halfA.total_orders + ', returned=' + halfAOrders.length);

  const halfB = await directQuery(client, { startTime: halfBStart, endTime: halfBEnd });
  const halfBOrders = extractOrders(halfB);
  const halfBIds = new Set(getIds(halfBOrders));
  console.log('Half B: total_orders=' + halfB.total_orders + ', returned=' + halfBOrders.length);

  let aInParent = 0, bInParent = 0;
  for (const id of halfAIds) if (parentIds.has(id)) aInParent++;
  for (const id of halfBIds) if (parentIds.has(id)) bInParent++;

  const allSeen = new Set([...parentIds, ...halfAIds, ...halfBIds]);
  const recoveredByA = [...halfAIds].filter(id => !parentIds.has(id)).length;
  const recoveredByB = [...halfBIds].filter(id => !parentIds.has(id)).length;

  console.log();
  console.log('Half A ∩ Parent: ' + aInParent + ' / ' + halfAIds.size + ' already in parent');
  console.log('Half B ∩ Parent: ' + bInParent + ' / ' + halfBIds.size + ' already in parent');
  console.log('Half A NEW (not in parent): ' + recoveredByA);
  console.log('Half B NEW (not in parent): ' + recoveredByB);
  console.log();
  console.log('Union (unique across parent+A+B): ' + allSeen.size);
  console.log('Naive sum:                         ' + (parentIds.size + halfAIds.size + halfBIds.size));
  console.log('Phantom/overcount:                 ' + ((parentIds.size + halfAIds.size + halfBIds.size) - allSeen.size));
  console.log();
  const totalInWindow = parseInt(parent.total_orders || 0, 10);
  console.log('Parent total_orders (ground truth): ' + totalInWindow);
  if (totalInWindow > 0) {
    console.log('Union coverage: ' + allSeen.size + ' of ' + totalInWindow + ' (' + ((allSeen.size / totalInWindow) * 100).toFixed(1) + '%)');
  }

  console.log();
  console.log('='.repeat(70));
  console.log('TEST 3 — DB reconcile');
  console.log('='.repeat(70));
  const c6Count = querySql('SELECT COUNT(*) n FROM orders WHERE client_id=6')[0].n;
  const c6Distinct = querySql('SELECT COUNT(DISTINCT order_id) n FROM orders WHERE client_id=6')[0].n;
  console.log('Client 6 order rows: ' + c6Count + ' (distinct order_id: ' + c6Distinct + ')');

  try {
    const syncRows = querySql("SELECT * FROM sync_state WHERE client_id=6 ORDER BY id DESC LIMIT 20");
    console.log();
    console.log('sync_state for client 6:');
    syncRows.forEach(r => console.log('  ', JSON.stringify(r)));
  } catch (e) {
    console.log('sync_state query: ' + e.message);
  }

  const { execSync } = require('child_process');
  try {
    const backups = execSync('ls -la /opt/binroute/data/*.db* /opt/binroute/*.json /opt/binroute/*.txt 2>&1 | head -40', { encoding: 'utf8' });
    console.log();
    console.log('Root data files:');
    console.log(backups);
  } catch (e) {}

  console.log();
  console.log('='.repeat(70));
  console.log('TEST 4 — Midnight boundary / timezone');
  console.log('='.repeat(70));
  const lateApr8 = await directQuery(client, { startTime: '23:55:00', endTime: '23:59:59', startDate: '04/08/2026', endDate: '04/08/2026' });
  const lateApr8Orders = extractOrders(lateApr8);
  console.log('Apr 8 23:55-23:59: total_orders=' + lateApr8.total_orders + ', returned=' + lateApr8Orders.length);
  if (lateApr8Orders.length > 0) {
    lateApr8Orders.slice(0, 3).forEach(o => {
      console.log('  id=' + o.order_id + ' acq_date=' + (o.acquisition_date || 'n/a') + ' created=' + (o.date_created || o.created_at || 'n/a'));
    });
  }

  const earlyApr9 = await directQuery(client, { startTime: '00:00:00', endTime: '00:04:59', startDate: '04/09/2026', endDate: '04/09/2026' });
  const earlyApr9Orders = extractOrders(earlyApr9);
  console.log();
  console.log('Apr 9 00:00-00:04: total_orders=' + earlyApr9.total_orders + ', returned=' + earlyApr9Orders.length);
  if (earlyApr9Orders.length > 0) {
    earlyApr9Orders.slice(0, 3).forEach(o => {
      console.log('  id=' + o.order_id + ' acq_date=' + (o.acquisition_date || 'n/a') + ' created=' + (o.date_created || o.created_at || 'n/a'));
    });
  }

  const apr8Full = await directQuery(client, { startDate: '04/08/2026', endDate: '04/08/2026' });
  const apr9Full = await directQuery(client, { startDate: '04/09/2026', endDate: '04/09/2026' });
  const bothDaysFull = await directQuery(client, { startDate: '04/08/2026', endDate: '04/09/2026' });
  const apr8Count = parseInt(apr8Full.total_orders || 0, 10);
  const apr9Count = parseInt(apr9Full.total_orders || 0, 10);
  const bothDays = parseInt(bothDaysFull.total_orders || 0, 10);
  console.log();
  console.log('Current apiTotal Apr 8:    ' + apr8Count + '   (overnight run saw: 29124)');
  console.log('Current apiTotal Apr 9:    ' + apr9Count + '   (overnight run saw: 7782)');
  console.log('Current apiTotal Apr 8-9:  ' + bothDays);
  console.log('Sum of singles:            ' + (apr8Count + apr9Count));
  console.log('Delta (two-day - sum):     ' + (bothDays - (apr8Count + apr9Count)));
  if (bothDays !== apr8Count + apr9Count) {
    console.log('  → Date boundaries are NOT cleanly additive');
  } else {
    console.log('  → Date boundaries are cleanly additive');
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
