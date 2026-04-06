const { initDb, querySql } = require('../src/db/connection');
initDb().then(() => {

const minDate = '2025-12-01';
const maxDate = '2026-03-28';

// Identify natural campaigns (campaigns that have att=1 orders)
const campaigns = querySql(
  `SELECT DISTINCT campaign_id FROM orders WHERE client_id = 1 AND derived_product_role = 'main_initial' AND attempt_number = 1`
);
const natCamps = new Set(campaigns.map(c => c.campaign_id));

console.log('=== RETRY / REPROCESSING ANALYSIS (main_initial) ===');
console.log('Natural campaigns:', natCamps.size);

// All att>=2, classify as customer_retry or reprocessing
const allRetries = querySql(
  `SELECT attempt_number, campaign_id, is_cascaded, order_status FROM orders
   WHERE client_id = 1 AND is_test = 0 AND is_internal_test = 0
   AND derived_product_role = 'main_initial' AND attempt_number >= 2 AND attempt_number <= 8
   AND acquisition_date >= ? AND acquisition_date <= ?`, [minDate, maxDate]
);

const summary = {};
for (const r of allRetries) {
  const type = natCamps.has(r.campaign_id) ? 'customer_retry' : 'reprocessing';
  const cascLabel = r.is_cascaded ? 'cascaded' : 'direct';
  const key = type + '|' + r.attempt_number + '|' + cascLabel;
  if (!summary[key]) summary[key] = { type, att: r.attempt_number, casc: cascLabel, cnt: 0, app: 0 };
  summary[key].cnt++;
  if ([2,6,8].includes(r.order_status)) summary[key].app++;
}

console.log('\n--- CUSTOMER RETRY vs REPROCESSING ---');
const sorted = Object.values(summary).sort((a,b) => a.type.localeCompare(b.type) || a.att - b.att || a.casc.localeCompare(b.casc));
let curType = '';
for (const s of sorted) {
  if (s.type !== curType) { console.log('\n  ' + s.type.toUpperCase()); curType = s.type; }
  console.log('    att ' + s.att + ' (' + s.casc + '): ' + s.app + '/' + s.cnt + ' = ' + (s.cnt > 0 ? (s.app/s.cnt*100).toFixed(1) : 0) + '%');
}

// Per processor retry performance (att 2, direct, using processing_gateway)
console.log('\n--- CUSTOMER RETRY PER PROCESSOR (att 2, direct) ---');
const allAtt2 = querySql(
  `SELECT o.processing_gateway_id, o.campaign_id, o.order_status FROM orders o
   WHERE o.client_id = 1 AND o.is_test = 0 AND o.is_internal_test = 0
   AND o.derived_product_role = 'main_initial' AND o.attempt_number = 2 AND o.is_cascaded = 0
   AND o.processing_gateway_id IS NOT NULL
   AND o.acquisition_date >= ? AND o.acquisition_date <= ?`, [minDate, maxDate]
);

const gwMap = {};
const gws = querySql('SELECT gateway_id, processor_name FROM gateways WHERE client_id = 1');
for (const g of gws) gwMap[g.gateway_id] = g.processor_name;

const retryByProc = {}, reprocByProc = {};
for (const r of allAtt2) {
  const proc = gwMap[r.processing_gateway_id] || '?';
  const isRetry = natCamps.has(r.campaign_id);
  const map = isRetry ? retryByProc : reprocByProc;
  if (!map[proc]) map[proc] = { att: 0, app: 0 };
  map[proc].att++;
  if ([2,6,8].includes(r.order_status)) map[proc].app++;
}

for (const [proc, d] of Object.entries(retryByProc).sort((a,b) => b[1].att - a[1].att)) {
  console.log('  ' + proc.padEnd(22) + d.app + '/' + d.att + ' = ' + (d.att > 0 ? (d.app/d.att*100).toFixed(1) : 0) + '%');
}

console.log('\n--- AUTO REPROCESSING PER PROCESSOR (att 2, direct) ---');
for (const [proc, d] of Object.entries(reprocByProc).sort((a,b) => b[1].att - a[1].att)) {
  console.log('  ' + proc.padEnd(22) + d.app + '/' + d.att + ' = ' + (d.att > 0 ? (d.app/d.att*100).toFixed(1) : 0) + '%');
}

// Per bank retry recovery
console.log('\n--- RETRY RECOVERY PER BANK (att 2-4, direct) ---');
const bankRetry = querySql(
  `SELECT b.issuer_bank, COUNT(*) as att,
    SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
  FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
  WHERE o.client_id = 1 AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.derived_product_role = 'main_initial'
    AND o.attempt_number BETWEEN 2 AND 4 AND o.is_cascaded = 0
    AND o.acquisition_date >= ? AND o.acquisition_date <= ?
  GROUP BY b.issuer_bank HAVING att >= 5 ORDER BY att DESC LIMIT 20`, [minDate, maxDate]
);

for (const r of bankRetry) {
  console.log('  ' + (r.issuer_bank||'').slice(0,38).padEnd(40) + r.app + '/' + r.att + ' = ' + (r.att>0?(r.app/r.att*100).toFixed(1):0) + '%');
}

// ROI: how many extra approvals from retries?
console.log('\n--- RETRY/REPROCESSING ROI ---');
const retryApprovals = allRetries.filter(r => [2,6,8].includes(r.order_status));
const retryOnly = retryApprovals.filter(r => natCamps.has(r.campaign_id));
const reprocOnly = retryApprovals.filter(r => !natCamps.has(r.campaign_id));
console.log('  Customer retry approvals:', retryOnly.length);
console.log('  Reprocessing approvals:', reprocOnly.length);
console.log('  Total extra approvals from salvage:', retryApprovals.length);
console.log('  As % of natural approvals: +' + (retryApprovals.length / querySql(
  `SELECT COUNT(*) as cnt FROM orders WHERE client_id = 1 AND derived_product_role = 'main_initial'
   AND attempt_number = 1 AND is_cascaded = 0 AND order_status IN (2,6,8)
   AND acquisition_date >= ? AND acquisition_date <= ?`, [minDate, maxDate]
)[0].cnt * 100).toFixed(1) + '% additional');

});
