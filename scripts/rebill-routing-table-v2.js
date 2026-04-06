const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

const db = new Database(path.join(__dirname, '..', 'data', 'binroute.db'));
const MIN = 15;
const PROCS = "('Paysafe','KURV','Priority','Cliq')";
const J = `JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
  JOIN bin_lookup b ON o.cc_first_6 = b.bin`;
const W = `o.client_id != 3 AND o.is_test = 0 AND o.is_internal_test = 0
  AND b.card_brand IN ('VISA','MASTERCARD') AND g.processor_name IN ${PROCS}`;

function q(role, extra, groupBy) {
  return db.prepare(`
    SELECT ${groupBy}, g.processor_name,
      COUNT(*) as att,
      COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) as app,
      ROUND(100.0 * COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as rate
    FROM orders o ${J}
    WHERE ${W} AND o.derived_product_role = '${role}' ${extra}
    GROUP BY ${groupBy}, g.processor_name
  `).all();
}

// Rebill queries
const rebL1 = q('main_rebill', 'AND o.derived_cycle = 1 AND o.derived_attempt = 1', 'b.issuer_bank, b.card_brand, b.card_type, b.is_prepaid');
const rebL2 = q('main_rebill', 'AND o.derived_cycle = 1 AND o.derived_attempt = 1', 'b.issuer_bank, b.card_brand, b.is_prepaid');
const rebL3 = q('main_rebill', 'AND o.derived_cycle = 1 AND o.derived_attempt = 1', 'b.card_brand, b.is_prepaid');

// Initial queries
const initL1 = q('main_initial', 'AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)', 'b.issuer_bank, b.card_brand, b.card_type, b.is_prepaid');
const initL2 = q('main_initial', 'AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)', 'b.issuer_bank, b.card_brand, b.is_prepaid');
const initL3 = q('main_initial', 'AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)', 'b.card_brand, b.is_prepaid');

// BINs
const binRows = db.prepare(`
  SELECT b.issuer_bank, b.card_brand, b.card_type, b.is_prepaid,
    GROUP_CONCAT(DISTINCT b.bin) as bins
  FROM bin_lookup b
  WHERE b.card_brand IN ('VISA','MASTERCARD') AND b.issuer_bank IS NOT NULL
  GROUP BY b.issuer_bank, b.card_brand, b.card_type, b.is_prepaid
`).all();
const binMap = new Map();
for (const r of binRows) binMap.set(`${r.issuer_bank}|${r.card_brand}|${r.card_type}|${r.is_prepaid}`, r.bins || '');

// All combos
const combos = db.prepare(`
  SELECT b.issuer_bank, b.card_brand, b.card_type, b.is_prepaid, COUNT(*) as total_att
  FROM orders o ${J}
  WHERE ${W} AND o.derived_product_role = 'main_rebill'
    AND o.derived_cycle = 1 AND o.derived_attempt = 1
  GROUP BY b.issuer_bank, b.card_brand, b.card_type, b.is_prepaid
  HAVING total_att >= 30
  ORDER BY total_att DESC
`).all();

function bestTwo(rows) {
  const s = [...rows].sort((a, b) => b.rate - a.rate);
  return { best: s[0], second: s[1] || null };
}

function findBest(data, match, minAtt) {
  const filtered = data.filter(r => {
    for (const [k, v] of Object.entries(match)) {
      if (r[k] !== v) return false;
    }
    return r.att >= minAtt;
  });
  return filtered;
}

const results = [];

for (const c of combos) {
  const pp = c.is_prepaid;
  const bank = c.issuer_bank;
  const brand = c.card_brand;
  const type = c.card_type;

  // Rebill: L1 → L2 → L3
  let rebillLevel, rebillBest, rebillSecond;
  let rl = findBest(rebL1, { issuer_bank: bank, card_brand: brand, card_type: type, is_prepaid: pp }, MIN);
  if (rl.length >= 2) {
    rebillLevel = 'BANK+TYPE';
    ({ best: rebillBest, second: rebillSecond } = bestTwo(rl));
  } else {
    rl = findBest(rebL2, { issuer_bank: bank, card_brand: brand, is_prepaid: pp }, MIN);
    if (rl.length >= 2) {
      rebillLevel = 'BANK';
      ({ best: rebillBest, second: rebillSecond } = bestTwo(rl));
    } else {
      rl = findBest(rebL3, { card_brand: brand, is_prepaid: pp }, MIN);
      if (rl.length >= 2) {
        rebillLevel = 'BRAND';
        ({ best: rebillBest, second: rebillSecond } = bestTwo(rl));
      } else continue;
    }
  }

  // Initial: L1 → L2 → L3
  let initLevel, initBest;
  let il = findBest(initL1, { issuer_bank: bank, card_brand: brand, card_type: type, is_prepaid: pp }, MIN);
  if (il.length >= 1) {
    il.sort((a, b) => b.rate - a.rate);
    initBest = il[0]; initLevel = 'BANK+TYPE';
  } else {
    il = findBest(initL2, { issuer_bank: bank, card_brand: brand, is_prepaid: pp }, MIN);
    if (il.length >= 1) {
      il.sort((a, b) => b.rate - a.rate);
      initBest = il[0]; initLevel = 'BANK';
    } else {
      il = findBest(initL3, { card_brand: brand, is_prepaid: pp }, MIN);
      if (il.length >= 1) {
        il.sort((a, b) => b.rate - a.rate);
        initBest = il[0]; initLevel = 'BRAND';
      } else continue;
    }
  }

  const bins = binMap.get(`${bank}|${brand}|${type}|${pp}`) || '';

  results.push({
    issuer_bank: bank,
    card_brand: brand,
    card_type: type,
    prepaid: pp ? 'YES' : 'NO',
    total_rebill_att: c.total_att,
    init_proc: initBest.processor_name,
    init_rate: initBest.rate,
    init_att: initBest.att,
    init_level: initLevel,
    rebill_proc: rebillBest.processor_name,
    rebill_rate: rebillBest.rate,
    rebill_att: rebillBest.att,
    rebill_level: rebillLevel,
    rebill_2nd: rebillSecond ? rebillSecond.processor_name : '',
    rebill_2nd_rate: rebillSecond ? rebillSecond.rate : '',
    action: (rebillBest.rate < 2 && rebillBest.att >= 15) ? 'BLOCK' : (initBest.processor_name === rebillBest.processor_name ? 'STAY' : 'SWITCH'),
    pricing: rebillBest.rate < 2 ? '' : rebillBest.rate < 10 ? 'VERY AGGRESSIVE PRICE DROP' : rebillBest.rate < 15 ? 'MODERATE PRICE DROP' : '',
    bins: bins,
  });
}

results.sort((a, b) => b.total_rebill_att - a.total_rebill_att);

// Write CSV
const header = 'Issuer Bank,Card Brand,Card Type,Prepaid,Rebill Attempts,Best Initial Proc,Init Rate %,Init Att,Init Level,Best Rebill Proc,Rebill Rate %,Rebill Att,Rebill Level,2nd Rebill Proc,2nd Rebill Rate %,Action,Pricing Signal,BINs,Bin Profiling';
const csvLines = [header];
for (const r of results) {
  csvLines.push([
    '"' + r.issuer_bank + '"', r.card_brand, r.card_type, r.prepaid, r.total_rebill_att,
    r.init_proc, r.init_rate, r.init_att, r.init_level,
    r.rebill_proc, r.rebill_rate, r.rebill_att, r.rebill_level,
    r.rebill_2nd, r.rebill_2nd_rate, r.action, r.pricing,
    '"' + r.bins + '"'
  ].join(','));
}

fs.writeFileSync(path.join(__dirname, '..', 'data', 'rebill_routing_table.csv'), csvLines.join('\n'));

// Also write xlsx with BINs preserved as text
try {
  const XLSX = require('xlsx');
  const wsData = [header.split(',')];
  for (const r of results) {
    wsData.push([
      r.issuer_bank, r.card_brand, r.card_type, r.prepaid, r.total_rebill_att,
      r.init_proc, r.init_rate, r.init_att, r.init_level,
      r.rebill_proc, r.rebill_rate, r.rebill_att, r.rebill_level,
      r.rebill_2nd, r.rebill_2nd_rate, r.action, r.pricing, r.bins,
      (r.bins || '').split(',').filter(Boolean).join('\n')
    ]);
  }
  const ws = XLSX.utils.aoa_to_sheet(wsData);
  // Force BINs column (R, index 17) and Bin Profiling (S, index 18) to text format
  const range = XLSX.utils.decode_range(ws['!ref']);
  for (let row = 1; row <= range.e.r; row++) {
    const cellR = ws[XLSX.utils.encode_cell({ r: row, c: 17 })];
    if (cellR) { cellR.t = 's'; cellR.z = '@'; }
    const cellS = ws[XLSX.utils.encode_cell({ r: row, c: 18 })];
    if (cellS) { cellS.t = 's'; cellS.z = '@'; }
  }
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Routing Table');
  XLSX.writeFile(wb, path.join(__dirname, '..', 'data', 'rebill_routing_table_v2.xlsx'));
  console.log('Also saved as data/rebill_routing_table.xlsx');
} catch (e) {
  console.log('xlsx export skipped:', e.message);
}
console.log('Saved to data/rebill_routing_table.csv');
console.log('Total rules:', results.length);
console.log('Non-prepaid:', results.filter(r => r.prepaid === 'NO').length);
console.log('Prepaid:', results.filter(r => r.prepaid === 'YES').length);
console.log('STAY:', results.filter(r => r.action === 'STAY').length);
console.log('SWITCH:', results.filter(r => r.action === 'SWITCH').length);
console.log('BLOCK:', results.filter(r => r.action === 'BLOCK').length);
console.log('Init levels - BANK+TYPE:', results.filter(r => r.init_level === 'BANK+TYPE').length,
  'BANK:', results.filter(r => r.init_level === 'BANK').length,
  'BRAND:', results.filter(r => r.init_level === 'BRAND').length);
console.log('Rebill levels - BANK+TYPE:', results.filter(r => r.rebill_level === 'BANK+TYPE').length,
  'BANK:', results.filter(r => r.rebill_level === 'BANK').length,
  'BRAND:', results.filter(r => r.rebill_level === 'BRAND').length);

db.close();
