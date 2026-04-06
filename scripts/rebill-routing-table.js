const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

const db = new Database(path.join(__dirname, '..', 'data', 'binroute.db'));
const MIN_ATT = 15;
const PROCS = "('Paysafe','KURV','Priority','Cliq')";
const BASE_WHERE = `o.is_test = 0 AND o.is_internal_test = 0 AND o.client_id != 3
  AND b.is_prepaid = 0 AND b.card_brand IN ('VISA','MASTERCARD')
  AND g.processor_name IN ${PROCS}`;
const JOIN = `JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
  JOIN bin_lookup b ON o.cc_first_6 = b.bin`;

// ── Rebill data at all 3 levels ──
const l1 = db.prepare(`
  SELECT b.issuer_bank, b.card_brand, b.card_type, g.processor_name,
    COUNT(*) as att,
    COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) as app,
    ROUND(100.0 * COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as rate
  FROM orders o ${JOIN}
  WHERE ${BASE_WHERE} AND o.derived_product_role = 'main_rebill'
    AND o.derived_cycle IN (1,2) AND o.derived_attempt = 1
  GROUP BY b.issuer_bank, b.card_brand, b.card_type, g.processor_name
`).all();

const l2 = db.prepare(`
  SELECT b.issuer_bank, b.card_brand, g.processor_name,
    COUNT(*) as att,
    COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) as app,
    ROUND(100.0 * COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as rate
  FROM orders o ${JOIN}
  WHERE ${BASE_WHERE} AND o.derived_product_role = 'main_rebill'
    AND o.derived_cycle IN (1,2) AND o.derived_attempt = 1
  GROUP BY b.issuer_bank, b.card_brand, g.processor_name
`).all();

const l3 = db.prepare(`
  SELECT b.card_brand, g.processor_name,
    COUNT(*) as att,
    COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) as app,
    ROUND(100.0 * COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as rate
  FROM orders o ${JOIN}
  WHERE ${BASE_WHERE} AND o.derived_product_role = 'main_rebill'
    AND o.derived_cycle IN (1,2) AND o.derived_attempt = 1
  GROUP BY b.card_brand, g.processor_name
`).all();

// ── Initial data at all 3 levels ──
const initL1 = db.prepare(`
  SELECT b.issuer_bank, b.card_brand, b.card_type, g.processor_name,
    COUNT(*) as att,
    COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) as app,
    ROUND(100.0 * COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as rate
  FROM orders o ${JOIN}
  WHERE ${BASE_WHERE} AND o.derived_product_role = 'main_initial'
    AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
  GROUP BY b.issuer_bank, b.card_brand, b.card_type, g.processor_name
`).all();

const initL2 = db.prepare(`
  SELECT b.issuer_bank, b.card_brand, g.processor_name,
    COUNT(*) as att,
    COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) as app,
    ROUND(100.0 * COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as rate
  FROM orders o ${JOIN}
  WHERE ${BASE_WHERE} AND o.derived_product_role = 'main_initial'
    AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
  GROUP BY b.issuer_bank, b.card_brand, g.processor_name
`).all();

const initL3 = db.prepare(`
  SELECT b.card_brand, g.processor_name,
    COUNT(*) as att,
    COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) as app,
    ROUND(100.0 * COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as rate
  FROM orders o ${JOIN}
  WHERE ${BASE_WHERE} AND o.derived_product_role = 'main_initial'
    AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
  GROUP BY b.card_brand, g.processor_name
`).all();

// ── BINs per bucket ──
const binsByBucket = db.prepare(`
  SELECT b.issuer_bank, b.card_brand, b.card_type,
    GROUP_CONCAT(DISTINCT b.bin) as bins
  FROM bin_lookup b
  WHERE b.is_prepaid = 0 AND b.card_brand IN ('VISA','MASTERCARD')
    AND b.issuer_bank IS NOT NULL
  GROUP BY b.issuer_bank, b.card_brand, b.card_type
`).all();

const binMap = new Map();
for (const r of binsByBucket) {
  binMap.set(`${r.issuer_bank}|${r.card_brand}|${r.card_type}`, r.bins || '');
}

// ── All combos with enough data ──
const combos = db.prepare(`
  SELECT b.issuer_bank, b.card_brand, b.card_type, COUNT(*) as total_att
  FROM orders o ${JOIN}
  WHERE ${BASE_WHERE} AND o.derived_product_role = 'main_rebill'
    AND o.derived_cycle IN (1,2) AND o.derived_attempt = 1
  GROUP BY b.issuer_bank, b.card_brand, b.card_type
  HAVING total_att >= 10
  ORDER BY total_att DESC
`).all();

function bestTwo(rows) {
  rows.sort((a, b) => b.rate - a.rate);
  return { best: rows[0], second: rows[1] || null };
}

const results = [];

for (const c of combos) {
  // ── Rebill: try L1 → L2 → L3 ──
  let rebillLevel, rebillBest, rebillSecond;
  const l1Rows = l1.filter(r => r.issuer_bank === c.issuer_bank && r.card_brand === c.card_brand && r.card_type === c.card_type && r.att >= MIN_ATT);
  if (l1Rows.length >= 2) {
    rebillLevel = 'BANK+TYPE';
    ({ best: rebillBest, second: rebillSecond } = bestTwo(l1Rows));
  } else {
    const l2Rows = l2.filter(r => r.issuer_bank === c.issuer_bank && r.card_brand === c.card_brand && r.att >= MIN_ATT);
    if (l2Rows.length >= 2) {
      rebillLevel = 'BANK';
      ({ best: rebillBest, second: rebillSecond } = bestTwo(l2Rows));
    } else {
      rebillLevel = 'BRAND';
      ({ best: rebillBest, second: rebillSecond } = bestTwo(l3.filter(r => r.card_brand === c.card_brand)));
    }
  }

  // ── Initial: try L1 → L2 → L3 ──
  let initLevel, initBest;
  const initL1Rows = initL1.filter(r => r.issuer_bank === c.issuer_bank && r.card_brand === c.card_brand && r.card_type === c.card_type && r.att >= MIN_ATT);
  if (initL1Rows.length >= 1) {
    initL1Rows.sort((a, b) => b.rate - a.rate);
    initBest = initL1Rows[0];
    initLevel = 'BANK+TYPE';
  } else {
    const initL2Rows = initL2.filter(r => r.issuer_bank === c.issuer_bank && r.card_brand === c.card_brand && r.att >= MIN_ATT);
    if (initL2Rows.length >= 1) {
      initL2Rows.sort((a, b) => b.rate - a.rate);
      initBest = initL2Rows[0];
      initLevel = 'BANK';
    } else {
      const initL3Rows = initL3.filter(r => r.card_brand === c.card_brand);
      initL3Rows.sort((a, b) => b.rate - a.rate);
      initBest = initL3Rows[0];
      initLevel = 'BRAND';
    }
  }

  // ── BINs ──
  const bins = binMap.get(`${c.issuer_bank}|${c.card_brand}|${c.card_type}`) || '';

  results.push({
    issuer_bank: c.issuer_bank,
    card_brand: c.card_brand,
    card_type: c.card_type,
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
    action: initBest.processor_name === rebillBest.processor_name ? 'STAY' : 'SWITCH',
    bins: bins,
  });
}

results.sort((a, b) => b.total_rebill_att - a.total_rebill_att);

// Write CSV
const header = 'Issuer Bank,Card Brand,Card Type,Rebill Attempts,Best Initial Proc,Init Rate %,Init Att,Init Level,Best Rebill Proc,Rebill Rate %,Rebill Att,Rebill Level,2nd Rebill Proc,2nd Rebill Rate %,Action,BINs';
const csvLines = [header];
for (const r of results) {
  csvLines.push([
    '"' + r.issuer_bank + '"', r.card_brand, r.card_type, r.total_rebill_att,
    r.init_proc, r.init_rate, r.init_att, r.init_level,
    r.rebill_proc, r.rebill_rate, r.rebill_att, r.rebill_level,
    r.rebill_2nd, r.rebill_2nd_rate, r.action,
    '"' + r.bins + '"'
  ].join(','));
}

fs.writeFileSync(path.join(__dirname, '..', 'data', 'rebill_routing_table.csv'), csvLines.join('\n'));
console.log('Saved to data/rebill_routing_table.csv');
console.log('Total rules:', results.length);
console.log('STAY:', results.filter(r => r.action === 'STAY').length);
console.log('SWITCH:', results.filter(r => r.action === 'SWITCH').length);
console.log('Init levels - BANK+TYPE:', results.filter(r => r.init_level === 'BANK+TYPE').length,
  'BANK:', results.filter(r => r.init_level === 'BANK').length,
  'BRAND:', results.filter(r => r.init_level === 'BRAND').length);
console.log('Rebill levels - BANK+TYPE:', results.filter(r => r.rebill_level === 'BANK+TYPE').length,
  'BANK:', results.filter(r => r.rebill_level === 'BANK').length,
  'BRAND:', results.filter(r => r.rebill_level === 'BRAND').length);

db.close();
