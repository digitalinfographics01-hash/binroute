const { initDb, querySql } = require('../src/db/connection');
initDb().then(() => {

console.log('=== CUSTOMER LIFECYCLE: INITIAL → CYCLE 1-2 → CYCLE 3+ ===');
console.log('Per processor, per bank, per L4 where data supports it');
console.log();

// Stage 1: Initial (main_initial, att=1, processing_gateway_id)
// Stage 2: Rebill cycle 1-2 (main_rebill, derived_cycle 1-2, att=1, not cascaded)
// Stage 3: Rebill cycle 3+ (main_rebill, derived_cycle 3+, att=1, not cascaded)

const minDate = '2025-12-01';
const maxDate = '2026-03-28';

// Get gateway map
const gwMap = {};
const gws = querySql('SELECT gateway_id, processor_name FROM gateways WHERE client_id = 1');
for (const g of gws) gwMap[g.gateway_id] = g.processor_name;

// Stage 1: Initials by processor + bank + brand + card_type
console.log('=== STAGE 1: INITIAL (main_initial att=1) ===');
const initials = querySql(`
  SELECT g.processor_name, b.issuer_bank, b.card_brand, b.card_type,
    COUNT(*) as att,
    SUM(CASE WHEN o.is_cascaded = 0 AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
  FROM orders o
  JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = 1
  JOIN bin_lookup b ON o.cc_first_6 = b.bin
  WHERE o.client_id = 1 AND o.is_test = 0 AND o.is_internal_test = 0
    AND (o.attempt_number = 1 OR o.attempt_number IS NULL)
    AND o.processing_gateway_id IS NOT NULL
    AND o.derived_product_role = 'main_initial'
    AND o.acquisition_date >= ? AND o.acquisition_date <= ?
  GROUP BY g.processor_name
  ORDER BY att DESC
`, [minDate, maxDate]);

console.log('Processor'.padEnd(22) + 'Rate'.padEnd(18) + 'Volume');
for (const r of initials) {
  if (!r.processor_name) continue;
  console.log(r.processor_name.padEnd(22) + (r.app+'/'+r.att+' = '+(r.att>0?(r.app/r.att*100).toFixed(1):0)+'%').padEnd(18) + r.att);
}

// Stage 2: Rebill cycle 1-2
console.log();
console.log('=== STAGE 2: REBILL CYCLE 1-2 (main_rebill, derived_cycle 1-2, att=1, not cascaded) ===');
const rebillC12 = querySql(`
  SELECT g.processor_name,
    COUNT(*) as att,
    SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
  FROM orders o
  JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
  WHERE o.client_id = 1 AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.derived_product_role = 'main_rebill'
    AND o.derived_cycle IN (1, 2)
    AND o.attempt_number = 1
    AND o.is_cascaded = 0
    AND o.acquisition_date >= ? AND o.acquisition_date <= ?
  GROUP BY g.processor_name ORDER BY att DESC
`, [minDate, maxDate]);

console.log('Processor'.padEnd(22) + 'Rate'.padEnd(18) + 'Volume');
for (const r of rebillC12) {
  if (!r.processor_name) continue;
  console.log(r.processor_name.padEnd(22) + (r.app+'/'+r.att+' = '+(r.att>0?(r.app/r.att*100).toFixed(1):0)+'%').padEnd(18) + r.att);
}

// Stage 3: Rebill cycle 3+
console.log();
console.log('=== STAGE 3: REBILL CYCLE 3+ (main_rebill, derived_cycle 3+, att=1, not cascaded) ===');
const rebillC3 = querySql(`
  SELECT g.processor_name,
    COUNT(*) as att,
    SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
  FROM orders o
  JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
  WHERE o.client_id = 1 AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.derived_product_role = 'main_rebill'
    AND o.derived_cycle >= 3
    AND o.attempt_number = 1
    AND o.is_cascaded = 0
    AND o.acquisition_date >= ? AND o.acquisition_date <= ?
  GROUP BY g.processor_name ORDER BY att DESC
`, [minDate, maxDate]);

console.log('Processor'.padEnd(22) + 'Rate'.padEnd(18) + 'Volume');
for (const r of rebillC3) {
  if (!r.processor_name) continue;
  console.log(r.processor_name.padEnd(22) + (r.app+'/'+r.att+' = '+(r.att>0?(r.app/r.att*100).toFixed(1):0)+'%').padEnd(18) + r.att);
}

// Side by side comparison
console.log();
console.log('=== SIDE BY SIDE: PROCESSOR PERFORMANCE ACROSS LIFECYCLE ===');
const initMap = new Map(initials.map(r => [r.processor_name, r]));
const c12Map = new Map(rebillC12.map(r => [r.processor_name, r]));
const c3Map = new Map(rebillC3.map(r => [r.processor_name, r]));
const allProcs = [...new Set([...initMap.keys(), ...c12Map.keys(), ...c3Map.keys()])].filter(Boolean).sort();

console.log('Processor'.padEnd(22) + 'Initial'.padEnd(15) + 'Rebill C1-2'.padEnd(15) + 'Rebill C3+'.padEnd(15) + 'Drop Init→C12'.padEnd(15) + 'Drop C12→C3+');
for (const proc of allProcs) {
  const i = initMap.get(proc);
  const c12 = c12Map.get(proc);
  const c3 = c3Map.get(proc);
  const iRate = i && i.att > 0 ? (i.app/i.att*100) : 0;
  const c12Rate = c12 && c12.att > 0 ? (c12.app/c12.att*100) : 0;
  const c3Rate = c3 && c3.att > 0 ? (c3.app/c3.att*100) : 0;
  const drop1 = iRate - c12Rate;
  const drop2 = c12Rate - c3Rate;
  console.log(proc.padEnd(22) + (iRate.toFixed(1)+'%').padEnd(15) + (c12Rate.toFixed(1)+'%').padEnd(15) + (c3Rate.toFixed(1)+'%').padEnd(15) + (drop1.toFixed(1)+'pp').padEnd(15) + drop2.toFixed(1)+'pp');
}

// Per bank lifecycle
console.log();
console.log('=== PER BANK LIFECYCLE (top banks by volume) ===');
const bankInit = querySql(`
  SELECT b.issuer_bank, COUNT(*) as att,
    SUM(CASE WHEN o.is_cascaded = 0 AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
  FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
  WHERE o.client_id = 1 AND o.is_test = 0 AND o.is_internal_test = 0
    AND (o.attempt_number = 1 OR o.attempt_number IS NULL)
    AND o.processing_gateway_id IS NOT NULL AND o.derived_product_role = 'main_initial'
    AND o.acquisition_date >= ? AND o.acquisition_date <= ?
  GROUP BY b.issuer_bank HAVING att >= 50 ORDER BY att DESC LIMIT 15
`, [minDate, maxDate]);

const bankC12 = querySql(`
  SELECT b.issuer_bank, COUNT(*) as att,
    SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
  FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
  WHERE o.client_id = 1 AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.derived_product_role = 'main_rebill' AND o.derived_cycle IN (1,2)
    AND o.attempt_number = 1 AND o.is_cascaded = 0
    AND o.acquisition_date >= ? AND o.acquisition_date <= ?
  GROUP BY b.issuer_bank
`, [minDate, maxDate]);

const bankC3 = querySql(`
  SELECT b.issuer_bank, COUNT(*) as att,
    SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
  FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
  WHERE o.client_id = 1 AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.derived_product_role = 'main_rebill' AND o.derived_cycle >= 3
    AND o.attempt_number = 1 AND o.is_cascaded = 0
    AND o.acquisition_date >= ? AND o.acquisition_date <= ?
  GROUP BY b.issuer_bank
`, [minDate, maxDate]);

const bc12Map = new Map(bankC12.map(r => [r.issuer_bank, r]));
const bc3Map = new Map(bankC3.map(r => [r.issuer_bank, r]));

console.log('Bank'.padEnd(38) + 'Initial'.padEnd(14) + 'C1-2'.padEnd(14) + 'C3+'.padEnd(14) + 'Drop');
for (const b of bankInit) {
  const c12 = bc12Map.get(b.issuer_bank);
  const c3 = bc3Map.get(b.issuer_bank);
  const iRate = b.att > 0 ? (b.app/b.att*100).toFixed(1) : '—';
  const c12Rate = c12 && c12.att > 0 ? (c12.app/c12.att*100).toFixed(1) : '—';
  const c3Rate = c3 && c3.att > 0 ? (c3.app/c3.att*100).toFixed(1) : '—';
  console.log(b.issuer_bank.slice(0,36).padEnd(38) + (iRate+'%').padEnd(14) + (c12Rate+'%').padEnd(14) + (c3Rate+'%').padEnd(14));
}

});
