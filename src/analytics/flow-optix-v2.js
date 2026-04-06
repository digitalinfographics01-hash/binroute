/**
 * Flow Optix V2 — Rebill routing engine rebuild.
 *
 * Cards grouped by issuer_bank + card_brand (L2).
 * All sub-groups (L3 prepaid, L4 type, L5 level) shown inside each card.
 *
 * Filters:
 *   Rebills: derived_product_role IN (main_rebill, upsell_rebill), derived_attempt, cycle 1-2
 *   Initials: derived_product_role = main_initial or upsell_initial, derived_attempt
 *   Natural: derived_attempt=1, not cascaded. Salvage: derived_attempt>=2, not cascaded
 *   Exclude: is_test, is_internal_test, exclude_from_analysis, order_total IN (6.96, 64.98)
 *
 * Weighting (active only): 30d×0.5, 30-90d×0.3, 90-180d×0.2
 * Confidence: HIGH>=20 app, MEDIUM>=10, LOW>=1, GATHERING=0
 * RPA = rate × avg_order_value
 * STOP threshold: RPA < $3
 */
const { querySql } = require('../db/connection');
const { getCachedOrCompute, CRM_ROUTING_EXCLUSION } = require('./engine');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const WEIGHTS = { d30: 0.5, d90: 0.3, d180: 0.2 };
const STOP_RPA = 3;
const HIST_BETTER_PP = 5;
const EXCLUDED_PRICES = [6.96, 64.98];
const TEST_BINS = ['144444', '777777'];

// Normalize bank names to merge variants into canonical names
function normalizeBank(name) {
  if (!name) return 'Unknown';
  const u = name.toUpperCase();
  if (u.includes('BANK OF AMERICA')) return 'BANK OF AMERICA, NATIONAL ASSOCIATION';
  if (u.includes('CITIBANK') || u.includes('CITI BANK')) return 'CITIBANK N.A.';
  if (u.includes('JPMORGAN') || u.includes('JP MORGAN')) return 'JPMORGAN CHASE BANK N.A.';
  if (u.includes('WELLS FARGO')) return 'WELLS FARGO BANK, NATIONAL ASSOCIATION';
  if (u.includes('U.S. BANK') || u.includes('US BANK')) return 'U.S. BANK NATIONAL ASSOCIATION';
  return name;
}

function confidence(approved) {
  if (approved >= 20) return 'HIGH';
  if (approved >= 10) return 'MEDIUM';
  if (approved >= 1) return 'LOW';
  return 'GATHERING';
}

function _rate(app, att) { return att > 0 ? Math.round((app / att) * 10000) / 100 : 0; }

const CONF_RANK = { HIGH: 3, MEDIUM: 2, LOW: 1, GATHERING: 0 };

// Pick best processor with confidence weighting
// Prefers MEDIUM+ over LOW confidence processors
// LOW with <5 approvals never beats MEDIUM+
// LOW with 5+ approvals must have >15pp rate gap to beat MEDIUM+
function _pickBest(processors, activeOnly) {
  const filtered = (processors || []).filter(p => activeOnly != null ? (activeOnly ? p.active : !p.active) : true).filter(p => p.app > 0);
  if (filtered.length === 0) return null;

  // Sort by rate descending
  const sorted = [...filtered].sort((a, b) => {
    const rA = a.weightedRate != null ? a.weightedRate : a.rawRate;
    const rB = b.weightedRate != null ? b.weightedRate : b.rawRate;
    return rB - rA;
  });

  const top = sorted[0];
  const topConf = CONF_RANK[top.confidence] || 0;

  // If top is already MEDIUM+, it's the pick
  if (topConf >= 2) return top;

  // Top is LOW — find the best MEDIUM+ processor
  const bestMedPlus = sorted.find(p => p !== top && (CONF_RANK[p.confidence] || 0) >= 2);
  if (!bestMedPlus) return top; // no MEDIUM+ exists, keep top

  // LOW with <5 approvals never beats MEDIUM+
  if (top.app < 5) return bestMedPlus;

  // LOW with 5+ approvals must have >15pp gap to beat MEDIUM+
  const topRate = top.weightedRate != null ? top.weightedRate : top.rawRate;
  const medRate = bestMedPlus.weightedRate != null ? bestMedPlus.weightedRate : bestMedPlus.rawRate;
  if (topRate - medRate <= 15) return bestMedPlus;

  return top;
}

// Build salvage sequence from raw rows + baseline processor info
function _buildSalvageSeq(salRows, bestActiveProc, monthlyAtt, minPriceAtt, activeProcs) {
  const seq = [];
  if (!salRows || salRows.length === 0) return seq;

  const byAtt = new Map();
  for (const sr of salRows) {
    const att = sr.attempt_number;
    const proc = sr.processor_name || 'Unknown';
    if (!byAtt.has(att)) byAtt.set(att, new Map());
    const am = byAtt.get(att);
    const isActive = activeProcs ? activeProcs.has(proc) : sr.gateway_active;
    if (!am.has(proc)) am.set(proc, { att: 0, app: 0, prices: new Map(), active: isActive ? 1 : 0 });
    const sd = am.get(proc);
    sd.att++;
    if ([2, 6, 8].includes(sr.order_status)) sd.app++;
    if (isActive) sd.active = 1;
    if (!sd.prices.has(sr.order_total)) sd.prices.set(sr.order_total, { att: 0, app: 0 });
    sd.prices.get(sr.order_total).att++;
    if ([2, 6, 8].includes(sr.order_status)) sd.prices.get(sr.order_total).app++;
  }

  const natRate = bestActiveProc ? (bestActiveProc.weightedRate || bestActiveProc.rawRate) : 0;
  seq.push({
    label: 'Decline att 1',
    processor: bestActiveProc?.name || 'N/A',
    price: bestActiveProc?.aov || 0,
    rate: natRate,
    rpa: bestActiveProc?.rpa || 0,
    volume: monthlyAtt,
    active: true,
  });

  let pVol = monthlyAtt, pRate = natRate / 100;
  for (let an = 2; an <= 8; an++) {
    const am = byAtt.get(an);
    if (!am) break;
    let bRpa = 0, bProc = null, bPrice = 0, bRate = 0;
    for (const [proc, sd] of am) {
      for (const [price, pd] of sd.prices) {
        if (pd.att >= minPriceAtt) {
          const rate = _rate(pd.app, pd.att);
          const rpa = Math.round(rate * price) / 100;
          if (rpa > bRpa) { bRpa = rpa; bProc = proc; bPrice = price; bRate = rate; }
        }
      }
    }
    if (bRpa < STOP_RPA || !bProc) {
      seq.push({ label: `Decline att ${an}`, isStop: true, processor: bProc || 'N/A', rpa: bRpa, volume: Math.round(pVol * (1 - pRate)) });
      break;
    }
    const vol = Math.round(pVol * (1 - pRate));
    seq.push({ label: `Decline att ${an}`, processor: bProc, price: bPrice, rate: bRate, rpa: bRpa, volume: vol, active: am.get(bProc)?.active ? true : false });
    pVol = vol;
    pRate = bRate / 100;
  }
  return seq;
}

// ---------------------------------------------------------------------------
// Main compute function
// ---------------------------------------------------------------------------

function computeFlowOptixV2(clientId, opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `fo-v2:${days}`;

  return getCachedOrCompute(clientId, 'flow-optix-v2', cacheKey, () => {
    return _compute(clientId, days, 'rebill');
  });
}

function computeFlowOptixV2Initials(clientId, opts = {}) {
  const days = opts.days ?? 180;
  const mode = opts.mode || 'main_initial'; // main_initial or upsell_initial
  const cacheKey = `fo-v2-init:${mode}:${days}`;

  return getCachedOrCompute(clientId, 'flow-optix-v2-initials', cacheKey, () => {
    return {
      main: _compute(clientId, days, 'main_initial'),
      upsell: _compute(clientId, days, 'upsell_initial'),
    };
  });
}

function _compute(clientId, days, mode) {
  const GW_EXCL = "COALESCE(g.exclude_from_analysis, 0) != 1";
  const BIN_EXCL = `o.cc_first_6 NOT IN (${TEST_BINS.map(b => `'${b}'`).join(',')})`;
  const PREPAID_EXCL = CRM_ROUTING_EXCLUSION; // Exclude CRM routing rule declines (e.g. prepaid blocks)

  let NAT_WHERE, SAL_WHERE;
  if (mode === 'rebill') {
    NAT_WHERE = `o.derived_product_role IN ('main_rebill', 'upsell_rebill') AND o.derived_attempt = 1
      AND o.derived_cycle IN (1, 2) AND o.is_cascaded = 0 AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.order_total NOT IN (6.96, 64.98) AND ${BIN_EXCL} AND ${PREPAID_EXCL}`;
    SAL_WHERE = `(o.derived_product_role IN ('main_rebill', 'upsell_rebill') AND o.derived_attempt >= 2
      AND o.derived_cycle IN (1, 2) AND o.is_cascaded = 0 AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.order_total NOT IN (6.96, 64.98) AND ${BIN_EXCL} AND ${PREPAID_EXCL})`;
  } else {
    // main_initial or upsell_initial
    // Include derived_attempt IS NULL to capture anonymous_decline orders (all status 7)
    NAT_WHERE = `o.derived_product_role = '${mode}' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_cascaded = 0 AND o.is_test = 0 AND o.is_internal_test = 0
      AND ${BIN_EXCL} AND ${PREPAID_EXCL}`;
    SAL_WHERE = `o.derived_product_role = '${mode}' AND o.derived_attempt >= 2
      AND o.is_cascaded = 0 AND o.is_test = 0 AND o.is_internal_test = 0
      AND ${BIN_EXCL} AND ${PREPAID_EXCL}`;
  }

  // =========================================================================
  // 0. Build active processor set + per-gateway MID info for new vs old detection
  // =========================================================================
  const activeProcessors = new Set();
  const gwInfo = new Map(); // gateway_id → { processor_name, gateway_active, gateway_alias, mcc, acquirer_bin }
  const gwRows = querySql(`
    SELECT gateway_id, processor_name, gateway_active, gateway_alias, mcc_code, acquiring_bin
    FROM gateways WHERE client_id = ? AND COALESCE(exclude_from_analysis, 0) != 1
  `, [clientId]);
  for (const gw of gwRows) {
    gwInfo.set(gw.gateway_id, gw);
    if (gw.gateway_active && gw.processor_name) activeProcessors.add(gw.processor_name);
  }

  // =========================================================================
  // 1. Fetch ALL natural attempt data
  // =========================================================================
  const natRows = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, b.card_level,
      o.cc_first_6 AS bin, o.gateway_id, g.processor_name, g.gateway_alias, g.gateway_active,
      o.order_status, o.order_total, o.acquisition_date
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${NAT_WHERE} AND ${GW_EXCL}
    ORDER BY b.issuer_bank, b.card_brand
  `, [clientId]);

  // =========================================================================
  // 1b. Fetch cascaded declines — credit decline to original gateway
  // =========================================================================
  let cascNatWhere;
  if (mode === 'rebill') {
    cascNatWhere = `o.derived_product_role IN ('main_rebill', 'upsell_rebill') AND o.derived_attempt = 1
      AND o.derived_cycle IN (1, 2) AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_total NOT IN (6.96, 64.98)
      AND ${BIN_EXCL} AND ${PREPAID_EXCL}`;
  } else {
    cascNatWhere = `o.derived_product_role = '${mode}' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL) AND o.is_test = 0 AND o.is_internal_test = 0
      AND ${BIN_EXCL} AND ${PREPAID_EXCL}`;
  }
  const cascNatRows = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, b.card_level,
      o.cc_first_6 AS bin, o.original_gateway_id AS gateway_id, g.processor_name, g.gateway_alias, g.gateway_active,
      7 AS order_status, o.order_total, o.acquisition_date
    FROM orders o
    JOIN gateways g ON o.original_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL
      AND ${cascNatWhere} AND ${GW_EXCL}
  `, [clientId]);

  // Merge cascaded declines into natural rows
  for (const cr of cascNatRows) natRows.push(cr);

  // =========================================================================
  // 1c. Fetch cascade TARGET data — kept SEPARATE from natural rows.
  //     Natural rate = "route a fresh order here, what happens?"
  //     Cascade save rate = "if another gateway declines, how well does this one rescue?"
  // =========================================================================
  const cascTargetRows = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, b.card_level,
      o.cc_first_6 AS bin, o.gateway_id, g.processor_name, g.gateway_alias, g.gateway_active,
      o.order_status, o.order_total, o.acquisition_date
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL
      AND ${cascNatWhere} AND ${GW_EXCL}
      AND ${BIN_EXCL} AND ${PREPAID_EXCL}
  `, [clientId]);
  // NOTE: cascTargetRows NOT merged into natRows — processed separately below

  // =========================================================================
  // 2. Fetch ALL salvage data
  // =========================================================================
  const salRows = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type,
      o.derived_attempt AS attempt_number, o.gateway_id, g.processor_name, g.gateway_active,
      o.order_status, o.order_total
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND ${SAL_WHERE} AND ${GW_EXCL}
  `, [clientId]);

  // 2b. Fetch cascaded salvage declines — credit decline to original gateway
  let cascSalWhere;
  if (mode === 'rebill') {
    cascSalWhere = `(o.derived_product_role IN ('main_rebill', 'upsell_rebill') AND o.derived_attempt >= 2
      AND o.derived_cycle IN (1, 2) AND o.is_test = 0 AND o.is_internal_test = 0 AND o.order_total NOT IN (6.96, 64.98)
      AND ${BIN_EXCL} AND ${PREPAID_EXCL})`;
  } else {
    cascSalWhere = `o.derived_product_role = '${mode}' AND o.derived_attempt >= 2 AND o.is_test = 0 AND o.is_internal_test = 0
      AND ${BIN_EXCL} AND ${PREPAID_EXCL}`;
  }
  const cascSalRows = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type,
      o.derived_attempt AS attempt_number, o.original_gateway_id AS gateway_id, g.processor_name, g.gateway_active,
      7 AS order_status, o.order_total
    FROM orders o
    JOIN gateways g ON o.original_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL
      AND ${cascSalWhere} AND ${GW_EXCL}
  `, [clientId]);

  for (const cr of cascSalRows) salRows.push(cr);

  // =========================================================================
  // 3. Build L2 card groups (issuer_bank + card_brand)
  // =========================================================================
  const cardMap = new Map(); // key → card data

  // Normalize bank/brand to prevent grouping splits from quote/case variations
  const normField = (v) => v ? v.replace(/"/g, '').trim().toUpperCase() : 'Unknown';

  for (const row of natRows) {
    const normBank = normField(row.issuer_bank);
    const normBrand = normField(row.card_brand);
    const key = `${normBank}|${normBrand}`;
    if (!cardMap.has(key)) {
      cardMap.set(key, {
        issuer_bank: normBank,
        card_brand: normBrand,
        bins: new Set(),
        processors: new Set(),
        totalAttempts: 0,
        totalApproved: 0,
        // L3 groups
        l3Groups: new Map(), // 'prepaid'|'non-prepaid' → { l4Groups, bins, att, app, priceData, procData }
        // Salvage data (added later)
        salvage: [],
        // Price data
        currentPrice: null,
        optimalPrice: null,
      });
    }
    const card = cardMap.get(key);
    card.bins.add(row.bin);
    if (row.processor_name) card.processors.add(row.processor_name);
    card.totalAttempts++;
    const isApp = [2, 6, 8].includes(row.order_status);
    if (isApp) card.totalApproved++;

    // Track per-gateway stats for new vs old MID detection
    if (!card._gwStats) card._gwStats = new Map();
    const gwId = row.gateway_id;
    if (!card._gwStats.has(gwId)) card._gwStats.set(gwId, { att: 0, app: 0, proc: row.processor_name, gwActive: row.gateway_active });
    const gws = card._gwStats.get(gwId);
    gws.att++;
    if (isApp) gws.app++;
    if (!card._minDate || row.acquisition_date < card._minDate) card._minDate = row.acquisition_date;
    if (!card._maxDate || row.acquisition_date > card._maxDate) card._maxDate = row.acquisition_date;

    // L3 group
    const l3Key = row.is_prepaid ? 'prepaid' : 'non-prepaid';
    if (!card.l3Groups.has(l3Key)) {
      card.l3Groups.set(l3Key, {
        is_prepaid: row.is_prepaid ? 1 : 0,
        bins: new Set(),
        att: 0, app: 0,
        l4Groups: new Map(),
        procData: new Map(), // processor → { att, app, d30_att, d30_app, d90_att, d90_app, d180_att, d180_app, active, last_seen }
        priceByProc: new Map(), // processor → Map(price → { att, app })
      });
    }
    const l3 = card.l3Groups.get(l3Key);
    l3.bins.add(row.bin);
    l3.att++;
    const isApproved = [2, 6, 8].includes(row.order_status);
    if (isApproved) l3.app++;

    // Processor data within L3
    const proc = row.processor_name || 'Unknown';
    if (!l3.procData.has(proc)) {
      l3.procData.set(proc, { att: 0, app: 0, d30_att: 0, d30_app: 0, d90_att: 0, d90_app: 0, d180_att: 0, d180_app: 0, active: activeProcessors.has(proc) ? 1 : 0, last_seen: null, aov_sum: 0, aov_count: 0 });
    }
    const pd = l3.procData.get(proc);
    pd.att++;
    if (isApproved) { pd.app++; pd.aov_sum += row.order_total; pd.aov_count++; }
    if (activeProcessors.has(proc)) pd.active = 1;
    if (!pd.last_seen || row.acquisition_date > pd.last_seen) pd.last_seen = row.acquisition_date;

    // Time buckets for weighting (compute once, apply to L3, L4, L5)
    const orderDate = new Date(row.acquisition_date);
    const daysAgo = (Date.now() - orderDate.getTime()) / (1000 * 60 * 60 * 24);
    let timeBucket = null;
    if (daysAgo <= 30) timeBucket = 'd30';
    else if (daysAgo <= 90) timeBucket = 'd90';
    else if (daysAgo <= 180) timeBucket = 'd180';

    if (timeBucket) {
      pd[timeBucket + '_att']++;
      if (isApproved) pd[timeBucket + '_app']++;
    }

    // Price data
    if (!l3.priceByProc.has(proc)) l3.priceByProc.set(proc, new Map());
    const priceMap = l3.priceByProc.get(proc);
    const price = row.order_total;
    if (!priceMap.has(price)) priceMap.set(price, { att: 0, app: 0 });
    priceMap.get(price).att++;
    if (isApproved) priceMap.get(price).app++;

    // L4 group (card_type)
    const l4Key = row.card_type || 'Unknown';
    if (!l3.l4Groups.has(l4Key)) {
      l3.l4Groups.set(l4Key, {
        card_type: l4Key,
        bins: new Set(),
        att: 0, app: 0,
        procData: new Map(),
        l5Groups: new Map(),
      });
    }
    const l4 = l3.l4Groups.get(l4Key);
    l4.bins.add(row.bin);
    l4.att++;
    if (isApproved) l4.app++;

    // L4 processor data (includes time buckets for weighting)
    if (!l4.procData.has(proc)) {
      l4.procData.set(proc, { att: 0, app: 0, d30_att: 0, d30_app: 0, d90_att: 0, d90_app: 0, d180_att: 0, d180_app: 0, active: row.gateway_active, last_seen: null, aov_sum: 0, aov_count: 0 });
    }
    const l4pd = l4.procData.get(proc);
    l4pd.att++;
    if (isApproved) { l4pd.app++; l4pd.aov_sum += row.order_total; l4pd.aov_count++; }
    if (activeProcessors.has(proc)) l4pd.active = 1;
    if (!l4pd.last_seen || row.acquisition_date > l4pd.last_seen) l4pd.last_seen = row.acquisition_date;
    if (timeBucket) { l4pd[timeBucket + '_att']++; if (isApproved) l4pd[timeBucket + '_app']++; }

    // L5 group (card_level)
    if (row.card_level) {
      const l5Key = row.card_level;
      if (!l4.l5Groups.has(l5Key)) {
        l4.l5Groups.set(l5Key, {
          card_level: l5Key,
          bins: new Set(),
          att: 0, app: 0,
          procData: new Map(),
          binStats: new Map(),
        });
      }
      const l5 = l4.l5Groups.get(l5Key);
      l5.bins.add(row.bin);
      l5.att++;
      if (isApproved) l5.app++;

      if (!l5.procData.has(proc)) {
        l5.procData.set(proc, { att: 0, app: 0, active: activeProcessors.has(proc) ? 1 : 0, last_seen: null, aov_sum: 0, aov_count: 0 });
      }
      const l5pd = l5.procData.get(proc);
      l5pd.att++;
      if (isApproved) { l5pd.app++; l5pd.aov_sum += row.order_total; l5pd.aov_count++; }
      if (activeProcessors.has(proc)) l5pd.active = 1;
      if (!l5pd.last_seen || row.acquisition_date > l5pd.last_seen) l5pd.last_seen = row.acquisition_date;

      // BIN stats for outlier detection
      if (!l5.binStats.has(row.bin)) l5.binStats.set(row.bin, { att: 0, app: 0, procRates: new Map() });
      const binSt = l5.binStats.get(row.bin);
      binSt.att++;
      if (isApproved) binSt.app++;
      if (!binSt.procRates.has(proc)) binSt.procRates.set(proc, { att: 0, app: 0 });
      const bpr = binSt.procRates.get(proc);
      bpr.att++;
      if (isApproved) bpr.app++;
    }
  }

  // =========================================================================
  // 4. Process salvage data per card+L3+L4
  // =========================================================================
  const salByL4 = new Map(); // "bank|brand|prepaid|card_type" → [rows]
  const salByL3 = new Map(); // "bank|brand|prepaid" → [rows]
  const salByL2 = new Map(); // "bank|brand" → [rows]
  for (const row of salRows) {
    const nb = normField(row.issuer_bank);
    const nbr = normField(row.card_brand);
    const l3Key = row.is_prepaid ? 'prepaid' : 'non-prepaid';
    const l4Key = row.card_type ? normField(row.card_type) : 'Unknown';
    const k4 = `${nb}|${nbr}|${l3Key}|${l4Key}`;
    const k3 = `${nb}|${nbr}|${l3Key}`;
    const k2 = `${nb}|${nbr}`;
    if (!salByL4.has(k4)) salByL4.set(k4, []);
    salByL4.get(k4).push(row);
    if (!salByL3.has(k3)) salByL3.set(k3, []);
    salByL3.get(k3).push(row);
    if (!salByL2.has(k2)) salByL2.set(k2, []);
    salByL2.get(k2).push(row);
  }

  // =========================================================================
  // 5. Build final card objects
  // =========================================================================
  const cards = [];
  const dateRangeDays = days;

  for (const [key, raw] of cardMap) {
    const card = {
      groupKey: key,
      issuer_bank: raw.issuer_bank,
      card_brand: raw.card_brand,
      totalAttempts: raw.totalAttempts,
      totalApproved: raw.totalApproved,
      totalRate: _rate(raw.totalApproved, raw.totalAttempts),
      binCount: raw.bins.size,
      processors: [...raw.processors],
      processorCount: raw.processors.size,
      bins: [...raw.bins],
      // Computed fields
      l3Groups: [],
      salvageSequence: [],
      acquisitionPriority: [],
      currentImpl: null,
      priceOptimization: null,
      highestConfidence: 'GATHERING',
      hasRecommendation: false,
      hasActiveProcessor: false,
      monthlyAttempts: 0, // set after building
    };

    // Process each L3 group
    for (const [l3Key, l3Raw] of raw.l3Groups) {
      const l3 = {
        key: l3Key,
        is_prepaid: l3Raw.is_prepaid,
        binCount: l3Raw.bins.size,
        bins: [...l3Raw.bins],
        att: l3Raw.att,
        app: l3Raw.app,
        rate: _rate(l3Raw.app, l3Raw.att),
        // Best price
        bestPrice: null,
        bestPriceRpa: 0,
        bestPriceConf: 'GATHERING',
        avgOrderValue: 0,
        // L4 sub-groups
        l4Groups: [],
      };

      // Find best price across all processors for this L3 (rebills only — initials have fixed pricing)
      if (mode === 'rebill') {
        let bestPriceRpa = 0;
        let bestPrice = null;
        let bestPriceApp = 0;
        for (const [proc, priceMap] of l3Raw.priceByProc) {
          for (const [price, data] of priceMap) {
            if (data.att >= 5) {
              const rate = _rate(data.app, data.att);
              const rpa = Math.round(rate * price) / 100;
              if (rpa > bestPriceRpa) {
                bestPriceRpa = rpa;
                bestPrice = price;
                bestPriceApp = data.app;
              }
            }
          }
        }
        l3.bestPrice = bestPrice;
        l3.bestPriceRpa = Math.round(bestPriceRpa * 100) / 100;
        l3.bestPriceConf = confidence(bestPriceApp);
      }

      // Compute avg order value from all approved
      let aovSum = 0, aovCount = 0;
      for (const pd of l3Raw.procData.values()) {
        aovSum += pd.aov_sum;
        aovCount += pd.aov_count;
      }
      l3.avgOrderValue = aovCount > 0 ? Math.round((aovSum / aovCount) * 100) / 100 : 0;

      // L3 processors (for L3-level optimization when no L4 qualifies)
      l3.processors = [];
      for (const [proc, pd] of l3Raw.procData) {
        const paov = pd.aov_count > 0 ? Math.round((pd.aov_sum / pd.aov_count) * 100) / 100 : 0;
        const rawRate = _rate(pd.app, pd.att);
        let weightedRate = rawRate;
        if (pd.active) {
          const wApp = pd.d30_app * WEIGHTS.d30 + pd.d90_app * WEIGHTS.d90 + pd.d180_app * WEIGHTS.d180;
          const wAtt = pd.d30_att * WEIGHTS.d30 + pd.d90_att * WEIGHTS.d90 + pd.d180_att * WEIGHTS.d180;
          weightedRate = wAtt > 0 ? Math.round((wApp / wAtt) * 10000) / 100 : 0;
          if (weightedRate === 0 && rawRate > 0) weightedRate = rawRate;
        }
        const effectiveRate = pd.active ? weightedRate : rawRate;
        const rpa = Math.round(effectiveRate * paov) / 100;
        l3.processors.push({
          name: proc, active: pd.active ? 1 : 0, att: pd.att, app: pd.app,
          rawRate, weightedRate: pd.active ? weightedRate : null, aov: paov, rpa,
          confidence: confidence(pd.app), lastSeen: pd.last_seen,
        });
      }
      l3.processors.sort((a, b) => {
        if (a.active !== b.active) return b.active - a.active;
        if (Math.abs(b.rpa - a.rpa) > 1) return b.rpa - a.rpa;
        return b.app - a.app;
      });
      l3.qualifies = l3.app >= 10;
      l3.singleProcessor = l3.processors.filter(p => p.app > 0).length === 1;
      l3.recommendedActive = _pickBest(l3.processors, true);
      l3.recommendedBest = _pickBest(l3.processors, false);

      // Process L4 groups
      for (const [l4Key, l4Raw] of l3Raw.l4Groups) {
        const l4 = {
          card_type: l4Raw.card_type,
          binCount: l4Raw.bins.size,
          bins: [...l4Raw.bins],
          att: l4Raw.att,
          app: l4Raw.app,
          processors: [],
          l5Groups: [],
        };

        // Build processor list for L4
        for (const [proc, pd] of l4Raw.procData) {
          const aov = pd.aov_count > 0 ? Math.round((pd.aov_sum / pd.aov_count) * 100) / 100 : 0;
          const rawRate = _rate(pd.app, pd.att);
          let weightedRate = rawRate;
          let hasRecentData = true;
          if (pd.active) {
            const wApp = pd.d30_app * WEIGHTS.d30 + pd.d90_app * WEIGHTS.d90 + pd.d180_app * WEIGHTS.d180;
            const wAtt = pd.d30_att * WEIGHTS.d30 + pd.d90_att * WEIGHTS.d90 + pd.d180_att * WEIGHTS.d180;
            weightedRate = wAtt > 0 ? Math.round((wApp / wAtt) * 10000) / 100 : 0;
            // Fallback: if no weighted data within 180d, use raw rate
            if (weightedRate === 0 && rawRate > 0) { weightedRate = rawRate; hasRecentData = false; }
          }
          const effectiveRate = pd.active ? weightedRate : rawRate;
          const rpa = Math.round(effectiveRate * aov) / 100;

          l4.processors.push({
            name: proc,
            active: pd.active ? 1 : 0,
            att: pd.att,
            app: pd.app,
            rawRate,
            weightedRate: pd.active ? weightedRate : null,
            aov,
            rpa,
            confidence: confidence(pd.app),
            lastSeen: pd.last_seen,
            d30_att: pd.d30_att, d30_app: pd.d30_app,
            d90_att: pd.d90_att, d90_app: pd.d90_app,
            d180_att: pd.d180_att, d180_app: pd.d180_app,
          });

          // Track card-level flags
          if (pd.active) card.hasActiveProcessor = true;
          if (pd.app >= 10) card.hasRecommendation = true;
          const c = confidence(pd.app);
          if (c === 'HIGH' || (c === 'MEDIUM' && card.highestConfidence !== 'HIGH') || (c === 'LOW' && card.highestConfidence === 'GATHERING')) {
            card.highestConfidence = c;
          }
        }

        // Sort: active first, then by RPA desc, then by approved count desc (breaks ties)
        l4.processors.sort((a, b) => {
          if (a.active !== b.active) return b.active - a.active;
          if (Math.abs(b.rpa - a.rpa) > 1) return b.rpa - a.rpa;
          return b.app - a.app; // Break near-ties by data volume
        });
        l4.qualifies = l4.app >= 10;
        l4.singleProcessor = l4.processors.filter(p => p.app > 0).length === 1;
        l4.recommendedActive = _pickBest(l4.processors, true);
        l4.recommendedBest = _pickBest(l4.processors, false);

        // Process L5 groups
        for (const [l5Key, l5Raw] of l4Raw.l5Groups) {
          if (l5Raw.att === 0) continue; // Skip L5 with no data at all
          const l5 = {
            card_level: l5Raw.card_level,
            binCount: l5Raw.bins.size,
            bins: [...l5Raw.bins],
            att: l5Raw.att,
            app: l5Raw.app,
            qualifies: l5Raw.app >= 10,
            processors: [],
            binOutliers: [],
          };
          for (const [proc, pd] of l5Raw.procData) {
            if (pd.app === 0 && !pd.active) continue;
            const aov = pd.aov_count > 0 ? Math.round((pd.aov_sum / pd.aov_count) * 100) / 100 : 0;
            const rawRate = _rate(pd.app, pd.att);
            l5.processors.push({
              name: proc, active: pd.active ? 1 : 0, att: pd.att, app: pd.app,
              rawRate, aov, rpa: Math.round(rawRate * aov) / 100,
              confidence: confidence(pd.app), lastSeen: pd.last_seen,
            });
          }
          l5.processors.sort((a, b) => { if (a.active !== b.active) return b.active - a.active; if (Math.abs(b.rpa - a.rpa) > 1) return b.rpa - a.rpa; return b.app - a.app; });
          l5.singleProcessor = l5.processors.filter(p => p.app > 0).length === 1;
          l5.recommendedActive = _pickBest(l5.processors, true);
          l5.recommendedBest = _pickBest(l5.processors, false);

          // BIN outlier detection (>5pp from L5 average, BIN must have ≥10 approved)
          if (l5.qualifies && l5Raw.binStats) {
            const l5Rate = _rate(l5Raw.app, l5Raw.att);
            for (const [bin, bs] of l5Raw.binStats) {
              if (bs.app >= 10) {
                const binRate = _rate(bs.app, bs.att);
                if (Math.abs(binRate - l5Rate) > 5) {
                  let bestProc = null, bestRate = 0;
                  for (const [proc, pr] of bs.procRates) {
                    const r = _rate(pr.app, pr.att);
                    if (r > bestRate) { bestRate = r; bestProc = proc; }
                  }
                  l5.binOutliers.push({
                    bin, att: bs.att, app: bs.app,
                    rate: binRate, l5Rate,
                    delta: Math.round((binRate - l5Rate) * 100) / 100,
                    bestProcessor: bestProc, bestRate,
                  });
                }
              }
            }
            l5.binOutliers.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
          }

          l4.l5Groups.push(l5);
        }
        l4.l5Groups.sort((a, b) => b.app - a.app);

        // Salvage sequence — cascade: L4 → L3 → L2 (rebills only — initials have cascade rules instead)
        const l3Key = l3.key || (l3Raw.is_prepaid ? 'prepaid' : 'non-prepaid');
        if (mode !== 'rebill') {
          l4.salvageSequence = [];
          l4.salvageLevel = null;
        } else {
        const salGDays = raw._minDate && raw._maxDate ? Math.max(30, Math.round((Date.now() - new Date(raw._minDate).getTime()) / (1000 * 60 * 60 * 24))) : dateRangeDays;
        const l4BestActive = l4.processors.find(p => p.active);
        const l4MonthlyAtt = Math.round(l4.att * 30 / salGDays);

        // Try L4 first
        const salL4Key = `${raw.issuer_bank || 'Unknown'}|${raw.card_brand || 'Unknown'}|${l3Key}|${l4Key}`;
        let salSeq = _buildSalvageSeq(salByL4.get(salL4Key), l4BestActive, l4MonthlyAtt, 3, activeProcessors);
        let salLevel = 'L4';

        // If L4 has no att 2+ data (only att 1 entry or empty), fall back to L3
        if (salSeq.length <= 1 || (salSeq.length === 2 && salSeq[1].isStop)) {
          const salL3Key = `${raw.issuer_bank || 'Unknown'}|${raw.card_brand || 'Unknown'}|${l3Key}`;
          const l3BestActive = (l3.processors || []).find(p => p.active) || l4BestActive;
          const l3MonthlyAtt = Math.round(l3.att * 30 / salGDays);
          const l3Seq = _buildSalvageSeq(salByL3.get(salL3Key), l3BestActive, l3MonthlyAtt, 4, activeProcessors);
          if (l3Seq.length > salSeq.length) { salSeq = l3Seq; salLevel = 'L3'; }
        }

        // If L3 also insufficient, fall back to L2 (brand)
        if (salSeq.length <= 1 || (salSeq.length === 2 && salSeq[1].isStop)) {
          const salL2Key = `${raw.issuer_bank || 'Unknown'}|${raw.card_brand || 'Unknown'}`;
          const l2MonthlyAtt = Math.round(raw.totalAttempts * 30 / salGDays);
          // Find brand-level best active across all L3/L4
          let l2BestActive = null, l2BestRpa = 0;
          for (const cl3 of card.l3Groups) {
            for (const cl4 of cl3.l4Groups) {
              const ba = cl4.processors.find(p => p.active);
              if (ba && ba.rpa > l2BestRpa) { l2BestActive = ba; l2BestRpa = ba.rpa; }
            }
          }
          const l2Seq = _buildSalvageSeq(salByL2.get(salL2Key), l2BestActive || l4BestActive, l2MonthlyAtt, 5, activeProcessors);
          if (l2Seq.length > salSeq.length) { salSeq = l2Seq; salLevel = 'L2'; }
        }

        l4.salvageSequence = salSeq;
        l4.salvageLevel = salLevel;
        } // end rebill-only salvage block

        l3.l4Groups.push(l4);
      }
      l3.l4Groups.sort((a, b) => b.app - a.app);

      card.l3Groups.push(l3);
    }
    card.l3Groups.sort((a, b) => b.app - a.app);

    // Best active processor (used for currentImpl)
    let bestActiveProc = null;
    let bestActiveRpa = 0;
    for (const l3 of card.l3Groups) {
      for (const l4 of l3.l4Groups) {
        for (const p of l4.processors) {
          if (p.active && p.rpa > bestActiveRpa) {
            bestActiveRpa = p.rpa;
            bestActiveProc = p;
          }
        }
      }
    }

    // Use actual date range for this group, not a fixed window
    const groupDays = raw._minDate && raw._maxDate
      ? Math.max(30, Math.round((Date.now() - new Date(raw._minDate).getTime()) / (1000 * 60 * 60 * 24)))
      : dateRangeDays;
    const monthlyAtt = Math.round(card.totalAttempts * 30 / groupDays);
    card.monthlyAttempts = monthlyAtt;

    // =====================================================================
    // Acquisition priority
    // =====================================================================
    const activeProcRpas = new Map();
    const histProcRpas = new Map();
    for (const l3 of card.l3Groups) {
      for (const l4 of l3.l4Groups) {
        for (const p of l4.processors) {
          if (p.active) {
            const cur = activeProcRpas.get(p.name) || { totalRpa: 0, count: 0 };
            cur.totalRpa += p.rpa * p.att;
            cur.count += p.att;
            activeProcRpas.set(p.name, cur);
          } else if (p.app >= 10) {
            if (!histProcRpas.has(p.name)) histProcRpas.set(p.name, { wins: [], totalUnlock: 0 });
            const bestActive = [...activeProcRpas.entries()].reduce((best, [, v]) =>
              v.count > 0 && (v.totalRpa / v.count) > (best?.rpa || 0) ? { rpa: v.totalRpa / v.count } : best, null);
            const activeRpa = bestActive?.rpa || 0;
            if (p.rpa - activeRpa >= HIST_BETTER_PP) {
              const entry = histProcRpas.get(p.name);
              entry.wins.push(`${l3.key === 'prepaid' ? 'Prepaid' : ''} ${l4.card_type} (${p.rawRate}%)`);
              entry.totalUnlock += (p.rpa - activeRpa) * Math.round(l4.att * 30 / dateRangeDays);
            }
          }
        }
      }
    }
    for (const [proc, data] of histProcRpas) {
      if (data.wins.length > 0) {
        card.acquisitionPriority.push({
          processor: proc,
          wins: data.wins.join(' · '),
          revenueUnlock: Math.round(data.totalUnlock * 100) / 100,
        });
      }
    }
    card.acquisitionPriority.sort((a, b) => b.revenueUnlock - a.revenueUnlock);

    // =====================================================================
    // Current implementation
    // =====================================================================
    if (bestActiveProc) {
      card.currentImpl = {
        bestActive: { name: bestActiveProc.name, rate: bestActiveProc.weightedRate || bestActiveProc.rawRate, rpa: bestActiveRpa, aov: bestActiveProc.aov },
      };
      // Find best historical
      let bestHistRpa = 0;
      let bestHistProc = null;
      for (const l3 of card.l3Groups) {
        for (const l4 of l3.l4Groups) {
          for (const p of l4.processors) {
            if (!p.active && p.app >= 10 && p.rpa > bestHistRpa) {
              bestHistRpa = p.rpa;
              bestHistProc = p;
            }
          }
        }
      }
      if (bestHistProc && bestHistRpa - bestActiveRpa >= HIST_BETTER_PP) {
        card.currentImpl.bestHistorical = { name: bestHistProc.name, rate: bestHistProc.rawRate, rpa: bestHistRpa, aov: bestHistProc.aov };
      }
    }

    // =====================================================================
    // Price optimization
    // =====================================================================
    // Find most common price in recent data
    const priceCounts = new Map();
    for (const l3 of raw.l3Groups.values()) {
      for (const [, priceMap] of l3.priceByProc) {
        for (const [price, data] of priceMap) {
          priceCounts.set(price, (priceCounts.get(price) || 0) + data.att);
        }
      }
    }
    let currentPrice = null;
    let maxCount = 0;
    for (const [price, count] of priceCounts) {
      if (count > maxCount) { maxCount = count; currentPrice = price; }
    }
    // Find optimal price (highest RPA across all)
    let optimalPrice = null;
    let optimalRpa = 0;
    for (const l3 of card.l3Groups) {
      if (l3.bestPrice && l3.bestPriceRpa > optimalRpa) {
        optimalRpa = l3.bestPriceRpa;
        optimalPrice = l3.bestPrice;
      }
    }
    // Price optimization only for rebills — initials have fixed pricing
    if (mode === 'rebill' && currentPrice && optimalPrice && currentPrice !== optimalPrice) {
      const currentRpa = _rate(raw.totalApproved, raw.totalAttempts) * currentPrice / 100;
      const rpaDiff = optimalRpa - currentRpa;
      // Only show alert when optimal is meaningfully better ($2+ RPA difference)
      if (rpaDiff >= 2) {
        card.priceOptimization = {
          currentPrice,
          currentRpa: Math.round(currentRpa * 100) / 100,
          currentRate: _rate(raw.totalApproved, raw.totalAttempts),
          optimalPrice,
          optimalRpa: Math.round(optimalRpa * 100) / 100,
          optimalRate: optimalPrice > 0 ? Math.round(optimalRpa / optimalPrice * 10000) / 100 : 0,
          monthlyImpact: Math.round(rpaDiff * monthlyAtt * 100) / 100,
        };
      }
    }

    // MID divergence detection — compare old vs new gateways per processor
    card.midDivergence = [];
    if (raw._gwStats) {
      // Group gateway stats by processor
      const procGws = new Map();
      for (const [gwId, gs] of raw._gwStats) {
        if (!gs.proc || gs.att < 3) continue;
        if (!procGws.has(gs.proc)) procGws.set(gs.proc, []);
        const gw = gwInfo.get(gwId);
        procGws.get(gs.proc).push({
          gwId, att: gs.att, app: gs.app,
          rate: _rate(gs.app, gs.att),
          isNew: gw?.gateway_active ? true : false,
          alias: gw?.gateway_alias || '',
          mcc: gw?.mcc_code || null,
          acquirerBin: gw?.acquiring_bin || null,
        });
      }
      for (const [proc, gws] of procGws) {
        const newGws = gws.filter(g => g.isNew);
        const oldGws = gws.filter(g => !g.isNew);
        if (newGws.length === 0 || oldGws.length === 0) continue;
        const newAtt = newGws.reduce((s, g) => s + g.att, 0);
        const newApp = newGws.reduce((s, g) => s + g.app, 0);
        const oldAtt = oldGws.reduce((s, g) => s + g.att, 0);
        const oldApp = oldGws.reduce((s, g) => s + g.app, 0);
        if (newAtt < 5 || oldAtt < 5) continue; // need minimum data
        const newRate = _rate(newApp, newAtt);
        const oldRate = _rate(oldApp, oldAtt);
        const delta = Math.round((newRate - oldRate) * 100) / 100;
        if (Math.abs(delta) > 5) { // >5pp divergence
          // Check if MCC or acquirer BIN differs
          const newMccs = [...new Set(newGws.map(g => g.mcc).filter(Boolean))];
          const oldMccs = [...new Set(oldGws.map(g => g.mcc).filter(Boolean))];
          const mccDiffers = newMccs.length > 0 && oldMccs.length > 0 && newMccs.join() !== oldMccs.join();
          const newAcq = [...new Set(newGws.map(g => g.acquirerBin).filter(Boolean))];
          const oldAcq = [...new Set(oldGws.map(g => g.acquirerBin).filter(Boolean))];
          const acqDiffers = newAcq.length > 0 && oldAcq.length > 0 && newAcq.join() !== oldAcq.join();
          card.midDivergence.push({
            processor: proc,
            newRate, newAtt, newApp,
            oldRate, oldAtt, oldApp,
            delta,
            mccDiffers, mccNew: newMccs, mccOld: oldMccs,
            acqDiffers, acqNew: newAcq, acqOld: oldAcq,
          });
        }
      }
    }

    // Cascade save rates — separate from natural performance.
    // "If another gateway declines, how well does this one rescue?"
    {
      const bankBrand = `${raw.issuer_bank || 'Unknown'}|${raw.card_brand || 'Unknown'}`;
      const cascRows = cascTargetRows.filter(r =>
        `${r.issuer_bank || 'Unknown'}|${r.card_brand || 'Unknown'}` === bankBrand
      );
      const cascByProc = new Map();
      for (const r of cascRows) {
        const proc = r.processor_name || 'Unknown';
        if (!cascByProc.has(proc)) cascByProc.set(proc, { att: 0, app: 0, active: false });
        const p = cascByProc.get(proc);
        p.att++;
        if ([2, 6, 8].includes(r.order_status)) p.app++;
        if (r.gateway_active) p.active = true;
      }
      const cascadeTargets = [];
      for (const [proc, stats] of cascByProc) {
        if (stats.att >= 3) {
          cascadeTargets.push({
            name: proc,
            att: stats.att,
            app: stats.app,
            rate: _rate(stats.app, stats.att),
            active: stats.active,
          });
        }
      }
      cascadeTargets.sort((a, b) => b.rate - a.rate);
      card.cascadeTargets = cascadeTargets;
      card.cascadeTotal = cascRows.length;
      card.cascadeApproved = cascRows.filter(r => [2, 6, 8].includes(r.order_status)).length;
    }

    cards.push(card);
  }

  // =========================================================================
  // Rebill blocking signal — for initials only
  // Flag banks that approve initials but block rebills
  // =========================================================================
  let rebillByBank = null;
  if (mode !== 'rebill') {
    rebillByBank = new Map();
    const rebillRows = querySql(`
      SELECT b.issuer_bank,
        COUNT(*) AS att,
        COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) AS app
      FROM orders o
      JOIN bin_lookup b ON o.cc_first_6 = b.bin
      WHERE o.client_id = ? AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
        AND o.derived_attempt = 1 AND o.is_cascaded = 0
        AND o.is_test = 0 AND o.is_internal_test = 0
      GROUP BY b.issuer_bank
      HAVING att >= 10
    `, [clientId]);
    for (const r of rebillRows) {
      rebillByBank.set(r.issuer_bank, {
        att: r.att,
        app: r.app,
        rate: r.att > 0 ? Math.round(r.app / r.att * 10000) / 100 : 0,
      });
    }
  }

  // =========================================================================
  // REGROUP: Merge L2 cards into L1 bank cards
  // =========================================================================
  const bankMap = new Map();
  for (const card of cards) {
    const bank = card.issuer_bank;
    if (!bankMap.has(bank)) {
      bankMap.set(bank, {
        issuer_bank: bank,
        brands: [],
        totalAttempts: 0,
        totalApproved: 0,
        binCount: 0,
        bins: [],
        processorCount: 0,
        processors: new Set(),
        highestConfidence: 'GATHERING',
        hasRecommendation: false,
        hasActiveProcessor: false,
        maxApproved: 0,
        totalRevenueUnlock: 0,
        // Best active/historical across all brands (for collapsed state)
        bestActive: null,
        bestHistorical: null,
        priceOptimization: null,
      });
    }
    const bankCard = bankMap.get(bank);

    // Brand qualifies if brand total approved >= 10
    const brandQualifies = card.totalApproved >= 10;

    bankCard.brands.push({
      card_brand: card.card_brand,
      qualifies: brandQualifies,
      ...card, // spread all L2 card data
    });

    bankCard.totalAttempts += card.totalAttempts;
    bankCard.totalApproved += card.totalApproved;
    bankCard.binCount += card.binCount;
    bankCard.bins.push(...card.bins);
    for (const p of card.processors || []) bankCard.processors.add(p);
    if (card.hasActiveProcessor) bankCard.hasActiveProcessor = true;
    if (card.hasRecommendation) bankCard.hasRecommendation = true;

    // Track highest confidence and max approved
    const confRank = { HIGH: 3, MEDIUM: 2, LOW: 1, GATHERING: 0 };
    if ((confRank[card.highestConfidence] || 0) > (confRank[bankCard.highestConfidence] || 0)) {
      bankCard.highestConfidence = card.highestConfidence;
    }
    if (card.totalApproved > bankCard.maxApproved) bankCard.maxApproved = card.totalApproved;

    // Accumulate revenue unlock
    bankCard.totalRevenueUnlock += (card.acquisitionPriority || []).reduce((s, a) => s + a.revenueUnlock, 0);

    // Best active across all brands
    if (card.currentImpl?.bestActive) {
      if (!bankCard.bestActive || card.currentImpl.bestActive.rpa > bankCard.bestActive.rpa) {
        bankCard.bestActive = card.currentImpl.bestActive;
      }
    }
    if (card.currentImpl?.bestHistorical) {
      if (!bankCard.bestHistorical || card.currentImpl.bestHistorical.rpa > bankCard.bestHistorical.rpa) {
        bankCard.bestHistorical = card.currentImpl.bestHistorical;
      }
    }

    // Best price optimization
    if (card.priceOptimization && (!bankCard.priceOptimization || card.priceOptimization.monthlyImpact > bankCard.priceOptimization.monthlyImpact)) {
      bankCard.priceOptimization = card.priceOptimization;
    }

    // Merge cascade targets (initials only)
    if (card.cascadeTargets) {
      if (!bankCard.cascadeTargets) { bankCard.cascadeTargets = []; bankCard.cascadeTotal = 0; bankCard.cascadeApproved = 0; }
      bankCard.cascadeTotal += card.cascadeTotal || 0;
      bankCard.cascadeApproved += card.cascadeApproved || 0;
      // Merge processor-level stats
      for (const ct of card.cascadeTargets) {
        const existing = bankCard.cascadeTargets.find(t => t.name === ct.name);
        if (existing) { existing.att += ct.att; existing.app += ct.app; existing.rate = _rate(existing.app, existing.att); if (ct.active) existing.active = true; }
        else { bankCard.cascadeTargets.push({ ...ct }); }
      }
    }

    // Rebill blocking signal
    if (rebillByBank) {
      const rb = rebillByBank.get(bank);
      if (rb) {
        bankCard.rebillSignal = { rate: rb.rate, att: rb.att, app: rb.app, blocking: rb.rate < 5 };
      } else {
        bankCard.rebillSignal = { rate: null, att: 0, app: 0, blocking: false, noData: true };
      }
    }
  }

  // Finalize bank cards
  const bankCards = [];
  for (const [, bc] of bankMap) {
    bc.processorCount = bc.processors.size;
    bc.processors = [...bc.processors];

    // Merge same-brand entries (from normalized bank names)
    const brandMerged = new Map();
    for (const br of bc.brands) {
      const bk = br.card_brand;
      if (!brandMerged.has(bk)) {
        brandMerged.set(bk, br);
      } else {
        // Merge L3 groups, bins, totals into existing brand
        const existing = brandMerged.get(bk);
        existing.totalAttempts += br.totalAttempts;
        existing.totalApproved += br.totalApproved;
        existing.binCount += br.binCount;
        existing.bins = [...(existing.bins || []), ...(br.bins || [])];

        // Deep-merge L3 groups by key (prepaid/non-prepaid)
        const l3Map = new Map();
        for (const l3 of existing.l3Groups) l3Map.set(l3.key, l3);
        for (const l3 of (br.l3Groups || [])) {
          if (!l3Map.has(l3.key)) {
            l3Map.set(l3.key, l3);
          } else {
            const el3 = l3Map.get(l3.key);
            el3.att += l3.att;
            el3.app += l3.app;
            el3.rate = _rate(el3.app, el3.att);
            el3.binCount += l3.binCount;
            el3.bins = [...(el3.bins || []), ...(l3.bins || [])];
            el3.qualifies = el3.app >= 10;

            // Merge L3 processors by name
            const procMap = new Map();
            for (const p of el3.processors) procMap.set(p.name, p);
            for (const p of (l3.processors || [])) {
              if (!procMap.has(p.name)) {
                procMap.set(p.name, p);
              } else {
                const ep = procMap.get(p.name);
                ep.att += p.att;
                ep.app += p.app;
                ep.rawRate = _rate(ep.app, ep.att);
                if (ep.active || p.active) ep.active = 1;
                ep.confidence = confidence(ep.app);
              }
            }
            el3.processors = [...procMap.values()];
            el3.processors.sort((a, b) => { if (a.active !== b.active) return b.active - a.active; if (Math.abs(b.rpa - a.rpa) > 1) return b.rpa - a.rpa; return b.app - a.app; });
            el3.singleProcessor = el3.processors.filter(p => p.app > 0).length === 1;

            // Deep-merge L4 groups by card_type
            const l4Map = new Map();
            for (const l4 of el3.l4Groups) l4Map.set(l4.card_type, l4);
            for (const l4 of (l3.l4Groups || [])) {
              if (!l4Map.has(l4.card_type)) {
                l4Map.set(l4.card_type, l4);
              } else {
                const el4 = l4Map.get(l4.card_type);
                el4.att += l4.att;
                el4.app += l4.app;
                el4.binCount += l4.binCount;
                el4.bins = [...(el4.bins || []), ...(l4.bins || [])];
                el4.qualifies = el4.app >= 10;

                // Merge L4 processors by name
                const l4ProcMap = new Map();
                for (const p of el4.processors) l4ProcMap.set(p.name, p);
                for (const p of (l4.processors || [])) {
                  if (!l4ProcMap.has(p.name)) {
                    l4ProcMap.set(p.name, p);
                  } else {
                    const ep = l4ProcMap.get(p.name);
                    ep.att += p.att;
                    ep.app += p.app;
                    ep.rawRate = _rate(ep.app, ep.att);
                    if (ep.active || p.active) ep.active = 1;
                    ep.confidence = confidence(ep.app);
                  }
                }
                el4.processors = [...l4ProcMap.values()];
                el4.processors.sort((a, b) => { if (a.active !== b.active) return b.active - a.active; if (Math.abs(b.rpa - a.rpa) > 1) return b.rpa - a.rpa; return b.app - a.app; });
                el4.singleProcessor = el4.processors.filter(p => p.app > 0).length === 1;

                // Deep-merge L5 groups by card_level
                const l5Map = new Map();
                for (const l5 of el4.l5Groups) l5Map.set(l5.card_level, l5);
                for (const l5 of (l4.l5Groups || [])) {
                  if (!l5Map.has(l5.card_level)) {
                    l5Map.set(l5.card_level, l5);
                  } else {
                    const el5 = l5Map.get(l5.card_level);
                    el5.att += l5.att;
                    el5.app += l5.app;
                    el5.binCount += l5.binCount;
                    el5.bins = [...(el5.bins || []), ...(l5.bins || [])];
                    el5.qualifies = el5.app >= 10;
                    // Merge L5 processors
                    const l5ProcMap = new Map();
                    for (const p of el5.processors) l5ProcMap.set(p.name, p);
                    for (const p of (l5.processors || [])) {
                      if (!l5ProcMap.has(p.name)) {
                        l5ProcMap.set(p.name, p);
                      } else {
                        const ep = l5ProcMap.get(p.name);
                        ep.att += p.att;
                        ep.app += p.app;
                        ep.rawRate = _rate(ep.app, ep.att);
                        if (ep.active || p.active) ep.active = 1;
                        ep.confidence = confidence(ep.app);
                      }
                    }
                    el5.processors = [...l5ProcMap.values()];
                    el5.processors.sort((a, b) => { if (a.active !== b.active) return b.active - a.active; if (Math.abs(b.rpa - a.rpa) > 1) return b.rpa - a.rpa; return b.app - a.app; });
                    el5.singleProcessor = el5.processors.filter(p => p.app > 0).length === 1;
                    // Merge BIN outliers (dedupe by bin)
                    const boSet = new Set(el5.binOutliers.map(o => o.bin));
                    for (const o of (l5.binOutliers || [])) {
                      if (!boSet.has(o.bin)) el5.binOutliers.push(o);
                    }
                  }
                }
                el4.l5Groups = [...l5Map.values()];
                el4.l5Groups.sort((a, b) => b.app - a.app);
              }
            }
            el3.l4Groups = [...l4Map.values()];
            el3.l4Groups.sort((a, b) => b.app - a.app);
          }
        }
        existing.l3Groups = [...l3Map.values()];
        existing.l3Groups.sort((a, b) => b.app - a.app);

        // Re-check qualifies with merged data (brand total >= 10)
        existing.qualifies = existing.totalApproved >= 10;
        // Merge implementation
        if (br.currentImpl?.bestActive && (!existing.currentImpl?.bestActive || br.currentImpl.bestActive.rpa > existing.currentImpl.bestActive.rpa)) {
          existing.currentImpl = br.currentImpl;
        }
        if (br.priceOptimization && (!existing.priceOptimization || br.priceOptimization.monthlyImpact > existing.priceOptimization.monthlyImpact)) {
          existing.priceOptimization = br.priceOptimization;
        }
        // Merge salvage + acquisition
        if (br.salvageSequence?.length > (existing.salvageSequence?.length || 0)) existing.salvageSequence = br.salvageSequence;
        existing.acquisitionPriority = [...(existing.acquisitionPriority || []), ...(br.acquisitionPriority || [])];
      }
    }
    bc.brands = [...brandMerged.values()];

    bc.brandCount = bc.brands.filter(b => b.qualifies).length;
    bc.brands.sort((a, b) => b.totalApproved - a.totalApproved);
    // Card visibility: bank total approved >= 20
    if (bc.totalApproved >= 20 || bc.totalAttempts >= 50) bc.hasRecommendation = true;

    // If bank qualifies, force all sub-levels to qualify — show everything
    if (bc.hasRecommendation) {
      for (const br of bc.brands) {
        if (!br.qualifies && (br.totalApproved >= 1 || br.totalAttempts >= 10)) br.qualifies = true;
        for (const l3 of (br.l3Groups || [])) {
          if (!l3.qualifies && l3.att > 0) l3.qualifies = true;
          for (const l4 of (l3.l4Groups || [])) {
            if (!l4.qualifies && l4.att > 0) l4.qualifies = true;
            for (const l5 of (l4.l5Groups || [])) {
              if (!l5.qualifies && l5.att > 0) l5.qualifies = true;
            }
          }
        }
      }
    }

    // Recompute collapsed metrics from ALL processor data across ALL brands
    const bankProcs = new Map(); // proc → { att, app, active, aov_sum, aov_count, d30_att, d30_app, d90_att, d90_app, d180_att, d180_app }
    const bankPrices = new Map(); // price → { att, app }
    for (const br of bc.brands) {
      for (const l3 of (br.l3Groups || [])) {
        for (const l4 of l3.l4Groups) {
          for (const p of l4.processors) {
            if (!bankProcs.has(p.name)) {
              bankProcs.set(p.name, { att: 0, app: 0, active: 0, aov_sum: 0, aov_count: 0, d30_att: 0, d30_app: 0, d90_att: 0, d90_app: 0, d180_att: 0, d180_app: 0 });
            }
            const bp = bankProcs.get(p.name);
            bp.att += p.att; bp.app += p.app;
            if (p.active) bp.active = 1;
            bp.aov_sum += (p.aov || 0) * p.app; bp.aov_count += p.app;
            // Time buckets from raw L4 procData if available
            if (p.d30_att != null) { bp.d30_att += p.d30_att; bp.d30_app += p.d30_app; bp.d90_att += p.d90_att; bp.d90_app += p.d90_app; bp.d180_att += p.d180_att; bp.d180_app += p.d180_app; }
          }
        }
        // Collect prices
        if (br.l3Groups) {
          for (const [proc, priceMap] of (l3.priceByProc || new Map())) {
            for (const [price, data] of priceMap) {
              if (!bankPrices.has(price)) bankPrices.set(price, { att: 0, app: 0 });
              bankPrices.get(price).att += data.att;
              bankPrices.get(price).app += data.app;
            }
          }
        }
      }
    }

    // Build processor-like array from bankProcs for _pickBest
    const bankProcList = [];
    for (const [name, bp] of bankProcs) {
      if (bp.app < 5) continue;
      const aov = bp.aov_count > 0 ? bp.aov_sum / bp.aov_count : 0;
      const rawRate = _rate(bp.app, bp.att);
      let weightedRate = rawRate;
      if (bp.active) {
        const wApp = bp.d30_app * 0.5 + bp.d90_app * 0.3 + bp.d180_app * 0.2;
        const wAtt = bp.d30_att * 0.5 + bp.d90_att * 0.3 + bp.d180_att * 0.2;
        weightedRate = wAtt > 0 ? Math.round((wApp / wAtt) * 10000) / 100 : 0;
        if (weightedRate === 0 && rawRate > 0) weightedRate = rawRate;
      }
      const effectiveRate = bp.active ? weightedRate : rawRate;
      const rpa = Math.round(effectiveRate * aov) / 100;
      bankProcList.push({
        name, active: bp.active ? 1 : 0, att: bp.att, app: bp.app,
        rawRate, weightedRate: bp.active ? weightedRate : null,
        aov: Math.round(aov * 100) / 100, rpa,
        confidence: confidence(bp.app),
      });
    }

    // Best active — confidence-weighted
    const bestAct = _pickBest(bankProcList, true);
    bc.bestActive = bestAct ? { name: bestAct.name, rate: bestAct.weightedRate || bestAct.rawRate, rpa: bestAct.rpa, aov: bestAct.aov, app: bestAct.app, att: bestAct.att } : null;

    // Best historical — confidence-weighted
    const bestHist = _pickBest(bankProcList, false);
    const bestActiveRpa = bc.bestActive?.rpa || 0;
    bc.bestHistorical = (bestHist && bestHist.rpa >= bestActiveRpa + 2) ? { name: bestHist.name, rate: bestHist.rawRate, rpa: bestHist.rpa, aov: bestHist.aov, app: bestHist.app, att: bestHist.att } : null;

    // Price optimization — bubble up best brand-level finding (already set at brand merge)
    // bc.priceOptimization was set during brand merging at line ~865, keep it as-is

    bankCards.push(bc);
  }

  // Sort: recommendation first by revenue_unlock DESC, then approved DESC
  bankCards.sort((a, b) => {
    if (a.hasRecommendation !== b.hasRecommendation) return b.hasRecommendation ? 1 : -1;
    if (a.totalRevenueUnlock !== b.totalRevenueUnlock) return b.totalRevenueUnlock - a.totalRevenueUnlock;
    return b.maxApproved - a.maxApproved;
  });

  // =========================================================================
  // Summary
  // =========================================================================
  const avgOvRow = querySql(
    `SELECT AVG(order_total) AS avg_val FROM orders WHERE client_id = ? AND derived_product_role IN ('main_rebill','upsell_rebill') AND order_status IN (2,6,8) AND is_test = 0 AND is_internal_test = 0 AND order_total NOT IN (6.96, 64.98)`,
    [clientId]
  );
  const avgOrderValue = Math.round((avgOvRow[0]?.avg_val || 70) * 100) / 100;

  const qualifyingBanks = bankCards.filter(c => c.hasRecommendation);
  const gatheringBanks = bankCards.filter(c => !c.hasRecommendation);

  const summary = {
    totalBanks: bankCards.length,
    qualifyingBanks: qualifyingBanks.length,
    gatheringBanks: gatheringBanks.length,
    avgOrderValue,
    confidenceCounts: {
      HIGH: bankCards.filter(c => c.highestConfidence === 'HIGH' || c.maxApproved >= 20).length,
      MEDIUM: bankCards.filter(c => c.maxApproved >= 10 && c.maxApproved < 20).length,
      GATHERING: bankCards.filter(c => c.maxApproved < 10).length,
    },
  };

  return { cards: bankCards, summary };
}

module.exports = { computeFlowOptixV2, computeFlowOptixV2Initials, _compute };
