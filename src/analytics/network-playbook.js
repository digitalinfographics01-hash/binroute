/**
 * Network Playbook — Unified cross-client routing intelligence.
 *
 * Mirrors routing-playbook.js structure but pools ALL client data
 * for higher confidence. Adds cross-client agreement indicators.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql } = require('../db/connection');
const {
  getCachedOrCompute, CLEAN_FILTER, CRM_ROUTING_EXCLUSION,
  setForceCompute,
} = require('./engine');
const {
  normalizeProcessor, normalizeBank, _rate, _getClients, BIN_EXCL,
  clearNetworkCache, NETWORK_CLIENT_ID,
} = require('./network-analysis');

const MIN_APP_CONFIDENT = 20;
const MIN_ATT_SIGNAL = 10;
const MIN_C1_CONFIDENT = 30;
const MIN_C1_EARLY = 10;
const STOP_RPA = 3;

function classifyRebillTier(c1Rate, c1Att, c1App) {
  if (c1Att < MIN_C1_EARLY) return 'NO_DATA';
  const isEarly = c1Att < MIN_C1_CONFIDENT;
  const prefix = isEarly ? 'Early: ' : '';
  if (c1App === 0) return prefix + 'UNTESTED';
  if (c1Rate < 3) return prefix + 'HOSTILE';
  if (c1Rate < 10) return prefix + 'RESISTANT';
  if (c1Rate < 25) return prefix + 'VIABLE';
  return prefix + 'STRONG';
}

function _groupBy(rows, keyFn) {
  const map = new Map();
  for (const r of rows) {
    const key = keyFn(r);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(r);
  }
  return map;
}

function _bk(bank, pp) { return `${bank}|${pp}`; }

// ─── Agreement helpers ─────────────────────────────────────────────

function _computeAgreement(procsByClient, networkBest, clientNames) {
  if (!procsByClient || procsByClient.size === 0) return null;
  const perClient = [];
  let agree = 0;
  for (const [clientId, procs] of procsByClient) {
    const best = procs.sort((a, b) => b.rate - a.rate)[0];
    if (best) {
      const agrees = normalizeProcessor(best.processor) === normalizeProcessor(networkBest);
      if (agrees) agree++;
      perClient.push({
        clientId,
        clientName: clientNames.get(clientId) || `Client ${clientId}`,
        best: best.processor,
        rate: best.rate,
        att: best.att,
        agrees,
      });
    }
  }
  return { networkBest, agree, total: perClient.length, perClient };
}

// Split rows by client and find each client's best processor
function _clientBestMap(rows, procField = 'processor_name', rateField = null) {
  const map = new Map();
  for (const r of rows) {
    if (!map.has(r.client_id)) map.set(r.client_id, []);
    const rate = rateField ? r[rateField] : (r.att > 0 ? Math.round(r.app / r.att * 10000) / 100 : 0);
    map.get(r.client_id).push({ processor: r[procField], rate, att: r.att || 0 });
  }
  return map;
}

// ═══════════════════════════════════════════════════════════════════
// Main entry point
// ═══════════════════════════════════════════════════════════════════

function computeUnifiedNetworkPlaybook(opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `unified-playbook:${days}`;
  return getCachedOrCompute(NETWORK_CLIENT_ID, 'unified-network-playbook', cacheKey, () => {
    return _compute(days);
  });
}

function _compute(days) {
  const clients = _getClients();
  const clientNames = new Map(clients.map(c => [c.id, c.name]));
  const clientCount = clients.length;
  const dateFilter = `AND o.acquisition_date >= date('now', '-${days} days')`;

  // ═══════════════════════════════════════════════════════════════════
  // PHASE 1: Cross-client SQL queries (no V2 dependency)
  // All queries include client_id in GROUP BY for agreement calculation
  // ═══════════════════════════════════════════════════════════════════

  // 1a. Bank groups — aggregate across all clients
  const bankGroupsRaw = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid,
      COUNT(DISTINCT CASE WHEN o.derived_product_role = 'main_initial' AND o.order_status IN (2,6,8) THEN o.customer_id END) as acquired,
      COUNT(DISTINCT o.cc_first_6) as bin_count,
      SUM(CASE WHEN o.derived_product_role = 'main_rebill' AND o.derived_cycle = 1 AND o.derived_attempt = 1 THEN 1 ELSE 0 END) as c1_att
    FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.is_test = 0 AND o.is_internal_test = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid
  `);

  // Aggregate to network level
  const bankAgg = new Map();
  for (const r of bankGroupsRaw) {
    const key = _bk(r.issuer_bank, r.is_prepaid);
    if (!bankAgg.has(key)) bankAgg.set(key, { issuer_bank: r.issuer_bank, is_prepaid: r.is_prepaid, acquired: 0, bin_count: 0, c1_att: 0, clientIds: new Set() });
    const a = bankAgg.get(key);
    a.acquired += r.acquired;
    a.bin_count += r.bin_count;
    a.c1_att += r.c1_att;
    a.clientIds.add(r.client_id);
  }
  // Filter: need enough C1 attempts across network
  const bankGroups = [...bankAgg.values()].filter(bg => bg.c1_att >= 30).sort((a, b) => b.acquired - a.acquired);

  // 1a2. Prepaid percentages per bank
  const prepaidPcts = querySql(`
    SELECT b.issuer_bank,
      COUNT(*) as total,
      SUM(CASE WHEN b.is_prepaid = 1 THEN 1 ELSE 0 END) as prepaid_count
    FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.is_test = 0 AND o.is_internal_test = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY b.issuer_bank
  `);
  const prepaidPctMap = new Map(prepaidPcts.map(r => [r.issuer_bank, {
    total: r.total,
    prepaidCount: r.prepaid_count,
    pct: r.total > 0 ? Math.round(r.prepaid_count / r.total * 1000) / 10 : 0,
  }]));

  // 1b. Initial processor performance per bank — includes client_id for agreement
  const initProcs = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name, g.bank_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.derived_product_role = 'main_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${CRM_ROUTING_EXCLUSION}
      ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name, g.bank_name
    HAVING att >= 3
  `);

  // 1c. Upsell processor performance per bank
  const upsellProcs = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name, g.bank_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.derived_product_role = 'upsell_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${CRM_ROUTING_EXCLUSION}
      ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name, g.bank_name
    HAVING att >= 3
  `);

  // 1d. Rebill C1+C2 per bank × processor
  const rebillProcs = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name, g.bank_name,
      SUM(CASE WHEN o.derived_cycle = 1 THEN 1 ELSE 0 END) as c1_att,
      SUM(CASE WHEN o.derived_cycle = 1 AND o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as c1_app,
      SUM(CASE WHEN o.derived_cycle = 2 THEN 1 ELSE 0 END) as c2_att,
      SUM(CASE WHEN o.derived_cycle = 2 AND o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as c2_app,
      COUNT(*) as total_att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as total_app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.derived_product_role IN ('main_rebill')
      AND o.derived_cycle IN (1, 2) AND o.derived_attempt = 1
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name, g.bank_name
    HAVING total_att >= 3
  `);

  // 1d2. Bank-level rebill aggregate
  const rebillAgg = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid,
      SUM(CASE WHEN o.derived_cycle = 1 THEN 1 ELSE 0 END) as c1_att,
      SUM(CASE WHEN o.derived_cycle = 1 AND o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as c1_app,
      SUM(CASE WHEN o.derived_cycle = 2 THEN 1 ELSE 0 END) as c2_att,
      SUM(CASE WHEN o.derived_cycle = 2 AND o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as c2_app
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.derived_product_role IN ('main_rebill')
      AND o.derived_attempt = 1
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid
  `);

  // 1e. Salvage: per bank, attempt 2+
  const salvageData = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid, o.derived_attempt as attempt,
      g.processor_name, g.bank_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app,
      ROUND(AVG(o.order_total), 2) as avg_price
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.derived_product_role IN ('main_rebill')
      AND o.derived_cycle IN (1, 2) AND o.derived_attempt >= 2
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid, o.derived_attempt, g.processor_name, g.bank_name
    HAVING att >= 3
  `);

  // 1f. Cascade decline reasons per bank (optimized: pre-compute recovered customers)
  const cascadeDeclines = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid, o.decline_reason,
      COUNT(*) as declined,
      SUM(CASE WHEN cr.customer_id IS NOT NULL THEN 1 ELSE 0 END) as recovered
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN (
      SELECT DISTINCT customer_id, client_id
      FROM orders
      WHERE is_cascaded = 1 AND order_status IN (2,6,8)
        AND derived_product_role = 'main_initial'
        AND is_test = 0 AND is_internal_test = 0
    ) cr ON o.customer_id = cr.customer_id AND o.client_id = cr.client_id
    WHERE o.derived_product_role = 'main_initial'
      AND (o.order_status = 7 OR o.is_cascaded = 1)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
      AND o.decline_reason != 'Prepaid Credit Cards Are Not Accepted'
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid, o.decline_reason
    HAVING declined >= 3
  `);

  // 1g. Rebill decline reasons (optimized: pre-compute retry-recovered customers)
  const rebillDeclines = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid, o.decline_reason,
      COUNT(*) as declined,
      SUM(CASE WHEN rr.customer_id IS NOT NULL THEN 1 ELSE 0 END) as recovered
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN (
      SELECT DISTINCT customer_id, derived_cycle, client_id
      FROM orders
      WHERE derived_product_role = 'main_rebill'
        AND order_status IN (2,6,8) AND derived_attempt > 1
        AND is_test = 0 AND is_internal_test = 0
    ) rr ON o.customer_id = rr.customer_id AND o.derived_cycle = rr.derived_cycle AND o.client_id = rr.client_id
    WHERE o.derived_product_role = 'main_rebill'
      AND (o.order_status = 7 OR o.is_cascaded = 1) AND o.derived_attempt = 1
      AND o.derived_cycle IN (1, 2)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
      AND o.decline_reason != 'Prepaid Credit Cards Are Not Accepted'
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid, o.decline_reason
    HAVING declined >= 3
  `);

  // 1h. Acquisition affinity per bank — self-join scoped within same client
  const acqAffinity = querySql(`
    SELECT acq.client_id, acq.issuer_bank, acq.is_prepaid, acq.init_proc,
      COUNT(*) as reb_att,
      SUM(CASE WHEN r.order_status IN (2,6,8) AND r.is_cascaded = 0 THEN 1 ELSE 0 END) as reb_app
    FROM (
      SELECT DISTINCT i.client_id, i.customer_id, b.issuer_bank, b.is_prepaid, g.processor_name as init_proc
      FROM orders i
      JOIN gateways g ON i.processing_gateway_id = g.gateway_id AND g.client_id = i.client_id
      JOIN bin_lookup b ON i.cc_first_6 = b.bin
      WHERE i.derived_product_role = 'main_initial' AND i.order_status IN (2,6,8) AND i.is_cascaded = 0
        AND i.is_test = 0 AND i.is_internal_test = 0 AND g.processor_name IS NOT NULL
        AND i.acquisition_date >= date('now', '-${days} days')
    ) acq
    JOIN orders r ON acq.customer_id = r.customer_id AND r.client_id = acq.client_id
    WHERE r.derived_product_role = 'main_rebill' AND r.derived_cycle IN (1, 2)
      AND r.derived_attempt = 1
      AND r.is_test = 0 AND r.is_internal_test = 0
      AND r.acquisition_date >= date('now', '-${days} days')
    GROUP BY acq.client_id, acq.issuer_bank, acq.is_prepaid, acq.init_proc
    HAVING reb_att >= 5
  `);

  // 1i. BIN lists per bank group
  const binLists = querySql(`
    SELECT b.issuer_bank, b.is_prepaid, GROUP_CONCAT(DISTINCT o.cc_first_6) as bins
    FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.is_test = 0 AND o.is_internal_test = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      ${dateFilter}
    GROUP BY b.issuer_bank, b.is_prepaid
  `);

  // 1j. L4 breakdown per bank
  const l4Breakdown = querySql(`
    SELECT o.client_id, b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type,
      COUNT(DISTINCT o.cc_first_6) as bin_count,
      GROUP_CONCAT(DISTINCT o.cc_first_6) as bins,
      SUM(CASE WHEN o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL) THEN 1 ELSE 0 END) as init_att,
      SUM(CASE WHEN o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL) AND o.is_cascaded = 0 AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as init_app,
      SUM(CASE WHEN o.derived_product_role = 'main_rebill' AND o.derived_cycle = 1 AND o.derived_attempt = 1 THEN 1 ELSE 0 END) as c1_att,
      SUM(CASE WHEN o.derived_product_role = 'main_rebill' AND o.derived_cycle = 1 AND o.derived_attempt = 1 AND o.is_cascaded = 0 AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as c1_app
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.is_test = 0 AND o.is_internal_test = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type
    HAVING (init_att >= 5 OR c1_att >= 5)
  `);

  // 1k. Cascade targets (for cascade chain — cascaded orders that succeeded)
  const cascadeTargetsRaw = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name, g.bank_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.is_cascaded = 1
      AND o.derived_product_role = 'main_initial'
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name, g.bank_name
    HAVING att >= 3
  `);

  // ═══════════════════════════════════════════════════════════════════
  // PHASE 2: Aggregate to network level and index by bank key
  // ═══════════════════════════════════════════════════════════════════

  // Helper: aggregate per-client processor rows to network level
  function _aggregateProcs(rows, bankKey) {
    const filtered = rows.filter(r => _bk(r.issuer_bank, r.is_prepaid) === bankKey);
    // Group by processor+acquiringBank across clients
    const procMap = new Map();
    for (const r of filtered) {
      const pk = `${normalizeProcessor(r.processor_name)}|${(r.bank_name || '').toUpperCase()}`;
      if (!procMap.has(pk)) procMap.set(pk, { processor_name: r.processor_name, bank_name: r.bank_name || '', att: 0, app: 0 });
      const p = procMap.get(pk);
      p.att += r.att;
      p.app += r.app;
    }
    return [...procMap.values()];
  }

  function _aggregateRebillProcs(rows, bankKey) {
    const filtered = rows.filter(r => _bk(r.issuer_bank, r.is_prepaid) === bankKey);
    const procMap = new Map();
    for (const r of filtered) {
      const pk = `${normalizeProcessor(r.processor_name)}|${(r.bank_name || '').toUpperCase()}`;
      if (!procMap.has(pk)) procMap.set(pk, { processor_name: r.processor_name, bank_name: r.bank_name || '', c1_att: 0, c1_app: 0, c2_att: 0, c2_app: 0, total_att: 0, total_app: 0 });
      const p = procMap.get(pk);
      p.c1_att += r.c1_att; p.c1_app += r.c1_app;
      p.c2_att += r.c2_att; p.c2_app += r.c2_app;
      p.total_att += r.total_att; p.total_app += r.total_app;
    }
    return [...procMap.values()];
  }

  function _procLabel(procName, bankName) {
    if (bankName) return `${procName} (${bankName})`;
    return procName;
  }

  // Aggregate rebill agg to network level
  const rebillAggNet = new Map();
  for (const r of rebillAgg) {
    const key = _bk(r.issuer_bank, r.is_prepaid);
    if (!rebillAggNet.has(key)) rebillAggNet.set(key, { c1_att: 0, c1_app: 0, c2_att: 0, c2_app: 0 });
    const a = rebillAggNet.get(key);
    a.c1_att += r.c1_att; a.c1_app += r.c1_app;
    a.c2_att += r.c2_att; a.c2_app += r.c2_app;
  }

  // Index other data by bank key
  const salvageIdx = _groupBy(salvageData, r => _bk(r.issuer_bank, r.is_prepaid));
  const cascDecIdx = _groupBy(cascadeDeclines, r => _bk(r.issuer_bank, r.is_prepaid));
  const rebDecIdx = _groupBy(rebillDeclines, r => _bk(r.issuer_bank, r.is_prepaid));
  const acqIdx = _groupBy(acqAffinity, r => _bk(r.issuer_bank, r.is_prepaid));
  const binIdx = new Map(binLists.map(r => [_bk(r.issuer_bank, r.is_prepaid), r.bins ? r.bins.split(',') : []]));

  // L4 aggregate to network level
  const l4AggMap = new Map();
  for (const r of l4Breakdown) {
    const k = `${r.issuer_bank}|${r.card_brand}|${r.is_prepaid}|${r.card_type}`;
    if (!l4AggMap.has(k)) l4AggMap.set(k, { issuer_bank: r.issuer_bank, card_brand: r.card_brand, is_prepaid: r.is_prepaid, card_type: r.card_type, bin_count: 0, bins: new Set(), init_att: 0, init_app: 0, c1_att: 0, c1_app: 0 });
    const a = l4AggMap.get(k);
    a.init_att += r.init_att; a.init_app += r.init_app;
    a.c1_att += r.c1_att; a.c1_app += r.c1_app;
    if (r.bins) r.bins.split(',').forEach(b => a.bins.add(b));
  }
  const l4AggArr = [...l4AggMap.values()].map(a => ({ ...a, bin_count: a.bins.size, bins: [...a.bins].join(',') }));
  const l4Idx = _groupBy(l4AggArr, r => r.issuer_bank);

  // Batch L4 data (cross-client)
  const l4Batch = _batchL4Data(days);

  // Cascade targets aggregated to network level
  const cascTargetsIdx = _groupBy(cascadeTargetsRaw, r => _bk(r.issuer_bank, r.is_prepaid));

  // ═══════════════════════════════════════════════════════════════════
  // PHASE 3: Build playbook rows
  // ═══════════════════════════════════════════════════════════════════
  const rows = [];

  for (const bg of bankGroups) {
    const key = _bk(bg.issuer_bank, bg.is_prepaid);
    const bins = binIdx.get(key) || [];

    // ── Initial routing (network level) ──
    const netInitProcs = _aggregateProcs(initProcs, key).map(p => ({
      processor: _procLabel(p.processor_name, p.bank_name), att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).sort((a, b) => b.rate - a.rate);

    const initBest = netInitProcs.filter(p => p.att >= MIN_ATT_SIGNAL && p.app > 0);
    const initBlock = netInitProcs.filter(p => p.att >= MIN_ATT_SIGNAL && p.app === 0);

    // Per-client initial agreement
    const initByClient = _clientBestMap(
      initProcs.filter(r => _bk(r.issuer_bank, r.is_prepaid) === key).map(r => ({
        ...r, processor_name: _procLabel(r.processor_name, r.bank_name),
      }))
    );
    const initAgreement = _computeAgreement(initByClient, initBest[0]?.processor || '', clientNames);

    // ── Upsell routing ──
    const netUpsProcs = _aggregateProcs(upsellProcs, key).map(p => ({
      processor: _procLabel(p.processor_name, p.bank_name), att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).sort((a, b) => b.rate - a.rate);

    const upsByClient = _clientBestMap(
      upsellProcs.filter(r => _bk(r.issuer_bank, r.is_prepaid) === key).map(r => ({
        ...r, processor_name: _procLabel(r.processor_name, r.bank_name),
      }))
    );
    const upsellAgreement = _computeAgreement(upsByClient, netUpsProcs[0]?.processor || '', clientNames);

    // ── Cascade chain (network aggregated) ──
    const netCascTargets = _aggregateProcs(cascadeTargetsRaw, key).map(p => ({
      name: _procLabel(p.processor_name, p.bank_name), att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).filter(t => t.rate > 0).sort((a, b) => b.rate - a.rate).slice(0, 3);

    // ── Cascade decline rules (aggregate across clients) ──
    const cascDecsRaw = cascDecIdx.get(key) || [];
    const cascDecAgg = new Map();
    for (const d of cascDecsRaw) {
      if (!cascDecAgg.has(d.decline_reason)) cascDecAgg.set(d.decline_reason, { declined: 0, recovered: 0 });
      const a = cascDecAgg.get(d.decline_reason);
      a.declined += d.declined; a.recovered += d.recovered;
    }
    const cascDecs = [...cascDecAgg.entries()].map(([reason, d]) => ({ decline_reason: reason, ...d })).sort((a, b) => b.declined - a.declined);
    const cascadeOn = cascDecs.filter(d => d.recovered > 0).map(d => ({
      reason: d.decline_reason, declined: d.declined, recovered: d.recovered,
      recoveryRate: Math.round(d.recovered / d.declined * 10000) / 100,
    })).sort((a, b) => b.recoveryRate - a.recoveryRate);
    const cascadeSkip = cascDecs.filter(d => d.recovered === 0 && d.declined >= 5).map(d => ({
      reason: d.decline_reason, declined: d.declined,
    }));

    // ── Rebill routing (network level) ──
    const netRebProcs = _aggregateRebillProcs(rebillProcs, key).map(p => ({
      processor: _procLabel(p.processor_name, p.bank_name),
      c1_att: p.c1_att, c1_app: p.c1_app,
      c1_rate: p.c1_att > 0 ? Math.round(p.c1_app / p.c1_att * 10000) / 100 : 0,
      c2_att: p.c2_att, c2_app: p.c2_app,
      c2_rate: p.c2_att > 0 ? Math.round(p.c2_app / p.c2_att * 10000) / 100 : 0,
      total_att: p.total_att, total_app: p.total_app,
      total_rate: p.total_att > 0 ? Math.round(p.total_app / p.total_att * 10000) / 100 : 0,
    })).sort((a, b) => b.c1_rate - a.c1_rate);

    const agg = rebillAggNet.get(key);
    const totalC1Att = agg ? agg.c1_att : netRebProcs.reduce((s, p) => s + p.c1_att, 0);
    const totalC1App = agg ? agg.c1_app : netRebProcs.reduce((s, p) => s + p.c1_app, 0);
    const totalC2Att = agg ? agg.c2_att : netRebProcs.reduce((s, p) => s + p.c2_att, 0);
    const totalC2App = agg ? agg.c2_app : netRebProcs.reduce((s, p) => s + p.c2_app, 0);
    const c1Rate = totalC1Att > 0 ? Math.round(totalC1App / totalC1Att * 10000) / 100 : 0;
    const c2Rate = totalC2Att > 0 ? Math.round(totalC2App / totalC2Att * 10000) / 100 : 0;

    const tier = classifyRebillTier(c1Rate, totalC1Att, totalC1App);
    const rebBest = netRebProcs.filter(p => p.total_att >= 5 && p.total_app > 0);
    const rebBlock = netRebProcs.filter(p => p.total_att >= MIN_ATT_SIGNAL && p.total_app === 0);

    // Per-client rebill agreement
    const rebByClient = _clientBestMap(
      rebillProcs.filter(r => _bk(r.issuer_bank, r.is_prepaid) === key).map(r => ({
        ...r,
        processor_name: _procLabel(r.processor_name, r.bank_name),
        att: r.total_att,
        app: r.total_app,
      }))
    );
    const rebillAgreement = _computeAgreement(rebByClient, rebBest[0]?.processor || '', clientNames);

    // ── Price strategy ──
    let priceStrategy = null;
    const currentPrice = 97.48;
    const currentRpa = c1Rate * currentPrice / 100;
    const baseTier = tier.replace('Early: ', '');

    if (baseTier === 'UNTESTED') {
      priceStrategy = {
        currentPrice, currentRate: c1Rate, currentRpa: 0,
        targetPrice: 39.97, breakEvenRate: 0,
        recommendation: `0% at $${currentPrice} — test at $39.97 to see if any approvals come through`,
        tier: 'UNTESTED',
      };
    } else if (baseTier === 'HOSTILE') {
      const targetPrice = 39.97;
      const breakEvenRate = currentRpa > 0 ? Math.round(currentRpa / targetPrice * 10000) / 100 : 0;
      priceStrategy = {
        currentPrice, currentRate: c1Rate, currentRpa: Math.round(currentRpa * 100) / 100,
        targetPrice, breakEvenRate,
        scenarios: [
          { label: '2x current rate', rate: Math.round(c1Rate * 2 * 100) / 100, rpa: Math.round(c1Rate * 2 * targetPrice) / 100 },
          { label: '3x current rate', rate: Math.round(c1Rate * 3 * 100) / 100, rpa: Math.round(c1Rate * 3 * targetPrice) / 100 },
        ],
        recommendation: `Drop to $${targetPrice} — any recovery is profit. Need ${breakEvenRate}% to match current RPA.`,
        tier: 'HOSTILE',
      };
    } else if (baseTier === 'RESISTANT') {
      const prices = [59.97, 39.97];
      const scenarios = prices.map(p => ({
        price: p,
        breakEvenRate: currentRpa > 0 ? Math.round(currentRpa / p * 10000) / 100 : 0,
        rpaAt2x: Math.round(c1Rate * 2 * p) / 100,
      }));
      priceStrategy = {
        currentPrice, currentRate: c1Rate, currentRpa: Math.round(currentRpa * 100) / 100,
        scenarios,
        recommendation: `Test at $59.97 (need ${scenarios[0].breakEvenRate}% to break even) or $39.97 (need ${scenarios[1].breakEvenRate}%)`,
        tier: 'RESISTANT',
      };
    } else if (baseTier === 'VIABLE' && c1Rate < 15) {
      const targetPrice = 79.97;
      const breakEvenRate = currentRpa > 0 ? Math.round(currentRpa / targetPrice * 10000) / 100 : 0;
      priceStrategy = {
        currentPrice, currentRate: c1Rate, currentRpa: Math.round(currentRpa * 100) / 100,
        targetPrice, breakEvenRate,
        recommendation: `Consider $${targetPrice} — need ${breakEvenRate}% to match current RPA`,
        tier: 'VIABLE',
      };
    }

    // ── Salvage sequence (network aggregated) ──
    const salvRaw = salvageIdx.get(key) || [];
    // Aggregate salvage across clients
    const salvAgg = new Map();
    for (const s of salvRaw) {
      const sk = `${s.attempt}|${normalizeProcessor(s.processor_name)}|${(s.bank_name || '').toUpperCase()}`;
      if (!salvAgg.has(sk)) salvAgg.set(sk, { attempt: s.attempt, processor_name: _procLabel(s.processor_name, s.bank_name), att: 0, app: 0, price_sum: 0, price_count: 0 });
      const a = salvAgg.get(sk);
      a.att += s.att; a.app += s.app;
      if (s.avg_price) { a.price_sum += s.avg_price * s.att; a.price_count += s.att; }
    }
    const salvAggArr = [...salvAgg.values()].map(a => ({ ...a, avg_price: a.price_count > 0 ? Math.round(a.price_sum / a.price_count * 100) / 100 : 97.48 }));

    const salvageSeq = [];
    for (let att = 2; att <= 4; att++) {
      const attData = salvAggArr.filter(s => s.attempt === att).sort((a, b) => {
        return (b.att > 0 ? b.app / b.att : 0) - (a.att > 0 ? a.app / a.att : 0);
      });
      const best = attData[0];
      if (!best || best.att < 3) {
        salvageSeq.push({ attempt: att, isStop: true, stopMessage: 'Insufficient data' });
        break;
      }
      const rate = best.att > 0 ? Math.round(best.app / best.att * 10000) / 100 : 0;
      const rpa = rate * (best.avg_price || 97.48) / 100;
      if (rpa < STOP_RPA) {
        salvageSeq.push({ attempt: att, isStop: true, stopMessage: `RPA $${rpa.toFixed(2)} < $${STOP_RPA}` });
        break;
      }
      salvageSeq.push({
        attempt: att, processor: best.processor_name, rate,
        att: best.att, app: best.app, price: best.avg_price,
        rpa: Math.round(rpa * 100) / 100, isStop: false,
      });
    }

    // ── Rebill decline rules (aggregate across clients) ──
    const rebDecsRaw = rebDecIdx.get(key) || [];
    const rebDecAgg = new Map();
    for (const d of rebDecsRaw) {
      if (!rebDecAgg.has(d.decline_reason)) rebDecAgg.set(d.decline_reason, { declined: 0, recovered: 0 });
      const a = rebDecAgg.get(d.decline_reason);
      a.declined += d.declined; a.recovered += d.recovered;
    }
    const rebDecs = [...rebDecAgg.entries()].map(([reason, d]) => ({ decline_reason: reason, ...d })).sort((a, b) => b.declined - a.declined);
    const rebillRetryOn = rebDecs.filter(d => d.recovered > 0).map(d => ({
      reason: d.decline_reason, declined: d.declined, recovered: d.recovered,
      recoveryRate: Math.round(d.recovered / d.declined * 10000) / 100,
    })).sort((a, b) => b.recoveryRate - a.recoveryRate);
    const rebillStopOn = rebDecs.filter(d => d.recovered === 0 && d.declined >= 5).map(d => ({
      reason: d.decline_reason, declined: d.declined,
    }));

    // ── Acquisition affinity (aggregate across clients) ──
    const acqRaw = acqIdx.get(key) || [];
    const acqAgg = new Map();
    for (const a of acqRaw) {
      const pk = normalizeProcessor(a.init_proc);
      if (!acqAgg.has(pk)) acqAgg.set(pk, { processor: a.init_proc, reb_att: 0, reb_app: 0 });
      const x = acqAgg.get(pk);
      x.reb_att += a.reb_att; x.reb_app += a.reb_app;
    }
    const acqAffs = [...acqAgg.values()].map(a => ({
      processor: a.processor,
      rebAtt: a.reb_att,
      rebApp: a.reb_app,
      rebRate: a.reb_att > 0 ? Math.round(a.reb_app / a.reb_att * 10000) / 100 : 0,
    })).sort((a, b) => b.rebRate - a.rebRate);

    // ── Confidence ──
    const maxApp = Math.max(
      netInitProcs.reduce((m, p) => Math.max(m, p.app), 0),
      netUpsProcs.reduce((m, p) => Math.max(m, p.app), 0),
      netRebProcs.reduce((m, p) => Math.max(m, p.total_app), 0),
    );
    const maxAtt = Math.max(
      netInitProcs.reduce((m, p) => Math.max(m, p.att), 0),
      totalC1Att,
    );
    const confidenceTier = maxApp >= MIN_APP_CONFIDENT ? 'Confident'
      : maxAtt >= MIN_ATT_SIGNAL ? 'Early signal' : 'Skip';

    if (confidenceTier === 'Skip') continue;

    // ── L4 sub-groups ──
    const l4Groups = _computeL4Groups(days, bg.issuer_bank, bg.is_prepaid, l4Idx, netInitProcs, c1Rate, l4Batch);

    rows.push({
      issuer_bank: bg.issuer_bank,
      is_prepaid: bg.is_prepaid,
      acquired: bg.acquired,
      bins,
      binCount: bins.length,
      confidenceTier,

      // Cross-client metadata
      clientCount: bg.clientIds.size,
      clientNames: [...bg.clientIds].map(id => clientNames.get(id) || `Client ${id}`),

      // Initial
      initialBest: initBest.slice(0, 3),
      initialBlock: initBlock,
      initialAgreement: initAgreement,

      // Cascade
      cascadeChain: netCascTargets,
      cascadeOn: cascadeOn.slice(0, 5),
      cascadeSkip: cascadeSkip.slice(0, 5),

      // Upsell
      upsellBest: netUpsProcs.slice(0, 3),
      upsellAgreement,

      // Rebill
      rebillTier: tier,
      c1: { att: totalC1Att, app: totalC1App, rate: c1Rate },
      c2: { att: totalC2Att, app: totalC2App, rate: c2Rate },
      rebillBest: rebBest.slice(0, 3),
      rebillBlock: rebBlock,
      rebillAgreement,
      priceStrategy,

      // Salvage
      salvageSequence: salvageSeq,
      rebillRetryOn: rebillRetryOn.slice(0, 5),
      rebillStopOn: rebillStopOn.slice(0, 5),

      // Lifecycle
      acquisitionAffinity: acqAffs.slice(0, 3),

      // L4
      l4Groups,

      // Flags
      isPrepaid: !!bg.is_prepaid,
      prepaidInfo: prepaidPctMap.get(bg.issuer_bank) || { total: 0, prepaidCount: 0, pct: 0 },
      isRebillBlocker: totalC1Att >= 20 && c1Rate < 5,
      notRebillWorthy: totalC1Att >= 10 && totalC1App === 0,
    });
  }

  // Sort: Confident first, then by volume
  const tierOrder = { 'Confident': 0, 'Early signal': 1 };
  rows.sort((a, b) => {
    const t = (tierOrder[a.confidenceTier] || 9) - (tierOrder[b.confidenceTier] || 9);
    if (t !== 0) return t;
    return b.acquired - a.acquired;
  });

  return {
    rows,
    clientCount,
    clientNames: [...clientNames.entries()].map(([id, name]) => ({ id, name })),
    summary: {
      totalRows: rows.length,
      confident: rows.filter(r => r.confidenceTier === 'Confident').length,
      earlySignal: rows.filter(r => r.confidenceTier === 'Early signal').length,
      untested: rows.filter(r => r.rebillTier.includes('UNTESTED')).length,
      hostile: rows.filter(r => r.rebillTier.includes('HOSTILE')).length,
      resistant: rows.filter(r => r.rebillTier.includes('RESISTANT')).length,
      viable: rows.filter(r => r.rebillTier.includes('VIABLE')).length,
      strong: rows.filter(r => r.rebillTier.includes('STRONG')).length,
      rebillBlockers: rows.filter(r => r.isRebillBlocker).length,
      prepaid: rows.filter(r => r.isPrepaid).length,
      withCascade: rows.filter(r => r.cascadeChain.length > 0).length,
      withSalvage: rows.filter(r => r.salvageSequence.length > 0 && !r.salvageSequence[0].isStop).length,
      withPriceOpt: rows.filter(r => r.priceStrategy).length,
    },
  };
}

// ─── Batch L4 data (cross-client, no client_id filter) ─────────────

function _batchL4Data(days) {
  const dateFilterInline = `AND o.acquisition_date >= date('now', '-${days} days')`;
  const l4Key = r => `${r.issuer_bank}|${r.card_brand}|${r.is_prepaid}|${r.card_type}`;

  const initRouting = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name, g.bank_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.derived_product_role = 'main_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${CRM_ROUTING_EXCLUSION}
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name, g.bank_name
    HAVING att >= 3
  `);

  const rebillRouting = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name, g.bank_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.derived_product_role = 'main_rebill'
      AND o.derived_cycle IN (1, 2) AND o.derived_attempt = 1
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name, g.bank_name
    HAVING att >= 3
  `);

  const cascadeTargets = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name, g.bank_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.is_cascaded = 1
      AND o.derived_product_role = 'main_initial'
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name, g.bank_name
    HAVING att >= 3
  `);

  const initDeclines = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, o.decline_reason,
      COUNT(*) as declined,
      SUM(CASE WHEN cr.customer_id IS NOT NULL THEN 1 ELSE 0 END) as recovered
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN (
      SELECT DISTINCT customer_id, client_id
      FROM orders
      WHERE is_cascaded = 1 AND order_status IN (2,6,8)
        AND derived_product_role = 'main_initial'
        AND is_test = 0 AND is_internal_test = 0
    ) cr ON o.customer_id = cr.customer_id AND o.client_id = cr.client_id
    WHERE o.derived_product_role = 'main_initial'
      AND (o.order_status = 7 OR o.is_cascaded = 1)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
      AND o.decline_reason != 'Prepaid Credit Cards Are Not Accepted'
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, o.decline_reason
    HAVING declined >= 3
  `);

  const rebDeclines = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, o.decline_reason,
      COUNT(*) as declined,
      SUM(CASE WHEN rr.customer_id IS NOT NULL THEN 1 ELSE 0 END) as recovered
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN (
      SELECT DISTINCT customer_id, derived_cycle, client_id
      FROM orders
      WHERE derived_product_role = 'main_rebill'
        AND order_status IN (2,6,8) AND derived_attempt > 1
        AND is_test = 0 AND is_internal_test = 0
    ) rr ON o.customer_id = rr.customer_id AND o.derived_cycle = rr.derived_cycle AND o.client_id = rr.client_id
    WHERE o.derived_product_role = 'main_rebill'
      AND (o.order_status = 7 OR o.is_cascaded = 1) AND o.derived_attempt = 1
      AND o.derived_cycle IN (1, 2)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
      AND o.decline_reason != 'Prepaid Credit Cards Are Not Accepted'
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, o.decline_reason
    HAVING declined >= 3
  `);

  const salvageDataL4 = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type,
      o.derived_attempt as attempt, g.processor_name, g.bank_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app,
      ROUND(AVG(o.order_total), 2) as avg_price
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.derived_product_role = 'main_rebill'
      AND o.derived_cycle IN (1, 2) AND o.derived_attempt >= 2
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, o.derived_attempt, g.processor_name, g.bank_name
    HAVING att >= 3
  `);

  return {
    initRouting: _groupBy(initRouting, l4Key),
    rebillRouting: _groupBy(rebillRouting, l4Key),
    cascadeTargets: _groupBy(cascadeTargets, l4Key),
    initDeclines: _groupBy(initDeclines, l4Key),
    rebDeclines: _groupBy(rebDeclines, l4Key),
    salvageData: _groupBy(salvageDataL4, l4Key),
  };
}

function _computeL4Groups(days, bankName, bankPrepaid, l4Idx, bankInitPs, bankC1Rate, l4Batch) {
  const allL4s = l4Idx.get(bankName) || [];
  const l4s = allL4s.filter(l => l.is_prepaid === bankPrepaid);
  if (l4s.length === 0) return [];

  const bankInitAtt = bankInitPs.reduce((s, p) => s + p.att, 0);
  const bankInitApp = bankInitPs.reduce((s, p) => s + p.app, 0);
  const bankInitRate = bankInitAtt > 0 ? Math.round(bankInitApp / bankInitAtt * 10000) / 100 : 0;

  const groups = [];
  for (const l4 of l4s) {
    const initRate = l4.init_att >= 10 ? Math.round(l4.init_app / l4.init_att * 10000) / 100 : null;
    const c1Rate = l4.c1_att >= 10 ? Math.round(l4.c1_app / l4.c1_att * 10000) / 100 : null;
    const initDelta = initRate !== null ? initRate - bankInitRate : null;
    const c1Delta = c1Rate !== null && bankC1Rate > 0 ? c1Rate - bankC1Rate : null;
    const isInitOutlier = initDelta !== null && Math.abs(initDelta) >= 5;
    const isC1Outlier = c1Delta !== null && Math.abs(c1Delta) >= 5;

    const bins = l4.bins ? l4.bins.split(',') : [];
    if (bins.length === 0) continue;

    const hasOwnData = l4.init_att >= MIN_ATT_SIGNAL || l4.c1_att >= MIN_ATT_SIGNAL;
    const l4k = `${l4.issuer_bank}|${l4.card_brand}|${l4.is_prepaid}|${l4.card_type}`;

    const _procLabelL4 = (p) => p.bank_name ? `${p.processor_name} (${p.bank_name})` : p.processor_name;

    const initRouting = (l4Batch.initRouting.get(l4k) || []).map(p => ({
      processor: _procLabelL4(p), att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).sort((a, b) => b.rate - a.rate);

    const rebillRouting = (l4Batch.rebillRouting.get(l4k) || []).map(p => ({
      processor: _procLabelL4(p), att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).sort((a, b) => b.rate - a.rate);

    const cascTargets = (l4Batch.cascadeTargets.get(l4k) || []).map(p => ({
      name: _procLabelL4(p), att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).filter(t => t.rate > 0).sort((a, b) => b.rate - a.rate).slice(0, 3);

    const initDeclinesL4 = (l4Batch.initDeclines.get(l4k) || []).sort((a, b) => b.declined - a.declined);
    const cascadeOn = initDeclinesL4.filter(d => d.recovered > 0).map(d => ({
      reason: d.decline_reason, declined: d.declined, recovered: d.recovered,
      recoveryRate: Math.round(d.recovered / d.declined * 10000) / 100,
    })).sort((a, b) => b.recoveryRate - a.recoveryRate).slice(0, 5);
    const cascadeSkip = initDeclinesL4.filter(d => d.recovered === 0 && d.declined >= 3).map(d => ({
      reason: d.decline_reason, declined: d.declined,
    })).slice(0, 5);

    const rebDeclinesL4 = (l4Batch.rebDeclines.get(l4k) || []).sort((a, b) => b.declined - a.declined);
    const rebillRetryOn = rebDeclinesL4.filter(d => d.recovered > 0).map(d => ({
      reason: d.decline_reason, declined: d.declined, recovered: d.recovered,
      recoveryRate: Math.round(d.recovered / d.declined * 10000) / 100,
    })).sort((a, b) => b.recoveryRate - a.recoveryRate).slice(0, 5);
    const rebillStopOn = rebDeclinesL4.filter(d => d.recovered === 0 && d.declined >= 3).map(d => ({
      reason: d.decline_reason, declined: d.declined,
    })).slice(0, 5);

    // Salvage
    const salvRaw = l4Batch.salvageData.get(l4k) || [];
    const salvageSeq = [];
    for (let att = 2; att <= 4; att++) {
      const attData = salvRaw.filter(s => s.attempt === att).sort((a, b) => {
        return (b.att > 0 ? b.app / b.att : 0) - (a.att > 0 ? a.app / a.att : 0);
      });
      const best = attData[0];
      if (!best || best.att < 3) {
        salvageSeq.push({ attempt: att, isStop: true, stopMessage: 'Insufficient data' });
        break;
      }
      const rate = best.att > 0 ? Math.round(best.app / best.att * 10000) / 100 : 0;
      const rpa = rate * (best.avg_price || 97.48) / 100;
      if (rpa < STOP_RPA) {
        salvageSeq.push({ attempt: att, isStop: true, stopMessage: `RPA $${rpa.toFixed(2)} < $${STOP_RPA}` });
        break;
      }
      salvageSeq.push({
        attempt: att, processor: best.processor_name ? _procLabelL4(best) : best.processor_name,
        rate, att: best.att, app: best.app, price: best.avg_price,
        rpa: Math.round(rpa * 100) / 100, isStop: false,
      });
    }

    const maxInitApp = initRouting.reduce((m, p) => Math.max(m, p.app), 0);
    const maxRebApp = rebillRouting.reduce((m, p) => Math.max(m, p.app), 0);
    const hasConfidentData = Math.max(maxInitApp, maxRebApp) >= MIN_APP_CONFIDENT;
    const routingLevel = hasConfidentData ? 'own' : hasOwnData ? 'partial' : 'fallback';

    groups.push({
      card_brand: l4.card_brand,
      is_prepaid: l4.is_prepaid,
      card_type: l4.card_type,
      bin_count: l4.bin_count,
      bins,
      initRate, initAtt: l4.init_att, initApp: l4.init_app,
      initDelta: initDelta !== null ? Math.round(initDelta * 10) / 10 : null,
      c1Rate, c1Att: l4.c1_att, c1App: l4.c1_app,
      c1Delta: c1Delta !== null ? Math.round(c1Delta * 10) / 10 : null,
      isInitOutlier, isC1Outlier,
      routingLevel,
      initRouting: initRouting.slice(0, 3),
      initBlock: initRouting.filter(p => p.att >= 5 && p.app === 0),
      rebillRouting: rebillRouting.slice(0, 3),
      rebillBlock: rebillRouting.filter(p => p.att >= 5 && p.app === 0),
      cascadeTargets: cascTargets,
      cascadeOn, cascadeSkip,
      rebillRetryOn, rebillStopOn,
      salvageSequence: salvageSeq,
    });
  }

  groups.sort((a, b) => {
    const levelOrder = { own: 0, partial: 1, fallback: 2 };
    const lDiff = (levelOrder[a.routingLevel] || 9) - (levelOrder[b.routingLevel] || 9);
    if (lDiff !== 0) return lDiff;
    const aMax = Math.max(Math.abs(a.initDelta || 0), Math.abs(a.c1Delta || 0));
    const bMax = Math.max(Math.abs(b.initDelta || 0), Math.abs(b.c1Delta || 0));
    return bMax - aMax;
  });

  return groups;
}

module.exports = { computeUnifiedNetworkPlaybook };
