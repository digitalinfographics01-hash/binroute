/**
 * Network Analysis — Cross-client analytics engine.
 *
 * Aggregates data across ALL clients to validate routing patterns
 * at the network level. Uses client_id = 0 as cache sentinel.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql, queryOneSql, runSql, saveDb } = require('../db/connection');
const {
  APPROVED_STATUS_SQL,
  CLEAN_FILTER,
  CRM_ROUTING_EXCLUSION,
  getCachedOrCompute,
  stddev,
  daysAgoFilter,
} = require('./engine');

const NETWORK_CLIENT_ID = 0;
const TEST_BINS = ['144444', '777777'];
const BIN_EXCL = TEST_BINS.map(b => `'${b}'`).join(',');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function _rate(app, att) { return att > 0 ? Math.round((app / att) * 10000) / 100 : 0; }

/** Normalize processor names for cross-client matching */
function normalizeProcessor(name) {
  if (!name) return 'UNKNOWN';
  return name.trim().toUpperCase();
}

/** Normalize issuer bank names (same as flow-optix-v2) */
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

/** Fetch all client names */
function _getClients() {
  return querySql('SELECT id, name FROM clients ORDER BY id');
}
function _clientNameMap() {
  const m = new Map();
  for (const c of _getClients()) m.set(c.id, c.name);
  return m;
}

/** Network confidence based on cross-client rate agreement */
function networkConfidence(clientResults) {
  const withData = clientResults.filter(r => r.total >= 20);
  const clientCount = withData.length;
  if (clientCount === 0) return { level: 'NO_DATA', score: 0, clients: 0 };
  if (clientCount === 1) return { level: 'SINGLE_CLIENT', score: 20, clients: 1 };

  const rates = withData.map(r => r.rate);
  const sd = stddev(rates);
  const allAdequate = withData.every(r => r.total >= 50);

  if (clientCount >= 3 && sd <= 3 && allAdequate) {
    return { level: 'NETWORK_VALIDATED', score: Math.round(90 + (10 - sd)), clients: clientCount };
  }
  if (clientCount >= 2 && sd <= 5) {
    return { level: 'MULTI_CLIENT', score: Math.round(60 + (20 - sd * 2)), clients: clientCount };
  }
  if (sd > 10) {
    return { level: 'CONFLICTING', score: 30, clients: clientCount };
  }
  return { level: 'PARTIAL', score: 45, clients: clientCount };
}

// ---------------------------------------------------------------------------
// 1. Processor Comparison
// ---------------------------------------------------------------------------

function computeProcessorComparison(opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `proc-comparison:${days}`;
  return getCachedOrCompute(NETWORK_CLIENT_ID, 'network-processor-comparison', cacheKey, () => {
    return _computeProcessorComparison(days);
  });
}

function _computeProcessorComparison(days) {
  const clients = _clientNameMap();
  const dateFilter = daysAgoFilter(days);

  // Natural first-attempt data per client × processor, split by product role
  const rows = querySql(`
    SELECT o.client_id, g.processor_name, g.bank_name, o.derived_product_role,
      COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END) AS total,
      COUNT(CASE WHEN o.order_status ${APPROVED_STATUS_SQL} AND o.is_cascaded = 0 THEN 1 END) AS approved,
      COUNT(CASE WHEN o.order_status = 7 THEN 1 END) AS declined
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE ${CLEAN_FILTER}
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.order_status IN (2,6,7,8)
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${CRM_ROUTING_EXCLUSION}
      AND ${dateFilter}
    GROUP BY o.client_id, g.processor_name, g.bank_name, o.derived_product_role
  `);

  // Cascade corrections: attribute cascade declines to original processor
  const cascRows = querySql(`
    SELECT o.client_id, g.processor_name, g.bank_name, o.derived_product_role,
      COUNT(*) AS casc_count
    FROM orders o
    JOIN gateways g ON o.original_gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL
      AND ${CLEAN_FILTER}
      AND g.processor_name IS NOT NULL
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${dateFilter}
    GROUP BY o.client_id, g.processor_name, g.bank_name, o.derived_product_role
  `);

  // Build cascade index for merging
  const cascIdx = new Map();
  for (const cr of cascRows) {
    const key = `${cr.client_id}|${normalizeProcessor(cr.processor_name)}|${cr.derived_product_role || 'all'}`;
    cascIdx.set(key, (cascIdx.get(key) || 0) + cr.casc_count);
  }

  // Group by normalized processor name
  const procMap = new Map();
  for (const r of rows) {
    const normProc = normalizeProcessor(r.processor_name);
    if (!procMap.has(normProc)) {
      procMap.set(normProc, { processor: r.processor_name, banks: new Set(), byRole: {} });
    }
    const entry = procMap.get(normProc);
    if (r.bank_name) entry.banks.add(r.bank_name);

    const role = r.derived_product_role || 'other';
    if (!entry.byRole[role]) entry.byRole[role] = {};
    if (!entry.byRole[role][r.client_id]) {
      entry.byRole[role][r.client_id] = { clientId: r.client_id, clientName: clients.get(r.client_id) || `Client ${r.client_id}`, total: 0, approved: 0, declined: 0 };
    }
    const c = entry.byRole[role][r.client_id];
    c.total += r.total;
    c.approved += r.approved;
    c.declined += r.declined;

    // Add cascade declines
    const cascKey = `${r.client_id}|${normProc}|${role}`;
    const cascCount = cascIdx.get(cascKey) || 0;
    if (cascCount > 0) {
      c.total += cascCount;
      c.declined += cascCount;
    }
  }

  // Build output: one entry per processor with per-role, per-client breakdown
  const processors = [];
  for (const [normProc, entry] of procMap) {
    const procResult = {
      processor: entry.processor,
      banks: [...entry.banks].join(', '),
      roles: {},
    };

    // Build "all" aggregate across roles
    const allByClient = {};

    for (const [role, clientMap] of Object.entries(entry.byRole)) {
      const clientArr = Object.values(clientMap).map(c => ({ ...c, rate: _rate(c.approved, c.total) }));
      const netTotal = clientArr.reduce((s, c) => s + c.total, 0);
      const netApp = clientArr.reduce((s, c) => s + c.approved, 0);

      procResult.roles[role] = {
        clients: clientArr,
        network: { total: netTotal, approved: netApp, rate: _rate(netApp, netTotal) },
        confidence: networkConfidence(clientArr),
      };

      // Accumulate into "all"
      for (const c of clientArr) {
        if (!allByClient[c.clientId]) allByClient[c.clientId] = { clientId: c.clientId, clientName: c.clientName, total: 0, approved: 0, declined: 0 };
        allByClient[c.clientId].total += c.total;
        allByClient[c.clientId].approved += c.approved;
        allByClient[c.clientId].declined += c.declined;
      }
    }

    const allArr = Object.values(allByClient).map(c => ({ ...c, rate: _rate(c.approved, c.total) }));
    const allTotal = allArr.reduce((s, c) => s + c.total, 0);
    const allApp = allArr.reduce((s, c) => s + c.approved, 0);
    procResult.all = {
      clients: allArr,
      network: { total: allTotal, approved: allApp, rate: _rate(allApp, allTotal) },
      confidence: networkConfidence(allArr),
    };

    processors.push(procResult);
  }

  // Sort by network volume desc
  processors.sort((a, b) => b.all.network.total - a.all.network.total);

  return { processors, clientCount: clients.size, computedAt: new Date().toISOString() };
}

// ---------------------------------------------------------------------------
// 2. Bank Rankings (issuing bank)
// ---------------------------------------------------------------------------

function computeBankRankings(opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `bank-rankings:${days}`;
  return getCachedOrCompute(NETWORK_CLIENT_ID, 'network-bank-rankings', cacheKey, () => {
    return _computeBankRankings(days);
  });
}

function _computeBankRankings(days) {
  const clients = _clientNameMap();
  const dateFilter = daysAgoFilter(days);

  const rows = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid,
      COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END) AS total,
      COUNT(CASE WHEN o.order_status ${APPROVED_STATUS_SQL} AND o.is_cascaded = 0 THEN 1 END) AS approved
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE ${CLEAN_FILTER}
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND o.order_status IN (2,6,7,8)
      AND ${CRM_ROUTING_EXCLUSION}
      AND ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid
    HAVING total >= 20
  `);

  // Group by normalized bank name
  const bankMap = new Map();
  for (const r of rows) {
    const normBank = normalizeBank(r.issuer_bank);
    if (!bankMap.has(normBank)) bankMap.set(normBank, { bank: normBank, isPrepaid: {}, clients: {} });
    const entry = bankMap.get(normBank);

    // Track prepaid split
    const ppKey = r.is_prepaid ? 'prepaid' : 'non_prepaid';
    if (!entry.isPrepaid[ppKey]) entry.isPrepaid[ppKey] = {};
    if (!entry.isPrepaid[ppKey][r.client_id]) {
      entry.isPrepaid[ppKey][r.client_id] = { clientId: r.client_id, clientName: clients.get(r.client_id) || `Client ${r.client_id}`, total: 0, approved: 0 };
    }
    entry.isPrepaid[ppKey][r.client_id].total += r.total;
    entry.isPrepaid[ppKey][r.client_id].approved += r.approved;

    // Aggregate (all card types)
    if (!entry.clients[r.client_id]) {
      entry.clients[r.client_id] = { clientId: r.client_id, clientName: clients.get(r.client_id) || `Client ${r.client_id}`, total: 0, approved: 0 };
    }
    entry.clients[r.client_id].total += r.total;
    entry.clients[r.client_id].approved += r.approved;
  }

  const banks = [];
  for (const [normBank, entry] of bankMap) {
    const clientArr = Object.values(entry.clients).map(c => ({ ...c, rate: _rate(c.approved, c.total) }));
    const netTotal = clientArr.reduce((s, c) => s + c.total, 0);
    const netApp = clientArr.reduce((s, c) => s + c.approved, 0);

    banks.push({
      bank: normBank,
      clients: clientArr,
      network: { total: netTotal, approved: netApp, rate: _rate(netApp, netTotal) },
      confidence: networkConfidence(clientArr),
    });
  }

  // Rank per client
  const clientIds = [...clients.keys()];
  for (const cid of clientIds) {
    const ranked = banks
      .filter(b => b.clients.some(c => c.clientId === cid && c.total >= 20))
      .sort((a, b) => {
        const ra = a.clients.find(c => c.clientId === cid)?.rate || 0;
        const rb = b.clients.find(c => c.clientId === cid)?.rate || 0;
        return rb - ra;
      });
    ranked.forEach((b, i) => {
      const cl = b.clients.find(c => c.clientId === cid);
      if (cl) cl.rank = i + 1;
    });
  }

  // Sort by network volume
  banks.sort((a, b) => b.network.total - a.network.total);

  return { banks, clientCount: clients.size, computedAt: new Date().toISOString() };
}

// ---------------------------------------------------------------------------
// 3. BIN Patterns
// ---------------------------------------------------------------------------

function computeBinPatterns(opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `bin-patterns:${days}`;
  return getCachedOrCompute(NETWORK_CLIENT_ID, 'network-bin-patterns', cacheKey, () => {
    return _computeBinPatterns(days);
  });
}

function _computeBinPatterns(days) {
  const clients = _clientNameMap();
  const dateFilter = daysAgoFilter(days);

  const rows = querySql(`
    SELECT o.client_id, b.issuer_bank, b.card_brand, b.is_prepaid,
      COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END) AS total,
      COUNT(CASE WHEN o.order_status ${APPROVED_STATUS_SQL} AND o.is_cascaded = 0 THEN 1 END) AS approved
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE ${CLEAN_FILTER}
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND o.order_status IN (2,6,7,8)
      AND ${CRM_ROUTING_EXCLUSION}
      AND ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.card_brand, b.is_prepaid
    HAVING total >= 30
  `);

  // Group by cluster key
  const clusterMap = new Map();
  for (const r of rows) {
    const normBank = normalizeBank(r.issuer_bank);
    const key = `${normBank}|${r.card_brand || 'Unknown'}|${r.is_prepaid}`;
    if (!clusterMap.has(key)) {
      clusterMap.set(key, {
        issuer_bank: normBank,
        card_brand: r.card_brand || 'Unknown',
        is_prepaid: !!r.is_prepaid,
        clients: {},
      });
    }
    const entry = clusterMap.get(key);
    if (!entry.clients[r.client_id]) {
      entry.clients[r.client_id] = { clientId: r.client_id, clientName: clients.get(r.client_id) || `Client ${r.client_id}`, total: 0, approved: 0 };
    }
    entry.clients[r.client_id].total += r.total;
    entry.clients[r.client_id].approved += r.approved;
  }

  const clusters = [];
  for (const [, entry] of clusterMap) {
    const clientArr = Object.values(entry.clients).map(c => ({ ...c, rate: _rate(c.approved, c.total) }));
    const netTotal = clientArr.reduce((s, c) => s + c.total, 0);
    const netApp = clientArr.reduce((s, c) => s + c.approved, 0);

    clusters.push({
      issuer_bank: entry.issuer_bank,
      card_brand: entry.card_brand,
      is_prepaid: entry.is_prepaid,
      clients: clientArr,
      network: { total: netTotal, approved: netApp, rate: _rate(netApp, netTotal) },
      confidence: networkConfidence(clientArr),
    });
  }

  clusters.sort((a, b) => b.network.total - a.network.total);
  return { clusters, clientCount: clients.size, computedAt: new Date().toISOString() };
}

// ---------------------------------------------------------------------------
// 4. Cascade Comparison
// ---------------------------------------------------------------------------

function computeCascadeComparison(opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `cascade-comparison:${days}`;
  return getCachedOrCompute(NETWORK_CLIENT_ID, 'network-cascade-comparison', cacheKey, () => {
    return _computeCascadeComparison(days);
  });
}

function _computeCascadeComparison(days) {
  const clients = _clientNameMap();
  const dateFilter = daysAgoFilter(days);

  const rows = querySql(`
    SELECT o.client_id, g.processor_name,
      COUNT(*) AS cascaded_to,
      COUNT(CASE WHEN o.order_status ${APPROVED_STATUS_SQL} THEN 1 END) AS recovered
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.is_cascaded = 1
      AND ${CLEAN_FILTER}
      AND g.processor_name IS NOT NULL
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${dateFilter}
    GROUP BY o.client_id, g.processor_name
    HAVING cascaded_to >= 5
  `);

  const procMap = new Map();
  for (const r of rows) {
    const normProc = normalizeProcessor(r.processor_name);
    if (!procMap.has(normProc)) procMap.set(normProc, { processor: r.processor_name, clients: {} });
    const entry = procMap.get(normProc);
    if (!entry.clients[r.client_id]) {
      entry.clients[r.client_id] = { clientId: r.client_id, clientName: clients.get(r.client_id) || `Client ${r.client_id}`, cascaded_to: 0, recovered: 0 };
    }
    entry.clients[r.client_id].cascaded_to += r.cascaded_to;
    entry.clients[r.client_id].recovered += r.recovered;
  }

  const targets = [];
  for (const [, entry] of procMap) {
    const clientArr = Object.values(entry.clients).map(c => ({
      ...c,
      total: c.cascaded_to,
      approved: c.recovered,
      rate: _rate(c.recovered, c.cascaded_to),
    }));
    const netCasc = clientArr.reduce((s, c) => s + c.cascaded_to, 0);
    const netRec = clientArr.reduce((s, c) => s + c.recovered, 0);

    targets.push({
      processor: entry.processor,
      clients: clientArr,
      network: { cascaded_to: netCasc, recovered: netRec, rate: _rate(netRec, netCasc) },
      confidence: networkConfidence(clientArr),
    });
  }

  targets.sort((a, b) => b.network.cascaded_to - a.network.cascaded_to);
  return { targets, clientCount: clients.size, computedAt: new Date().toISOString() };
}

// ---------------------------------------------------------------------------
// 5. Rebill Decay
// ---------------------------------------------------------------------------

function computeRebillDecay(opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `rebill-decay:${days}`;
  return getCachedOrCompute(NETWORK_CLIENT_ID, 'network-rebill-decay', cacheKey, () => {
    return _computeRebillDecay(days);
  });
}

function _computeRebillDecay(days) {
  const clients = _clientNameMap();
  const dateFilter = daysAgoFilter(days);

  const rows = querySql(`
    SELECT o.client_id, g.processor_name,
      CASE WHEN o.derived_cycle >= 3 THEN 3 ELSE o.derived_cycle END AS cycle_group,
      COUNT(*) AS att,
      COUNT(CASE WHEN o.order_status ${APPROVED_STATUS_SQL} AND o.is_cascaded = 0 THEN 1 END) AS app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.derived_product_role = 'main_rebill'
      AND o.derived_attempt = 1
      AND ${CLEAN_FILTER}
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${dateFilter}
    GROUP BY o.client_id, g.processor_name, cycle_group
    HAVING att >= 10
  `);

  // Group by processor → cycle → client
  const procMap = new Map();
  for (const r of rows) {
    const normProc = normalizeProcessor(r.processor_name);
    if (!procMap.has(normProc)) procMap.set(normProc, { processor: r.processor_name, cycles: {} });
    const entry = procMap.get(normProc);
    const cycleLabel = r.cycle_group >= 3 ? 'C3+' : `C${r.cycle_group}`;
    if (!entry.cycles[cycleLabel]) entry.cycles[cycleLabel] = {};
    if (!entry.cycles[cycleLabel][r.client_id]) {
      entry.cycles[cycleLabel][r.client_id] = { clientId: r.client_id, clientName: clients.get(r.client_id) || `Client ${r.client_id}`, att: 0, app: 0 };
    }
    entry.cycles[cycleLabel][r.client_id].att += r.att;
    entry.cycles[cycleLabel][r.client_id].app += r.app;
  }

  const processors = [];
  for (const [, entry] of procMap) {
    const cycleData = {};
    for (const [cycle, clientMap] of Object.entries(entry.cycles)) {
      const clientArr = Object.values(clientMap).map(c => ({ ...c, total: c.att, approved: c.app, rate: _rate(c.app, c.att) }));
      const netAtt = clientArr.reduce((s, c) => s + c.att, 0);
      const netApp = clientArr.reduce((s, c) => s + c.app, 0);
      cycleData[cycle] = {
        clients: clientArr,
        network: { att: netAtt, app: netApp, rate: _rate(netApp, netAtt) },
      };
    }
    processors.push({ processor: entry.processor, cycles: cycleData });
  }

  // Sort by C1 network volume
  processors.sort((a, b) => (b.cycles.C1?.network?.att || 0) - (a.cycles.C1?.network?.att || 0));
  return { processors, clientCount: clients.size, computedAt: new Date().toISOString() };
}

// ---------------------------------------------------------------------------
// 6. Decline Distribution
// ---------------------------------------------------------------------------

function computeDeclineDistribution(opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `decline-distribution:${days}`;
  return getCachedOrCompute(NETWORK_CLIENT_ID, 'network-decline-distribution', cacheKey, () => {
    return _computeDeclineDistribution(days);
  });
}

function _computeDeclineDistribution(days) {
  const clients = _clientNameMap();
  const dateFilter = daysAgoFilter(days);

  const rows = querySql(`
    SELECT o.client_id, g.processor_name, o.decline_reason,
      COUNT(*) AS count
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.order_status = 7
      AND ${CLEAN_FILTER}
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
      AND g.processor_name IS NOT NULL
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${dateFilter}
    GROUP BY o.client_id, g.processor_name, o.decline_reason
    HAVING count >= 5
  `);

  // Group by processor → decline_reason → client
  const procMap = new Map();
  for (const r of rows) {
    const normProc = normalizeProcessor(r.processor_name);
    if (!procMap.has(normProc)) procMap.set(normProc, { processor: r.processor_name, reasons: {} });
    const entry = procMap.get(normProc);
    if (!entry.reasons[r.decline_reason]) entry.reasons[r.decline_reason] = {};
    if (!entry.reasons[r.decline_reason][r.client_id]) {
      entry.reasons[r.decline_reason][r.client_id] = { clientId: r.client_id, clientName: clients.get(r.client_id) || `Client ${r.client_id}`, count: 0 };
    }
    entry.reasons[r.decline_reason][r.client_id].count += r.count;
  }

  const processors = [];
  for (const [, entry] of procMap) {
    const reasons = [];
    for (const [reason, clientMap] of Object.entries(entry.reasons)) {
      const clientArr = Object.values(clientMap);
      const netCount = clientArr.reduce((s, c) => s + c.count, 0);
      reasons.push({ reason, clients: clientArr, networkCount: netCount, clientCoverage: clientArr.length });
    }
    reasons.sort((a, b) => b.networkCount - a.networkCount);
    processors.push({ processor: entry.processor, reasons: reasons.slice(0, 20) });
  }

  processors.sort((a, b) => {
    const aTotal = a.reasons.reduce((s, r) => s + r.networkCount, 0);
    const bTotal = b.reasons.reduce((s, r) => s + r.networkCount, 0);
    return bTotal - aTotal;
  });

  return { processors, clientCount: clients.size, computedAt: new Date().toISOString() };
}

// ---------------------------------------------------------------------------
// 7. Cross-Client Routing Playbook
// ---------------------------------------------------------------------------

function computeNetworkPlaybook(opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `network-playbook:${days}`;
  return getCachedOrCompute(NETWORK_CLIENT_ID, 'network-playbook', cacheKey, () => {
    return _computeNetworkPlaybook(days);
  });
}

function _computeNetworkPlaybook(days) {
  const clients = _clientNameMap();
  const dateFilter = daysAgoFilter(days);

  // Get initial processor performance per bank per client
  const initRows = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name,
      COUNT(*) AS att,
      SUM(CASE WHEN o.order_status ${APPROVED_STATUS_SQL} AND o.is_cascaded = 0 THEN 1 ELSE 0 END) AS app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.derived_product_role = 'main_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND ${CLEAN_FILTER}
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${CRM_ROUTING_EXCLUSION}
      AND ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name
    HAVING att >= 10
  `);

  // Get rebill C1 processor performance per bank per client
  const rebillRows = querySql(`
    SELECT o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name,
      COUNT(*) AS att,
      SUM(CASE WHEN o.order_status ${APPROVED_STATUS_SQL} AND o.is_cascaded = 0 THEN 1 ELSE 0 END) AS app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.derived_product_role = 'main_rebill'
      AND o.derived_cycle IN (1, 2) AND o.derived_attempt = 1
      AND ${CLEAN_FILTER}
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${dateFilter}
    GROUP BY o.client_id, b.issuer_bank, b.is_prepaid, g.processor_name
    HAVING att >= 10
  `);

  // Build bank × processor matrix across clients
  const bankProcMap = new Map();
  const _key = (bank, pp) => `${normalizeBank(bank)}|${pp}`;

  function _addRows(rows, type) {
    for (const r of rows) {
      const bk = _key(r.issuer_bank, r.is_prepaid);
      if (!bankProcMap.has(bk)) {
        bankProcMap.set(bk, { bank: normalizeBank(r.issuer_bank), is_prepaid: !!r.is_prepaid, initProcs: {}, rebillProcs: {} });
      }
      const entry = bankProcMap.get(bk);
      const normProc = normalizeProcessor(r.processor_name);
      const target = type === 'init' ? entry.initProcs : entry.rebillProcs;
      if (!target[normProc]) target[normProc] = { processor: r.processor_name, byClient: {} };
      target[normProc].byClient[r.client_id] = {
        clientId: r.client_id, clientName: clients.get(r.client_id) || `Client ${r.client_id}`,
        att: r.att, app: r.app, rate: _rate(r.app, r.att),
      };
    }
  }
  _addRows(initRows, 'init');
  _addRows(rebillRows, 'rebill');

  // Build output: per bank group with processor rankings
  const bankGroups = [];
  for (const [, entry] of bankProcMap) {
    function _buildProcList(procMap) {
      const list = [];
      for (const [, pd] of Object.entries(procMap)) {
        const clientArr = Object.values(pd.byClient);
        const netAtt = clientArr.reduce((s, c) => s + c.att, 0);
        const netApp = clientArr.reduce((s, c) => s + c.app, 0);
        list.push({
          processor: pd.processor,
          clients: clientArr,
          network: { att: netAtt, app: netApp, rate: _rate(netApp, netAtt) },
          confidence: networkConfidence(clientArr.map(c => ({ ...c, total: c.att, approved: c.app }))),
        });
      }
      list.sort((a, b) => b.network.rate - a.network.rate);
      return list;
    }

    const initProcs = _buildProcList(entry.initProcs);
    const rebillProcs = _buildProcList(entry.rebillProcs);
    const totalInitVol = initProcs.reduce((s, p) => s + p.network.att, 0);
    const totalRebillVol = rebillProcs.reduce((s, p) => s + p.network.att, 0);

    // Check if best processor is consistent across clients
    let initAgreement = 'N/A';
    if (initProcs.length > 0) {
      const bestProc = normalizeProcessor(initProcs[0].processor);
      const clientBests = {};
      for (const p of initProcs) {
        for (const c of p.clients) {
          if (!clientBests[c.clientId] || c.rate > clientBests[c.clientId].rate) {
            clientBests[c.clientId] = { processor: normalizeProcessor(p.processor), rate: c.rate };
          }
        }
      }
      const bests = Object.values(clientBests);
      const agree = bests.filter(b => b.processor === bestProc).length;
      initAgreement = `${agree}/${bests.length}`;
    }

    bankGroups.push({
      bank: entry.bank,
      is_prepaid: entry.is_prepaid,
      initProcessors: initProcs,
      rebillProcessors: rebillProcs,
      initVolume: totalInitVol,
      rebillVolume: totalRebillVol,
      initAgreement,
    });
  }

  bankGroups.sort((a, b) => b.initVolume - a.initVolume);
  return { bankGroups, clientCount: clients.size, computedAt: new Date().toISOString() };
}

// ---------------------------------------------------------------------------
// Summary (all analyses in one call)
// ---------------------------------------------------------------------------

function computeNetworkSummary(opts = {}) {
  return {
    processorComparison: computeProcessorComparison(opts),
    bankRankings: computeBankRankings(opts),
    binPatterns: computeBinPatterns(opts),
    cascadeComparison: computeCascadeComparison(opts),
    rebillDecay: computeRebillDecay(opts),
    declineDistribution: computeDeclineDistribution(opts),
    playbook: computeNetworkPlaybook(opts),
  };
}

/** Clear network-level cache entries */
function clearNetworkCache() {
  try {
    runSql('DELETE FROM analytics_cache WHERE client_id = ?', [NETWORK_CLIENT_ID]);
    saveDb();
  } catch (e) { /* table may not exist */ }
}

module.exports = {
  computeProcessorComparison,
  computeBankRankings,
  computeBinPatterns,
  computeCascadeComparison,
  computeRebillDecay,
  computeDeclineDistribution,
  computeNetworkPlaybook,
  computeNetworkSummary,
  clearNetworkCache,
  NETWORK_CLIENT_ID,
  // Shared utilities for network-playbook.js
  normalizeProcessor,
  normalizeBank,
  _rate,
  _getClients,
  BIN_EXCL,
};
