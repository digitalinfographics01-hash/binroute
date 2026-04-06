/**
 * Flow Optix — Unified rebill card generator.
 *
 * Combines gateway routing + LTV price analysis + salvage sequence into one
 * output per BIN group.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql } = require('../db/connection');
const { CLEAN_FILTER, getCachedOrCompute, daysAgoFilter } = require('./engine');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const BUCKETS = [
  { label: '$0-25',   min: 0,   max: 25 },
  { label: '$26-50',  min: 26,  max: 50 },
  { label: '$51-75',  min: 51,  max: 75 },
  { label: '$76-100', min: 76,  max: 100 },
  { label: '$100+',   min: 101, max: 999999 },
];

const BUCKET_MIDPOINTS = {
  '$0-25': 12.50,
  '$26-50': 38,
  '$51-75': 63,
  '$76-100': 88,
  '$100+': 110,
};

const MIN_ATTEMPTS        = 30;
const MIN_SALVAGE_ATTEMPTS = 30;
const MIN_ATTEMPT_SAMPLE  = 15;
const CPA                 = 42.50;
const FULL_PRICE_BUCKET   = '$76-100';
const FULL_PRICE_MIDPOINT = 88;
const BREAKEVEN_IMPROVEMENT = 0.70;

const MAX_ATTEMPT_NUMBER = 10;

// ---------------------------------------------------------------------------
// Decline Eligibility for Salvage Retry
// ---------------------------------------------------------------------------

/**
 * Normalize issuer bank name to merge variants.
 * "JPMORGAN CHASE BANK N.A. - DEBIT" → "JPMORGAN CHASE BANK N.A."
 * "JPMORGAN CHASE BANK, N.A. - PREPAID DEBIT" → "JPMORGAN CHASE BANK, N.A."
 * Strips " - DEBIT", " - CREDIT", " - PREPAID DEBIT", " - CONSUMER CREDIT" etc.
 */
function normalizeIssuer(name) {
  if (!name) return '';
  return name.replace(/\s*-\s*(DEBIT|CREDIT|PREPAID\s+DEBIT|CONSUMER\s+CREDIT)$/i, '').trim();
}

/**
 * Build issuer-specific recovery rate lookup from actual data.
 * Groups by normalized issuer name to merge variants (e.g. "JPMORGAN CHASE BANK N.A." + "- DEBIT").
 * Key: `${decline_reason}|${normalized_issuer}|${card_type}` → { attempts, recovered, rate }
 */
function buildRecoveryLookup(clientId) {
  // Query at raw issuer level, then aggregate in JS after normalizing
  const rows = querySql(`
    SELECT d.decline_reason, b.issuer_bank, b.card_type,
      COUNT(*) as attempts,
      COUNT(CASE WHEN r.order_id IS NOT NULL THEN 1 END) as recovered
    FROM orders d
    LEFT JOIN bin_lookup b ON d.cc_first_6 = b.bin
    LEFT JOIN orders r ON r.customer_id = d.customer_id
      AND r.product_group_id = d.product_group_id
      AND r.derived_cycle = d.derived_cycle
      AND r.attempt_number > d.attempt_number
      AND r.order_status IN (2,6,8) AND r.is_test = 0
    WHERE d.client_id = ? AND d.derived_product_role IN ('main_rebill','upsell_rebill')
    AND d.is_test = 0 AND d.is_internal_test = 0
    AND d.order_status = 7
    AND d.decline_reason IS NOT NULL AND d.decline_reason != ''
    GROUP BY d.decline_reason, b.issuer_bank, b.card_type
  `, [clientId]);

  // Aggregate by normalized issuer name
  const agg = new Map();
  for (const r of rows) {
    const normIssuer = normalizeIssuer(r.issuer_bank);
    const key = `${r.decline_reason}|${normIssuer}|${r.card_type || ''}`;
    if (!agg.has(key)) agg.set(key, { attempts: 0, recovered: 0 });
    const entry = agg.get(key);
    entry.attempts += r.attempts;
    entry.recovered += r.recovered;
  }

  // Filter to 15+ attempts and compute rate
  const lookup = new Map();
  for (const [key, entry] of agg) {
    if (entry.attempts >= MIN_LOOKUP_ATTEMPTS) {
      lookup.set(key, {
        attempts: entry.attempts,
        recovered: entry.recovered,
        rate: entry.attempts > 0 ? (entry.recovered / entry.attempts) * 100 : 0,
      });
    }
  }

  // DNH issuer-specific overrides at 2% threshold.
  // These 5 issuers are marginally profitable to retry (~$57 net per recovery).
  // All other DNH issuers below 4% remain BLOCK.
  const DNH_OVERRIDES = [
    { issuer: 'DISCOVER ISSUER',                         type: 'CREDIT', rate: 2.7, att: 364, rec: 10 },
    { issuer: 'BANK OF AMERICA, NATIONAL ASSOCIATION',   type: 'DEBIT',  rate: 2.2, att: 138, rec: 3 },
    { issuer: 'FISERV SOLUTIONS, LLC',                   type: 'DEBIT',  rate: 1.9, att: 104, rec: 2 },
    { issuer: 'BMO BANK NATIONAL ASSOCIATION',           type: 'DEBIT',  rate: 2.7, att: 74,  rec: 2 },
    { issuer: 'SYNCHRONY BANK',                          type: 'CREDIT', rate: 3.1, att: 65,  rec: 2 },
  ];
  for (const ov of DNH_OVERRIDES) {
    const normIssuer = normalizeIssuer(ov.issuer);
    const key = `Do Not Honor|${normIssuer}|${ov.type}`;
    const existing = lookup.get(key);
    // Only override if the data-driven rate is below threshold
    if (!existing || existing.rate < RECOVERY_THRESHOLD) {
      lookup.set(key, {
        attempts: existing ? existing.attempts : ov.att,
        recovered: existing ? existing.recovered : ov.rec,
        rate: ov.rate, // Actual rate — below 4% but explicitly allowed
        override: true,
        override_reason: 'DNH marginal — net positive',
      });
    }
  }

  return lookup;
}

const RECOVERY_THRESHOLD = 4; // 4% minimum recovery rate to be eligible
const MIN_LOOKUP_ATTEMPTS = 15;

/**
 * Determine if a declined order is eligible for salvage retry.
 * Uses issuer-specific recovery rates when available (15+ attempts),
 * falls back to category-level rules otherwise.
 * @param {string} declineCategory
 * @param {string} declineReason
 * @param {string} issuerBank
 * @param {string} cardType
 * @param {Map} recoveryLookup - from buildRecoveryLookup()
 * Returns { eligible, mustSwitchGw, reason }
 */
function declineEligibility(declineCategory, declineReason, issuerBank, cardType, recoveryLookup) {
  const cat = (declineCategory || '').toLowerCase();

  // CRM blocks → never eligible (regardless of data)
  if (cat === 'crm_routing_rule' || cat === 'crm_breach') {
    return { eligible: false, mustSwitchGw: false, reason: 'CRM block' };
  }

  // NULL / unclassified → ineligible
  if (!cat) {
    return { eligible: false, mustSwitchGw: false, reason: 'unclassified' };
  }

  // Processor → always eligible but must switch gateway
  if (cat === 'processor') {
    return { eligible: true, mustSwitchGw: true, reason: 'processor — must switch gateway' };
  }

  // For soft + issuer: check issuer-specific recovery rate first
  const reason = (declineReason || '').toLowerCase();
  const isInsufficientFunds = reason.includes('insufficient');

  if (recoveryLookup && declineReason) {
    const normIssuer = normalizeIssuer(issuerBank);
    const key = `${declineReason}|${normIssuer}|${cardType || ''}`;
    const stats = recoveryLookup.get(key);

    if (stats && stats.attempts >= MIN_LOOKUP_ATTEMPTS) {
      if (stats.rate >= RECOVERY_THRESHOLD || stats.override) {
        const overrideNote = stats.override ? ` [${stats.override_reason}]` : '';
        return {
          eligible: true,
          mustSwitchGw: false,
          reason: `${stats.rate.toFixed(1)}% recovery (${stats.recovered}/${stats.attempts})${overrideNote}`,
          isInsufficientFunds,
          issuerRecoveryRate: stats.rate,
        };
      } else if (isInsufficientFunds) {
        // Insufficient funds with < 4% overall — still eligible with timing override
        // Give them one chance at 3-4 day timing
        return {
          eligible: true,
          mustSwitchGw: false,
          reason: `${stats.rate.toFixed(1)}% overall — eligible via timing override (wait 3-4 days)`,
          isInsufficientFunds: true,
          issuerRecoveryRate: stats.rate,
          timingOverride: true,
        };
      } else {
        return {
          eligible: false,
          mustSwitchGw: false,
          reason: `${stats.rate.toFixed(1)}% recovery < ${RECOVERY_THRESHOLD}% threshold (${stats.attempts} att)`,
        };
      }
    }
  }

  // Fallback: category-level rules (insufficient issuer-specific data)
  if (cat === 'soft') {
    return { eligible: true, mustSwitchGw: false, reason: 'soft decline (category fallback)', isInsufficientFunds };
  }

  if (cat === 'issuer') {
    if (isInsufficientFunds) {
      // Insufficient funds always gets one chance with timing
      return { eligible: true, mustSwitchGw: false, reason: 'insufficient funds (category fallback, timing override)', isInsufficientFunds: true, timingOverride: true };
    }
    return { eligible: false, mustSwitchGw: false, reason: 'issuer decline (no issuer-specific data)' };
  }

  return { eligible: false, mustSwitchGw: false, reason: 'unknown category' };
}

// ---------------------------------------------------------------------------
// Insufficient Funds — Special Timing Rules
// ---------------------------------------------------------------------------

const INSUF_TIMING = {
  attempt1WaitDays: '3-4',   // Wait 3-4 days before first retry
  attempt2WaitDays: '8-10',  // Wait 8-10 days if first retry also fails (payday hypothesis)
  maxAttempts: 2,            // Stop after 2 salvage attempts for insufficient funds
  attempt2MinRecovery: 2,    // 2% minimum overall recovery to proceed to attempt 2 (exploratory)
  attempt2Flag: 'is_payday_test', // Tag for tracking payday hypothesis
};

/**
 * Get insufficient funds timing recommendation for a given attempt.
 * Returns { waitDays, isExploratoryy, note } or null if no special timing.
 */
function insufficientFundsTiming(declineReason, attemptNumber, issuerOverallRecoveryRate) {
  if (!(declineReason || '').toLowerCase().includes('insufficient')) return null;

  if (attemptNumber === 1) {
    // First salvage attempt after insufficient funds decline
    return {
      waitDays: INSUF_TIMING.attempt1WaitDays,
      isExploratory: false,
      note: 'Wait 3-4 days before retry',
    };
  }

  if (attemptNumber === 2) {
    // Second attempt — only if issuer has >= 2% overall recovery
    if (issuerOverallRecoveryRate >= INSUF_TIMING.attempt2MinRecovery) {
      return {
        waitDays: INSUF_TIMING.attempt2WaitDays,
        isExploratory: true,
        note: `Wait 8-10 days (payday test — ${INSUF_TIMING.attempt2Flag})`,
      };
    }
    return null; // Block — issuer too low to justify exploratory attempt
  }

  return null; // Attempt 3+ → stop for insufficient funds
}

// ---------------------------------------------------------------------------
// LTV Decision Engine
// ---------------------------------------------------------------------------

/**
 * Compounding LTV calculation.
 * Models retention decay: each cycle, only approval_rate% of customers survive.
 * Returns LTV per customer (normalized from 100 starting customers).
 */
function compoundingLtv(approvalRate, priceMidpoint, cycles) {
  let retained = 100;
  let totalRevenue = 0;
  for (let i = 0; i < cycles; i++) {
    retained = retained * (approvalRate / 100);
    totalRevenue += retained * priceMidpoint;
  }
  return Math.round((totalRevenue / 100) * 100) / 100; // per customer
}

/**
 * LTV decision engine: Should we reduce price for this group?
 * Uses compounding retention model — each cycle only approval% of customers remain.
 * Returns { shouldReduce, ltvFull, ltvReduced, ltvGain, explanation, remainingCycles, conditions }
 */
function ltvDecision(fullPriceRate, reducedPriceRate, reducedBucket, expectedLifetime, currentCycle) {
  const remainingCycles = Math.max(0, Math.round((expectedLifetime || 5) - currentCycle));
  const reducedMidpoint = BUCKET_MIDPOINTS[reducedBucket] || 63;

  // Compounding LTV per customer
  const ltvFull = compoundingLtv(fullPriceRate, FULL_PRICE_MIDPOINT, remainingCycles);
  const ltvReduced = compoundingLtv(reducedPriceRate, reducedMidpoint, remainingCycles);

  // Condition 1: reduced LTV > full LTV (compounding model)
  const cond1 = ltvReduced > ltvFull;

  // Condition 2: approval improvement >= 70% (breakeven buffer for $63 vs $88)
  const improvementRatio = fullPriceRate > 0 ? (reducedPriceRate - fullPriceRate) / fullPriceRate : 0;
  const cond2 = improvementRatio >= BREAKEVEN_IMPROVEMENT;

  // Condition 3: enough subscription life remaining (>= 3 cycles)
  const cond3 = remainingCycles >= 3;

  const shouldReduce = cond1 && cond2 && cond3;
  const ltvGain = Math.round((ltvReduced - ltvFull) * 100) / 100;

  const explanation = shouldReduce
    ? `LTV per customer: $${ltvReduced.toFixed(2)} (reduced) vs $${ltvFull.toFixed(2)} (full price) | Gain: $${ltvGain.toFixed(2)}`
    : `LTV does not justify — ${!cond1 ? '$' + ltvReduced.toFixed(2) + ' reduced < $' + ltvFull.toFixed(2) + ' full' : !cond2 ? 'approval lift <70% (' + Math.round(improvementRatio * 100) + '%)' : 'remaining cycles <3 (' + remainingCycles + ')'}`;

  return { shouldReduce, ltvFull, ltvReduced, ltvGain, explanation, remainingCycles, conditions: { cond1, cond2, cond3 } };
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

function getBucket(orderTotal) {
  for (const b of BUCKETS) {
    if (orderTotal >= b.min && orderTotal <= b.max) return b.label;
  }
  return BUCKETS[BUCKETS.length - 1].label;
}

function buildGroupKey(row, level) {
  const bank = row.issuer_bank || 'Unknown';
  const isPrepaid = row.is_prepaid ? true : false;
  // Prepaid is always separate. Prepaid groups don't further split by card_type.
  switch (level) {
    case 1: return `${bank}|${isPrepaid ? 'PREPAID' : 'STD'}`;
    case 2: return `${bank}|${row.card_brand || 'Unknown'}|${isPrepaid ? 'PREPAID' : 'STD'}`;
    case 3: return isPrepaid
      ? `${bank}|${row.card_brand || 'Unknown'}|PREPAID`
      : `${bank}|${row.card_brand || 'Unknown'}|${row.card_type || 'Unknown'}`;
    case 4: return isPrepaid
      ? `${bank}|${row.card_brand || 'Unknown'}|PREPAID|${row.card_level || 'Unknown'}`
      : `${bank}|${row.card_brand || 'Unknown'}|${row.card_type || 'Unknown'}|${row.card_level || 'Unknown'}`;
    default: return `${bank}|${row.card_brand || 'Unknown'}|${isPrepaid ? 'PREPAID' : 'STD'}`;
  }
}

function buildGroupLabel(key, level) {
  const parts = key.split('|');
  const lastPart = parts[parts.length - 1];
  const isPrepaid = lastPart === 'PREPAID';
  // At L3+, "PREPAID" replaces card_type in label. At L1-L2, it's the suffix.
  switch (level) {
    case 1: return `${parts[0]}${isPrepaid ? ' \u00b7 Prepaid' : ''}`;
    case 2: return `${parts[0]} \u00b7 ${parts[1]}${isPrepaid ? ' \u00b7 Prepaid' : ''}`;
    case 3: return `${parts[0]} \u00b7 ${parts[1]} \u00b7 ${isPrepaid ? 'Prepaid' : parts[2]}`;
    case 4: return `${parts[0]} \u00b7 ${parts[1]} \u00b7 ${isPrepaid ? 'Prepaid' : parts[2]} \u00b7 ${parts[3]}`;
    default: return parts.join(' \u00b7 ');
  }
}

function _rate(approved, total) {
  if (total === 0) return 0;
  return Math.round((approved / total) * 10000) / 100;
}

function _getBucketStats(orders, targetBucket) {
  let attempts = 0;
  let approved = 0;
  for (const o of orders) {
    if (o.bucket === targetBucket) {
      attempts++;
      if (o.isApproved) approved++;
    }
  }
  return { attempts, approved, rate: _rate(approved, attempts) };
}

function _findBestGateway(orders) {
  if (orders.length === 0) {
    return { gatewayId: null, gatewayAlias: null, processorName: null, rate: 0 };
  }

  const gwStats = new Map();
  for (const o of orders) {
    if (!gwStats.has(o.gateway_id)) {
      gwStats.set(o.gateway_id, {
        attempts: 0,
        approved: 0,
        alias: o.gateway_alias,
        processor: o.processor_name,
      });
    }
    const s = gwStats.get(o.gateway_id);
    s.attempts++;
    if (o.isApproved) s.approved++;
  }

  let bestGwId = null;
  let bestRate = -1;
  for (const [gwId, s] of gwStats) {
    const rate = _rate(s.approved, s.attempts);
    if (rate > bestRate) {
      bestRate = rate;
      bestGwId = gwId;
    }
  }

  if (bestGwId === null) {
    return { gatewayId: null, gatewayAlias: null, processorName: null, rate: 0 };
  }

  const best = gwStats.get(bestGwId);
  return {
    gatewayId: bestGwId,
    gatewayAlias: best.alias,
    processorName: best.processor,
    rate: bestRate,
  };
}

function _findBestGatewayAtBucket(orders, targetBucket) {
  const filtered = orders.filter(o => o.bucket === targetBucket);
  return _findBestGateway(filtered);
}

// ---------------------------------------------------------------------------
// computeFlowOptix
// ---------------------------------------------------------------------------

/**
 * Generate unified Flow Optix rebill cards: gateway routing + LTV price
 * analysis + salvage sequence combined into one output per BIN group.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {number} [opts.level] - Grouping level 1-4 (default 2)
 * @param {number} [opts.days]  - Lookback window in days (default 90)
 * @returns {{ cards: Array<object>, processorAffinityCards: Array<object>, summary: object }}
 */
function computeFlowOptix(clientId, opts = {}) {
  const level = opts.level ?? 2;
  const days  = opts.days ?? 180;

  const cacheKey = `flow-optix:${level}:${days}`;

  return getCachedOrCompute(clientId, 'flow-optix', cacheKey, () => {
    return _computeFlowOptix(clientId, level, days);
  });
}

// ---------------------------------------------------------------------------
// Internal implementation
// ---------------------------------------------------------------------------

function _computeFlowOptix(clientId, level, days) {
  // =========================================================================
  // Step 1: Load gateway metadata
  // =========================================================================
  // Build issuer-specific recovery rate lookup
  const recoveryLookup = buildRecoveryLookup(clientId);

  const gwRows = querySql(`
    SELECT gateway_id, gateway_alias, bank_name, processor_name,
           lifecycle_state, gateway_active, exclude_from_analysis
    FROM gateways WHERE client_id = ?
  `, [clientId]);

  const gwMap = new Map();
  const activeGwIds = new Set();
  const gwToProcessor = new Map();

  for (const gw of gwRows) {
    gwMap.set(gw.gateway_id, gw);
    gwToProcessor.set(gw.gateway_id, gw.processor_name || gw.gateway_alias || `Gateway #${gw.gateway_id}`);

    const isClosed   = (gw.lifecycle_state || '').toLowerCase() === 'closed';
    const isInactive = gw.gateway_active === 0;
    const isExcluded = gw.exclude_from_analysis === 1;

    if (!isClosed && !isInactive && !isExcluded) {
      activeGwIds.add(gw.gateway_id);
    }
  }

  // =========================================================================
  // Step 2: Query rebill performance (tp_rebill only)
  // =========================================================================
  const rebillRows = querySql(`
    SELECT o.cc_first_6 AS bin, o.gateway_id, o.order_total, o.order_status,
      b.issuer_bank, b.card_brand, b.card_type, b.card_level, b.is_prepaid,
      g.gateway_alias, g.processor_name
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role IN ('main_rebill', 'upsell_rebill') AND o.derived_attempt = 1
      AND o.order_status IN (2,6,7,8) AND o.order_total > 0
      AND COALESCE(g.exclude_from_analysis, 0) != 1
      AND ${daysAgoFilter(days)}
  `, [clientId]);

  // =========================================================================
  // Step 3: Query salvage chain data (tp_rebill + tp_rebill_salvage)
  // =========================================================================
  const salvageRows = querySql(`
    SELECT o.cc_first_6 AS bin, o.customer_id, o.product_group_id, o.derived_cycle,
      o.attempt_number, o.gateway_id, o.order_total, o.order_status, o.tx_type,
      o.decline_category, o.decline_reason,
      b.issuer_bank, b.card_brand, b.card_type, b.card_level, b.is_prepaid,
      g.gateway_alias, g.processor_name
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.order_status IN (2,6,7,8) AND o.order_total > 0
      AND COALESCE(g.exclude_from_analysis, 0) != 1
      AND ${daysAgoFilter(days)}
    ORDER BY o.customer_id, o.product_group_id, o.derived_cycle, o.attempt_number
  `, [clientId]);

  // Query expected lifetime (avg cycles per customer with 2+ cycles)
  // Expected subscription lifetime — planning assumption, not derived from
  // incomplete data (avg current cycle is ~2.2 but subscriptions are ongoing)
  const expectedLifetime = 6;

  if (rebillRows.length === 0 && salvageRows.length === 0) {
    return {
      cards: [],
      processorAffinityCards: [],
      summary: {
        totalCards: 0,
        level,
        byVerdict: { 'GATEWAY + PRICE': 0, 'GATEWAY ONLY': 0, 'PRICE + GATEWAY': 0, 'REVIEW': 0 },
        avgCyclesPerCustomer: expectedLifetime,
        processorAffinity: { total: 0 },
      },
    };
  }

  // =========================================================================
  // Step 4: Group rebill rows by BIN group
  // =========================================================================
  const rebillGroupMap = new Map();

  for (const row of rebillRows) {
    const key = buildGroupKey(row, level);
    const bucket = getBucket(row.order_total);
    const isApproved = [2, 6, 8].includes(row.order_status);

    if (!rebillGroupMap.has(key)) {
      rebillGroupMap.set(key, {
        issuer_bank: row.issuer_bank || 'Unknown',
        card_brand:  level >= 2 ? (row.card_brand || 'Unknown') : null,
        card_type:   level >= 3 ? (row.card_type || 'Unknown') : null,
        card_level:  level === 4 ? (row.card_level || 'Unknown') : null,
        is_prepaid:  row.is_prepaid ? 1 : 0,
        bins:        new Set(),
        totalAttempts: 0,
        buckets: {},
        _gwBuckets: {},
      });
      for (const b of BUCKETS) {
        rebillGroupMap.get(key).buckets[b.label] = { attempts: 0, approved: 0, rate: 0 };
        rebillGroupMap.get(key)._gwBuckets[b.label] = new Map();
      }
    }

    const grp = rebillGroupMap.get(key);
    grp.totalAttempts++;
    if (row.bin) grp.bins.add(row.bin);

    const bkt = grp.buckets[bucket];
    bkt.attempts++;
    if (isApproved) bkt.approved++;

    const gwBktMap = grp._gwBuckets[bucket];
    if (!gwBktMap.has(row.gateway_id)) {
      gwBktMap.set(row.gateway_id, { attempts: 0, approved: 0, alias: row.gateway_alias, processor: row.processor_name });
    }
    const gwBkt = gwBktMap.get(row.gateway_id);
    gwBkt.attempts++;
    if (isApproved) gwBkt.approved++;
  }

  // Compute bucket rates
  for (const [, grp] of rebillGroupMap) {
    for (const b of BUCKETS) {
      const bkt = grp.buckets[b.label];
      bkt.rate = bkt.attempts > 0
        ? Math.round((bkt.approved / bkt.attempts) * 10000) / 100
        : 0;
    }
  }

  // =========================================================================
  // Step 4b: Group salvage rows — build chains
  // =========================================================================
  const chainMap = new Map();

  for (const row of salvageRows) {
    const chainKey = `${row.customer_id}|${row.product_group_id}|${row.derived_cycle}`;
    if (!chainMap.has(chainKey)) {
      chainMap.set(chainKey, { rows: [], originGatewayId: null });
    }
    const chain = chainMap.get(chainKey);
    chain.rows.push(row);
    if (row.attempt_number === 1) {
      chain.originGatewayId = row.gateway_id;
    }
  }

  // Flatten into salvage group map
  const salvageGroupMap = new Map();

  for (const [, chain] of chainMap) {
    chain.orderEntries = [];
    for (const row of chain.rows) {
      const attemptNum = row.attempt_number;
      if (attemptNum < 1 || attemptNum > MAX_ATTEMPT_NUMBER) continue;

      const key = buildGroupKey(row, level);
      const isApproved = [2, 6, 8].includes(row.order_status);
      const bucket = getBucket(row.order_total);
      const isSameGw = (chain.originGatewayId !== null) && (row.gateway_id === chain.originGatewayId);

      if (!salvageGroupMap.has(key)) {
        salvageGroupMap.set(key, {
          issuer_bank: row.issuer_bank || 'Unknown',
          card_brand:  level >= 2 ? (row.card_brand || 'Unknown') : null,
          card_type:   level >= 3 ? (row.card_type || 'Unknown') : null,
          card_level:  level === 4 ? (row.card_level || 'Unknown') : null,
          is_prepaid:  row.is_prepaid ? 1 : 0,
          bins:        new Set(),
          totalAttempts: 0,
          attemptMap:  new Map(),
        });
      }

      const grp = salvageGroupMap.get(key);
      grp.totalAttempts++;
      if (row.bin) grp.bins.add(row.bin);

      if (!grp.attemptMap.has(attemptNum)) {
        grp.attemptMap.set(attemptNum, {
          allOrders: [],
          sameGwOrders: [],
          switchGwOrders: [],
        });
      }

      const slot = grp.attemptMap.get(attemptNum);
      const orderEntry = {
        bucket,
        isApproved,
        gateway_id: row.gateway_id,
        gateway_alias: row.gateway_alias || `Gateway #${row.gateway_id}`,
        processor_name: row.processor_name || null,
        eligibility: row.order_status === 7
          ? declineEligibility(row.decline_category, row.decline_reason, row.issuer_bank, row.card_type, recoveryLookup)
          : null,
        declineCategory: row.decline_category,
        declineReason: row.decline_reason,
      };

      slot.allOrders.push(orderEntry);
      chain.orderEntries.push(orderEntry);

      if (attemptNum >= 2) {
        if (isSameGw) {
          slot.sameGwOrders.push(orderEntry);
        } else {
          slot.switchGwOrders.push(orderEntry);
        }
      }
    }
  }

  // =========================================================================
  // Step 4c: Mark priorDeclineEligible on each chain order
  // =========================================================================
  for (const [, chain] of chainMap) {
    for (let i = 0; i < chain.orderEntries.length; i++) {
      if (i === 0) {
        chain.orderEntries[i].priorDeclineEligible = true;
      } else {
        const prior = chain.orderEntries[i - 1];
        chain.orderEntries[i].priorDeclineEligible = prior.isApproved ? true : (prior.eligibility?.eligible ?? false);
        chain.orderEntries[i].priorDeclineCategory = prior.isApproved ? null : prior.declineCategory;
        chain.orderEntries[i].priorDeclineReason = prior.isApproved ? null : prior.declineReason;
      }
    }
  }

  // =========================================================================
  // Step 4d: Add eligible order filters to each attempt slot
  // =========================================================================
  for (const [, grp] of salvageGroupMap) {
    for (const [, slot] of grp.attemptMap) {
      slot.eligibleOrders = slot.allOrders.filter(o => o.priorDeclineEligible);
      slot.eligibleSameGw = slot.sameGwOrders.filter(o => o.priorDeclineEligible);
      slot.eligibleSwitchGw = slot.switchGwOrders.filter(o => o.priorDeclineEligible);
    }
  }

  // =========================================================================
  // Step 5-8: Build cards — merge all BIN group keys
  // =========================================================================
  const allKeys = new Set([...rebillGroupMap.keys(), ...salvageGroupMap.keys()]);
  const cards = [];

  for (const key of allKeys) {
    const rebillGrp  = rebillGroupMap.get(key);
    const salvageGrp = salvageGroupMap.get(key);

    // Use whichever group has metadata
    const meta = rebillGrp || salvageGrp;

    // Total attempts across both datasets
    const totalAttempts = (rebillGrp ? rebillGrp.totalAttempts : 0)
                        + (salvageGrp ? salvageGrp.totalAttempts : 0);

    if (totalAttempts < MIN_ATTEMPTS) continue;

    // Merge BINs from both sources
    const binsSet = new Set();
    if (rebillGrp) for (const b of rebillGrp.bins) binsSet.add(b);
    if (salvageGrp) for (const b of salvageGrp.bins) binsSet.add(b);

    // -------------------------------------------------------------------
    // GATEWAY section
    // -------------------------------------------------------------------
    let gatewaySection = null;
    let gatewayLift = 0;

    if (rebillGrp) {
      const gwBktCurrent = rebillGrp._gwBuckets[FULL_PRICE_BUCKET];
      const gwPerf = [];

      for (const [gwId, stats] of gwBktCurrent) {
        const isActive = activeGwIds.has(gwId);
        const rate = stats.attempts > 0
          ? Math.round((stats.approved / stats.attempts) * 10000) / 100
          : 0;
        gwPerf.push({
          gateway_id: gwId,
          alias: stats.alias || gwMap.get(gwId)?.gateway_alias || `Gateway #${gwId}`,
          processor: stats.processor || gwMap.get(gwId)?.processor_name || null,
          attempts: stats.attempts,
          approved: stats.approved,
          rate,
          isActive,
        });
      }

      gwPerf.sort((a, b) => b.rate - a.rate);

      // Best active gateway (highest rate among active)
      const activeGws = gwPerf.filter(g => g.isActive);
      const bestActive = activeGws.length > 0
        ? activeGws.reduce((best, g) => g.rate > best.rate ? g : best, activeGws[0])
        : null;
      // Current gateway = highest volume among active (what we're routing to now)
      const currentGw = activeGws.length > 0
        ? activeGws.reduce((max, g) => g.attempts > max.attempts ? g : max, activeGws[0])
        : null;

      // Always show gateway info if we have active gateways
      if (bestActive && currentGw) {
        if (activeGws.length >= 2 && bestActive.gateway_id !== currentGw.gateway_id) {
          gatewayLift = Math.round((bestActive.rate - currentGw.rate) * 100) / 100;
        }

        gatewaySection = {
          bestGateway: {
            gateway_id: bestActive.gateway_id,
            alias: bestActive.alias,
            processor: bestActive.processor,
            rate: bestActive.rate,
          },
          currentGateway: {
            gateway_id: currentGw.gateway_id,
            alias: currentGw.alias,
            processor: currentGw.processor,
            rate: currentGw.rate,
          },
          liftPp: gatewayLift > 0 ? gatewayLift : 0,
          singleGateway: activeGws.length < 2,
        };
        if (gatewayLift > 0) {
          // Keep existing behavior — verdicts use gatewayLift
        }
      }
    }

    // -------------------------------------------------------------------
    // PRICE section
    // -------------------------------------------------------------------
    let priceSection = null;

    if (rebillGrp) {
      const fullPriceRate = rebillGrp.buckets[FULL_PRICE_BUCKET].rate;

      // Find best other bucket by LTV
      let bestLtvResult = null;
      let bestOtherBucket = null;
      let bestOtherRate = 0;

      const groupIsPrepaid = (meta && meta.is_prepaid) ? true : false;

      for (const b of BUCKETS) {
        if (b.label === FULL_PRICE_BUCKET) continue;
        // Non-prepaid: floor at $51-75, never recommend $0-25 or $26-50
        if (!groupIsPrepaid && (b.label === '$0-25' || b.label === '$26-50')) continue;
        const bkt = rebillGrp.buckets[b.label];
        if (bkt.attempts < 10) continue;

        const ltv = ltvDecision(fullPriceRate, bkt.rate, b.label, expectedLifetime, 1);
        if (ltv.shouldReduce) {
          // For prepaid $0-25: only if ltvGain > 100 (very high bar)
          if (b.label === '$0-25' && ltv.ltvGain <= 100) continue;

          if (!bestLtvResult || ltv.ltvGain > bestLtvResult.ltvGain) {
            bestLtvResult = ltv;
            bestOtherBucket = b.label;
            bestOtherRate = bkt.rate;
          }
        }
      }

      if (bestLtvResult && bestOtherBucket) {
        priceSection = {
          recommendedBucket: bestOtherBucket,
          currentBucket: FULL_PRICE_BUCKET,
          rateAtRecommended: bestOtherRate,
          rateAtCurrent: fullPriceRate,
          ltv: bestLtvResult,
          isLowPrice: bestOtherBucket === '$0-25' || bestOtherBucket === '$26-50',
        };
      }
    }

    // -------------------------------------------------------------------
    // SALVAGE section
    // -------------------------------------------------------------------
    let salvageSection = null;

    if (salvageGrp) {
      if (salvageGrp.totalAttempts < MIN_SALVAGE_ATTEMPTS) {
        salvageSection = {
          totalAttempts: salvageGrp.totalAttempts,
          insufficient: true,
          attempts: [],
        };
      } else {
        // Determine price bucket for salvage: use reduced bucket if price section passed, else $76-100
        const salvageBucket = priceSection ? priceSection.recommendedBucket : FULL_PRICE_BUCKET;

        // -------------------------------------------------------------------
        // Eligibility stats for this BIN group's salvage declines
        // -------------------------------------------------------------------
        const allSalvageDeclines = [];
        for (const [num, slot] of salvageGrp.attemptMap) {
          for (const o of slot.allOrders) {
            if (!o.isApproved) allSalvageDeclines.push(o);
          }
        }
        const eligibleDeclines = allSalvageDeclines.filter(o => o.eligibility?.eligible);
        const ineligibleDeclines = allSalvageDeclines.filter(o => !o.eligibility?.eligible);
        const eligiblePct = allSalvageDeclines.length > 0 ? (eligibleDeclines.length / allSalvageDeclines.length * 100) : 0;

        // Historical recovery rate for this group
        let totalAtt2Plus = 0, recoveredAtt2Plus = 0;
        for (const [num, slot] of salvageGrp.attemptMap) {
          if (num < 2) continue;
          totalAtt2Plus += slot.allOrders.length;
          recoveredAtt2Plus += slot.allOrders.filter(o => o.isApproved).length;
        }
        const historicalRecoveryRate = totalAtt2Plus > 0 ? (recoveredAtt2Plus / totalAtt2Plus * 100) : 0;

        const eligibilityStats = {
          totalDeclines: allSalvageDeclines.length,
          eligible: eligibleDeclines.length,
          ineligible: ineligibleDeclines.length,
          eligiblePct: Math.round(eligiblePct * 10) / 10,
        };

        // -------------------------------------------------------------------
        // Viability checks
        // -------------------------------------------------------------------
        const salvageNotViable = eligiblePct < 10;
        const notWorthRetrying = eligiblePct > 50 && historicalRecoveryRate < 2;

        if (salvageNotViable) {
          salvageSection = {
            totalAttempts: salvageGrp.totalAttempts,
            insufficient: false,
            attempts: [],
            notViable: true,
            notViableMessage: `Salvage not viable — ${(100 - eligiblePct).toFixed(0)}% of declines are hard issuer-side. Focus on attempt 1 routing only.`,
            eligibilityStats,
          };
        } else if (notWorthRetrying) {
          salvageSection = {
            totalAttempts: salvageGrp.totalAttempts,
            insufficient: false,
            attempts: [],
            notWorthRetrying: true,
            notWorthMessage: `Not worth retrying — ${Math.round(eligiblePct)}% eligible but only ${historicalRecoveryRate.toFixed(1)}% historical recovery. Gateway routing on attempt 1 is the only lever.`,
            eligibilityStats,
          };
        } else {
          // -------------------------------------------------------------------
          // Build attempt rows (normal path)
          // -------------------------------------------------------------------
          const attemptRows = [];

          for (let num = 1; num <= MAX_ATTEMPT_NUMBER; num++) {
            const slot = salvageGrp.attemptMap.get(num);

            if (!slot || slot.allOrders.length < MIN_ATTEMPT_SAMPLE) {
              if (num > 1) {
                attemptRows.push({
                  attemptNumber: num,
                  gateway: null,
                  processor: null,
                  bucket: null,
                  approvalRate: 0,
                  estRevenue: 0,
                  sameGwRate: null,
                  recommendation: null,
                  liftPp: 0,
                  isStop: true,
                  stopReason: `Sample < ${MIN_ATTEMPT_SAMPLE} attempts`,
                  mustSwitchGw: false,
                  allowReasons: null,
                  blockReasons: null,
                  allowMore: null,
                  blockMore: null,
                });
                break;
              }
              continue;
            }

            if (num === 1) {
              // Attempt 1: best gateway at the chosen bucket
              const bestGw = _findBestGatewayAtBucket(slot.allOrders, salvageBucket);
              const bestGwInfo = bestGw.gatewayAlias ? bestGw : _findBestGateway(slot.allOrders);
              const stats = _getBucketStats(slot.allOrders, salvageBucket);
              const totalApproved = slot.allOrders.filter(o => o.isApproved).length;
              const approvalRate = stats.attempts > 0 ? stats.rate : _rate(totalApproved, slot.allOrders.length);
              const estRevenue = (approvalRate / 100) * (BUCKET_MIDPOINTS[salvageBucket] || 88);

              attemptRows.push({
                attemptNumber: 1,
                gateway: bestGwInfo.gatewayAlias || bestGwInfo.processorName,
                processor: bestGwInfo.processorName,
                bucket: salvageBucket,
                approvalRate,
                estRevenue: Math.round(estRevenue * 100) / 100,
                sameGwRate: null,
                recommendation: null,
                liftPp: 0,
                isStop: false,
                stopReason: null,
                mustSwitchGw: false,
                allowReasons: null,
                blockReasons: null,
                allowMore: null,
                blockMore: null,
              });
            } else {
              // Attempt 2+: compare same_gw vs switch_gw using eligible orders
              const sameGwAttempts  = slot.eligibleSameGw.length;
              const sameGwApproved  = slot.eligibleSameGw.filter(o => o.isApproved).length;
              const sameGwRate      = _rate(sameGwApproved, sameGwAttempts);

              const switchGwAttempts  = slot.eligibleSwitchGw.length;
              const switchGwApproved  = slot.eligibleSwitchGw.filter(o => o.isApproved).length;
              const switchGwRate      = _rate(switchGwApproved, switchGwAttempts);

              const switchBestGw = _findBestGateway(slot.eligibleSwitchGw);

              const liftPp = Math.round((switchGwRate - sameGwRate) * 100) / 100;

              // Check if any eligible orders had a processor-side prior decline
              const mustSwitchGw = slot.eligibleOrders.some(o => o.priorDeclineCategory === 'processor');

              // ---------------------------------------------------------------
              // Decline reason allow/block — issuer-specific, rate-based
              // Uses actual recovery data for THIS BIN group only
              // ---------------------------------------------------------------
              const declineReasonStats = {};
              // Count all orders (eligible + ineligible) by prior decline reason
              for (const o of slot.allOrders) {
                const reason = o.priorDeclineReason;
                if (!reason) continue;
                if (!declineReasonStats[reason]) declineReasonStats[reason] = { count: 0, recovered: 0 };
                declineReasonStats[reason].count++;
                if (o.isApproved) declineReasonStats[reason].recovered++;
              }

              const allowReasons = [];
              const blockReasons = [];
              const MIN_REASON_ATTEMPTS = 15;
              const MIN_RECOVERY_RATE = 4; // 4% threshold

              for (const [reason, stats] of Object.entries(declineReasonStats)) {
                if (stats.count < MIN_REASON_ATTEMPTS) continue; // skip low-sample reasons
                const recoveryRate = stats.count > 0 ? (stats.recovered / stats.count) * 100 : 0;
                if (recoveryRate >= MIN_RECOVERY_RATE) {
                  allowReasons.push({ reason, count: stats.count, recovered: stats.recovered, rate: Math.round(recoveryRate * 10) / 10 });
                } else {
                  blockReasons.push({ reason, count: stats.count, recovered: stats.recovered, rate: Math.round(recoveryRate * 10) / 10 });
                }
              }
              allowReasons.sort((a, b) => b.count - a.count);
              blockReasons.sort((a, b) => b.count - a.count);

              let recommendation;
              if (mustSwitchGw) {
                recommendation = 'switch';
              } else if (switchGwRate > sameGwRate && liftPp >= 3) {
                recommendation = 'switch';
              } else {
                recommendation = 'stay';
              }

              // Use the overall best gateway info at the salvage bucket
              const bestGw = _findBestGatewayAtBucket(slot.allOrders, salvageBucket);
              const bestGwInfo = bestGw.gatewayAlias ? bestGw : _findBestGateway(slot.allOrders);
              const stats = _getBucketStats(slot.allOrders, salvageBucket);
              const totalApproved = slot.allOrders.filter(o => o.isApproved).length;
              const approvalRate = stats.attempts > 0 ? stats.rate : _rate(totalApproved, slot.allOrders.length);
              const estRevenue = (approvalRate / 100) * (BUCKET_MIDPOINTS[salvageBucket] || 88);

              // Determine gateway display — if switching, show the switch gateway
              const displayGw = recommendation === 'switch'
                ? (switchBestGw.gatewayAlias || switchBestGw.processorName || bestGwInfo.gatewayAlias)
                : (bestGwInfo.gatewayAlias || bestGwInfo.processorName);
              const displayProcessor = recommendation === 'switch'
                ? (switchBestGw.processorName || bestGwInfo.processorName)
                : bestGwInfo.processorName;

              // Stop conditions: estRevenue < $3 or approval rate < 3%
              const isStop = estRevenue < 3 || approvalRate < 3;
              const stopReason = isStop
                ? (estRevenue < 3 ? 'Estimated revenue < $3/attempt' : 'Approval rate < 3%')
                : null;

              // For stop condition sample check, use eligible orders
              if (slot.eligibleOrders.length < MIN_ATTEMPT_SAMPLE) {
                attemptRows.push({
                  attemptNumber: num,
                  gateway: null,
                  processor: null,
                  bucket: null,
                  approvalRate: 0,
                  estRevenue: 0,
                  sameGwRate: null,
                  recommendation: null,
                  liftPp: 0,
                  isStop: true,
                  stopReason: `Eligible sample < ${MIN_ATTEMPT_SAMPLE} attempts`,
                  mustSwitchGw: false,
                  allowReasons: allowReasons.slice(0, 4),
                  blockReasons: blockReasons.slice(0, 4),
                  allowMore: Math.max(0, allowReasons.length - 4),
                  blockMore: Math.max(0, blockReasons.length - 4),
                });
                break;
              }

              attemptRows.push({
                attemptNumber: num,
                gateway: displayGw,
                processor: displayProcessor,
                bucket: salvageBucket,
                approvalRate,
                estRevenue: Math.round(estRevenue * 100) / 100,
                sameGwRate,
                recommendation,
                liftPp,
                isStop,
                stopReason,
                mustSwitchGw,
                allowReasons: allowReasons.slice(0, 4),
                blockReasons: blockReasons.slice(0, 4),
                allowMore: Math.max(0, allowReasons.length - 4),
                blockMore: Math.max(0, blockReasons.length - 4),
              });

              if (isStop) break;
            }
          }

          salvageSection = {
            totalAttempts: salvageGrp.totalAttempts,
            insufficient: false,
            attempts: attemptRows,
            eligibilityStats,
          };
        }
      }
    }

    // -------------------------------------------------------------------
    // Step 6: Determine card verdict + border color
    // -------------------------------------------------------------------
    let verdict;
    let borderColor;

    if (gatewayLift > 5 && priceSection) {
      verdict = 'GATEWAY + PRICE';
    } else if (gatewayLift > 5 && !priceSection) {
      verdict = 'GATEWAY ONLY';
    } else if (priceSection && gatewayLift <= 5) {
      verdict = 'PRICE + GATEWAY';
    } else {
      verdict = 'REVIEW';
    }

    if (gatewayLift > 10) {
      borderColor = 'green';
    } else if (gatewayLift > 0 && !priceSection) {
      borderColor = 'blue';
    } else if (priceSection) {
      borderColor = 'amber';
    } else {
      borderColor = 'gray';
    }

    // -------------------------------------------------------------------
    // Step 8: Build copyProfileText
    // -------------------------------------------------------------------
    const binsArr = Array.from(binsSet);
    const groupLabel = buildGroupLabel(key, level);

    const lines = [];
    lines.push(`${groupLabel} — Complete Flow Optix Profile:`);
    lines.push(`BINs: ${binsArr.join(', ')}`);
    lines.push('');

    // Initial routing
    const initialProcessor = gatewaySection
      ? (gatewaySection.bestGateway.processor || gatewaySection.bestGateway.alias)
      : (salvageSection && salvageSection.attempts.length > 0
        ? (salvageSection.attempts[0].processor || salvageSection.attempts[0].gateway)
        : 'Default');
    lines.push(`Initial routing: ${initialProcessor}`);

    // Price
    if (priceSection) {
      lines.push(`Price: ${priceSection.recommendedBucket} (${priceSection.ltv.explanation})`);
    } else {
      lines.push('Price: Full price ($76-100)');
    }

    // Salvage sequence
    if (salvageSection && !salvageSection.insufficient && salvageSection.attempts.length > 0) {
      lines.push('');
      lines.push('Salvage sequence:');
      let lastAttemptNum = 1;
      for (const att of salvageSection.attempts) {
        if (att.attemptNumber === 1) continue;
        if (att.isStop && att.stopReason) {
          lastAttemptNum = att.attemptNumber;
          break;
        }
        const processor = att.processor || att.gateway || 'same';
        if (att.recommendation === 'switch') {
          lines.push(`Attempt ${att.attemptNumber}: Switch to ${processor}, ${att.bucket}`);
        } else {
          lines.push(`Attempt ${att.attemptNumber}: Stay with ${processor}, ${att.bucket}`);
        }
        lastAttemptNum = att.attemptNumber;
      }
      lines.push(`Stop after attempt ${lastAttemptNum}`);
    }

    const copyProfileText = lines.join('\n');

    // -------------------------------------------------------------------
    // Step 7: Build card object
    // -------------------------------------------------------------------
    // Confidence level
    const cardConf = totalAttempts >= 200 ? 'HIGH' : totalAttempts >= 100 ? 'MEDIUM' : 'LOW';

    cards.push({
      groupKey: key,
      groupLabel,
      level,
      issuer_bank: meta.issuer_bank,
      card_brand: meta.card_brand,
      card_type: meta.card_type,
      card_level: meta.card_level,
      is_prepaid: meta.is_prepaid,
      bins: binsArr,
      totalAttempts,
      confidence: cardConf,

      // appliesTo for level-engine compatibility
      appliesTo: {
        tx_group: 'REBILLS',
        issuer_bank: meta.issuer_bank,
        card_brand: meta.card_brand || null,
        card_type: meta.card_type || null,
        card_level: meta.card_level || null,
        is_prepaid: meta.is_prepaid || 0,
      },
      binsInGroup: binsArr,

      verdict,
      borderColor,

      gateway: gatewaySection,
      price: priceSection,
      salvage: salvageSection,

      copyProfileText,
    });
  }

  // =========================================================================
  // Step 9: Sort cards
  // =========================================================================
  const verdictOrder = {
    'GATEWAY + PRICE': 0,
    'GATEWAY ONLY': 1,
    'PRICE + GATEWAY': 2,
    'REVIEW': 3,
  };

  cards.sort((a, b) => {
    const vo = verdictOrder[a.verdict] - verdictOrder[b.verdict];
    if (vo !== 0) return vo;
    return b.totalAttempts - a.totalAttempts;
  });

  // =========================================================================
  // Step 10: Processor Affinity cards for rebills
  // =========================================================================
  const processorAffinityCards = [];

  const affinityRows = querySql(`
    SELECT o.cc_first_6 AS bin, o.gateway_id, g.processor_name,
      o.order_total, o.order_status,
      b.issuer_bank, b.card_brand, b.card_type, b.card_level, b.is_prepaid,
      g.gateway_alias, g.gateway_active, COALESCE(g.exclude_from_analysis, 0) as excluded
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.order_status IN (2,6,7,8) AND o.order_total > 0
      AND g.processor_name IS NOT NULL
      AND ${daysAgoFilter(days)}
  `, [clientId]);

  // Group by BIN group → processor_name
  const affinityGroupMap = new Map();

  for (const row of affinityRows) {
    const key = buildGroupKey(row, level);
    const isApproved = [2, 6, 8].includes(row.order_status);

    if (!affinityGroupMap.has(key)) {
      affinityGroupMap.set(key, {
        issuer_bank: row.issuer_bank || 'Unknown',
        card_brand:  level >= 2 ? (row.card_brand || 'Unknown') : null,
        card_type:   level >= 3 ? (row.card_type || 'Unknown') : null,
        card_level:  level === 4 ? (row.card_level || 'Unknown') : null,
        is_prepaid:  level === 3 ? (row.is_prepaid ?? 0) : 0,
        bins:        new Set(),
        totalAttempts: 0,
        processors:  new Map(),
      });
    }

    const grp = affinityGroupMap.get(key);
    grp.totalAttempts++;
    if (row.bin) grp.bins.add(row.bin);

    if (!grp.processors.has(row.processor_name)) {
      grp.processors.set(row.processor_name, {
        attempts: 0,
        approved: 0,
        hasActiveNonExcluded: false,
        activeGateways: new Map(),
      });
    }

    const proc = grp.processors.get(row.processor_name);
    proc.attempts++;
    if (isApproved) proc.approved++;

    // Track if this processor has any active, non-excluded gateway
    if (row.gateway_active === 1 && row.excluded === 0) {
      proc.hasActiveNonExcluded = true;
      if (!proc.activeGateways.has(row.gateway_id)) {
        proc.activeGateways.set(row.gateway_id, { gateway_id: row.gateway_id, alias: row.gateway_alias });
      }
    }
  }

  // Build affinity cards
  for (const [key, grp] of affinityGroupMap) {
    if (grp.totalAttempts < 30) continue;

    // Find bestProcessor (highest approval rate with min 20 attempts)
    let bestProcessor = null;
    let bestActiveProcessor = null;

    for (const [procName, proc] of grp.processors) {
      if (proc.attempts < 20) continue;
      const rate = _rate(proc.approved, proc.attempts);

      if (!bestProcessor || rate > bestProcessor.rate) {
        bestProcessor = {
          name: procName,
          rate,
          attempts: proc.attempts,
          approved: proc.approved,
          hasActiveNonExcluded: proc.hasActiveNonExcluded,
          activeGateways: Array.from(proc.activeGateways.values()),
        };
      }

      if (proc.hasActiveNonExcluded) {
        if (!bestActiveProcessor || rate > bestActiveProcessor.rate) {
          bestActiveProcessor = {
            name: procName,
            rate,
            attempts: proc.attempts,
            approved: proc.approved,
          };
        }
      }
    }

    if (!bestProcessor || !bestActiveProcessor) continue;

    // Only generate card if bestProcessor is NOT currently active
    if (bestProcessor.hasActiveNonExcluded) continue;

    // Must have meaningful lift
    const liftPp = Math.round((bestProcessor.rate - bestActiveProcessor.rate) * 100) / 100;
    if (liftPp <= 5) continue;

    const groupLabel = buildGroupLabel(key, level);

    processorAffinityCards.push({
      section: 'processor_affinity',
      groupKey: key,
      groupLabel,
      bins: Array.from(grp.bins),
      totalAttempts: grp.totalAttempts,
      bestProcessor: {
        name: bestProcessor.name,
        rate: bestProcessor.rate,
        attempts: bestProcessor.attempts,
        approved: bestProcessor.approved,
      },
      bestActiveProcessor: {
        name: bestActiveProcessor.name,
        rate: bestActiveProcessor.rate,
        attempts: bestActiveProcessor.attempts,
        approved: bestActiveProcessor.approved,
      },
      liftPp,
      activeGateways: bestProcessor.activeGateways,
      recommendation: `When adding new ${bestProcessor.name} MID \u2192 route these rebill BINs there immediately`,
    });
  }

  // Enrich affinity cards with salvage & price from matching main cards
  const mainCardMap = new Map(cards.map(c => [c.groupKey, c]));
  for (const pa of processorAffinityCards) {
    const mainCard = mainCardMap.get(pa.groupKey);
    if (mainCard) {
      pa.salvage = mainCard.salvage || null;
      pa.price = mainCard.price || null;
    }
  }

  // Sort by liftPp descending
  processorAffinityCards.sort((a, b) => b.liftPp - a.liftPp);

  // =========================================================================
  // Summary
  // =========================================================================
  // Compute actual avg rebill order value from approved rebills
  const avgOvRow = querySql(
    `SELECT AVG(order_total) AS avg_val FROM orders
     WHERE client_id = ? AND derived_product_role IN ('main_rebill','upsell_rebill')
       AND order_status IN (2,6,8) AND is_test = 0 AND is_internal_test = 0`,
    [clientId]
  );
  const avgOrderValue = Math.round((avgOvRow[0]?.avg_val || 70) * 100) / 100;

  const summary = {
    totalCards: cards.length,
    level,
    avgOrderValue,
    byVerdict: {
      'GATEWAY + PRICE': cards.filter(c => c.verdict === 'GATEWAY + PRICE').length,
      'GATEWAY ONLY':    cards.filter(c => c.verdict === 'GATEWAY ONLY').length,
      'PRICE + GATEWAY': cards.filter(c => c.verdict === 'PRICE + GATEWAY').length,
      'REVIEW':          cards.filter(c => c.verdict === 'REVIEW').length,
    },
    avgCyclesPerCustomer: expectedLifetime,
    insufficientFundsTiming: INSUF_TIMING,
    processorAffinity: {
      total: processorAffinityCards.length,
    },
  };

  // Attach split suggestions to rebill cards
  try {
    const { computeSplitSuggestion } = require('./split-engine');
    const REBILL_TX = ['tp_rebill', 'tp_rebill_salvage', 'sticky_cof_rebill'];
    for (const card of cards) {
      if (card.level >= 5) { card.splitSuggestion = null; continue; }
      card.ruleId = card.ruleId || card.groupKey;
      card.splitSuggestion = computeSplitSuggestion(clientId, card, { txTypes: REBILL_TX, days });
    }
  } catch (e) {
    console.error('[FlowOptix] Split suggestion error:', e.message);
  }

  // Enrich cards with weighted rates + processor selection + fallback chain
  try {
    const { computeWeightedRates, selectProcessor, buildFallbackChain, getConfidence, isHistoricalOnly } = require('./weighted-rates');
    const { checkCapAlerts } = require('./cap-tracking');
    const capAlerts = checkCapAlerts(clientId);
    const cappedGwIds = new Set(capAlerts.filter(a => a.severity === 'critical').map(a => a.gateway_id));

    for (const card of cards) {
      const rates = computeWeightedRates(clientId, card.bins || []);
      const selection = selectProcessor(rates.processors, rates.mids);
      const chain = buildFallbackChain(selection, rates.mids);

      card.routingEngine = {
        selection: {
          type: selection.type,
          primary: selection.primary ? {
            name: selection.primary.name,
            rate: selection.primary.weighted_rate,
            approved: selection.primary.approved_count,
            confidence: getConfidence(selection.primary.approved_count),
            historical: selection.primary ? isHistoricalOnly(selection.primary) : false,
          } : null,
          secondary: selection.secondary ? {
            name: selection.secondary.name,
            rate: selection.secondary.weighted_rate,
            approved: selection.secondary.approved_count,
            confidence: getConfidence(selection.secondary.approved_count),
          } : null,
          variance: selection.variance,
          bestMid: selection.bestMid ? {
            gateway_id: selection.bestMid.gateway_id,
            name: selection.bestMid.name,
            rate: selection.bestMid.weighted_rate,
            standout: selection.bestMid.standout,
          } : null,
          gatheringProcessors: selection.gatheringProcessors || [],
          missingProcessors: selection.missingProcessors || [],
        },
        chain: {
          primary: chain.primary_gateway_id,
          secondary: chain.secondary_gateway_id,
          tertiary: chain.tertiary_gateway_id,
        },
        capAlerts: capAlerts.filter(a =>
          a.gateway_id === chain.primary_gateway_id ||
          a.gateway_id === chain.secondary_gateway_id
        ),
      };
    }
  } catch (e) {
    console.error('[FlowOptix] Routing engine error:', e.message);
  }

  // Compute and persist processor intelligence during recompute
  try {
    const { computeProcessorIntelligence } = require('./processor-intelligence');
    computeProcessorIntelligence(clientId);
  } catch (e) {
    console.error('[FlowOptix] Processor intelligence error:', e.message);
  }

  return { cards, processorAffinityCards, summary };
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = { computeFlowOptix, declineEligibility, buildRecoveryLookup, insufficientFundsTiming };
