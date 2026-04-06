/**
 * Price Point Analysis — Rebill order approval rates by price bucket.
 * Salvage Sequence Analysis — Multi-attempt rebill chain analysis.
 *
 * All queries are READ ONLY. No data modifications.
 */
const { querySql } = require('../db/connection');
const {
  CLEAN_FILTER, getCachedOrCompute, daysAgoFilter,
} = require('./engine');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const REBILL_TYPES = ['tp_rebill'];

const BUCKETS = [
  { label: '$0-25',   min: 0,   max: 25 },
  { label: '$26-50',  min: 26,  max: 50 },
  { label: '$51-75',  min: 51,  max: 75 },
  { label: '$76-100', min: 76,  max: 100 },
  { label: '$100+',   min: 101, max: 999999 },
];

const MIN_ATTEMPTS = 30;

const BUCKET_MIDPOINTS = {
  '$0-25': 12.50,
  '$26-50': 38,
  '$51-75': 63,
  '$76-100': 88,
  '$100+': 110,
};
const MIN_SUGGEST_BUCKET_INDEX = 2; // Index of '$51-75' — never suggest below this

// LTV Decision Engine constants
const CPA = 42.50;               // Average cost per acquisition
const MIN_CYCLES_TO_RECOVER = 3; // Minimum rebill cycles to recover CPA
const EXPECTED_LIFETIME_CYCLES = 6; // Expected subscription lifetime (planning assumption)
const FULL_PRICE_BUCKET = '$76-100';
const FULL_PRICE_MIDPOINT = 88;
const BREAKEVEN_IMPROVEMENT = 0.70;  // Lower price approval must be 70%+ higher than full price

// ---------------------------------------------------------------------------
// LTV Decision Engine (compounding retention model)
// ---------------------------------------------------------------------------

function compoundingLtv(approvalRate, priceMidpoint, cycles) {
  let retained = 100;
  let totalRevenue = 0;
  for (let i = 0; i < cycles; i++) {
    retained = retained * (approvalRate / 100);
    totalRevenue += retained * priceMidpoint;
  }
  return Math.round((totalRevenue / 100) * 100) / 100;
}

function ltvDecision(fullPriceRate, reducedPriceRate, reducedBucket, expectedLifetime, currentCycle) {
  const remainingCycles = Math.max(0, Math.round((expectedLifetime || EXPECTED_LIFETIME_CYCLES) - currentCycle));
  const reducedMidpoint = BUCKET_MIDPOINTS[reducedBucket] || 63;

  const ltvFull = compoundingLtv(fullPriceRate, FULL_PRICE_MIDPOINT, remainingCycles);
  const ltvReduced = compoundingLtv(reducedPriceRate, reducedMidpoint, remainingCycles);

  const cond1 = ltvReduced > ltvFull;
  const improvementRatio = fullPriceRate > 0 ? (reducedPriceRate - fullPriceRate) / fullPriceRate : 0;
  const cond2 = improvementRatio >= BREAKEVEN_IMPROVEMENT;
  const cond3 = remainingCycles >= 3;

  const shouldReduce = cond1 && cond2 && cond3;
  const ltvGain = Math.round((ltvReduced - ltvFull) * 100) / 100;

  const explanation = shouldReduce
    ? `LTV per customer: $${ltvReduced.toFixed(2)} (reduced) vs $${ltvFull.toFixed(2)} (full price) | Gain: $${ltvGain.toFixed(2)}`
    : `LTV does not justify — ${!cond1 ? '$' + ltvReduced.toFixed(2) + ' reduced < $' + ltvFull.toFixed(2) + ' full' : !cond2 ? 'approval lift <70% (' + Math.round(improvementRatio * 100) + '%)' : 'remaining cycles <3 (' + remainingCycles + ')'}`;

  return { shouldReduce, ltvFull, ltvReduced, ltvGain, explanation, remainingCycles, conditions: { cond1, cond2, cond3 } };
}

// ---------------------------------------------------------------------------
// computePricePoints
// ---------------------------------------------------------------------------

/**
 * Analyze rebill approval rates across price-point buckets, grouped by
 * issuer / card attributes at a configurable detail level.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {number} [opts.level] - Grouping level 1-4 (default 2)
 * @param {number} [opts.days]  - Lookback window in days (default 90)
 * @returns {{ groups: Array<object>, summary: object }}
 */
function computePricePoints(clientId, opts = {}) {
  const level = opts.level ?? 2;
  const days  = opts.days ?? 180;

  const cacheKey = `${level}:${days}`;

  return getCachedOrCompute(clientId, 'price-points', cacheKey, () => {
    return _computePricePoints(clientId, level, days);
  });
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/**
 * Determine which price bucket an order_total falls into.
 */
function getBucket(orderTotal) {
  for (const b of BUCKETS) {
    if (orderTotal >= b.min && orderTotal <= b.max) return b.label;
  }
  return BUCKETS[BUCKETS.length - 1].label;
}

/**
 * Build a group key from a row based on the requested level.
 */
function buildGroupKey(row, level) {
  const bank = row.issuer_bank || 'Unknown';
  switch (level) {
    case 1: return bank;
    case 2: return `${bank}|${row.card_brand || 'Unknown'}`;
    case 3: return `${bank}|${row.card_brand || 'Unknown'}|${row.card_type || 'Unknown'}|${row.is_prepaid ?? 0}`;
    case 4: return `${bank}|${row.card_brand || 'Unknown'}|${row.card_type || 'Unknown'}|${row.card_level || 'Unknown'}`;
    default: return `${bank}|${row.card_brand || 'Unknown'}`;
  }
}

/**
 * Build a human-readable label from a group key.
 */
function buildGroupLabel(key, level) {
  const parts = key.split('|');
  switch (level) {
    case 1: return parts[0];
    case 2: return `${parts[0]} \u00b7 ${parts[1]}`;
    case 3: return `${parts[0]} \u00b7 ${parts[1]} \u00b7 ${parts[2]}${parts[3] === '1' ? ' (Prepaid)' : ''}`;
    case 4: return `${parts[0]} \u00b7 ${parts[1]} \u00b7 ${parts[2]} \u00b7 ${parts[3]}`;
    default: return key.replace(/\|/g, ' \u00b7 ');
  }
}

// ---------------------------------------------------------------------------
// Price Points — Internal implementation
// ---------------------------------------------------------------------------

function _computePricePoints(clientId, level, days) {
  // -----------------------------------------------------------------------
  // 1. Fetch all qualifying rebill orders
  // -----------------------------------------------------------------------
  const txPlaceholders = REBILL_TYPES.map(() => '?').join(',');
  const rows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.gateway_id,
      o.order_total,
      o.order_status,
      b.issuer_bank,
      b.card_brand,
      b.card_type,
      b.card_level,
      b.is_prepaid
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = ?
      AND o.tx_type IN (${txPlaceholders})
      AND ${CLEAN_FILTER}
      AND o.order_status IN (2,6,7,8)
      AND o.order_total > 0
      AND g.exclude_from_analysis != 1
      AND ${daysAgoFilter(days)}
  `, [clientId, ...REBILL_TYPES]);

  if (rows.length === 0) {
    return { groups: [], summary: { totalGroups: 0, totalAttempts: 0, optimizeCount: 0, gatewayOptimizeCount: 0, level } };
  }

  // -----------------------------------------------------------------------
  // 2. Fetch gateway metadata for display names
  // -----------------------------------------------------------------------
  const gwMeta = querySql(`
    SELECT gateway_id,
           COALESCE(gateway_alias, 'Gateway #' || gateway_id) AS gateway_name,
           processor_name
    FROM gateways
    WHERE client_id = ?
  `, [clientId]);

  const gwNameMap = new Map();
  for (const gw of gwMeta) {
    gwNameMap.set(gw.gateway_id, { gateway_name: gw.gateway_name, processor_name: gw.processor_name || null });
  }

  // -----------------------------------------------------------------------
  // 3. Group rows and compute per-bucket / per-gateway stats
  // -----------------------------------------------------------------------
  const groupMap = new Map(); // groupKey -> group data

  for (const row of rows) {
    const key    = buildGroupKey(row, level);
    const bucket = getBucket(row.order_total);
    const isApproved = [2, 6, 8].includes(row.order_status);

    if (!groupMap.has(key)) {
      groupMap.set(key, {
        groupKey:    key,
        issuer_bank: row.issuer_bank || 'Unknown',
        card_brand:  level >= 2 ? (row.card_brand || 'Unknown') : null,
        card_type:   level >= 3 ? (row.card_type || 'Unknown') : null,
        card_level:  level === 4 ? (row.card_level || 'Unknown') : null,
        is_prepaid:  level === 3 ? (row.is_prepaid ?? 0) : 0,
        bins:        new Set(),
        totalAttempts: 0,
        buckets: {},
        // Per-gateway per-bucket tracking
        _gwBuckets: {},
      });
      // Initialise bucket counters
      for (const b of BUCKETS) {
        groupMap.get(key).buckets[b.label] = { attempts: 0, approved: 0, rate: 0 };
        groupMap.get(key)._gwBuckets[b.label] = new Map();
      }
    }

    const grp = groupMap.get(key);
    grp.totalAttempts++;
    if (row.bin) grp.bins.add(row.bin);

    // Bucket stats
    const bkt = grp.buckets[bucket];
    bkt.attempts++;
    if (isApproved) bkt.approved++;

    // Per-gateway bucket stats
    const gwBktMap = grp._gwBuckets[bucket];
    if (!gwBktMap.has(row.gateway_id)) {
      gwBktMap.set(row.gateway_id, { attempts: 0, approved: 0 });
    }
    const gwBkt = gwBktMap.get(row.gateway_id);
    gwBkt.attempts++;
    if (isApproved) gwBkt.approved++;
  }

  // -----------------------------------------------------------------------
  // 4. Compute rates, determine best bucket, gateway performance
  // -----------------------------------------------------------------------
  const groups = [];

  for (const [key, grp] of groupMap) {
    if (grp.totalAttempts < MIN_ATTEMPTS) continue;

    // Compute rates per bucket
    for (const b of BUCKETS) {
      const bkt = grp.buckets[b.label];
      bkt.rate = bkt.attempts > 0
        ? Math.round((bkt.approved / bkt.attempts) * 10000) / 100
        : 0;
    }

    // Best bucket (minimum 10 attempts in bucket)
    let bestBucket  = null;
    let bestRate    = -1;
    for (const b of BUCKETS) {
      const bkt = grp.buckets[b.label];
      if (bkt.attempts >= 10 && bkt.rate > bestRate) {
        bestRate   = bkt.rate;
        bestBucket = b.label;
      }
    }

    const currentBucket = '$76-100';
    const currentRate   = grp.buckets[currentBucket].rate;
    const liftPp        = bestRate >= 0 ? Math.round((bestRate - currentRate) * 100) / 100 : 0;
    // Run LTV decision engine to validate price optimization
    let groupLtv = null;
    let hasOptimizeFlag = false;
    if (bestBucket !== null && bestBucket !== currentBucket && liftPp > 5) {
      groupLtv = ltvDecision(currentRate, bestRate, bestBucket, EXPECTED_LIFETIME_CYCLES, 1);
      hasOptimizeFlag = groupLtv.shouldReduce;
    }
    const ltvGainCategory = groupLtv
      ? (groupLtv.ltvGain > 50 ? 'high' : groupLtv.ltvGain >= 20 ? 'medium' : 'low')
      : null;

    // ---- Per-gateway performance at current price point ($76-100) ----
    const gwBktCurrent = grp._gwBuckets[currentBucket];
    const gatewayPerformance = [];
    for (const [gwId, stats] of gwBktCurrent) {
      const meta = gwNameMap.get(gwId) || { gateway_name: `Gateway #${gwId}`, processor_name: null };
      gatewayPerformance.push({
        gateway_id:     gwId,
        gateway_name:   meta.gateway_name,
        processor_name: meta.processor_name,
        attempts:       stats.attempts,
        approved:       stats.approved,
        rate:           stats.attempts > 0
          ? Math.round((stats.approved / stats.attempts) * 10000) / 100
          : 0,
      });
    }
    gatewayPerformance.sort((a, b) => b.rate - a.rate);

    // Best and current gateway at current price point
    const bestGateway    = gatewayPerformance.length > 0 ? gatewayPerformance[0] : null;
    const currentGateway = gatewayPerformance.length > 1
      ? gatewayPerformance[gatewayPerformance.length - 1]
      : bestGateway;
    const gatewayLiftPp  = (bestGateway && currentGateway)
      ? Math.round((bestGateway.rate - currentGateway.rate) * 100) / 100
      : 0;

    groups.push({
      groupKey:     key,
      groupLabel:   buildGroupLabel(key, level),
      issuer_bank:  grp.issuer_bank,
      card_brand:   grp.card_brand,
      card_type:    grp.card_type,
      card_level:   grp.card_level,
      is_prepaid:   grp.is_prepaid,
      bins:         Array.from(grp.bins),
      totalAttempts: grp.totalAttempts,
      buckets:      grp.buckets,
      bestBucket,
      currentBucket,
      currentRate,
      bestRate:     bestRate >= 0 ? bestRate : 0,
      liftPp,
      hasOptimizeFlag,
      ltv: groupLtv,
      ltvGainCategory,
      gatewayPerformance,
      bestGateway:    bestGateway ? { gateway_id: bestGateway.gateway_id, gateway_name: bestGateway.gateway_name, processor_name: bestGateway.processor_name, rate: bestGateway.rate } : null,
      currentGateway: currentGateway ? { gateway_id: currentGateway.gateway_id, gateway_name: currentGateway.gateway_name, processor_name: currentGateway.processor_name, rate: currentGateway.rate } : null,
      gatewayLiftPp,
    });
  }

  // Sort by totalAttempts descending
  groups.sort((a, b) => b.totalAttempts - a.totalAttempts);

  // -----------------------------------------------------------------------
  // 5. Build summary
  // -----------------------------------------------------------------------
  const summary = {
    totalGroups:          groups.length,
    totalAttempts:        groups.reduce((s, g) => s + g.totalAttempts, 0),
    optimizeCount:        groups.filter(g => g.hasOptimizeFlag).length,
    gatewayOptimizeCount: groups.filter(g => g.gatewayLiftPp > 5).length,
    level,
  };

  return { groups, summary };
}

// ---------------------------------------------------------------------------
// computeSalvageSequence
// ---------------------------------------------------------------------------

/**
 * Salvage sequence analysis — analyze multi-attempt rebill chains to find
 * optimal gateway switching strategies per BIN group and attempt number.
 *
 * @param {number} clientId
 * @param {object} [opts]
 * @param {number} [opts.level] - Grouping level 1-4 (default 2)
 * @param {number} [opts.days]  - Lookback window in days (default 90)
 * @returns {{ groups: Array<object>, summary: object }}
 */
function computeSalvageSequence(clientId, opts = {}) {
  const level = opts.level ?? 2;
  const days  = opts.days ?? 180;

  const cacheKey = `salvage:${level}:${days}`;

  return getCachedOrCompute(clientId, 'salvage-sequence', cacheKey, () => {
    return _computeSalvageSequence(clientId, level, days);
  });
}

// ---------------------------------------------------------------------------
// Salvage Sequence — Internal implementation
// ---------------------------------------------------------------------------

const SALVAGE_MIN_GROUP_ATTEMPTS = 30;
const SALVAGE_MIN_ATTEMPT_ROWS   = 15;
const MAX_ATTEMPT_NUMBER         = 10;

function _computeSalvageSequence(clientId, level, days) {
  // -----------------------------------------------------------------------
  // 1. Fetch all rebill + salvage orders with attempt context
  // -----------------------------------------------------------------------
  const rows = querySql(`
    SELECT
      o.cc_first_6 AS bin,
      o.customer_id,
      o.product_group_id,
      o.derived_cycle,
      o.attempt_number,
      o.gateway_id,
      o.order_total,
      o.order_status,
      o.tx_type,
      b.issuer_bank, b.card_brand, b.card_type, b.card_level, b.is_prepaid,
      g.gateway_alias, g.processor_name
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role IN ('main_rebill', 'upsell_rebill')
      AND o.order_status IN (2,6,7,8)
      AND o.order_total > 0
      AND g.exclude_from_analysis != 1
      AND ${daysAgoFilter(days)}
    ORDER BY o.customer_id, o.product_group_id, o.derived_cycle, o.attempt_number
  `, [clientId]);

  if (rows.length === 0) {
    return {
      groups: [],
      summary: { totalGroups: 0, totalAttempts: 0, level, greenCards: 0, amberCards: 0, grayCards: 0 },
    };
  }

  // -----------------------------------------------------------------------
  // 1b. Query avg total cycles per customer
  // -----------------------------------------------------------------------
  const avgCyclesRows = querySql(`
    SELECT ROUND(AVG(max_cycle), 1) as avg_cycles FROM (
      SELECT customer_id, MAX(derived_cycle) as max_cycle
      FROM orders WHERE client_id = ? AND is_test = 0 AND is_internal_test = 0
        AND derived_product_role IN ('main_rebill', 'upsell_rebill')
        AND customer_id IS NOT NULL
      GROUP BY customer_id
    )
  `, [clientId]);
  const EXPECTED_LIFETIME_CYCLES = (avgCyclesRows.length > 0 && avgCyclesRows[0].avg_cycles != null)
    ? avgCyclesRows[0].avg_cycles
    : 4;

  // -----------------------------------------------------------------------
  // 2. Build chains: group by customer_id + product_group_id + derived_cycle
  // -----------------------------------------------------------------------
  const chainMap = new Map(); // chainKey -> { rows: [], originGatewayId }

  for (const row of rows) {
    const chainKey = `${row.customer_id}|${row.product_group_id}|${row.derived_cycle}`;

    if (!chainMap.has(chainKey)) {
      chainMap.set(chainKey, { rows: [], originGatewayId: null });
    }
    const chain = chainMap.get(chainKey);
    chain.rows.push(row);

    // Attempt 1 establishes the original gateway
    if (row.attempt_number === 1) {
      chain.originGatewayId = row.gateway_id;
    }
  }

  // -----------------------------------------------------------------------
  // 3. Flatten chain rows into group-keyed, attempt-keyed buckets
  // -----------------------------------------------------------------------
  // groupKey -> { meta, bins, attemptMap: { attemptNumber -> { allOrders, sameGwOrders, switchGwOrders } } }
  const groupMap = new Map();

  for (const [, chain] of chainMap) {
    for (const row of chain.rows) {
      const attemptNum = row.attempt_number;
      if (attemptNum < 1 || attemptNum > MAX_ATTEMPT_NUMBER) continue;

      const key = buildGroupKey(row, level);
      const isApproved = [2, 6, 8].includes(row.order_status);
      const bucket = getBucket(row.order_total);
      const isSameGw = (chain.originGatewayId !== null) && (row.gateway_id === chain.originGatewayId);

      if (!groupMap.has(key)) {
        groupMap.set(key, {
          groupKey:    key,
          issuer_bank: row.issuer_bank || 'Unknown',
          card_brand:  level >= 2 ? (row.card_brand || 'Unknown') : null,
          card_type:   level >= 3 ? (row.card_type || 'Unknown') : null,
          card_level:  level === 4 ? (row.card_level || 'Unknown') : null,
          is_prepaid:  level === 3 ? (row.is_prepaid ?? 0) : 0,
          bins:        new Set(),
          totalAttempts: 0,
          attemptMap:  new Map(),
        });
      }

      const grp = groupMap.get(key);
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
      };

      slot.allOrders.push(orderEntry);

      if (attemptNum >= 2) {
        if (isSameGw) {
          slot.sameGwOrders.push(orderEntry);
        } else {
          slot.switchGwOrders.push(orderEntry);
        }
      }
    }
  }

  // -----------------------------------------------------------------------
  // 4. For each group, build attempt rows
  // -----------------------------------------------------------------------
  const groups = [];

  for (const [key, grp] of groupMap) {
    if (grp.totalAttempts < SALVAGE_MIN_GROUP_ATTEMPTS) continue;

    const attemptRows = [];

    for (let num = 1; num <= MAX_ATTEMPT_NUMBER; num++) {
      // Cap at 3 salvage attempts — attempt 4+ is a hard stop
      if (num >= 4) {
        attemptRows.push({
          attemptNumber: num,
          bestBucket: null,
          bestGateway: null,
          bestProcessor: null,
          approvalRate: 0,
          estRevenue: 0,
          totalAttempts: 0,
          sameGw: null,
          switchGw: null,
          recommendation: 'stop',
          liftPp: 0,
          isStop: true,
          stopMessage: 'Maximum 3 salvage attempts at $51-75 floor',
          ltv: null,
          secondarySuggestion: null,
        });
        break;
      }

      const slot = grp.attemptMap.get(num);
      if (!slot || slot.allOrders.length < SALVAGE_MIN_ATTEMPT_ROWS) {
        // If we've started and now have insufficient data, stop
        if (num > 1) {
          attemptRows.push({
            attemptNumber: num,
            bestBucket: null,
            bestGateway: null,
            bestProcessor: null,
            approvalRate: 0,
            estRevenue: 0,
            totalAttempts: slot ? slot.allOrders.length : 0,
            sameGw: null,
            switchGw: null,
            recommendation: 'stop',
            liftPp: 0,
            isStop: true,
            stopMessage: 'Sample < 15 attempts',
            ltv: null,
            secondarySuggestion: null,
          });
          break;
        }
        continue;
      }

      const totalAttempts = slot.allOrders.length;
      const totalApproved = slot.allOrders.filter(o => o.isApproved).length;

      // LTV remaining cycles check — stop if < 3
      // Note: num is attempt number within a cycle, NOT the billing cycle number
      // Use currentCycle=1 since salvage happens within one billing cycle
      const ltvRemainingCheck = ltvDecision(0, 0, '$51-75', EXPECTED_LIFETIME_CYCLES, 1);
      if (ltvRemainingCheck.remainingCycles < MIN_CYCLES_TO_RECOVER && num > 1 && ltvRemainingCheck.remainingCycles <= 0) {
        attemptRows.push({
          attemptNumber: num,
          bestBucket: null,
          bestGateway: null,
          bestProcessor: null,
          approvalRate: 0,
          estRevenue: 0,
          totalAttempts,
          sameGw: null,
          switchGw: null,
          recommendation: 'stop',
          liftPp: 0,
          isStop: true,
          stopMessage: `LTV remaining cycles (${ltvRemainingCheck.remainingCycles.toFixed(1)}) < 3`,
          ltv: null,
          secondarySuggestion: null,
        });
        break;
      }

      if (num === 1) {
        // Attempt 1 — always recommend $76-100 (full price)
        const bestBucket = '$76-100';
        const bestGwAt76 = _findBestGatewayAtBucket(slot.allOrders, '$76-100');
        // Fall back to overall best gateway if no data at $76-100
        const bestGwInfo = bestGwAt76.gatewayAlias ? bestGwAt76 : _findBestGateway(slot.allOrders);
        const stats76 = _getBucketStats(slot.allOrders, '$76-100');
        const approvalRate = stats76.attempts > 0 ? stats76.rate : _rate(totalApproved, totalAttempts);
        const estRevenue = approvalRate / 100 * BUCKET_MIDPOINTS[bestBucket];

        // Run LTV decision for $51-75 secondary suggestion
        let secondarySuggestion = null;
        let att1Ltv = null;
        const stats51 = _getBucketStats(slot.allOrders, '$51-75');
        if (stats51.attempts >= 10 && stats76.attempts >= 10) {
          const ltv51 = ltvDecision(stats76.rate, stats51.rate, '$51-75', EXPECTED_LIFETIME_CYCLES, 1);
          att1Ltv = ltv51;
          if (ltv51.shouldReduce) {
            const revenue51 = stats51.rate / 100 * BUCKET_MIDPOINTS['$51-75'];
            secondarySuggestion = {
              bucket: '$51-75',
              rate: stats51.rate,
              estRevenue: Math.round(revenue51 * 100) / 100,
              ltv: ltv51,
              note: ltv51.explanation,
            };
          } else {
            // Try $26-50 and $0-25 only if LTV gain > $100 (rare cases)
            for (const fallbackBucket of ['$26-50', '$0-25']) {
              const statsFb = _getBucketStats(slot.allOrders, fallbackBucket);
              if (statsFb.attempts >= 10) {
                const ltvFb = ltvDecision(stats76.rate, statsFb.rate, fallbackBucket, EXPECTED_LIFETIME_CYCLES, 1);
                if (ltvFb.shouldReduce && ltvFb.ltvGain > 100) {
                  const revenueFb = statsFb.rate / 100 * BUCKET_MIDPOINTS[fallbackBucket];
                  secondarySuggestion = {
                    bucket: fallbackBucket,
                    rate: statsFb.rate,
                    estRevenue: Math.round(revenueFb * 100) / 100,
                    ltv: ltvFb,
                    note: ltvFb.explanation,
                  };
                  att1Ltv = ltvFb;
                  break;
                }
              }
            }
          }
        }

        attemptRows.push({
          attemptNumber: 1,
          bestBucket,
          bestGateway: bestGwInfo.gatewayAlias,
          bestProcessor: bestGwInfo.processorName,
          approvalRate,
          estRevenue: Math.round(estRevenue * 100) / 100,
          totalAttempts,
          sameGw: null,
          switchGw: null,
          recommendation: null,
          liftPp: 0,
          isStop: false,
          ltv: att1Ltv,
          secondarySuggestion,
        });
      } else if (num === 2) {
        // Attempt 2 — primary: best gateway at $76-100
        const sameGwAttempts  = slot.sameGwOrders.length;
        const sameGwApproved  = slot.sameGwOrders.filter(o => o.isApproved).length;
        const sameGwRate      = _rate(sameGwApproved, sameGwAttempts);

        const switchGwAttempts  = slot.switchGwOrders.length;
        const switchGwApproved  = slot.switchGwOrders.filter(o => o.isApproved).length;
        const switchGwRate      = _rate(switchGwApproved, switchGwAttempts);

        const switchBestGw = _findBestGateway(slot.switchGwOrders);

        const liftPp = Math.round((switchGwRate - sameGwRate) * 100) / 100;

        // Determine recommendation (same/switch logic preserved)
        let recommendation;
        if (sameGwAttempts < SALVAGE_MIN_ATTEMPT_ROWS && switchGwAttempts < SALVAGE_MIN_ATTEMPT_ROWS) {
          recommendation = 'insufficient';
        } else if (switchGwRate > sameGwRate && liftPp >= 3) {
          recommendation = 'switch';
        } else if (sameGwRate >= switchGwRate && sameGwRate < 1 && switchGwRate < 1) {
          recommendation = 'stay_hurts';
        } else {
          recommendation = 'stay';
        }

        // Primary: best gateway at $76-100
        const stats76 = _getBucketStats(slot.allOrders, '$76-100');
        const stats51 = _getBucketStats(slot.allOrders, '$51-75');
        let bestBucket = '$76-100';
        let bucketRate = stats76.attempts > 0 ? stats76.rate : _rate(totalApproved, totalAttempts);
        let att2Ltv = null;
        let secondarySuggestion = null;

        // If approval at $76-100 < 5%: run ltvDecision for $51-75
        if (stats76.rate < 5) {
          const ltv51 = ltvDecision(stats76.rate, stats51.attempts > 0 ? stats51.rate : 0, '$51-75', EXPECTED_LIFETIME_CYCLES, 1);
          att2Ltv = ltv51;
          if (ltv51.shouldReduce) {
            bestBucket = '$51-75';
            bucketRate = stats51.attempts > 0 ? stats51.rate : _rate(totalApproved, totalAttempts);
            secondarySuggestion = {
              bucket: '$51-75',
              rate: bucketRate,
              estRevenue: Math.round((bucketRate / 100 * BUCKET_MIDPOINTS['$51-75']) * 100) / 100,
              ltv: ltv51,
              note: ltv51.explanation,
            };
          } else {
            secondarySuggestion = {
              bucket: '$76-100',
              rate: stats76.rate,
              estRevenue: Math.round((stats76.rate / 100 * BUCKET_MIDPOINTS['$76-100']) * 100) / 100,
              ltv: ltv51,
              note: 'Stay full price — LTV does not justify reduction',
            };
          }
        }

        const bestGwAtBucket = _findBestGatewayAtBucket(slot.allOrders, bestBucket);
        const bestGwInfo = bestGwAtBucket.gatewayAlias ? bestGwAtBucket : _findBestGateway(slot.allOrders);
        const approvalRate = bucketRate;
        const estRevenue = approvalRate / 100 * BUCKET_MIDPOINTS[bestBucket];

        const isStop = (estRevenue < 3 && (stats51.attempts > 0 ? (stats51.rate / 100 * BUCKET_MIDPOINTS['$51-75']) : 0) < 3)
          || totalAttempts < SALVAGE_MIN_ATTEMPT_ROWS;

        attemptRows.push({
          attemptNumber: num,
          bestBucket,
          bestGateway: bestGwInfo.gatewayAlias,
          bestProcessor: bestGwInfo.processorName,
          approvalRate,
          estRevenue: Math.round(estRevenue * 100) / 100,
          totalAttempts,
          sameGw: {
            attempts: sameGwAttempts,
            approved: sameGwApproved,
            rate: sameGwRate,
          },
          switchGw: {
            attempts: switchGwAttempts,
            approved: switchGwApproved,
            rate: switchGwRate,
            bestGateway: switchBestGw.gatewayAlias,
            bestProcessor: switchBestGw.processorName,
          },
          recommendation,
          liftPp,
          isStop,
          ltv: att2Ltv,
          secondarySuggestion,
        });

        if (isStop) break;
      } else if (num === 3) {
        // Attempt 3 — same logic as attempt 2 but with $51-75 as the candidate
        const sameGwAttempts  = slot.sameGwOrders.length;
        const sameGwApproved  = slot.sameGwOrders.filter(o => o.isApproved).length;
        const sameGwRate      = _rate(sameGwApproved, sameGwAttempts);

        const switchGwAttempts  = slot.switchGwOrders.length;
        const switchGwApproved  = slot.switchGwOrders.filter(o => o.isApproved).length;
        const switchGwRate      = _rate(switchGwApproved, switchGwAttempts);

        const switchBestGw = _findBestGateway(slot.switchGwOrders);

        const liftPp = Math.round((switchGwRate - sameGwRate) * 100) / 100;

        // Determine recommendation (same/switch logic preserved)
        let recommendation;
        if (sameGwAttempts < SALVAGE_MIN_ATTEMPT_ROWS && switchGwAttempts < SALVAGE_MIN_ATTEMPT_ROWS) {
          recommendation = 'insufficient';
        } else if (switchGwRate > sameGwRate && liftPp >= 3) {
          recommendation = 'switch';
        } else if (sameGwRate >= switchGwRate && sameGwRate < 1 && switchGwRate < 1) {
          recommendation = 'stay_hurts';
        } else {
          recommendation = 'stay';
        }

        // Run LTV engine for $51-75 — only suggest if it passes
        const stats76 = _getBucketStats(slot.allOrders, '$76-100');
        const stats51 = _getBucketStats(slot.allOrders, '$51-75');
        const fullRate = stats76.attempts > 0 ? stats76.rate : _rate(totalApproved, totalAttempts);
        const reducedRate = stats51.attempts > 0 ? stats51.rate : _rate(totalApproved, totalAttempts);
        const ltv51 = ltvDecision(fullRate, reducedRate, '$51-75', EXPECTED_LIFETIME_CYCLES, 1);

        let bestBucket;
        let bucketRate;
        let secondarySuggestion = null;
        if (ltv51.shouldReduce) {
          bestBucket = '$51-75';
          bucketRate = reducedRate;
          secondarySuggestion = {
            bucket: '$51-75',
            rate: reducedRate,
            estRevenue: Math.round((reducedRate / 100 * BUCKET_MIDPOINTS['$51-75']) * 100) / 100,
            ltv: ltv51,
            note: ltv51.explanation,
          };
        } else {
          bestBucket = '$76-100';
          bucketRate = fullRate;
          secondarySuggestion = {
            bucket: '$76-100',
            rate: fullRate,
            estRevenue: Math.round((fullRate / 100 * BUCKET_MIDPOINTS['$76-100']) * 100) / 100,
            ltv: ltv51,
            note: 'Stay full price — LTV does not justify reduction',
          };
        }

        const bestGwAtBucket = _findBestGatewayAtBucket(slot.allOrders, bestBucket);
        const bestGwInfo = bestGwAtBucket.gatewayAlias ? bestGwAtBucket : _findBestGateway(slot.allOrders);
        const approvalRate = bucketRate;
        const estRevenue = approvalRate / 100 * BUCKET_MIDPOINTS[bestBucket];

        const altEstRevenue = ltv51.shouldReduce
          ? (fullRate / 100 * BUCKET_MIDPOINTS['$76-100'])
          : (reducedRate / 100 * BUCKET_MIDPOINTS['$51-75']);
        const isStop = (estRevenue < 3 && altEstRevenue < 3) || totalAttempts < SALVAGE_MIN_ATTEMPT_ROWS;

        attemptRows.push({
          attemptNumber: num,
          bestBucket,
          bestGateway: bestGwInfo.gatewayAlias,
          bestProcessor: bestGwInfo.processorName,
          approvalRate,
          estRevenue: Math.round(estRevenue * 100) / 100,
          totalAttempts,
          sameGw: {
            attempts: sameGwAttempts,
            approved: sameGwApproved,
            rate: sameGwRate,
          },
          switchGw: {
            attempts: switchGwAttempts,
            approved: switchGwApproved,
            rate: switchGwRate,
            bestGateway: switchBestGw.gatewayAlias,
            bestProcessor: switchBestGw.processorName,
          },
          recommendation,
          liftPp,
          isStop,
          ltv: ltv51,
          secondarySuggestion,
        });

        if (isStop) break;
      }
    }

    // -------------------------------------------------------------------
    // 5. Determine card border color
    // -------------------------------------------------------------------
    const att1Revenue = attemptRows[0]?.estRevenue || 0;
    let borderColor = att1Revenue > 15 ? 'green' : att1Revenue >= 5 ? 'amber' : 'red';

    // -------------------------------------------------------------------
    // 6. Build copy sequence text
    // -------------------------------------------------------------------
    const sequenceLines = [];
    let lastAttemptShown = 1;
    const switchRows = attemptRows.filter(r => r.attemptNumber >= 2 && r.recommendation === 'switch');
    for (const row of attemptRows) {
      if (row.isStop && row.stopMessage) {
        sequenceLines.push(`Attempt ${row.attemptNumber}: STOP — ${row.stopMessage}`);
        lastAttemptShown = row.attemptNumber;
      } else if (row.attemptNumber >= 2 && row.recommendation === 'switch') {
        const processor = row.switchGw.bestProcessor || row.switchGw.bestGateway;
        sequenceLines.push(`Attempt ${row.attemptNumber}: Switch to ${processor}, charge ${row.bestBucket}, est $${(row.estRevenue || 0).toFixed(2)}/attempt`);
        lastAttemptShown = row.attemptNumber;
      } else if (row.attemptNumber >= 2) {
        const processor = row.bestProcessor || row.bestGateway || 'same';
        sequenceLines.push(`Attempt ${row.attemptNumber}: Stay with ${processor}, charge ${row.bestBucket}, est $${(row.estRevenue || 0).toFixed(2)}/attempt`);
        lastAttemptShown = row.attemptNumber;
      }
    }

    // Find the stop point (non-hard-stop rows)
    const stopRow = attemptRows.find(r => r.isStop && !r.stopMessage);
    const lastAttemptRow = attemptRows[attemptRows.length - 1];
    if (stopRow) {
      sequenceLines.push(`Stop after attempt ${stopRow.attemptNumber}`);
    } else if (lastAttemptRow && !lastAttemptRow.isStop && lastAttemptRow.attemptNumber > lastAttemptShown) {
      sequenceLines.push(`Stop after attempt ${lastAttemptRow.attemptNumber}`);
    }

    const copySequenceText = sequenceLines.join('\n');
    const hasOverrides = switchRows.length > 0;

    groups.push({
      groupKey:     key,
      groupLabel:   buildGroupLabel(key, level),
      issuer_bank:  grp.issuer_bank,
      card_brand:   grp.card_brand,
      card_type:    grp.card_type,
      card_level:   grp.card_level,
      is_prepaid:   grp.is_prepaid,
      bins:         Array.from(grp.bins),
      totalAttempts: grp.totalAttempts,
      attempts:     attemptRows,
      borderColor,
      copySequenceText,
      hasOverrides,
    });
  }

  // Sort by totalAttempts descending
  groups.sort((a, b) => b.totalAttempts - a.totalAttempts);

  // -----------------------------------------------------------------------
  // 7. Build summary
  // -----------------------------------------------------------------------
  const summary = {
    totalGroups:   groups.length,
    totalAttempts: groups.reduce((s, g) => s + g.totalAttempts, 0),
    level,
    greenCards:    groups.filter(g => g.borderColor === 'green').length,
    amberCards:    groups.filter(g => g.borderColor === 'amber').length,
    redCards:      groups.filter(g => g.borderColor === 'red').length,
    grayCards:     groups.filter(g => g.borderColor === 'gray').length,
  };

  return { groups, summary };
}

// ---------------------------------------------------------------------------
// Salvage Sequence — Utility functions
// ---------------------------------------------------------------------------

/**
 * Compute a rounded approval rate percentage.
 */
function _rate(approved, total) {
  if (total === 0) return 0;
  return Math.round((approved / total) * 10000) / 100;
}

/**
 * Find the price bucket with the highest approval rate among orders.
 * Requires at least 10 orders in a bucket to consider it.
 */
function _findBestBucket(orders) {
  const bucketStats = {};
  for (const b of BUCKETS) {
    bucketStats[b.label] = { attempts: 0, approved: 0 };
  }

  for (const o of orders) {
    const s = bucketStats[o.bucket];
    if (s) {
      s.attempts++;
      if (o.isApproved) s.approved++;
    }
  }

  let bestBucket = null;
  let bestRate = -1;
  for (const b of BUCKETS) {
    const s = bucketStats[b.label];
    if (s.attempts >= 10) {
      const rate = _rate(s.approved, s.attempts);
      if (rate > bestRate) {
        bestRate = rate;
        bestBucket = b.label;
      }
    }
  }

  // Fallback: if no bucket has 10+ attempts, pick the one with the most
  if (bestBucket === null) {
    let maxAttempts = 0;
    for (const b of BUCKETS) {
      if (bucketStats[b.label].attempts > maxAttempts) {
        maxAttempts = bucketStats[b.label].attempts;
        bestBucket = b.label;
        bestRate = _rate(bucketStats[b.label].approved, bucketStats[b.label].attempts);
      }
    }
  }

  return { bucket: bestBucket, rate: bestRate };
}

/**
 * Find the gateway with the highest approval rate among orders.
 * Returns gateway alias and processor name.
 */
function _findBestGateway(orders) {
  if (orders.length === 0) {
    return { gatewayAlias: null, processorName: null, rate: 0 };
  }

  const gwStats = new Map(); // gateway_id -> { attempts, approved, alias, processor }

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
    return { gatewayAlias: null, processorName: null, rate: 0 };
  }

  const best = gwStats.get(bestGwId);
  return {
    gatewayAlias: best.alias,
    processorName: best.processor,
    rate: bestRate,
  };
}

/**
 * Find the gateway with the highest approval rate among orders in a specific bucket.
 */
function _findBestGatewayAtBucket(orders, targetBucket) {
  const filtered = orders.filter(o => o.bucket === targetBucket);
  return _findBestGateway(filtered);
}

/**
 * Compute approval stats for a specific bucket within an order set.
 */
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

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = { computePricePoints, computeSalvageSequence, ltvDecision };
