/**
 * Routing Playbook — Complete routing intelligence per bank card group.
 *
 * Combines: initials, cascade, upsells, rebills (3-tier), salvage, decline rules,
 * price optimization, and lifecycle affinity into one actionable row per L4 group.
 *
 * Reads from V2 cache + direct SQL for decline/salvage/affinity data.
 */
const { querySql } = require('../db/connection');
const { getCachedOrCompute, CLEAN_FILTER, CRM_ROUTING_EXCLUSION } = require('./engine');
const { computeFlowOptixV2, computeFlowOptixV2Initials } = require('./flow-optix-v2');

const MIN_APP_CONFIDENT = 20;
const MIN_ATT_SIGNAL = 10;
const MIN_C1_CONFIDENT = 30;  // 30+ C1 attempts for confident tier
const MIN_C1_EARLY = 10;      // 10-29 C1 for early signal
const STOP_RPA = 3;

// 5-tier rebill classification
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
const TEST_BINS = ['144444', '777777'];
const BIN_EXCL = TEST_BINS.map(b => `'${b}'`).join(',');

function computeRoutingPlaybook(clientId, opts = {}) {
  const days = opts.days ?? 180;
  const cacheKey = `playbook:${days}`;
  return getCachedOrCompute(clientId, 'routing-playbook', cacheKey, () => {
    return _computePlaybook(clientId, days);
  });
}

function _computePlaybook(clientId, days) {
  // ═══════════════════════════════════════════════════════════════════
  // PHASE 1: Read V2 cache for initial/upsell/cascade card data
  // ═══════════════════════════════════════════════════════════════════
  const initialsData = computeFlowOptixV2Initials(clientId, { days });
  const rebillData = computeFlowOptixV2(clientId, { days });

  const mainCards = initialsData?.main?.cards || [];
  const upsellCards = initialsData?.upsell?.cards || [];
  const rebillCards = rebillData?.cards || [];

  // Extract bank-level data from V2 cards (keyed by bank|is_prepaid)
  const mainByBank = _bankIndex(mainCards);
  const upsellByBank = _bankIndex(upsellCards);
  const rebillByBank = _bankIndex(rebillCards);

  // ═══════════════════════════════════════════════════════════════════
  // PHASE 2: Direct SQL for dimensions not in V2 cache
  // ═══════════════════════════════════════════════════════════════════

  // 2a. All bank groups with enough volume
  const dateFilter = `AND o.acquisition_date >= date('now', '-${days} days')`;
  const bankGroups = querySql(`
    SELECT b.issuer_bank, b.is_prepaid,
      COUNT(DISTINCT CASE WHEN o.derived_product_role = 'main_initial' AND o.order_status IN (2,6,8) THEN o.customer_id END) as acquired,
      COUNT(DISTINCT o.cc_first_6) as bin_count,
      SUM(CASE WHEN o.derived_product_role = 'main_rebill' AND o.derived_cycle = 1 AND o.derived_attempt = 1 THEN 1 ELSE 0 END) as c1_att
    FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY b.issuer_bank, b.is_prepaid
    HAVING c1_att >= 30
    ORDER BY acquired DESC
  `, [clientId]);

  // 2a2. Prepaid percentage per bank (across all orders, not split by is_prepaid)
  const prepaidPcts = querySql(`
    SELECT b.issuer_bank,
      COUNT(*) as total,
      SUM(CASE WHEN b.is_prepaid = 1 THEN 1 ELSE 0 END) as prepaid_count
    FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY b.issuer_bank
  `, [clientId]);
  const prepaidPctMap = new Map(prepaidPcts.map(r => [r.issuer_bank, {
    total: r.total,
    prepaidCount: r.prepaid_count,
    pct: r.total > 0 ? Math.round(r.prepaid_count / r.total * 1000) / 10 : 0,
  }]));

  // 2b. Initial processor performance per bank
  // Cascaded records count as attempts (decline on original gateway) but not approvals
  const initProcs = querySql(`
    SELECT b.issuer_bank, b.is_prepaid, g.processor_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role = 'main_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${CRM_ROUTING_EXCLUSION}
      ${dateFilter}
    GROUP BY b.issuer_bank, b.is_prepaid, g.processor_name
    HAVING att >= 5
  `, [clientId]);

  // 2c. Upsell processor performance per bank
  // Cascaded records count as attempts but not approvals
  const upsellProcs = querySql(`
    SELECT b.issuer_bank, b.is_prepaid, g.processor_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role = 'upsell_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${CRM_ROUTING_EXCLUSION}
      ${dateFilter}
    GROUP BY b.issuer_bank, b.is_prepaid, g.processor_name
    HAVING att >= 5
  `, [clientId]);

  // 2d. Rebill C1+C2 per bank × rebill processor
  // Cascaded records count as attempts (decline on original gateway) but not approvals
  const rebillProcs = querySql(`
    SELECT b.issuer_bank, b.is_prepaid, g.processor_name,
      SUM(CASE WHEN o.derived_cycle = 1 THEN 1 ELSE 0 END) as c1_att,
      SUM(CASE WHEN o.derived_cycle = 1 AND o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as c1_app,
      SUM(CASE WHEN o.derived_cycle = 2 THEN 1 ELSE 0 END) as c2_att,
      SUM(CASE WHEN o.derived_cycle = 2 AND o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as c2_app,
      COUNT(*) as total_att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as total_app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role IN ('main_rebill')
      AND o.derived_cycle IN (1, 2) AND o.derived_attempt = 1
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY b.issuer_bank, b.is_prepaid, g.processor_name
    HAVING total_att >= 5
  `, [clientId]);

  // 2d2. Bank-level rebill aggregate (not per-processor, catches banks where no single proc hits 5)
  // Cascaded records count as attempts but not approvals
  const rebillAgg = querySql(`
    SELECT b.issuer_bank, b.is_prepaid,
      SUM(CASE WHEN o.derived_cycle = 1 THEN 1 ELSE 0 END) as c1_att,
      SUM(CASE WHEN o.derived_cycle = 1 AND o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as c1_app,
      SUM(CASE WHEN o.derived_cycle = 2 THEN 1 ELSE 0 END) as c2_att,
      SUM(CASE WHEN o.derived_cycle = 2 AND o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as c2_app
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role IN ('main_rebill')
      AND o.derived_attempt = 1
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY b.issuer_bank, b.is_prepaid
  `, [clientId]);
  const _bk = (bank, pp) => `${bank}|${pp}`;
  const rebillAggIdx = new Map(rebillAgg.map(r => [_bk(r.issuer_bank, r.is_prepaid), r]));

  // 2e. Rebill salvage: per bank, attempt 2+ performance
  // Cascaded records count as attempts but not approvals
  const salvageData = querySql(`
    SELECT b.issuer_bank, b.is_prepaid, o.derived_attempt as attempt,
      g.processor_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app,
      ROUND(AVG(o.order_total), 2) as avg_price
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role IN ('main_rebill')
      AND o.derived_cycle IN (1, 2) AND o.derived_attempt >= 2
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY b.issuer_bank, b.is_prepaid, o.derived_attempt, g.processor_name
    HAVING att >= 3
  `, [clientId]);

  // 2f. Decline reasons — cascade recovery rate per bank
  const cascadeDeclines = querySql(`
    SELECT b.issuer_bank, b.is_prepaid, o.decline_reason,
      COUNT(*) as declined,
      SUM(CASE WHEN casc.order_status IN (2,6,8) THEN 1 ELSE 0 END) as recovered
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN orders casc ON o.customer_id = casc.customer_id AND casc.client_id = o.client_id
      AND casc.is_cascaded = 1 AND casc.order_status IN (2,6,8)
      AND casc.derived_product_role = 'main_initial'
      AND casc.acquisition_date >= o.acquisition_date
    WHERE o.client_id = ? AND o.derived_product_role = 'main_initial'
      AND (o.order_status = 7 OR o.is_cascaded = 1)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
      AND o.decline_reason != 'Prepaid Credit Cards Are Not Accepted'
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY b.issuer_bank, b.is_prepaid, o.decline_reason
    HAVING declined >= 5
  `, [clientId]);

  // 2g. Rebill decline reasons — retry recovery per bank
  const rebillDeclines = querySql(`
    SELECT b.issuer_bank, b.is_prepaid, o.decline_reason,
      COUNT(*) as declined,
      SUM(CASE WHEN retry.order_status IN (2,6,8) THEN 1 ELSE 0 END) as recovered
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN orders retry ON o.customer_id = retry.customer_id AND retry.client_id = o.client_id
      AND retry.derived_product_role = 'main_rebill'
      AND retry.derived_attempt > o.derived_attempt
      AND retry.derived_cycle = o.derived_cycle
      AND retry.order_status IN (2,6,8)
    WHERE o.client_id = ? AND o.derived_product_role = 'main_rebill'
      AND (o.order_status = 7 OR o.is_cascaded = 1) AND o.derived_attempt = 1
      AND o.derived_cycle IN (1, 2)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
      AND o.decline_reason != 'Prepaid Credit Cards Are Not Accepted'
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY b.issuer_bank, b.is_prepaid, o.decline_reason
    HAVING declined >= 5
  `, [clientId]);

  // 2h. Acquisition affinity per bank
  // Cascaded rebill records count as attempts but not approvals
  const acqAffinity = querySql(`
    SELECT acq.issuer_bank, acq.is_prepaid, acq.init_proc,
      COUNT(*) as reb_att,
      SUM(CASE WHEN r.order_status IN (2,6,8) AND r.is_cascaded = 0 THEN 1 ELSE 0 END) as reb_app
    FROM (
      SELECT DISTINCT i.customer_id, b.issuer_bank, b.is_prepaid, g.processor_name as init_proc
      FROM orders i
      JOIN gateways g ON i.processing_gateway_id = g.gateway_id AND g.client_id = i.client_id
      JOIN bin_lookup b ON i.cc_first_6 = b.bin
      WHERE i.client_id = ? AND i.derived_product_role = 'main_initial' AND i.order_status IN (2,6,8) AND i.is_cascaded = 0
        AND i.is_test = 0 AND i.is_internal_test = 0 AND g.processor_name IS NOT NULL
        AND i.acquisition_date >= date('now', '-${days} days')
    ) acq
    JOIN orders r ON acq.customer_id = r.customer_id AND r.client_id = ?
    WHERE r.derived_product_role = 'main_rebill' AND r.derived_cycle IN (1, 2)
      AND r.derived_attempt = 1
      AND r.is_test = 0 AND r.is_internal_test = 0
      AND r.acquisition_date >= date('now', '-${days} days')
    GROUP BY acq.issuer_bank, acq.is_prepaid, acq.init_proc
    HAVING reb_att >= 5
  `, [clientId, clientId]);

  // 2i. BIN list per bank group
  const binLists = querySql(`
    SELECT b.issuer_bank, b.is_prepaid, GROUP_CONCAT(DISTINCT o.cc_first_6) as bins
    FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
      ${dateFilter}
    GROUP BY b.issuer_bank, b.is_prepaid
  `, [clientId]);

  // 2j. L4 breakdown per bank — for outlier detection
  const l4Breakdown = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type,
      COUNT(DISTINCT o.cc_first_6) as bin_count,
      GROUP_CONCAT(DISTINCT o.cc_first_6) as bins,
      -- Initials: cascaded records count as attempts but not approvals
      SUM(CASE WHEN o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL) THEN 1 ELSE 0 END) as init_att,
      SUM(CASE WHEN o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL) AND o.is_cascaded = 0 AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as init_app,
      -- C1 rebill: same pattern
      SUM(CASE WHEN o.derived_product_role = 'main_rebill' AND o.derived_cycle = 1 AND o.derived_attempt = 1 THEN 1 ELSE 0 END) as c1_att,
      SUM(CASE WHEN o.derived_product_role = 'main_rebill' AND o.derived_cycle = 1 AND o.derived_attempt = 1 AND o.is_cascaded = 0 AND o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as c1_app
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilter}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type
    HAVING (init_att >= 10 OR c1_att >= 10)
  `, [clientId]);

  const l4Idx = _groupBy(l4Breakdown, r => r.issuer_bank);

  // ═══════════════════════════════════════════════════════════════════
  // PHASE 3: Index all data by bank key
  // ═══════════════════════════════════════════════════════════════════
  const bk = _bk;

  const initProcIdx = _groupBy(initProcs, r => bk(r.issuer_bank, r.is_prepaid));
  const upsellProcIdx = _groupBy(upsellProcs, r => bk(r.issuer_bank, r.is_prepaid));
  const rebillProcIdx = _groupBy(rebillProcs, r => bk(r.issuer_bank, r.is_prepaid));
  const salvageIdx = _groupBy(salvageData, r => bk(r.issuer_bank, r.is_prepaid));
  const cascDecIdx = _groupBy(cascadeDeclines, r => bk(r.issuer_bank, r.is_prepaid));
  const rebDecIdx = _groupBy(rebillDeclines, r => bk(r.issuer_bank, r.is_prepaid));
  const acqIdx = _groupBy(acqAffinity, r => bk(r.issuer_bank, r.is_prepaid));
  const binIdx = new Map(binLists.map(r => [bk(r.issuer_bank, r.is_prepaid), r.bins ? r.bins.split(',') : []]));

  // ═══════════════════════════════════════════════════════════════════
  // PHASE 3.5: Batch all L4 queries (6 queries total instead of 6 per L4 group)
  // ═══════════════════════════════════════════════════════════════════
  const l4Batch = _batchL4Data(clientId, days);

  // ═══════════════════════════════════════════════════════════════════
  // PHASE 4: Build playbook rows
  // ═══════════════════════════════════════════════════════════════════
  const rows = [];

  for (const bg of bankGroups) {
    const key = bk(bg.issuer_bank, bg.is_prepaid);
    const bins = binIdx.get(key) || [];

    // ── Initial routing ──
    const initPs = (initProcIdx.get(key) || []).map(p => ({
      processor: p.processor_name, att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).sort((a, b) => b.rate - a.rate);

    const initBest = initPs.filter(p => p.att >= MIN_ATT_SIGNAL && p.app > 0);
    const initBlock = initPs.filter(p => p.att >= MIN_ATT_SIGNAL && p.app === 0);

    // ── Upsell routing ──
    const upsPs = (upsellProcIdx.get(key) || []).map(p => ({
      processor: p.processor_name, att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).sort((a, b) => b.rate - a.rate);

    // ── Cascade chain (from V2 card data — keyed by bank only, V2 doesn't split by prepaid) ──
    const mainCard = mainByBank.get(bg.issuer_bank);
    const cascadeTargets = (mainCard?.cascadeTargets || [])
      .filter(t => t.rate > 0)
      .sort((a, b) => b.rate - a.rate)
      .slice(0, 3);

    // ── Cascade decline rules ──
    const cascDecs = (cascDecIdx.get(key) || []).sort((a, b) => b.declined - a.declined);
    const cascadeOn = cascDecs.filter(d => d.recovered > 0).map(d => ({
      reason: d.decline_reason,
      declined: d.declined,
      recovered: d.recovered,
      recoveryRate: Math.round(d.recovered / d.declined * 10000) / 100,
    })).sort((a, b) => b.recoveryRate - a.recoveryRate);
    const cascadeSkip = cascDecs.filter(d => d.recovered === 0 && d.declined >= 5).map(d => ({
      reason: d.decline_reason,
      declined: d.declined,
    }));

    // ── Rebill routing ──
    const rebPs = (rebillProcIdx.get(key) || []).map(p => ({
      processor: p.processor_name,
      c1_att: p.c1_att, c1_app: p.c1_app,
      c1_rate: p.c1_att > 0 ? Math.round(p.c1_app / p.c1_att * 10000) / 100 : 0,
      c2_att: p.c2_att, c2_app: p.c2_app,
      c2_rate: p.c2_att > 0 ? Math.round(p.c2_app / p.c2_att * 10000) / 100 : 0,
      total_att: p.total_att, total_app: p.total_app,
      total_rate: p.total_att > 0 ? Math.round(p.total_app / p.total_att * 10000) / 100 : 0,
    })).sort((a, b) => b.c1_rate - a.c1_rate);

    // Use bank-level aggregate as primary (catches all attempts), per-processor for breakdown
    const agg = rebillAggIdx.get(key);
    const totalC1Att = agg ? agg.c1_att : rebPs.reduce((s, p) => s + p.c1_att, 0);
    const totalC1App = agg ? agg.c1_app : rebPs.reduce((s, p) => s + p.c1_app, 0);
    const totalC2Att = agg ? agg.c2_att : rebPs.reduce((s, p) => s + p.c2_att, 0);
    const totalC2App = agg ? agg.c2_app : rebPs.reduce((s, p) => s + p.c2_app, 0);
    const c1Rate = totalC1Att > 0 ? Math.round(totalC1App / totalC1Att * 10000) / 100 : 0;
    const c2Rate = totalC2Att > 0 ? Math.round(totalC2App / totalC2Att * 10000) / 100 : 0;

    const tier = classifyRebillTier(c1Rate, totalC1Att, totalC1App);

    const rebBest = rebPs.filter(p => p.total_att >= 5 && p.total_app > 0);
    const rebBlock = rebPs.filter(p => p.total_att >= MIN_ATT_SIGNAL && p.total_app === 0);

    // ── Price strategy (per tier) ──
    let priceStrategy = null;
    const currentPrice = 97.48;
    const currentRpa = c1Rate * currentPrice / 100;
    const baseTier = tier.replace('Early: ', '');

    if (baseTier === 'UNTESTED') {
      // 0% at $97.48 — test at floor
      const targetPrice = 39.97;
      priceStrategy = {
        currentPrice, currentRate: c1Rate, currentRpa: 0,
        targetPrice, breakEvenRate: 0,
        recommendation: `0% at $${currentPrice} — test at $${targetPrice} to see if any approvals come through`,
        tier: 'UNTESTED',
      };
    } else if (baseTier === 'HOSTILE') {
      // 0.1-3% — drop to floor, any recovery is profit
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
      // 3-10% — try two price points
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
      // 10-15% — optional price tuning
      const targetPrice = 79.97;
      const breakEvenRate = currentRpa > 0 ? Math.round(currentRpa / targetPrice * 10000) / 100 : 0;
      priceStrategy = {
        currentPrice, currentRate: c1Rate, currentRpa: Math.round(currentRpa * 100) / 100,
        targetPrice, breakEvenRate,
        recommendation: `Consider $${targetPrice} — need ${breakEvenRate}% to match current RPA`,
        tier: 'VIABLE',
      };
    }
    // STRONG and VIABLE >15%: no price strategy needed

    // ── Rebill salvage sequence ──
    const salv = (salvageIdx.get(key) || []);
    const salvageSeq = [];
    for (let att = 2; att <= 4; att++) {
      const attData = salv.filter(s => s.attempt === att).sort((a, b) => {
        const rateA = a.att > 0 ? a.app / a.att : 0;
        const rateB = b.att > 0 ? b.app / b.att : 0;
        return rateB - rateA;
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
        attempt: att,
        processor: best.processor_name,
        rate,
        att: best.att,
        app: best.app,
        price: best.avg_price,
        rpa: Math.round(rpa * 100) / 100,
        isStop: false,
      });
    }

    // ── Rebill decline rules ──
    const rebDecs = (rebDecIdx.get(key) || []).sort((a, b) => b.declined - a.declined);
    const rebillRetryOn = rebDecs.filter(d => d.recovered > 0).map(d => ({
      reason: d.decline_reason,
      declined: d.declined,
      recovered: d.recovered,
      recoveryRate: Math.round(d.recovered / d.declined * 10000) / 100,
    })).sort((a, b) => b.recoveryRate - a.recoveryRate);
    const rebillStopOn = rebDecs.filter(d => d.recovered === 0 && d.declined >= 5).map(d => ({
      reason: d.decline_reason,
      declined: d.declined,
    }));

    // ── Acquisition affinity ──
    const acqAffs = (acqIdx.get(key) || []).map(a => ({
      processor: a.init_proc,
      rebAtt: a.reb_att,
      rebApp: a.reb_app,
      rebRate: a.reb_att > 0 ? Math.round(a.reb_app / a.reb_att * 10000) / 100 : 0,
    })).sort((a, b) => b.rebRate - a.rebRate);

    // ── Confidence ──
    const maxApp = Math.max(
      initPs.reduce((m, p) => Math.max(m, p.app), 0),
      upsPs.reduce((m, p) => Math.max(m, p.app), 0),
      rebPs.reduce((m, p) => Math.max(m, p.total_app), 0),
    );
    const maxAtt = Math.max(
      initPs.reduce((m, p) => Math.max(m, p.att), 0),
      totalC1Att,
    );
    const confidenceTier = maxApp >= MIN_APP_CONFIDENT ? 'Confident'
      : maxAtt >= MIN_ATT_SIGNAL ? 'Early signal' : 'Skip';

    if (confidenceTier === 'Skip') continue;

    // ── Price optimization (from V2 rebill card — keyed by bank only) ──
    const rebCard = rebillByBank.get(bg.issuer_bank);
    const priceOpt = rebCard?.priceOptimization || null;

    rows.push({
      issuer_bank: bg.issuer_bank,
      is_prepaid: bg.is_prepaid,
      acquired: bg.acquired,
      bins,
      binCount: bins.length,
      confidenceTier,

      // Initial
      initialBest: initBest.slice(0, 3),
      initialBlock: initBlock,

      // Cascade
      cascadeChain: cascadeTargets,
      cascadeOn: cascadeOn.slice(0, 5),
      cascadeSkip: cascadeSkip.slice(0, 5),

      // Upsell
      upsellBest: upsPs.slice(0, 3),

      // Rebill
      rebillTier: tier,
      c1: { att: totalC1Att, app: totalC1App, rate: c1Rate },
      c2: { att: totalC2Att, app: totalC2App, rate: c2Rate },
      rebillBest: rebBest.slice(0, 3),
      rebillBlock: rebBlock,
      priceOptimization: priceOpt,
      priceStrategy,

      // Salvage
      salvageSequence: salvageSeq,
      rebillRetryOn: rebillRetryOn.slice(0, 5),
      rebillStopOn: rebillStopOn.slice(0, 5),

      // Lifecycle
      acquisitionAffinity: acqAffs.slice(0, 3),

      // L4 sub-groups — each with own routing when data permits, bank-level is fallback
      l4Groups: _computeL4Groups(clientId, days, bg.issuer_bank, bg.is_prepaid, l4Idx, initPs, c1Rate, l4Batch),

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
      withPriceOpt: rows.filter(r => r.priceOptimization).length,
    },
  };
}

// ─── Helpers ───

// V2 cards are grouped at L2 (issuer_bank + card_brand), not by is_prepaid.
// Key by issuer_bank only — prepaid split lives at L3 within cards.
function _bankIndex(cards) {
  const map = new Map();
  for (const c of cards) {
    if (!map.has(c.issuer_bank)) map.set(c.issuer_bank, c);
  }
  return map;
}

// ─── Batch L4 data (runs once for ALL L4 groups, then sliced per group) ───

function _batchL4Data(clientId, days) {
  const dateFilterInline = `AND o.acquisition_date >= date('now', '-${days} days')`;
  const l4Key = r => `${r.issuer_bank}|${r.card_brand}|${r.is_prepaid}|${r.card_type}`;

  // Batch 1: Initial processor routing per L4
  const initRouting = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role = 'main_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      AND ${CRM_ROUTING_EXCLUSION}
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name
    HAVING att >= 3
  `, [clientId]);

  // Batch 2: Rebill C1 processor routing per L4
  const rebillRouting = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role = 'main_rebill'
      AND o.derived_cycle IN (1, 2) AND o.derived_attempt = 1
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name
    HAVING att >= 3
  `, [clientId]);

  // Batch 3: Cascade targets per L4
  const cascadeTargets = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) as app
    FROM orders o
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.is_cascaded = 1
      AND o.derived_product_role = 'main_initial'
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, g.processor_name
    HAVING att >= 3
  `, [clientId]);

  // Batch 4: Initial decline reasons with cascade recovery per L4
  // Optimized: pre-compute cascade-recovered customers as a small subquery
  // instead of LEFT JOIN on full orders table
  const initDeclines = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, o.decline_reason,
      COUNT(*) as declined,
      SUM(CASE WHEN cr.customer_id IS NOT NULL THEN 1 ELSE 0 END) as recovered
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN (
      SELECT DISTINCT customer_id
      FROM orders
      WHERE client_id = ? AND is_cascaded = 1 AND order_status IN (2,6,8)
        AND derived_product_role = 'main_initial'
        AND is_test = 0 AND is_internal_test = 0
    ) cr ON o.customer_id = cr.customer_id
    WHERE o.client_id = ? AND o.derived_product_role = 'main_initial'
      AND (o.order_status = 7 OR o.is_cascaded = 1)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
      AND o.decline_reason != 'Prepaid Credit Cards Are Not Accepted'
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, o.decline_reason
    HAVING declined >= 3
  `, [clientId, clientId]);

  // Batch 5: Rebill decline reasons with retry recovery per L4
  // Optimized: pre-compute retry-recovered customers as a small subquery
  const rebDeclines = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, o.decline_reason,
      COUNT(*) as declined,
      SUM(CASE WHEN rr.customer_id IS NOT NULL THEN 1 ELSE 0 END) as recovered
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    LEFT JOIN (
      SELECT DISTINCT customer_id, derived_cycle
      FROM orders
      WHERE client_id = ? AND derived_product_role = 'main_rebill'
        AND order_status IN (2,6,8) AND derived_attempt > 1
        AND is_test = 0 AND is_internal_test = 0
    ) rr ON o.customer_id = rr.customer_id AND o.derived_cycle = rr.derived_cycle
    WHERE o.client_id = ? AND o.derived_product_role = 'main_rebill'
      AND (o.order_status = 7 OR o.is_cascaded = 1) AND o.derived_attempt = 1
      AND o.derived_cycle IN (1, 2)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
      AND o.decline_reason != 'Prepaid Credit Cards Are Not Accepted'
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, o.decline_reason
    HAVING declined >= 3
  `, [clientId, clientId]);

  // Batch 6: Rebill salvage per L4
  const salvageData = querySql(`
    SELECT b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type,
      o.derived_attempt as attempt, g.processor_name,
      COUNT(*) as att,
      SUM(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 ELSE 0 END) as app,
      ROUND(AVG(o.order_total), 2) as avg_price
    FROM orders o
    JOIN gateways g ON o.processing_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = ? AND o.derived_product_role = 'main_rebill'
      AND o.derived_cycle IN (1, 2) AND o.derived_attempt >= 2
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.processor_name IS NOT NULL AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${BIN_EXCL})
      ${dateFilterInline}
    GROUP BY b.issuer_bank, b.card_brand, b.is_prepaid, b.card_type, o.derived_attempt, g.processor_name
    HAVING att >= 3
  `, [clientId]);

  // Index all batch results by L4 key
  return {
    initRouting: _groupBy(initRouting, l4Key),
    rebillRouting: _groupBy(rebillRouting, l4Key),
    cascadeTargets: _groupBy(cascadeTargets, l4Key),
    initDeclines: _groupBy(initDeclines, l4Key),
    rebDeclines: _groupBy(rebDeclines, l4Key),
    salvageData: _groupBy(salvageData, l4Key),
  };
}

function _computeL4Groups(clientId, days, bankName, bankPrepaid, l4Idx, bankInitPs, bankC1Rate, l4Batch) {
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

    // ── Slice from batch data (no SQL queries!) ──
    const initRouting = (l4Batch.initRouting.get(l4k) || []).map(p => ({
      processor: p.processor_name, att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).sort((a, b) => b.rate - a.rate);

    const rebillRouting = (l4Batch.rebillRouting.get(l4k) || []).map(p => ({
      processor: p.processor_name, att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).sort((a, b) => b.rate - a.rate);

    const cascadeTargets = (l4Batch.cascadeTargets.get(l4k) || []).map(p => ({
      name: p.processor_name, att: p.att, app: p.app,
      rate: p.att > 0 ? Math.round(p.app / p.att * 10000) / 100 : 0,
    })).filter(t => t.rate > 0).sort((a, b) => b.rate - a.rate).slice(0, 3);

    const initDeclines = (l4Batch.initDeclines.get(l4k) || []).sort((a, b) => b.declined - a.declined);
    const cascadeOn = initDeclines.filter(d => d.recovered > 0).map(d => ({
      reason: d.decline_reason, declined: d.declined, recovered: d.recovered,
      recoveryRate: Math.round(d.recovered / d.declined * 10000) / 100,
    })).sort((a, b) => b.recoveryRate - a.recoveryRate).slice(0, 5);
    const cascadeSkip = initDeclines.filter(d => d.recovered === 0 && d.declined >= 3).map(d => ({
      reason: d.decline_reason, declined: d.declined,
    })).slice(0, 5);

    const rebDeclines = (l4Batch.rebDeclines.get(l4k) || []).sort((a, b) => b.declined - a.declined);
    const rebillRetryOn = rebDeclines.filter(d => d.recovered > 0).map(d => ({
      reason: d.decline_reason, declined: d.declined, recovered: d.recovered,
      recoveryRate: Math.round(d.recovered / d.declined * 10000) / 100,
    })).sort((a, b) => b.recoveryRate - a.recoveryRate).slice(0, 5);
    const rebillStopOn = rebDeclines.filter(d => d.recovered === 0 && d.declined >= 3).map(d => ({
      reason: d.decline_reason, declined: d.declined,
    })).slice(0, 5);

    // ── Salvage sequence (from batch) ──
    const salvRaw = l4Batch.salvageData.get(l4k) || [];
    const salvageSeq = [];
    for (let att = 2; att <= 4; att++) {
      const attData = salvRaw.filter(s => s.attempt === att).sort((a, b) => {
        const rateA = a.att > 0 ? a.app / a.att : 0;
        const rateB = b.att > 0 ? b.app / b.att : 0;
        return rateB - rateA;
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
      isInitOutlier,
      isC1Outlier,
      routingLevel,
      initRouting: initRouting.slice(0, 3),
      initBlock: initRouting.filter(p => p.att >= 5 && p.app === 0),
      rebillRouting: rebillRouting.slice(0, 3),
      rebillBlock: rebillRouting.filter(p => p.att >= 5 && p.app === 0),
      cascadeTargets,
      cascadeOn,
      cascadeSkip,
      rebillRetryOn,
      rebillStopOn,
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

function _groupBy(rows, keyFn) {
  const map = new Map();
  for (const r of rows) {
    const key = keyFn(r);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(r);
  }
  return map;
}

module.exports = { computeRoutingPlaybook };
