/**
 * /api/route — the shadow-mode routing endpoint.
 *
 * Flow per request:
 *   1. Look up BIN metadata (issuer_bank, card_type, is_prepaid, card_brand).
 *   2. Pull active candidate gateways for the authenticated client.
 *   3. Apply lookup-engine filter (hard_exclude + soft_downrank).
 *   4. Ask the Python scoring daemon for AI scores per survivor.
 *   5. Pick the winner (AI top-score among survivors; lookup fallback on daemon fail).
 *   6. Insert one shadow_decisions row; return { shadow_id, gateway_id, processor, confidence, reason, latency_ms }.
 *
 * This endpoint NEVER forces a gateway for Stage 0 — the recommendation is
 * logged only. Beast Insights continues to route actual traffic on Kytsan.
 *
 * Also exposes POST /submit-timing for the PHP extension to report whether
 * the response arrived before the customer clicked Place Order.
 */

const express = require('express');
const crypto = require('crypto');
const { querySql, queryOneSql, runSql } = require('../db/connection');
const {
  filterInitialCandidates,
  loadTables,
  cardTypeMerged,
} = require('../routing/lookup-engine');
const { scoreInitialCandidates } = require('../ml/scoring-client');

const router = express.Router();

const DEFAULT_MODEL_VERSION = process.env.INITIAL_MODEL_VERSION || 'five_model_initial.pkl';

// Preload lookup tables on first require of this module so the first request
// isn't paying file-read latency.
try { loadTables(); } catch (_) { /* ignored — will retry on first real call */ }

function hashEmail(email) {
  if (!email || typeof email !== 'string') return null;
  return crypto.createHash('sha256').update(email.trim().toLowerCase()).digest('hex');
}

function isSixDigitBin(s) {
  return typeof s === 'string' && /^\d{6}$/.test(s);
}

// ---------------------------------------------------------------------------
// A/B Experiment — deterministic bucketing + assignment
// ---------------------------------------------------------------------------

function stableBucket(input, modulo = 10000) {
  const hash = crypto.createHash('sha256').update(String(input)).digest('hex');
  return parseInt(hash.slice(0, 8), 16) % modulo;
}

function assignExperiment({ clientId, emailHash, pickedGatewayId, reason }) {
  const assignment = {
    experimentId: null,
    experimentVariant: 'none',
    selectedBy: 'beast',
    experimentBucket: null,
    experimentSkipReason: null,
    wouldForceGateway: 0,
    forceGatewayRequested: 0,
    forceGatewayResult: 'not_treatment',
    experimentAssignmentKey: null,
  };

  if (process.env.EXPERIMENT_KILL_SWITCH === '1') {
    assignment.experimentSkipReason = 'kill_switch';
    return assignment;
  }
  if (pickedGatewayId == null || reason === 'no_candidates') {
    assignment.experimentSkipReason = 'no_candidates';
    return assignment;
  }

  let activeExp;
  try {
    activeExp = queryOneSql(
      `SELECT id, traffic_pct, treatment_pct FROM experiments
       WHERE client_id = ? AND status = 'active' ORDER BY id DESC LIMIT 1`,
      [clientId]
    );
  } catch (err) {
    console.error('[api/route] experiment lookup failed:', err.message);
    assignment.experimentSkipReason = 'assignment_error';
    return assignment;
  }

  if (!activeExp) {
    assignment.experimentSkipReason = 'no_active_experiment';
    return assignment;
  }

  assignment.experimentId = activeExp.id;

  const stableKey = [activeExp.id, clientId, emailHash || 'no_customer'].join(':');
  assignment.experimentAssignmentKey = crypto.createHash('sha256').update(stableKey).digest('hex').slice(0, 16);

  const bucket = stableBucket(stableKey);
  assignment.experimentBucket = bucket;

  const trafficLimit = Math.round(Number(activeExp.traffic_pct || 0) * 100);
  if (bucket >= trafficLimit) {
    assignment.experimentSkipReason = 'traffic_rollout_none';
    return assignment;
  }

  const treatmentLimit = Math.round(trafficLimit * (Number(activeExp.treatment_pct || 0) / 100));
  if (bucket < treatmentLimit) {
    assignment.experimentVariant = 'treatment';
    assignment.selectedBy = 'ai';
    assignment.wouldForceGateway = 1;
    assignment.forceGatewayRequested = 0;  // Release 1: never force
    assignment.forceGatewayResult = 'not_enabled_release_1';
  } else {
    assignment.experimentVariant = 'control';
    assignment.selectedBy = 'beast';
    assignment.forceGatewayResult = 'not_requested';
  }

  return assignment;
}

/**
 * Build the lookup map keyed by processor_name for each candidate.
 * Returns { processorByName, entriesByName } where entriesByName holds the raw
 * lookup entry so we can surface approval_rate + sample_size in candidate_pool_json.
 */
function buildCandidatePool(filterResult) {
  const pool = {};
  // The lookup-engine already captured per-candidate entries — we reconstruct
  // by walking its candidates + excluded + downranked output. The `log` object
  // lists the tier used; `candidates` is the processors that survived.
  return pool;
}

/**
 * POST /api/route
 */
router.post('/', async (req, res) => {
  const startedAt = Date.now();
  const clientId = req.apiClientId;
  const { bin, amount, product_id, email, sales_type } = req.body || {};

  // --- basic input validation ---------------------------------------------
  if (!isSixDigitBin(bin)) {
    return res.status(400).json({ error: 'bin must be a 6-digit string' });
  }
  const salesType = (sales_type || 'INITIALS').toUpperCase();
  if (salesType !== 'INITIALS') {
    // Stage 0 supports initials only; rebills/upsells come in a later phase.
    return res.status(400).json({ error: `unsupported sales_type: ${salesType}` });
  }

  const amt = amount != null ? parseFloat(amount) : null;
  const pid = product_id != null ? parseInt(product_id, 10) || null : null;
  const emailHash = hashEmail(email);

  // --- 0. Merchant vertical (for P3 logging) --------------------------------
  const clientRow = queryOneSql(
    'SELECT merchant_vertical FROM clients WHERE id = ?', [clientId]
  );
  const merchantVertical = (clientRow && clientRow.merchant_vertical) || null;

  // --- 1. BIN lookup ------------------------------------------------------
  const binRow = queryOneSql(
    'SELECT bin, issuer_bank, card_brand, card_type, is_prepaid FROM bin_lookup WHERE bin = ?',
    [bin]
  ) || { bin, issuer_bank: null, card_brand: null, card_type: null, is_prepaid: 0 };

  // --- 2. Candidate gateways ---------------------------------------------
  const gateways = querySql(
    `SELECT gateway_id, processor_name, bank_name, mcc_code, gateway_created,
            COALESCE(is_warming_up, 0)    AS is_warming_up,
            COALESCE(is_exploration, 0)   AS is_exploration
       FROM gateways
      WHERE client_id = ?
        AND gateway_active = 1
        AND (exclude_from_analysis = 0 OR exclude_from_analysis IS NULL)
        AND processor_name IS NOT NULL`,
    [clientId]
  );

  if (gateways.length === 0) {
    return writeAndRespond(res, {
      clientId, bin, amount: amt, product_id: pid, emailHash, salesType,
      recommendedGatewayId: null,
      recommendedProcessor: null,
      confidence: 0,
      reason: 'no_candidates',
      poolJson: JSON.stringify([]),
      lookupHasData: 0,
      lookupBestRate: null,
      daemonTimedOut: 0,
      daemonLatencyMs: null,
      featureSnapshot: { bin_row: binRow, sales_type: salesType },
      modelVersion: DEFAULT_MODEL_VERSION,
      aiScore: null,
      aiRecommendedGatewayId: null,
      wouldHaveApproved: null,
      startedAt,
      // P3 fields — all null/default for no-candidates path
      merchantVertical,
      issuerBank: binRow.issuer_bank,
      cardType: binRow.card_type,
      cardBrand: binRow.card_brand,
      isPrepaid: binRow.is_prepaid ? 1 : 0,
      hourOfDay: now.getUTCHours(),
      dayOfWeek: now.getUTCDay(),
      amountVsBinAvg: null,
      lookupBestGatewayId: null,
      aiDisagreedWithLookup: null,
      confidenceTier: 'no_data',
      aiScoreSpread: null,
      lookupScoreSpread: null,
      bestLookupRate: null,
      chosenLookupRate: null,
      regret: null,
      wouldHaveApprovedBinary: null,
      expectedApproval: null,
    }, 200);
  }

  // --- 3. Lookup filter ---------------------------------------------------
  // Lookup tables store processor names in UPPERCASE. The gateways table has
  // mixed case ("Priority", "Paysafe"). Canonicalize before querying, keep the
  // raw value for the AI daemon (its label encoder was fit on raw-case values).
  const canonProc = (s) => (typeof s === 'string' ? s.trim().toUpperCase() : s);

  // Distinct processors (uppercased) — lookup works at the processor level.
  const distinctProcsCanon = Array.from(new Set(gateways.map(g => canonProc(g.processor_name))));
  const filterResult = filterInitialCandidates(
    binRow.issuer_bank,
    binRow.card_type,
    binRow.is_prepaid,
    distinctProcsCanon,
    clientId
  );

  // Collect per-processor lookup entries from the filter output.
  // filterResult shape: { candidates, excluded:[{processor, lookup}], downranked:[{processor, lookup}], log }
  const lookupByProc = new Map(); // canon(proc) -> entry
  for (const ent of (filterResult.excluded || [])) {
    if (ent && ent.lookup) lookupByProc.set(ent.processor, ent.lookup);
  }
  for (const ent of (filterResult.downranked || [])) {
    if (ent && ent.lookup) lookupByProc.set(ent.processor, ent.lookup);
  }
  // Survivors don't carry their lookup in the filter result — re-query the table.
  const tables = loadTables();
  const initialTable = tables && tables.initial;
  function lookupForProc(procCanon) {
    if (lookupByProc.has(procCanon)) return lookupByProc.get(procCanon);
    if (!initialTable) return null;
    const cardKey = cardTypeMerged(binRow.card_type, binRow.is_prepaid);
    const key3d = `${binRow.issuer_bank}|${cardKey}|${procCanon}`;
    const key2d = `${binRow.issuer_bank}|${procCanon}`;
    const v = (initialTable.tier_3d && initialTable.tier_3d[key3d])
           || (initialTable.tier_2d && initialTable.tier_2d[key2d])
           || null;
    if (v) lookupByProc.set(procCanon, v);
    return v;
  }

  // Which processors (canonical) survived the filter? Hard-excluded processors
  // disqualify ALL gateways that share their processor_name. Soft-downranked
  // processors are informational — gateways pass through to the AI.
  const survivorSet = new Set(filterResult.candidates || []);
  const hardExcludedSet = new Set((filterResult.excluded || []).map(e => e.processor));

  // Eligible gateways = all gateways whose canonical processor is a survivor.
  const eligibleGateways = gateways.filter(g => survivorSet.has(canonProc(g.processor_name)));

  const lookupHasData = eligibleGateways.some(g => lookupForProc(canonProc(g.processor_name)) != null) ? 1 : 0;
  let lookupBestRate = null;
  for (const g of eligibleGateways) {
    const lk = lookupForProc(canonProc(g.processor_name));
    if (lk && typeof lk.approval_rate === 'number') {
      if (lookupBestRate == null || lk.approval_rate > lookupBestRate) {
        lookupBestRate = lk.approval_rate;
      }
    }
  }

  // --- 4. Ask the AI daemon ----------------------------------------------
  // Score EACH eligible gateway individually. Same processor but different
  // acquiring_bank / mcc_code / mid_age_days can yield different AI scores.
  const now = new Date();
  // Find lookup's best rate for computing per-candidate lift
  let lookupBestRateForDaemon = -Infinity;
  for (const g of eligibleGateways) {
    const lk = lookupForProc(canonProc(g.processor_name));
    const rate = lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : null;
    if (rate != null && rate > lookupBestRateForDaemon) lookupBestRateForDaemon = rate;
  }

  const daemonCandidates = eligibleGateways.map(g => {
    const createdAt = g.gateway_created ? new Date(g.gateway_created) : null;
    const midAgeDays = createdAt
      ? Math.max(0, Math.floor((now - createdAt) / (1000 * 60 * 60 * 24)))
      : null;
    const pc = canonProc(g.processor_name);
    const lk = lookupForProc(pc);
    const lkRate = lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : null;
    const lkSample = lk && typeof lk.sample_size === 'number' ? lk.sample_size : 0;
    let lkTier = 0;
    if (lkSample >= 100) lkTier = 3;
    else if (lkSample >= 30) lkTier = 2;
    else if (lkSample > 0) lkTier = 1;
    const isLookupPick = (lkRate != null && lookupBestRateForDaemon > -Infinity && Math.abs(lkRate - lookupBestRateForDaemon) < 0.001) ? 1 : 0;
    const liftVsLookup = (lkRate != null && lookupBestRateForDaemon > -Infinity) ? (lkRate - lookupBestRateForDaemon) * 100 : 0;

    return {
      gateway_id: g.gateway_id,
      processor_name: g.processor_name,
      acquiring_bank: g.bank_name,
      mcc_code: g.mcc_code,
      mid_age_days: midAgeDays,
      is_warming_up: g.is_warming_up ? 1 : 0,
      // V2 lookup features for daemon
      lookup_rate: lkRate,
      lookup_sample: lkSample,
      lookup_confidence_tier: lkTier,
      is_lookup_pick: isLookupPick,
      lift_vs_lookup: liftVsLookup,
    };
  });

  const daemonPayload = {
    client_id: clientId,
    bin,
    amount: amt,
    sales_type: salesType,
    request_at: now.toISOString(),
    bin_features: {
      issuer_bank: binRow.issuer_bank,
      card_brand: binRow.card_brand,
      card_type: binRow.card_type,
      is_prepaid: binRow.is_prepaid ? 1 : 0,
    },
    candidates: daemonCandidates,
  };

  let daemonResult = { ok: false, scores: [], timed_out: false, latency_ms: 0 };
  if (eligibleGateways.length >= 2) {
    daemonResult = await scoreInitialCandidates(daemonPayload);
  } else {
    daemonResult = { ok: false, skipped_single: true, latency_ms: 0 };
  }

  // gateway_id -> AI score (per-gateway, not per-processor).
  const scoreByGw = new Map();
  if (daemonResult.ok && Array.isArray(daemonResult.scores)) {
    for (const s of daemonResult.scores) {
      if (s && s.gateway_id != null) scoreByGw.set(s.gateway_id, s.score);
    }
  }

  // --- 5. Pick winner + reason -------------------------------------------
  // Decision hierarchy (locked 2026-04-29):
  //   1. Hard rules (already applied in step 3)
  //   2. High-confidence lookup (30+ samples) — wins by default
  //   3. AI model — only when lookup is weak OR AI proves +10pp lift with 30+ samples
  //   4. Exploration — separate, labeled, capped (step 5b)
  //   5. Fallback — daemon down or no data

  const LOOKUP_MIN_SAMPLES = 30;
  const AI_OVERRIDE_MIN_LIFT_PP = 10;
  const AI_OVERRIDE_MIN_SAMPLES = 30;
  const CONCENTRATION_MAX_PROC = 0.35;   // 35% per processor group
  const CONCENTRATION_MAX_MID = 0.20;    // 20% per individual MID

  let pickedGw = null;
  let reason;
  let confidence = 0;
  let overrideDiag = null; // override diagnostics object

  // Pre-compute lookup best gateway (used by multiple branches)
  let lookupTopGw = null; let lookupTopRate = -Infinity; let lookupTopSample = 0;
  for (const g of eligibleGateways) {
    const pc = canonProc(g.processor_name);
    const lk = lookupForProc(pc);
    const rate = lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : null;
    const sample = lk && typeof lk.sample_size === 'number' ? lk.sample_size : 0;
    if (rate != null && rate > lookupTopRate) {
      lookupTopRate = rate; lookupTopGw = g; lookupTopSample = sample;
    }
  }

  // Pre-compute AI best gateway
  let aiTopGwCandidate = null; let aiTopScoreVal = -Infinity;
  if (daemonResult.ok && scoreByGw.size > 0) {
    for (const g of eligibleGateways) {
      const s = scoreByGw.has(g.gateway_id) ? scoreByGw.get(g.gateway_id) : -Infinity;
      if (s > aiTopScoreVal) { aiTopScoreVal = s; aiTopGwCandidate = g; }
    }
  }

  // Concentration tracking — recent recommendation distribution
  let concByProc = {};
  let concByMid = {};
  try {
    const recentRecs = querySql(
      `SELECT recommended_processor, recommended_gateway_id, COUNT(*) as cnt
       FROM shadow_decisions WHERE client_id = ? AND request_received_at > datetime('now', '-7 days')
       GROUP BY recommended_processor, recommended_gateway_id`, [clientId]
    );
    const totalRecs = recentRecs.reduce((s, r) => s + r.cnt, 0);
    if (totalRecs > 0) {
      for (const r of recentRecs) {
        const proc = r.recommended_processor;
        concByProc[proc] = (concByProc[proc] || 0) + r.cnt;
        concByMid[r.recommended_gateway_id] = (concByMid[r.recommended_gateway_id] || 0) + r.cnt;
      }
      for (const k of Object.keys(concByProc)) concByProc[k] /= totalRecs;
      for (const k of Object.keys(concByMid)) concByMid[k] /= totalRecs;
    }
  } catch (_) { /* concentration check failure is non-fatal */ }

  if (eligibleGateways.length === 0) {
    pickedGw = gateways[0];
    reason = 'no_candidates';
    confidence = 0;
  } else if (eligibleGateways.length === 1) {
    pickedGw = eligibleGateways[0];
    reason = 'single_candidate';
    const lk = lookupForProc(canonProc(pickedGw.processor_name));
    confidence = lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : 0;
  } else if (daemonResult.ok && scoreByGw.size > 0) {
    // Both lookup and AI are available — apply decision hierarchy.

    const aiProcCanon = aiTopGwCandidate ? canonProc(aiTopGwCandidate.processor_name) : null;
    const lookupProcCanon = lookupTopGw ? canonProc(lookupTopGw.processor_name) : null;
    const aiGwLookup = aiTopGwCandidate ? lookupForProc(aiProcCanon) : null;
    const aiGwLookupRate = aiGwLookup && typeof aiGwLookup.approval_rate === 'number' ? aiGwLookup.approval_rate : null;
    const aiGwLookupSample = aiGwLookup && typeof aiGwLookup.sample_size === 'number' ? aiGwLookup.sample_size : 0;
    const expectedLiftPp = (aiGwLookupRate != null && lookupTopRate > -Infinity)
      ? (aiGwLookupRate - lookupTopRate) * 100 : null;

    // Check if lookup and AI agree (same processor)
    const aiAgreesWithLookup = (aiProcCanon === lookupProcCanon);

    if (aiAgreesWithLookup) {
      // Agreement — use AI's gateway pick (may differ at MID level)
      pickedGw = aiTopGwCandidate;
      confidence = aiTopScoreVal;
      reason = 'lookup_ai_hybrid';
    } else if (lookupTopSample >= LOOKUP_MIN_SAMPLES) {
      // High-confidence lookup — AI wants to override. Check if allowed.
      let overrideAllowed = false;
      let blockReason = null;

      if (aiGwLookupSample < AI_OVERRIDE_MIN_SAMPLES) {
        blockReason = 'low_confidence_ai_gateway';
      } else if (expectedLiftPp == null || expectedLiftPp <= AI_OVERRIDE_MIN_LIFT_PP) {
        blockReason = expectedLiftPp != null && expectedLiftPp < 0
          ? 'negative_expected_lift' : 'high_confidence_lookup_protected';
      } else {
        // AI has 30+ samples AND expected lift > +10pp — check concentration
        const aiProc = aiTopGwCandidate.processor_name;
        const aiMid = aiTopGwCandidate.gateway_id;
        if ((concByProc[aiProc] || 0) >= CONCENTRATION_MAX_PROC) {
          blockReason = 'concentration_cap_processor';
        } else if ((concByMid[aiMid] || 0) >= CONCENTRATION_MAX_MID) {
          blockReason = 'concentration_cap_mid';
        } else {
          overrideAllowed = true;
        }
      }

      // Build override diagnostics
      overrideDiag = {
        lookup_gateway: lookupTopGw.gateway_id,
        lookup_processor: lookupTopGw.processor_name,
        lookup_rate: lookupTopRate,
        lookup_sample: lookupTopSample,
        ai_gateway: aiTopGwCandidate.gateway_id,
        ai_processor: aiTopGwCandidate.processor_name,
        ai_rate: aiGwLookupRate,
        ai_sample: aiGwLookupSample,
        ai_score: aiTopScoreVal,
        expected_lift_pp: expectedLiftPp != null ? Math.round(expectedLiftPp * 10) / 10 : null,
        override_allowed: overrideAllowed,
        override_block_reason: blockReason,
        conc_proc: Math.round((concByProc[aiTopGwCandidate.processor_name] || 0) * 1000) / 10,
        conc_mid: Math.round((concByMid[aiTopGwCandidate.gateway_id] || 0) * 1000) / 10,
      };

      if (overrideAllowed) {
        pickedGw = aiTopGwCandidate;
        confidence = aiTopScoreVal;
        reason = 'ai_override_justified';
      } else {
        // Defer to lookup — pick lookup's best gateway
        pickedGw = lookupTopGw;
        confidence = lookupTopRate;
        reason = 'lookup_protected';
      }
    } else {
      // Lookup is low-confidence (< 30 samples) — AI is free to pick
      pickedGw = aiTopGwCandidate;
      confidence = aiTopScoreVal;
      reason = (lookupTopGw && lookupProcCanon !== aiProcCanon) ? 'ai_override' : 'lookup_ai_hybrid';
    }
  } else {
    // Daemon unavailable / timeout / malformed — fall back to lookup's top-rate gateway.
    let best = null;
    for (const g of eligibleGateways) {
      const lk = lookupForProc(canonProc(g.processor_name));
      const rate = lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : null;
      if (rate != null && (best == null || rate > best.rate)) best = { gw: g, rate };
    }
    pickedGw = best ? best.gw : eligibleGateways[0];
    confidence = best ? best.rate : 0;
    reason = daemonResult.timed_out ? 'daemon_timeout' : 'lookup_only_fallback';
  }

  // ai_recommended_gateway_id records "what the AI model would have picked"
  // (pre-lookup-protection, pre-exploration) so we can compare AI vs engine.
  const aiTopGatewayId = aiTopGwCandidate ? aiTopGwCandidate.gateway_id : null;
  const aiTopScore = (daemonResult.ok && scoreByGw.size > 0 && aiTopGatewayId != null)
    ? (scoreByGw.has(aiTopGatewayId) ? scoreByGw.get(aiTopGatewayId) : null)
    : null;

  // --- 5b. 10% exploration override for new processors --------------------
  // When any eligible gateway is flagged is_exploration=1, with probability
  // EXPLORATION_RATE we force-pick one (prefer the highest AI score among them)
  // so we collect real signal on new processors the AI hasn't seen enough of.
  // The AI's top pick is still recorded in ai_recommended_gateway_id so we
  // can compare "what AI wanted" vs "what engine did" in the shadow data.
  const EXPLORATION_RATE = parseFloat(process.env.EXPLORATION_RATE || '0.10');
  const explorationCandidates = eligibleGateways.filter(g => g.is_exploration === 1);
  let exploredPickedGw = null;
  if (
    explorationCandidates.length > 0 &&
    reason !== 'no_candidates' &&
    reason !== 'single_candidate' &&
    Math.random() < EXPLORATION_RATE
  ) {
    // Pick the best exploration candidate by AI score (fall back to first if no AI).
    let best = null;
    for (const g of explorationCandidates) {
      const s = scoreByGw.has(g.gateway_id) ? scoreByGw.get(g.gateway_id) : -Infinity;
      if (best == null || s > best.score) best = { gw: g, score: s };
    }
    exploredPickedGw = best ? best.gw : explorationCandidates[0];
    // Only override if exploration actually changes the pick.
    if (exploredPickedGw && exploredPickedGw.gateway_id !== (pickedGw ? pickedGw.gateway_id : null)) {
      pickedGw = exploredPickedGw;
      reason = 'exploration';
      const explScore = scoreByGw.has(pickedGw.gateway_id) ? scoreByGw.get(pickedGw.gateway_id) : null;
      confidence = Number.isFinite(explScore) ? explScore : 0;
    }
  }

  const pickedGatewayId = pickedGw ? pickedGw.gateway_id : null;
  const pickedProc = pickedGw ? pickedGw.processor_name : null;

  // ai_recommended_gateway_id records the AI's raw top pick (pre-exploration).
  // ai_score likewise records the AI's score for its OWN top pick, not the
  // exploration winner. This lets us reconstruct both decisions from the log.
  const aiScore = aiTopScore;
  const aiRecommendedGatewayId = (daemonResult.ok && scoreByGw.size > 0) ? aiTopGatewayId : null;

  // would_have_approved: historical approval rate of the picked processor for this combo.
  const pickedLookup = pickedGw ? lookupForProc(canonProc(pickedGw.processor_name)) : null;
  const wouldHaveApproved = pickedLookup && typeof pickedLookup.approval_rate === 'number'
    ? pickedLookup.approval_rate
    : null;

  // --- P3.2: Compute decision metadata for shadow logging -------------------

  // lookup_best_gateway_id: gateway with the highest lookup rate among eligible.
  let lookupBestGatewayId = null;
  let bestLookupRate = null;
  for (const g of eligibleGateways) {
    const lk = lookupForProc(canonProc(g.processor_name));
    const rate = lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : null;
    if (rate != null && (bestLookupRate == null || rate > bestLookupRate)) {
      bestLookupRate = rate;
      lookupBestGatewayId = g.gateway_id;
    }
  }

  // chosen_lookup_rate: lookup rate of the recommended (final pick) gateway.
  const chosenLookupRate = pickedLookup && typeof pickedLookup.approval_rate === 'number'
    ? pickedLookup.approval_rate : null;

  // regret: best_lookup_rate - chosen_lookup_rate (spec §5.2).
  const regret = (bestLookupRate != null && chosenLookupRate != null)
    ? bestLookupRate - chosenLookupRate : null;

  // ai_disagreed_with_lookup: 1 if AI and lookup picked different gateways.
  const aiDisagreedWithLookup = (lookupBestGatewayId != null && aiRecommendedGatewayId != null)
    ? (lookupBestGatewayId !== aiRecommendedGatewayId ? 1 : 0)
    : null;

  // ai_score_spread: top-1 minus top-2 AI score.
  const sortedAiScores = Array.from(scoreByGw.values())
    .filter(s => Number.isFinite(s))
    .sort((a, b) => b - a);
  const aiScoreSpread = sortedAiScores.length >= 2
    ? sortedAiScores[0] - sortedAiScores[1] : null;

  // lookup_score_spread: top-1 minus top-2 lookup rate.
  const lookupRates = eligibleGateways
    .map(g => { const lk = lookupForProc(canonProc(g.processor_name)); return lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : null; })
    .filter(r => r != null)
    .sort((a, b) => b - a);
  const lookupScoreSpread = lookupRates.length >= 2
    ? lookupRates[0] - lookupRates[1] : null;

  // confidence_tier: qualitative label for the decision.
  let confidenceTier;
  if (lookupHasData === 0 && aiScore == null) {
    confidenceTier = 'no_data';
  } else if (aiScore != null && aiScore >= 0.70 && aiScoreSpread != null && aiScoreSpread >= 0.05) {
    confidenceTier = 'high';
  } else if (aiScore != null && aiScore >= 0.50) {
    confidenceTier = 'medium';
  } else {
    confidenceTier = 'low';
  }

  // would_have_approved_binary: Stage-0 proxy (1 if chosen_lookup_rate > 0.50).
  const wouldHaveApprovedBinary = chosenLookupRate != null
    ? (chosenLookupRate > 0.50 ? 1 : 0) : null;

  // expected_approval: COALESCE(ai_score, chosen_lookup_rate) — EAR input.
  const expectedApproval = aiScore != null ? aiScore : chosenLookupRate;

  // Transaction-level context from binRow + request time.
  const hourOfDay = now.getUTCHours();
  const dayOfWeek = now.getUTCDay();
  const binAvgAmt = queryOneSql(
    'SELECT AVG(order_total) AS avg FROM orders WHERE client_id = ? AND cc_first_6 = ? AND order_total > 0',
    [clientId, bin]
  );
  const amountVsBinAvg = (amt != null && binAvgAmt && binAvgAmt.avg > 0)
    ? amt / binAvgAmt.avg : null;

  // Build candidate_pool_json — one row per ELIGIBLE gateway_id (no duplicates).
  const pool = eligibleGateways.map(g => {
    const pc = canonProc(g.processor_name);
    const lk = lookupForProc(pc);
    const isDownranked = (filterResult.downranked || []).some(e => e.processor === pc);
    return {
      gateway_id: g.gateway_id,
      processor: g.processor_name,
      acquiring_bank: g.bank_name,
      lookup_rate: lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : null,
      lookup_sample_size: lk && typeof lk.sample_size === 'number' ? lk.sample_size : null,
      lookup_action: lk && lk.action ? lk.action : (isDownranked ? 'soft_downrank' : null),
      ai_score: scoreByGw.has(g.gateway_id) ? scoreByGw.get(g.gateway_id) : null,
      is_pick: g.gateway_id === pickedGatewayId ? 1 : 0,
      is_ai_top: g.gateway_id === aiTopGatewayId ? 1 : 0,
      is_warming_up: g.is_warming_up ? 1 : 0,
      is_exploration: g.is_exploration ? 1 : 0,
    };
  });
  // Rank by ai_score desc (or lookup_rate desc when no AI).
  pool.sort((a, b) => {
    const aKey = a.ai_score != null ? a.ai_score : (a.lookup_rate != null ? a.lookup_rate : -Infinity);
    const bKey = b.ai_score != null ? b.ai_score : (b.lookup_rate != null ? b.lookup_rate : -Infinity);
    return bKey - aKey;
  });
  // P3.3: Enrich pool with spread_vs_best, spread_vs_avg, rank_position.
  const aiScoresInPool = pool.map(e => e.ai_score).filter(s => s != null);
  const maxAiScore = aiScoresInPool.length > 0 ? Math.max(...aiScoresInPool) : null;
  const avgAiScore = aiScoresInPool.length > 0 ? aiScoresInPool.reduce((a, b) => a + b, 0) / aiScoresInPool.length : null;
  pool.forEach((entry, i) => {
    entry.rank_position = i;
    entry.spread_vs_best = entry.ai_score != null && maxAiScore != null ? entry.ai_score - maxAiScore : null;
    entry.spread_vs_avg = entry.ai_score != null && avgAiScore != null ? entry.ai_score - avgAiScore : null;
  });

  // Add hard-excluded gateways to the pool for full transparency (one row per gateway_id).
  for (const g of gateways) {
    const pc = canonProc(g.processor_name);
    if (!hardExcludedSet.has(pc)) continue;
    const lk = lookupByProc.get(pc);
    pool.push({
      gateway_id: g.gateway_id,
      processor: g.processor_name,
      acquiring_bank: g.bank_name,
      lookup_rate: lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : null,
      lookup_sample_size: lk && typeof lk.sample_size === 'number' ? lk.sample_size : null,
      lookup_action: 'hard_exclude',
      ai_score: null,
      is_pick: 0,
      rank_position: null,
      spread_vs_best: null,
      spread_vs_avg: null,
    });
  }

  const featureSnapshot = {
    bin_row: binRow,
    amount: amt,
    product_id: pid,
    sales_type: salesType,
    request_at: now.toISOString(),
    hour_of_day: now.getUTCHours(),
    day_of_week: now.getUTCDay(),
    daemon_payload: daemonPayload,
    override_diagnostics: overrideDiag,
    concentration: {
      by_processor: concByProc,
      by_mid: concByMid,
    },
  };

  // --- Experiment assignment (deterministic, logging-only in Release 1) ---
  const exp = assignExperiment({ clientId, emailHash, pickedGatewayId, reason });

  return writeAndRespond(res, {
    clientId, bin, amount: amt, product_id: pid, emailHash, salesType,
    recommendedGatewayId: pickedGatewayId,
    recommendedProcessor: pickedProc,
    confidence,
    reason,
    poolJson: JSON.stringify(pool),
    lookupHasData,
    lookupBestRate,
    daemonTimedOut: daemonResult.timed_out ? 1 : 0,
    daemonLatencyMs: daemonResult.latency_ms != null ? daemonResult.latency_ms : null,
    featureSnapshot,
    modelVersion: daemonResult.model_version || DEFAULT_MODEL_VERSION,
    aiScore,
    aiRecommendedGatewayId,
    wouldHaveApproved,
    startedAt,
    // P3 fields
    merchantVertical,
    issuerBank: binRow.issuer_bank,
    cardType: binRow.card_type,
    cardBrand: binRow.card_brand,
    isPrepaid: binRow.is_prepaid ? 1 : 0,
    hourOfDay,
    dayOfWeek,
    amountVsBinAvg,
    lookupBestGatewayId,
    aiDisagreedWithLookup,
    confidenceTier,
    aiScoreSpread,
    lookupScoreSpread,
    bestLookupRate,
    chosenLookupRate,
    regret,
    wouldHaveApprovedBinary,
    expectedApproval,
    // Experiment fields
    experimentId: exp.experimentId,
    experimentVariant: exp.experimentVariant,
    selectedBy: exp.selectedBy,
    experimentBucket: exp.experimentBucket,
    experimentSkipReason: exp.experimentSkipReason,
    wouldForceGateway: exp.wouldForceGateway,
    forceGatewayRequested: exp.forceGatewayRequested,
    forceGatewayResult: exp.forceGatewayResult,
    experimentAssignmentKey: exp.experimentAssignmentKey,
  }, 200);
});

/**
 * Synchronously inserts the shadow_decisions row and returns the HTTP response.
 * Kept as a helper so the catastrophic-no-candidates path and the happy path
 * share one write implementation.
 */
function writeAndRespond(res, d, httpStatus) {
  const shadowId = crypto.randomUUID();
  const now = new Date();
  const latencyMs = Date.now() - d.startedAt;

  try {
    runSql(
      `INSERT INTO shadow_decisions (
         shadow_id, client_id, bin, amount, product_id, email_hash, sales_type,
         recommended_gateway_id, recommended_processor, confidence, reason,
         candidate_pool_json, lookup_has_data, lookup_best_rate,
         daemon_timed_out, daemon_latency_ms, feature_snapshot_json, model_version,
         request_received_at, response_sent_at, latency_ms_server,
         ai_model_version, ai_score, ai_recommended_gateway_id, ai_scored_at,
         would_have_approved,
         merchant_vertical, issuer_bank, card_type, card_brand, is_prepaid,
         hour_of_day, day_of_week, amount_vs_bin_avg,
         lookup_best_gateway_id, ai_disagreed_with_lookup, confidence_tier,
         ai_score_spread, lookup_score_spread, best_lookup_rate, chosen_lookup_rate,
         regret, would_have_approved_binary, expected_approval,
         experiment_id, experiment_variant, selected_by, experiment_bucket,
         experiment_skip_reason, would_force_gateway, force_gateway_requested,
         force_gateway_result, experiment_assignment_key
       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [
        shadowId, d.clientId, d.bin, d.amount, d.product_id, d.emailHash, d.salesType,
        d.recommendedGatewayId, d.recommendedProcessor, d.confidence, d.reason,
        d.poolJson, d.lookupHasData, d.lookupBestRate,
        d.daemonTimedOut, d.daemonLatencyMs, JSON.stringify(d.featureSnapshot), d.modelVersion,
        new Date(d.startedAt).toISOString(), now.toISOString(), latencyMs,
        d.aiScore != null ? d.modelVersion : null, d.aiScore, d.aiRecommendedGatewayId,
        d.aiScore != null ? now.toISOString() : null,
        d.wouldHaveApproved,
        d.merchantVertical, d.issuerBank, d.cardType, d.cardBrand, d.isPrepaid,
        d.hourOfDay, d.dayOfWeek, d.amountVsBinAvg,
        d.lookupBestGatewayId, d.aiDisagreedWithLookup, d.confidenceTier,
        d.aiScoreSpread, d.lookupScoreSpread, d.bestLookupRate, d.chosenLookupRate,
        d.regret, d.wouldHaveApprovedBinary, d.expectedApproval,
        d.experimentId, d.experimentVariant || 'none', d.selectedBy || 'beast',
        d.experimentBucket, d.experimentSkipReason,
        d.wouldForceGateway ? 1 : 0, d.forceGatewayRequested ? 1 : 0,
        d.forceGatewayResult, d.experimentAssignmentKey,
      ]
    );
  } catch (err) {
    // Write failure shouldn't break checkout — still return a decision.
    // (The in-memory decision is already chosen.)
    console.error('[api/route] shadow_decisions insert failed:', err.message);
  }

  return res.status(httpStatus || 200).json({
    shadow_id: shadowId,
    gateway_id: d.recommendedGatewayId,
    processor: d.recommendedProcessor,
    confidence: d.confidence,
    reason: d.reason,
    latency_ms: latencyMs,
    experiment_id: d.experimentId || null,
    experiment_variant: d.experimentVariant || 'none',
    selected_by: d.selectedBy || 'beast',
    would_force_gateway: d.wouldForceGateway === 1,
    force_gateway: false,  // Release 1: always false — no live routing
  });
}

/**
 * POST /api/route/submit-timing
 * Called from the PHP extension's form-submit hook via navigator.sendBeacon.
 * Updates the shadow_decisions row with client-side latency measurement.
 */
router.post('/submit-timing', (req, res) => {
  const { shadow_id, latency_ms_client, arrived_before_submit } = req.body || {};
  if (typeof shadow_id !== 'string' || shadow_id.length === 0) {
    return res.status(400).json({ error: 'shadow_id required' });
  }
  const lat = latency_ms_client != null ? parseInt(latency_ms_client, 10) : null;
  const arrived = arrived_before_submit ? 1 : 0;

  try {
    const result = runSql(
      `UPDATE shadow_decisions
          SET latency_ms_client = ?,
              arrived_before_submit = ?,
              submit_timing_at = CURRENT_TIMESTAMP
        WHERE shadow_id = ?
          AND client_id = ?`,
      [lat, arrived, shadow_id, req.apiClientId]
    );
    return res.json({ ok: true, updated: result && result.changes ? result.changes : 0 });
  } catch (err) {
    console.error('[api/route/submit-timing] update failed:', err.message);
    return res.status(500).json({ error: 'update_failed' });
  }
});

module.exports = router;
