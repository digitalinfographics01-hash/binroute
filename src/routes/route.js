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
  const daemonCandidates = eligibleGateways.map(g => {
    const createdAt = g.gateway_created ? new Date(g.gateway_created) : null;
    const midAgeDays = createdAt
      ? Math.max(0, Math.floor((now - createdAt) / (1000 * 60 * 60 * 24)))
      : null;
    return {
      gateway_id: g.gateway_id,
      // send raw case — matches training data seen by the model's encoder
      processor_name: g.processor_name,
      acquiring_bank: g.bank_name,
      mcc_code: g.mcc_code,
      mid_age_days: midAgeDays,
      is_warming_up: g.is_warming_up ? 1 : 0,
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
  let pickedGw = null;
  let reason;
  let confidence = 0;

  if (eligibleGateways.length === 0) {
    // Catastrophic: lookup hard-excluded every processor AND the filter safeguard
    // didn't restore. Fall back to the first active gateway on file.
    pickedGw = gateways[0];
    reason = 'no_candidates';
    confidence = 0;
  } else if (eligibleGateways.length === 1) {
    pickedGw = eligibleGateways[0];
    reason = 'single_candidate';
    const lk = lookupForProc(canonProc(pickedGw.processor_name));
    confidence = lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : 0;
  } else if (daemonResult.ok && scoreByGw.size > 0) {
    // AI ranked per-gateway — pick the highest.
    let best = null;
    for (const g of eligibleGateways) {
      const s = scoreByGw.has(g.gateway_id) ? scoreByGw.get(g.gateway_id) : -Infinity;
      if (best == null || s > best.score) best = { gw: g, score: s };
    }
    pickedGw = best.gw;
    confidence = Number.isFinite(best.score) ? best.score : 0;

    // Would lookup alone have picked something different? Compare the AI winner's
    // processor to the processor with the top lookup approval rate.
    let lookupTopProcCanon = null; let lookupTopRate = -Infinity;
    for (const g of eligibleGateways) {
      const pc = canonProc(g.processor_name);
      const lk = lookupForProc(pc);
      const rate = lk && typeof lk.approval_rate === 'number' ? lk.approval_rate : null;
      if (rate != null && rate > lookupTopRate) { lookupTopRate = rate; lookupTopProcCanon = pc; }
    }
    const pickedProcCanon = canonProc(pickedGw.processor_name);
    if (lookupTopProcCanon && pickedProcCanon !== lookupTopProcCanon) {
      reason = 'ai_override';
    } else if ((filterResult.excluded && filterResult.excluded.length > 0)
            || (filterResult.downranked && filterResult.downranked.length > 0)) {
      reason = 'lookup_filtered';
    } else {
      reason = 'lookup_ai_hybrid';
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

  // At this point pickedGw = AI's / lookup's top pick. Capture it before any
  // exploration override so shadow_decisions.ai_recommended_gateway_id records
  // "what the model would have done" separately from the final engine decision.
  const aiTopGw = pickedGw;
  const aiTopGatewayId = aiTopGw ? aiTopGw.gateway_id : null;
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
  pool.forEach((entry, i) => { entry.rank = i; });

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
      rank: null,
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
  };

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
         would_have_approved
       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [
        shadowId, d.clientId, d.bin, d.amount, d.product_id, d.emailHash, d.salesType,
        d.recommendedGatewayId, d.recommendedProcessor, d.confidence, d.reason,
        d.poolJson, d.lookupHasData, d.lookupBestRate,
        d.daemonTimedOut, d.daemonLatencyMs, JSON.stringify(d.featureSnapshot), d.modelVersion,
        new Date(d.startedAt).toISOString(), now.toISOString(), latencyMs,
        d.aiScore != null ? d.modelVersion : null, d.aiScore, d.aiRecommendedGatewayId,
        d.aiScore != null ? now.toISOString() : null,
        d.wouldHaveApproved,
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
