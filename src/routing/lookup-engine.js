/**
 * Lookup Engine — tiered query functions for all 4 routing lookup tables.
 *
 * Tables:
 *   1. Main Initial (3D): issuer × card_type_merged × target
 *   2. Upsell (4D): issuer × card_type_merged × initial_processor × target
 *   3. Rebill First-Attempt (4D): issuer × card_type_merged × initial_proc × target
 *   4. Rebill Salvage (4D primary + 5D optional): decline × issuer × failed × target [+ card_type_merged]
 *
 * Execution rules:
 *   - Never reduce candidates below 2 (fallback to model)
 *   - Lookup is filter + assist, not full decision system
 *   - Always has fallback: lookup → model
 *   - Client-specific overrides when 50+ rows exist
 */

const fs = require('fs');
const path = require('path');

const MODELS_DIR = path.join(__dirname, '..', '..', 'data', 'models');

// ──────────────────────────────────────────────
// Table cache — loaded once, reused
// ──────────────────────────────────────────────

let tables = null;

function loadTables() {
  if (tables) return tables;

  const load = (filename) => {
    const filePath = path.join(MODELS_DIR, filename);
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  };

  tables = {
    initial: load('initial_lookup.json'),
    upsell: load('upsell_lookup.json'),
    rebill: load('rebill_first_attempt_lookup.json'),
    salvage: load('rebill_salvage_lookup.json'),
  };

  return tables;
}

function reloadTables() {
  tables = null;
  return loadTables();
}

// ──────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────

function cardTypeMerged(cardType, isPrepaid) {
  if (isPrepaid === 1 || isPrepaid === '1' || isPrepaid === true) return 'PREPAID';
  return cardType || null;
}

function lookupEntry(tier, key, clientId, clientOverrides) {
  if (!tier) return null;
  const entry = tier[key];
  if (!entry) return null;
  if (clientId && clientOverrides && clientOverrides[key] && clientOverrides[key][clientId]) {
    return { ...clientOverrides[key][clientId], source: 'client_override' };
  }
  return { ...entry, source: 'cross_client' };
}

/**
 * Generic candidate filter — applies lookup to a list of processors.
 * Never reduces below 2 candidates (safeguard).
 */
function _filterCandidates(tableName, candidates, lookupFn) {
  const results = candidates.map(proc => {
    const lookup = lookupFn(proc);
    return { processor: proc, lookup };
  });

  const excluded = results.filter(r => r.lookup?.action === 'hard_exclude');
  const downranked = results.filter(r => r.lookup?.action === 'soft_downrank');
  const remaining = results.filter(r => r.lookup?.action !== 'hard_exclude').map(r => r.processor);

  const finalCandidates = remaining.length >= 2 ? remaining : candidates;
  const safeguardTriggered = remaining.length < 2 && excluded.length > 0;

  return {
    candidates: finalCandidates,
    excluded,
    downranked,
    log: {
      table: tableName,
      input_candidates: candidates.length,
      after_lookup: finalCandidates.length,
      hard_excluded: excluded.length,
      soft_downranked: downranked.length,
      safeguard_triggered: safeguardTriggered,
    },
  };
}

// ──────────────────────────────────────────────
// TABLE 1: MAIN INITIAL (3D)
// issuer × card_type_merged × target
// Fallback: 3D → 2D (issuer × target) → null
// ──────────────────────────────────────────────

function queryInitial(issuer, cardType, isPrepaid, targetProcessor, clientId) {
  const t = loadTables();
  if (!t.initial) return null;
  const ctm = cardTypeMerged(cardType, isPrepaid);

  if (issuer && ctm) {
    const entry = lookupEntry(t.initial.tier_3d, `${issuer}|${ctm}|${targetProcessor}`, clientId, t.initial.client_overrides_3d);
    if (entry) return { ...entry, tier: '3D' };
  }
  if (issuer) {
    const entry = lookupEntry(t.initial.tier_2d, `${issuer}|${targetProcessor}`);
    if (entry) return { ...entry, tier: '2D' };
  }
  return null;
}

function filterInitialCandidates(issuer, cardType, isPrepaid, candidates, clientId) {
  return _filterCandidates('initial', candidates, proc =>
    queryInitial(issuer, cardType, isPrepaid, proc, clientId)
  );
}

// ──────────────────────────────────────────────
// TABLE 2: UPSELL (4D)
// issuer × card_type_merged × initial_processor × target
// Fallback: 4D → 3D (issuer × init_proc × target) → 2D (issuer × target) → null
// ──────────────────────────────────────────────

function queryUpsell(issuer, cardType, isPrepaid, initialProcessor, targetProcessor, clientId) {
  const t = loadTables();
  if (!t.upsell) return null;
  const ctm = cardTypeMerged(cardType, isPrepaid);

  if (issuer && ctm && initialProcessor) {
    const entry = lookupEntry(t.upsell.tier_4d, `${issuer}|${ctm}|${initialProcessor}|${targetProcessor}`, clientId, t.upsell.client_overrides_4d);
    if (entry) return { ...entry, tier: '4D' };
  }
  if (issuer && initialProcessor) {
    const entry = lookupEntry(t.upsell.tier_3d, `${issuer}|${initialProcessor}|${targetProcessor}`);
    if (entry) return { ...entry, tier: '3D' };
  }
  if (issuer) {
    const entry = lookupEntry(t.upsell.tier_2d, `${issuer}|${targetProcessor}`);
    if (entry) return { ...entry, tier: '2D' };
  }
  return null;
}

function filterUpsellCandidates(issuer, cardType, isPrepaid, initialProcessor, candidates, clientId) {
  return _filterCandidates('upsell', candidates, proc =>
    queryUpsell(issuer, cardType, isPrepaid, initialProcessor, proc, clientId)
  );
}

// ──────────────────────────────────────────────
// TABLE 3: REBILL FIRST-ATTEMPT (4D)
// issuer × card_type_merged × initial_processor × target
// Fallback: 4D → 3D → 2D → null
// Scope: C1-C2 only. C3+ returns null.
// ──────────────────────────────────────────────

function queryRebill(issuer, cardType, isPrepaid, initialProcessor, targetProcessor, cycle, clientId) {
  const t = loadTables();
  if (!t.rebill) return null;
  if (cycle >= 3) return null;
  const ctm = cardTypeMerged(cardType, isPrepaid);

  if (issuer && ctm && initialProcessor) {
    const entry = lookupEntry(t.rebill.tier_4d, `${issuer}|${ctm}|${initialProcessor}|${targetProcessor}`, clientId, t.rebill.client_overrides_4d);
    if (entry) return { ...entry, tier: '4D' };
  }
  if (issuer && initialProcessor) {
    const entry = lookupEntry(t.rebill.tier_3d, `${issuer}|${initialProcessor}|${targetProcessor}`);
    if (entry) return { ...entry, tier: '3D' };
  }
  if (issuer) {
    const entry = lookupEntry(t.rebill.tier_2d, `${issuer}|${targetProcessor}`);
    if (entry) return { ...entry, tier: '2D' };
  }
  return null;
}

function filterRebillCandidates(issuer, cardType, isPrepaid, initialProcessor, candidates, cycle, clientId) {
  if (cycle >= 3) {
    return {
      candidates,
      excluded: [],
      downranked: [],
      log: { table: 'rebill', input_candidates: candidates.length, after_lookup: candidates.length,
        hard_excluded: 0, soft_downranked: 0, safeguard_triggered: false, skipped: 'C3+' },
    };
  }
  return _filterCandidates('rebill', candidates, proc =>
    queryRebill(issuer, cardType, isPrepaid, initialProcessor, proc, cycle, clientId)
  );
}

// ──────────────────────────────────────────────
// TABLE 4: REBILL SALVAGE (4D + 5D optional)
// 5D: decline × issuer × card_type_merged × failed × target (if 35+)
// 4D: decline × issuer × failed × target
// 3D: issuer × failed × target
// ──────────────────────────────────────────────

function querySalvage(declineReason, issuer, cardType, isPrepaid, failedProcessor, targetProcessor) {
  const t = loadTables();
  if (!t.salvage) return null;
  const ctm = cardTypeMerged(cardType, isPrepaid);

  if (declineReason && issuer && ctm && failedProcessor) {
    const entry5d = lookupEntry(t.salvage.tier_5d_optional, `${declineReason}|${issuer}|${ctm}|${failedProcessor}|${targetProcessor}`);
    if (entry5d && entry5d.sample_size >= 35) return { ...entry5d, tier: '5D' };
  }
  if (declineReason && issuer && failedProcessor) {
    const entry = lookupEntry(t.salvage.tier_4d, `${declineReason}|${issuer}|${failedProcessor}|${targetProcessor}`);
    if (entry) return { ...entry, tier: '4D' };
  }
  if (issuer && failedProcessor) {
    const entry = lookupEntry(t.salvage.tier_3d, `${issuer}|${failedProcessor}|${targetProcessor}`);
    if (entry) return { ...entry, tier: '3D' };
  }
  return null;
}

function filterSalvageCandidates(declineReason, issuer, cardType, isPrepaid, failedProcessor, candidates) {
  return _filterCandidates('salvage', candidates, proc =>
    querySalvage(declineReason, issuer, cardType, isPrepaid, failedProcessor, proc)
  );
}

// ──────────────────────────────────────────────
// Exports
// ──────────────────────────────────────────────

module.exports = {
  loadTables,
  reloadTables,
  cardTypeMerged,
  queryInitial, filterInitialCandidates,
  queryUpsell, filterUpsellCandidates,
  queryRebill, filterRebillCandidates,
  querySalvage, filterSalvageCandidates,
};
