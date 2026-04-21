# BinRoute AI Expert Briefing

A concise technical briefing on the BinRoute routing-AI system, its current
state, and the open questions we'd like an external expert to review.

Read time: ~10 minutes.

---

## 1. What we're trying to do

**Business goal:** improve credit-card approval rates on e-commerce checkouts
for direct-response marketers, by choosing the best payment processor (gateway)
per transaction based on the 6-digit BIN (Bank Identification Number) and
contextual features.

**Current deployment target:** one client only — "Kytsan" (a supplements brand).
Other historical clients are winding down their businesses, leaving Kytsan as
the single active AI routing target. Their data remains available for training
but they will not be routed.

**Expected lift:** +10-14pp in approval rate vs. current round-robin /
rules-based routing (Beast Insights). This matches our offline backtest
estimates (see §4).

**Strategic structure:** scale-on-lift flywheel. If AI produces measurable
approval-rate lift on Kytsan in weeks 1-6 of shadow mode, Kytsan scales ad
spend / volume (week 6-8), producing more data, producing better specialization,
producing more lift. We get paid on that lift.

---

## 2. The problem in one paragraph

Each credit-card transaction at checkout can be routed to one of ~10 active
gateways (merchant accounts with different acquiring banks, processor
identities, and BIN-level approval patterns). Approval rates vary dramatically
— some BINs approve 70% on Gateway A and 20% on Gateway B, others the reverse.
Historical routing was round-robin (no intelligence). Beast Insights, a
competing product, was switched on ~2-3 weeks ago with simple BIN rules. We
want to replace Beast with an AI model that learns the full BIN × processor ×
context approval surface and routes accordingly.

---

## 3. System architecture

### Runtime flow (target state, Stage 0 shadow today)

```
Customer enters BIN at checkout
    ↓
PHP extension → POST /api/route  (Node server, port 3001 prod)
    ↓
api-key auth middleware
    ↓
/api/route endpoint:
  1. BIN lookup in local bin_lookup table → {issuer_bank, card_brand, card_type, is_prepaid}
  2. Pull active gateways for client
  3. Lookup filter: hard-exclude / soft-downrank processors via 4 lookup tables
     (initial, upsell, rebill, rebill_salvage) — built from historical approval rates
  4. Build feature vector (22 features) per remaining candidate gateway
  5. POST to Python scoring daemon (loaded model in memory, 127.0.0.1:5001)
  6. Pick highest-scoring candidate among survivors
  7. 10% random exploration override for flagged new processors
  8. Log decision to shadow_decisions table (UUID + full feature snapshot + candidate pool)
  9. Return {gateway_id, shadow_id, confidence} to PHP in ~30-40ms server-side
    ↓
PHP appends "BinRoute_shadow: id=<uuid> rec_gw=<id>" to customNotes
  (Stage 0 shadow: does NOT set forceGatewayId; Beast still routes live traffic)
    ↓
Order flows to Sticky.io, gets approved/declined, syncs back daily
    ↓
Post-sync reconciler matches shadow_id → fills actual_gateway_id, actual_outcome, would_match
    ↓
Shadow report: lift = mean(would_have_approved) - mean(actual_approved)
```

### Fail-safe properties

- 30ms hard timeout on Node → Python daemon. Timeout → fall back to lookup-only pick.
- Daemon down → lookup-only pick in ~3ms.
- No eligible gateway → return first active gateway (always-attempt rule).
- API key auth bcrypt, 60s cache, revocable.

### Stages

| Stage | Scope |
|---|---|
| 0 | Shadow mode — observe only, never set forceGatewayId (CURRENT) |
| 1 | Live routing on Kytsan at current volume (~100-200 tx/week) |
| 2 | Scale Kytsan volume if lift holds |
| 3+ | (Deferred indefinitely — no other active clients) |

---

## 4. Training methodology + results

### Dataset

- **112,329 initial-main-attempt transactions** across 5 clients (round-robin era, pre-Beast)
- Time-based 80/20 split (train: 89,863; test: 22,466). No random split — no customer leakage.
- Filters applied: test BINs removed, system-decline rows removed, excluded gateways (`exclude_from_analysis=1` — e.g. Payfac, non-routable) removed, customer-input declines removed.

### Algorithm selection (evidence-based, 118 experiments)

LightGBM chosen for initials over CatBoost, XGBoost, LogReg:

- LightGBM: 0.7084 AUC
- CatBoost (native categoricals): 0.7001 AUC
- XGBoost: slightly below

For rebills, CatBoost won (+34pp over LightGBM) because CatBoost's native
categorical handling learns `last_approved_processor == current_processor`
identity correctly — but we discovered this creates a "stay" routing bias
(see §4.4 below).

### Features (22 total)

**Categorical:**
`processor_name, acquiring_bank, mcc_code, issuer_bank_grouped, card_brand,
card_type, billing_state, client_id`

**Numerical (approximate feature importance from `four_model_results.json`):**

| Feature | Importance | Type |
|---|---|---|
| mid_age_days | 10.67% | processor MID state |
| te_acquiring_bank | 9.68% | target-encoded bank approval rate |
| amount_vs_bin_avg | 9.18% | relative amount |
| mid_velocity_daily | 9.04% | MID state |
| bin_velocity_weekly | 8.33% | BIN state |
| billing_state | 6.92% | geography |
| issuer_bank_grouped | 6.89% | issuer identity |
| bin_proc_approval_rate | 6.52% | BIN × processor historical rate |
| bin_approval_rate | 6.25% | BIN overall rate |
| bin_approval_7d | 1.5-2% | BIN recent rate |
| bin_approval_30d | 1.5-2% | BIN recent rate |
| processor_name | 1.69% | processor identity (low) |
| (others) | <2% each | — |

Feature ablation confirmed every feature contributes positively. Features that
hurt were removed (order_total — correlated; offer_name — 0% importance; month
— overfit -2.1pp; cross-feature interactions — too sparse for 112K rows).

### Results — AUC ceiling: 0.71

**Initial model:** 0.7084 AUC on 22,466-row holdout (time-based split).

Per-client AUC:

| Client | AUC | Volume share |
|---|---|---|
| C1 (Kytsan) | 0.68 | ~20% |
| C2 | 0.74 | — |
| C3 | 0.64 | — |
| C4 | 0.61 | — |
| C5 | 0.73 | — |

**Calibration verified on live holdout** (Kytsan data): predicted probabilities
match actual approval rates within 2-5pp across all confidence bands.

### Routing strategy backtest — 4-way comparison on 22,466 test transactions

| Strategy | Approval rate | Lift vs RR |
|---|---|---|
| Historical round-robin | 50.83% | — |
| Lookup table only (hardcoded BIN × processor rules) | 58.75% | +7.9pp |
| AI model only | 62.25% | +11.4pp |
| **Lookup-enhanced AI hybrid** | **67.31%** | **+16.5pp** |

Realistic production estimate (after accounting for lookup validation gap):
**+10-12pp**.

### Rebill model — additional validation (separate experiment)

For rebills (second+ charges on a recurring customer), we discovered that
high-AUC models (CatBoost at 0.92) learned an identity shortcut: "if the
processor name matches `last_approved_processor`, predict approval." This
correctly predicts approval on stayed rows but gives bad routing advice
(never switches).

**Model B redesign:** drop raw processor names as categoricals; use only
computed RATES (`target_proc_rebill_rate`, `is_same_as_initial`, etc.). This
produced:

- Model A (processor names): AUC 0.9243, routing approval 44.46%
- Model B (rates only): AUC 0.8808, routing approval **44.61% (+16.40pp)**
- Model C (blended): worst of both

Lower AUC but better routing. Confirmed: **AUC is the wrong proxy for routing
quality**.

---

## 5. Known bugs / items we're actively fixing

### Inference-time feature parity (HIGH IMPACT)

The scoring daemon currently sends these features to the model with degraded
values at inference:

| Feature | Training value | Inference value | Importance |
|---|---|---|---|
| `mid_velocity_daily` | Real computed value | **0 (zeroed)** | 9.04% |
| `bin_velocity_weekly` | Real computed value | **0 (zeroed)** | 8.33% |
| `customer_history_on_proc` | Real computed value | **0 (zeroed)** | ~5% |
| `bin_approval_7d` | Real 7-day windowed rate | **falls back to all-time BIN rate** | 1.5-2% |
| `bin_approval_30d` | Real 30-day windowed rate | **falls back to all-time BIN rate** | 1.5-2% |

Combined: ~25% of the model's feature importance is either zeroed or proxied
at inference. The model was trained on one distribution and deployed on a
different one. This is a genuine bug, not a design choice — we know the values
exist; we just haven't wired them into the daemon's per-request cache.

**Estimated impact:** recovering 2-3pp of the theoretical +16.5pp backtest.
Fix priority: HIGH, tractable (~2-3 days of work).

### Warming-MID substitution (NEW, added this session)

When a new MID is added (age < 30 days or flagged `is_warming_up=1`), the AI
was systematically down-ranking it because `mid_age_days` is the #1 feature.
We added a substitution: replace `mid_age_days` + `mid_velocity_daily` with
the median of mature peers on the same (processor, bank). Verified
mechanically (score shifts as expected) but **not yet validated in production**
on real new-MID onboarding events.

### 10% exploration override (NEW, added this session)

When a new processor is added (gateway flagged `is_exploration=1`), 10% of
traffic gets routed to it regardless of AI score — so we collect signal on it
while the AI is still blind. Spec called for decay (15% → 10% → 5% → 2%); we
implemented flat 10%. Decay not yet built.

---

## 6. Open questions for the AI expert

### Question 1: Is 0.71 really the initial-model AUC ceiling?

We ran 118 experiments (algorithm tournament, feature additions, ablations,
hyperparameter sweeps) and hit a ceiling at 0.71. We believe this is because
cold-card prediction (no prior customer history) is inherently hard — the
only signals are BIN, amount, time, geography.

**Question:** Are there approaches we missed that could push past 0.71?
Candidates we're considering:
- Pairwise / listwise ranking loss (LambdaRank) instead of pointwise
  classification
- Two-stage model: first stage predicts "will this approve anywhere" (fraud /
  closed card), second stage ranks processors only for viable transactions
- Cost-sensitive loss (treat hard declines differently from soft declines)
- Feature engineering: cascade success history for BIN, retry-attempt number,
  amount bucketing, time-since-last-transaction, geographic clustering

We already validated ranking-based for rebills (Model B). Is it worth
replicating for initials? Estimated lift from shifting pointwise → ranking?

### Question 2: Target encoding methodology

Our top-2 feature by importance is `te_acquiring_bank` at 9.68%.
Target-encoded features are notorious for leakage if not done out-of-fold.

**We don't know from memory whether this was computed out-of-fold on
training data or naively on the full dataset.** Need to audit.

**Question:** What's your recommended validation protocol to detect target
encoding leakage post-hoc? Do we retrain with confirmed out-of-fold encoding
and compare AUC?

### Question 3: Pooled vs per-client for single-client deployment

We have 112K training rows across 5 clients. Kytsan (our only live target) is
~20% of those (~22K rows). Per-client AUC:

- Pooled model AUC on Kytsan: 0.68
- Pooled AUC overall: 0.68

`client_id` is already a categorical feature, so the tree model can (and
apparently does) specialize per-client where the signal supports it.

**Our current recommendation:** keep pooled training. Don't specialize until
Kytsan's own shadow/live data grows to ~50-100K rows (6+ months out).

**Question:** Do you agree? Is there evidence you'd look for to justify
specialization sooner? Is there a better middle ground (e.g., client-weighted
sample weights, mixture-of-experts, transfer learning from pooled → Kytsan)?

### Question 4: Evaluation metric for routing

AUC measures ranking quality on a binary classifier, but our real metric is
"approval rate lift when we use the model's top pick." We've seen cases where
higher-AUC models route WORSE (Model A vs Model B on rebills).

**Question:** What's your recommended primary metric for the production
gate? Options we've considered:
- Aggregate approval rate uplift on backtest (what we use now)
- NDCG@1 or similar ranking metric
- Regret (approval rate of picked vs approval rate of best possible pick)
- Per-segment approval rate with floor constraints (no segment drops by >Xpp)

### Question 5: Counterfactual bias in our backtest

Our +16.5pp backtest number uses `would_have_approved` computed from historical
rates for "what the AI would have picked." But those historical rates are from
orders the historical router (round-robin, pre-Beast) sent to that gateway —
not from uniform random exploration. This is a selection-bias issue.

Beast is only 2-3 weeks live, so we have limited post-Beast data, but ~5%+ of
recent rows are Beast-influenced. For the pre-Beast majority, round-robin gave
reasonable coverage across BIN × gateway pairs but wasn't uniform.

**Question:** How would you estimate the true counterfactual lift
pre-deployment? Options:
- Inverse-propensity weighting — but we don't have explicit propensity scores
  from historical routing
- Doubly-robust estimators — same issue
- Accept the backtest overestimate and wait for live A/B data
- Use shadow mode's reconciled rows (once we have volume) as ground truth

### Question 6: Stage 0 shadow mode design review

We ship shadow mode first: the routing decision is logged but never applied.
Customer → Beast routes the order → order imports → reconciler fills in
"actual gateway" and "actual outcome" → we compare.

**Two known issues:**
1. Shadow traffic IS Beast's routing distribution, not a random/controlled
   allocation. Our "would have done better" estimate is vs Beast, but we can't
   observe what AI's pick would have ACTUALLY done — we use its historical
   approval rate as a proxy.
2. Exploration picks (10%) are NOT applied — they're logged but Beast still
   routes the actual traffic. So exploration collects no real signal in
   Stage 0, only in Stage 1.

**Question:** Is Stage 0 shadow mode worth it given these limitations, or
should we skip to Stage 1 with a small % traffic allocation (say 5%) and a
kill switch? The advantages of the smaller Stage 1 over our current Stage 0:
- Exploration actually collects signal
- Counterfactual bias goes away (we observe what OUR pick did)
- Faster iteration

### Question 7: Inference-time feature engineering priority

Given our known 25%-of-importance degradation at inference due to zeroed/proxied
features, should we:
1. Fix the inference pipeline to compute these features correctly at request
   time (~2-3 days work, tractable)?
2. Retrain the model WITHOUT these features so training-inference distributions
   match (shorter-term fix, sacrifices ~2-3pp of potential)?
3. Both: ship option 2 now as V1.1, option 1 as V2?

---

## 7. What we'd value most from the review

In order of importance:

1. **Is our training methodology sound?** Look for leakage, bias, evaluation
   issues.
2. **Is the Lookup+AI hybrid architecture correct?** Or should we unify into a
   single model with rate features?
3. **Is pointwise classification the wrong formulation for initials?**
   Listwise ranking worked for rebills (+16.4pp). Should we do the same for
   initials, given we're Kytsan-only now?
4. **Counterfactual evaluation methodology.** We're making a 10-14pp lift
   claim based on potentially-biased historical rates. How do we de-bias?
5. **Feature engineering priorities.** Which 3 additions would you expect to
   move the needle most?

---

## 8. Code / data we can share on request

- `scripts/ml/train_four_models.py` — training pipeline
- `scripts/ml/scoring_daemon.py` — production inference daemon
- `scripts/ml/backtest_routing_strategies.py` — the backtest that produced the
  +16.5pp number
- `scripts/ml/compare_rebill_models.py` — rebill Model A/B/C comparison
- `data/models/four_model_results.json` — feature importance + CV metrics
- Anonymized sample of `tx_features` table (22 features × 100K rows) — on
  request, with PII stripped
- Anonymized `shadow_decisions` sample once we have live shadow data

Happy to share any of these for deeper review.

---

## Contact

Client domain: [redacted]
Project lead: [your name]
Technical stack: Node.js / better-sqlite3 / LightGBM (Python) / Flask daemon
Repo: BinRoute (internal)
