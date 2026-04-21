# AI Fix Baseline — Pre-Parity Snapshot

Captured: 2026-04-21
Git tag: `pre-ai-fixes-2026-04-16` (commit 460fa4d)
DB snapshot: `data/binroute.db.pre-ai-fixes`
Source: `data/models/four_model_results.json` (trained 2026-04-14)

## Training Data

- Source: `transaction_attempts` where `feature_version >= 3`
- Total attempts: 295,110
- Train/test split: 80/20 (time-based)

## Initial Model (target of P1 fixes)

- **Algorithm:** LightGBM
- **AUC:** 0.6598
- **F1:** 0.5114
- **Rows:** 31,115 (37.5% approval rate)
- **Features (22):**

| # | Feature | Importance | Notes |
|---|---------|-----------|-------|
| 1 | mid_age_days | 10.67% | |
| 2 | te_acquiring_bank | 9.68% | P2 audit target |
| 3 | amount_vs_bin_avg | 9.18% | |
| 4 | mid_velocity_daily | 9.04% | **P1: zeroed at inference** |
| 5 | bin_velocity_weekly | 8.33% | **P1: zeroed at inference** |
| 6 | billing_state | 6.92% | |
| 7 | issuer_bank_grouped | 6.89% | |
| 8 | bin_proc_approval_rate | 6.52% | |
| 9 | bin_approval_rate | 6.25% | |
| 10 | hour_of_day | 6.01% | |
| 11 | bin_approval_7d | 4.14% | **P1: falls back to all-time** |
| 12 | mcc_code | 4.01% | |
| 13 | day_of_week | 2.81% | |
| 14 | processor_name | 1.69% | |
| 15 | card_brand | 1.58% | |
| 16 | is_prepaid | — | (below top 15) |
| 17 | card_type | — | |
| 18 | client_id | — | |
| 19 | customer_history_on_proc | — | **P1: zeroed at inference, DROP** |
| 20 | bin_approval_30d | — | **P1: falls back to all-time** |
| 21 | is_near_payday | — | |
| 22 | acquiring_bank | — | (categorical) |

### Degraded features at inference (~25% of importance)

| Feature | Training | Inference | Importance |
|---------|----------|-----------|-----------|
| mid_velocity_daily | Real value | 0 (zeroed) | 9.04% |
| bin_velocity_weekly | Real value | 0 (zeroed) | 8.33% |
| customer_history_on_proc | Real value | 0 (zeroed) | ~2-5% |
| bin_approval_7d | Real 7d window | All-time fallback | 4.14% |
| bin_approval_30d | Real 30d window | All-time fallback | ~1-2% |

## Other Models (not targets of this fix round)

| Model | Algorithm | AUC | Rows |
|-------|-----------|-----|------|
| Upsell | LightGBM | 0.7749 | 124,855 |
| Cascade | Logistic Regression | 0.9132 | 22,537 |
| Rebill (Main) | CatBoost | 0.9063 | 34,139 |
| Rebill Salvage | LightGBM | 0.6568 | 82,464 |

## Routing Backtest (from AI_EXPERT_BRIEFING.md)

| Strategy | Approval Rate | Lift vs RR |
|----------|-------------|------|
| Historical round-robin | 50.83% | — |
| Lookup table only | 58.75% | +7.9pp |
| AI model only | 62.25% | +11.4pp |
| **Lookup-enhanced AI** | **67.31%** | **+16.5pp** |

Realistic production estimate: +10-12pp (after validation gap adjustment).

## Post-Fix Comparison Target

After P1 (inference parity fix + retrain):
- Accept if AUC within +/-1pp of 0.6598
- Drop > 1pp → investigate before advancing
- Expect shift in `predict_proba` on BINs with real 7d/30d history
