"""
BinRoute AI — Initial Model V2 Feature Experiment

Tests 5 new feature categories on top of the V1 winner (grouped issuer + BIN features).
Then runs full ablation + correlation to find the optimal set.

New features:
  1. Target-encoded categoricals (processor, acquiring_bank, billing_state)
  2. Rolling BIN approval rates (7-day, 30-day)
  3. Amount relative to BIN average
  4. Processor × card_type affinity
  5. Time features (day_of_month, is_near_payday)

Usage: py -3 scripts/ml/test_initial_v2.py [--db=PATH]
"""

import os
import sys
import sqlite3
import warnings
import numpy as np
import pandas as pd
from collections import defaultdict

from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score

import lightgbm as lgb

warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
for arg in sys.argv:
    if arg.startswith('--db='):
        DB_PATH = arg.split('=', 1)[1]

TRAIN_RATIO = 0.80
ISSUER_MIN_COUNT = 500


# ── Feature definitions ──

CATEGORICAL_V1 = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank_grouped', 'card_brand', 'card_type',
    'billing_state', 'client_id',
]

NUMERICAL_V1 = [
    'is_prepaid', 'order_total', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'customer_history_on_proc',
    'bin_velocity_weekly', 'mid_age_days',
    'bin_approval_rate', 'bin_proc_approval_rate',
]

# New features to test
NEW_NUMERICAL = [
    # Target encoding
    'te_processor', 'te_acquiring_bank', 'te_billing_state',
    # Rolling BIN rates
    'bin_approval_7d', 'bin_approval_30d',
    # Amount relative
    'amount_vs_bin_avg',
    # Processor × card_type affinity
    'proc_cardtype_rate',
    # Time
    'day_of_month', 'is_near_payday',
]


def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            ta.client_id, ta.order_id, ta.gateway_id,
            ta.processor_name, ta.acquiring_bank, ta.mcc_code,
            ta.outcome, ta.cc_first_6, ta.order_total, ta.acquisition_date,
            ta.derived_product_role, ta.model_target,
            ta.issuer_bank, ta.card_brand, ta.card_type, ta.is_prepaid,
            ta.hour_of_day, ta.day_of_week, ta.mid_age_days,
            ta.offer_name, ta.billing_state,
            ta.mid_velocity_daily, ta.customer_history_on_proc,
            ta.bin_velocity_weekly, ta.initial_was_payfac
        FROM transaction_attempts ta
        WHERE ta.feature_version >= 3
          AND ta.model_target = 'initial'
          AND ta.derived_product_role LIKE '%main%'
          AND ta.id NOT IN (
            SELECT ta2.id FROM transaction_attempts ta2
            JOIN decline_reason_classes drc ON ta2.decline_reason = drc.decline_reason
            WHERE drc.decline_class IN ('customer_input', 'system_decline')
          )
          AND ta.gateway_id NOT IN (
            SELECT g.gateway_id FROM gateways g
            WHERE g.client_id = ta.client_id AND g.exclude_from_analysis = 1
          )
          AND ta.cc_first_6 NOT IN ('144444','777777','444444','411111','000000','666666','518426')
        ORDER BY ta.acquisition_date ASC, ta.id ASC
    """, conn)
    conn.close()
    df['label'] = (df['outcome'] == 'approved').astype(int)
    print(f"  Loaded {len(df):,} initial main attempts")
    print(f"  Approval rate: {df['label'].mean():.1%}")
    return df


def compute_all_features(df):
    """Compute all features using expanding window (no leakage)."""
    df = df.sort_values('acquisition_date').reset_index(drop=True)
    n = len(df)
    global_rate = df['label'].mean()

    # Parse dates once
    dates = pd.to_datetime(df['acquisition_date'], errors='coerce')
    df['_date'] = dates
    df['day_of_month'] = dates.dt.day.fillna(15).astype(int)
    df['is_near_payday'] = df['day_of_month'].apply(
        lambda d: 1 if d <= 3 or (13 <= d <= 17) or d >= 28 else 0
    )

    # Pre-allocate arrays
    bin_approval = np.full(n, np.nan)
    bin_proc_approval = np.full(n, np.nan)
    bin_approval_7d = np.full(n, np.nan)
    bin_approval_30d = np.full(n, np.nan)
    amount_vs_bin = np.full(n, 1.0)
    proc_ct_rate = np.full(n, np.nan)
    te_processor = np.full(n, np.nan)
    te_acq_bank = np.full(n, np.nan)
    te_billing_st = np.full(n, np.nan)

    # Accumulators
    bin_stats = {}                    # {bin: [approvals, total]}
    bin_proc_stats = {}               # {(bin, proc): [approvals, total]}
    bin_amount_stats = {}             # {bin: [sum_amount, count]}
    proc_ct_stats = {}                # {(proc, card_type): [approvals, total]}
    proc_stats = {}                   # {proc: [approvals, total]}
    acq_stats = {}                    # {acq_bank: [approvals, total]}
    state_stats = {}                  # {state: [approvals, total]}

    # For rolling windows, store (timestamp, approved) per BIN
    bin_events = defaultdict(list)    # {bin: [(timestamp, approved), ...]}

    print("  Computing features (expanding window, no leakage)...")

    for i in range(n):
        bin6 = df.loc[i, 'cc_first_6']
        proc = df.loc[i, 'processor_name']
        ct = df.loc[i, 'card_type']
        acq = df.loc[i, 'acquiring_bank']
        state = df.loc[i, 'billing_state']
        amt = df.loc[i, 'order_total'] or 0
        approved = df.loc[i, 'label']
        dt = df.loc[i, '_date']

        # ── READ from history ──

        # BIN overall
        if bin6 in bin_stats:
            a, t = bin_stats[bin6]
            if t >= 5:
                bin_approval[i] = a / t

        # BIN × processor
        bp_key = (bin6, proc)
        if bp_key in bin_proc_stats:
            a, t = bin_proc_stats[bp_key]
            if t >= 3:
                bin_proc_approval[i] = a / t

        # BIN rolling 7d and 30d
        if pd.notna(dt) and bin6 in bin_events:
            events = bin_events[bin6]
            ts = dt.timestamp()
            approvals_7d = 0
            total_7d = 0
            approvals_30d = 0
            total_30d = 0
            for evt_ts, evt_app in events:
                age = ts - evt_ts
                if age <= 7 * 86400:
                    total_7d += 1
                    approvals_7d += evt_app
                if age <= 30 * 86400:
                    total_30d += 1
                    approvals_30d += evt_app
            if total_7d >= 3:
                bin_approval_7d[i] = approvals_7d / total_7d
            if total_30d >= 5:
                bin_approval_30d[i] = approvals_30d / total_30d

        # Amount vs BIN average
        if bin6 in bin_amount_stats:
            s, c = bin_amount_stats[bin6]
            if c >= 5 and s > 0:
                avg = s / c
                amount_vs_bin[i] = amt / avg if avg > 0 else 1.0

        # Processor × card_type
        pct_key = (proc, ct)
        if pct_key in proc_ct_stats:
            a, t = proc_ct_stats[pct_key]
            if t >= 10:
                proc_ct_rate[i] = a / t

        # Target encoding: processor
        if proc in proc_stats:
            a, t = proc_stats[proc]
            if t >= 20:
                te_processor[i] = a / t

        # Target encoding: acquiring bank
        if acq in acq_stats:
            a, t = acq_stats[acq]
            if t >= 20:
                te_acq_bank[i] = a / t

        # Target encoding: billing state
        if state in state_stats:
            a, t = state_stats[state]
            if t >= 20:
                te_billing_st[i] = a / t

        # ── WRITE into accumulators ──

        if bin6 not in bin_stats:
            bin_stats[bin6] = [0, 0]
        bin_stats[bin6][1] += 1
        bin_stats[bin6][0] += approved

        if bp_key not in bin_proc_stats:
            bin_proc_stats[bp_key] = [0, 0]
        bin_proc_stats[bp_key][1] += 1
        bin_proc_stats[bp_key][0] += approved

        if bin6 not in bin_amount_stats:
            bin_amount_stats[bin6] = [0, 0]
        bin_amount_stats[bin6][0] += amt
        bin_amount_stats[bin6][1] += 1

        if pct_key not in proc_ct_stats:
            proc_ct_stats[pct_key] = [0, 0]
        proc_ct_stats[pct_key][1] += 1
        proc_ct_stats[pct_key][0] += approved

        if proc not in proc_stats:
            proc_stats[proc] = [0, 0]
        proc_stats[proc][1] += 1
        proc_stats[proc][0] += approved

        if acq not in acq_stats:
            acq_stats[acq] = [0, 0]
        acq_stats[acq][1] += 1
        acq_stats[acq][0] += approved

        if state not in state_stats:
            state_stats[state] = [0, 0]
        state_stats[state][1] += 1
        state_stats[state][0] += approved

        if pd.notna(dt):
            bin_events[bin6].append((dt.timestamp(), approved))

        if (i + 1) % 20000 == 0:
            print(f"    {i+1:,}/{n:,}")

    # Assign arrays
    df['bin_approval_rate'] = pd.Series(bin_approval).fillna(global_rate).values
    df['bin_proc_approval_rate'] = pd.Series(bin_proc_approval).fillna(global_rate).values
    df['bin_approval_7d'] = pd.Series(bin_approval_7d).fillna(global_rate).values
    df['bin_approval_30d'] = pd.Series(bin_approval_30d).fillna(global_rate).values
    df['amount_vs_bin_avg'] = amount_vs_bin
    df['proc_cardtype_rate'] = pd.Series(proc_ct_rate).fillna(global_rate).values
    df['te_processor'] = pd.Series(te_processor).fillna(global_rate).values
    df['te_acquiring_bank'] = pd.Series(te_acq_bank).fillna(global_rate).values
    df['te_billing_state'] = pd.Series(te_billing_st).fillna(global_rate).values

    # Group issuer
    counts = df['issuer_bank'].fillna('UNKNOWN').value_counts()
    top_banks = set(counts[counts >= ISSUER_MIN_COUNT].index)
    df['issuer_bank_grouped'] = df['issuer_bank'].fillna('UNKNOWN').apply(
        lambda x: x if x in top_banks else 'OTHER'
    )

    print(f"  Features computed. {len(bin_stats):,} BINs, {len(bin_proc_stats):,} BIN×Proc")
    return df


def train_lgbm(X_train, y_train, X_test, y_test):
    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()
    spw = neg / pos if pos > 0 else 1
    model = lgb.LGBMClassifier(
        n_estimators=300, max_depth=8, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=spw, random_state=42, verbose=-1, n_jobs=-1,
    )
    model.fit(X_train, y_train)
    y_pred = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred)
    return auc, model


def prepare(df, categorical, numerical):
    encoded_cols = []
    for col in categorical:
        le = LabelEncoder()
        values = df[col].fillna('UNKNOWN').astype(str)
        le.fit(values)
        df[f'{col}_enc'] = le.transform(values)
        encoded_cols.append(f'{col}_enc')
    for col in numerical:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    feature_cols = encoded_cols + numerical
    X = df[feature_cols].values.astype(np.float32)
    y = df['label'].values
    feature_names = [col.replace('_enc', '') for col in feature_cols]
    return X, y, feature_names


def print_importance(model, names, label, auc):
    importances = model.feature_importances_
    total = importances.sum()
    pairs = sorted(zip(names, importances), key=lambda x: -x[1])
    print(f"\n  {label} — AUC: {auc:.4f}")
    print(f"  {'Feature':<30} {'Imp':>6} {'%':>7}")
    print(f"  {'-'*45}")
    for fname, imp in pairs[:15]:
        print(f"  {fname:<30} {imp:>6.0f} {imp/total*100:>6.1f}%")


def main():
    print("=" * 70)
    print("Initial Model V2 — Full Feature Experiment + Ablation")
    print("=" * 70)

    df = load_data()
    df = compute_all_features(df)

    split_idx = int(len(df) * TRAIN_RATIO)
    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()
    print(f"\n  Train: {len(train_df):,} | Test: {len(test_df):,}")

    # ════════════════════════════════════════════
    # A: V1 baseline (grouped issuer + BIN features)
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("A: V1 BASELINE (grouped issuer + BIN features)")
    print("=" * 70)
    X_tr, y_tr, names = prepare(train_df.copy(), CATEGORICAL_V1, NUMERICAL_V1)
    X_te, y_te, _ = prepare(test_df.copy(), CATEGORICAL_V1, NUMERICAL_V1)
    auc_v1, mdl = train_lgbm(X_tr, y_tr, X_te, y_te)
    print_importance(mdl, names, "V1 BASELINE", auc_v1)

    # ════════════════════════════════════════════
    # B: Individual new feature contribution
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("B: INDIVIDUAL NEW FEATURE CONTRIBUTION")
    print("=" * 70)

    individual_results = []
    for feat in NEW_NUMERICAL:
        X_tr, y_tr, n = prepare(train_df.copy(), CATEGORICAL_V1, NUMERICAL_V1 + [feat])
        X_te, y_te, _ = prepare(test_df.copy(), CATEGORICAL_V1, NUMERICAL_V1 + [feat])
        auc, _ = train_lgbm(X_tr, y_tr, X_te, y_te)
        delta = (auc - auc_v1) * 100
        individual_results.append((feat, auc, delta))
        sign = '+' if delta >= 0 else ''
        print(f"  +{feat:<30} AUC: {auc:.4f}  ({sign}{delta:.2f}pp)")

    # ════════════════════════════════════════════
    # C: All new features combined
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("C: ALL NEW FEATURES COMBINED")
    print("=" * 70)
    all_num = NUMERICAL_V1 + NEW_NUMERICAL
    X_tr, y_tr, names = prepare(train_df.copy(), CATEGORICAL_V1, all_num)
    X_te, y_te, _ = prepare(test_df.copy(), CATEGORICAL_V1, all_num)
    auc_all, mdl_all = train_lgbm(X_tr, y_tr, X_te, y_te)
    print_importance(mdl_all, names, "ALL NEW FEATURES", auc_all)

    # ════════════════════════════════════════════
    # D: Only features that helped individually
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("D: ONLY FEATURES THAT HELPED INDIVIDUALLY (>0pp)")
    print("=" * 70)
    helpful = [f for f, a, d in individual_results if d > 0]
    if helpful:
        X_tr, y_tr, names = prepare(train_df.copy(), CATEGORICAL_V1, NUMERICAL_V1 + helpful)
        X_te, y_te, _ = prepare(test_df.copy(), CATEGORICAL_V1, NUMERICAL_V1 + helpful)
        auc_helpful, mdl_h = train_lgbm(X_tr, y_tr, X_te, y_te)
        print_importance(mdl_h, names, f"HELPFUL ONLY ({len(helpful)} new features)", auc_helpful)
    else:
        auc_helpful = auc_v1
        print("  No individual feature helped!")

    # ════════════════════════════════════════════
    # E: Full ablation on best combined set
    # ════════════════════════════════════════════
    best_num = NUMERICAL_V1 + helpful if helpful else all_num
    best_auc = max(auc_all, auc_helpful)
    best_label = "helpful-only" if auc_helpful >= auc_all else "all"

    print("\n" + "=" * 70)
    print(f"E: ABLATION on {best_label} set (AUC {best_auc:.4f})")
    print("=" * 70)

    all_features = CATEGORICAL_V1 + best_num
    ablation_results = []

    for feat in all_features:
        cat_minus = [c for c in CATEGORICAL_V1 if c != feat]
        num_minus = [n for n in best_num if n != feat]
        X_tr, y_tr, _ = prepare(train_df.copy(), cat_minus, num_minus)
        X_te, y_te, _ = prepare(test_df.copy(), cat_minus, num_minus)
        auc, _ = train_lgbm(X_tr, y_tr, X_te, y_te)
        delta = (auc - best_auc) * 100
        ablation_results.append((feat, auc, delta))

    ablation_results.sort(key=lambda x: -x[2])
    print(f"\n  {'Feature':<30} {'AUC w/o':>10} {'Delta':>10} {'Verdict':>15}")
    print(f"  {'-'*67}")
    for feat, auc, delta in ablation_results:
        if delta > 0.3:
            verdict = "DRAGGING"
        elif delta > 0:
            verdict = "slight drag"
        elif delta > -0.3:
            verdict = "marginal"
        else:
            verdict = "KEEP"
        sign = '+' if delta >= 0 else ''
        print(f"  {feat:<30} {auc:>10.4f} {sign}{delta:>9.2f}pp {verdict:>15}")

    # ════════════════════════════════════════════
    # F: Correlation analysis
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("F: CORRELATION MATRIX")
    print("=" * 70)

    num_df = df[best_num].apply(pd.to_numeric, errors='coerce').fillna(0)
    corr = num_df.corr()

    print(f"\n  Pairs with |correlation| > 0.40:")
    print(f"  {'Feature A':<28} {'Feature B':<28} {'Corr':>8}")
    print(f"  {'-'*66}")
    found = False
    for i, col_a in enumerate(best_num):
        for j, col_b in enumerate(best_num):
            if j <= i:
                continue
            c = corr.loc[col_a, col_b]
            if abs(c) > 0.40:
                found = True
                print(f"  {col_a:<28} {col_b:<28} {c:>8.3f}")
    if not found:
        print("  None found.")

    print(f"\n  Correlation with approval outcome:")
    print(f"  {'Feature':<30} {'Corr':>10}")
    print(f"  {'-'*42}")
    label_corrs = []
    for col in best_num:
        c = num_df[col].corr(df['label'].astype(float))
        label_corrs.append((col, c))
    label_corrs.sort(key=lambda x: -abs(x[1]))
    for col, c in label_corrs:
        print(f"  {col:<30} {c:>10.4f}")

    # ════════════════════════════════════════════
    # G: Progressive removal (greedy)
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("G: PROGRESSIVE REMOVAL (greedy)")
    print("=" * 70)

    current_cat = list(CATEGORICAL_V1)
    current_num = list(best_num)
    current_auc = best_auc
    removed = []

    for round_num in range(8):
        best_feat = None
        best_round_auc = current_auc
        best_delta = 0

        for feat in current_cat + current_num:
            cat_try = [c for c in current_cat if c != feat]
            num_try = [n for n in current_num if n != feat]
            X_tr, y_tr, _ = prepare(train_df.copy(), cat_try, num_try)
            X_te, y_te, _ = prepare(test_df.copy(), cat_try, num_try)
            auc, _ = train_lgbm(X_tr, y_tr, X_te, y_te)
            if auc > best_round_auc + 0.001:
                best_round_auc = auc
                best_feat = feat
                best_delta = (auc - current_auc) * 100

        if best_feat is None:
            print(f"  Round {round_num+1}: No removal improves AUC by >0.1pp. Stopping.")
            break

        current_cat = [c for c in current_cat if c != best_feat]
        current_num = [n for n in current_num if n != best_feat]
        current_auc = best_round_auc
        removed.append(best_feat)
        print(f"  Round {round_num+1}: Remove '{best_feat}' → AUC {best_round_auc:.4f} (+{best_delta:.2f}pp)")

    # ════════════════════════════════════════════
    # FINAL SUMMARY
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)
    print(f"  Original baseline (no BIN features):  0.6240")
    print(f"  V1 (grouped issuer + BIN):            {auc_v1:.4f}")
    print(f"  V2 all new features:                  {auc_all:.4f}")
    print(f"  V2 helpful-only:                      {auc_helpful:.4f}")
    print(f"  After progressive removal:            {current_auc:.4f}")
    print(f"\n  Total improvement: +{(current_auc - 0.6240)*100:.2f}pp")
    if removed:
        print(f"  Removed features: {', '.join(removed)}")
    print(f"  Final feature count: {len(current_cat)} cat + {len(current_num)} num = {len(current_cat)+len(current_num)}")

    # Print final feature set
    print(f"\n  FINAL CATEGORICAL: {current_cat}")
    print(f"  FINAL NUMERICAL: {current_num}")

    # Train and show final model importance
    print("\n" + "=" * 70)
    print("FINAL MODEL IMPORTANCE")
    print("=" * 70)
    X_tr, y_tr, names = prepare(train_df.copy(), current_cat, current_num)
    X_te, y_te, _ = prepare(test_df.copy(), current_cat, current_num)
    final_auc, final_model = train_lgbm(X_tr, y_tr, X_te, y_te)
    print_importance(final_model, names, "FINAL MODEL", final_auc)


if __name__ == '__main__':
    main()
