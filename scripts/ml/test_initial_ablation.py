"""
BinRoute AI — Initial Model Feature Ablation

Starts from the enhanced feature set (baseline + bin_approval_rate + bin_proc_approval_rate)
and removes one feature at a time. If removing a feature IMPROVES AUC, it's dragging the model.

Also checks correlation between features to identify redundancy.

Usage: py -3 scripts/ml/test_initial_ablation.py [--db=PATH]
"""

import os
import sys
import sqlite3
import warnings
import numpy as np
import pandas as pd
from datetime import datetime

from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score

import lightgbm as lgb

warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
for arg in sys.argv:
    if arg.startswith('--db='):
        DB_PATH = arg.split('=', 1)[1]

TRAIN_RATIO = 0.80

CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'offer_name', 'billing_state', 'client_id',
]

NUMERICAL = [
    'is_prepaid', 'order_total', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'customer_history_on_proc',
    'bin_velocity_weekly', 'mid_age_days', 'initial_was_payfac',
    'bin_approval_rate', 'bin_proc_approval_rate',
]


def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            ta.client_id, ta.order_id, ta.attempt_seq, ta.gateway_id,
            ta.processor_name, ta.acquiring_bank, ta.mcc_code,
            ta.outcome, ta.decline_reason,
            ta.cc_first_6, ta.order_total, ta.acquisition_date,
            ta.derived_product_role, ta.derived_cycle, ta.derived_attempt,
            ta.offer_name, ta.billing_state, ta.is_cascaded,
            ta.model_target,
            ta.issuer_bank, ta.card_brand, ta.card_type, ta.is_prepaid,
            ta.hour_of_day, ta.day_of_week, ta.mid_age_days,
            ta.initial_processor, ta.last_approved_processor,
            ta.mid_velocity_daily, ta.mid_velocity_weekly,
            ta.customer_history_on_proc, ta.bin_velocity_weekly,
            ta.initial_was_payfac
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
    return df


def compute_bin_features(df):
    print("  Computing BIN-level features...")
    df = df.sort_values('acquisition_date').reset_index(drop=True)

    bin_approval = np.full(len(df), np.nan)
    bin_proc_approval = np.full(len(df), np.nan)

    bin_stats = {}
    bin_proc_stats = {}

    for i in range(len(df)):
        bin6 = df.loc[i, 'cc_first_6']
        proc = df.loc[i, 'processor_name']
        approved = df.loc[i, 'label']

        if bin6 in bin_stats:
            a, t = bin_stats[bin6]
            if t >= 5:
                bin_approval[i] = a / t

        bp_key = (bin6, proc)
        if bp_key in bin_proc_stats:
            a, t = bin_proc_stats[bp_key]
            if t >= 3:
                bin_proc_approval[i] = a / t

        if bin6 not in bin_stats:
            bin_stats[bin6] = [0, 0]
        bin_stats[bin6][1] += 1
        bin_stats[bin6][0] += approved

        if bp_key not in bin_proc_stats:
            bin_proc_stats[bp_key] = [0, 0]
        bin_proc_stats[bp_key][1] += 1
        bin_proc_stats[bp_key][0] += approved

    global_rate = df['label'].mean()
    df['bin_approval_rate'] = pd.Series(bin_approval).fillna(global_rate).values
    df['bin_proc_approval_rate'] = pd.Series(bin_proc_approval).fillna(global_rate).values

    print(f"  Done. {len(bin_stats):,} BINs, {len(bin_proc_stats):,} BIN×Proc combos")
    return df


def train_lgbm(X_train, y_train, X_test, y_test):
    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()
    spw = neg / pos if pos > 0 else 1

    model = lgb.LGBMClassifier(
        n_estimators=300, max_depth=8, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=spw,
        random_state=42, verbose=-1, n_jobs=-1,
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


def main():
    print("=" * 70)
    print("Initial Model — Feature Ablation + Correlation Analysis")
    print("=" * 70)

    df = load_data()
    df = compute_bin_features(df)

    split_idx = int(len(df) * TRAIN_RATIO)
    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()

    print(f"\n  Train: {len(train_df):,} | Test: {len(test_df):,}")

    # ── Full model AUC ──
    print("\n" + "=" * 70)
    print("FULL MODEL (all features)")
    print("=" * 70)

    X_train, y_train, names = prepare(train_df.copy(), CATEGORICAL, NUMERICAL)
    X_test, y_test, _ = prepare(test_df.copy(), CATEGORICAL, NUMERICAL)
    full_auc, full_model = train_lgbm(X_train, y_train, X_test, y_test)

    importances = full_model.feature_importances_
    total_imp = importances.sum()
    pairs = sorted(zip(names, importances), key=lambda x: -x[1])
    print(f"\n  Full model AUC: {full_auc:.4f}")
    print(f"\n  {'Feature':<30} {'Importance':>10} {'%':>8}")
    print(f"  {'-'*50}")
    for fname, imp in pairs:
        print(f"  {fname:<30} {imp:>10.0f} {imp/total_imp*100:>7.1f}%")

    # ── Ablation: remove one feature at a time ──
    print("\n" + "=" * 70)
    print("ABLATION — Remove one feature at a time")
    print("=" * 70)

    all_features = CATEGORICAL + NUMERICAL
    results = []

    for feat in all_features:
        cat_minus = [c for c in CATEGORICAL if c != feat]
        num_minus = [n for n in NUMERICAL if n != feat]

        X_tr, y_tr, n = prepare(train_df.copy(), cat_minus, num_minus)
        X_te, y_te, _ = prepare(test_df.copy(), cat_minus, num_minus)
        auc, _ = train_lgbm(X_tr, y_tr, X_te, y_te)

        delta = (auc - full_auc) * 100
        results.append((feat, auc, delta))

    # Sort by delta descending — features at top IMPROVE when removed (dragging)
    results.sort(key=lambda x: -x[2])

    print(f"\n  {'Feature':<30} {'AUC w/o':>10} {'Delta':>10} {'Verdict':>15}")
    print(f"  {'-'*67}")
    for feat, auc, delta in results:
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

    # ── Correlation matrix for numerical features ──
    print("\n" + "=" * 70)
    print("CORRELATION MATRIX (numerical features)")
    print("=" * 70)

    num_df = df[NUMERICAL].apply(pd.to_numeric, errors='coerce').fillna(0)
    corr = num_df.corr()

    # Show high correlations (>0.4)
    print(f"\n  Pairs with |correlation| > 0.40:")
    print(f"  {'Feature A':<28} {'Feature B':<28} {'Corr':>8}")
    print(f"  {'-'*66}")
    found = False
    for i, col_a in enumerate(NUMERICAL):
        for j, col_b in enumerate(NUMERICAL):
            if j <= i:
                continue
            c = corr.loc[col_a, col_b]
            if abs(c) > 0.40:
                found = True
                print(f"  {col_a:<28} {col_b:<28} {c:>8.3f}")
    if not found:
        print("  None found.")

    # ── Correlation with label ──
    print(f"\n  Feature correlation with approval outcome:")
    print(f"  {'Feature':<30} {'Corr w/ label':>15}")
    print(f"  {'-'*47}")
    label_corrs = []
    for col in NUMERICAL:
        c = num_df[col].corr(df['label'].astype(float))
        label_corrs.append((col, c))
    label_corrs.sort(key=lambda x: -abs(x[1]))
    for col, c in label_corrs:
        print(f"  {col:<30} {c:>15.4f}")

    # ── Best subset recommendation ──
    print("\n" + "=" * 70)
    print("RECOMMENDED OPTIMAL FEATURE SET")
    print("=" * 70)

    # Remove features that drag or are marginal noise
    keep_cat = []
    keep_num = []
    drop = []
    for feat, auc, delta in results:
        if delta > 0.3:
            drop.append(feat)
        else:
            if feat in CATEGORICAL:
                keep_cat.append(feat)
            else:
                keep_num.append(feat)

    if drop:
        print(f"\n  Features to DROP (improve AUC when removed):")
        for f in drop:
            d = next(r[2] for r in results if r[0] == f)
            print(f"    - {f} (+{d:.2f}pp without it)")

    # Train optimal
    X_tr_opt, y_tr_opt, n_opt = prepare(train_df.copy(), keep_cat, keep_num)
    X_te_opt, y_te_opt, _ = prepare(test_df.copy(), keep_cat, keep_num)
    opt_auc, _ = train_lgbm(X_tr_opt, y_tr_opt, X_te_opt, y_te_opt)

    print(f"\n  Full model AUC:     {full_auc:.4f}  ({len(all_features)} features)")
    print(f"  Optimized AUC:      {opt_auc:.4f}  ({len(keep_cat) + len(keep_num)} features)")
    print(f"  Delta:              {'+' if opt_auc >= full_auc else ''}{(opt_auc - full_auc) * 100:.2f}pp")

    # Also try progressive removal of worst offenders
    print("\n" + "=" * 70)
    print("PROGRESSIVE REMOVAL (greedy)")
    print("=" * 70)

    current_cat = list(CATEGORICAL)
    current_num = list(NUMERICAL)
    current_auc = full_auc
    removed = []

    for round_num in range(5):  # max 5 rounds
        best_feat = None
        best_auc = current_auc
        best_delta = 0

        for feat in current_cat + current_num:
            cat_try = [c for c in current_cat if c != feat]
            num_try = [n for n in current_num if n != feat]
            X_tr, y_tr, _ = prepare(train_df.copy(), cat_try, num_try)
            X_te, y_te, _ = prepare(test_df.copy(), cat_try, num_try)
            auc, _ = train_lgbm(X_tr, y_tr, X_te, y_te)
            if auc > best_auc + 0.001:  # must improve by at least 0.1pp
                best_auc = auc
                best_feat = feat
                best_delta = (auc - current_auc) * 100

        if best_feat is None:
            print(f"\n  Round {round_num + 1}: No feature removal improves AUC by >0.1pp. Stopping.")
            break

        current_cat = [c for c in current_cat if c != best_feat]
        current_num = [n for n in current_num if n != best_feat]
        current_auc = best_auc
        removed.append(best_feat)
        print(f"  Round {round_num + 1}: Remove '{best_feat}' → AUC {best_auc:.4f} (+{best_delta:.2f}pp)")

    print(f"\n  Final AUC after progressive removal: {current_auc:.4f}")
    print(f"  Total improvement: {'+' if current_auc >= full_auc else ''}{(current_auc - full_auc)*100:.2f}pp")
    if removed:
        print(f"  Removed: {', '.join(removed)}")
    print(f"  Remaining features: {len(current_cat)} categorical + {len(current_num)} numerical = {len(current_cat)+len(current_num)}")


if __name__ == '__main__':
    main()
