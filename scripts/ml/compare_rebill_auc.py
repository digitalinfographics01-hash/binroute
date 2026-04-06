"""
BinRoute AI — Rebill AUC Comparison

Trains LightGBM with and without subscription features,
then compares AUC-ROC by tx_class to show the impact.

Usage: py -3 scripts/ml/compare_rebill_auc.py
"""

import os
import sqlite3
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')

CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'tx_class', 'cycle_depth', 'prev_decline_reason',
    'initial_processor',
]

NUMERICAL_BASE = [
    'is_prepaid', 'amount', 'attempt_number',
    'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
]

NUMERICAL_SUB = [
    'consecutive_approvals', 'days_since_last_charge',
    'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
    'initial_amount', 'amount_ratio', 'prior_declines_in_cycle',
]


def main():
    print("=" * 65)
    print("Rebill AUC Comparison: Base 19 vs Full 27 Features")
    print("=" * 65)

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT processor_name, acquiring_bank, mcc_code,
               issuer_bank, card_brand, card_type, is_prepaid,
               amount, tx_class, attempt_number, cycle_depth,
               hour_of_day, day_of_week, prev_decline_reason,
               initial_processor,
               mid_velocity_daily, mid_velocity_weekly,
               customer_history_on_proc, bin_velocity_weekly,
               consecutive_approvals, days_since_last_charge,
               days_since_initial, lifetime_charges, lifetime_revenue,
               initial_amount, amount_ratio, prior_declines_in_cycle,
               outcome, acquisition_date
        FROM tx_features
        WHERE feature_version >= 2
        ORDER BY acquisition_date ASC, id ASC
    """, conn)
    conn.close()

    df['label'] = (df['outcome'] == 'approved').astype(int)
    print(f"\n  Total rows: {len(df):,}")

    # Encode categoricals
    for col in CATEGORICAL:
        le = LabelEncoder()
        df[f'{col}_enc'] = le.fit_transform(df[col].fillna('UNKNOWN').astype(str))

    for col in NUMERICAL_BASE + NUMERICAL_SUB:
        df[col] = df[col].fillna(0)

    cat_cols = [f'{col}_enc' for col in CATEGORICAL]
    base_features = cat_cols + NUMERICAL_BASE
    full_features = cat_cols + NUMERICAL_BASE + NUMERICAL_SUB

    # Time-based split
    split_idx = int(len(df) * 0.80)
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]

    y_train = train['label'].values
    y_test = test['label'].values

    scale_pos = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    # Train BASE model (19 features)
    print("\n  Training BASE model (19 features)...", end=" ", flush=True)
    model_base = lgb.LGBMClassifier(
        n_estimators=300, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=scale_pos, verbose=-1, random_state=42
    )
    model_base.fit(train[base_features].values.astype(np.float32), y_train)
    prob_base = model_base.predict_proba(test[base_features].values.astype(np.float32))[:, 1]
    print("done")

    # Train FULL model (27 features)
    print("  Training FULL model (27 features)...", end=" ", flush=True)
    model_full = lgb.LGBMClassifier(
        n_estimators=300, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=scale_pos, verbose=-1, random_state=42
    )
    model_full.fit(train[full_features].values.astype(np.float32), y_train)
    prob_full = model_full.predict_proba(test[full_features].values.astype(np.float32))[:, 1]
    print("done")

    # Overall AUC
    auc_base_all = roc_auc_score(y_test, prob_base)
    auc_full_all = roc_auc_score(y_test, prob_full)

    print(f"\n  {'':30} {'Base (19)':>10} {'Full (27)':>10} {'Diff':>8}")
    print(f"  {'-'*62}")
    print(f"  {'OVERALL':<30} {auc_base_all:>10.4f} {auc_full_all:>10.4f} {auc_full_all-auc_base_all:>+8.4f}")

    # Per tx_class AUC
    print(f"\n  Per TX Class:")
    print(f"  {'TX Class':<15} {'Count':>6} {'Base (19)':>10} {'Full (27)':>10} {'Diff':>8} {'Verdict':>12}")
    print(f"  {'-'*65}")

    for tx_class in ['initial', 'rebill', 'salvage', 'cascade', 'upsell']:
        mask = test['tx_class'].values == tx_class
        if mask.sum() < 50:
            continue

        cls_y = y_test[mask]
        # Need both classes for AUC
        if len(set(cls_y)) < 2:
            continue

        cls_base = prob_base[mask]
        cls_full = prob_full[mask]

        auc_b = roc_auc_score(cls_y, cls_base)
        auc_f = roc_auc_score(cls_y, cls_full)
        diff = auc_f - auc_b

        if diff > 0.01:
            verdict = "IMPROVED"
        elif diff > 0.002:
            verdict = "slight +"
        elif diff < -0.005:
            verdict = "worse"
        else:
            verdict = "same"

        print(f"  {tx_class:<15} {mask.sum():>6} {auc_b:>10.4f} {auc_f:>10.4f} {diff:>+8.4f} {verdict:>12}")

    # Rebill deep dive by cycle
    print(f"\n  Rebill Deep Dive by Cycle Depth:")
    print(f"  {'Cycle':<10} {'Count':>6} {'Base (19)':>10} {'Full (27)':>10} {'Diff':>8}")
    print(f"  {'-'*48}")

    rebill_mask = test['tx_class'].values == 'rebill'
    for cd in ['C1', 'C2', 'C3+']:
        mask = rebill_mask & (test['cycle_depth'].values == cd)
        if mask.sum() < 30:
            continue
        cls_y = y_test[mask]
        if len(set(cls_y)) < 2:
            continue
        auc_b = roc_auc_score(cls_y, prob_base[mask])
        auc_f = roc_auc_score(cls_y, prob_full[mask])
        print(f"  {cd:<10} {mask.sum():>6} {auc_b:>10.4f} {auc_f:>10.4f} {auc_f-auc_b:>+8.4f}")

    # Salvage deep dive by attempt
    print(f"\n  Salvage Deep Dive by Attempt:")
    print(f"  {'Attempt':<10} {'Count':>6} {'Base (19)':>10} {'Full (27)':>10} {'Diff':>8}")
    print(f"  {'-'*48}")

    salvage_mask = test['tx_class'].values == 'salvage'
    for att in [2, 3, 4]:
        mask = salvage_mask & (test['attempt_number'].values == att)
        if mask.sum() < 30:
            continue
        cls_y = y_test[mask]
        if len(set(cls_y)) < 2:
            continue
        auc_b = roc_auc_score(cls_y, prob_base[mask])
        auc_f = roc_auc_score(cls_y, prob_full[mask])
        label = f"Attempt {att}"
        print(f"  {label:<10} {mask.sum():>6} {auc_b:>10.4f} {auc_f:>10.4f} {auc_f-auc_b:>+8.4f}")

    print("\nDone!")


if __name__ == '__main__':
    main()
