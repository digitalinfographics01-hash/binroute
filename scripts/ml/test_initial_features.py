"""
BinRoute AI — Initial Model Feature Experiment

Tests whether BIN-level historical features improve the initial model AUC.

New features tested:
  1. bin_approval_rate      — overall historical approval rate for this BIN
  2. bin_proc_approval_rate — approval rate for this BIN on this specific processor
  3. bin_proc_volume        — number of past transactions for this BIN on this processor
  4. bin_distinct_procs     — how many distinct processors this BIN has been seen on
  5. affiliate_approval     — historical approval rate for this affiliate/traffic source

All computed using ONLY data BEFORE the transaction date (no data leakage).

Usage: py -3 scripts/ml/test_initial_features.py [--db=PATH]
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

# ── Baseline features (same as current initial model) ──
CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'offer_name', 'billing_state', 'client_id',
]

NUMERICAL = [
    'is_prepaid', 'order_total', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'customer_history_on_proc',
    'bin_velocity_weekly', 'mid_age_days', 'initial_was_payfac',
]

# ── New features to test ──
NEW_NUMERICAL = [
    'bin_approval_rate',
    'bin_proc_approval_rate',
    'bin_proc_volume',
    'bin_distinct_procs',
    'affiliate_approval_rate',
]


def load_data():
    """Load initial main transactions."""
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
            ta.initial_was_payfac,
            o.affiliate, o.afid, o.sid, o.utm_source
        FROM transaction_attempts ta
        LEFT JOIN orders o ON ta.order_id = o.id
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


def compute_bin_features(df):
    """
    Compute BIN-level historical features using expanding window.
    For each row, uses only data from BEFORE that transaction (no leakage).
    """
    print("\n  Computing BIN-level historical features (no leakage)...")

    df = df.sort_values('acquisition_date').reset_index(drop=True)

    # Pre-allocate
    bin_approval = np.full(len(df), np.nan)
    bin_proc_approval = np.full(len(df), np.nan)
    bin_proc_vol = np.zeros(len(df))
    bin_distinct = np.zeros(len(df))
    aff_approval = np.full(len(df), np.nan)

    # Running accumulators
    # BIN overall: {bin: [approvals, total]}
    bin_stats = {}
    # BIN x Processor: {(bin, proc): [approvals, total]}
    bin_proc_stats = {}
    # BIN distinct processors: {bin: set()}
    bin_procs_seen = {}
    # Affiliate: {affiliate: [approvals, total]}
    aff_stats = {}

    for i in range(len(df)):
        bin6 = df.loc[i, 'cc_first_6']
        proc = df.loc[i, 'processor_name']
        aff = df.loc[i, 'affiliate']
        approved = df.loc[i, 'label']

        # ── READ features from history (before this row) ──
        if bin6 in bin_stats:
            a, t = bin_stats[bin6]
            if t >= 5:  # min sample
                bin_approval[i] = a / t

        bp_key = (bin6, proc)
        if bp_key in bin_proc_stats:
            a, t = bin_proc_stats[bp_key]
            if t >= 3:
                bin_proc_approval[i] = a / t
            bin_proc_vol[i] = t

        if bin6 in bin_procs_seen:
            bin_distinct[i] = len(bin_procs_seen[bin6])

        if aff and aff in aff_stats:
            a, t = aff_stats[aff]
            if t >= 10:
                aff_approval[i] = a / t

        # ── WRITE this row into accumulators (for future rows) ──
        if bin6 not in bin_stats:
            bin_stats[bin6] = [0, 0]
        bin_stats[bin6][1] += 1
        bin_stats[bin6][0] += approved

        if bp_key not in bin_proc_stats:
            bin_proc_stats[bp_key] = [0, 0]
        bin_proc_stats[bp_key][1] += 1
        bin_proc_stats[bp_key][0] += approved

        if bin6 not in bin_procs_seen:
            bin_procs_seen[bin6] = set()
        if proc:
            bin_procs_seen[bin6].add(proc)

        if aff:
            if aff not in aff_stats:
                aff_stats[aff] = [0, 0]
            aff_stats[aff][1] += 1
            aff_stats[aff][0] += approved

        if (i + 1) % 20000 == 0:
            print(f"    {i + 1:,}/{len(df):,} rows processed")

    df['bin_approval_rate'] = bin_approval
    df['bin_proc_approval_rate'] = bin_proc_approval
    df['bin_proc_volume'] = bin_proc_vol
    df['bin_distinct_procs'] = bin_distinct
    df['affiliate_approval_rate'] = aff_approval

    # Fill NaN with global mean (cold start)
    global_rate = df['label'].mean()
    df['bin_approval_rate'] = df['bin_approval_rate'].fillna(global_rate)
    df['bin_proc_approval_rate'] = df['bin_proc_approval_rate'].fillna(global_rate)
    df['affiliate_approval_rate'] = df['affiliate_approval_rate'].fillna(global_rate)

    print(f"  BIN features computed for {len(df):,} rows")
    print(f"  Unique BINs: {len(bin_stats):,}")
    print(f"  Unique BIN×Proc combos: {len(bin_proc_stats):,}")

    return df


def train_and_eval(X_train, y_train, X_test, y_test, feature_names, label):
    """Train LightGBM and return AUC."""
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

    # Feature importance
    importances = model.feature_importances_
    total = importances.sum()
    pairs = sorted(zip(feature_names, importances), key=lambda x: -x[1])

    print(f"\n  {label} — AUC: {auc:.4f}")
    print(f"  {'Feature':<30} {'Importance':>10} {'%':>8}")
    print(f"  {'-'*50}")
    for fname, imp in pairs[:15]:
        print(f"  {fname:<30} {imp:>10.0f} {imp/total*100:>7.1f}%")

    return auc, model


def prepare(df, categorical, numerical):
    """Encode and prepare feature matrix."""
    encoders = {}
    encoded_cols = []

    for col in categorical:
        le = LabelEncoder()
        values = df[col].fillna('UNKNOWN').astype(str)
        le.fit(values)
        df[f'{col}_enc'] = le.transform(values)
        encoders[col] = le
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
    print("Initial Model Feature Experiment")
    print("=" * 70)

    df = load_data()
    df = compute_bin_features(df)

    # Time-based split
    split_idx = int(len(df) * TRAIN_RATIO)
    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()

    print(f"\n  Train: {len(train_df):,} | Test: {len(test_df):,}")
    print(f"  Train approval: {train_df['label'].mean():.1%} | Test approval: {test_df['label'].mean():.1%}")

    # ── A: Baseline (current features) ──
    print("\n" + "=" * 70)
    print("A: BASELINE (current features)")
    print("=" * 70)

    train_a = train_df.copy()
    test_a = test_df.copy()
    X_train_a, y_train_a, names_a = prepare(train_a, CATEGORICAL, NUMERICAL)
    X_test_a, y_test_a, _ = prepare(test_a, CATEGORICAL, NUMERICAL)

    auc_a, _ = train_and_eval(X_train_a, y_train_a, X_test_a, y_test_a, names_a, "BASELINE")

    # ── B: With BIN-level features ──
    print("\n" + "=" * 70)
    print("B: WITH BIN-LEVEL FEATURES")
    print("=" * 70)

    train_b = train_df.copy()
    test_b = test_df.copy()
    X_train_b, y_train_b, names_b = prepare(train_b, CATEGORICAL, NUMERICAL + NEW_NUMERICAL)
    X_test_b, y_test_b, _ = prepare(test_b, CATEGORICAL, NUMERICAL + NEW_NUMERICAL)

    auc_b, _ = train_and_eval(X_train_b, y_train_b, X_test_b, y_test_b, names_b, "WITH BIN FEATURES")

    # ── C: Individual feature contribution ──
    print("\n" + "=" * 70)
    print("C: INDIVIDUAL FEATURE CONTRIBUTION")
    print("=" * 70)

    for feat in NEW_NUMERICAL:
        train_c = train_df.copy()
        test_c = test_df.copy()
        X_train_c, y_train_c, names_c = prepare(train_c, CATEGORICAL, NUMERICAL + [feat])
        X_test_c, y_test_c, _ = prepare(test_c, CATEGORICAL, NUMERICAL + [feat])

        auc_c, _ = train_and_eval(X_train_c, y_train_c, X_test_c, y_test_c, names_c, f"+{feat}")

        delta = (auc_c - auc_a) * 100
        print(f"  Delta vs baseline: {'+' if delta >= 0 else ''}{delta:.2f} AUC points")

    # ── Summary ──
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    delta_all = (auc_b - auc_a) * 100
    print(f"  Baseline AUC:         {auc_a:.4f}")
    print(f"  With BIN features:    {auc_b:.4f}  ({'+' if delta_all >= 0 else ''}{delta_all:.2f} points)")
    print(f"\n  {'IMPROVEMENT' if delta_all > 0.5 else 'MARGINAL' if delta_all > 0 else 'NO IMPROVEMENT'}")


if __name__ == '__main__':
    main()
