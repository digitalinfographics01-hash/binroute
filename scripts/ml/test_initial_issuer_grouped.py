"""
BinRoute AI — Initial Model: Issuer Bank Grouping Test

Tests whether grouping rare issuer banks improves over raw label or removal.

Three variants:
  A: No issuer_bank (removed, as ablation suggested)
  B: Raw issuer_bank (current, high cardinality)
  C: Grouped issuer_bank (top N banks kept, rest → OTHER)
  D: Grouped + no mcc_code

Usage: py -3 scripts/ml/test_initial_issuer_grouped.py [--db=PATH]
"""

import os
import sys
import sqlite3
import warnings
import numpy as np
import pandas as pd

from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score

import lightgbm as lgb

warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
for arg in sys.argv:
    if arg.startswith('--db='):
        DB_PATH = arg.split('=', 1)[1]

TRAIN_RATIO = 0.80

CATEGORICAL_BASE = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'card_brand', 'card_type',
    'billing_state', 'client_id',
]

NUMERICAL = [
    'is_prepaid', 'order_total', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'customer_history_on_proc',
    'bin_velocity_weekly', 'mid_age_days',
    'bin_approval_rate', 'bin_proc_approval_rate',
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
    return df


def compute_bin_features(df):
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
    return df


def group_issuer(df, min_count=200):
    """Group rare issuer banks into OTHER."""
    counts = df['issuer_bank'].fillna('UNKNOWN').value_counts()
    top_banks = set(counts[counts >= min_count].index)
    df['issuer_bank_grouped'] = df['issuer_bank'].fillna('UNKNOWN').apply(
        lambda x: x if x in top_banks else 'OTHER'
    )
    n_kept = len(top_banks)
    n_total = len(counts)
    pct = counts[counts >= min_count].sum() / len(df) * 100
    print(f"  Issuer grouping: kept {n_kept}/{n_total} banks ({pct:.1f}% of traffic)")
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


def run_variant(label, train_df, test_df, categorical, numerical):
    X_tr, y_tr, names = prepare(train_df.copy(), categorical, numerical)
    X_te, y_te, _ = prepare(test_df.copy(), categorical, numerical)
    auc, model = train_lgbm(X_tr, y_tr, X_te, y_te)

    importances = model.feature_importances_
    total = importances.sum()
    pairs = sorted(zip(names, importances), key=lambda x: -x[1])

    print(f"\n  {label} — AUC: {auc:.4f}")
    print(f"  {'Feature':<30} {'Imp':>6} {'%':>7}")
    print(f"  {'-'*45}")
    for fname, imp in pairs[:12]:
        print(f"  {fname:<30} {imp:>6.0f} {imp/total*100:>6.1f}%")

    return auc


def main():
    print("=" * 70)
    print("Initial Model — Issuer Bank Grouping Test")
    print("=" * 70)

    df = load_data()
    df = compute_bin_features(df)
    df = group_issuer(df, min_count=200)

    split_idx = int(len(df) * TRAIN_RATIO)
    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()
    print(f"\n  Train: {len(train_df):,} | Test: {len(test_df):,}")

    # A: No issuer_bank
    print("\n" + "=" * 70)
    print("A: NO issuer_bank")
    print("=" * 70)
    auc_a = run_variant("No issuer", train_df, test_df,
        CATEGORICAL_BASE, NUMERICAL)

    # B: Raw issuer_bank
    print("\n" + "=" * 70)
    print("B: RAW issuer_bank (current)")
    print("=" * 70)
    auc_b = run_variant("Raw issuer", train_df, test_df,
        CATEGORICAL_BASE + ['issuer_bank'], NUMERICAL)

    # C: Grouped issuer_bank
    print("\n" + "=" * 70)
    print("C: GROUPED issuer_bank (top banks, rest=OTHER)")
    print("=" * 70)
    auc_c = run_variant("Grouped issuer", train_df, test_df,
        CATEGORICAL_BASE + ['issuer_bank_grouped'], NUMERICAL)

    # D: Grouped issuer + no mcc_code
    print("\n" + "=" * 70)
    print("D: GROUPED issuer + NO mcc_code")
    print("=" * 70)
    cat_d = [c for c in CATEGORICAL_BASE if c != 'mcc_code'] + ['issuer_bank_grouped']
    auc_d = run_variant("Grouped issuer, no mcc", train_df, test_df,
        cat_d, NUMERICAL)

    # E: Also test different grouping thresholds
    print("\n" + "=" * 70)
    print("E: GROUPING THRESHOLD SWEEP")
    print("=" * 70)
    for threshold in [50, 100, 200, 500, 1000]:
        df_t = df.copy()
        counts = df_t['issuer_bank'].fillna('UNKNOWN').value_counts()
        top = set(counts[counts >= threshold].index)
        df_t['issuer_bank_sweep'] = df_t['issuer_bank'].fillna('UNKNOWN').apply(
            lambda x: x if x in top else 'OTHER'
        )
        n_kept = len(top)
        tr = df_t.iloc[:split_idx].copy()
        te = df_t.iloc[split_idx:].copy()
        X_tr, y_tr, _ = prepare(tr, CATEGORICAL_BASE + ['issuer_bank_sweep'], NUMERICAL)
        X_te, y_te, _ = prepare(te, CATEGORICAL_BASE + ['issuer_bank_sweep'], NUMERICAL)
        auc_t, _ = train_lgbm(X_tr, y_tr, X_te, y_te)
        print(f"  min_count={threshold:>5} → {n_kept:>3} banks → AUC {auc_t:.4f}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  A: No issuer_bank          {auc_a:.4f}")
    print(f"  B: Raw issuer_bank         {auc_b:.4f}")
    print(f"  C: Grouped issuer_bank     {auc_c:.4f}")
    print(f"  D: Grouped + no mcc_code   {auc_d:.4f}")
    best = max([('A', auc_a), ('B', auc_b), ('C', auc_c), ('D', auc_d)], key=lambda x: x[1])
    print(f"\n  WINNER: {best[0]} ({best[1]:.4f})")


if __name__ == '__main__':
    main()
