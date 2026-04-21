"""
BinRoute AI — Rebill AUC Investigation

Why did rebill AUC drop from 0.96 (CatBoost, 5-model training) to 0.66-0.72 here?

Possible causes:
  1. CatBoost vs LightGBM — CatBoost handles categoricals natively
  2. LabelEncoder creates arbitrary ordinal values for categoricals
  3. The 5-model training used different data filtering
  4. last_approved_processor has too many categories for LabelEncoder

Tests:
  A: LightGBM with LabelEncoder (current test setup)
  B: CatBoost with native categoricals (same as production)
  C: LightGBM with target encoding for processor features
  D: CatBoost WITHOUT processor memory features
  E: CatBoost with ONLY C1 rebills (user's main interest)

Usage: python3 -u scripts/ml/test_rebill_auc_investigation.py [--db=PATH]
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
from catboost import CatBoostClassifier

warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
for arg in sys.argv:
    if arg.startswith('--db='):
        DB_PATH = arg.split('=', 1)[1]

TRAIN_RATIO = 0.80

CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'offer_name', 'billing_state',
    'initial_processor', 'last_approved_processor', 'card_level',
]

NUMERICAL = [
    'is_prepaid', 'order_total', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'mid_age_days',
    'consecutive_approvals', 'days_since_last_charge',
    'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
    'initial_amount', 'amount_ratio', 'initial_was_payfac',
]


def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            ta.client_id, ta.order_id, ta.gateway_id,
            ta.processor_name, ta.acquiring_bank, ta.mcc_code,
            ta.outcome, ta.cc_first_6, ta.order_total, ta.acquisition_date,
            ta.derived_product_role, ta.model_target,
            ta.derived_cycle,
            ta.issuer_bank, ta.card_brand, ta.card_type, ta.is_prepaid,
            ta.hour_of_day, ta.day_of_week, ta.mid_age_days,
            ta.offer_name, ta.billing_state,
            ta.mid_velocity_daily, ta.mid_velocity_weekly,
            ta.customer_history_on_proc, ta.bin_velocity_weekly,
            ta.initial_processor, ta.last_approved_processor,
            ta.consecutive_approvals, ta.days_since_last_charge,
            ta.days_since_initial, ta.lifetime_charges, ta.lifetime_revenue,
            ta.initial_amount, ta.amount_ratio, ta.initial_was_payfac,
            bl.card_level
        FROM transaction_attempts ta
        LEFT JOIN bin_lookup bl ON ta.cc_first_6 = bl.bin
        WHERE ta.feature_version >= 3
          AND ta.model_target = 'rebill'
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
    print(f"  Loaded {len(df):,} rebill main attempts")
    print(f"  Approval rate: {df['label'].mean():.1%}")
    print(f"  Cycle distribution:")
    print(df['derived_cycle'].value_counts().to_string())
    return df


def prepare_lgbm(df, categorical, numerical):
    """LabelEncoder for LightGBM."""
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


def prepare_catboost(df, categorical, numerical):
    """String categoricals for CatBoost native handling."""
    for col in categorical:
        df[col] = df[col].fillna('UNKNOWN').astype(str)
    for col in numerical:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    feature_cols = categorical + numerical
    X = df[feature_cols]
    y = df['label'].values
    cat_indices = list(range(len(categorical)))
    return X, y, feature_cols, cat_indices


def train_lgbm(X_train, y_train, X_test, y_test):
    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()
    spw = neg / pos if pos > 0 else 1
    model = lgb.LGBMClassifier(
        n_estimators=300, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=spw, random_state=42, verbose=-1, n_jobs=-1,
    )
    model.fit(X_train, y_train)
    y_pred = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred)
    return auc, model


def train_catboost(X_train, y_train, X_test, y_test, cat_indices):
    model = CatBoostClassifier(
        iterations=300, depth=8, learning_rate=0.1,
        auto_class_weights='Balanced',
        cat_features=cat_indices,
        verbose=0, random_seed=42,
    )
    model.fit(X_train, y_train)
    y_pred = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred)
    return auc, model


def print_importance(model, names, label, auc, is_catboost=False):
    if is_catboost:
        importances = model.get_feature_importance()
    else:
        importances = model.feature_importances_
    total = importances.sum()
    pairs = sorted(zip(names, importances), key=lambda x: -x[1])
    print(f"\n  {label} — AUC: {auc:.4f}")
    print(f"  {'Feature':<30} {'Imp':>6} {'%':>7}")
    print(f"  {'-'*45}")
    for fname, imp in pairs[:12]:
        print(f"  {fname:<30} {imp:>6.0f} {imp/total*100:>6.1f}%")


def main():
    print("=" * 70)
    print("Rebill AUC Investigation")
    print("=" * 70)

    df = load_data()

    split_idx = int(len(df) * TRAIN_RATIO)
    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()
    print(f"\n  Train: {len(train_df):,} | Test: {len(test_df):,}")

    # ════════════════════════════════════════════
    # A: LightGBM with LabelEncoder (previous test)
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("A: LightGBM + LabelEncoder (all features)")
    print("=" * 70)
    X_tr, y_tr, names = prepare_lgbm(train_df.copy(), CATEGORICAL, NUMERICAL)
    X_te, y_te, _ = prepare_lgbm(test_df.copy(), CATEGORICAL, NUMERICAL)
    auc_a, mdl_a = train_lgbm(X_tr, y_tr, X_te, y_te)
    print_importance(mdl_a, names, "LightGBM + LabelEncoder", auc_a)

    # ════════════════════════════════════════════
    # B: CatBoost with native categoricals (matches production)
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("B: CatBoost + native categoricals (same as production)")
    print("=" * 70)
    X_tr_cb, y_tr_cb, names_cb, cat_idx = prepare_catboost(train_df.copy(), CATEGORICAL, NUMERICAL)
    X_te_cb, y_te_cb, _, _ = prepare_catboost(test_df.copy(), CATEGORICAL, NUMERICAL)
    auc_b, mdl_b = train_catboost(X_tr_cb, y_tr_cb, X_te_cb, y_te_cb, cat_idx)
    print_importance(mdl_b, names_cb, "CatBoost native", auc_b, is_catboost=True)

    # ════════════════════════════════════════════
    # C: CatBoost WITHOUT processor memory
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("C: CatBoost WITHOUT last_approved_processor + initial_processor")
    print("=" * 70)
    cat_no_proc = [c for c in CATEGORICAL if c not in ('last_approved_processor', 'initial_processor')]
    X_tr_c, y_tr_c, names_c, cat_idx_c = prepare_catboost(train_df.copy(), cat_no_proc, NUMERICAL)
    X_te_c, y_te_c, _, _ = prepare_catboost(test_df.copy(), cat_no_proc, NUMERICAL)
    auc_c, mdl_c = train_catboost(X_tr_c, y_tr_c, X_te_c, y_te_c, cat_idx_c)
    print_importance(mdl_c, names_c, "CatBoost honest", auc_c, is_catboost=True)

    # ════════════════════════════════════════════
    # D: LightGBM WITHOUT processor memory
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("D: LightGBM WITHOUT last_approved_processor + initial_processor")
    print("=" * 70)
    cat_no_proc_lgb = [c for c in CATEGORICAL if c not in ('last_approved_processor', 'initial_processor')]
    X_tr_d, y_tr_d, names_d = prepare_lgbm(train_df.copy(), cat_no_proc_lgb, NUMERICAL)
    X_te_d, y_te_d, _ = prepare_lgbm(test_df.copy(), cat_no_proc_lgb, NUMERICAL)
    auc_d, mdl_d = train_lgbm(X_tr_d, y_tr_d, X_te_d, y_te_d)
    print_importance(mdl_d, names_d, "LightGBM honest", auc_d)

    # ════════════════════════════════════════════
    # E: C1 ONLY — CatBoost with all features
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("E: C1 ONLY — CatBoost (your main interest)")
    print("=" * 70)
    c1_df = df[df['derived_cycle'] == 1].copy()
    c1_split = int(len(c1_df) * TRAIN_RATIO)
    c1_train = c1_df.iloc[:c1_split].copy()
    c1_test = c1_df.iloc[c1_split:].copy()
    print(f"  C1 rows: {len(c1_df):,} ({len(c1_df)/len(df)*100:.1f}% of rebills)")
    print(f"  C1 approval rate: {c1_df['label'].mean():.1%}")
    print(f"  C1 train: {len(c1_train):,} | test: {len(c1_test):,}")

    if len(c1_test) > 50 and len(set(c1_test['label'])) >= 2:
        X_tr_e, y_tr_e, names_e, cat_idx_e = prepare_catboost(c1_train, CATEGORICAL, NUMERICAL)
        X_te_e, y_te_e, _, _ = prepare_catboost(c1_test, CATEGORICAL, NUMERICAL)
        auc_e, mdl_e = train_catboost(X_tr_e, y_tr_e, X_te_e, y_te_e, cat_idx_e)
        print_importance(mdl_e, names_e, "C1 CatBoost", auc_e, is_catboost=True)
    else:
        auc_e = 0
        print("  Not enough C1 data")

    # ════════════════════════════════════════════
    # F: C1 ONLY — CatBoost WITHOUT processor memory
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("F: C1 ONLY — CatBoost WITHOUT processor memory (honest)")
    print("=" * 70)
    if len(c1_test) > 50 and len(set(c1_test['label'])) >= 2:
        c1_train_f = c1_train.copy()
        c1_test_f = c1_test.copy()
        X_tr_f, y_tr_f, names_f, cat_idx_f = prepare_catboost(c1_train_f, cat_no_proc, NUMERICAL)
        X_te_f, y_te_f, _, _ = prepare_catboost(c1_test_f, cat_no_proc, NUMERICAL)
        auc_f, mdl_f = train_catboost(X_tr_f, y_tr_f, X_te_f, y_te_f, cat_idx_f)
        print_importance(mdl_f, names_f, "C1 CatBoost honest", auc_f, is_catboost=True)
    else:
        auc_f = 0

    # ════════════════════════════════════════════
    # SUMMARY
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("SUMMARY — Why AUC differs")
    print("=" * 70)
    print(f"\n  ALL REBILLS:")
    print(f"    A: LightGBM + LabelEncoder + all features   {auc_a:.4f}")
    print(f"    B: CatBoost + native cats + all features    {auc_b:.4f}  ← matches production")
    print(f"    C: CatBoost WITHOUT proc memory             {auc_c:.4f}  ← honest CatBoost")
    print(f"    D: LightGBM WITHOUT proc memory             {auc_d:.4f}  ← honest LightGBM")
    print(f"\n  C1 ONLY:")
    print(f"    E: CatBoost + all features                  {auc_e:.4f}")
    print(f"    F: CatBoost WITHOUT proc memory             {auc_f:.4f}  ← honest C1")
    print(f"\n  KEY FINDINGS:")
    print(f"    CatBoost vs LightGBM gap (with proc):       {(auc_b - auc_a)*100:+.2f}pp")
    print(f"    CatBoost vs LightGBM gap (without proc):    {(auc_c - auc_d)*100:+.2f}pp")
    print(f"    Processor memory boost (CatBoost):          {(auc_b - auc_c)*100:+.2f}pp")
    print(f"    Processor memory boost (LightGBM):          {(auc_a - auc_d)*100:+.2f}pp")


if __name__ == '__main__':
    main()
