"""
BinRoute AI — Rebill: Model vs Lookup Table Test

Tests whether the lookup table adds value on top of the model.

Variants:
  A: Model only (0.96 AUC baseline)
  B: Lookup table only (historical 4D rates as features)
  C: Model + Lookup combined (does lookup add signal?)
  D: Model on cases where lookup has NO data (cold start)
  E: Model on cases where lookup HAS data (redundancy test)

Usage: python3 scripts/ml/test_rebill_lookup_vs_model.py [--db=PATH]
"""

import os
import sys
import sqlite3
import warnings
import numpy as np
import pandas as pd
import json

from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score

import lightgbm as lgb

warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
LOOKUP_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models', 'rebill_first_attempt_lookup.json')

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
            ta.issuer_bank, ta.card_brand, ta.card_type, ta.is_prepaid,
            ta.hour_of_day, ta.day_of_week, ta.mid_age_days,
            ta.offer_name, ta.billing_state,
            ta.mid_velocity_daily, ta.mid_velocity_weekly,
            ta.customer_history_on_proc, ta.bin_velocity_weekly,
            ta.initial_processor, ta.last_approved_processor,
            ta.parent_declined_processor, ta.prev_decline_reason,
            ta.consecutive_approvals, ta.days_since_last_charge,
            ta.days_since_initial, ta.lifetime_charges, ta.lifetime_revenue,
            ta.initial_amount, ta.amount_ratio, ta.initial_was_payfac,
            ta.prior_declines_in_cycle,
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
    return df


def load_lookup():
    """Load rebill lookup table and return as dict."""
    if not os.path.exists(LOOKUP_PATH):
        print(f"  WARNING: Lookup table not found at {LOOKUP_PATH}")
        return None
    with open(LOOKUP_PATH) as f:
        data = json.load(f)
    # The lookup is structured as entries with keys like "issuer|card_type|initial_proc|target"
    print(f"  Loaded rebill lookup: {len(data)} entries")
    return data


def add_lookup_features(df, lookup):
    """
    For each row, look up the historical approval rate from the lookup table.
    Match on: issuer_bank × card_type × initial_processor × processor_name (target)
    """
    lookup_rate = np.full(len(df), np.nan)
    lookup_volume = np.zeros(len(df))
    lookup_has_data = np.zeros(len(df), dtype=int)

    if lookup is None:
        df['lookup_rate'] = np.nan
        df['lookup_volume'] = 0
        df['lookup_has_data'] = 0
        return df

    # Build a flat lookup dict from whatever structure the JSON has
    flat = {}
    if isinstance(lookup, list):
        for entry in lookup:
            key = (
                str(entry.get('issuer_bank', '')),
                str(entry.get('card_type', '')),
                str(entry.get('initial_processor', '')),
                str(entry.get('target', entry.get('processor_name', ''))),
            )
            flat[key] = {
                'rate': entry.get('approval_rate', entry.get('rate', 0)),
                'volume': entry.get('total', entry.get('volume', entry.get('attempts', 0))),
            }
    elif isinstance(lookup, dict):
        # Could be nested or flat key structure
        for key_str, val in lookup.items():
            if isinstance(val, dict):
                rate = val.get('approval_rate', val.get('rate', 0))
                volume = val.get('total', val.get('volume', val.get('attempts', 0)))
                parts = key_str.split('|')
                if len(parts) == 4:
                    flat[tuple(parts)] = {'rate': rate, 'volume': volume}
                elif len(parts) == 3:
                    flat[tuple(parts)] = {'rate': rate, 'volume': volume}

    print(f"  Flat lookup entries: {len(flat)}")

    matched = 0
    for i in range(len(df)):
        issuer = str(df.iloc[i]['issuer_bank'] or '')
        ct = str(df.iloc[i]['card_type'] or '')
        init_proc = str(df.iloc[i]['initial_processor'] or '')
        proc = str(df.iloc[i]['processor_name'] or '')

        # Try 4D match
        key4 = (issuer, ct, init_proc, proc)
        if key4 in flat:
            entry = flat[key4]
            lookup_rate[i] = entry['rate']
            lookup_volume[i] = entry['volume']
            lookup_has_data[i] = 1
            matched += 1
            continue

        # Try 3D match (without initial_processor)
        key3 = (issuer, ct, proc)
        if key3 in flat:
            entry = flat[key3]
            lookup_rate[i] = entry['rate']
            lookup_volume[i] = entry['volume']
            lookup_has_data[i] = 1
            matched += 1

    df['lookup_rate'] = lookup_rate
    df['lookup_volume'] = lookup_volume
    df['lookup_has_data'] = lookup_has_data

    pct = matched / len(df) * 100
    print(f"  Lookup matched: {matched:,}/{len(df):,} ({pct:.1f}%)")
    return df


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


def print_importance(model, names, label, auc, top_n=10):
    importances = model.feature_importances_
    total = importances.sum()
    pairs = sorted(zip(names, importances), key=lambda x: -x[1])
    print(f"\n  {label} — AUC: {auc:.4f}")
    print(f"  {'Feature':<30} {'Imp':>6} {'%':>7}")
    print(f"  {'-'*45}")
    for fname, imp in pairs[:top_n]:
        print(f"  {fname:<30} {imp:>6.0f} {imp/total*100:>6.1f}%")


def main():
    print("=" * 70)
    print("Rebill: Model vs Lookup Table Redundancy Test")
    print("=" * 70)

    df = load_data()
    lookup = load_lookup()
    df = add_lookup_features(df, lookup)

    split_idx = int(len(df) * TRAIN_RATIO)
    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()
    print(f"\n  Train: {len(train_df):,} | Test: {len(test_df):,}")

    # ════════════════════════════════════════════
    # A: Model only (current production)
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("A: MODEL ONLY (current production)")
    print("=" * 70)
    X_tr, y_tr, names = prepare(train_df.copy(), CATEGORICAL, NUMERICAL)
    X_te, y_te, _ = prepare(test_df.copy(), CATEGORICAL, NUMERICAL)
    auc_model, mdl_a = train_lgbm(X_tr, y_tr, X_te, y_te)
    print_importance(mdl_a, names, "MODEL ONLY", auc_model)

    # ════════════════════════════════════════════
    # B: Lookup rate only (as a single feature baseline)
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("B: LOOKUP TABLE AS SOLE PREDICTOR")
    print("=" * 70)
    has_lookup_test = test_df['lookup_has_data'].values == 1
    if has_lookup_test.sum() > 50:
        lookup_preds = test_df.loc[has_lookup_test, 'lookup_rate'].fillna(0.5).values
        lookup_labels = test_df.loc[has_lookup_test, 'label'].values
        if len(set(lookup_labels)) >= 2:
            auc_lookup = roc_auc_score(lookup_labels, lookup_preds)
            print(f"  Lookup-only AUC (on {has_lookup_test.sum():,} matched rows): {auc_lookup:.4f}")
        else:
            auc_lookup = 0
            print("  Cannot compute AUC — single class in matched rows")
    else:
        auc_lookup = 0
        print(f"  Only {has_lookup_test.sum()} matched rows — not enough for AUC")

    # ════════════════════════════════════════════
    # C: Model + Lookup combined
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("C: MODEL + LOOKUP COMBINED")
    print("=" * 70)
    combined_num = NUMERICAL + ['lookup_rate', 'lookup_volume', 'lookup_has_data']
    X_tr, y_tr, names = prepare(train_df.copy(), CATEGORICAL, combined_num)
    X_te, y_te, _ = prepare(test_df.copy(), CATEGORICAL, combined_num)
    auc_combined, mdl_c = train_lgbm(X_tr, y_tr, X_te, y_te)
    print_importance(mdl_c, names, "MODEL + LOOKUP", auc_combined)

    delta_c = (auc_combined - auc_model) * 100
    print(f"\n  Delta vs model-only: {'+' if delta_c >= 0 else ''}{delta_c:.2f}pp")

    # ════════════════════════════════════════════
    # D: Model on rows WHERE LOOKUP HAS NO DATA
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("D: MODEL PERFORMANCE — COLD START (no lookup data)")
    print("=" * 70)
    cold_train = train_df[train_df['lookup_has_data'] == 0].copy()
    cold_test = test_df[test_df['lookup_has_data'] == 0].copy()
    if len(cold_test) > 50 and len(set(cold_test['label'])) >= 2:
        X_tr, y_tr, _ = prepare(cold_train.copy(), CATEGORICAL, NUMERICAL)
        X_te, y_te, _ = prepare(cold_test.copy(), CATEGORICAL, NUMERICAL)
        auc_cold, _ = train_lgbm(X_tr, y_tr, X_te, y_te)
        print(f"  Cold start rows: {len(cold_test):,} ({len(cold_test)/len(test_df)*100:.1f}% of test)")
        print(f"  Model AUC on cold start: {auc_cold:.4f}")
    else:
        auc_cold = 0
        print(f"  Only {len(cold_test)} cold-start test rows — not enough")

    # ════════════════════════════════════════════
    # E: Model on rows WHERE LOOKUP HAS DATA
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("E: MODEL PERFORMANCE — WARM (lookup has data)")
    print("=" * 70)
    warm_train = train_df[train_df['lookup_has_data'] == 1].copy()
    warm_test = test_df[test_df['lookup_has_data'] == 1].copy()
    if len(warm_test) > 50 and len(set(warm_test['label'])) >= 2:
        X_tr, y_tr, _ = prepare(warm_train.copy(), CATEGORICAL, NUMERICAL)
        X_te, y_te, _ = prepare(warm_test.copy(), CATEGORICAL, NUMERICAL)
        auc_warm, _ = train_lgbm(X_tr, y_tr, X_te, y_te)
        print(f"  Warm rows: {len(warm_test):,} ({len(warm_test)/len(test_df)*100:.1f}% of test)")
        print(f"  Model AUC on warm: {auc_warm:.4f}")
        print(f"  Lookup AUC on same rows: {auc_lookup:.4f}")
        if auc_lookup > 0:
            print(f"  Model advantage: {'+' if auc_warm > auc_lookup else ''}{(auc_warm-auc_lookup)*100:.2f}pp")
    else:
        auc_warm = 0
        print(f"  Only {len(warm_test)} warm test rows — not enough")

    # ════════════════════════════════════════════
    # F: Without last_approved_processor (honest test)
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("F: MODEL WITHOUT last_approved_processor + initial_processor")
    print("=" * 70)
    cat_honest = [c for c in CATEGORICAL if c not in ('last_approved_processor', 'initial_processor')]
    X_tr, y_tr, names = prepare(train_df.copy(), cat_honest, NUMERICAL)
    X_te, y_te, _ = prepare(test_df.copy(), cat_honest, NUMERICAL)
    auc_honest, mdl_f = train_lgbm(X_tr, y_tr, X_te, y_te)
    print_importance(mdl_f, names, "HONEST MODEL (no proc memory)", auc_honest)

    # G: Honest model + lookup
    print("\n" + "=" * 70)
    print("G: HONEST MODEL + LOOKUP (does lookup help when model is weaker?)")
    print("=" * 70)
    X_tr, y_tr, names = prepare(train_df.copy(), cat_honest, combined_num)
    X_te, y_te, _ = prepare(test_df.copy(), cat_honest, combined_num)
    auc_honest_lookup, mdl_g = train_lgbm(X_tr, y_tr, X_te, y_te)
    print_importance(mdl_g, names, "HONEST + LOOKUP", auc_honest_lookup)
    delta_g = (auc_honest_lookup - auc_honest) * 100
    print(f"\n  Lookup adds: {'+' if delta_g >= 0 else ''}{delta_g:.2f}pp to honest model")

    # ════════════════════════════════════════════
    # SUMMARY
    # ════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  A: Model only                  {auc_model:.4f}")
    print(f"  B: Lookup only                 {auc_lookup:.4f}")
    print(f"  C: Model + Lookup              {auc_combined:.4f}  ({'+' if auc_combined >= auc_model else ''}{(auc_combined-auc_model)*100:.2f}pp vs A)")
    print(f"  D: Model on cold-start         {auc_cold:.4f}")
    print(f"  E: Model on warm               {auc_warm:.4f}")
    print(f"  F: Honest model (no proc mem)  {auc_honest:.4f}")
    print(f"  G: Honest + Lookup             {auc_honest_lookup:.4f}  ({'+' if auc_honest_lookup >= auc_honest else ''}{(auc_honest_lookup-auc_honest)*100:.2f}pp vs F)")
    print()
    if abs(auc_combined - auc_model) < 0.005:
        print(f"  VERDICT: Lookup is REDUNDANT for rebill — model already captures everything")
    elif auc_combined > auc_model + 0.005:
        print(f"  VERDICT: Lookup ADDS VALUE — keep it alongside model")
    else:
        print(f"  VERDICT: Lookup SLIGHTLY HURTS — drop it")


if __name__ == '__main__':
    main()
