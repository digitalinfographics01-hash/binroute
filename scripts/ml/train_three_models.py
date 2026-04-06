"""
BinRoute AI — Three-Model Training

Trains separate models for:
  1. Initial + Cascade — routing first-time orders
  2. Rebill — routing natural rebill attempts
  3. Rebill Salvage — retrying failed rebills

Each model uses features specific to its scenario.

Usage: python3 scripts/ml/train_three_models.py [--db=PATH]
"""

import os
import sys
import json
import time
import sqlite3
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score, f1_score, precision_score, recall_score
import lightgbm as lgb

warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models')

# Override DB path from args
for arg in sys.argv:
    if arg.startswith('--db='):
        DB_PATH = arg.split('=', 1)[1]

TRAIN_RATIO = 0.80

# ── Feature definitions per model ──

SHARED_CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'cycle_depth', 'offer_name', 'training_client_id', 'billing_state',
]

SHARED_NUMERICAL = [
    'is_prepaid', 'amount', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'mid_age_days',
]

MODEL_CONFIGS = {
    'initial_cascade': {
        'name': 'Initial + Cascade',
        'filter': "tx_class IN ('initial', 'upsell', 'cascade')",
        'categorical': SHARED_CATEGORICAL + ['tx_class'],
        'numerical': SHARED_NUMERICAL + [
            'attempt_number', 'cascade_depth',
            'cascade_n_processors', 'cascade_had_nsf',
            'cascade_had_do_not_honor', 'cascade_had_pickup',
        ],
        'filename': 'binroute_initial_cascade',
    },
    'rebill': {
        'name': 'Rebill (Natural)',
        'filter': "tx_class = 'rebill'",
        'categorical': SHARED_CATEGORICAL + ['initial_processor', 'last_approved_processor'],
        'numerical': SHARED_NUMERICAL + [
            'consecutive_approvals', 'days_since_last_charge',
            'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
            'initial_amount', 'amount_ratio',
        ],
        'filename': 'binroute_rebill',
    },
    'rebill_salvage': {
        'name': 'Rebill Salvage',
        'filter': "tx_class = 'salvage'",
        'categorical': SHARED_CATEGORICAL + [
            'initial_processor', 'last_approved_processor',
            'parent_declined_processor', 'prev_decline_reason',
        ],
        'numerical': SHARED_NUMERICAL + [
            'attempt_number',
            'consecutive_approvals', 'days_since_last_charge',
            'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
            'initial_amount', 'amount_ratio', 'prior_declines_in_cycle',
            'cascade_depth', 'cascade_n_processors',
        ],
        'filename': 'binroute_rebill_salvage',
    },
}


def main():
    print("=" * 70)
    print("BinRoute AI — Three-Model Training")
    print("=" * 70)
    print(f"  DB: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    # Load all data
    df_all = pd.read_sql_query("""
        SELECT * FROM tx_features
        WHERE feature_version >= 2
        ORDER BY acquisition_date ASC, id ASC
    """, conn)
    conn.close()

    print(f"  Total rows: {len(df_all):,}")
    df_all['label'] = (df_all['outcome'] == 'approved').astype(int)

    # Derive cascade features
    df_all['cascade_n_processors'] = df_all['cascade_processors_tried'].apply(
        lambda x: len(x.split(',')) if pd.notna(x) and x else 0)
    df_all['cascade_had_nsf'] = df_all['cascade_decline_reasons'].apply(
        lambda x: 1 if pd.notna(x) and 'Insufficient' in x else 0)
    df_all['cascade_had_do_not_honor'] = df_all['cascade_decline_reasons'].apply(
        lambda x: 1 if pd.notna(x) and 'Do Not Honor' in x else 0)
    df_all['cascade_had_pickup'] = df_all['cascade_decline_reasons'].apply(
        lambda x: 1 if pd.notna(x) and 'Pick up' in x else 0)

    results = {}

    for model_key, config in MODEL_CONFIGS.items():
        print(f"\n{'='*70}")
        print(f"  MODEL: {config['name']}")
        print(f"{'='*70}")

        # Filter data
        df = df_all.query(config['filter']).copy()
        if len(df) < 100:
            print(f"  SKIP — only {len(df)} rows")
            continue

        print(f"  Rows: {len(df):,}")
        print(f"  Approved: {df['label'].sum():,} ({df['label'].mean():.1%})")
        print(f"  Declined: {(1-df['label']).sum():,} ({(1-df['label']).mean():.1%})")

        # Encode categoricals
        encoders = {}
        encoded_cols = []
        for col in config['categorical']:
            le = LabelEncoder()
            values = df[col].fillna('UNKNOWN').astype(str)
            le.fit(values)
            df[f'{col}_enc'] = le.transform(values)
            encoders[col] = le
            encoded_cols.append(f'{col}_enc')

        # Fill numerical NAs
        for col in config['numerical']:
            df[col] = df[col].fillna(0)

        # Build feature matrix
        feature_cols = encoded_cols + config['numerical']
        X = df[feature_cols].values.astype(np.float32)
        y = df['label'].values
        feature_names = [col.replace('_enc', '') for col in feature_cols]

        # Time-based split
        split_idx = int(len(X) * TRAIN_RATIO)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        print(f"  Train: {len(X_train):,} | Test: {len(X_test):,}")
        print(f"  Train approval: {y_train.mean():.1%} | Test approval: {y_test.mean():.1%}")

        # Train LightGBM
        scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
        model = lgb.LGBMClassifier(
            n_estimators=300, max_depth=8, learning_rate=0.1,
            subsample=0.8, colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
            verbose=-1, random_state=42
        )

        start = time.time()
        model.fit(X_train, y_train)
        elapsed = time.time() - start

        y_prob = model.predict_proba(X_test)[:, 1]
        y_pred = model.predict(X_test)

        auc = roc_auc_score(y_test, y_prob)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec = recall_score(y_test, y_pred, zero_division=0)

        print(f"\n  AUC-ROC: {auc:.4f} | F1: {f1:.4f} | Precision: {prec:.4f} | Recall: {rec:.4f} | Time: {elapsed:.1f}s")

        # Feature importance
        importances = sorted(zip(feature_names, model.feature_importances_),
                           key=lambda x: x[1], reverse=True)
        print(f"\n  Top 10 Features:")
        for i, (feat, imp) in enumerate(importances[:10]):
            print(f"    {i+1:>2}. {feat:<30} {imp:.0f}")

        # Per cycle-depth AUC
        test_df = df.iloc[split_idx:].copy()
        test_df['y_prob'] = y_prob
        test_df['y_true'] = y_test

        print(f"\n  Per Cycle Depth:")
        for cd in ['C0', 'C1', 'C2', 'C3+']:
            if cd not in encoders.get('cycle_depth', LabelEncoder()).classes_:
                continue
            cd_enc = encoders['cycle_depth'].transform([cd])[0]
            mask = test_df['cycle_depth_enc'] == cd_enc
            sub = test_df[mask]
            if len(sub) >= 20 and sub['y_true'].nunique() >= 2:
                cd_auc = roc_auc_score(sub['y_true'], sub['y_prob'])
                print(f"    {cd:<6} {len(sub):>6,} orders, {sub['y_true'].mean()*100:>5.1f}% appr, AUC: {cd_auc:.4f}")

        # Save model
        import pickle
        pkl_path = os.path.join(OUTPUT_DIR, f'{config["filename"]}.pkl')
        with open(pkl_path, 'wb') as f:
            pickle.dump(model, f)
        print(f"\n  Saved: {pkl_path}")

        results[model_key] = {
            'name': config['name'],
            'rows': len(df),
            'auc': auc, 'f1': f1, 'precision': prec, 'recall': rec,
            'train_time': elapsed,
            'features': feature_names,
            'top_features': [(f, float(i)) for f, i in importances[:15]],
        }

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY — Three Model Comparison")
    print(f"{'='*70}")
    print(f"\n  {'Model':<25} {'Rows':>8} {'AUC':>8} {'F1':>8} {'Prec':>8} {'Recall':>8}")
    print(f"  {'-'*67}")
    for key, r in results.items():
        print(f"  {r['name']:<25} {r['rows']:>8,} {r['auc']:>7.4f} {r['f1']:>7.4f} {r['precision']:>7.4f} {r['recall']:>7.4f}")

    # Compare vs blended
    print(f"\n  Previous blended model AUC: 0.9119")
    print(f"  Note: Blended model compromised across all tx types.")
    print(f"  Specialized models should outperform on their specific scenarios.")

    # Save results
    results_path = os.path.join(OUTPUT_DIR, 'three_model_results.json')
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Results saved: {results_path}")
    print("\nDone!")


if __name__ == '__main__':
    main()
