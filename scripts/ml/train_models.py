"""
BinRoute AI — Layer 3: Model Training Tournament

Trains 4 models on tx_features data, compares them, and exports the winner to ONNX.

Models:
  1. Logistic Regression (baseline)
  2. Random Forest
  3. XGBoost
  4. LightGBM

Split: Time-based (oldest 80% train, newest 20% test) — mirrors production reality.

Usage: py -3 scripts/ml/train_models.py
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

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, classification_report, confusion_matrix
)

import xgboost as xgb
import lightgbm as lgb

warnings.filterwarnings('ignore')

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models')
TRAIN_RATIO = 0.80  # 80% train, 20% test (time-based)

# Features to use for training
CATEGORICAL_FEATURES = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'tx_class', 'cycle_depth', 'prev_decline_reason',
    'initial_processor', 'offer_name',
]

NUMERICAL_FEATURES = [
    'is_prepaid', 'amount', 'attempt_number',
    'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    # Layer 2.5 — subscription health
    'consecutive_approvals', 'days_since_last_charge',
    'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
    'initial_amount', 'amount_ratio', 'prior_declines_in_cycle',
    # Layer 3 — cascade chain + MID age
    'cascade_depth', 'mid_age_days',
    # Derived cascade features
    'cascade_n_processors', 'cascade_had_nsf',
    'cascade_had_do_not_honor', 'cascade_had_pickup',
]

TARGET = 'outcome'


def main():
    print("=" * 60)
    print("BinRoute AI — Model Training Tournament")
    print("=" * 60)

    # --- Load data ---
    print("\n[1/5] Loading tx_features from database...")
    df = load_data()
    print(f"  Loaded {len(df):,} rows")
    print(f"  Approved: {(df['label'] == 1).sum():,} ({(df['label'] == 1).mean():.1%})")
    print(f"  Declined: {(df['label'] == 0).sum():,} ({(df['label'] == 0).mean():.1%})")

    # --- Prepare features ---
    print("\n[2/5] Preparing features...")
    X, y, feature_names, encoders = prepare_features(df)
    print(f"  Feature matrix: {X.shape[0]:,} rows x {X.shape[1]} features")

    # --- Time-based split ---
    print("\n[3/5] Time-based train/test split...")
    split_idx = int(len(X) * TRAIN_RATIO)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    print(f"  Train: {len(X_train):,} rows (oldest {TRAIN_RATIO:.0%})")
    print(f"  Test:  {len(X_test):,} rows (newest {1-TRAIN_RATIO:.0%})")
    print(f"  Train approval rate: {y_train.mean():.1%}")
    print(f"  Test  approval rate: {y_test.mean():.1%}")

    # --- Train models ---
    print("\n[4/5] Training models...\n")
    results = {}

    # Model 1: Logistic Regression
    results['Logistic Regression'] = train_and_evaluate(
        'Logistic Regression',
        LogisticRegression(max_iter=1000, solver='lbfgs', C=1.0, class_weight='balanced'),
        X_train, X_test, y_train, y_test
    )

    # Model 2: Random Forest
    results['Random Forest'] = train_and_evaluate(
        'Random Forest',
        RandomForestClassifier(
            n_estimators=200, max_depth=12, min_samples_leaf=20,
            class_weight='balanced', n_jobs=-1, random_state=42
        ),
        X_train, X_test, y_train, y_test
    )

    # Model 3: XGBoost
    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    results['XGBoost'] = train_and_evaluate(
        'XGBoost',
        xgb.XGBClassifier(
            n_estimators=300, max_depth=8, learning_rate=0.1,
            subsample=0.8, colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
            eval_metric='logloss', random_state=42,
            tree_method='hist'
        ),
        X_train, X_test, y_train, y_test
    )

    # Model 4: LightGBM
    results['LightGBM'] = train_and_evaluate(
        'LightGBM',
        lgb.LGBMClassifier(
            n_estimators=300, max_depth=8, learning_rate=0.1,
            subsample=0.8, colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
            verbose=-1, random_state=42
        ),
        X_train, X_test, y_train, y_test
    )

    # --- Compare and pick winner ---
    print("\n[5/5] Tournament Results\n")
    print_tournament_results(results)

    winner_name = max(results, key=lambda k: results[k]['auc_roc'])
    winner = results[winner_name]
    print(f"\n  WINNER: {winner_name} (AUC-ROC: {winner['auc_roc']:.4f})")

    # --- Feature importance from best tree model ---
    print("\n--- Feature Importance (from best tree model) ---\n")
    best_tree_name = max(
        [k for k in results if k != 'Logistic Regression'],
        key=lambda k: results[k]['auc_roc']
    )
    print_feature_importance(results[best_tree_name]['model'], feature_names, best_tree_name)

    # --- Per-transaction-type evaluation ---
    print(f"\n--- Per Transaction Type AUC ({winner_name}) ---\n")
    evaluate_per_tx_type(winner['model'], df, split_idx, feature_names, encoders)

    # --- Export winner to ONNX ---
    print(f"\n--- Exporting {winner_name} to ONNX ---\n")
    export_to_onnx(winner['model'], winner_name, feature_names, X_train)

    # --- Save results summary ---
    save_results(results, feature_names, winner_name, len(df),
                 len(X_train), len(X_test))

    print("\nDone!")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data():
    """Load tx_features from SQLite, sorted by acquisition_date for time-based split."""
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
               cascade_depth, mid_age_days,
               cascade_processors_tried, cascade_decline_reasons,
               offer_name, outcome, acquisition_date
        FROM tx_features
        WHERE feature_version >= 2
        ORDER BY acquisition_date ASC, id ASC
    """, conn)
    conn.close()

    # Binary label: 1 = approved, 0 = declined
    df['label'] = (df['outcome'] == 'approved').astype(int)

    # Derive features from cascade chain strings
    # Number of unique processors tried in cascade
    df['cascade_n_processors'] = df['cascade_processors_tried'].apply(
        lambda x: len(x.split(',')) if pd.notna(x) and x else 0
    )
    # Whether specific hard-decline reasons appeared in cascade
    df['cascade_had_nsf'] = df['cascade_decline_reasons'].apply(
        lambda x: 1 if pd.notna(x) and 'Insufficient funds' in x else 0
    )
    df['cascade_had_do_not_honor'] = df['cascade_decline_reasons'].apply(
        lambda x: 1 if pd.notna(x) and 'Do Not Honor' in x else 0
    )
    df['cascade_had_pickup'] = df['cascade_decline_reasons'].apply(
        lambda x: 1 if pd.notna(x) and 'Pick up card' in x else 0
    )

    return df


# ---------------------------------------------------------------------------
# Feature preparation
# ---------------------------------------------------------------------------

def prepare_features(df):
    """Encode categoricals, fill NAs, scale numericals."""
    encoders = {}
    encoded_cols = []

    # Label-encode categoricals
    for col in CATEGORICAL_FEATURES:
        le = LabelEncoder()
        # Fill NAs with 'UNKNOWN' before encoding
        values = df[col].fillna('UNKNOWN').astype(str)
        le.fit(values)
        df[f'{col}_enc'] = le.transform(values)
        encoders[col] = le
        encoded_cols.append(f'{col}_enc')

    # Fill NA numericals with 0
    for col in NUMERICAL_FEATURES:
        df[col] = df[col].fillna(0)

    # Build feature matrix
    feature_cols = encoded_cols + NUMERICAL_FEATURES
    X = df[feature_cols].values.astype(np.float32)
    y = df['label'].values

    feature_names = [col.replace('_enc', '') for col in feature_cols]

    return X, y, feature_names, encoders


# ---------------------------------------------------------------------------
# Training & evaluation
# ---------------------------------------------------------------------------

def train_and_evaluate(name, model, X_train, X_test, y_train, y_test):
    """Train a model and evaluate on test set."""
    print(f"  Training {name}...", end=" ", flush=True)
    start = time.time()

    model.fit(X_train, y_train)

    elapsed = time.time() - start
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    metrics = {
        'model': model,
        'accuracy': accuracy_score(y_test, y_pred),
        'precision': precision_score(y_test, y_pred, zero_division=0),
        'recall': recall_score(y_test, y_pred, zero_division=0),
        'f1': f1_score(y_test, y_pred, zero_division=0),
        'auc_roc': roc_auc_score(y_test, y_prob),
        'train_time': elapsed,
    }

    print(f"AUC-ROC: {metrics['auc_roc']:.4f} ({elapsed:.1f}s)")
    return metrics


def evaluate_per_tx_type(model, df, split_idx, feature_names, encoders):
    """Evaluate model AUC per transaction type and cycle depth."""
    test_df = df.iloc[split_idx:].copy()

    # Rebuild X for test set
    encoded_cols = [f'{col}_enc' for col in CATEGORICAL_FEATURES]
    feature_cols = encoded_cols + NUMERICAL_FEATURES
    X_test = test_df[feature_cols].values.astype(np.float32)
    y_test = test_df['label'].values
    y_prob = model.predict_proba(X_test)[:, 1]

    test_df['y_prob'] = y_prob
    test_df['y_true'] = y_test

    # Per tx_class
    print(f"  {'TX Class':<20} {'Count':>7} {'Appr%':>6} {'AUC':>7}")
    print(f"  " + "-" * 42)
    for tx in sorted(test_df['tx_class'].unique()):
        mask = test_df['tx_class'] == tx
        sub = test_df[mask]
        if len(sub) < 20 or sub['y_true'].nunique() < 2:
            continue
        auc = roc_auc_score(sub['y_true'], sub['y_prob'])
        appr = sub['y_true'].mean() * 100
        print(f"  {tx:<20} {len(sub):>7,} {appr:>5.1f}% {auc:>6.4f}")

    # Per cycle_depth
    print(f"\n  {'Cycle Depth':<20} {'Count':>7} {'Appr%':>6} {'AUC':>7}")
    print(f"  " + "-" * 42)
    for cd in ['C0', 'C1', 'C2', 'C3+']:
        mask = test_df['cycle_depth_enc'] == encoders['cycle_depth'].transform([cd])[0] if cd in encoders['cycle_depth'].classes_ else pd.Series([False] * len(test_df))
        sub = test_df[mask]
        if len(sub) < 20 or sub['y_true'].nunique() < 2:
            continue
        auc = roc_auc_score(sub['y_true'], sub['y_prob'])
        appr = sub['y_true'].mean() * 100
        print(f"  {cd:<20} {len(sub):>7,} {appr:>5.1f}% {auc:>6.4f}")

    # Per tx_class + cycle_depth combo
    print(f"\n  {'TX Class + Cycle':<25} {'Count':>7} {'Appr%':>6} {'AUC':>7}")
    print(f"  " + "-" * 47)
    for tx in ['initial', 'upsell', 'rebill', 'cascade', 'salvage']:
        for cd in ['C0', 'C1', 'C2', 'C3+']:
            tx_mask = test_df['tx_class'] == tx
            if cd in encoders['cycle_depth'].classes_:
                cd_mask = test_df['cycle_depth_enc'] == encoders['cycle_depth'].transform([cd])[0]
            else:
                continue
            mask = tx_mask & cd_mask
            sub = test_df[mask]
            if len(sub) < 20 or sub['y_true'].nunique() < 2:
                continue
            auc = roc_auc_score(sub['y_true'], sub['y_prob'])
            appr = sub['y_true'].mean() * 100
            print(f"  {tx + ' ' + cd:<25} {len(sub):>7,} {appr:>5.1f}% {auc:>6.4f}")


# ---------------------------------------------------------------------------
# Results display
# ---------------------------------------------------------------------------

def print_tournament_results(results):
    """Print comparison table."""
    header = f"  {'Model':<22} {'AUC-ROC':>8} {'F1':>8} {'Prec':>8} {'Recall':>8} {'Acc':>8} {'Time':>7}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    sorted_results = sorted(results.items(), key=lambda x: x[1]['auc_roc'], reverse=True)
    for name, m in sorted_results:
        print(f"  {name:<22} {m['auc_roc']:>8.4f} {m['f1']:>8.4f} "
              f"{m['precision']:>8.4f} {m['recall']:>8.4f} {m['accuracy']:>8.4f} "
              f"{m['train_time']:>6.1f}s")


def print_feature_importance(model, feature_names, model_name):
    """Print top features from tree-based model."""
    if hasattr(model, 'feature_importances_'):
        importances = model.feature_importances_
    else:
        print("  (Model does not expose feature importances)")
        return

    # Sort by importance
    indices = np.argsort(importances)[::-1]

    print(f"  {'Rank':<6} {'Feature':<30} {'Importance':>10}")
    print("  " + "-" * 48)
    for rank, idx in enumerate(indices[:19], 1):
        bar = "#" * int(importances[idx] * 50)
        print(f"  {rank:<6} {feature_names[idx]:<30} {importances[idx]:>10.4f}  {bar}")


# ---------------------------------------------------------------------------
# ONNX export
# ---------------------------------------------------------------------------

def export_to_onnx(model, model_name, feature_names, X_train_sample):
    """Export the winning model to ONNX format."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    safe_name = model_name.lower().replace(' ', '_')
    onnx_path = os.path.join(OUTPUT_DIR, f'binroute_{safe_name}.onnx')

    try:
        from skl2onnx import convert_sklearn
        from skl2onnx.common.data_types import FloatTensorType
        from onnxmltools import convert_xgboost, convert_lightgbm
        from onnxmltools.convert.common.data_types import FloatTensorType as OnnxFloatTensorType

        n_features = X_train_sample.shape[1]

        if model_name == 'XGBoost':
            initial_type = [('features', OnnxFloatTensorType([None, n_features]))]
            onnx_model = convert_xgboost(model, initial_types=initial_type)
        elif model_name == 'LightGBM':
            initial_type = [('features', OnnxFloatTensorType([None, n_features]))]
            onnx_model = convert_lightgbm(model, initial_types=initial_type)
        else:
            initial_type = [('features', FloatTensorType([None, n_features]))]
            onnx_model = convert_sklearn(model, initial_types=initial_type)

        import onnx
        onnx.save_model(onnx_model, onnx_path)
        print(f"  Saved: {onnx_path}")
        print(f"  Size: {os.path.getsize(onnx_path) / 1024:.0f} KB")

        # Verify it loads
        import onnxruntime as ort
        sess = ort.InferenceSession(onnx_path)
        test_input = X_train_sample[:5].astype(np.float32)
        input_name = sess.get_inputs()[0].name
        result = sess.run(None, {input_name: test_input})
        print(f"  Verified: ONNX model loads and produces output")

    except Exception as e:
        print(f"  ONNX export failed: {e}")
        print(f"  (Model is still available as pickle — ONNX export can be retried)")

        # Fallback: save as pickle
        import pickle
        pkl_path = os.path.join(OUTPUT_DIR, f'binroute_{safe_name}.pkl')
        with open(pkl_path, 'wb') as f:
            pickle.dump(model, f)
        print(f"  Saved pickle fallback: {pkl_path}")


# ---------------------------------------------------------------------------
# Save results
# ---------------------------------------------------------------------------

def save_results(results, feature_names, winner_name, total_rows, train_rows, test_rows):
    """Save tournament results to JSON for the Node.js dashboard."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    summary = {
        'trained_at': datetime.now().isoformat(),
        'total_rows': total_rows,
        'train_rows': train_rows,
        'test_rows': test_rows,
        'feature_count': len(feature_names),
        'features': feature_names,
        'winner': winner_name,
        'models': {}
    }

    for name, m in results.items():
        summary['models'][name] = {
            'auc_roc': round(m['auc_roc'], 4),
            'f1': round(m['f1'], 4),
            'precision': round(m['precision'], 4),
            'recall': round(m['recall'], 4),
            'accuracy': round(m['accuracy'], 4),
            'train_time_seconds': round(m['train_time'], 2),
        }

        # Feature importance for tree models
        if hasattr(m['model'], 'feature_importances_'):
            importances = m['model'].feature_importances_
            indices = np.argsort(importances)[::-1]
            summary['models'][name]['feature_importance'] = [
                {'feature': feature_names[i], 'importance': round(float(importances[i]), 4)}
                for i in indices
            ]

    results_path = os.path.join(OUTPUT_DIR, 'training_results.json')
    with open(results_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"\n  Results saved: {results_path}")


if __name__ == '__main__':
    main()
