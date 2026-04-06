"""
BinRoute AI — Live Holdout Test

Scores newly synced Kytsan orders with the trained LightGBM model
and compares predictions to actual outcomes.

Usage: py -3 scripts/ml/test_on_new_data.py
"""

import os
import sqlite3
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report
)
import onnxruntime as ort

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models', 'binroute_lightgbm.onnx')

CATEGORICAL_FEATURES = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'tx_class', 'cycle_depth', 'prev_decline_reason',
    'initial_processor',
]

NUMERICAL_FEATURES = [
    'is_prepaid', 'amount', 'attempt_number',
    'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
]


def main():
    print("=" * 60)
    print("BinRoute AI — Live Holdout Test (Kytsan)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # Load ALL tx_features for encoding consistency
    print("\n[1] Loading all tx_features for encoder fitting...")
    all_df = pd.read_sql_query("""
        SELECT processor_name, acquiring_bank, mcc_code,
               issuer_bank, card_brand, card_type, is_prepaid,
               amount, tx_class, attempt_number, cycle_depth,
               hour_of_day, day_of_week, prev_decline_reason,
               initial_processor,
               mid_velocity_daily, mid_velocity_weekly,
               customer_history_on_proc, bin_velocity_weekly,
               outcome, acquisition_date, client_id, id
        FROM tx_features
        WHERE feature_version = 2
        ORDER BY acquisition_date ASC, id ASC
    """, conn)

    # The training data was the oldest 80% — find the cutoff
    train_cutoff_idx = int(len(all_df) * 0.80)
    training_df = all_df.iloc[:train_cutoff_idx]

    # New data = Kytsan rows that are AFTER the training cutoff
    # (these are rows the model has never seen)
    new_df = all_df[(all_df['client_id'] == 1) & (all_df.index >= train_cutoff_idx)]

    if len(new_df) == 0:
        # Try: any Kytsan rows from last 3 days
        print("  No new rows past training cutoff. Looking for recent Kytsan data...")
        new_df = all_df[
            (all_df['client_id'] == 1) &
            (all_df['acquisition_date'] >= (pd.Timestamp.now() - pd.Timedelta(days=3)).strftime('%Y-%m-%d'))
        ]

    if len(new_df) == 0:
        print("  No new Kytsan data found to test. Exiting.")
        conn.close()
        return

    print(f"  Total rows: {len(all_df):,}")
    print(f"  Training cutoff index: {train_cutoff_idx:,}")
    print(f"  New Kytsan rows to score: {len(new_df):,}")
    print(f"  Date range: {new_df['acquisition_date'].min()} to {new_df['acquisition_date'].max()}")

    # Fit encoders on ALL data (so new categories are handled)
    print("\n[2] Encoding features...")
    encoders = {}
    for col in CATEGORICAL_FEATURES:
        le = LabelEncoder()
        all_df[f'{col}_enc'] = le.fit_transform(all_df[col].fillna('UNKNOWN').astype(str))
        encoders[col] = le

    for col in NUMERICAL_FEATURES:
        all_df[col] = all_df[col].fillna(0)

    feature_cols = [f'{col}_enc' for col in CATEGORICAL_FEATURES] + NUMERICAL_FEATURES

    # Extract new data features
    new_X = all_df.loc[new_df.index, feature_cols].values.astype(np.float32)
    new_y = (new_df['outcome'] == 'approved').astype(int).values

    print(f"  Feature matrix: {new_X.shape}")
    print(f"  Actual approvals: {new_y.sum():,} ({new_y.mean():.1%})")
    print(f"  Actual declines: {(new_y == 0).sum():,} ({(new_y == 0).mean():.1%})")

    # Load ONNX model
    print("\n[3] Loading LightGBM ONNX model...")
    sess = ort.InferenceSession(MODEL_PATH)
    input_name = sess.get_inputs()[0].name

    # Score
    print("\n[4] Scoring new transactions...")
    raw_output = sess.run(None, {input_name: new_X})

    # ONNX output: [predictions, probabilities]
    predictions = raw_output[0]
    probabilities = raw_output[1]

    # Extract approval probability
    if isinstance(probabilities, list):
        prob_approved = np.array([p.get(1, p.get('1', 0)) if isinstance(p, dict) else p for p in probabilities])
    elif hasattr(probabilities, 'shape') and len(probabilities.shape) == 2:
        prob_approved = probabilities[:, 1]
    else:
        # probabilities might be list of dicts from LightGBM ONNX
        try:
            prob_approved = np.array([p[1] for p in probabilities])
        except:
            prob_approved = predictions.astype(float)

    y_pred = (prob_approved >= 0.5).astype(int)

    # Metrics
    print("\n" + "=" * 60)
    print("RESULTS — Model vs Reality on Unseen Kytsan Data")
    print("=" * 60)

    acc = accuracy_score(new_y, y_pred)
    prec = precision_score(new_y, y_pred, zero_division=0)
    rec = recall_score(new_y, y_pred, zero_division=0)
    f1 = f1_score(new_y, y_pred, zero_division=0)
    auc = roc_auc_score(new_y, prob_approved)

    print(f"\n  AUC-ROC:    {auc:.4f}")
    print(f"  Accuracy:   {acc:.4f} ({acc:.1%})")
    print(f"  Precision:  {prec:.4f}")
    print(f"  Recall:     {rec:.4f}")
    print(f"  F1:         {f1:.4f}")

    # Confusion matrix
    cm = confusion_matrix(new_y, y_pred)
    print(f"\n  Confusion Matrix:")
    print(f"                    Predicted")
    print(f"                  Decline  Approve")
    print(f"  Actual Decline   {cm[0][0]:>6}   {cm[0][1]:>6}")
    print(f"  Actual Approve   {cm[1][0]:>6}   {cm[1][1]:>6}")

    # Breakdown by tx_class
    print(f"\n  Breakdown by TX Class:")
    print(f"  {'TX Class':<12} {'Count':>6} {'Actual':>8} {'Predicted':>10} {'AUC':>7}")
    print(f"  {'-'*47}")

    for tx_class in sorted(new_df['tx_class'].unique()):
        mask = new_df['tx_class'].values == tx_class
        if mask.sum() < 10:
            continue
        cls_y = new_y[mask]
        cls_pred = prob_approved[mask]
        cls_rate = cls_y.mean()
        pred_rate = (cls_pred >= 0.5).mean()
        try:
            cls_auc = roc_auc_score(cls_y, cls_pred)
        except ValueError:
            cls_auc = 0
        print(f"  {tx_class:<12} {mask.sum():>6} {cls_rate:>8.1%} {pred_rate:>10.1%} {cls_auc:>7.4f}")

    # Confidence distribution
    print(f"\n  Model Confidence Distribution:")
    bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    for i in range(len(bins)-1):
        mask = (prob_approved >= bins[i]) & (prob_approved < bins[i+1])
        if mask.sum() == 0:
            continue
        actual_rate = new_y[mask].mean()
        print(f"    P({bins[i]:.1f}-{bins[i+1]:.1f}): {mask.sum():>5} transactions, actual approval: {actual_rate:.1%}")

    conn.close()
    print("\nDone!")


if __name__ == '__main__':
    main()
