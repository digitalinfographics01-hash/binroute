"""
BinRoute AI — Dedicated Rebill Model

Trains a specialized LightGBM model on rebill + rebill-salvage data only.
Compares against the general model on rebill performance.

At scoring time:
  - rebill / rebill-salvage -> rebill model
  - everything else -> general model

Usage: py -3 scripts/ml/train_rebill_model.py
"""

import os, json, sqlite3, time
import numpy as np, pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score, accuracy_score
import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models')

CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code', 'issuer_bank',
    'card_brand', 'card_type', 'cycle_depth',
    'prev_decline_reason', 'initial_processor',
]

NUMERICAL = [
    'amount', 'attempt_number', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'consecutive_approvals', 'days_since_last_charge', 'days_since_initial',
    'lifetime_charges', 'lifetime_revenue', 'initial_amount',
    'amount_ratio', 'prior_declines_in_cycle',
]


def main():
    print("=" * 70)
    print("BinRoute AI — Dedicated Rebill Model")
    print("=" * 70)

    conn = sqlite3.connect(DB_PATH)
    all_df = pd.read_sql_query("""
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
        FROM tx_features WHERE feature_version >= 2
        ORDER BY acquisition_date ASC, id ASC
    """, conn)
    conn.close()

    all_df['label'] = (all_df['outcome'] == 'approved').astype(int)

    # Filter to rebill + salvage on rebill cycles (cycle >= 1)
    rebill_mask = (
        (all_df['tx_class'].isin(['rebill', 'salvage'])) &
        (all_df['cycle_depth'].isin(['C1', 'C2', 'C3+']))
    )
    rebill_df = all_df[rebill_mask].copy().reset_index(drop=True)

    print(f"\n  All data:    {len(all_df):,} rows")
    print(f"  Rebill data: {len(rebill_df):,} rows ({len(rebill_df)/len(all_df)*100:.1f}%)")
    print(f"  Rebill approved: {rebill_df['label'].sum():,} ({rebill_df['label'].mean():.1%})")
    print(f"  Rebill declined: {(rebill_df['label']==0).sum():,} ({(rebill_df['label']==0).mean():.1%})")

    # Breakdown
    print(f"\n  Rebill breakdown:")
    for tc in ['rebill', 'salvage']:
        for cd in ['C1', 'C2', 'C3+']:
            mask = (rebill_df['tx_class'] == tc) & (rebill_df['cycle_depth'] == cd)
            if mask.sum() > 0:
                rate = rebill_df.loc[mask, 'label'].mean()
                print(f"    {tc:<10} {cd:<4} {mask.sum():>7,} rows  {rate:.1%} approval")

    # Encode categoricals (fit on ALL data for consistency, even though we train on rebill only)
    for col in CATEGORICAL:
        le = LabelEncoder()
        le.fit(all_df[col].fillna('UNKNOWN').astype(str))
        rebill_df[f'{col}_enc'] = le.transform(rebill_df[col].fillna('UNKNOWN').astype(str))
        all_df[f'{col}_enc'] = le.transform(all_df[col].fillna('UNKNOWN').astype(str))

    for col in NUMERICAL:
        rebill_df[col] = rebill_df[col].fillna(0)
        all_df[col] = all_df[col].fillna(0)

    feature_cols = [f'{c}_enc' for c in CATEGORICAL] + NUMERICAL
    feature_names = CATEGORICAL + NUMERICAL

    # Time-based split on rebill data
    split_idx = int(len(rebill_df) * 0.80)
    r_train = rebill_df.iloc[:split_idx]
    r_test = rebill_df.iloc[split_idx:]
    y_r_train = r_train['label'].values
    y_r_test = r_test['label'].values

    print(f"\n  Rebill train: {len(r_train):,}  test: {len(r_test):,}")
    print(f"  Train approval: {y_r_train.mean():.1%}  Test approval: {y_r_test.mean():.1%}")

    scale_pos = (y_r_train == 0).sum() / max((y_r_train == 1).sum(), 1)

    # --- Train GENERAL model (on all data) for comparison ---
    print(f"\n[1] Training GENERAL model (all 275K rows)...", end=" ", flush=True)
    all_split = int(len(all_df) * 0.80)
    all_train = all_df.iloc[:all_split]
    y_all_train = all_train['label'].values
    all_scale = (y_all_train == 0).sum() / max((y_all_train == 1).sum(), 1)

    # Need tx_class for general model — encode on all_df before splitting
    general_cats = CATEGORICAL + ['tx_class']
    le_txclass = LabelEncoder()
    all_df['tx_class_enc'] = le_txclass.fit_transform(all_df['tx_class'].fillna('UNKNOWN').astype(str))
    general_cols = [f'{c}_enc' for c in general_cats] + NUMERICAL
    # Re-split after adding column
    all_train = all_df.iloc[:all_split]
    y_all_train = all_train['label'].values

    m_general = lgb.LGBMClassifier(n_estimators=300, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8, scale_pos_weight=all_scale, verbose=-1, random_state=42)
    m_general.fit(all_train[general_cols].values.astype(np.float32), y_all_train)

    # Score rebill test set with general model
    # Need to extract rebill test rows from the all_df perspective
    # Use the same time-based split on all_df, then filter to rebill
    all_test = all_df.iloc[all_split:]
    all_test_rebill_mask = (
        (all_test['tx_class'].isin(['rebill', 'salvage'])) &
        (all_test['cycle_depth'].isin(['C1', 'C2', 'C3+']))
    )
    all_test_rebill = all_test[all_test_rebill_mask]
    y_general_rebill = all_test_rebill['label'].values
    p_general = m_general.predict_proba(all_test_rebill[general_cols].values.astype(np.float32))[:, 1]
    start = time.time()
    print(f"done ({time.time()-start:.1f}s)")

    # --- Train REBILL SPECIALIST model ---
    print(f"[2] Training REBILL SPECIALIST model ({len(r_train):,} rows)...", end=" ", flush=True)
    start = time.time()
    m_rebill = lgb.LGBMClassifier(n_estimators=500, max_depth=10, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, min_child_samples=30,
        scale_pos_weight=scale_pos, verbose=-1, random_state=42)
    m_rebill.fit(r_train[feature_cols].values.astype(np.float32), y_r_train)
    elapsed = time.time() - start
    print(f"done ({elapsed:.1f}s)")

    # Score rebill test set with specialist
    p_rebill = m_rebill.predict_proba(r_test[feature_cols].values.astype(np.float32))[:, 1]

    # --- Compare ---
    print(f"\n{'='*70}")
    print(f"HEAD TO HEAD: General vs Rebill Specialist")
    print(f"{'='*70}")

    # The test sets may differ slightly, so evaluate both on the rebill-specific test set
    # For fair comparison, use the rebill test set (r_test) and score with both models
    # General model needs tx_class column
    # Add tx_class_enc to r_test for general model scoring
    r_test = r_test.copy()
    r_test['tx_class_enc'] = le_txclass.transform(r_test['tx_class'].fillna('UNKNOWN').astype(str))
    r_test_general_cols = general_cols

    p_gen_on_rtest = m_general.predict_proba(r_test[r_test_general_cols].values.astype(np.float32))[:, 1]
    p_spec_on_rtest = p_rebill

    print(f"\n  {'Category':<20} {'Count':>6} {'General':>10} {'Specialist':>10} {'Diff':>8} {'Winner':>10}")
    print(f"  {'-'*68}")

    categories = [
        ('ALL REBILL', None),
        ('rebill C1', ('rebill', 'C1')),
        ('rebill C2', ('rebill', 'C2')),
        ('rebill C3+', ('rebill', 'C3+')),
        ('salvage C1', ('salvage', 'C1')),
        ('salvage C2', ('salvage', 'C2')),
        ('salvage C3+', ('salvage', 'C3+')),
    ]

    for label, filt in categories:
        if filt is None:
            mask = np.ones(len(y_r_test), dtype=bool)
        else:
            mask = (r_test['tx_class'].values == filt[0]) & (r_test['cycle_depth'].values == filt[1])

        if mask.sum() < 30:
            continue
        cls_y = y_r_test[mask]
        if len(set(cls_y)) < 2:
            continue

        auc_g = roc_auc_score(cls_y, p_gen_on_rtest[mask])
        auc_s = roc_auc_score(cls_y, p_spec_on_rtest[mask])
        diff = auc_s - auc_g
        winner = 'SPEC +' if diff > 0.002 else ('GEN +' if diff < -0.002 else 'TIE')
        print(f"  {label:<20} {mask.sum():>6} {auc_g:>10.4f} {auc_s:>10.4f} {diff:>+8.4f} {winner:>10}")

    # Overall metrics for specialist
    y_pred = (p_spec_on_rtest >= 0.5).astype(int)
    print(f"\n  Rebill Specialist Metrics:")
    print(f"    AUC-ROC:   {roc_auc_score(y_r_test, p_spec_on_rtest):.4f}")
    print(f"    Accuracy:  {accuracy_score(y_r_test, y_pred):.4f}")
    print(f"    Precision: {precision_score(y_r_test, y_pred, zero_division=0):.4f}")
    print(f"    Recall:    {recall_score(y_r_test, y_pred, zero_division=0):.4f}")
    print(f"    F1:        {f1_score(y_r_test, y_pred, zero_division=0):.4f}")

    # Feature importance
    print(f"\n  Rebill Specialist — Feature Importance:")
    print(f"  {'Rank':<6} {'Feature':<30} {'Importance':>10}")
    print(f"  {'-'*48}")
    importances = m_rebill.feature_importances_
    indices = np.argsort(importances)[::-1]
    for rank, idx in enumerate(indices, 1):
        bar = "#" * min(int(importances[idx] / max(importances) * 30), 30)
        print(f"  {rank:<6} {feature_names[idx]:<30} {importances[idx]:>10}  {bar}")

    # Export rebill model to ONNX
    print(f"\n  Exporting rebill specialist to ONNX...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    try:
        from onnxmltools import convert_lightgbm
        from onnxmltools.convert.common.data_types import FloatTensorType
        import onnx

        initial_type = [('features', FloatTensorType([None, len(feature_cols)]))]
        onnx_model = convert_lightgbm(m_rebill, initial_types=initial_type)
        onnx_path = os.path.join(OUTPUT_DIR, 'binroute_rebill_specialist.onnx')
        onnx.save_model(onnx_model, onnx_path)
        print(f"  Saved: {onnx_path} ({os.path.getsize(onnx_path)/1024:.0f} KB)")

        import onnxruntime as ort
        sess = ort.InferenceSession(onnx_path)
        test_in = r_test[feature_cols].values[:5].astype(np.float32)
        sess.run(None, {sess.get_inputs()[0].name: test_in})
        print(f"  Verified: loads and produces output")
    except Exception as e:
        print(f"  ONNX export failed: {e}")

    # Save metadata
    meta = {
        'model_type': 'rebill_specialist',
        'trained_at': pd.Timestamp.now().isoformat(),
        'train_rows': len(r_train),
        'test_rows': len(r_test),
        'features': feature_names,
        'feature_count': len(feature_names),
        'applies_to': 'tx_class in (rebill, salvage) AND cycle_depth in (C1, C2, C3+)',
        'auc_roc': round(roc_auc_score(y_r_test, p_spec_on_rtest), 4),
        'note': 'No tx_class feature — model only sees rebill/salvage data, no is_prepaid',
    }
    meta_path = os.path.join(OUTPUT_DIR, 'rebill_specialist_meta.json')
    with open(meta_path, 'w') as f:
        json.dump(meta, f, indent=2)
    print(f"  Metadata: {meta_path}")

    print("\nDone!")


if __name__ == '__main__':
    main()
