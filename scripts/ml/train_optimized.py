"""
BinRoute AI — Optimized Model Training

Trains LightGBM with is_prepaid and prior_declines_in_cycle removed,
compares against the full 27-feature model on every tx_class + cycle.

Usage: py -3 scripts/ml/train_optimized.py
"""

import os, sqlite3, numpy as np, pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')

CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code', 'issuer_bank',
    'card_brand', 'card_type', 'tx_class', 'cycle_depth',
    'prev_decline_reason', 'initial_processor',
]
NUMERICAL_FULL = [
    'is_prepaid', 'amount', 'attempt_number', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'consecutive_approvals', 'days_since_last_charge', 'days_since_initial',
    'lifetime_charges', 'lifetime_revenue', 'initial_amount',
    'amount_ratio', 'prior_declines_in_cycle',
]
NUMERICAL_OPTIMIZED = [
    'amount', 'attempt_number', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'consecutive_approvals', 'days_since_last_charge', 'days_since_initial',
    'lifetime_charges', 'lifetime_revenue', 'initial_amount',
    'amount_ratio',
]


def main():
    print("=" * 75)
    print("Optimized Model — Drop is_prepaid + prior_declines_in_cycle")
    print("=" * 75)

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
        FROM tx_features WHERE feature_version >= 2
        ORDER BY acquisition_date ASC, id ASC
    """, conn)
    conn.close()

    df['label'] = (df['outcome'] == 'approved').astype(int)

    for col in CATEGORICAL:
        le = LabelEncoder()
        df[f'{col}_enc'] = le.fit_transform(df[col].fillna('UNKNOWN').astype(str))
    for col in NUMERICAL_FULL:
        df[col] = df[col].fillna(0)

    split_idx = int(len(df) * 0.80)
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]
    y_train = train['label'].values
    y_test = test['label'].values
    scale_pos = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    cat_cols = [f'{c}_enc' for c in CATEGORICAL]
    full_cols = cat_cols + NUMERICAL_FULL
    opt_cols = cat_cols + NUMERICAL_OPTIMIZED

    # Train both
    print(f"\n  Training FULL model (27 features)...", end=" ", flush=True)
    m_full = lgb.LGBMClassifier(n_estimators=300, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8, scale_pos_weight=scale_pos, verbose=-1, random_state=42)
    m_full.fit(train[full_cols].values.astype(np.float32), y_train)
    p_full = m_full.predict_proba(test[full_cols].values.astype(np.float32))[:, 1]
    print("done")

    print(f"  Training OPTIMIZED model (25 features)...", end=" ", flush=True)
    m_opt = lgb.LGBMClassifier(n_estimators=300, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8, scale_pos_weight=scale_pos, verbose=-1, random_state=42)
    m_opt.fit(train[opt_cols].values.astype(np.float32), y_train)
    p_opt = m_opt.predict_proba(test[opt_cols].values.astype(np.float32))[:, 1]
    print("done")

    # Compare
    print(f"\n  {'Category':<20} {'Count':>6} {'Full 27':>10} {'Opt 25':>10} {'Diff':>8} {'Winner':>10}")
    print(f"  {'-'*68}")

    categories = [
        ('OVERALL', None),
        ('initial', 'initial'),
        ('rebill', 'rebill'),
        ('  rebill C1', ('rebill', 'C1')),
        ('  rebill C2', ('rebill', 'C2')),
        ('  rebill C3+', ('rebill', 'C3+')),
        ('salvage', 'salvage'),
        ('  salvage att 2', ('salvage', 2)),
        ('  salvage att 3', ('salvage', 3)),
        ('  salvage att 4', ('salvage', 4)),
        ('cascade', 'cascade'),
        ('upsell', 'upsell'),
    ]

    for label, filt in categories:
        if filt is None:
            mask = np.ones(len(y_test), dtype=bool)
        elif isinstance(filt, str):
            mask = test['tx_class'].values == filt
        elif isinstance(filt, tuple) and isinstance(filt[1], str):
            mask = (test['tx_class'].values == filt[0]) & (test['cycle_depth'].values == filt[1])
        elif isinstance(filt, tuple) and isinstance(filt[1], int):
            mask = (test['tx_class'].values == filt[0]) & (test['attempt_number'].values == filt[1])
        else:
            continue

        if mask.sum() < 30:
            continue
        cls_y = y_test[mask]
        if len(set(cls_y)) < 2:
            continue

        auc_f = roc_auc_score(cls_y, p_full[mask])
        auc_o = roc_auc_score(cls_y, p_opt[mask])
        diff = auc_o - auc_f
        winner = 'OPT +' if diff > 0.001 else ('FULL +' if diff < -0.001 else 'TIE')

        print(f"  {label:<20} {mask.sum():>6} {auc_f:>10.4f} {auc_o:>10.4f} {diff:>+8.4f} {winner:>10}")

    # Export optimized model to ONNX
    print(f"\n  Exporting optimized model to ONNX...")
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models')
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        from onnxmltools import convert_lightgbm
        from onnxmltools.convert.common.data_types import FloatTensorType
        import onnx

        initial_type = [('features', FloatTensorType([None, len(opt_cols)]))]
        onnx_model = convert_lightgbm(m_opt, initial_types=initial_type)
        onnx_path = os.path.join(OUTPUT_DIR, 'binroute_lightgbm_v2.onnx')
        onnx.save_model(onnx_model, onnx_path)
        print(f"  Saved: {onnx_path} ({os.path.getsize(onnx_path)/1024:.0f} KB)")

        import onnxruntime as ort
        sess = ort.InferenceSession(onnx_path)
        test_input = test[opt_cols].values[:5].astype(np.float32)
        sess.run(None, {sess.get_inputs()[0].name: test_input})
        print(f"  Verified: loads and produces output")
    except Exception as e:
        print(f"  ONNX export failed: {e}")

    print("\nDone!")


if __name__ == '__main__':
    main()
