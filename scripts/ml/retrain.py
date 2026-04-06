"""
BinRoute AI — Weekly Retrain Pipeline

Full automated cycle:
  1. Load latest tx_features (includes all new orders from syncs)
  2. Train general model + rebill specialist
  3. Compare against current production models
  4. If improved: promote new models, archive old ones
  5. Log performance history for tracking over time

Usage: py -3 scripts/ml/retrain.py

Exit codes:
  0 = success, models promoted (or no improvement, kept current)
  1 = error
"""

import os, sys, json, shutil, time
from datetime import datetime
import numpy as np, pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
MODELS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models')
HISTORY_PATH = os.path.join(MODELS_DIR, 'retrain_history.json')

# Feature definitions
GEN_CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code', 'issuer_bank',
    'card_brand', 'card_type', 'tx_class', 'cycle_depth',
    'prev_decline_reason', 'initial_processor',
]
GEN_NUMERICAL = [
    'is_prepaid', 'amount', 'attempt_number', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'consecutive_approvals', 'days_since_last_charge', 'days_since_initial',
    'lifetime_charges', 'lifetime_revenue', 'initial_amount',
    'amount_ratio', 'prior_declines_in_cycle',
]

REB_CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code', 'issuer_bank',
    'card_brand', 'card_type', 'cycle_depth',
    'prev_decline_reason', 'initial_processor',
]
REB_NUMERICAL = [
    'amount', 'attempt_number', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'consecutive_approvals', 'days_since_last_charge', 'days_since_initial',
    'lifetime_charges', 'lifetime_revenue', 'initial_amount',
    'amount_ratio', 'prior_declines_in_cycle',
]

COLUMNS_SQL = """
    processor_name, acquiring_bank, mcc_code, issuer_bank,
    card_brand, card_type, is_prepaid, amount, tx_class,
    attempt_number, cycle_depth, hour_of_day, day_of_week,
    prev_decline_reason, initial_processor,
    mid_velocity_daily, mid_velocity_weekly,
    customer_history_on_proc, bin_velocity_weekly,
    consecutive_approvals, days_since_last_charge, days_since_initial,
    lifetime_charges, lifetime_revenue, initial_amount,
    amount_ratio, prior_declines_in_cycle,
    outcome, acquisition_date
"""


def main():
    start_time = time.time()
    now = datetime.now()
    print(f"[Retrain] {now.strftime('%Y-%m-%d %H:%M')} — Starting weekly retrain...")

    import sqlite3
    conn = sqlite3.connect(DB_PATH)

    # 1. Load data
    df = pd.read_sql_query(f"""
        SELECT {COLUMNS_SQL}
        FROM tx_features WHERE feature_version >= 2
        ORDER BY acquisition_date ASC, id ASC
    """, conn)
    conn.close()

    df['label'] = (df['outcome'] == 'approved').astype(int)
    total_rows = len(df)
    print(f"[Retrain] Loaded {total_rows:,} rows ({df['label'].mean():.1%} approved)")

    if total_rows < 10000:
        print("[Retrain] Not enough data to train. Skipping.")
        sys.exit(0)

    # Encode
    all_encoders = {}
    for col in set(GEN_CATEGORICAL + REB_CATEGORICAL):
        le = LabelEncoder()
        df[f'{col}_enc'] = le.fit_transform(df[col].fillna('UNKNOWN').astype(str))
        all_encoders[col] = le
    for col in set(GEN_NUMERICAL + REB_NUMERICAL):
        df[col] = df[col].fillna(0)

    # Time-based split
    split_idx = int(len(df) * 0.80)
    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]

    # --- GENERAL MODEL ---
    print("[Retrain] Training general model...")
    gen_cols = [f'{c}_enc' for c in GEN_CATEGORICAL] + GEN_NUMERICAL
    y_train = train_df['label'].values
    y_test = test_df['label'].values
    scale_pos = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    gen_model = lgb.LGBMClassifier(
        n_estimators=300, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=scale_pos, verbose=-1, random_state=42
    )
    gen_model.fit(train_df[gen_cols].values.astype(np.float32), y_train)
    gen_probs = gen_model.predict_proba(test_df[gen_cols].values.astype(np.float32))[:, 1]
    gen_auc = roc_auc_score(y_test, gen_probs)
    print(f"[Retrain] General AUC: {gen_auc:.4f}")

    # Per-class AUC
    gen_class_auc = {}
    for tc in ['initial', 'rebill', 'salvage', 'cascade', 'upsell']:
        mask = test_df['tx_class'].values == tc
        if mask.sum() > 50 and len(set(y_test[mask])) >= 2:
            gen_class_auc[tc] = round(roc_auc_score(y_test[mask], gen_probs[mask]), 4)

    # --- REBILL SPECIALIST ---
    print("[Retrain] Training rebill specialist...")
    reb_mask_train = (train_df['tx_class'].isin(['rebill', 'salvage'])) & (train_df['cycle_depth'].isin(['C1', 'C2', 'C3+']))
    reb_mask_test = (test_df['tx_class'].isin(['rebill', 'salvage'])) & (test_df['cycle_depth'].isin(['C1', 'C2', 'C3+']))

    reb_train = train_df[reb_mask_train]
    reb_test = test_df[reb_mask_test]
    reb_cols = [f'{c}_enc' for c in REB_CATEGORICAL] + REB_NUMERICAL

    y_reb_train = reb_train['label'].values
    y_reb_test = reb_test['label'].values
    reb_scale = (y_reb_train == 0).sum() / max((y_reb_train == 1).sum(), 1)

    reb_model = lgb.LGBMClassifier(
        n_estimators=500, max_depth=10, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, min_child_samples=30,
        scale_pos_weight=reb_scale, verbose=-1, random_state=42
    )
    reb_model.fit(reb_train[reb_cols].values.astype(np.float32), y_reb_train)
    reb_probs = reb_model.predict_proba(reb_test[reb_cols].values.astype(np.float32))[:, 1]
    reb_auc = roc_auc_score(y_reb_test, reb_probs)
    print(f"[Retrain] Rebill specialist AUC: {reb_auc:.4f}")

    # --- COMPARE WITH CURRENT ---
    history = load_history()
    prev = history[-1] if history else None
    prev_gen_auc = prev['general_auc'] if prev else 0
    prev_reb_auc = prev['rebill_auc'] if prev else 0

    gen_improved = gen_auc > prev_gen_auc - 0.002  # allow tiny regression
    reb_improved = reb_auc > prev_reb_auc - 0.002

    print(f"\n[Retrain] Comparison with current production:")
    print(f"  General:  {prev_gen_auc:.4f} -> {gen_auc:.4f} ({'improved' if gen_auc > prev_gen_auc else 'same/slight dip'})")
    print(f"  Rebill:   {prev_reb_auc:.4f} -> {reb_auc:.4f} ({'improved' if reb_auc > prev_reb_auc else 'same/slight dip'})")

    # --- PROMOTE OR SKIP ---
    promote = gen_improved and reb_improved
    if promote:
        print(f"\n[Retrain] Promoting new models...")
        os.makedirs(MODELS_DIR, exist_ok=True)

        # Archive current models
        archive_dir = os.path.join(MODELS_DIR, 'archive')
        os.makedirs(archive_dir, exist_ok=True)
        timestamp = now.strftime('%Y%m%d_%H%M')

        for fname in ['binroute_lightgbm.onnx', 'binroute_rebill_specialist.onnx']:
            src = os.path.join(MODELS_DIR, fname)
            if os.path.exists(src):
                dst = os.path.join(archive_dir, f"{timestamp}_{fname}")
                shutil.copy2(src, dst)

        # Export new models to ONNX
        export_onnx(gen_model, gen_cols, os.path.join(MODELS_DIR, 'binroute_lightgbm.onnx'), 'general')
        export_onnx(reb_model, reb_cols, os.path.join(MODELS_DIR, 'binroute_rebill_specialist.onnx'), 'rebill')
        print(f"[Retrain] Models promoted. Old models archived to {archive_dir}")
    else:
        print(f"\n[Retrain] Models not improved enough to promote. Keeping current.")

    # --- LOG HISTORY ---
    entry = {
        'date': now.isoformat(),
        'total_rows': total_rows,
        'train_rows': len(train_df),
        'test_rows': len(test_df),
        'general_auc': round(gen_auc, 4),
        'general_class_auc': gen_class_auc,
        'rebill_auc': round(reb_auc, 4),
        'rebill_train_rows': len(reb_train),
        'rebill_test_rows': len(reb_test),
        'prev_general_auc': round(prev_gen_auc, 4),
        'prev_rebill_auc': round(prev_reb_auc, 4),
        'promoted': promote,
    }
    history.append(entry)
    save_history(history)

    elapsed = time.time() - start_time
    print(f"\n[Retrain] Done in {elapsed:.1f}s. History: {len(history)} entries.")


def export_onnx(model, feature_cols, path, label):
    try:
        from onnxmltools import convert_lightgbm
        from onnxmltools.convert.common.data_types import FloatTensorType
        import onnx

        initial_type = [('features', FloatTensorType([None, len(feature_cols)]))]
        onnx_model = convert_lightgbm(model, initial_types=initial_type)
        onnx.save_model(onnx_model, path)
        print(f"  [{label}] Saved: {path} ({os.path.getsize(path)/1024:.0f} KB)")
    except Exception as e:
        print(f"  [{label}] ONNX export failed: {e}")


def load_history():
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH) as f:
            return json.load(f)
    return []


def save_history(history):
    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
    with open(HISTORY_PATH, 'w') as f:
        json.dump(history, f, indent=2)


if __name__ == '__main__':
    main()
