"""
BinRoute AI — Feature Ablation Study

Remove each feature one at a time and measure AUC change.
If removing a feature IMPROVES AUC, that feature is hurting the model.

Usage: py -3 scripts/ml/feature_ablation.py
"""

import os, sqlite3, time
import numpy as np, pandas as pd
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
NUMERICAL = [
    'is_prepaid', 'amount', 'attempt_number', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'consecutive_approvals', 'days_since_last_charge', 'days_since_initial',
    'lifetime_charges', 'lifetime_revenue', 'initial_amount',
    'amount_ratio', 'prior_declines_in_cycle',
]

ALL_FEATURES = CATEGORICAL + NUMERICAL


def main():
    print("=" * 70)
    print("Feature Ablation Study — Does removing features help?")
    print("=" * 70)

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
    print(f"\n  Rows: {len(df):,}")

    # Encode categoricals
    for col in CATEGORICAL:
        le = LabelEncoder()
        df[f'{col}_enc'] = le.fit_transform(df[col].fillna('UNKNOWN').astype(str))
    for col in NUMERICAL:
        df[col] = df[col].fillna(0)

    # Time-based split
    split_idx = int(len(df) * 0.80)
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]
    y_train = train['label'].values
    y_test = test['label'].values
    scale_pos = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    # Build feature column list
    all_cols = [f'{c}_enc' for c in CATEGORICAL] + NUMERICAL
    all_names = CATEGORICAL + NUMERICAL

    # --- Baseline: all 27 features ---
    print("\n  Training baseline (all 27 features)...", end=" ", flush=True)
    baseline_auc, baseline_rebill, baseline_salvage = train_and_eval(
        train, test, y_train, y_test, all_cols, scale_pos, df
    )
    print(f"Overall: {baseline_auc:.4f}  Rebill: {baseline_rebill:.4f}  Salvage: {baseline_salvage:.4f}")

    # --- Remove one feature at a time ---
    print(f"\n  {'Feature Removed':<30} {'Overall':>8} {'Diff':>8} {'Rebill':>8} {'Diff':>8} {'Salvage':>8} {'Diff':>8} {'Verdict':>12}")
    print(f"  {'-'*96}")

    results = []
    for i, feat_name in enumerate(all_names):
        col_name = f'{feat_name}_enc' if feat_name in CATEGORICAL else feat_name
        reduced_cols = [c for c in all_cols if c != col_name]

        auc, rebill_auc, salvage_auc = train_and_eval(
            train, test, y_train, y_test, reduced_cols, scale_pos, df
        )

        diff = auc - baseline_auc
        rdiff = rebill_auc - baseline_rebill
        sdiff = salvage_auc - baseline_salvage

        if diff > 0.001:
            verdict = "DROP IT"
        elif diff > 0.0003:
            verdict = "slight +"
        elif diff < -0.003:
            verdict = "KEEP"
        elif diff < -0.001:
            verdict = "useful"
        else:
            verdict = "neutral"

        results.append((feat_name, auc, diff, rebill_auc, rdiff, salvage_auc, sdiff, verdict))
        print(f"  {feat_name:<30} {auc:>8.4f} {diff:>+8.4f} {rebill_auc:>8.4f} {rdiff:>+8.4f} {salvage_auc:>8.4f} {sdiff:>+8.4f} {verdict:>12}")

    # --- Summary ---
    print(f"\n  {'='*96}")
    print(f"  BASELINE:  Overall={baseline_auc:.4f}  Rebill={baseline_rebill:.4f}  Salvage={baseline_salvage:.4f}")

    drop_candidates = [(r[0], r[2], r[4], r[6]) for r in results if r[7] in ('DROP IT', 'slight +')]
    if drop_candidates:
        print(f"\n  Features that HURT the model (removing them improves AUC):")
        for name, d, rd, sd in sorted(drop_candidates, key=lambda x: x[1], reverse=True):
            print(f"    {name:<30} overall: {d:>+.4f}  rebill: {rd:>+.4f}  salvage: {sd:>+.4f}")
    else:
        print(f"\n  No individual feature hurts overall AUC. All features are contributing.")

    # --- Try removing multiple candidates at once ---
    if drop_candidates:
        print(f"\n  Testing removal of ALL hurt features together...")
        drop_cols = set()
        for name, _, _, _ in drop_candidates:
            col = f'{name}_enc' if name in CATEGORICAL else name
            drop_cols.add(col)
        multi_cols = [c for c in all_cols if c not in drop_cols]
        multi_auc, multi_rebill, multi_salvage = train_and_eval(
            train, test, y_train, y_test, multi_cols, scale_pos, df
        )
        print(f"    Without {len(drop_candidates)} features: Overall={multi_auc:.4f} ({multi_auc-baseline_auc:+.4f})  "
              f"Rebill={multi_rebill:.4f} ({multi_rebill-baseline_rebill:+.4f})  "
              f"Salvage={multi_salvage:.4f} ({multi_salvage-baseline_salvage:+.4f})")

    # --- Also test: what if we ONLY keep top 10 features? ---
    print(f"\n  Testing: Top 15 features only (drop bottom 12)...")
    # Sort by how much AUC drops when removed (most important first)
    sorted_results = sorted(results, key=lambda x: x[2])  # most negative diff = most important
    top15_names = [r[0] for r in sorted_results[:15]]
    top15_cols = [f'{n}_enc' if n in CATEGORICAL else n for n in top15_names]
    top15_auc, top15_rebill, top15_salvage = train_and_eval(
        train, test, y_train, y_test, top15_cols, scale_pos, df
    )
    print(f"    Top 15: Overall={top15_auc:.4f} ({top15_auc-baseline_auc:+.4f})  "
          f"Rebill={top15_rebill:.4f} ({top15_rebill-baseline_rebill:+.4f})  "
          f"Salvage={top15_salvage:.4f} ({top15_salvage-baseline_salvage:+.4f})")
    print(f"    Features: {', '.join(top15_names)}")

    print("\nDone!")


def train_and_eval(train, test, y_train, y_test, feature_cols, scale_pos, full_df):
    """Train LightGBM and return (overall_auc, rebill_auc, salvage_auc)."""
    model = lgb.LGBMClassifier(
        n_estimators=300, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=scale_pos, verbose=-1, random_state=42
    )
    X_train = train[feature_cols].values.astype(np.float32)
    X_test = test[feature_cols].values.astype(np.float32)
    model.fit(X_train, y_train)
    probs = model.predict_proba(X_test)[:, 1]

    overall = roc_auc_score(y_test, probs)

    # Rebill AUC
    rebill_mask = test['tx_class'].values == 'rebill'
    rebill_y = y_test[rebill_mask]
    rebill_auc = roc_auc_score(rebill_y, probs[rebill_mask]) if len(set(rebill_y)) >= 2 and rebill_mask.sum() > 50 else 0

    # Salvage AUC
    salvage_mask = test['tx_class'].values == 'salvage'
    salvage_y = y_test[salvage_mask]
    salvage_auc = roc_auc_score(salvage_y, probs[salvage_mask]) if len(set(salvage_y)) >= 2 and salvage_mask.sum() > 50 else 0

    return overall, rebill_auc, salvage_auc


if __name__ == '__main__':
    main()
