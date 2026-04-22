"""
BinRoute AI — Phase 2: Target-Encoding Audit (te_acquiring_bank)

Compares the current expanding-window TE approach against a proper
TimeSeriesSplit OOF variant. If the approaches diverge by >1pp AUC,
flags for switching; otherwise keeps the current implementation.

Decision thresholds (LOCKED):
  < 0.5pp delta → keep current, "no action needed"
  0.5–1pp       → keep current, document, revisit later
  > 1pp         → flag for switching to OOF variant

Usage: py -3 scripts/ml/te_audit.py [--db=PATH]
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
from collections import defaultdict, deque

from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score

import lightgbm as lgb

warnings.filterwarnings('ignore')

# ── Paths ──
DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models')
TRAIN_RATIO = 0.80

# TE smoothing hyperparameter (for OOF variant)
TE_SMOOTHING_M = 20

for arg in sys.argv:
    if arg.startswith('--db='):
        DB_PATH = arg.split('=', 1)[1]

# ── Initial model config (mirrors train_four_models.py) ──
ISSUER_MIN_COUNT = 500

INITIAL_CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank_grouped', 'card_brand', 'card_type',
    'billing_state', 'client_id',
]

INITIAL_NUMERICAL = [
    'is_prepaid', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily',
    'bin_velocity_weekly', 'mid_age_days',
    'bin_approval_rate', 'bin_proc_approval_rate',
    'te_acquiring_bank', 'bin_approval_7d', 'bin_approval_30d',
    'amount_vs_bin_avg', 'is_near_payday',
]


# ═══════════════════════════════════════════════════════════════════════════
# Data Loading (same as train_four_models.py)
# ═══════════════════════════════════════════════════════════════════════════

def load_data():
    """Load transaction_attempts, excluding customer_input and system_decline."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            ta.client_id, ta.order_id, ta.attempt_seq, ta.gateway_id,
            ta.processor_name, ta.acquiring_bank, ta.mcc_code,
            ta.outcome, ta.decline_reason,
            ta.cc_first_6, ta.order_total, ta.acquisition_date,
            ta.derived_product_role, ta.product_type_classified,
            ta.derived_cycle, ta.derived_attempt, ta.product_group_id,
            ta.offer_name, ta.billing_state, ta.is_cascaded,
            ta.initial_declined_processor, ta.initial_decline_reason,
            ta.cascade_position, ta.total_attempts, ta.processors_tried_before,
            ta.cascade_final_outcome, ta.cascade_approved_processor,
            ta.model_target, ta.source,
            ta.issuer_bank, ta.card_brand, ta.card_type, ta.is_prepaid,
            ta.hour_of_day, ta.day_of_week, ta.mid_age_days,
            ta.had_nsf, ta.had_do_not_honor, ta.had_pickup,
            ta.initial_processor, ta.last_approved_processor,
            ta.parent_declined_processor, ta.prev_decline_reason,
            ta.mid_velocity_daily, ta.mid_velocity_weekly,
            ta.customer_history_on_proc, ta.bin_velocity_weekly,
            ta.consecutive_approvals, ta.days_since_last_charge,
            ta.days_since_initial, ta.lifetime_charges, ta.lifetime_revenue,
            ta.initial_amount, ta.amount_ratio, ta.prior_declines_in_cycle,
            ta.initial_was_payfac,
            bl.card_level
        FROM transaction_attempts ta
        LEFT JOIN bin_lookup bl ON ta.cc_first_6 = bl.bin
        WHERE ta.feature_version >= 3
          AND ta.model_target NOT IN ('excluded')
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
    print(f"  Loaded {len(df):,} attempts from transaction_attempts")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# BIN-level feature enrichment (same expanding window as train script)
# ═══════════════════════════════════════════════════════════════════════════

def enrich_initial_features(df):
    """
    Compute BIN-level and target-encoded features for the initial model.
    Uses expanding window: each row only sees data from BEFORE it.
    Must be called on time-sorted data (acquisition_date ASC).

    This is the CURRENT approach — produces te_acquiring_bank using
    simple expanding-window mean (threshold >= 20 samples).
    """
    n = len(df)
    global_rate = df['label'].mean()

    # Parse dates once
    dates = pd.to_datetime(df['acquisition_date'], errors='coerce')
    df['day_of_month'] = dates.dt.day.fillna(15).astype(int)
    df['is_near_payday'] = df['day_of_month'].apply(
        lambda d: 1 if d <= 3 or (13 <= d <= 17) or d >= 28 else 0
    )
    df['is_weekend'] = df['day_of_week'].apply(lambda d: 1 if d >= 5 else 0)

    # Group issuer bank
    counts = df['issuer_bank'].fillna('UNKNOWN').value_counts()
    top_banks = set(counts[counts >= ISSUER_MIN_COUNT].index)
    df['issuer_bank_grouped'] = df['issuer_bank'].fillna('UNKNOWN').apply(
        lambda x: x if x in top_banks else 'OTHER'
    )
    print(f"  Issuer grouping: {len(top_banks)} banks kept (rest -> OTHER)")

    # Convert to numpy for fast row access
    bins = df['cc_first_6'].values
    procs = df['processor_name'].values
    acqs = df['acquiring_bank'].values
    amts = df['order_total'].fillna(0).values.astype(float)
    labels = df['label'].values
    timestamps = dates.values.astype(np.int64) // 10**9
    ts_valid = ~np.isnan(dates.values.astype(np.float64))

    # Pre-allocate output
    bin_approval = np.full(n, np.nan)
    bin_proc_approval = np.full(n, np.nan)
    bin_approval_7d = np.full(n, np.nan)
    bin_approval_30d = np.full(n, np.nan)
    amount_vs_bin = np.ones(n)
    te_acq_bank = np.full(n, np.nan)

    # Accumulators
    bin_stats = {}
    bin_proc_stats = {}
    bin_amount_stats = {}
    acq_stats = {}

    bin_window = defaultdict(lambda: {'events': deque(), 'a7': 0, 't7': 0, 'a30': 0, 't30': 0})

    SECS_7D = 7 * 86400
    SECS_30D = 30 * 86400

    print(f"  Computing BIN-level features (expanding window)...")

    for i in range(n):
        bin6 = bins[i]
        proc = procs[i]
        acq = acqs[i]
        amt = amts[i]
        approved = labels[i]
        ts = timestamps[i]
        has_ts = ts_valid[i]

        # ── READ from history ──
        if bin6 in bin_stats:
            a, t = bin_stats[bin6]
            if t >= 5:
                bin_approval[i] = a / t

        bp_key = (bin6, proc)
        if bp_key in bin_proc_stats:
            a, t = bin_proc_stats[bp_key]
            if t >= 3:
                bin_proc_approval[i] = a / t

        if has_ts and bin6 in bin_window:
            w = bin_window[bin6]
            while w['events'] and w['events'][0][0] < ts - SECS_30D:
                old_ts, old_app = w['events'].popleft()
                w['t30'] -= 1
                w['a30'] -= old_app

            a7, t7 = 0, 0
            for evt_ts, evt_app in w['events']:
                if evt_ts >= ts - SECS_7D:
                    t7 += 1
                    a7 += evt_app

            if t7 >= 3:
                bin_approval_7d[i] = a7 / t7
            if w['t30'] >= 5:
                bin_approval_30d[i] = w['a30'] / w['t30']

        if bin6 in bin_amount_stats:
            s, c = bin_amount_stats[bin6]
            if c >= 5 and s > 0:
                avg = s / c
                amount_vs_bin[i] = amt / avg if avg > 0 else 1.0

        # TE acquiring bank (current expanding-window approach)
        if acq in acq_stats:
            a, t = acq_stats[acq]
            if t >= 20:
                te_acq_bank[i] = a / t

        # ── WRITE into accumulators ──
        if bin6 not in bin_stats:
            bin_stats[bin6] = [0, 0]
        bin_stats[bin6][1] += 1
        bin_stats[bin6][0] += approved

        if bp_key not in bin_proc_stats:
            bin_proc_stats[bp_key] = [0, 0]
        bin_proc_stats[bp_key][1] += 1
        bin_proc_stats[bp_key][0] += approved

        if bin6 not in bin_amount_stats:
            bin_amount_stats[bin6] = [0, 0]
        bin_amount_stats[bin6][0] += amt
        bin_amount_stats[bin6][1] += 1

        if acq not in acq_stats:
            acq_stats[acq] = [0, 0]
        acq_stats[acq][1] += 1
        acq_stats[acq][0] += approved

        if has_ts:
            w = bin_window[bin6]
            w['events'].append((ts, approved))
            w['t30'] += 1
            w['a30'] += approved

        if (i + 1) % 100000 == 0:
            print(f"    {i+1:,}/{n:,}")

    df['bin_approval_rate'] = pd.Series(bin_approval, index=df.index).fillna(global_rate)
    df['bin_proc_approval_rate'] = pd.Series(bin_proc_approval, index=df.index).fillna(global_rate)
    df['bin_approval_7d'] = pd.Series(bin_approval_7d, index=df.index).fillna(global_rate)
    df['bin_approval_30d'] = pd.Series(bin_approval_30d, index=df.index).fillna(global_rate)
    df['amount_vs_bin_avg'] = amount_vs_bin
    df['te_acquiring_bank'] = pd.Series(te_acq_bank, index=df.index).fillna(global_rate)

    print(f"  Enrichment done: {len(bin_stats):,} BINs, {len(acq_stats):,} acquiring banks")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# OOF Target Encoding via TimeSeriesSplit
# ═══════════════════════════════════════════════════════════════════════════

def compute_oof_te(df, n_splits=5):
    """
    Out-of-fold target encoding for acquiring_bank using TimeSeriesSplit.

    TimeSeriesSplit produces expanding-window folds where each validation
    fold is strictly AFTER its training rows. Per fold, we compute
    te_acquiring_bank for validation rows using only the training window.

    Smoothing: weight = n_bank / (n_bank + m), m = TE_SMOOTHING_M
        te = weight * bank_mean + (1 - weight) * global_mean

    Unseen banks in a fold get the global mean.

    Returns: numpy array of OOF TE values aligned to df index.
    """
    n = len(df)
    acq_banks = df['acquiring_bank'].values
    labels = df['label'].values

    oof_te = np.full(n, np.nan)
    fold_details = []

    tscv = TimeSeriesSplit(n_splits=n_splits)

    print(f"  Computing OOF TE with TimeSeriesSplit (n_splits={n_splits})...")

    for fold_idx, (train_idx, val_idx) in enumerate(tscv.split(df)):
        fold_start = time.time()

        train_labels = labels[train_idx]
        train_acqs = acq_banks[train_idx]
        val_acqs = acq_banks[val_idx]

        global_mean = train_labels.mean()

        # Build per-bank stats from training fold
        bank_stats = {}  # {bank: [approvals, total]}
        for j in range(len(train_idx)):
            bank = train_acqs[j]
            if bank not in bank_stats:
                bank_stats[bank] = [0, 0]
            bank_stats[bank][0] += train_labels[j]
            bank_stats[bank][1] += 1

        # Encode validation rows using smoothed TE
        n_unseen = 0
        for j in range(len(val_idx)):
            bank = val_acqs[j]
            if bank in bank_stats:
                a, t = bank_stats[bank]
                bank_mean = a / t
                weight = t / (t + TE_SMOOTHING_M)
                oof_te[val_idx[j]] = weight * bank_mean + (1 - weight) * global_mean
            else:
                oof_te[val_idx[j]] = global_mean
                n_unseen += 1

        fold_elapsed = time.time() - fold_start
        fold_info = {
            'fold': fold_idx + 1,
            'train_size': len(train_idx),
            'val_size': len(val_idx),
            'n_banks_in_train': len(bank_stats),
            'n_unseen_in_val': n_unseen,
            'global_mean': round(float(global_mean), 6),
            'elapsed_sec': round(fold_elapsed, 2),
        }
        fold_details.append(fold_info)

        print(f"    Fold {fold_idx+1}/{n_splits}: "
              f"train={len(train_idx):,} val={len(val_idx):,} "
              f"banks={len(bank_stats):,} unseen={n_unseen} "
              f"({fold_elapsed:.1f}s)")

    # Rows not in any validation fold (first chunk that is only used as
    # training) get the global mean fallback
    global_mean_all = labels.mean()
    still_nan = np.isnan(oof_te)
    n_fallback = still_nan.sum()
    if n_fallback > 0:
        oof_te[still_nan] = global_mean_all
        print(f"    {n_fallback:,} rows not in any val fold -> global mean fallback")

    return oof_te, fold_details


# ═══════════════════════════════════════════════════════════════════════════
# Feature Preparation (same as train_four_models.py)
# ═══════════════════════════════════════════════════════════════════════════

def prepare_features(df, categorical, numerical):
    """Encode categoricals, fill NAs, return X, y, feature_names."""
    encoders = {}
    encoded_cols = []

    for col in categorical:
        le = LabelEncoder()
        values = df[col].fillna('UNKNOWN').astype(str)
        le.fit(values)
        df[f'{col}_enc'] = le.transform(values)
        encoders[col] = le
        encoded_cols.append(f'{col}_enc')

    for col in numerical:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    feature_cols = encoded_cols + numerical
    X = df[feature_cols].values.astype(np.float32)
    y = df['label'].values
    feature_names = [col.replace('_enc', '') for col in feature_cols]

    return X, y, feature_names


# ═══════════════════════════════════════════════════════════════════════════
# Train LightGBM + evaluate
# ═══════════════════════════════════════════════════════════════════════════

def train_lgbm(X_train, y_train, X_test, y_test, label=""):
    """Train LightGBM and return AUC on test set."""
    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    model = lgb.LGBMClassifier(
        n_estimators=300, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        verbose=-1, random_state=42,
    )

    start = time.time()
    model.fit(X_train, y_train)
    elapsed = time.time() - start

    y_prob = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_prob)

    print(f"    {label:<30} AUC: {auc:.6f}  ({elapsed:.1f}s)")
    return auc, model, elapsed


# ═══════════════════════════════════════════════════════════════════════════
# Correlation analysis between current TE and OOF TE
# ═══════════════════════════════════════════════════════════════════════════

def analyze_te_correlation(current_te, oof_te):
    """Compute correlation and divergence stats between the two TE variants."""
    valid_mask = np.isfinite(current_te) & np.isfinite(oof_te)
    c = current_te[valid_mask]
    o = oof_te[valid_mask]

    if len(c) < 10:
        return {'error': 'too few valid values'}

    corr = float(np.corrcoef(c, o)[0, 1])
    mae = float(np.mean(np.abs(c - o)))
    rmse = float(np.sqrt(np.mean((c - o) ** 2)))
    max_diff = float(np.max(np.abs(c - o)))
    mean_current = float(np.mean(c))
    mean_oof = float(np.mean(o))
    std_current = float(np.std(c))
    std_oof = float(np.std(o))

    return {
        'pearson_correlation': round(corr, 6),
        'mae': round(mae, 6),
        'rmse': round(rmse, 6),
        'max_abs_diff': round(max_diff, 6),
        'mean_current': round(mean_current, 6),
        'mean_oof': round(mean_oof, 6),
        'std_current': round(std_current, 6),
        'std_oof': round(std_oof, 6),
        'n_valid': int(valid_mask.sum()),
    }


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 75)
    print("  BinRoute AI — Phase 2: Target-Encoding Audit")
    print("  Comparing expanding-window TE vs TimeSeriesSplit OOF TE")
    print("=" * 75)
    print(f"  DB: {os.path.abspath(DB_PATH)}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  TE smoothing (m): {TE_SMOOTHING_M}")
    total_start = time.time()

    # ── Step 1: Load data ──
    print(f"\n[1/6] Loading data...")
    df_all = load_data()

    # ── Step 2: Enrich with BIN features (includes current TE) ──
    print(f"\n[2/6] Enriching initial model features (current expanding-window TE)...")
    df_all = enrich_initial_features(df_all)

    # ── Step 3: Filter to initial model rows ──
    print(f"\n[3/6] Filtering to initial model rows...")
    mask = (df_all['model_target'] == 'initial') & (df_all['derived_product_role'].str.contains('main', na=False))
    df = df_all.loc[mask].copy().reset_index(drop=True)
    print(f"  Initial rows: {len(df):,}")
    print(f"  Approval rate: {df['label'].mean():.1%}")

    # Save the current TE values before they get overwritten
    current_te_values = df['te_acquiring_bank'].values.copy()

    # ── Step 4: Compute OOF TE with TimeSeriesSplit ──
    print(f"\n[4/6] Computing OOF TE with TimeSeriesSplit...")
    oof_te_values, fold_details = compute_oof_te(df, n_splits=5)

    # ── Step 5: Correlation analysis ──
    print(f"\n[5/6] Analyzing TE correlation...")
    corr_stats = analyze_te_correlation(current_te_values, oof_te_values)
    print(f"  Pearson correlation: {corr_stats.get('pearson_correlation', 'N/A')}")
    print(f"  MAE:                 {corr_stats.get('mae', 'N/A')}")
    print(f"  RMSE:                {corr_stats.get('rmse', 'N/A')}")
    print(f"  Max abs diff:        {corr_stats.get('max_abs_diff', 'N/A')}")
    print(f"  Mean (current):      {corr_stats.get('mean_current', 'N/A')}")
    print(f"  Mean (OOF):          {corr_stats.get('mean_oof', 'N/A')}")

    # ── Step 6: Comparative training ──
    print(f"\n[6/6] Comparative LightGBM training...")

    # 80/20 time-based split (same as train_four_models.py)
    split_idx = int(len(df) * TRAIN_RATIO)

    # --- Variant A: Current expanding-window TE ---
    print(f"\n  Variant A: Current expanding-window TE")
    df_a = df.copy()
    # te_acquiring_bank already has the current values from enrichment
    X_a, y_a, feat_names_a = prepare_features(df_a, INITIAL_CATEGORICAL, INITIAL_NUMERICAL)

    X_train_a, X_test_a = X_a[:split_idx], X_a[split_idx:]
    y_train_a, y_test_a = y_a[:split_idx], y_a[split_idx:]
    print(f"  Train: {len(X_train_a):,} | Test: {len(X_test_a):,}")

    auc_current, model_current, time_current = train_lgbm(
        X_train_a, y_train_a, X_test_a, y_test_a,
        label="Current (expanding-window)"
    )

    # --- Variant B: OOF TE with TimeSeriesSplit ---
    print(f"\n  Variant B: OOF TE with TimeSeriesSplit")
    df_b = df.copy()
    df_b['te_acquiring_bank'] = oof_te_values
    X_b, y_b, feat_names_b = prepare_features(df_b, INITIAL_CATEGORICAL, INITIAL_NUMERICAL)

    X_train_b, X_test_b = X_b[:split_idx], X_b[split_idx:]
    y_train_b, y_test_b = y_b[:split_idx], y_b[split_idx:]

    auc_oof, model_oof, time_oof = train_lgbm(
        X_train_b, y_train_b, X_test_b, y_test_b,
        label="OOF (TimeSeriesSplit)"
    )

    # ── Decision ──
    delta_pp = abs(auc_oof - auc_current) * 100  # in percentage points
    delta_signed = (auc_oof - auc_current) * 100
    better_variant = "OOF" if auc_oof > auc_current else "Current"

    if delta_pp < 0.5:
        decision = "no_action_needed"
        decision_text = "AUC delta < 0.5pp — keep current, no action needed"
    elif delta_pp <= 1.0:
        decision = "document_and_revisit"
        decision_text = f"AUC delta {delta_pp:.2f}pp — keep current, document, revisit later"
    else:
        decision = "flag_for_switching"
        decision_text = f"AUC delta {delta_pp:.2f}pp (>{1.0}pp) — FLAG: consider switching to {better_variant} variant"

    print(f"\n{'=' * 75}")
    print(f"  AUDIT RESULTS")
    print(f"{'=' * 75}")
    print(f"  Current TE AUC:   {auc_current:.6f}")
    print(f"  OOF TE AUC:       {auc_oof:.6f}")
    print(f"  Delta:            {delta_signed:+.4f} ({delta_pp:.2f}pp)")
    print(f"  Better variant:   {better_variant}")
    print(f"  Decision:         {decision_text}")
    print(f"{'=' * 75}")

    # ── Feature importance comparison for te_acquiring_bank ──
    te_col_idx = feat_names_a.index('te_acquiring_bank') if 'te_acquiring_bank' in feat_names_a else None
    te_importance_current = None
    te_importance_oof = None
    te_rank_current = None
    te_rank_oof = None

    if te_col_idx is not None and hasattr(model_current, 'feature_importances_'):
        imp_current = model_current.feature_importances_.astype(float)
        total = imp_current.sum()
        if total > 0:
            te_importance_current = float(imp_current[te_col_idx] / total)
            sorted_idx = np.argsort(-imp_current)
            te_rank_current = int(np.where(sorted_idx == te_col_idx)[0][0]) + 1

    if te_col_idx is not None and hasattr(model_oof, 'feature_importances_'):
        imp_oof = model_oof.feature_importances_.astype(float)
        total = imp_oof.sum()
        if total > 0:
            te_importance_oof = float(imp_oof[te_col_idx] / total)
            sorted_idx = np.argsort(-imp_oof)
            te_rank_oof = int(np.where(sorted_idx == te_col_idx)[0][0]) + 1

    if te_importance_current is not None:
        print(f"\n  te_acquiring_bank importance:")
        print(f"    Current: {te_importance_current:.4%} (rank {te_rank_current}/{len(feat_names_a)})")
        print(f"    OOF:     {te_importance_oof:.4%} (rank {te_rank_oof}/{len(feat_names_b)})")

    total_elapsed = time.time() - total_start

    # ── Save results ──
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    results = {
        'audit_type': 'te_acquiring_bank_train_serve_skew',
        'timestamp': datetime.now().isoformat(),
        'db_path': os.path.abspath(DB_PATH),
        'initial_rows': len(df),
        'train_size': split_idx,
        'test_size': len(df) - split_idx,
        'approval_rate': round(float(df['label'].mean()), 6),
        'te_smoothing_m': TE_SMOOTHING_M,
        'current_te': {
            'method': 'expanding_window',
            'description': 'Each row uses mean of all PRIOR rows for that bank (threshold >= 20)',
            'auc': round(float(auc_current), 6),
            'train_time_sec': round(time_current, 2),
            'te_importance_pct': round(te_importance_current, 6) if te_importance_current else None,
            'te_rank': te_rank_current,
        },
        'oof_te': {
            'method': 'TimeSeriesSplit_oof',
            'description': f'TimeSeriesSplit(n_splits=5), smoothed TE (m={TE_SMOOTHING_M}) per fold',
            'auc': round(float(auc_oof), 6),
            'train_time_sec': round(time_oof, 2),
            'te_importance_pct': round(te_importance_oof, 6) if te_importance_oof else None,
            'te_rank': te_rank_oof,
            'fold_details': fold_details,
        },
        'comparison': {
            'auc_delta_signed': round(float(delta_signed), 4),
            'auc_delta_pp': round(float(delta_pp), 4),
            'better_variant': better_variant,
            'correlation_analysis': corr_stats,
        },
        'decision': {
            'code': decision,
            'text': decision_text,
            'thresholds': {
                'no_action_needed': '< 0.5pp',
                'document_and_revisit': '0.5–1.0pp',
                'flag_for_switching': '> 1.0pp',
            },
        },
        'total_elapsed_sec': round(total_elapsed, 1),
    }

    output_path = os.path.join(OUTPUT_DIR, 'te_audit.json')
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n  Results saved: {output_path}")
    print(f"  Total elapsed: {total_elapsed:.1f}s")
    print(f"\nDone.")


if __name__ == '__main__':
    main()
