"""
BinRoute AI — Five-Model Multi-Algorithm Tournament

Queries directly from transaction_attempts (feature_version >= 3).
Trains 5 algorithms per model target, picks the best per model,
then compares ML feature importance vs BinRoute's rule-based routing factors.

Models:
  1. Initial     — first-time main orders (cascade_position = 0)
  2. Upsell      — upsell initial + upsell rebill orders
  3. Cascade     — retry attempts in cascade chain
  4. Rebill      — natural recurring charges (attempt 1)
  5. Rebill Salvage — retry failed rebills (attempt 2+)

Algorithms per model:
  1. Logistic Regression (baseline)
  2. Random Forest
  3. XGBoost
  4. LightGBM
  5. CatBoost

Split: Time-based (oldest 80% train, newest 20% test).

Usage: py -3 scripts/ml/train_four_models.py [--db=PATH]
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

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    roc_auc_score, f1_score, precision_score, recall_score,
    accuracy_score, log_loss,
)

import xgboost as xgb
import lightgbm as lgb
from catboost import CatBoostClassifier

warnings.filterwarnings('ignore')

# ── Paths ──
DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models')
TRAIN_RATIO = 0.80

for arg in sys.argv:
    if arg.startswith('--db='):
        DB_PATH = arg.split('=', 1)[1]

# ── Feature definitions per model ──

SHARED_CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'offer_name', 'billing_state',
]

SHARED_NUMERICAL = [
    'is_prepaid', 'order_total', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'mid_age_days',
]

# Issuer bank grouping: top banks by volume, rare banks → OTHER
ISSUER_MIN_COUNT = 500

MODEL_CONFIGS = {
    'initial': {
        'name': 'Initial (Main)',
        'filter': lambda df: (df['model_target'] == 'initial') & (df['derived_product_role'].str.contains('main', na=False)),
        'categorical': [
            'processor_name', 'acquiring_bank', 'mcc_code',
            'issuer_bank_grouped', 'card_brand', 'card_type',
            'billing_state', 'client_id',
        ],
        'numerical': [
            'is_prepaid', 'hour_of_day', 'day_of_week',
            'mid_velocity_daily', 'customer_history_on_proc',
            'bin_velocity_weekly', 'mid_age_days',
            'bin_approval_rate', 'bin_proc_approval_rate',
            'te_acquiring_bank', 'bin_approval_7d', 'bin_approval_30d',
            'amount_vs_bin_avg', 'is_near_payday', 'is_weekend',
        ],
        'force_algo': 'LightGBM',
        'filename': 'five_model_initial',
        'needs_enrichment': True,
    },
    'upsell': {
        'name': 'Upsell',
        'filter': lambda df: df['derived_product_role'].str.contains('upsell', na=False),
        'categorical': SHARED_CATEGORICAL + [
            'client_id', 'derived_product_role', 'initial_processor',
        ],
        'numerical': SHARED_NUMERICAL + [
            'initial_was_payfac', 'initial_amount',
            'consecutive_approvals', 'days_since_initial',
        ],
        'filename': 'five_model_upsell',
    },
    'cascade': {
        'name': 'Cascade',
        'filter': lambda df: (df['model_target'] == 'cascade') & (~df['derived_product_role'].str.contains('upsell', na=False)),
        'categorical': SHARED_CATEGORICAL + [
            'initial_decline_reason', 'initial_declined_processor',
        ],
        'numerical': SHARED_NUMERICAL + [
            'attempt_seq', 'cascade_position', 'total_attempts',
            'had_nsf', 'had_do_not_honor', 'had_pickup',
        ],
        'filename': 'five_model_cascade',
    },
    'rebill': {
        'name': 'Rebill (Main)',
        'filter': lambda df: (df['model_target'] == 'rebill') & (df['derived_product_role'].str.contains('main', na=False)),
        'categorical': SHARED_CATEGORICAL + [
            'card_level',
        ],
        'numerical': [n for n in SHARED_NUMERICAL if n != 'mid_age_days'] + [
            'consecutive_approvals', 'days_since_last_charge',
            'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
            'initial_amount', 'amount_ratio', 'initial_was_payfac',
            # Model B rate features (replace processor name categoricals)
            'target_proc_rebill_rate', 'initial_proc_rebill_rate',
            'last_proc_rebill_rate', 'is_same_as_initial', 'is_same_as_last',
        ],
        'filename': 'five_model_rebill',
        'needs_rate_enrichment': True,
    },
    'rebill_salvage': {
        'name': 'Rebill Salvage (Main)',
        'filter': lambda df: (df['model_target'] == 'rebill_salvage') & (df['derived_product_role'].str.contains('main', na=False)),
        'categorical': SHARED_CATEGORICAL + [
            'initial_processor', 'last_approved_processor',
            'parent_declined_processor', 'prev_decline_reason',
        ],
        'numerical': SHARED_NUMERICAL + [
            'attempt_seq',
            'consecutive_approvals', 'days_since_last_charge',
            'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
            'initial_amount', 'amount_ratio', 'prior_declines_in_cycle',
            'cascade_position', 'total_attempts',
        ],
        'filename': 'five_model_rebill_salvage',
    },
}

# BinRoute's rule-based routing uses these dimensions
BINROUTE_ROUTING_FACTORS = {
    'BIN (cc_first_6)':          'Routing decisions are primarily BIN-based: per-BIN gateway selection',
    'gateway_id / processor':    'Which MID to route to — the core routing decision',
    'tx_type (product role)':    'INITIALS vs REBILLS vs UPSELLS — separate routing rules per type',
    'mcc_code':                  'MCC matching constraints — must match gateway MCC',
    'approval_rate (historical)':'Historical approval rate per BIN+gateway combo drives recommendations',
    'confidence / sample_size':  'Statistical confidence from weekly variance + sample count',
    'cascade_chain':             'Cascade fallback order: which MID to try next after decline',
    'decline_reason':            'Hard vs soft decline determines if cascade should continue',
}


# ═══════════════════════════════════════════════════════════════════════════
# Data Loading
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
          -- Filter customer_input + system_decline (unrecoverable, no routing signal)
          AND ta.id NOT IN (
            SELECT ta2.id FROM transaction_attempts ta2
            JOIN decline_reason_classes drc ON ta2.decline_reason = drc.decline_reason
            WHERE drc.decline_class IN ('customer_input', 'system_decline')
          )
          -- Filter excluded gateways (Payfac, dry-run MIDs)
          AND ta.gateway_id NOT IN (
            SELECT g.gateway_id FROM gateways g
            WHERE g.client_id = ta.client_id AND g.exclude_from_analysis = 1
          )
          -- Filter test BINs
          AND ta.cc_first_6 NOT IN ('144444','777777','444444','411111','000000','666666','518426')
        ORDER BY ta.acquisition_date ASC, ta.id ASC
    """, conn)
    conn.close()

    df['label'] = (df['outcome'] == 'approved').astype(int)
    print(f"  Loaded {len(df):,} attempts from transaction_attempts")
    print(f"  Model targets: {df['model_target'].value_counts().to_dict()}")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Initial Model Enrichment (expanding window, no leakage)
# ═══════════════════════════════════════════════════════════════════════════

def enrich_initial_features(df):
    """
    Compute BIN-level and target-encoded features for the initial model.
    Uses expanding window: each row only sees data from BEFORE it.
    Must be called on time-sorted data (acquisition_date ASC).

    Rolling 7d/30d windows use running counters with a deque for O(1) amortized
    per row instead of scanning the full event list.
    """
    from collections import defaultdict, deque
    import bisect

    n = len(df)
    global_rate = df['label'].mean()

    # Parse dates once — vectorized
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
    timestamps = dates.values.astype(np.int64) // 10**9  # seconds
    ts_valid = ~np.isnan(dates.values.astype(np.float64))

    # Pre-allocate output
    bin_approval = np.full(n, np.nan)
    bin_proc_approval = np.full(n, np.nan)
    bin_approval_7d = np.full(n, np.nan)
    bin_approval_30d = np.full(n, np.nan)
    amount_vs_bin = np.ones(n)
    te_acq_bank = np.full(n, np.nan)

    # Accumulators — all-time
    bin_stats = {}                    # {bin: [approvals, total]}
    bin_proc_stats = {}               # {(bin, proc): [approvals, total]}
    bin_amount_stats = {}             # {bin: [sum_amount, count]}
    acq_stats = {}                    # {acq_bank: [approvals, total]}

    # Rolling window accumulators per BIN
    # Each BIN stores a deque of (timestamp, approved) sorted by time
    # Plus running counters for 7d and 30d windows
    bin_window = defaultdict(lambda: {'events': deque(), 'a7': 0, 't7': 0, 'a30': 0, 't30': 0})

    SECS_7D = 7 * 86400
    SECS_30D = 30 * 86400

    print(f"  Computing BIN-level features (expanding window, optimized)...")

    for i in range(n):
        bin6 = bins[i]
        proc = procs[i]
        acq = acqs[i]
        amt = amts[i]
        approved = labels[i]
        ts = timestamps[i]
        has_ts = ts_valid[i]

        # ── READ from history ──

        # BIN all-time
        if bin6 in bin_stats:
            a, t = bin_stats[bin6]
            if t >= 5:
                bin_approval[i] = a / t

        # BIN × processor
        bp_key = (bin6, proc)
        if bp_key in bin_proc_stats:
            a, t = bin_proc_stats[bp_key]
            if t >= 3:
                bin_proc_approval[i] = a / t

        # BIN rolling 7d/30d — expire old events from the window
        if has_ts and bin6 in bin_window:
            w = bin_window[bin6]
            # Expire events older than 30d
            while w['events'] and w['events'][0][0] < ts - SECS_30D:
                old_ts, old_app = w['events'].popleft()
                w['t30'] -= 1
                w['a30'] -= old_app
                # If it was already expired from 7d, don't double-subtract
                # (7d events expire naturally before 30d)
            # Expire 7d counter by scanning from front
            # We track 7d separately: events in [ts-7d, ts-30d] are in 30d but not 7d
            # Simpler approach: recount 7d from 30d window (max 30d of events)
            a7, t7 = 0, 0
            for evt_ts, evt_app in w['events']:
                if evt_ts >= ts - SECS_7D:
                    t7 += 1
                    a7 += evt_app

            if t7 >= 3:
                bin_approval_7d[i] = a7 / t7
            if w['t30'] >= 5:
                bin_approval_30d[i] = w['a30'] / w['t30']

        # Amount vs BIN avg
        if bin6 in bin_amount_stats:
            s, c = bin_amount_stats[bin6]
            if c >= 5 and s > 0:
                avg = s / c
                amount_vs_bin[i] = amt / avg if avg > 0 else 1.0

        # Target-encoded acquiring bank
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

        # Add to rolling window
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

    print(f"  Enrichment done: {len(bin_stats):,} BINs, {len(bin_proc_stats):,} BIN×Proc combos")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Rebill Rate Feature Enrichment (expanding window, no leakage)
# ═══════════════════════════════════════════════════════════════════════════

REBILL_RATE_MIN_SAMPLES = 15

def enrich_rebill_rate_features(df):
    """
    Compute processor rate features for the rebill model using expanding window.
    For each rebill row, looks up historical approval rates per
    issuer × card_type × processor from ALL prior rows (no leakage).

    Adds: target_proc_rebill_rate, initial_proc_rebill_rate,
          last_proc_rebill_rate, is_same_as_initial, is_same_as_last
    """
    from collections import defaultdict

    n = len(df)
    global_rate = df['label'].mean()

    target_rate = np.full(n, np.nan)
    init_rate = np.full(n, np.nan)
    last_rate = np.full(n, np.nan)
    is_same_init = np.zeros(n, dtype=np.float32)
    is_same_last = np.zeros(n, dtype=np.float32)

    # Extract arrays for fast access
    issuers = df['issuer_bank'].fillna('UNKNOWN').values
    ctypes = df['card_type'].fillna('UNKNOWN').values
    procs = df['processor_name'].values
    init_procs = df['initial_processor'].fillna('UNKNOWN').values
    last_procs = df['last_approved_processor'].fillna('UNKNOWN').values
    labels = df['label'].values

    # Accumulator: (issuer, card_type, processor) -> [approvals, total]
    stats = defaultdict(lambda: [0, 0])

    print(f"  Computing rebill rate features (expanding window)...")

    for i in range(n):
        issuer = issuers[i]
        ct = ctypes[i]
        proc = procs[i]
        ip = init_procs[i]
        lp = last_procs[i]

        # ── READ: lookup rates from history ──
        key_target = (issuer, ct, proc)
        if stats[key_target][1] >= REBILL_RATE_MIN_SAMPLES:
            a, t = stats[key_target]
            target_rate[i] = a / t

        key_init = (issuer, ct, ip)
        if stats[key_init][1] >= REBILL_RATE_MIN_SAMPLES:
            a, t = stats[key_init]
            init_rate[i] = a / t

        key_last = (issuer, ct, lp)
        if stats[key_last][1] >= REBILL_RATE_MIN_SAMPLES:
            a, t = stats[key_last]
            last_rate[i] = a / t

        # Boolean features (always available)
        is_same_init[i] = 1.0 if proc == ip else 0.0
        is_same_last[i] = 1.0 if proc == lp else 0.0

        # ── WRITE: update accumulator ──
        stats[key_target][0] += labels[i]
        stats[key_target][1] += 1

        if (i + 1) % 100000 == 0:
            print(f"    {i+1:,}/{n:,}")

    df['target_proc_rebill_rate'] = pd.Series(target_rate, index=df.index).fillna(global_rate)
    df['initial_proc_rebill_rate'] = pd.Series(init_rate, index=df.index).fillna(global_rate)
    df['last_proc_rebill_rate'] = pd.Series(last_rate, index=df.index).fillna(global_rate)
    df['is_same_as_initial'] = is_same_init
    df['is_same_as_last'] = is_same_last

    covered = (target_rate == target_rate).sum()  # non-NaN before fillna
    print(f"  Rebill rate enrichment done: {len(stats):,} combos, {covered:,}/{n:,} had target rate data")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Feature Preparation
# ═══════════════════════════════════════════════════════════════════════════

def prepare_features(df, config):
    """Encode categoricals, fill NAs, return X, y, feature_names, encoders."""
    encoders = {}
    encoded_cols = []

    for col in config['categorical']:
        le = LabelEncoder()
        values = df[col].fillna('UNKNOWN').astype(str)
        le.fit(values)
        df[f'{col}_enc'] = le.transform(values)
        encoders[col] = le
        encoded_cols.append(f'{col}_enc')

    for col in config['numerical']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    feature_cols = encoded_cols + config['numerical']
    X = df[feature_cols].values.astype(np.float32)
    y = df['label'].values
    feature_names = [col.replace('_enc', '') for col in feature_cols]

    return X, y, feature_names, encoders


# ═══════════════════════════════════════════════════════════════════════════
# Model Builders
# ═══════════════════════════════════════════════════════════════════════════

def build_models(scale_pos_weight):
    """Return dict of model_name -> (model, needs_scaling)."""
    return {
        'Logistic Regression': (
            LogisticRegression(
                max_iter=1000, solver='lbfgs', C=1.0,
                class_weight='balanced',
            ),
            True,  # needs feature scaling
        ),
        'Random Forest': (
            RandomForestClassifier(
                n_estimators=200, max_depth=12, min_samples_leaf=20,
                class_weight='balanced', n_jobs=-1, random_state=42,
            ),
            False,
        ),
        'XGBoost': (
            xgb.XGBClassifier(
                n_estimators=300, max_depth=8, learning_rate=0.1,
                subsample=0.8, colsample_bytree=0.8,
                scale_pos_weight=scale_pos_weight,
                eval_metric='logloss', random_state=42,
                tree_method='hist',
            ),
            False,
        ),
        'LightGBM': (
            lgb.LGBMClassifier(
                n_estimators=300, max_depth=8, learning_rate=0.1,
                subsample=0.8, colsample_bytree=0.8,
                scale_pos_weight=scale_pos_weight,
                verbose=-1, random_state=42,
            ),
            False,
        ),
        'CatBoost': (
            CatBoostClassifier(
                iterations=300, depth=8, learning_rate=0.1,
                auto_class_weights='Balanced',
                verbose=0, random_seed=42,
            ),
            False,
        ),
    }


def train_and_evaluate(name, model, X_train, X_test, y_train, y_test, needs_scaling=False):
    """Train model, evaluate, return metrics dict."""
    from sklearn.preprocessing import StandardScaler

    X_tr, X_te = X_train.copy(), X_test.copy()
    if needs_scaling:
        scaler = StandardScaler()
        X_tr = scaler.fit_transform(X_tr)
        X_te = scaler.transform(X_te)

    print(f"      {name:<22}", end="", flush=True)
    start = time.time()
    model.fit(X_tr, y_train)
    elapsed = time.time() - start

    y_prob = model.predict_proba(X_te)[:, 1]
    y_pred = model.predict(X_te)

    auc = roc_auc_score(y_test, y_prob)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    prec = precision_score(y_test, y_pred, zero_division=0)
    rec = recall_score(y_test, y_pred, zero_division=0)
    acc = accuracy_score(y_test, y_pred)
    ll = log_loss(y_test, y_prob)

    print(f" AUC: {auc:.4f}  F1: {f1:.4f}  LogLoss: {ll:.4f}  ({elapsed:.1f}s)")

    return {
        'model': model,
        'auc': auc, 'f1': f1, 'precision': prec, 'recall': rec,
        'accuracy': acc, 'log_loss': ll, 'train_time': elapsed,
        'needs_scaling': needs_scaling,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Feature Importance Extraction
# ═══════════════════════════════════════════════════════════════════════════

def get_feature_importance(model, feature_names, model_name):
    """Extract normalized feature importance from any model type."""
    if hasattr(model, 'feature_importances_'):
        raw = model.feature_importances_.astype(float)
    elif hasattr(model, 'coef_'):
        raw = np.abs(model.coef_[0]).astype(float)
    else:
        return []

    total = raw.sum()
    if total > 0:
        normalized = raw / total
    else:
        normalized = raw

    pairs = sorted(zip(feature_names, normalized, raw),
                   key=lambda x: x[1], reverse=True)
    return pairs


# ═══════════════════════════════════════════════════════════════════════════
# ML vs BinRoute Routing Comparison
# ═══════════════════════════════════════════════════════════════════════════

def compare_ml_vs_routing(all_model_results):
    """
    Compare what ML models find important vs what BinRoute's rule-based
    routing uses. Groups features into routing-relevant vs ML-discovered.
    """
    print("\n" + "=" * 75)
    print("  ML FEATURE INSIGHTS vs BINROUTE ROUTING FACTORS")
    print("=" * 75)

    # Features that map to BinRoute's routing dimensions
    routing_features = {
        'processor_name', 'acquiring_bank', 'mcc_code', 'gateway_id',
    }
    # Features that are card/BIN identity (BinRoute routes by BIN)
    bin_identity_features = {
        'issuer_bank', 'card_brand', 'card_type', 'is_prepaid',
    }
    # Decline/cascade features (BinRoute uses decline_reason for cascade)
    cascade_features = {
        'had_nsf', 'had_do_not_honor', 'had_pickup',
        'initial_decline_reason', 'initial_declined_processor',
        'prev_decline_reason', 'parent_declined_processor',
        'cascade_position', 'total_attempts', 'attempt_seq',
    }
    # ML-discovered behavioral signals (NOT used by rule-based routing)
    ml_behavioral_features = {
        'mid_velocity_daily', 'mid_velocity_weekly',
        'bin_velocity_weekly', 'customer_history_on_proc',
        'hour_of_day', 'day_of_week', 'mid_age_days',
        'order_total', 'billing_state', 'offer_name',
        'bin_approval_rate', 'bin_proc_approval_rate',
        'bin_approval_7d', 'bin_approval_30d',
        'te_acquiring_bank', 'amount_vs_bin_avg', 'is_near_payday',
    }
    # Subscription journey features (ML-only)
    subscription_features = {
        'consecutive_approvals', 'days_since_last_charge',
        'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
        'initial_amount', 'amount_ratio', 'prior_declines_in_cycle',
        'initial_processor', 'last_approved_processor',
        'target_proc_rebill_rate', 'initial_proc_rebill_rate',
        'last_proc_rebill_rate', 'is_same_as_initial', 'is_same_as_last',
    }

    categories = [
        ('ROUTING (gateway/processor/MCC)',  routing_features),
        ('BIN IDENTITY (issuer/brand/type)', bin_identity_features),
        ('CASCADE/DECLINE signals',          cascade_features),
        ('BEHAVIORAL (velocity/temporal)',    ml_behavioral_features),
        ('SUBSCRIPTION JOURNEY',             subscription_features),
    ]

    for model_key, result in all_model_results.items():
        importances = result.get('importances', [])
        if not importances:
            continue

        print(f"\n  --- {result['name']} (winner: {result['winner_algo']}, AUC: {result['winner_auc']:.4f}) ---\n")

        # Build lookup
        imp_map = {feat: (norm, raw) for feat, norm, raw in importances}

        # Score each category
        cat_scores = []
        for cat_name, cat_feats in categories:
            present = [(f, imp_map[f][0]) for f in cat_feats if f in imp_map]
            total_importance = sum(v for _, v in present)
            cat_scores.append((cat_name, total_importance, present))

        cat_scores.sort(key=lambda x: x[1], reverse=True)

        print(f"    {'Feature Category':<40} {'Total %':>8}  Top Contributors")
        print(f"    {'-'*85}")
        for cat_name, total_imp, features in cat_scores:
            if not features:
                continue
            top3 = sorted(features, key=lambda x: x[1], reverse=True)[:3]
            top3_str = ', '.join(f"{f} ({v:.1%})" for f, v in top3)
            print(f"    {cat_name:<40} {total_imp:>7.1%}  {top3_str}")

        # What BinRoute routing DOESN'T capture
        routing_total = sum(s for cn, s, _ in cat_scores if 'ROUTING' in cn or 'BIN' in cn)
        ml_total = sum(s for cn, s, _ in cat_scores if 'BEHAVIORAL' in cn or 'SUBSCRIPTION' in cn or 'CASCADE' in cn)

        print(f"\n    Rule-based routing captures:  {routing_total:.1%} of model signal")
        print(f"    ML-only signals contribute:   {ml_total:.1%} of model signal")

        if ml_total > routing_total:
            gap = ml_total - routing_total
            print(f"    >>> ML discovers {gap:.1%} MORE signal than rule-based routing alone")
        else:
            print(f"    >>> Rule-based routing covers the dominant signals for this model")

    # Overall summary
    print(f"\n{'='*75}")
    print("  SUMMARY: What ML Adds Beyond BinRoute's Rule-Based Routing")
    print(f"{'='*75}")
    print("""
    BinRoute's current routing engine optimizes by:
      - BIN -> Gateway mapping (historical approval rates)
      - Transaction type segmentation (initials vs rebills)
      - MCC code matching constraints
      - Cascade chain ordering after declines

    ML models discover ADDITIONAL signals that rule-based routing cannot use:
      - Velocity patterns (MID load, BIN hotspots) — timing matters
      - Subscription health (consecutive approvals, lifetime value)
      - Temporal patterns (hour-of-day, day-of-week effects)
      - MID age / maturity effects on approval rates
      - Cross-processor customer history

    RECOMMENDATION: Use ML models to SCORE each gateway option before routing,
    combining BinRoute's constraint-based filtering with ML's probability
    estimates for a hybrid approach.
    """)


# ═══════════════════════════════════════════════════════════════════════════
# Per-client breakdown
# ═══════════════════════════════════════════════════════════════════════════

def per_client_evaluation(model, df_test, feature_cols, feature_names):
    """Show AUC per client for the winning model."""
    X_test = df_test[feature_cols].values.astype(np.float32)
    y_test = df_test['label'].values
    y_prob = model.predict_proba(X_test)[:, 1]

    print(f"\n      Per Client:")
    print(f"      {'Client':>10} {'Count':>8} {'Appr%':>7} {'AUC':>8}")
    print(f"      {'-'*36}")
    for cid in sorted(df_test['client_id'].unique()):
        mask = df_test['client_id'].values == cid
        sub_y = y_test[mask]
        sub_p = y_prob[mask]
        if len(sub_y) < 20 or len(set(sub_y)) < 2:
            continue
        auc = roc_auc_score(sub_y, sub_p)
        appr = sub_y.mean() * 100
        print(f"      {cid:>10} {mask.sum():>8,} {appr:>6.1f}% {auc:>7.4f}")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 75)
    print("  BinRoute AI — Five-Model Multi-Algorithm Tournament")
    print("  Source: transaction_attempts (feature_version >= 3)")
    print("=" * 75)
    print(f"  DB: {os.path.abspath(DB_PATH)}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    print(f"\n[1/4] Loading data...")
    df_all = load_data()

    # Enrich with BIN-level features for initial model
    print(f"\n[1.5/4] Enriching initial model features...")
    df_all = enrich_initial_features(df_all)

    # Enrich rebill rate features (expanding window on rebill rows only)
    print(f"\n[1.6/4] Enriching rebill rate features...")
    rebill_mask = (df_all['model_target'] == 'rebill') & (df_all['derived_product_role'].str.contains('main', na=False))
    rebill_idx = df_all.index[rebill_mask]
    if len(rebill_idx) > 0:
        rebill_df = df_all.loc[rebill_idx].copy()
        rebill_df = enrich_rebill_rate_features(rebill_df)
        for col in ['target_proc_rebill_rate', 'initial_proc_rebill_rate',
                     'last_proc_rebill_rate', 'is_same_as_initial', 'is_same_as_last']:
            df_all[col] = 0.0
            df_all.loc[rebill_idx, col] = rebill_df[col].values

    all_results = {}
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Train each model
    print(f"\n[2/4] Training tournament per model target...\n")

    for model_key, config in MODEL_CONFIGS.items():
        print(f"  {'='*70}")
        print(f"  MODEL: {config['name']}")
        print(f"  {'='*70}")

        # Filter
        df = df_all.loc[config['filter'](df_all)].copy()
        if len(df) < 100:
            print(f"  SKIP — only {len(df)} rows")
            continue

        appr_rate = df['label'].mean()
        print(f"  Rows: {len(df):,}")
        print(f"  Approved: {df['label'].sum():,} ({appr_rate:.1%})")
        print(f"  Declined: {(1-df['label']).sum():.0f} ({1-appr_rate:.1%})")

        # Prepare features
        X, y, feature_names, encoders = prepare_features(df, config)
        encoded_cols = [f'{c}_enc' for c in config['categorical']]
        feature_cols = encoded_cols + config['numerical']

        # Time-based split
        split_idx = int(len(X) * TRAIN_RATIO)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        print(f"  Train: {len(X_train):,} | Test: {len(X_test):,}")
        print(f"  Train appr: {y_train.mean():.1%} | Test appr: {y_test.mean():.1%}")

        # Build and run tournament
        scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
        models = build_models(scale_pos_weight)

        # If force_algo is set, only run that algorithm
        force_algo = config.get('force_algo')
        if force_algo:
            models = {k: v for k, v in models.items() if k == force_algo}
            print(f"\n    Forced algorithm: {force_algo}")
        else:
            print(f"\n    Algorithm Tournament:")

        algo_results = {}
        for algo_name, (model, needs_scaling) in models.items():
            try:
                result = train_and_evaluate(
                    algo_name, model, X_train, X_test, y_train, y_test, needs_scaling
                )
                algo_results[algo_name] = result
            except Exception as e:
                print(f"      {algo_name:<22} FAILED: {e}")

        if not algo_results:
            print(f"  No algorithms succeeded!")
            continue

        # Find winner
        winner_name = max(algo_results, key=lambda k: algo_results[k]['auc'])
        winner = algo_results[winner_name]

        # Print comparison table
        print(f"\n    {'Algorithm':<22} {'AUC':>8} {'F1':>8} {'Prec':>8} {'Recall':>8} {'LogLoss':>9} {'Time':>7}")
        print(f"    {'-'*72}")
        for name in sorted(algo_results, key=lambda k: algo_results[k]['auc'], reverse=True):
            r = algo_results[name]
            marker = " <-- WINNER" if name == winner_name else ""
            print(f"    {name:<22} {r['auc']:>7.4f} {r['f1']:>7.4f} "
                  f"{r['precision']:>7.4f} {r['recall']:>7.4f} {r['log_loss']:>8.4f} "
                  f"{r['train_time']:>6.1f}s{marker}")

        # Feature importance from winner
        importances = get_feature_importance(winner['model'], feature_names, winner_name)

        print(f"\n    Top 15 Features ({winner_name}):")
        print(f"    {'Rank':>4} {'Feature':<35} {'Importance':>10} {'Pct':>7}")
        print(f"    {'-'*60}")
        for i, (feat, norm, raw) in enumerate(importances[:15], 1):
            bar = "|" * min(int(norm * 80), 40)
            print(f"    {i:>4} {feat:<35} {raw:>10.0f} {norm:>6.1%}  {bar}")

        # Per-client breakdown
        df_test = df.iloc[split_idx:].copy()
        per_client_evaluation(winner['model'], df_test, feature_cols, feature_names)

        # Save winner model
        import pickle
        pkl_path = os.path.join(OUTPUT_DIR, f'{config["filename"]}.pkl')
        with open(pkl_path, 'wb') as f:
            pickle.dump(winner['model'], f)
        print(f"\n    Saved: {pkl_path}")

        # Store results
        all_results[model_key] = {
            'name': config['name'],
            'rows': len(df),
            'approval_rate': float(appr_rate),
            'winner_algo': winner_name,
            'winner_auc': winner['auc'],
            'importances': importances,
            'algorithms': {
                name: {
                    'auc': r['auc'], 'f1': r['f1'],
                    'precision': r['precision'], 'recall': r['recall'],
                    'accuracy': r['accuracy'], 'log_loss': r['log_loss'],
                    'train_time': r['train_time'],
                }
                for name, r in algo_results.items()
            },
            'features': feature_names,
            'top_features': [(f, float(n), float(r)) for f, n, r in importances[:15]],
        }

    # ── Summary ──
    print(f"\n[3/4] Tournament Summary\n")
    print(f"  {'Model':<20} {'Rows':>9} {'Appr%':>7} {'Winner':<22} {'AUC':>8} {'F1':>8}")
    print(f"  {'-'*76}")
    for key, r in all_results.items():
        algos = r['algorithms']
        winner_f1 = algos[r['winner_algo']]['f1']
        print(f"  {r['name']:<20} {r['rows']:>9,} {r['approval_rate']:>6.1%} "
              f"{r['winner_algo']:<22} {r['winner_auc']:>7.4f} {winner_f1:>7.4f}")

    # Previous 3-model baseline comparison
    print(f"\n  Previous 3-Model Baseline (from tx_features):")
    print(f"    Initial+Cascade: AUC 0.9633 | Rebill: AUC 0.8604 | Salvage: AUC 0.7861")
    print(f"    Blended single model: AUC 0.9085 (LightGBM)")

    # Cross-algorithm comparison
    print(f"\n  Algorithm Win Count:")
    win_counts = {}
    for r in all_results.values():
        w = r['winner_algo']
        win_counts[w] = win_counts.get(w, 0) + 1
    for algo, count in sorted(win_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"    {algo:<22} {count} win(s)")

    # Full algo comparison across all models
    print(f"\n  Average AUC by Algorithm (across all 4 models):")
    algo_aucs = {}
    for r in all_results.values():
        for algo_name, algo_r in r['algorithms'].items():
            if algo_name not in algo_aucs:
                algo_aucs[algo_name] = []
            algo_aucs[algo_name].append(algo_r['auc'])

    print(f"    {'Algorithm':<22} {'Avg AUC':>8} {'Min':>8} {'Max':>8}")
    print(f"    {'-'*48}")
    for algo in sorted(algo_aucs, key=lambda k: np.mean(algo_aucs[k]), reverse=True):
        aucs = algo_aucs[algo]
        print(f"    {algo:<22} {np.mean(aucs):>7.4f} {min(aucs):>7.4f} {max(aucs):>7.4f}")

    # ── ML vs Routing Comparison ──
    print(f"\n[4/4] ML vs BinRoute Routing Insights...")
    compare_ml_vs_routing(all_results)

    # ── Save results JSON ──
    save_data = {
        'trained_at': datetime.now().isoformat(),
        'source': 'transaction_attempts (feature_version >= 3)',
        'total_attempts': len(df_all),
        'train_ratio': TRAIN_RATIO,
        'models': {},
    }

    for key, r in all_results.items():
        save_data['models'][key] = {
            'name': r['name'],
            'rows': r['rows'],
            'approval_rate': r['approval_rate'],
            'winner': r['winner_algo'],
            'winner_auc': r['winner_auc'],
            'features': r['features'],
            'top_features': [
                {'feature': f, 'importance_pct': round(n, 4), 'importance_raw': round(raw, 1)}
                for f, n, raw in r['top_features']
            ],
            'algorithms': r['algorithms'],
        }

    results_path = os.path.join(OUTPUT_DIR, 'four_model_results.json')
    with open(results_path, 'w') as f:
        json.dump(save_data, f, indent=2, default=str)
    print(f"\n  Results saved: {results_path}")

    print(f"\nDone! Total time: trained {len(all_results)} models x 5 algorithms = "
          f"{len(all_results) * 5} model runs.")


if __name__ == '__main__':
    main()
