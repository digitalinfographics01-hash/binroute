"""
BinRoute AI — Initial Model Comprehensive Experiment

Tests EVERY combination of algorithms, features, and hyperparameters
to find the maximum achievable AUC for the Initial model.

Phases:
  1. Algorithm tournament (LightGBM vs CatBoost native vs XGBoost)
  2. Feature additions (one at a time, on top 2 algorithms)
  3. Feature ablation (remove one at a time)
  4. Hyperparameter sweep
  5. Best combination

Usage: py -3 scripts/ml/test_initial_comprehensive.py [--db=PATH]
"""

import os, sys, time, sqlite3, warnings
import numpy as np
import pandas as pd
from collections import defaultdict, deque
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score

import lightgbm as lgb
import xgboost as xgb
from catboost import CatBoostClassifier

warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
for arg in sys.argv:
    if arg.startswith('--db='):
        DB_PATH = arg.split('=', 1)[1]

TRAIN_RATIO = 0.80
ISSUER_MIN_COUNT = 500


# ════════════════════════════════════════════════════════════════
# Current baseline feature sets
# ════════════════════════════════════════════════════════════════

BASE_CAT = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank_grouped', 'card_brand', 'card_type',
    'billing_state', 'client_id',
]
BASE_NUM = [
    'is_prepaid', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'customer_history_on_proc',
    'bin_velocity_weekly', 'mid_age_days',
    'bin_approval_rate', 'bin_proc_approval_rate',
    'te_acquiring_bank', 'bin_approval_7d', 'bin_approval_30d',
    'amount_vs_bin_avg', 'is_near_payday',
]


# ════════════════════════════════════════════════════════════════
# Data Loading
# ════════════════════════════════════════════════════════════════

def load_data():
    conn = sqlite3.connect(DB_PATH)

    # Check if bin_lookup exists
    has_bin_lookup = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='bin_lookup'", conn
    ).shape[0] > 0

    bl_join = "LEFT JOIN bin_lookup bl ON ta.cc_first_6 = bl.bin" if has_bin_lookup else ""
    bl_col = ", bl.card_level" if has_bin_lookup else ", NULL as card_level"

    df = pd.read_sql_query(f"""
        SELECT
            ta.client_id, ta.order_id, ta.gateway_id,
            ta.processor_name, ta.acquiring_bank, ta.mcc_code,
            ta.outcome, ta.decline_reason,
            ta.cc_first_6, ta.order_total, ta.acquisition_date,
            ta.derived_product_role, ta.model_target,
            ta.offer_name, ta.billing_state,
            ta.issuer_bank, ta.card_brand, ta.card_type, ta.is_prepaid,
            ta.hour_of_day, ta.day_of_week, ta.mid_age_days,
            ta.mid_velocity_daily, ta.mid_velocity_weekly,
            ta.customer_history_on_proc, ta.bin_velocity_weekly,
            ta.initial_was_payfac
            {bl_col}
        FROM transaction_attempts ta
        {bl_join}
        WHERE ta.feature_version >= 3
          AND ta.model_target = 'initial'
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
    print(f"Loaded {len(df):,} initial main attempts")
    print(f"Approval rate: {df['label'].mean():.1%}")
    print(f"Clients: {sorted(df['client_id'].unique())}")
    print(f"Date range: {df['acquisition_date'].min()} → {df['acquisition_date'].max()}")
    return df


# ════════════════════════════════════════════════════════════════
# Feature Enrichment — compute ALL possible features once
# ════════════════════════════════════════════════════════════════

def enrich_all_features(df):
    """Compute every candidate feature using expanding window (no leakage)."""
    n = len(df)
    global_rate = df['label'].mean()

    # ── Simple derived features ──
    dates = pd.to_datetime(df['acquisition_date'], errors='coerce')
    df['day_of_month'] = dates.dt.day.fillna(15).astype(int)
    df['is_near_payday'] = df['day_of_month'].apply(
        lambda d: 1 if d <= 3 or (13 <= d <= 17) or d >= 28 else 0
    )
    df['is_weekend'] = df['day_of_week'].apply(lambda d: 1 if d >= 5 else 0)
    df['month'] = dates.dt.month.fillna(0).astype(int)
    df['bin_prefix_4'] = df['cc_first_6'].fillna('000000').str[:4]
    df['card_level'] = df['card_level'].fillna('UNKNOWN').astype(str)
    df['hour_bucket'] = pd.cut(
        df['hour_of_day'].fillna(12), bins=[0,6,12,18,24],
        labels=['night','morning','afternoon','evening'], include_lowest=True
    ).astype(str)

    # ── Issuer bank grouping ──
    counts = df['issuer_bank'].fillna('UNKNOWN').value_counts()
    top_banks = set(counts[counts >= ISSUER_MIN_COUNT].index)
    df['issuer_bank_grouped'] = df['issuer_bank'].fillna('UNKNOWN').apply(
        lambda x: x if x in top_banks else 'OTHER'
    )
    print(f"  Issuer grouping: {len(top_banks)} banks kept (>={ISSUER_MIN_COUNT}), rest → OTHER")

    # ── Expanding window features (numpy for speed) ──
    bins = df['cc_first_6'].values
    procs = df['processor_name'].values
    acqs = df['acquiring_bank'].values
    issuers = df['issuer_bank'].fillna('UNKNOWN').values
    card_types = df['card_type'].fillna('UNKNOWN').values
    card_brands = df['card_brand'].fillna('UNKNOWN').values
    bstates = df['billing_state'].fillna('UNKNOWN').values
    amts = df['order_total'].fillna(0).values.astype(float)
    labels = df['label'].values
    timestamps = dates.values.astype(np.int64) // 10**9
    ts_valid = ~np.isnan(dates.values.astype(np.float64))

    # Pre-allocate
    bin_approval = np.full(n, np.nan)
    bin_proc_approval = np.full(n, np.nan)
    bin_approval_7d = np.full(n, np.nan)
    bin_approval_30d = np.full(n, np.nan)
    amount_vs_bin = np.ones(n)
    te_acq_bank = np.full(n, np.nan)
    te_processor = np.full(n, np.nan)
    te_billing_state = np.full(n, np.nan)
    te_card_brand = np.full(n, np.nan)
    proc_issuer_rate = np.full(n, np.nan)
    proc_cardtype_rate = np.full(n, np.nan)
    proc_cardbrand_rate = np.full(n, np.nan)
    issuer_cardtype_rate = np.full(n, np.nan)
    bin_distinct_procs = np.zeros(n)
    bin_volume = np.zeros(n)

    # Accumulators
    bin_stats = {}
    bin_proc_stats = {}
    bin_amount_stats = {}
    acq_stats = {}
    proc_stats = {}
    state_stats = {}
    brand_stats = {}
    proc_issuer_stats = {}
    proc_cardtype_stats = {}
    proc_cardbrand_stats = {}
    issuer_cardtype_stats = {}
    bin_procs_seen = {}

    bin_window = defaultdict(lambda: {'events': deque(), 'a30': 0, 't30': 0})
    SECS_7D = 7 * 86400
    SECS_30D = 30 * 86400

    print("  Computing expanding-window features...")

    for i in range(n):
        b6 = bins[i]; proc = procs[i]; acq = acqs[i]
        iss = issuers[i]; ct = card_types[i]; cb = card_brands[i]
        bs = bstates[i]; amt = amts[i]; app = labels[i]
        ts = timestamps[i]; has_ts = ts_valid[i]

        # ── READ from history ──

        if b6 in bin_stats:
            a, t = bin_stats[b6]
            if t >= 5: bin_approval[i] = a / t
            bin_volume[i] = t

        bp = (b6, proc)
        if bp in bin_proc_stats:
            a, t = bin_proc_stats[bp]
            if t >= 3: bin_proc_approval[i] = a / t

        if has_ts and b6 in bin_window:
            w = bin_window[b6]
            while w['events'] and w['events'][0][0] < ts - SECS_30D:
                _, oa = w['events'].popleft(); w['t30'] -= 1; w['a30'] -= oa
            a7, t7 = 0, 0
            for et, ea in w['events']:
                if et >= ts - SECS_7D: t7 += 1; a7 += ea
            if t7 >= 3: bin_approval_7d[i] = a7 / t7
            if w['t30'] >= 5: bin_approval_30d[i] = w['a30'] / w['t30']

        if b6 in bin_amount_stats:
            s, c = bin_amount_stats[b6]
            if c >= 5 and s > 0: amount_vs_bin[i] = amt / (s / c) if (s/c) > 0 else 1.0

        if acq in acq_stats:
            a, t = acq_stats[acq]
            if t >= 20: te_acq_bank[i] = a / t

        if proc in proc_stats:
            a, t = proc_stats[proc]
            if t >= 20: te_processor[i] = a / t

        if bs in state_stats:
            a, t = state_stats[bs]
            if t >= 20: te_billing_state[i] = a / t

        if cb in brand_stats:
            a, t = brand_stats[cb]
            if t >= 20: te_card_brand[i] = a / t

        pi = (proc, iss)
        if pi in proc_issuer_stats:
            a, t = proc_issuer_stats[pi]
            if t >= 10: proc_issuer_rate[i] = a / t

        pc = (proc, ct)
        if pc in proc_cardtype_stats:
            a, t = proc_cardtype_stats[pc]
            if t >= 10: proc_cardtype_rate[i] = a / t

        pcb = (proc, cb)
        if pcb in proc_cardbrand_stats:
            a, t = proc_cardbrand_stats[pcb]
            if t >= 10: proc_cardbrand_rate[i] = a / t

        ic = (iss, ct)
        if ic in issuer_cardtype_stats:
            a, t = issuer_cardtype_stats[ic]
            if t >= 10: issuer_cardtype_rate[i] = a / t

        if b6 in bin_procs_seen:
            bin_distinct_procs[i] = len(bin_procs_seen[b6])

        # ── WRITE into accumulators ──

        if b6 not in bin_stats: bin_stats[b6] = [0, 0]
        bin_stats[b6][1] += 1; bin_stats[b6][0] += app

        if bp not in bin_proc_stats: bin_proc_stats[bp] = [0, 0]
        bin_proc_stats[bp][1] += 1; bin_proc_stats[bp][0] += app

        if b6 not in bin_amount_stats: bin_amount_stats[b6] = [0, 0]
        bin_amount_stats[b6][0] += amt; bin_amount_stats[b6][1] += 1

        if acq not in acq_stats: acq_stats[acq] = [0, 0]
        acq_stats[acq][1] += 1; acq_stats[acq][0] += app

        if proc not in proc_stats: proc_stats[proc] = [0, 0]
        proc_stats[proc][1] += 1; proc_stats[proc][0] += app

        if bs not in state_stats: state_stats[bs] = [0, 0]
        state_stats[bs][1] += 1; state_stats[bs][0] += app

        if cb not in brand_stats: brand_stats[cb] = [0, 0]
        brand_stats[cb][1] += 1; brand_stats[cb][0] += app

        if pi not in proc_issuer_stats: proc_issuer_stats[pi] = [0, 0]
        proc_issuer_stats[pi][1] += 1; proc_issuer_stats[pi][0] += app

        if pc not in proc_cardtype_stats: proc_cardtype_stats[pc] = [0, 0]
        proc_cardtype_stats[pc][1] += 1; proc_cardtype_stats[pc][0] += app

        if pcb not in proc_cardbrand_stats: proc_cardbrand_stats[pcb] = [0, 0]
        proc_cardbrand_stats[pcb][1] += 1; proc_cardbrand_stats[pcb][0] += app

        if ic not in issuer_cardtype_stats: issuer_cardtype_stats[ic] = [0, 0]
        issuer_cardtype_stats[ic][1] += 1; issuer_cardtype_stats[ic][0] += app

        if b6 not in bin_procs_seen: bin_procs_seen[b6] = set()
        bin_procs_seen[b6].add(proc)

        if has_ts:
            w = bin_window[b6]
            w['events'].append((ts, app)); w['t30'] += 1; w['a30'] += app

        if (i + 1) % 50000 == 0:
            print(f"    {i+1:,}/{n:,}")

    # Assign all to dataframe
    df['bin_approval_rate'] = pd.Series(bin_approval, index=df.index).fillna(global_rate)
    df['bin_proc_approval_rate'] = pd.Series(bin_proc_approval, index=df.index).fillna(global_rate)
    df['bin_approval_7d'] = pd.Series(bin_approval_7d, index=df.index).fillna(global_rate)
    df['bin_approval_30d'] = pd.Series(bin_approval_30d, index=df.index).fillna(global_rate)
    df['amount_vs_bin_avg'] = amount_vs_bin
    df['te_acquiring_bank'] = pd.Series(te_acq_bank, index=df.index).fillna(global_rate)
    df['te_processor'] = pd.Series(te_processor, index=df.index).fillna(global_rate)
    df['te_billing_state'] = pd.Series(te_billing_state, index=df.index).fillna(global_rate)
    df['te_card_brand'] = pd.Series(te_card_brand, index=df.index).fillna(global_rate)
    df['proc_issuer_rate'] = pd.Series(proc_issuer_rate, index=df.index).fillna(global_rate)
    df['proc_cardtype_rate'] = pd.Series(proc_cardtype_rate, index=df.index).fillna(global_rate)
    df['proc_cardbrand_rate'] = pd.Series(proc_cardbrand_rate, index=df.index).fillna(global_rate)
    df['issuer_cardtype_rate'] = pd.Series(issuer_cardtype_rate, index=df.index).fillna(global_rate)
    df['bin_distinct_procs'] = bin_distinct_procs
    df['bin_volume'] = bin_volume

    print(f"  Done: {len(bin_stats):,} BINs, {len(proc_issuer_stats):,} proc×issuer, "
          f"{len(proc_cardtype_stats):,} proc×cardtype combos")
    return df


# ════════════════════════════════════════════════════════════════
# Experiment Runner
# ════════════════════════════════════════════════════════════════

def run_experiment(name, algo, df_train_orig, df_test_orig, cat_cols, num_cols, hparams=None):
    """Run one experiment. Returns (overall_auc, {client_id: auc})."""
    if hparams is None: hparams = {}

    df_tr = df_train_orig.copy()
    df_te = df_test_orig.copy()
    y_train = df_tr['label'].values
    y_test = df_te['label'].values

    n_pos = max(y_train.sum(), 1)
    spw = (len(y_train) - n_pos) / n_pos

    start = time.time()

    if algo == 'cb_native':
        # CatBoost with native categorical handling
        feat_cols = cat_cols + num_cols
        for c in cat_cols:
            df_tr[c] = df_tr[c].fillna('UNKNOWN').astype(str)
            df_te[c] = df_te[c].fillna('UNKNOWN').astype(str)
        for c in num_cols:
            df_tr[c] = pd.to_numeric(df_tr[c], errors='coerce').fillna(0)
            df_te[c] = pd.to_numeric(df_te[c], errors='coerce').fillna(0)

        X_tr = df_tr[feat_cols].copy()
        X_te = df_te[feat_cols].copy()
        cat_idx = list(range(len(cat_cols)))

        model = CatBoostClassifier(
            iterations=hparams.get('iterations', 300),
            depth=hparams.get('depth', 8),
            learning_rate=hparams.get('learning_rate', 0.1),
            l2_leaf_reg=hparams.get('l2_leaf_reg', 3),
            auto_class_weights='Balanced',
            cat_features=cat_idx,
            verbose=0, random_seed=42,
        )
        model.fit(X_tr, y_train)
        y_prob = model.predict_proba(X_te)[:, 1]

    else:
        # LabelEncoder path (LightGBM / XGBoost)
        enc_cols = []
        for c in cat_cols:
            le = LabelEncoder()
            v_tr = df_tr[c].fillna('UNKNOWN').astype(str)
            v_te = df_te[c].fillna('UNKNOWN').astype(str)
            le.fit(pd.concat([v_tr, v_te]))
            df_tr[f'{c}_enc'] = le.transform(v_tr)
            df_te[f'{c}_enc'] = le.transform(v_te)
            enc_cols.append(f'{c}_enc')

        for c in num_cols:
            df_tr[c] = pd.to_numeric(df_tr[c], errors='coerce').fillna(0)
            df_te[c] = pd.to_numeric(df_te[c], errors='coerce').fillna(0)

        feat_cols = enc_cols + num_cols
        X_tr = df_tr[feat_cols].values.astype(np.float32)
        X_te = df_te[feat_cols].values.astype(np.float32)

        if algo == 'lgbm':
            model = lgb.LGBMClassifier(
                n_estimators=hparams.get('n_estimators', 300),
                max_depth=hparams.get('max_depth', 8),
                learning_rate=hparams.get('learning_rate', 0.1),
                num_leaves=hparams.get('num_leaves', 31),
                min_child_samples=hparams.get('min_child_samples', 20),
                subsample=hparams.get('subsample', 0.8),
                colsample_bytree=hparams.get('colsample_bytree', 0.8),
                reg_alpha=hparams.get('reg_alpha', 0),
                reg_lambda=hparams.get('reg_lambda', 0),
                scale_pos_weight=spw,
                verbose=-1, random_state=42,
            )
        elif algo == 'xgb':
            model = xgb.XGBClassifier(
                n_estimators=hparams.get('n_estimators', 300),
                max_depth=hparams.get('max_depth', 8),
                learning_rate=hparams.get('learning_rate', 0.1),
                subsample=hparams.get('subsample', 0.8),
                colsample_bytree=hparams.get('colsample_bytree', 0.8),
                scale_pos_weight=spw,
                eval_metric='logloss', random_state=42, tree_method='hist',
            )

        model.fit(X_tr, y_train)
        y_prob = model.predict_proba(X_te)[:, 1]

    elapsed = time.time() - start
    auc = roc_auc_score(y_test, y_prob)

    # Per-client AUC
    client_aucs = {}
    for cid in sorted(df_te['client_id'].unique()):
        mask = df_te['client_id'].values == cid
        if mask.sum() >= 20 and len(set(y_test[mask])) > 1:
            client_aucs[cid] = roc_auc_score(y_test[mask], y_prob[mask])

    return auc, client_aucs, elapsed


def print_result(name, auc, client_aucs, elapsed, baseline_auc=None):
    """Print one result line."""
    diff = f"  ({auc - baseline_auc:+.4f})" if baseline_auc is not None else ""
    clients = "  ".join([f"C{k}:{v:.3f}" for k, v in client_aucs.items()])
    print(f"  {name:<45} AUC={auc:.4f}{diff}  [{elapsed:.1f}s]  {clients}")


# ════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════

def main():
    print("=" * 90)
    print("INITIAL MODEL — COMPREHENSIVE EXPERIMENT")
    print("=" * 90)

    # ── Load & enrich ──
    df = load_data()
    df = enrich_all_features(df)

    # ── Time-based split ──
    split_idx = int(len(df) * TRAIN_RATIO)
    df_train = df.iloc[:split_idx].copy()
    df_test = df.iloc[split_idx:].copy()
    print(f"\nSplit: {len(df_train):,} train / {len(df_test):,} test")
    print(f"Train approval: {df_train['label'].mean():.1%}  Test approval: {df_test['label'].mean():.1%}")

    results = []  # (name, auc, client_aucs, elapsed)

    def run(name, algo, cat, num, hp=None):
        auc, ca, el = run_experiment(name, algo, df_train, df_test, cat, num, hp)
        results.append((name, auc, ca, el))
        return auc

    # ════════════════════════════════════════════════════════════
    # PHASE 1: Algorithm Tournament (current features)
    # ════════════════════════════════════════════════════════════
    print("\n" + "─" * 90)
    print("PHASE 1: ALGORITHM TOURNAMENT (current feature set)")
    print("─" * 90)

    baseline = run("A1: LightGBM (baseline)",    'lgbm',      BASE_CAT, BASE_NUM)
    run("A2: CatBoost native",                    'cb_native', BASE_CAT, BASE_NUM)
    run("A3: XGBoost",                            'xgb',       BASE_CAT, BASE_NUM)

    # CatBoost with raw issuer (not grouped) — it handles high cardinality natively
    cat_raw_issuer = [c if c != 'issuer_bank_grouped' else 'issuer_bank' for c in BASE_CAT]
    run("A4: CatBoost + raw issuer_bank",         'cb_native', cat_raw_issuer, BASE_NUM)

    # CatBoost with cc_first_6 as native categorical (replaces manual BIN features?)
    cat_with_bin = BASE_CAT + ['cc_first_6']
    run("A5: CatBoost + cc_first_6 as cat",       'cb_native', cat_with_bin, BASE_NUM)

    # CatBoost with raw issuer + cc_first_6
    cat_raw_bin = cat_raw_issuer + ['cc_first_6']
    run("A6: CatBoost + raw issuer + BIN",        'cb_native', cat_raw_bin, BASE_NUM)

    # CatBoost minimal: BIN + raw issuer, NO manual BIN features (can CatBoost learn them?)
    num_no_bin = [n for n in BASE_NUM if n not in (
        'bin_approval_rate', 'bin_proc_approval_rate', 'bin_approval_7d',
        'bin_approval_30d', 'amount_vs_bin_avg'
    )]
    run("A7: CatBoost + BIN cat, NO manual BIN feats", 'cb_native', cat_raw_bin, num_no_bin)

    for name, auc, ca, el in results:
        print_result(name, auc, ca, el, baseline)

    # ════════════════════════════════════════════════════════════
    # PHASE 2: Feature Additions (one at a time)
    # Test on both LightGBM and CatBoost native
    # ════════════════════════════════════════════════════════════
    print("\n" + "─" * 90)
    print("PHASE 2: FEATURE ADDITIONS (one at a time)")
    print("─" * 90)

    new_features = {
        # New numerical features
        'mid_velocity_weekly':  ('num', 'mid_velocity_weekly'),
        'order_total':          ('num', 'order_total'),
        'day_of_month':         ('num', 'day_of_month'),
        'is_weekend':           ('num', 'is_weekend'),
        'month':                ('num', 'month'),
        'te_processor':         ('num', 'te_processor'),
        'te_billing_state':     ('num', 'te_billing_state'),
        'te_card_brand':        ('num', 'te_card_brand'),
        'proc_issuer_rate':     ('num', 'proc_issuer_rate'),
        'proc_cardtype_rate':   ('num', 'proc_cardtype_rate'),
        'proc_cardbrand_rate':  ('num', 'proc_cardbrand_rate'),
        'issuer_cardtype_rate': ('num', 'issuer_cardtype_rate'),
        'bin_distinct_procs':   ('num', 'bin_distinct_procs'),
        'bin_volume':           ('num', 'bin_volume'),
        'initial_was_payfac':   ('num', 'initial_was_payfac'),
        # New categorical features
        'card_level':           ('cat', 'card_level'),
        'bin_prefix_4':         ('cat', 'bin_prefix_4'),
        'offer_name':           ('cat', 'offer_name'),
        'hour_bucket':          ('cat', 'hour_bucket'),
    }

    phase2_start = len(results)
    for feat_name, (feat_type, col_name) in new_features.items():
        if feat_type == 'num':
            cat, num = BASE_CAT, BASE_NUM + [col_name]
        else:
            cat, num = BASE_CAT + [col_name], BASE_NUM

        run(f"F+{feat_name} (LGBM)",      'lgbm',      cat, num)
        run(f"F+{feat_name} (CatBoost)",   'cb_native', cat, num)

    print("\n  Feature addition results (sorted by AUC):")
    phase2 = results[phase2_start:]
    for name, auc, ca, el in sorted(phase2, key=lambda x: -x[1])[:20]:
        print_result(name, auc, ca, el, baseline)

    # ════════════════════════════════════════════════════════════
    # PHASE 3: Feature Ablation (remove one at a time from baseline)
    # ════════════════════════════════════════════════════════════
    print("\n" + "─" * 90)
    print("PHASE 3: FEATURE ABLATION (remove one at a time)")
    print("─" * 90)

    phase3_start = len(results)

    for col in BASE_CAT:
        cat = [c for c in BASE_CAT if c != col]
        run(f"R-{col} (LGBM)",      'lgbm',      cat, BASE_NUM)
        run(f"R-{col} (CatBoost)",   'cb_native', cat, BASE_NUM)

    for col in BASE_NUM:
        num = [c for c in BASE_NUM if c != col]
        run(f"R-{col} (LGBM)",      'lgbm',      BASE_CAT, num)
        run(f"R-{col} (CatBoost)",   'cb_native', BASE_CAT, num)

    print("\n  Ablation results — features that HURT when present (removal improves AUC):")
    phase3 = results[phase3_start:]
    improvements = [(n,a,ca,el) for n,a,ca,el in phase3 if a > baseline]
    for name, auc, ca, el in sorted(improvements, key=lambda x: -x[1]):
        print_result(name, auc, ca, el, baseline)

    if not improvements:
        print("  (none — all features contribute positively)")

    print("\n  Ablation results — features that HELP most (removal hurts AUC most):")
    for name, auc, ca, el in sorted(phase3, key=lambda x: x[1])[:10]:
        print_result(name, auc, ca, el, baseline)

    # ════════════════════════════════════════════════════════════
    # PHASE 4: Hyperparameter Sweep
    # Run on both LightGBM and CatBoost with baseline features
    # ════════════════════════════════════════════════════════════
    print("\n" + "─" * 90)
    print("PHASE 4: HYPERPARAMETER SWEEP")
    print("─" * 90)

    phase4_start = len(results)

    # LightGBM sweeps
    lgbm_hparams = [
        ("depth=5",               {'max_depth': 5}),
        ("depth=6",               {'max_depth': 6}),
        ("depth=10",              {'max_depth': 10}),
        ("depth=12",              {'max_depth': 12}),
        ("depth=-1 (unlimited)",  {'max_depth': -1}),
        ("trees=500 lr=0.05",    {'n_estimators': 500, 'learning_rate': 0.05}),
        ("trees=800 lr=0.03",    {'n_estimators': 800, 'learning_rate': 0.03}),
        ("trees=1000 lr=0.01",   {'n_estimators': 1000, 'learning_rate': 0.01}),
        ("leaves=63",             {'num_leaves': 63}),
        ("leaves=127",            {'num_leaves': 127}),
        ("leaves=255",            {'num_leaves': 255}),
        ("min_child=5",           {'min_child_samples': 5}),
        ("min_child=50",          {'min_child_samples': 50}),
        ("subsample=0.6",         {'subsample': 0.6}),
        ("colsample=0.6",         {'colsample_bytree': 0.6}),
        ("reg_alpha=0.1",         {'reg_alpha': 0.1}),
        ("reg_lambda=1.0",        {'reg_lambda': 1.0}),
        ("reg_alpha=0.1 lambda=1", {'reg_alpha': 0.1, 'reg_lambda': 1.0}),
    ]
    for label, hp in lgbm_hparams:
        run(f"H: LGBM {label}", 'lgbm', BASE_CAT, BASE_NUM, hp)

    # CatBoost sweeps
    cb_hparams = [
        ("depth=5",               {'depth': 5}),
        ("depth=6",               {'depth': 6}),
        ("depth=10",              {'depth': 10}),
        ("trees=500 lr=0.05",    {'iterations': 500, 'learning_rate': 0.05}),
        ("trees=800 lr=0.03",    {'iterations': 800, 'learning_rate': 0.03}),
        ("trees=1000 lr=0.01",   {'iterations': 1000, 'learning_rate': 0.01}),
        ("l2_reg=1",              {'l2_leaf_reg': 1}),
        ("l2_reg=5",              {'l2_leaf_reg': 5}),
        ("l2_reg=10",             {'l2_leaf_reg': 10}),
    ]
    for label, hp in cb_hparams:
        run(f"H: CatBoost {label}", 'cb_native', BASE_CAT, BASE_NUM, hp)

    print("\n  Top 10 hyperparameter results:")
    phase4 = results[phase4_start:]
    for name, auc, ca, el in sorted(phase4, key=lambda x: -x[1])[:10]:
        print_result(name, auc, ca, el, baseline)

    # ════════════════════════════════════════════════════════════
    # PHASE 5: Best Combination
    # Combine winning features + algo + hyperparams
    # ════════════════════════════════════════════════════════════
    print("\n" + "─" * 90)
    print("PHASE 5: BEST COMBINATIONS")
    print("─" * 90)

    # Find best additions from phase 2
    phase2_lgbm = [(n,a) for n,a,_,_ in results[phase2_start:phase3_start] if 'LGBM' in n and a > baseline]
    phase2_cb   = [(n,a) for n,a,_,_ in results[phase2_start:phase3_start] if 'CatBoost' in n and a > baseline]

    print(f"\n  Features that improved LGBM: {len(phase2_lgbm)}")
    for n, a in sorted(phase2_lgbm, key=lambda x: -x[1]):
        feat = n.split('F+')[1].split(' (')[0]
        print(f"    {feat}: {a:.4f} ({a-baseline:+.4f})")

    print(f"\n  Features that improved CatBoost: {len(phase2_cb)}")
    cb_base = next((a for n,a,_,_ in results if n == "A2: CatBoost native"), baseline)
    for n, a in sorted(phase2_cb, key=lambda x: -x[1]):
        feat = n.split('F+')[1].split(' (')[0]
        print(f"    {feat}: {a:.4f} ({a-cb_base:+.4f})")

    # Combo 1: LGBM + all improving features
    improving_lgbm_feats = []
    for n, a in phase2_lgbm:
        feat = n.split('F+')[1].split(' (')[0]
        improving_lgbm_feats.append(feat)

    if improving_lgbm_feats:
        combo_cat = list(BASE_CAT)
        combo_num = list(BASE_NUM)
        for feat in improving_lgbm_feats:
            info = new_features.get(feat)
            if info:
                ftype, col = info
                if ftype == 'num' and col not in combo_num: combo_num.append(col)
                if ftype == 'cat' and col not in combo_cat: combo_cat.append(col)

        run("COMBO: LGBM + all improving feats", 'lgbm', combo_cat, combo_num)

    # Combo 2: CatBoost + all improving features
    improving_cb_feats = []
    for n, a in phase2_cb:
        feat = n.split('F+')[1].split(' (')[0]
        improving_cb_feats.append(feat)

    if improving_cb_feats:
        combo_cat = list(BASE_CAT)
        combo_num = list(BASE_NUM)
        for feat in improving_cb_feats:
            info = new_features.get(feat)
            if info:
                ftype, col = info
                if ftype == 'num' and col not in combo_num: combo_num.append(col)
                if ftype == 'cat' and col not in combo_cat: combo_cat.append(col)

        run("COMBO: CatBoost + all improving feats", 'cb_native', combo_cat, combo_num)

    # Combo 3: CatBoost + raw issuer + cc_first_6 + improving features
    if improving_cb_feats:
        combo_cat = [c if c != 'issuer_bank_grouped' else 'issuer_bank' for c in BASE_CAT]
        combo_cat.append('cc_first_6')
        combo_num = list(BASE_NUM)
        for feat in improving_cb_feats:
            info = new_features.get(feat)
            if info:
                ftype, col = info
                if ftype == 'num' and col not in combo_num: combo_num.append(col)
                if ftype == 'cat' and col not in combo_cat: combo_cat.append(col)

        run("COMBO: CatBoost + raw issuer + BIN + improving feats", 'cb_native', combo_cat, combo_num)

    # Combo 4: Best algo + best features + best hyperparams
    # Find best HP for each algo
    best_lgbm_hp = max(
        [(n,a,el) for n,a,_,el in results[phase4_start:phase4_start+len(lgbm_hparams)]],
        key=lambda x: x[1]
    )
    best_cb_hp = max(
        [(n,a,el) for n,a,_,el in results[phase4_start+len(lgbm_hparams):phase4_start+len(lgbm_hparams)+len(cb_hparams)]],
        key=lambda x: x[1]
    )

    print(f"\n  Best LGBM HP: {best_lgbm_hp[0]} = {best_lgbm_hp[1]:.4f}")
    print(f"  Best CatBoost HP: {best_cb_hp[0]} = {best_cb_hp[1]:.4f}")

    # Extract the best HP dict
    best_lgbm_hp_name = best_lgbm_hp[0].replace("H: LGBM ", "")
    best_cb_hp_name = best_cb_hp[0].replace("H: CatBoost ", "")

    best_lgbm_hp_dict = dict(next(hp for label, hp in lgbm_hparams if label == best_lgbm_hp_name))
    best_cb_hp_dict = dict(next(hp for label, hp in cb_hparams if label == best_cb_hp_name))

    # LGBM: best features + best HP
    if improving_lgbm_feats:
        combo_cat = list(BASE_CAT)
        combo_num = list(BASE_NUM)
        for feat in improving_lgbm_feats:
            info = new_features.get(feat)
            if info:
                ftype, col = info
                if ftype == 'num' and col not in combo_num: combo_num.append(col)
                if ftype == 'cat' and col not in combo_cat: combo_cat.append(col)
        run("COMBO: LGBM + best feats + best HP", 'lgbm', combo_cat, combo_num, best_lgbm_hp_dict)

    # CatBoost: raw issuer + BIN + best features + best HP
    if improving_cb_feats:
        combo_cat = [c if c != 'issuer_bank_grouped' else 'issuer_bank' for c in BASE_CAT]
        combo_cat.append('cc_first_6')
        combo_num = list(BASE_NUM)
        for feat in improving_cb_feats:
            info = new_features.get(feat)
            if info:
                ftype, col = info
                if ftype == 'num' and col not in combo_num: combo_num.append(col)
                if ftype == 'cat' and col not in combo_cat: combo_cat.append(col)
        run("COMBO: CatBoost + raw + BIN + best feats + best HP", 'cb_native', combo_cat, combo_num, best_cb_hp_dict)

    # ════════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("FINAL RESULTS — ALL EXPERIMENTS SORTED BY AUC")
    print("=" * 90)

    for rank, (name, auc, ca, el) in enumerate(sorted(results, key=lambda x: -x[1]), 1):
        diff = auc - baseline
        clients = "  ".join([f"C{k}:{v:.3f}" for k, v in ca.items()])
        marker = " ★" if rank <= 3 else ""
        print(f"  {rank:3d}. {name:<50} {auc:.4f}  ({diff:+.4f}){marker}  {clients}")

    print(f"\n  Total experiments: {len(results)}")
    print(f"  Baseline (LGBM current): {baseline:.4f}")
    best_name, best_auc, best_ca, _ = max(results, key=lambda x: x[1])
    print(f"  Best: {best_name} = {best_auc:.4f} ({best_auc - baseline:+.4f})")
    print(f"  Per-client: {dict(best_ca)}")


if __name__ == '__main__':
    main()
