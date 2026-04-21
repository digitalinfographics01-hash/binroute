"""
BinRoute AI — Three Rebill Model Comparison + 4-Strategy Backtest

Trains 3 rebill models and runs the 4-strategy routing backtest on each:
  Model A: Current (CatBoost with processor names — baseline 0.96 AUC)
  Model B: Rate-only (no processor names, uses lookup rates + is_same booleans)
  Model C: Blended (both processor names + rate features)

Strategies per model:
  1. Historical (what actually happened)
  2. Lookup table only
  3. AI model only
  4. Lookup-enhanced AI

Goal: find which model architecture makes better ROUTING decisions
(not just better AUC — we want the model that picks the right processor).

Usage: python3 -u scripts/ml/compare_rebill_models.py [--db=PATH]
"""

import os, sys, time, sqlite3, warnings, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import numpy as np
import pandas as pd
from collections import defaultdict
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score
from catboost import CatBoostClassifier
import lightgbm as lgb

warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
for arg in sys.argv:
    if arg.startswith('--db='):
        DB_PATH = arg.split('=', 1)[1]

TRAIN_RATIO = 0.80
LOOKUP_MIN_SAMPLES = 30
LOOKUP_FALLBACK_MIN = 15


# ════════════════════════════════════════════════════════════════
# Data Loading
# ════════════════════════════════════════════════════════════════

def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            ta.client_id, ta.order_id, ta.gateway_id,
            ta.processor_name, ta.acquiring_bank, ta.mcc_code,
            ta.outcome, ta.decline_reason,
            ta.cc_first_6, ta.order_total, ta.acquisition_date,
            ta.derived_product_role, ta.model_target,
            ta.derived_cycle,
            ta.offer_name, ta.billing_state,
            ta.issuer_bank, ta.card_brand, ta.card_type, ta.is_prepaid,
            ta.hour_of_day, ta.day_of_week, ta.mid_age_days,
            ta.mid_velocity_daily, ta.mid_velocity_weekly,
            ta.customer_history_on_proc, ta.bin_velocity_weekly,
            ta.initial_processor, ta.last_approved_processor,
            ta.consecutive_approvals, ta.days_since_last_charge,
            ta.days_since_initial, ta.lifetime_charges, ta.lifetime_revenue,
            ta.initial_amount, ta.amount_ratio, ta.initial_was_payfac,
            bl.card_level
        FROM transaction_attempts ta
        LEFT JOIN bin_lookup bl ON ta.cc_first_6 = bl.bin
        WHERE ta.feature_version >= 3
          AND ta.model_target = 'rebill'
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
    print(f"Loaded {len(df):,} rebill main attempts")
    print(f"Approval rate: {df['label'].mean():.1%}")
    print(f"Clients: {sorted(df['client_id'].unique())}")
    return df


# ════════════════════════════════════════════════════════════════
# Lookup Table Builder
# ════════════════════════════════════════════════════════════════

def build_lookup_table(df_train):
    """Build tiered lookup: issuer × card_type × initial_proc × target."""
    df = df_train.copy()
    df['issuer'] = df['issuer_bank'].fillna('UNKNOWN')
    df['ctype'] = df['card_type'].fillna('UNKNOWN')
    df['init_proc'] = df['initial_processor'].fillna('UNKNOWN')

    # Tier 1: 4D — issuer × card_type × initial_processor × target
    tier1 = {}
    grp1 = df.groupby(['client_id', 'issuer', 'ctype', 'init_proc', 'processor_name'])['label'].agg(['sum', 'count']).reset_index()
    for _, row in grp1.iterrows():
        if row['count'] >= LOOKUP_MIN_SAMPLES:
            key = (row['client_id'], row['issuer'], row['ctype'], row['init_proc'], row['processor_name'])
            tier1[key] = {'rate': row['sum'] / row['count'], 'count': int(row['count'])}

    # Tier 2: 3D — issuer × card_type × target (drop initial_processor)
    tier2 = {}
    grp2 = df.groupby(['client_id', 'issuer', 'ctype', 'processor_name'])['label'].agg(['sum', 'count']).reset_index()
    for _, row in grp2.iterrows():
        if row['count'] >= LOOKUP_FALLBACK_MIN:
            key = (row['client_id'], row['issuer'], row['ctype'], row['processor_name'])
            tier2[key] = {'rate': row['sum'] / row['count'], 'count': int(row['count'])}

    # Tier 3: 2D — issuer × target
    tier3 = {}
    grp3 = df.groupby(['client_id', 'issuer', 'processor_name'])['label'].agg(['sum', 'count']).reset_index()
    for _, row in grp3.iterrows():
        if row['count'] >= LOOKUP_FALLBACK_MIN:
            key = (row['client_id'], row['issuer'], row['processor_name'])
            tier3[key] = {'rate': row['sum'] / row['count'], 'count': int(row['count'])}

    # Available processors per client
    client_procs = {}
    for cid in df['client_id'].unique():
        client_procs[cid] = sorted(df[df['client_id'] == cid]['processor_name'].unique())

    print(f"  Lookup: {len(tier1):,} tier1 (4D), {len(tier2):,} tier2 (3D), {len(tier3):,} tier3 (2D)")
    return tier1, tier2, tier3, client_procs


def lookup_best_processor(client_id, issuer, card_type, init_proc, tier1, tier2, tier3, client_procs):
    """Find best processor for this combo. Returns (best_proc, best_rate, all_rates, tier_used)."""
    procs = client_procs.get(client_id, [])

    # Try 4D
    proc_rates = {}
    for proc in procs:
        key = (client_id, issuer, card_type, init_proc, proc)
        if key in tier1:
            proc_rates[proc] = tier1[key]
    if len(proc_rates) >= 2:
        best = max(proc_rates, key=lambda p: proc_rates[p]['rate'])
        return best, proc_rates[best]['rate'], proc_rates, 1

    # Try 3D
    proc_rates = {}
    for proc in procs:
        key = (client_id, issuer, card_type, proc)
        if key in tier2:
            proc_rates[proc] = tier2[key]
    if len(proc_rates) >= 2:
        best = max(proc_rates, key=lambda p: proc_rates[p]['rate'])
        return best, proc_rates[best]['rate'], proc_rates, 2

    # Try 2D
    proc_rates = {}
    for proc in procs:
        key = (client_id, issuer, proc)
        if key in tier3:
            proc_rates[proc] = tier3[key]
    if len(proc_rates) >= 2:
        best = max(proc_rates, key=lambda p: proc_rates[p]['rate'])
        return best, proc_rates[best]['rate'], proc_rates, 3

    return None, None, {}, None


# ════════════════════════════════════════════════════════════════
# Rate Feature Computation (for Models B & C)
# ════════════════════════════════════════════════════════════════

def compute_rate_features(df, tier1, tier2, tier3, client_procs):
    """Add processor-rate features for each row: target proc rate, initial proc rate,
    last proc rate, is_same booleans, rate_vs_best."""
    n = len(df)
    target_proc_rate = np.full(n, np.nan)
    initial_proc_rate = np.full(n, np.nan)
    last_proc_rate = np.full(n, np.nan)
    best_proc_rate = np.full(n, np.nan)
    is_same_initial = np.zeros(n, dtype=int)
    is_same_last = np.zeros(n, dtype=int)
    rate_vs_best = np.zeros(n)
    rate_vs_initial = np.zeros(n)
    lookup_has_data = np.zeros(n, dtype=int)

    issuers = df['issuer_bank'].fillna('UNKNOWN').values
    ctypes = df['card_type'].fillna('UNKNOWN').values
    procs = df['processor_name'].values
    init_procs = df['initial_processor'].fillna('UNKNOWN').values
    last_procs = df['last_approved_processor'].fillna('UNKNOWN').values
    clients = df['client_id'].values

    for i in range(n):
        cid = clients[i]
        issuer = issuers[i]
        ct = ctypes[i]
        ip = init_procs[i]
        lp = last_procs[i]
        tp = procs[i]

        # Is same booleans
        is_same_initial[i] = 1 if tp == ip else 0
        is_same_last[i] = 1 if tp == lp else 0

        # Get all processor rates for this combo
        _, br, all_rates, tier = lookup_best_processor(cid, issuer, ct, ip, tier1, tier2, tier3, client_procs)

        if all_rates:
            lookup_has_data[i] = 1
            if br is not None:
                best_proc_rate[i] = br

            # Target processor rate
            if tp in all_rates:
                target_proc_rate[i] = all_rates[tp]['rate']
            # Initial processor rate
            if ip in all_rates:
                initial_proc_rate[i] = all_rates[ip]['rate']
            # Last approved processor rate
            if lp in all_rates:
                last_proc_rate[i] = all_rates[lp]['rate']

            # Relative features
            if not np.isnan(best_proc_rate[i]) and not np.isnan(target_proc_rate[i]):
                rate_vs_best[i] = target_proc_rate[i] - best_proc_rate[i]
            if not np.isnan(initial_proc_rate[i]) and not np.isnan(target_proc_rate[i]):
                rate_vs_initial[i] = target_proc_rate[i] - initial_proc_rate[i]

    global_rate = df['label'].mean()
    df['target_proc_rebill_rate'] = pd.Series(target_proc_rate, index=df.index).fillna(global_rate)
    df['initial_proc_rebill_rate'] = pd.Series(initial_proc_rate, index=df.index).fillna(global_rate)
    df['last_proc_rebill_rate'] = pd.Series(last_proc_rate, index=df.index).fillna(global_rate)
    df['best_proc_rebill_rate'] = pd.Series(best_proc_rate, index=df.index).fillna(global_rate)
    df['is_same_as_initial'] = is_same_initial
    df['is_same_as_last'] = is_same_last
    df['rate_vs_best'] = rate_vs_best
    df['rate_vs_initial'] = rate_vs_initial
    df['lookup_has_data'] = lookup_has_data

    covered = (lookup_has_data > 0).sum()
    print(f"  Rate features: {covered:,}/{n:,} ({covered/n:.1%}) have lookup data")
    return df


# ════════════════════════════════════════════════════════════════
# Model Definitions
# ════════════════════════════════════════════════════════════════

# Shared base features (non-processor)
BASE_CATEGORICAL = [
    'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'offer_name', 'billing_state', 'card_level',
]

BASE_NUMERICAL = [
    'is_prepaid', 'order_total', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'consecutive_approvals', 'days_since_last_charge',
    'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
    'initial_amount', 'amount_ratio', 'initial_was_payfac',
]

# Model A: Current — CatBoost with processor names
MODEL_A_CAT = BASE_CATEGORICAL + [
    'processor_name', 'initial_processor', 'last_approved_processor',
]
MODEL_A_NUM = BASE_NUMERICAL.copy()

# Model B: Rate-only — NO processor names, uses rate features instead
MODEL_B_CAT = BASE_CATEGORICAL + [
    'processor_name',  # still need target processor for scoring
]
MODEL_B_NUM = BASE_NUMERICAL + [
    'target_proc_rebill_rate', 'initial_proc_rebill_rate', 'last_proc_rebill_rate',
    'is_same_as_initial', 'is_same_as_last',
]

# Model C: Blended — processor names + rate features
MODEL_C_CAT = BASE_CATEGORICAL + [
    'processor_name', 'initial_processor', 'last_approved_processor',
]
MODEL_C_NUM = BASE_NUMERICAL + [
    'target_proc_rebill_rate', 'initial_proc_rebill_rate', 'last_proc_rebill_rate',
    'best_proc_rebill_rate',
    'is_same_as_initial', 'is_same_as_last',
    'rate_vs_best', 'rate_vs_initial',
]


# ════════════════════════════════════════════════════════════════
# Training
# ════════════════════════════════════════════════════════════════

def train_catboost(df_train, df_test, cat_cols, num_cols, label="Model"):
    """Train CatBoost with native categoricals. Returns model, feature_cols, test_probs."""
    feature_cols = cat_cols + num_cols
    cat_indices = list(range(len(cat_cols)))

    df_tr = df_train.copy()
    df_te = df_test.copy()

    for col in cat_cols:
        df_tr[col] = df_tr[col].fillna('UNKNOWN').replace('nan', 'UNKNOWN').astype(str)
        df_te[col] = df_te[col].fillna('UNKNOWN').replace('nan', 'UNKNOWN').astype(str)
    for col in num_cols:
        df_tr[col] = pd.to_numeric(df_tr[col], errors='coerce').fillna(0)
        df_te[col] = pd.to_numeric(df_te[col], errors='coerce').fillna(0)

    model = CatBoostClassifier(
        iterations=300, depth=8, learning_rate=0.1,
        auto_class_weights='Balanced',
        cat_features=cat_indices,
        verbose=0, random_seed=42,
    )
    model.fit(df_tr[feature_cols], df_tr['label'].values)

    probs = model.predict_proba(df_te[feature_cols])[:, 1]
    auc = roc_auc_score(df_te['label'].values, probs)
    print(f"  {label} AUC: {auc:.4f}")

    # Per-client AUC
    for cid in sorted(df_te['client_id'].unique()):
        mask = df_te['client_id'].values == cid
        if mask.sum() >= 50 and len(set(df_te['label'].values[mask])) >= 2:
            c_auc = roc_auc_score(df_te['label'].values[mask], probs[mask])
            print(f"    Client {cid}: {c_auc:.4f} ({mask.sum():,} rows)")

    # Feature importance
    importances = model.get_feature_importance()
    total_imp = importances.sum()
    pairs = sorted(zip(feature_cols, importances), key=lambda x: -x[1])
    print(f"  Top 10 features:")
    for fname, imp in pairs[:10]:
        print(f"    {fname:<30} {imp/total_imp*100:>5.1f}%")

    return model, feature_cols, cat_indices, probs, auc


def score_catboost(model, feature_cols, cat_cols, num_cols, row_dict, proc_name, proc_info, rate_info=None):
    """Score a single row on a specific processor. Returns probability."""
    row = dict(row_dict)
    row['processor_name'] = proc_name
    row['acquiring_bank'] = proc_info.get('acquiring_bank', row.get('acquiring_bank', 'UNKNOWN'))
    row['mcc_code'] = proc_info.get('mcc_code', row.get('mcc_code', 'UNKNOWN'))

    # Update rate features if provided
    if rate_info is not None:
        for k, v in rate_info.items():
            row[k] = v

    vals = []
    for col in feature_cols:
        if col in cat_cols:
            vals.append(str(row.get(col, 'UNKNOWN') or 'UNKNOWN'))
        else:
            v = row.get(col, 0)
            try:
                vals.append(float(v) if v is not None else 0.0)
            except (ValueError, TypeError):
                vals.append(0.0)

    df_row = pd.DataFrame([vals], columns=feature_cols)
    for col in cat_cols:
        df_row[col] = df_row[col].astype(str)
    for col in num_cols:
        df_row[col] = pd.to_numeric(df_row[col], errors='coerce').fillna(0)

    return model.predict_proba(df_row)[0, 1]


# ════════════════════════════════════════════════════════════════
# Backtest Engine
# ════════════════════════════════════════════════════════════════

def run_backtest(model, feature_cols, cat_indices, cat_cols, num_cols,
                 df_test, test_probs, tier1, tier2, tier3, client_procs,
                 proc_info, model_name, needs_rate_features=False):
    """Run 4-strategy backtest for a given model. Returns results dict."""
    print(f"\n{'═'*80}")
    print(f"  BACKTEST: {model_name}")
    print(f"{'═'*80}")

    n_test = len(df_test)
    test = df_test.reset_index(drop=True)

    # ── Strategy 1: Historical ──
    actual_approvals = test['label'].sum()
    actual_rate = test['label'].mean()
    print(f"\n  S1 HISTORICAL: {actual_rate:.2%} ({actual_approvals:,}/{n_test:,})")

    # ── Pre-compute lookup recommendations ──
    lookup_recs = []
    for _, row in test.iterrows():
        issuer = row['issuer_bank'] if pd.notna(row['issuer_bank']) else 'UNKNOWN'
        ctype = row['card_type'] if pd.notna(row['card_type']) else 'UNKNOWN'
        init_proc = row['initial_processor'] if pd.notna(row['initial_processor']) else 'UNKNOWN'
        cid = row['client_id']
        actual_proc = row['processor_name']

        bp, br, all_rates, tier = lookup_best_processor(
            cid, issuer, ctype, init_proc, tier1, tier2, tier3, client_procs
        )
        actual_lu_rate = all_rates.get(actual_proc, {}).get('rate', None) if all_rates else None

        lookup_recs.append({
            'best_proc': bp, 'best_rate': br,
            'actual_proc': actual_proc, 'actual_rate': actual_lu_rate,
            'all_rates': all_rates, 'tier': tier,
            'actual_outcome': row['label'],
            'would_reroute': bp is not None and bp != actual_proc,
        })
    lookup_df = pd.DataFrame(lookup_recs)

    # ── Strategy 2: Lookup only ──
    has_lookup = lookup_df['best_proc'].notna()
    would_reroute = lookup_df['would_reroute']
    covered = has_lookup.sum()
    rerouted = would_reroute.sum()

    stayed_approvals = lookup_df.loc[has_lookup & ~would_reroute, 'actual_outcome'].sum()
    no_data_approvals = lookup_df.loc[~has_lookup, 'actual_outcome'].sum()
    reroute_rows = lookup_df.loc[would_reroute]
    rerouted_expected = reroute_rows['best_rate'].sum() if len(reroute_rows) > 0 else 0

    lookup_total = stayed_approvals + no_data_approvals + rerouted_expected
    lookup_rate = lookup_total / n_test

    reroute_actual = reroute_rows['actual_outcome'].mean() if len(reroute_rows) > 0 else 0
    print(f"  S2 LOOKUP:     {lookup_rate:.2%} ({lookup_rate - actual_rate:+.2%}) "
          f"| coverage {covered/n_test:.0%}, rerouted {rerouted:,}, "
          f"rerouted actual {reroute_actual:.1%}")

    # Validation gap
    stayed_rows = lookup_df.loc[has_lookup & ~would_reroute]
    if len(stayed_rows) > 0:
        stayed_actual = stayed_rows['actual_outcome'].mean()
        stayed_predicted = stayed_rows['actual_rate'].dropna().mean()
        val_gap = abs(stayed_actual - stayed_predicted)
        print(f"         validation: stayed actual {stayed_actual:.2%} vs predicted {stayed_predicted:.2%} (gap {val_gap:.2%})")

    # ── Strategy 3: AI Only ──
    print(f"  S3 AI ONLY:    scoring {n_test:,} transactions...", end="", flush=True)
    t0 = time.time()

    ai_best_procs = []
    ai_best_probs = []

    for i in range(n_test):
        row = test.iloc[i]
        cid = row['client_id']
        actual_proc = row['processor_name']
        procs = client_procs.get(cid, [actual_proc])

        best_proc = actual_proc
        best_prob = test_probs[i]

        issuer = row['issuer_bank'] if pd.notna(row['issuer_bank']) else 'UNKNOWN'
        ctype = row['card_type'] if pd.notna(row['card_type']) else 'UNKNOWN'
        init_proc_val = row['initial_processor'] if pd.notna(row['initial_processor']) else 'UNKNOWN'
        last_proc_val = row['last_approved_processor'] if pd.notna(row['last_approved_processor']) else 'UNKNOWN'

        # Get all rates for rate feature computation
        _, _, all_rates, _ = lookup_best_processor(
            cid, issuer, ctype, init_proc_val, tier1, tier2, tier3, client_procs
        )

        for proc in procs:
            if proc == actual_proc:
                continue
            pi = proc_info.get((cid, proc))
            if pi is None:
                continue

            rate_info = None
            if needs_rate_features and all_rates:
                # Compute rate features for this alternative processor
                t_rate = all_rates.get(proc, {}).get('rate', test['label'].mean())
                i_rate = all_rates.get(init_proc_val, {}).get('rate', test['label'].mean())
                l_rate = all_rates.get(last_proc_val, {}).get('rate', test['label'].mean())
                rates_list = [v['rate'] for v in all_rates.values()]
                b_rate = max(rates_list) if rates_list else test['label'].mean()

                rate_info = {
                    'target_proc_rebill_rate': t_rate if isinstance(t_rate, (int, float)) else test['label'].mean(),
                    'initial_proc_rebill_rate': i_rate if isinstance(i_rate, (int, float)) else test['label'].mean(),
                    'last_proc_rebill_rate': l_rate if isinstance(l_rate, (int, float)) else test['label'].mean(),
                    'best_proc_rebill_rate': b_rate,
                    'is_same_as_initial': 1 if proc == init_proc_val else 0,
                    'is_same_as_last': 1 if proc == last_proc_val else 0,
                    'rate_vs_best': (t_rate - b_rate) if isinstance(t_rate, (int, float)) and isinstance(b_rate, (int, float)) else 0,
                    'rate_vs_initial': (t_rate - i_rate) if isinstance(t_rate, (int, float)) and isinstance(i_rate, (int, float)) else 0,
                }

            p = score_catboost(model, feature_cols, cat_cols, num_cols,
                               row.to_dict(), proc, pi, rate_info)
            if p > best_prob:
                best_prob = p
                best_proc = proc

        ai_best_procs.append(best_proc)
        ai_best_probs.append(best_prob)

        if (i+1) % 2000 == 0:
            elapsed = time.time() - t0
            rate_per_sec = (i+1) / elapsed
            eta = (n_test - i - 1) / rate_per_sec
            print(f"\r  S3 AI ONLY:    {i+1:,}/{n_test:,} ({rate_per_sec:.0f}/s, ETA {eta:.0f}s)   ", end="", flush=True)

    elapsed = time.time() - t0
    print(f"\r  S3 AI ONLY:    done in {elapsed:.0f}s                                    ")

    test['ai_best_proc'] = ai_best_procs
    test['ai_best_prob'] = ai_best_probs
    ai_would_reroute = test['ai_best_proc'] != test['processor_name']

    ai_rerouted_count = ai_would_reroute.sum()
    ai_stayed_approvals = test.loc[~ai_would_reroute, 'label'].sum()
    ai_rerouted_rows = test.loc[ai_would_reroute]

    # Estimate rerouted outcomes using lookup rates
    ai_rerouted_expected = 0
    ai_no_lookup = 0
    for _, row in ai_rerouted_rows.iterrows():
        issuer = row['issuer_bank'] if pd.notna(row['issuer_bank']) else 'UNKNOWN'
        ctype = row['card_type'] if pd.notna(row['card_type']) else 'UNKNOWN'
        init_proc_val = row['initial_processor'] if pd.notna(row['initial_processor']) else 'UNKNOWN'
        cid = row['client_id']
        rec_proc = row['ai_best_proc']

        key1 = (cid, issuer, ctype, init_proc_val, rec_proc)
        key2 = (cid, issuer, ctype, rec_proc)
        key3 = (cid, issuer, rec_proc)

        if key1 in tier1:
            ai_rerouted_expected += tier1[key1]['rate']
        elif key2 in tier2:
            ai_rerouted_expected += tier2[key2]['rate']
        elif key3 in tier3:
            ai_rerouted_expected += tier3[key3]['rate']
        else:
            ai_rerouted_expected += row['ai_best_prob']
            ai_no_lookup += 1

    ai_total = ai_stayed_approvals + ai_rerouted_expected
    ai_rate = ai_total / n_test
    ai_reroute_actual = ai_rerouted_rows['label'].mean() if len(ai_rerouted_rows) > 0 else 0

    print(f"             {ai_rate:.2%} ({ai_rate - actual_rate:+.2%}) "
          f"| rerouted {ai_rerouted_count:,} ({ai_rerouted_count/n_test:.0%}), "
          f"rerouted actual {ai_reroute_actual:.1%}, no-lookup {ai_no_lookup}")

    # ── Strategy 4: Lookup+AI ──
    # Same as AI but the model was trained WITH lookup features (Model C does this natively)
    # For all models, we use the same scoring but provide lookup context
    print(f"  S4 LOOKUP+AI:  scoring {n_test:,} transactions...", end="", flush=True)
    t0 = time.time()

    enh_best_procs = []
    enh_best_probs = []

    for i in range(n_test):
        row = test.iloc[i]
        cid = row['client_id']
        actual_proc = row['processor_name']
        procs = client_procs.get(cid, [actual_proc])
        issuer = row['issuer_bank'] if pd.notna(row['issuer_bank']) else 'UNKNOWN'
        ctype = row['card_type'] if pd.notna(row['card_type']) else 'UNKNOWN'
        init_proc_val = row['initial_processor'] if pd.notna(row['initial_processor']) else 'UNKNOWN'
        last_proc_val = row['last_approved_processor'] if pd.notna(row['last_approved_processor']) else 'UNKNOWN'

        _, _, all_rates, _ = lookup_best_processor(
            cid, issuer, ctype, init_proc_val, tier1, tier2, tier3, client_procs
        )

        best_proc = actual_proc
        best_prob = test_probs[i]

        # If lookup says a different processor is better AND has good data,
        # use that as a strong prior
        lookup_best = None
        if all_rates:
            rates_list = sorted(all_rates.items(), key=lambda x: -x[1]['rate'])
            if rates_list:
                lookup_best = rates_list[0][0]

        for proc in procs:
            if proc == actual_proc:
                continue
            pi = proc_info.get((cid, proc))
            if pi is None:
                continue

            rate_info = None
            if needs_rate_features and all_rates:
                t_rate = all_rates.get(proc, {}).get('rate', test['label'].mean())
                i_rate = all_rates.get(init_proc_val, {}).get('rate', test['label'].mean())
                l_rate = all_rates.get(last_proc_val, {}).get('rate', test['label'].mean())
                rates_vals = [v['rate'] for v in all_rates.values()]
                b_rate = max(rates_vals) if rates_vals else test['label'].mean()

                rate_info = {
                    'target_proc_rebill_rate': t_rate if isinstance(t_rate, (int, float)) else test['label'].mean(),
                    'initial_proc_rebill_rate': i_rate if isinstance(i_rate, (int, float)) else test['label'].mean(),
                    'last_proc_rebill_rate': l_rate if isinstance(l_rate, (int, float)) else test['label'].mean(),
                    'best_proc_rebill_rate': b_rate,
                    'is_same_as_initial': 1 if proc == init_proc_val else 0,
                    'is_same_as_last': 1 if proc == last_proc_val else 0,
                    'rate_vs_best': (t_rate - b_rate) if isinstance(t_rate, (int, float)) and isinstance(b_rate, (int, float)) else 0,
                    'rate_vs_initial': (t_rate - i_rate) if isinstance(t_rate, (int, float)) and isinstance(i_rate, (int, float)) else 0,
                }

            p = score_catboost(model, feature_cols, cat_cols, num_cols,
                               row.to_dict(), proc, pi, rate_info)

            # Boost probability if lookup strongly agrees
            if all_rates and proc in all_rates and lookup_best == proc:
                lu_rate = all_rates[proc]['rate']
                lu_count = all_rates[proc]['count']
                if lu_count >= 50 and lu_rate > 0.15:
                    # Blend: 70% model + 30% lookup
                    p = 0.7 * p + 0.3 * lu_rate

            if p > best_prob:
                best_prob = p
                best_proc = proc

        enh_best_procs.append(best_proc)
        enh_best_probs.append(best_prob)

        if (i+1) % 2000 == 0:
            elapsed = time.time() - t0
            rate_per_sec = (i+1) / elapsed
            eta = (n_test - i - 1) / rate_per_sec
            print(f"\r  S4 LOOKUP+AI:  {i+1:,}/{n_test:,} ({rate_per_sec:.0f}/s, ETA {eta:.0f}s)   ", end="", flush=True)

    elapsed = time.time() - t0
    print(f"\r  S4 LOOKUP+AI:  done in {elapsed:.0f}s                                    ")

    test['enh_best_proc'] = enh_best_procs
    test['enh_best_prob'] = enh_best_probs
    enh_would_reroute = test['enh_best_proc'] != test['processor_name']

    enh_rerouted_count = enh_would_reroute.sum()
    enh_stayed_approvals = test.loc[~enh_would_reroute, 'label'].sum()
    enh_rerouted_rows = test.loc[enh_would_reroute]

    enh_rerouted_expected = 0
    for _, row in enh_rerouted_rows.iterrows():
        issuer = row['issuer_bank'] if pd.notna(row['issuer_bank']) else 'UNKNOWN'
        ctype = row['card_type'] if pd.notna(row['card_type']) else 'UNKNOWN'
        init_proc_val = row['initial_processor'] if pd.notna(row['initial_processor']) else 'UNKNOWN'
        cid = row['client_id']
        rec_proc = row['enh_best_proc']

        key1 = (cid, issuer, ctype, init_proc_val, rec_proc)
        key2 = (cid, issuer, ctype, rec_proc)
        key3 = (cid, issuer, rec_proc)

        if key1 in tier1:
            enh_rerouted_expected += tier1[key1]['rate']
        elif key2 in tier2:
            enh_rerouted_expected += tier2[key2]['rate']
        elif key3 in tier3:
            enh_rerouted_expected += tier3[key3]['rate']
        else:
            enh_rerouted_expected += row['enh_best_prob']

    enh_total = enh_stayed_approvals + enh_rerouted_expected
    enh_rate = enh_total / n_test
    enh_reroute_actual = enh_rerouted_rows['label'].mean() if len(enh_rerouted_rows) > 0 else 0

    print(f"             {enh_rate:.2%} ({enh_rate - actual_rate:+.2%}) "
          f"| rerouted {enh_rerouted_count:,} ({enh_rerouted_count/n_test:.0%}), "
          f"rerouted actual {enh_reroute_actual:.1%}")

    # ── Routing decision analysis ──
    print(f"\n  Routing decisions:")
    ai_switch_pct = ai_rerouted_count / n_test * 100
    enh_switch_pct = enh_rerouted_count / n_test * 100
    print(f"    AI would switch:       {ai_rerouted_count:,} ({ai_switch_pct:.1f}%)")
    print(f"    Lookup+AI would switch: {enh_rerouted_count:,} ({enh_switch_pct:.1f}%)")

    # Where AI switches, what's the actual approval on those rows?
    if ai_rerouted_count > 0:
        ai_switch_actual = ai_rerouted_rows['label'].mean()
        ai_stay_actual = test.loc[~ai_would_reroute, 'label'].mean()
        print(f"    AI switch rows actual approval: {ai_switch_actual:.1%} (stay: {ai_stay_actual:.1%})")

    return {
        'model': model_name,
        'historical': actual_rate,
        'lookup': lookup_rate,
        'ai_only': ai_rate,
        'lookup_ai': enh_rate,
        'ai_rerouted_pct': ai_rerouted_count / n_test,
        'enh_rerouted_pct': enh_rerouted_count / n_test,
        'ai_rerouted_actual_rate': ai_reroute_actual,
    }


# ════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════

def main():
    print("=" * 80)
    print("THREE REBILL MODEL COMPARISON + 4-STRATEGY BACKTEST")
    print("=" * 80)

    # ── Load data ──
    df = load_data()

    split_idx = int(len(df) * TRAIN_RATIO)
    df_train = df.iloc[:split_idx].copy()
    df_test = df.iloc[split_idx:].copy()
    print(f"\nSplit: {len(df_train):,} train / {len(df_test):,} test")
    print(f"Train approval: {df_train['label'].mean():.1%} | Test approval: {df_test['label'].mean():.1%}")

    # ── Build lookup table from training data ──
    print("\n── Building lookup table ──")
    tier1, tier2, tier3, client_procs = build_lookup_table(df_train)

    # Build processor info
    proc_info = {}
    for (cid, proc), grp in df_train.groupby(['client_id', 'processor_name']):
        proc_info[(cid, proc)] = {
            'acquiring_bank': grp['acquiring_bank'].mode().iloc[0] if len(grp['acquiring_bank'].mode()) > 0 else 'UNKNOWN',
            'mcc_code': grp['mcc_code'].mode().iloc[0] if len(grp['mcc_code'].mode()) > 0 else 'UNKNOWN',
        }

    # ── Compute rate features ──
    print("\n── Computing rate features ──")
    df_train = compute_rate_features(df_train, tier1, tier2, tier3, client_procs)
    df_test = compute_rate_features(df_test, tier1, tier2, tier3, client_procs)

    # ════════════════════════════════════════════════════════════════
    # TRAIN ALL 3 MODELS
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("TRAINING 3 MODELS")
    print("=" * 80)

    # Model A: Current — CatBoost with processor names
    print("\n── Model A: Current (CatBoost + processor names) ──")
    model_a, feats_a, cat_idx_a, probs_a, auc_a = train_catboost(
        df_train, df_test, MODEL_A_CAT, MODEL_A_NUM, "Model A (Current)"
    )

    # Model B: Rate-only — no processor identity as categorical
    print("\n── Model B: Rate-only (no proc names, uses lookup rates) ──")
    model_b, feats_b, cat_idx_b, probs_b, auc_b = train_catboost(
        df_train, df_test, MODEL_B_CAT, MODEL_B_NUM, "Model B (Rate-only)"
    )

    # Model C: Blended — processor names + rate features
    print("\n── Model C: Blended (proc names + rates) ──")
    model_c, feats_c, cat_idx_c, probs_c, auc_c = train_catboost(
        df_train, df_test, MODEL_C_CAT, MODEL_C_NUM, "Model C (Blended)"
    )

    # ════════════════════════════════════════════════════════════════
    # BACKTESTS
    # ════════════════════════════════════════════════════════════════

    results_a = run_backtest(
        model_a, feats_a, cat_idx_a, MODEL_A_CAT, MODEL_A_NUM,
        df_test, probs_a, tier1, tier2, tier3, client_procs,
        proc_info, "Model A: Current", needs_rate_features=False,
    )

    results_b = run_backtest(
        model_b, feats_b, cat_idx_b, MODEL_B_CAT, MODEL_B_NUM,
        df_test, probs_b, tier1, tier2, tier3, client_procs,
        proc_info, "Model B: Rate-only", needs_rate_features=True,
    )

    results_c = run_backtest(
        model_c, feats_c, cat_idx_c, MODEL_C_CAT, MODEL_C_NUM,
        df_test, probs_c, tier1, tier2, tier3, client_procs,
        proc_info, "Model C: Blended", needs_rate_features=True,
    )

    # ════════════════════════════════════════════════════════════════
    # FINAL COMPARISON
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("FINAL COMPARISON — ALL 3 MODELS × 4 STRATEGIES")
    print("=" * 80)

    all_results = [results_a, results_b, results_c]
    hist = all_results[0]['historical']

    print(f"\n  {'Model':<25} {'AUC':>6} {'Historical':>11} {'Lookup':>11} {'AI Only':>11} {'Lookup+AI':>11}")
    print(f"  {'─'*25} {'─'*6} {'─'*11} {'─'*11} {'─'*11} {'─'*11}")
    for r, auc in zip(all_results, [auc_a, auc_b, auc_c]):
        print(f"  {r['model']:<25} {auc:.4f}"
              f"  {r['historical']:>9.2%}"
              f"  {r['lookup']:>9.2%}"
              f"  {r['ai_only']:>9.2%}"
              f"  {r['lookup_ai']:>9.2%}")

    print(f"\n  LIFT vs Historical:")
    print(f"  {'Model':<25} {'Lookup':>11} {'AI Only':>11} {'Lookup+AI':>11} {'AI Reroute%':>12}")
    print(f"  {'─'*25} {'─'*11} {'─'*11} {'─'*11} {'─'*12}")
    for r, auc in zip(all_results, [auc_a, auc_b, auc_c]):
        print(f"  {r['model']:<25}"
              f"  {r['lookup'] - hist:>+9.2%}"
              f"  {r['ai_only'] - hist:>+9.2%}"
              f"  {r['lookup_ai'] - hist:>+9.2%}"
              f"  {r['ai_rerouted_pct']:>10.1%}")

    # ── Key questions ──
    print(f"\n  KEY QUESTIONS:")
    # Does Model C catch moderate-gap switches that Model A misses?
    print(f"  1. Model A reroute rate: {results_a['ai_rerouted_pct']:.1%} vs Model C: {results_c['ai_rerouted_pct']:.1%}")
    if results_c['ai_rerouted_pct'] > results_a['ai_rerouted_pct']:
        print(f"     → Model C switches MORE often (catches moderate gaps)")
    else:
        print(f"     → Model C does NOT switch more — rate features didn't help rerouting")

    # Does Model B lose info by dropping processor names?
    diff_ab = results_b['ai_only'] - results_a['ai_only']
    print(f"  2. Dropping proc names: AI approval {diff_ab:+.2%}")
    if diff_ab > 0.005:
        print(f"     → Rate-only is BETTER — processor names were a crutch")
    elif diff_ab < -0.005:
        print(f"     → Rate-only is WORSE — processor names carry useful info")
    else:
        print(f"     → Negligible difference — processor names are redundant with rates")

    # Which model gives best realistic lift?
    best_model = max(all_results, key=lambda r: r['lookup_ai'])
    print(f"  3. Best lookup+AI: {best_model['model']} at {best_model['lookup_ai']:.2%} ({best_model['lookup_ai'] - hist:+.2%})")

    print("\nDone!")


if __name__ == '__main__':
    main()
