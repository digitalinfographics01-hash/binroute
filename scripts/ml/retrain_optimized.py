"""
Retrain comparison: BEFORE vs AFTER
- AFTER drops mid_velocity_weekly from initial model
- AFTER adds initial_was_payfac flag to initial model
- Other models unchanged (just confirm no regression)
"""
import sqlite3, numpy as np, pandas as pd, pickle, warnings, os
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score, f1_score, precision_score, recall_score
import lightgbm as lgb
from catboost import CatBoostClassifier
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models')

conn = sqlite3.connect(DB_PATH)
df_all = pd.read_sql_query("""
    SELECT client_id, customer_id, model_target, outcome, acquisition_date,
        processor_name, acquiring_bank, mcc_code, issuer_bank, card_brand, card_type,
        offer_name, billing_state, initial_processor, last_approved_processor,
        parent_declined_processor, prev_decline_reason,
        initial_declined_processor, initial_decline_reason,
        is_prepaid, order_total, hour_of_day, day_of_week,
        mid_velocity_daily, mid_velocity_weekly, customer_history_on_proc, bin_velocity_weekly,
        mid_age_days, attempt_seq, cascade_position, total_attempts,
        had_nsf, had_do_not_honor, had_pickup,
        consecutive_approvals, days_since_last_charge, days_since_initial,
        lifetime_charges, lifetime_revenue, initial_amount, amount_ratio, prior_declines_in_cycle,
        initial_was_payfac
    FROM transaction_attempts
    WHERE feature_version >= 3 AND model_target NOT IN ('excluded')
    ORDER BY acquisition_date ASC, id ASC
""", conn)
conn.close()
df_all['label'] = (df_all['outcome'] == 'approved').astype(int)

MODELS = {
    'initial': {
        'filter': "model_target == 'initial'",
        'use_catboost': True,
        'before_cat': ['processor_name','acquiring_bank','mcc_code','issuer_bank','card_brand','card_type','offer_name','billing_state'],
        'before_num': ['is_prepaid','order_total','hour_of_day','day_of_week','mid_velocity_daily','mid_velocity_weekly','customer_history_on_proc','bin_velocity_weekly','mid_age_days'],
        'after_cat':  ['processor_name','acquiring_bank','mcc_code','issuer_bank','card_brand','card_type','offer_name','billing_state'],
        'after_num':  ['is_prepaid','order_total','hour_of_day','day_of_week','mid_velocity_daily','customer_history_on_proc','bin_velocity_weekly','mid_age_days','initial_was_payfac'],
    },
    'cascade': {
        'filter': "model_target == 'cascade'",
        'use_catboost': True,
        'before_cat': ['processor_name','acquiring_bank','mcc_code','issuer_bank','card_brand','card_type','offer_name','billing_state','initial_decline_reason','initial_declined_processor'],
        'before_num': ['is_prepaid','order_total','hour_of_day','day_of_week','mid_velocity_daily','mid_velocity_weekly','customer_history_on_proc','bin_velocity_weekly','mid_age_days','attempt_seq','cascade_position','total_attempts','had_nsf','had_do_not_honor','had_pickup'],
        'after_cat': None,
        'after_num': None,
    },
    'rebill': {
        'filter': "model_target == 'rebill'",
        'use_catboost': True,
        'before_cat': ['processor_name','acquiring_bank','mcc_code','issuer_bank','card_brand','card_type','offer_name','billing_state','initial_processor','last_approved_processor'],
        'before_num': ['is_prepaid','order_total','hour_of_day','day_of_week','mid_velocity_daily','mid_velocity_weekly','customer_history_on_proc','bin_velocity_weekly','mid_age_days','consecutive_approvals','days_since_last_charge','days_since_initial','lifetime_charges','lifetime_revenue','initial_amount','amount_ratio'],
        'after_cat': None,
        'after_num': None,
    },
    'rebill_salvage': {
        'filter': "model_target == 'rebill_salvage'",
        'use_catboost': False,
        'before_cat': ['processor_name','acquiring_bank','mcc_code','issuer_bank','card_brand','card_type','offer_name','billing_state','initial_processor','last_approved_processor','parent_declined_processor','prev_decline_reason'],
        'before_num': ['is_prepaid','order_total','hour_of_day','day_of_week','mid_velocity_daily','mid_velocity_weekly','customer_history_on_proc','bin_velocity_weekly','mid_age_days','attempt_seq','consecutive_approvals','days_since_last_charge','days_since_initial','lifetime_charges','lifetime_revenue','initial_amount','amount_ratio','prior_declines_in_cycle','cascade_position','total_attempts'],
        'after_cat': None,
        'after_num': None,
    },
}


def train_model(df, cat, num, use_catboost=False):
    enc_cols = []
    for c in cat:
        le = LabelEncoder()
        df[f'{c}_enc'] = le.fit_transform(df[c].fillna('UNKNOWN').astype(str))
        enc_cols.append(f'{c}_enc')
    for c in num:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    feat = enc_cols + num
    names = [c.replace('_enc', '') for c in feat]
    X = df[feat].values.astype(np.float32)
    y = df['label'].values
    s = int(len(X) * 0.80)
    X_tr, X_te, y_tr, y_te = X[:s], X[s:], y[:s], y[s:]

    spw = (y_tr == 0).sum() / max((y_tr == 1).sum(), 1)

    if use_catboost:
        m = CatBoostClassifier(iterations=300, depth=8, learning_rate=0.1,
                               auto_class_weights='Balanced', verbose=0, random_seed=42)
    else:
        m = lgb.LGBMClassifier(n_estimators=300, max_depth=8, learning_rate=0.1,
                                subsample=0.8, colsample_bytree=0.8,
                                scale_pos_weight=spw, verbose=-1, random_state=42)

    m.fit(X_tr, y_tr)
    p = m.predict_proba(X_te)[:, 1]
    pred = (p >= 0.5).astype(int)

    auc = roc_auc_score(y_te, p)
    f1 = f1_score(y_te, pred, zero_division=0)
    prec = precision_score(y_te, pred, zero_division=0)
    rec = recall_score(y_te, pred, zero_division=0)

    # Feature importance
    imps = []
    if hasattr(m, 'feature_importances_'):
        raw = m.feature_importances_.astype(float)
        total = raw.sum()
        norm = raw / total if total > 0 else raw
        imps = sorted(zip(names, norm), key=lambda x: x[1], reverse=True)

    return auc, f1, prec, rec, m, imps, df.iloc[s:], y_te, p


print("=" * 80)
print("  RETRAIN: BEFORE vs AFTER")
print("  Changes: initial model drops mid_velocity_weekly, adds initial_was_payfac")
print("=" * 80)

for key, cfg in MODELS.items():
    df = df_all.query(cfg['filter']).copy()
    has_changes = cfg['after_cat'] is not None
    algo = "CatBoost" if cfg['use_catboost'] else "LightGBM"

    print(f"\n  {'=' * 70}")
    print(f"  {key.upper()} ({len(df):,} rows) - {algo}")
    print(f"  {'=' * 70}")

    # BEFORE
    auc_b, f1_b, prec_b, rec_b, _, _, _, _, _ = train_model(
        df.copy(), cfg['before_cat'], cfg['before_num'], cfg['use_catboost'])
    print(f"    BEFORE:  AUC={auc_b:.4f}  F1={f1_b:.4f}  Prec={prec_b:.4f}  Recall={rec_b:.4f}")

    if has_changes:
        auc_a, f1_a, prec_a, rec_a, model, imps, test_df, y_te, y_prob = train_model(
            df.copy(), cfg['after_cat'], cfg['after_num'], cfg['use_catboost'])
        diff = auc_a - auc_b
        verdict = "IMPROVED" if diff > 0.003 else ("SIMILAR" if abs(diff) <= 0.003 else "DEGRADED")
        print(f"    AFTER:   AUC={auc_a:.4f}  F1={f1_a:.4f}  Prec={prec_a:.4f}  Recall={rec_a:.4f}")
        print(f"    DIFF:    AUC={diff:+.4f}  {verdict}")

        # Top features
        print(f"\n    Top 10 Features (AFTER):")
        for i, (feat, imp) in enumerate(imps[:10], 1):
            print(f"      {i:>2}. {feat:<35} {imp:.1%}")

        # Per-client AUC
        print(f"\n    Per Client (AFTER):")
        for cid in sorted(test_df['client_id'].unique()):
            mask = test_df['client_id'].values == cid
            sub_y = y_te[mask]
            sub_p = y_prob[mask]
            if len(sub_y) < 30 or len(set(sub_y)) < 2:
                continue
            cauc = roc_auc_score(sub_y, sub_p)
            print(f"      Client {cid}: AUC={cauc:.4f} (n={mask.sum():,}, appr={sub_y.mean():.1%})")

        # Payfac vs non-payfac
        for pf, lbl in [(0, 'Non-Payfac'), (1, 'Payfac')]:
            mask = test_df['initial_was_payfac'].values == pf
            sub_y = y_te[mask]
            sub_p = y_prob[mask]
            if len(sub_y) >= 30 and len(set(sub_y)) >= 2:
                pauc = roc_auc_score(sub_y, sub_p)
                print(f"      {lbl} customers: AUC={pauc:.4f} (n={mask.sum():,})")

        # Per cycle for rebill
        if key == 'initial':
            print(f"\n    Per product role (AFTER):")
            for role in ['main_initial', 'upsell_initial']:
                mask = test_df['derived_product_role'].values == role if 'derived_product_role' in test_df.columns else np.zeros(len(test_df), dtype=bool)
                sub_y = y_te[mask]
                sub_p = y_prob[mask]
                if len(sub_y) >= 30 and len(set(sub_y)) >= 2:
                    rauc = roc_auc_score(sub_y, sub_p)
                    print(f"      {role}: AUC={rauc:.4f} (n={mask.sum():,}, appr={sub_y.mean():.1%})")

        # Save model
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        pkl_path = os.path.join(OUTPUT_DIR, f'four_model_{key}_v2.pkl')
        with open(pkl_path, 'wb') as f:
            pickle.dump(model, f)
        print(f"\n    Saved: {pkl_path}")
    else:
        print(f"    (no changes — keeping existing model)")

print(f"\n{'=' * 80}")
print("  DONE")
print(f"{'=' * 80}")
