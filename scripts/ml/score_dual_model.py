"""
BinRoute AI — Dual Model Scoring

Uses two models:
  - General model (27 features, includes tx_class) for initials, upsells, cascade
  - Rebill specialist (25 features, no tx_class/is_prepaid) for rebills + rebill salvage

Scores all queued orders and generates final recommendations CSV.

Usage: py -3 scripts/ml/score_dual_model.py
"""

import os, sqlite3, json
import numpy as np, pandas as pd
from sklearn.preprocessing import LabelEncoder
import onnxruntime as ort
import warnings
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
GENERAL_MODEL = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models', 'binroute_lightgbm.onnx')
REBILL_MODEL = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models', 'binroute_rebill_specialist.onnx')
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'queued_orders_dual_model.csv')

# General model features (27)
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

# Rebill specialist features (25) — no tx_class, no is_prepaid
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

# Gateways excluded from rebill/salvage
REBILL_EXCLUDED_GW = {192}

# ------- ORDER DATA -------
# (sub_id, parent_order_id, billing_cycle, status, tags, decline_reason, current_gw_id, current_gw_name, assigned_gw_id, price, customer)

PROCESSED_ORDERS = [
    (341819, 644455, 2, 'Approved', '1st Decline Recycling', None, 189, 'PNC_0920', 187, 79.97, 'Stephen Mooney'),
    (341507, 644144, 1, 'Approved', '2nd Decline Recycling', None, 190, 'PNC_0953', 194, 59.97, 'Alan Hochderffer'),
    (341489, 640110, 1, 'Approved', '2nd Decline Recycling', None, 191, 'PNC_0938', 188, 39.97, 'Woodrow Morris III'),
    (341305, 640511, 1, 'Approved', '3rd Decline Recycling', None, 189, 'PNC_0920', 172, 44.91, 'Rony jean noel'),
    (341265, 646639, 1, 'Approved', 'Linked', None, 187, 'PNC_0946', 191, 97.48, 'Donald Chambers'),
    (340880, 645980, 1, 'Approved', 'Linked', None, 190, 'PNC_0953', 191, 89.98, 'Ventzilav Dimitrov'),
    (338445, 639907, 5, 'Approved', 'Linked', None, 188, 'PNC_0961', 191, 6.96, 'Debbie West'),
    (341817, 642415, 1, 'Declined', '1st Recycle Failed', 'Insufficient funds', 190, 'PNC_0953', 191, 49.97, 'Will Davison'),
    (341788, 642910, 1, 'Declined', '1st Recycle Failed', 'Insufficient funds', 189, 'PNC_0920', 191, 49.97, 'Steve Morton'),
    (341786, 642416, 1, 'Declined', '1st Recycle Failed', 'Insufficient funds', 191, 'PNC_0938', 188, 49.97, 'Will Davison'),
    (341503, 635970, 3, 'Declined', '2nd Recycle Failed', 'Insufficient funds', 180, 'EMS_closed', 188, 49.97, 'Roger D. Nation'),
    (341501, 644363, 1, 'Declined', '2nd Recycle Failed', 'Insufficient funds', 172, 'Cliq', 190, 59.97, 'Joel Cannon'),
    (341499, 642414, 1, 'Declined', '2nd Recycle Failed', 'Insufficient funds', 189, 'PNC_0920', 188, 49.97, 'Will Davison'),
    (341487, 644118, 1, 'Declined', '2nd Recycle Failed', 'Do Not Honor', 190, 'PNC_0953', 172, 59.97, 'Phillip Winston'),
    (341264, 646624, 1, 'Declined', 'Linked', 'Issuer Declined', 188, 'PNC_0961', 190, 97.48, 'Albert Seeney'),
    (341263, 646622, 1, 'Declined', 'Linked', 'Issuer Declined', 172, 'Cliq', 190, 97.48, 'Emilio Ramirez'),
    (341261, 646619, 1, 'Declined', 'Linked', 'Issuer Declined', 172, 'Cliq', 187, 97.48, 'Bhupendra Panchal'),
    (341260, 646609, 1, 'Declined', 'Linked', 'Issuer Declined', 191, 'PNC_0938', 189, 6.96, 'Willie Moore'),
    (341256, 646605, 1, 'Declined', 'Linked', 'Issuer Declined', 187, 'PNC_0946', 189, 97.48, 'Willie Moore'),
    (341255, 646603, 1, 'Declined', 'Linked', 'Do Not Honor', 190, 'PNC_0953', 172, 97.48, 'Codricas Campbell'),
    (341254, 646602, 1, 'Declined', 'Linked', 'Issuer Declined', 189, 'PNC_0920', 190, 6.96, 'Phillip Walters'),
    (341250, 646597, 1, 'Declined', 'Linked', 'Insufficient funds', 172, 'Cliq', 190, 97.48, 'William Tuin'),
    (341248, 646594, 1, 'Declined', 'Linked', 'Issuer Declined', 188, 'PNC_0961', 189, 97.48, 'Roosevelt Hughes'),
    (341246, 646588, 1, 'Declined', 'Linked', 'Issuer Declined', 190, 'PNC_0953', 187, 97.48, 'Miles Henderson'),
    (341196, 646490, 1, 'Declined', 'Linked', 'Insufficient funds', 172, 'Cliq', 190, 97.48, 'William Tuin'),
    (340778, 645764, 1, 'Declined', 'Linked', 'Issuer Declined', 190, 'PNC_0953', 189, 89.98, 'Jean Musypay'),
    (340615, 645179, 2, 'Declined', 'Linked', 'Issuer Declined', 172, 'Cliq', 191, 89.98, 'Matthew Clark'),
    (340571, 645114, 2, 'Declined', 'Linked', 'Do Not Honor', 191, 'PNC_0938', 172, 89.98, 'Donald Tanner'),
    (340544, 645056, 2, 'Declined', 'Linked', 'Issuer Declined', 191, 'PNC_0938', 189, 89.98, 'Rudi Ayala'),
    (340541, 645049, 2, 'Declined', 'Linked', 'Insufficient funds', 191, 'PNC_0938', 172, 89.98, 'William Recor'),
    (340221, 644306, 2, 'Declined', 'Linked', 'Do Not Honor', 190, 'PNC_0953', 172, 97.48, 'Jonathan Loyo'),
    (339740, 643443, 1, 'Declined', 'Linked', 'Issuer Declined', 189, 'PNC_0920', 191, 59.97, 'Ronald Cooks'),
    (339720, 643425, 1, 'Declined', 'Linked', 'Issuer Declined', 190, 'PNC_0953', 187, 49.97, 'Tony Cummings'),
    (339719, 643424, 1, 'Declined', 'Linked', 'Issuer Declined', 189, 'PNC_0920', 188, 59.97, 'Tony Cummings'),
    (338516, 640133, 4, 'Declined', 'Linked', 'Issuer Declined', 189, 'PNC_0920', 190, 59.97, 'Sandra Kowtko'),
    (338175, 638992, 4, 'Declined', 'Linked', 'Issuer Declined', 172, 'Cliq', 187, 59.97, 'Debra Waterbury'),
]

QUEUED_ORDERS = [
    (341889, 646490, 1, 'Queue', '1st Decline Recycling', None, 172, 'Cliq', 190, 79.97, 'William Tuin'),
    (341887, 646609, 1, 'Queue', '1st Decline Recycling', None, 191, 'PNC_0938', 187, 6.96, 'Willie Moore'),
    (341886, 643443, 1, 'Queue', '1st Decline Recycling', None, 189, 'PNC_0920', 196, 49.97, 'Ronald Cooks'),
    (341885, 645056, 2, 'Queue', '1st Decline Recycling', None, 191, 'PNC_0938', 198, 79.97, 'Rudi Ayala'),
    (341883, 638992, 4, 'Queue', '1st Decline Recycling', None, 172, 'Cliq', 191, 59.97, 'Debra Waterbury'),
    (341882, 646597, 1, 'Queue', '1st Decline Recycling', None, 189, 'PNC_0920', 200, 79.97, 'Phillip Walters'),
    (341881, 645114, 2, 'Queue', '1st Decline Recycling', None, 191, 'PNC_0938', 187, 79.97, 'Donald Tanner'),
    (341878, 645049, 2, 'Queue', '1st Decline Recycling', None, 191, 'PNC_0938', 187, 79.97, 'William Recor'),
    (341877, 646605, 1, 'Queue', '1st Decline Recycling', None, 187, 'PNC_0946', 199, 79.97, 'Willie Moore'),
    (341876, 646594, 1, 'Queue', '1st Decline Recycling', None, 188, 'PNC_0961', 198, 79.97, 'Roosevelt Hughes'),
    (341874, 640133, 4, 'Queue', '1st Decline Recycling', None, 189, 'PNC_0920', 188, 59.97, 'Sandra Kowtko'),
    (341873, 644306, 2, 'Queue', '1st Decline Recycling', None, 190, 'PNC_0953', 198, 79.97, 'Jonathan Loyo'),
    (341872, 643425, 1, 'Queue', '1st Decline Recycling', None, 190, 'PNC_0953', 197, 49.97, 'Tony Cummings'),
    (341871, 645764, 1, 'Queue', '1st Decline Recycling', None, 190, 'PNC_0953', 188, 79.97, 'Jean Musypay'),
    (341868, 646602, 1, 'Queue', '1st Decline Recycling', None, 189, 'PNC_0920', 187, 6.96, 'Phillip Walters'),
    (341866, 645179, 2, 'Queue', '1st Decline Recycling', None, 172, 'Cliq', 189, 79.97, 'Matthew Clark'),
    (341862, 646603, 1, 'Queue', '1st Decline Recycling', None, 190, 'PNC_0953', 198, 79.97, 'Codricas Campbell'),
    (341861, 643424, 1, 'Queue', '1st Decline Recycling', None, 189, 'PNC_0920', 198, 49.97, 'Tony Cummings'),
    (341860, 646588, 1, 'Queue', '1st Decline Recycling', None, 190, 'PNC_0953', 199, 79.97, 'Miles Henderson'),
    (341858, 646619, 1, 'Queue', '1st Decline Recycling', None, 172, 'Cliq', 187, 79.97, 'Bhupendra Panchal'),
    (341857, 646624, 1, 'Queue', '1st Decline Recycling', None, 188, 'PNC_0961', 191, 79.97, 'Albert Seeney'),
    (341856, 646622, 1, 'Queue', '1st Decline Recycling', None, 172, 'Cliq', 190, 79.97, 'Emilio Ramirez'),
    (341850, 645517, 1, 'Queue', '1st Decline Recycling', None, 188, 'PNC_0961', 191, 79.97, 'Randy England'),
    (341848, 645675, 1, 'Queue', '1st Decline Recycling', None, 189, 'PNC_0920', 191, 79.97, 'Larry Farmer'),
    (341847, 645635, 1, 'Queue', '1st Decline Recycling', None, 190, 'PNC_0953', 199, 79.97, 'Hank Carr'),
    (341845, 644773, 2, 'Queue', '1st Decline Recycling', None, 172, 'Cliq', 191, 79.97, 'John Byrd'),
    (341844, 645650, 1, 'Queue', '1st Decline Recycling', None, 190, 'PNC_0953', 187, 79.97, 'ASSEFA Seyoum'),
    (341838, 644027, 2, 'Queue', '1st Decline Recycling', None, 187, 'PNC_0946', 190, 79.97, 'Harold Allen'),
    (341836, 646457, 1, 'Queue', '1st Decline Recycling', None, 191, 'PNC_0938', 198, 79.97, 'Mark Adkins'),
    (341833, 645565, 1, 'Queue', '1st Decline Recycling', None, 187, 'PNC_0946', 189, 79.97, 'Iren Lytle'),
    (341832, 645740, 1, 'Queue', '1st Decline Recycling', None, 187, 'PNC_0946', 189, 79.97, 'Gabino Trinidad'),
    (341830, 646466, 1, 'Queue', '1st Decline Recycling', None, 172, 'Cliq', 199, 79.97, 'Edward Hurd'),
    (341829, 644039, 2, 'Queue', '1st Decline Recycling', None, 172, 'Cliq', 198, 79.97, 'Tyrone Mason'),
    (341828, 645667, 1, 'Queue', '1st Decline Recycling', None, 191, 'PNC_0938', 187, 79.97, 'Timothy Collier'),
    (341827, 643961, 2, 'Queue', '1st Decline Recycling', None, 187, 'PNC_0946', 196, 79.97, 'Matthew Clark'),
    (341826, 646307, 1, 'Queue', '1st Decline Recycling', None, 188, 'PNC_0961', 187, 79.97, 'Henry Miller'),
    (341825, 646311, 1, 'Queue', '1st Decline Recycling', None, 191, 'PNC_0938', 190, 79.97, 'Santos Nuncio'),
    (341824, 645716, 1, 'Queue', '1st Decline Recycling', None, 187, 'PNC_0946', 196, 79.97, 'Charles Johnson'),
    (341823, 646472, 1, 'Queue', '1st Decline Recycling', None, 189, 'PNC_0920', 188, 79.97, 'Bryan Chapman'),
    (341545, 647194, 1, 'Queue', 'Linked', None, 193, 'Ridge_SYN', 188, 129.97, 'Doug Rosenberry'),
]


def main():
    print("=" * 75)
    print("BinRoute AI — Dual Model Scoring (General + Rebill Specialist)")
    print("=" * 75)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Load both models
    print("\n[1] Loading models...")
    sess_gen = ort.InferenceSession(GENERAL_MODEL)
    sess_reb = ort.InferenceSession(REBILL_MODEL)
    print(f"  General model: {sess_gen.get_inputs()[0].shape} features")
    print(f"  Rebill model:  {sess_reb.get_inputs()[0].shape} features")

    # Fit encoders on all tx_features
    all_txf = pd.read_sql_query("SELECT * FROM tx_features WHERE feature_version >= 2", conn)
    gen_encoders = {}
    for col in GEN_CATEGORICAL:
        le = LabelEncoder()
        le.fit(all_txf[col].fillna('UNKNOWN').astype(str))
        gen_encoders[col] = le
    reb_encoders = {}
    for col in REB_CATEGORICAL:
        le = LabelEncoder()
        le.fit(all_txf[col].fillna('UNKNOWN').astype(str))
        reb_encoders[col] = le

    # Load gateways
    gw_rows = conn.execute("SELECT gateway_id, gateway_alias, processor_name, bank_name, mcc_code FROM gateways WHERE client_id = 1").fetchall()
    gateways = {r['gateway_id']: dict(r) for r in gw_rows}
    active_gws = [gid for gid, g in gateways.items() if g['processor_name'] and not (g['gateway_alias'] or '').startswith('Closed')]

    # Enrich all orders
    all_orders = PROCESSED_ORDERS + QUEUED_ORDERS
    parent_ids = list(set(o[1] for o in all_orders))
    enrich = enrich_orders(conn, parent_ids)

    # --- Score processed orders ---
    print(f"\n{'='*75}")
    print(f"PROCESSED ORDERS — General vs Dual Model Validation")
    print(f"{'='*75}")

    print(f"\n  {'Customer':<22} {'Status':<10} {'Model':<10} {'Used GW':<10} {'Used%':>6} {'AI Best':<10} {'Best%':>6} {'Gap':>7}")
    print(f"  {'-'*83}")

    for order in PROCESSED_ORDERS:
        result = score_order(order, enrich, gateways, active_gws,
                            sess_gen, sess_reb, gen_encoders, reb_encoders)
        if not result:
            continue
        status = order[3][:8]
        print(f"  {result['customer']:<22} {status:<10} {result['model_used']:<10} "
              f"{result['current_name']:<10} {result['current_pct']:>5.1f}% "
              f"{result['best_name']:<10} {result['best_pct']:>5.1f}% "
              f"{result['gap']:>+6.1f}%")

    # --- Score queued orders ---
    print(f"\n{'='*75}")
    print(f"QUEUED ORDERS — Dual Model Recommendations")
    print(f"{'='*75}")

    print(f"\n  {'Customer':<22} {'Model':<10} {'Curr GW':<10} {'Curr%':>6} {'AI Best':<10} {'Best%':>6} {'Gap':>7} {'Action':>8}")
    print(f"  {'-'*83}")

    csv_rows = []
    for order in QUEUED_ORDERS:
        result = score_order(order, enrich, gateways, active_gws,
                            sess_gen, sess_reb, gen_encoders, reb_encoders)
        if not result:
            continue

        action = 'SWITCH' if result['gap'] > 2.0 else 'KEEP'
        print(f"  {result['customer']:<22} {result['model_used']:<10} "
              f"{result['current_name']:<10} {result['current_pct']:>5.1f}% "
              f"{result['best_name']:<10} {result['best_pct']:>5.1f}% "
              f"{result['gap']:>+6.1f}% {action:>8}")

        csv_rows.append({
            'Sub ID': order[0],
            'Customer': result['customer'],
            'Card': result['card'],
            'Issuer': result['issuer'],
            'Amount': order[9],
            'TX Class': result['tx_class'],
            'Model Used': result['model_used'],
            'Current GW ID': order[6],
            'Current GW': result['current_name'],
            'Current %': round(result['current_pct'], 1),
            'AI Best GW ID': result['best_gw_id'],
            'AI Best GW': result['best_name'],
            'AI Best %': round(result['best_pct'], 1),
            'Lift pp': round(result['gap'], 1),
            'Action': action,
            'Top 3': result['top3_str'],
        })

    # Summary
    switches = [r for r in csv_rows if r['Action'] == 'SWITCH']
    keeps = [r for r in csv_rows if r['Action'] == 'KEEP']
    print(f"\n  SUMMARY: {len(keeps)} KEEP, {len(switches)} SWITCH")
    if switches:
        avg_lift = np.mean([r['Lift pp'] for r in switches])
        print(f"  Average lift on switches: {avg_lift:+.1f}pp")

    # Save CSV
    if csv_rows:
        df_out = pd.DataFrame(csv_rows)
        df_out.to_csv(OUTPUT_CSV, index=False)
        print(f"\n  CSV saved: {OUTPUT_CSV}")

    conn.close()
    print("\nDone!")


def enrich_orders(conn, parent_ids):
    """Build enrichment lookup from parent order IDs."""
    placeholders = ','.join('?' * len(parent_ids))

    card_rows = conn.execute(f"""
        SELECT o.order_id, o.customer_id, o.cc_first_6,
               o.derived_product_role, o.derived_cycle, o.derived_attempt,
               b.issuer_bank, b.card_brand, b.card_type, b.is_prepaid
        FROM orders o LEFT JOIN bin_lookup b ON b.bin = o.cc_first_6
        WHERE o.client_id = 1 AND o.order_id IN ({placeholders})
    """, parent_ids).fetchall()
    card_lookup = {r['order_id']: dict(r) for r in card_rows}

    # Initial processors
    cust_ids = list(set(card_lookup[pid]['customer_id'] for pid in parent_ids
                       if pid in card_lookup and card_lookup[pid]['customer_id']))
    init_proc_map = {}
    if cust_ids:
        ph2 = ','.join('?' * len(cust_ids))
        init_rows = conn.execute(f"""
            SELECT o.customer_id, g.processor_name
            FROM orders o JOIN gateways g ON g.client_id = o.client_id AND g.gateway_id = o.processing_gateway_id
            WHERE o.client_id = 1 AND o.derived_product_role = 'main_initial' AND o.order_status IN (2,6,8)
              AND o.customer_id IN ({ph2}) ORDER BY o.acquisition_date ASC
        """, cust_ids).fetchall()
        for r in init_rows:
            if r['customer_id'] not in init_proc_map:
                init_proc_map[r['customer_id']] = r['processor_name'].strip().upper() if r['processor_name'] else 'UNKNOWN'

    # Subscription features
    sub_lookup = {}
    for pid in parent_ids:
        row = conn.execute("SELECT * FROM tx_features WHERE client_id = 1 AND sticky_order_id = ?", [pid]).fetchone()
        if row:
            sub_lookup[pid] = dict(row)

    return {'cards': card_lookup, 'init_proc': init_proc_map, 'subs': sub_lookup}


def normalize_issuer(issuer):
    if not issuer:
        return 'Unknown'
    u = issuer.upper()
    if 'BANK OF AMERICA' in u: return 'BANK OF AMERICA, NATIONAL ASSOCIATION'
    elif 'JPMORGAN' in u or 'JP MORGAN' in u: return 'JPMORGAN CHASE BANK N.A.'
    elif 'CITIBANK' in u: return 'CITIBANK N.A.'
    elif 'WELLS FARGO' in u: return 'WELLS FARGO BANK, NATIONAL ASSOCIATION'
    return issuer


def determine_tx_class(card, tags):
    role = card.get('derived_product_role', '')
    attempt = card.get('derived_attempt', 1) or 1

    if '1st' in tags: attempt = max(attempt, 2)
    elif '2nd' in tags: attempt = max(attempt, 3)
    elif '3rd' in tags: attempt = max(attempt, 4)

    tx_class = 'salvage'
    if attempt == 1:
        if role == 'main_initial': tx_class = 'initial'
        elif role == 'upsell_initial': tx_class = 'upsell'
        elif role in ('main_rebill', 'upsell_rebill'): tx_class = 'rebill'

    dc = card.get('derived_cycle', 0) or 0
    cycle_depth = 'C0' if dc == 0 else ('C1' if dc == 1 else ('C2' if dc == 2 else 'C3+'))

    return tx_class, cycle_depth, attempt


def use_rebill_model(tx_class, cycle_depth):
    """Determine if rebill specialist should be used."""
    return tx_class in ('rebill', 'salvage') and cycle_depth in ('C1', 'C2', 'C3+')


def score_gateway(features_dict, categorical_list, numerical_list, encoders, sess):
    """Build feature vector and score with given model."""
    encoded = []
    for col in categorical_list:
        val = features_dict.get(col, 'UNKNOWN')
        le = encoders[col]
        if val in le.classes_:
            encoded.append(le.transform([val])[0])
        elif 'UNKNOWN' in le.classes_:
            encoded.append(le.transform(['UNKNOWN'])[0])
        else:
            encoded.append(0)
    for col in numerical_list:
        encoded.append(features_dict.get(col, 0) or 0)

    X = np.array([encoded], dtype=np.float32)
    raw = sess.run(None, {sess.get_inputs()[0].name: X})
    probs = raw[1]
    if isinstance(probs, list) and isinstance(probs[0], dict):
        return probs[0].get(1, probs[0].get('1', 0))
    elif hasattr(probs, 'shape') and len(probs.shape) == 2:
        return probs[0][1]
    return float(probs[0])


def score_order(order_tuple, enrich, gateways, active_gws,
                sess_gen, sess_reb, gen_encoders, reb_encoders):
    """Score one order across all gateways using the appropriate model."""
    sub_id, parent_id, billing_cycle, status, tags, decline_reason, current_gw, current_gw_name, assigned_gw, price, customer = order_tuple

    card = enrich['cards'].get(parent_id)
    if not card or not card.get('cc_first_6'):
        return None

    sub = enrich['subs'].get(parent_id, {})
    cust_id = card.get('customer_id')
    init_proc = enrich['init_proc'].get(cust_id, 'UNKNOWN')
    issuer = normalize_issuer(card.get('issuer_bank'))

    tx_class, cycle_depth, attempt_num = determine_tx_class(card, tags)
    is_rebill = use_rebill_model(tx_class, cycle_depth)

    # Choose model
    if is_rebill:
        model_name = 'REBILL'
        sess = sess_reb
        cat_list = REB_CATEGORICAL
        num_list = REB_NUMERICAL
        encoders = reb_encoders
    else:
        model_name = 'GENERAL'
        sess = sess_gen
        cat_list = GEN_CATEGORICAL
        num_list = GEN_NUMERICAL
        encoders = gen_encoders

    # Score each gateway
    scores = []
    for gw_id in active_gws:
        gw = gateways.get(gw_id)
        if not gw or not gw['processor_name']:
            continue
        if gw_id in REBILL_EXCLUDED_GW and tx_class in ('rebill', 'salvage', 'cascade'):
            continue

        features = {
            'processor_name': gw['processor_name'].strip().upper(),
            'acquiring_bank': gw.get('bank_name') or 'UNKNOWN',
            'mcc_code': gw.get('mcc_code') or 'UNKNOWN',
            'issuer_bank': issuer,
            'card_brand': card.get('card_brand') or 'UNKNOWN',
            'card_type': card.get('card_type') or 'UNKNOWN',
            'tx_class': tx_class,
            'cycle_depth': cycle_depth,
            'prev_decline_reason': decline_reason or 'UNKNOWN',
            'initial_processor': init_proc,
            'is_prepaid': card.get('is_prepaid', 0) or 0,
            'amount': price,
            'attempt_number': attempt_num,
            'hour_of_day': 12,
            'day_of_week': 0,
            'mid_velocity_daily': 200,
            'mid_velocity_weekly': 1400,
            'customer_history_on_proc': 1,
            'bin_velocity_weekly': 100,
            'consecutive_approvals': sub.get('consecutive_approvals', 0) or 0,
            'days_since_last_charge': sub.get('days_since_last_charge', 0) or 0,
            'days_since_initial': sub.get('days_since_initial', 0) or 0,
            'lifetime_charges': sub.get('lifetime_charges', 0) or 0,
            'lifetime_revenue': sub.get('lifetime_revenue', 0) or 0,
            'initial_amount': sub.get('initial_amount', 0) or 0,
            'amount_ratio': sub.get('amount_ratio', 0) or 0,
            'prior_declines_in_cycle': sub.get('prior_declines_in_cycle', 0) or 0,
        }

        prob = score_gateway(features, cat_list, num_list, encoders, sess)
        alias = gw.get('gateway_alias', str(gw_id)) or str(gw_id)
        # Shorten alias
        short = alias.replace('JoyP_', '').replace('Ridge_', 'R:')[:18]
        scores.append((gw_id, short, prob))

    scores.sort(key=lambda x: x[2], reverse=True)
    current_score = next((s for s in scores if s[0] == current_gw), None)
    best = scores[0] if scores else None

    top3_str = ' | '.join(f"[{s[0]}] {s[2]*100:.1f}%" for s in scores[:3])

    return {
        'customer': customer,
        'card': f"{card.get('card_brand','?')}/{card.get('card_type','?')}",
        'issuer': issuer[:25],
        'tx_class': tx_class,
        'model_used': model_name,
        'current_name': (current_score[1] if current_score else current_gw_name)[:10],
        'current_pct': current_score[2] * 100 if current_score else 0,
        'best_gw_id': best[0] if best else 0,
        'best_name': (best[1] if best else '?')[:10],
        'best_pct': best[2] * 100 if best else 0,
        'gap': (best[2] * 100 if best else 0) - (current_score[2] * 100 if current_score else 0),
        'top3_str': top3_str,
    }


if __name__ == '__main__':
    main()
