"""
BinRoute AI — Full Week Scoring from CSV

Reads the subscription report CSV, scores every queued order using the
dual model (general + rebill specialist), compares against the PROCESSING
gateway (what will actually be used), and outputs recommendations + A/B plan.

Processing gateway logic:
  - Forced gateway if present, otherwise Assigned gateway
  - Current gateway = where it was previously attempted (the past)

Usage: py -3 scripts/ml/score_full_week.py
"""

import os, csv, json, re, sys
import sqlite3
import numpy as np, pandas as pd
from sklearn.preprocessing import LabelEncoder
import onnxruntime as ort
import warnings
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
GENERAL_MODEL = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models', 'binroute_lightgbm.onnx')
REBILL_MODEL = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models', 'binroute_rebill_specialist.onnx')
INPUT_CSV = os.path.join(os.path.dirname(__file__), '..', '..', 'data', '2220_report-subscription_2026-04-05T04_54_06Z.csv')
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'full_week_ai_recommendations.csv')
AB_CSV = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'full_week_ab_test.csv')

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

REBILL_EXCLUDED_GW = {192}


def extract_gw_id(processing_gw_str):
    """Extract gateway ID from processing gateway string like 'JoyP_PNC_0920_30K_(189)' or just '172'."""
    if not processing_gw_str or processing_gw_str.strip() in ('--', ''):
        return None
    s = processing_gw_str.strip()
    # Try to find (NNN) pattern
    m = re.search(r'\((\d+)\)', s)
    if m:
        return int(m.group(1))
    # Try plain number
    try:
        return int(s)
    except:
        return None


def main():
    print("=" * 75)
    print("BinRoute AI - Full Week Scoring (324 orders)")
    print("=" * 75)

    # Load CSV
    orders = []
    with open(INPUT_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            orders.append(row)
    print(f"\n  Loaded {len(orders)} orders from CSV")

    # Parse processing gateway
    for o in orders:
        pg_str = o.get('Processing gateway', '').strip()
        if pg_str in ('--', ''):
            # Derive: forced > assigned > current
            forced = o.get('Forced Gateway', '').strip()
            assigned = o.get('Assigned Gateway', '').strip()
            current = o.get('Current Gateway', '').strip()
            if forced and forced != '--':
                o['_proc_gw_id'] = int(forced)
            elif assigned and assigned != '--':
                o['_proc_gw_id'] = int(assigned)
            elif current and current != '--':
                o['_proc_gw_id'] = int(current)
            else:
                o['_proc_gw_id'] = None
        else:
            o['_proc_gw_id'] = extract_gw_id(pg_str)

        o['_price'] = float(o.get('Price', '0') or '0')
        o['_parent_id'] = int(o.get('Parent Order ID (CRM)', '0') or '0')
        o['_sub_id'] = o.get('Sub. ID', '').strip()
        o['_customer'] = o.get('Customer Name', '').strip()
        o['_tags'] = o.get('Tags', '').strip()
        o['_billing_cycle'] = int(o.get('Billing Cycle', '0') or '0')

    # Load models + encoders
    print("  Loading models...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    sess_gen = ort.InferenceSession(GENERAL_MODEL)
    sess_reb = ort.InferenceSession(REBILL_MODEL)

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

    # Enrich with card data
    print("  Enriching with card data...")
    parent_ids = list(set(o['_parent_id'] for o in orders if o['_parent_id']))
    ph = ','.join('?' * len(parent_ids))
    card_rows = conn.execute(f"""
        SELECT o.order_id, o.customer_id, o.cc_first_6,
               o.derived_product_role, o.derived_cycle, o.derived_attempt,
               b.issuer_bank, b.card_brand, b.card_type, b.is_prepaid
        FROM orders o LEFT JOIN bin_lookup b ON b.bin = o.cc_first_6
        WHERE o.client_id = 1 AND o.order_id IN ({ph})
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

    matched = sum(1 for o in orders if o['_parent_id'] in card_lookup and card_lookup[o['_parent_id']].get('cc_first_6'))
    print(f"  Matched {matched}/{len(orders)} to card data")

    # Score all orders
    print("  Scoring all orders...\n")
    results = []
    for o in orders:
        card = card_lookup.get(o['_parent_id'])
        if not card or not card.get('cc_first_6'):
            continue

        sub = sub_lookup.get(o['_parent_id'], {})
        cust_id = card.get('customer_id')
        init_proc = init_proc_map.get(cust_id, 'UNKNOWN')

        # Normalize issuer
        issuer = card.get('issuer_bank') or 'Unknown'
        u = issuer.upper()
        if 'BANK OF AMERICA' in u: issuer = 'BANK OF AMERICA, NATIONAL ASSOCIATION'
        elif 'JPMORGAN' in u: issuer = 'JPMORGAN CHASE BANK N.A.'
        elif 'CITIBANK' in u: issuer = 'CITIBANK N.A.'
        elif 'WELLS FARGO' in u: issuer = 'WELLS FARGO BANK, NATIONAL ASSOCIATION'

        # TX class
        role = card.get('derived_product_role', '')
        attempt = card.get('derived_attempt', 1) or 1
        tags = o['_tags']
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

        is_rebill_model = tx_class in ('rebill', 'salvage') and cycle_depth in ('C1', 'C2', 'C3+')

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
                'prev_decline_reason': 'UNKNOWN',
                'initial_processor': init_proc,
                'is_prepaid': card.get('is_prepaid', 0) or 0,
                'amount': o['_price'],
                'attempt_number': attempt,
                'hour_of_day': 12, 'day_of_week': 0,
                'mid_velocity_daily': 200, 'mid_velocity_weekly': 1400,
                'customer_history_on_proc': 1, 'bin_velocity_weekly': 100,
                'consecutive_approvals': sub.get('consecutive_approvals', 0) or 0,
                'days_since_last_charge': sub.get('days_since_last_charge', 0) or 0,
                'days_since_initial': sub.get('days_since_initial', 0) or 0,
                'lifetime_charges': sub.get('lifetime_charges', 0) or 0,
                'lifetime_revenue': sub.get('lifetime_revenue', 0) or 0,
                'initial_amount': sub.get('initial_amount', 0) or 0,
                'amount_ratio': sub.get('amount_ratio', 0) or 0,
                'prior_declines_in_cycle': sub.get('prior_declines_in_cycle', 0) or 0,
            }

            if is_rebill_model:
                cat_list, num_list, encs, sess = REB_CATEGORICAL, REB_NUMERICAL, reb_encoders, sess_reb
            else:
                cat_list, num_list, encs, sess = GEN_CATEGORICAL, GEN_NUMERICAL, gen_encoders, sess_gen

            encoded = []
            for col in cat_list:
                val = features[col]
                le = encs[col]
                encoded.append(le.transform([val])[0] if val in le.classes_ else
                              (le.transform(['UNKNOWN'])[0] if 'UNKNOWN' in le.classes_ else 0))
            for col in num_list:
                encoded.append(features.get(col, 0) or 0)

            X = np.array([encoded], dtype=np.float32)
            raw = sess.run(None, {sess.get_inputs()[0].name: X})
            probs = raw[1]
            if isinstance(probs, list) and isinstance(probs[0], dict):
                prob = probs[0].get(1, 0)
            elif hasattr(probs, 'shape') and len(probs.shape) == 2:
                prob = probs[0][1]
            else:
                prob = float(probs[0])

            scores.append((gw_id, prob))

        if not scores:
            continue

        scores.sort(key=lambda x: x[1], reverse=True)
        proc_gw = o['_proc_gw_id']
        proc_score = next((s for s in scores if s[0] == proc_gw), None)
        best = scores[0]

        proc_pct = proc_score[1] * 100 if proc_score else 0
        best_pct = best[1] * 100
        gap = best_pct - proc_pct
        action = 'SWITCH' if gap > 2.0 and best[0] != proc_gw else 'KEEP'

        # Get gateway names
        proc_name = gateways.get(proc_gw, {}).get('gateway_alias', str(proc_gw)) if proc_gw else '?'
        best_name = gateways.get(best[0], {}).get('gateway_alias', str(best[0]))

        results.append({
            'sub_id': o['_sub_id'],
            'customer': o['_customer'],
            'card': f"{card.get('card_brand','?')}/{card.get('card_type','?')}",
            'issuer': (issuer or '?')[:30],
            'amount': o['_price'],
            'tx_class': tx_class,
            'model': 'REBILL' if is_rebill_model else 'GENERAL',
            'tags': o['_tags'],
            'proc_gw_id': proc_gw,
            'proc_gw_name': proc_name,
            'proc_pct': round(proc_pct, 1),
            'ai_gw_id': best[0],
            'ai_gw_name': best_name,
            'ai_pct': round(best_pct, 1),
            'gap': round(gap, 1),
            'action': action,
            'top3': ' | '.join(f"[{s[0]}] {s[1]*100:.1f}%" for s in scores[:3]),
            'est_date': o.get('Estimated Processing Date', ''),
        })

    # Summary
    switches = [r for r in results if r['action'] == 'SWITCH']
    keeps = [r for r in results if r['action'] == 'KEEP']
    print(f"  Scored: {len(results)} orders")
    print(f"  KEEP: {len(keeps)} | SWITCH: {len(switches)}")
    if switches:
        avg_lift = np.mean([r['gap'] for r in switches])
        print(f"  Average lift on switches: {avg_lift:+.1f}pp")

    # Save full recommendations CSV
    if results:
        df = pd.DataFrame(results)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"\n  Recommendations CSV: {OUTPUT_CSV}")

    # Build A/B test
    switches_sorted = sorted(switches, key=lambda x: x['gap'], reverse=True)
    treatment = []
    control = []
    for i, r in enumerate(switches_sorted):
        if i % 3 == 2:
            control.append(r)
        else:
            treatment.append(r)

    print(f"\n  A/B Test Plan:")
    print(f"    TREATMENT (AI gateway): {len(treatment)}")
    print(f"    CONTROL (keep current): {len(control)}")
    print(f"    KEEP (AI agrees):       {len(keeps)}")

    # Save A/B plan
    ab_rows = []
    for r in treatment:
        ab_rows.append({**r, 'group': 'TREATMENT', 'use_gw_id': r['ai_gw_id'], 'use_gw_name': r['ai_gw_name']})
    for r in control:
        ab_rows.append({**r, 'group': 'CONTROL', 'use_gw_id': r['proc_gw_id'], 'use_gw_name': r['proc_gw_name']})
    for r in keeps:
        ab_rows.append({**r, 'group': 'KEEP', 'use_gw_id': r['proc_gw_id'], 'use_gw_name': r['proc_gw_name']})

    if ab_rows:
        df_ab = pd.DataFrame(ab_rows)
        df_ab.to_csv(AB_CSV, index=False)
        print(f"  A/B Test CSV: {AB_CSV}")

    # Print top switches
    print(f"\n  Top 20 SWITCH orders by lift:")
    print(f"  {'Sub ID':<10} {'Customer':<22} {'Proc GW':<8} {'Proc%':>6} {'AI GW':<8} {'AI%':>6} {'Lift':>7} {'Model':<8}")
    print(f"  {'-'*78}")
    for r in switches_sorted[:20]:
        print(f"  {r['sub_id']:<10} {r['customer'][:20]:<22} {r['proc_gw_id'] or '?':<8} {r['proc_pct']:>5.1f}% "
              f"{r['ai_gw_id']:<8} {r['ai_pct']:>5.1f}% {r['gap']:>+6.1f}% {r['model']:<8}")

    conn.close()
    print("\nDone!")


if __name__ == '__main__':
    main()
