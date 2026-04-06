"""
BinRoute AI — Validate model against already-processed orders.

Scores each processed order across all gateways, then compares:
- For APPROVED: did the model rank the actual gateway highly?
- For DECLINED: did the model have a better gateway available?

Usage: py -3 scripts/ml/validate_processed.py
"""

import os, sqlite3, numpy as np, pandas as pd
from sklearn.preprocessing import LabelEncoder
import onnxruntime as ort
import warnings
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models', 'binroute_lightgbm.onnx')

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

# Gateways excluded from rebill/salvage scoring
REBILL_EXCLUDED = {192}

# Processed orders: (sub_id, parent_order_id, billing_cycle, prepaid, status, decline_reason, current_gw_id, current_gw_name, price, customer)
PROCESSED = [
    (341819, 644455, 2, 'Yes', 'Approved', None, 189, 'PNC_0920', 79.97, 'Stephen Mooney'),
    (341507, 644144, 1, 'Yes', 'Approved', None, 190, 'PNC_0953', 59.97, 'Alan Hochderffer'),
    (341489, 640110, 1, 'Yes', 'Approved', None, 191, 'PNC_0938', 39.97, 'Woodrow Morris III'),
    (341305, 640511, 1, 'No', 'Approved', None, 189, 'PNC_0920', 44.91, 'Rony jean noel'),
    (341265, 646639, 1, 'No', 'Approved', None, 187, 'PNC_0946', 97.48, 'Donald Chambers'),
    (340880, 645980, 1, 'No', 'Approved', None, 190, 'PNC_0953', 89.98, 'Ventzilav Dimitrov'),
    (338445, 639907, 5, 'No', 'Approved', None, 188, 'PNC_0961', 6.96, 'Debbie West'),
    (341817, 642415, 1, 'No', 'Declined', 'Insufficient funds', 190, 'PNC_0953', 49.97, 'Will Davison'),
    (341788, 642910, 1, 'Yes', 'Declined', 'Insufficient funds', 189, 'PNC_0920', 49.97, 'Steve Morton'),
    (341786, 642416, 1, 'No', 'Declined', 'Insufficient funds', 191, 'PNC_0938', 49.97, 'Will Davison'),
    (341503, 635970, 3, 'No', 'Declined', 'Insufficient funds', 180, 'EMS_0587(closed)', 49.97, 'Roger D. Nation'),
    (341501, 644363, 1, 'No', 'Declined', 'Insufficient funds', 172, 'Cliq', 59.97, 'Joel Cannon'),
    (341499, 642414, 1, 'No', 'Declined', 'Insufficient funds', 189, 'PNC_0920', 49.97, 'Will Davison'),
    (341487, 644118, 1, 'No', 'Declined', 'Do Not Honor', 190, 'PNC_0953', 59.97, 'Phillip Winston'),
    (341264, 646624, 1, 'No', 'Declined', 'Issuer Declined', 188, 'PNC_0961', 97.48, 'Albert Seeney'),
    (341263, 646622, 1, 'No', 'Declined', 'Issuer Declined', 172, 'Cliq', 97.48, 'Emilio Ramirez'),
    (341261, 646619, 1, 'No', 'Declined', 'Issuer Declined', 172, 'Cliq', 97.48, 'Bhupendra Panchal'),
    (341260, 646609, 1, 'No', 'Declined', 'Issuer Declined', 191, 'PNC_0938', 6.96, 'Willie Moore'),
    (341256, 646605, 1, 'No', 'Declined', 'Issuer Declined', 187, 'PNC_0946', 97.48, 'Willie Moore'),
    (341255, 646603, 1, 'No', 'Declined', 'Do Not Honor', 190, 'PNC_0953', 97.48, 'Codricas Campbell'),
    (341254, 646602, 1, 'No', 'Declined', 'Issuer Declined', 189, 'PNC_0920', 6.96, 'Phillip Walters'),
    (341250, 646597, 1, 'No', 'Declined', 'Insufficient funds', 172, 'Cliq', 97.48, 'William Tuin'),
    (341248, 646594, 1, 'Yes', 'Declined', 'Issuer Declined', 188, 'PNC_0961', 97.48, 'Roosevelt Hughes'),
    (341246, 646588, 1, 'Yes', 'Declined', 'Issuer Declined', 190, 'PNC_0953', 97.48, 'Miles Henderson'),
    (341196, 646490, 1, 'No', 'Declined', 'Insufficient funds', 172, 'Cliq', 97.48, 'William Tuin'),
    (340778, 645764, 1, 'No', 'Declined', 'Issuer Declined', 190, 'PNC_0953', 89.98, 'Jean Musypay'),
    (340615, 645179, 2, 'No', 'Declined', 'Issuer Declined', 172, 'Cliq', 89.98, 'Matthew Clark'),
    (340571, 645114, 2, 'No', 'Declined', 'Do Not Honor', 191, 'PNC_0938', 89.98, 'Donald Tanner'),
    (340544, 645056, 2, 'No', 'Declined', 'Issuer Declined', 191, 'PNC_0938', 89.98, 'Rudi Ayala'),
    (340541, 645049, 2, 'Yes', 'Declined', 'Insufficient funds', 191, 'PNC_0938', 89.98, 'William Recor'),
    (340221, 644306, 2, 'No', 'Declined', 'Do Not Honor', 190, 'PNC_0953', 97.48, 'Jonathan Loyo'),
    (339740, 643443, 1, 'No', 'Declined', 'Issuer Declined', 189, 'PNC_0920', 59.97, 'Ronald Cooks'),
    (339720, 643425, 1, 'No', 'Declined', 'Issuer Declined', 190, 'PNC_0953', 49.97, 'Tony Cummings'),
    (339719, 643424, 1, 'No', 'Declined', 'Issuer Declined', 189, 'PNC_0920', 59.97, 'Tony Cummings'),
    (338516, 640133, 4, 'Yes', 'Declined', 'Issuer Declined', 189, 'PNC_0920', 59.97, 'Sandra Kowtko'),
    (338175, 638992, 4, 'Yes', 'Declined', 'Issuer Declined', 172, 'Cliq', 59.97, 'Debra Waterbury'),
    (341818, 645411, 1, 'No', 'Declined_AFR', 'Bad Bin or Host Disconnect', 188, 'PNC_0961', 79.97, 'Glenn Tate'),
    (341814, 643631, 2, 'No', 'Declined_AFR', 'Bad Bin or Host Disconnect', 189, 'PNC_0920', 79.97, 'John Byrd'),
    (341810, 646291, 1, 'No', 'Declined_AFR', 'Issuer Declined', 191, 'PNC_0938', 79.97, 'Randy Clark'),
    (341809, 645059, 1, 'No', 'Declined_AFR', 'Bad Bin or Host Disconnect', 187, 'PNC_0946', 79.97, 'Earnest Parker sr'),
    (341805, 646296, 1, 'Yes', 'Declined_AFR', 'Activity limit exceeded', 191, 'PNC_0938', 79.97, 'John Taylor'),
    (341804, 646179, 1, 'Yes', 'Declined_AFR', 'Issuer Declined', 190, 'PNC_0953', 79.97, 'Erik Taylor'),
    (341802, 645400, 1, 'No', 'Declined_AFR', 'Bad Bin or Host Disconnect', 190, 'PNC_0953', 79.97, 'Lynn Spears'),
    (341799, 638242, 4, 'No', 'Declined_AFR', 'Issuer Declined', 182, 'EMS_0595(closed)', 59.97, 'Alice Griffin'),
    (341794, 646157, 1, 'No', 'Declined_AFR', 'Bad Bin or Host Disconnect', 172, 'Cliq', 79.97, 'Kenneth Hudson Jr'),
    (341782, 638432, 4, 'Yes', 'Declined_AFR', 'Issuer Declined', 190, 'PNC_0953', 59.97, 'Valorie Smith'),
    (341781, 646287, 1, 'No', 'Declined_AFR', 'Issuer Declined', 187, 'PNC_0946', 79.97, 'Lisa Vaughn'),
    (341780, 644561, 2, 'Yes', 'Declined_AFR', 'Issuer Declined', 172, 'Cliq', 79.97, 'Antonio Jordan'),
    (341779, 645384, 1, 'No', 'Declined_AFR', 'Issuer Declined', 190, 'PNC_0953', 79.97, 'Hector Rauda'),
    (341778, 646301, 1, 'No', 'Declined_AFR', 'Issuer Declined', 172, 'Cliq', 79.97, 'Christian Holden'),
]


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Load model + encoders
    all_txf = pd.read_sql_query("SELECT * FROM tx_features WHERE feature_version >= 2 ORDER BY acquisition_date ASC", conn)
    encoders = {}
    for col in CATEGORICAL:
        le = LabelEncoder()
        le.fit(all_txf[col].fillna('UNKNOWN').astype(str))
        encoders[col] = le
    sess = ort.InferenceSession(MODEL_PATH)

    # Load gateways
    gw_rows = conn.execute("SELECT gateway_id, gateway_alias, processor_name, bank_name, mcc_code FROM gateways WHERE client_id = 1").fetchall()
    gateways = {r['gateway_id']: dict(r) for r in gw_rows}
    active_gws = [gid for gid, g in gateways.items() if g['processor_name'] and not (g['gateway_alias'] or '').startswith('Closed')]

    # Enrich orders with card data
    parent_ids = [o[1] for o in PROCESSED]
    placeholders = ','.join('?' * len(parent_ids))
    card_rows = conn.execute(f"""
        SELECT o.order_id, o.customer_id, o.cc_first_6,
               o.derived_product_role, o.derived_cycle, o.derived_attempt,
               b.issuer_bank, b.card_brand, b.card_type, b.is_prepaid
        FROM orders o LEFT JOIN bin_lookup b ON b.bin = o.cc_first_6
        WHERE o.client_id = 1 AND o.order_id IN ({placeholders})
    """, parent_ids).fetchall()
    card_lookup = {r['order_id']: dict(r) for r in card_rows}

    # Get initial processors
    cust_ids = list(set(card_lookup[pid]['customer_id'] for pid in parent_ids if pid in card_lookup and card_lookup[pid]['customer_id']))
    ph2 = ','.join('?' * len(cust_ids))
    init_rows = conn.execute(f"""
        SELECT o.customer_id, g.processor_name
        FROM orders o JOIN gateways g ON g.client_id = o.client_id AND g.gateway_id = o.processing_gateway_id
        WHERE o.client_id = 1 AND o.derived_product_role = 'main_initial' AND o.order_status IN (2,6,8)
          AND o.customer_id IN ({ph2}) ORDER BY o.acquisition_date ASC
    """, cust_ids).fetchall()
    init_proc_map = {}
    for r in init_rows:
        if r['customer_id'] not in init_proc_map:
            init_proc_map[r['customer_id']] = r['processor_name'].strip().upper() if r['processor_name'] else 'UNKNOWN'

    # Get subscription features from tx_features
    sub_lookup = {}
    for pid in parent_ids:
        row = conn.execute("SELECT * FROM tx_features WHERE client_id = 1 AND sticky_order_id = ?", [pid]).fetchone()
        if row:
            sub_lookup[pid] = dict(row)

    print("=" * 90)
    print("AI MODEL VALIDATION — Already Processed Orders")
    print("=" * 90)

    approved_results = []
    declined_results = []

    for (sub_id, parent_id, cycle, prepaid, status, decline_reason, current_gw, gw_name, price, customer) in PROCESSED:
        card = card_lookup.get(parent_id, {})
        if not card.get('cc_first_6'):
            continue

        sub = sub_lookup.get(parent_id, {})
        cust_id = card.get('customer_id')
        ip = init_proc_map.get(cust_id, 'UNKNOWN')

        # Determine tx_class
        role = card.get('derived_product_role', '')
        attempt = card.get('derived_attempt', 1) or 1
        tx_class = 'salvage'
        if attempt == 1:
            if role == 'main_initial': tx_class = 'initial'
            elif role == 'upsell_initial': tx_class = 'upsell'
            elif role in ('main_rebill', 'upsell_rebill'): tx_class = 'rebill'

        dc = card.get('derived_cycle', 0) or 0
        cycle_depth = 'C0' if dc == 0 else ('C1' if dc == 1 else ('C2' if dc == 2 else 'C3+'))

        issuer = card.get('issuer_bank', 'Unknown') or 'Unknown'
        u = issuer.upper()
        if 'BANK OF AMERICA' in u: issuer = 'BANK OF AMERICA, NATIONAL ASSOCIATION'
        elif 'JPMORGAN' in u: issuer = 'JPMORGAN CHASE BANK N.A.'

        # Score each gateway
        scores = []
        for gw_id in active_gws:
            gw = gateways[gw_id]
            if gw_id in REBILL_EXCLUDED and tx_class in ('rebill', 'salvage', 'cascade'):
                continue

            features = {}
            for col in CATEGORICAL:
                if col == 'processor_name': features[col] = gw['processor_name'].strip().upper()
                elif col == 'acquiring_bank': features[col] = gw.get('bank_name') or 'UNKNOWN'
                elif col == 'mcc_code': features[col] = gw.get('mcc_code') or 'UNKNOWN'
                elif col == 'issuer_bank': features[col] = issuer
                elif col == 'card_brand': features[col] = card.get('card_brand') or 'UNKNOWN'
                elif col == 'card_type': features[col] = card.get('card_type') or 'UNKNOWN'
                elif col == 'tx_class': features[col] = tx_class
                elif col == 'cycle_depth': features[col] = cycle_depth
                elif col == 'prev_decline_reason': features[col] = decline_reason or 'UNKNOWN'
                elif col == 'initial_processor': features[col] = ip

            encoded = []
            for col in CATEGORICAL:
                le = encoders[col]
                val = features[col]
                encoded.append(le.transform([val])[0] if val in le.classes_ else (le.transform(['UNKNOWN'])[0] if 'UNKNOWN' in le.classes_ else 0))

            for col in NUMERICAL:
                if col == 'is_prepaid': encoded.append(card.get('is_prepaid', 0) or 0)
                elif col == 'amount': encoded.append(price)
                elif col == 'attempt_number': encoded.append(attempt)
                elif col == 'hour_of_day': encoded.append(12)
                elif col == 'day_of_week': encoded.append(5)
                elif col == 'mid_velocity_daily': encoded.append(200)
                elif col == 'mid_velocity_weekly': encoded.append(1400)
                elif col == 'customer_history_on_proc': encoded.append(1)
                elif col == 'bin_velocity_weekly': encoded.append(100)
                elif col == 'consecutive_approvals': encoded.append(sub.get('consecutive_approvals', 0) or 0)
                elif col == 'days_since_last_charge': encoded.append(sub.get('days_since_last_charge', 0) or 0)
                elif col == 'days_since_initial': encoded.append(sub.get('days_since_initial', 0) or 0)
                elif col == 'lifetime_charges': encoded.append(sub.get('lifetime_charges', 0) or 0)
                elif col == 'lifetime_revenue': encoded.append(sub.get('lifetime_revenue', 0) or 0)
                elif col == 'initial_amount': encoded.append(sub.get('initial_amount', 0) or 0)
                elif col == 'amount_ratio': encoded.append(sub.get('amount_ratio', 0) or 0)
                elif col == 'prior_declines_in_cycle': encoded.append(sub.get('prior_declines_in_cycle', 0) or 0)

            X = np.array([encoded], dtype=np.float32)
            raw = sess.run(None, {sess.get_inputs()[0].name: X})
            probs = raw[1]
            if isinstance(probs, list) and isinstance(probs[0], dict):
                prob = probs[0].get(1, 0)
            elif hasattr(probs, 'shape') and len(probs.shape) == 2:
                prob = probs[0][1]
            else:
                prob = float(probs[0])

            scores.append((gw_id, gw['gateway_alias'] or str(gw_id), prob))

        scores.sort(key=lambda x: x[2], reverse=True)
        current_score = next((s for s in scores if s[0] == current_gw), None)
        best = scores[0] if scores else None
        current_rank = next((i+1 for i, s in enumerate(scores) if s[0] == current_gw), len(scores))

        entry = {
            'sub_id': sub_id, 'customer': customer, 'status': status,
            'decline_reason': decline_reason, 'price': price,
            'tx_class': tx_class,
            'issuer': (card.get('issuer_bank') or '?')[:25],
            'card': f"{card.get('card_brand','?')}/{card.get('card_type','?')}",
            'current_gw': current_gw, 'gw_name': gw_name,
            'current_pct': current_score[2] * 100 if current_score else 0,
            'current_rank': current_rank,
            'best_gw': best[0] if best else 0,
            'best_name': best[1][:20] if best else '?',
            'best_pct': best[2] * 100 if best else 0,
            'total_gws': len(scores),
        }

        if status == 'Approved':
            approved_results.append(entry)
        else:
            declined_results.append(entry)

    # === APPROVED ===
    print(f"\n{'='*90}")
    print(f"APPROVED ORDERS ({len(approved_results)})")
    print(f"{'='*90}")
    print(f"  {'Customer':<22} {'Card':<14} {'Used GW':<10} {'Used%':>6} {'Rank':>5} {'AI Best GW':<10} {'Best%':>6} {'Match':>6}")
    print(f"  {'-'*82}")
    for r in approved_results:
        match = 'YES' if r['current_gw'] == r['best_gw'] else f"#{r['current_rank']}"
        print(f"  {r['customer']:<22} {r['card']:<14} {r['gw_name']:<10} {r['current_pct']:>5.1f}% {r['current_rank']:>4}/{r['total_gws']} "
              f"{r['best_name']:<10} {r['best_pct']:>5.1f}% {match:>6}")

    # === DECLINED ===
    print(f"\n{'='*90}")
    print(f"DECLINED ORDERS ({len(declined_results)})")
    print(f"{'='*90}")

    # Group by decline reason
    for reason in ['Insufficient funds', 'Issuer Declined', 'Do Not Honor', 'Bad Bin or Host Disconnect', 'Activity limit exceeded', 'Account Closed']:
        group = [r for r in declined_results if r['decline_reason'] == reason]
        if not group:
            continue
        print(f"\n  --- {reason} ({len(group)} orders) ---")
        print(f"  {'Customer':<22} {'Used GW':<10} {'Used%':>6} {'Rank':>5} {'AI Best':<10} {'Best%':>6} {'Gap':>7} {'Would Help?':>12}")
        print(f"  {'-'*85}")
        for r in sorted(group, key=lambda x: x['best_pct'] - x['current_pct'], reverse=True):
            gap = r['best_pct'] - r['current_pct']
            already_best = r['current_gw'] == r['best_gw']
            if already_best:
                verdict = 'BEST USED'
            elif gap > 10:
                verdict = 'BIG MISS'
            elif gap > 3:
                verdict = 'COULD HELP'
            else:
                verdict = 'MARGINAL'
            print(f"  {r['customer']:<22} {r['gw_name']:<10} {r['current_pct']:>5.1f}% {r['current_rank']:>4}/{r['total_gws']} "
                  f"{r['best_name']:<10} {r['best_pct']:>5.1f}% {gap:>+6.1f}% {verdict:>12}")

    # === SUMMARY ===
    already_best_count = sum(1 for r in declined_results if r['current_gw'] == r['best_gw'])
    big_miss = sum(1 for r in declined_results if (r['best_pct'] - r['current_pct']) > 10)
    could_help = sum(1 for r in declined_results if 3 < (r['best_pct'] - r['current_pct']) <= 10)
    marginal = sum(1 for r in declined_results if 0 < (r['best_pct'] - r['current_pct']) <= 3)

    print(f"\n{'='*90}")
    print(f"SCORECARD")
    print(f"{'='*90}")
    print(f"  Approved orders: {len(approved_results)}")
    ai_top3 = sum(1 for r in approved_results if r['current_rank'] <= 3)
    print(f"    Gateway was in AI's top 3: {ai_top3}/{len(approved_results)}")
    print(f"\n  Declined orders: {len(declined_results)}")
    print(f"    Already on best gateway:   {already_best_count}")
    print(f"    AI had MUCH better option:  {big_miss} (>10pp gap)")
    print(f"    AI had better option:       {could_help} (3-10pp gap)")
    print(f"    Marginal difference:        {marginal} (<3pp gap)")
    print(f"    = Routing couldn't help:    {already_best_count + marginal}")
    print(f"    = Routing COULD have helped: {big_miss + could_help}")

    avg_gap = np.mean([r['best_pct'] - r['current_pct'] for r in declined_results if r['current_gw'] != r['best_gw']])
    print(f"\n    Avg gap when AI disagrees: {avg_gap:.1f}pp")

    conn.close()
    print("\nDone!")


if __name__ == '__main__':
    main()
