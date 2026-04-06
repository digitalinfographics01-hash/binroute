"""
BinRoute AI — Score Reprocess Candidates

Takes 10 declined cascaded initial orders from Kytsan DB,
scores each across all available gateways using the LightGBM model,
and outputs a table showing AI-recommended gateway + confidence score.

Usage: py -3 scripts/ml/score_reprocess_candidates.py
"""

import os
import sqlite3
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import onnxruntime as ort
import warnings
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models', 'binroute_lightgbm.onnx')

CATEGORICAL_FEATURES = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'tx_class', 'cycle_depth', 'prev_decline_reason',
    'initial_processor',
]
NUMERICAL_FEATURES = [
    'is_prepaid', 'amount', 'attempt_number',
    'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'consecutive_approvals', 'days_since_last_charge',
    'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
    'initial_amount', 'amount_ratio', 'prior_declines_in_cycle',
]


def main():
    print("=" * 90)
    print("BinRoute AI — Reprocess Candidate Scoring")
    print("=" * 90)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get 10 declined cascaded initials under $10 with both gateways known
    orders = conn.execute("""
        SELECT o.order_id, o.acquisition_date, o.order_total, o.order_status,
               o.gateway_id, o.original_gateway_id, o.cc_first_6, o.cc_type,
               o.decline_reason, o.original_decline_reason, o.billing_cycle,
               o.decline_category, o.prepaid, o.customer_id, o.campaign_id,
               o.derived_cycle, o.derived_attempt, o.derived_product_role,
               o.processing_gateway_id,
               g.gateway_alias, g.processor_name as cascade_processor,
               g2.gateway_alias as original_gw_alias, g2.processor_name as original_processor,
               b.issuer_bank, b.card_brand, b.card_type as bin_card_type, b.is_prepaid as bin_is_prepaid
        FROM orders o
        LEFT JOIN gateways g ON o.gateway_id = g.gateway_id AND o.client_id = g.client_id
        LEFT JOIN gateways g2 ON o.original_gateway_id = g2.gateway_id AND o.client_id = g2.client_id
        LEFT JOIN bin_lookup b ON b.bin = o.cc_first_6
        WHERE o.is_cascaded = 1
          AND o.original_gateway_id IS NOT NULL
          AND o.order_status = 7
          AND o.order_total < 10
          AND o.client_id = 1
          AND o.billing_cycle = 0
        ORDER BY o.acquisition_date DESC
        LIMIT 10
    """).fetchall()

    orders = [dict(o) for o in orders]
    print(f"\n  Found {len(orders)} declined cascaded orders under $10")

    # Load model + encoders
    print("  Loading AI model...")
    sess, encoders, all_txf = load_model_and_encoders(conn)

    # Load gateway metadata
    gateways = load_gateways(conn)
    active_gw_ids = [gw_id for gw_id, gw in gateways.items()
                     if gw['processor_name'] and not gw['gateway_alias'].startswith('Closed')]
    print(f"  Active gateways: {len(active_gw_ids)}")

    # Get initial processor for each customer
    enrich_initial_processor(orders, conn)

    # Get subscription features
    enrich_subscription_features(orders, conn)

    # Score each order
    print("\n" + "=" * 90)
    print(f"  {'Order':<8} {'BIN':>6} {'Card':>5} {'Decline Reason':<25} {'Orig GW':<12} {'Cascade GW':<12} {'AI Best GW':<12} {'AI Prob':>7} {'Verdict'}")
    print("  " + "-" * 100)

    results_table = []
    for o in orders:
        result = score_order(o, gateways, active_gw_ids, sess, encoders)
        results_table.append(result)

        verdict = "RETRY" if result['best_prob'] >= 0.30 else "MAYBE" if result['best_prob'] >= 0.15 else "SKIP"

        print(f"  {o['order_id']:<8} {o['cc_first_6']:>6} {(o['cc_type'] or '?'):>5} "
              f"{(o['decline_reason'] or '?'):<25} "
              f"{result['orig_gw_short']:<12} {result['cascade_gw_short']:<12} "
              f"{result['best_gw_short']:<12} {result['best_prob']*100:5.1f}%  {verdict}")

    # Detailed breakdown
    print("\n" + "=" * 90)
    print("DETAILED AI SCORING PER ORDER")
    print("=" * 90)

    for i, (o, r) in enumerate(zip(orders, results_table)):
        print(f"\n  [{i+1}] Order {o['order_id']} — ${o['order_total']:.2f} — BIN {o['cc_first_6']} ({o['cc_type']})")
        print(f"      Issuer: {o.get('issuer_bank', 'Unknown')}")
        print(f"      Original: {o['original_gw_alias']} — declined: {o['original_decline_reason']}")
        print(f"      Cascade:  {o['gateway_alias']} — declined: {o['decline_reason']}")
        print(f"      Both tried: {r['orig_gw_short']}, {r['cascade_gw_short']} — both failed")
        print(f"      AI Top 5 gateways:")
        for j, gw_score in enumerate(r['all_scores'][:5]):
            tried = ""
            if gw_score['gw_id'] == o['original_gateway_id']:
                tried = " (ORIG - already failed)"
            elif gw_score['gw_id'] == o['gateway_id']:
                tried = " (CASCADE - already failed)"
            print(f"        {j+1}. [{gw_score['gw_id']}] {gw_score['gw_name']:<40} {gw_score['prob']*100:5.1f}%{tried}")

        # Best UNTRIED gateway
        untried = [s for s in r['all_scores']
                   if s['gw_id'] != o['original_gateway_id'] and s['gw_id'] != o['gateway_id']]
        if untried:
            best_untried = untried[0]
            print(f"      >>> AI RECOMMENDS: [{best_untried['gw_id']}] {best_untried['gw_name']} at {best_untried['prob']*100:.1f}% confidence")

    # Summary
    print("\n" + "=" * 90)
    print("REPROCESS RECOMMENDATION SUMMARY")
    print("=" * 90)

    retry_orders = [r for r in results_table if r['best_untried_prob'] >= 0.30]
    maybe_orders = [r for r in results_table if 0.15 <= r['best_untried_prob'] < 0.30]
    skip_orders = [r for r in results_table if r['best_untried_prob'] < 0.15]

    print(f"\n  RETRY (>=30% confidence): {len(retry_orders)} orders")
    print(f"  MAYBE (15-30%):           {len(maybe_orders)} orders")
    print(f"  SKIP  (<15%):             {len(skip_orders)} orders")

    if retry_orders:
        print(f"\n  Orders to reprocess:")
        print(f"  {'Order':<8} {'Amount':>7} {'Best Untried GW':<35} {'Confidence':>10}")
        print(f"  " + "-" * 65)
        for r in sorted(retry_orders, key=lambda x: x['best_untried_prob'], reverse=True):
            print(f"  {r['order_id']:<8} ${r['amount']:>5.2f} "
                  f"{r['best_untried_gw']:<35} {r['best_untried_prob']*100:>8.1f}%")

    conn.close()
    print("\nDone!")


def load_model_and_encoders(conn):
    all_txf = pd.read_sql_query("""
        SELECT * FROM tx_features WHERE feature_version >= 2
        ORDER BY acquisition_date ASC
    """, conn)

    encoders = {}
    for col in CATEGORICAL_FEATURES:
        le = LabelEncoder()
        le.fit(all_txf[col].fillna('UNKNOWN').astype(str))
        encoders[col] = le

    sess = ort.InferenceSession(MODEL_PATH)
    return sess, encoders, all_txf


def load_gateways(conn):
    rows = conn.execute("""
        SELECT gateway_id, gateway_alias, processor_name, bank_name, mcc_code
        FROM gateways WHERE client_id = 1
    """).fetchall()
    return {r['gateway_id']: dict(r) for r in rows}


def enrich_initial_processor(orders, conn):
    cust_ids = list(set(o.get('customer_id') for o in orders if o.get('customer_id')))
    if not cust_ids:
        return
    placeholders = ','.join('?' * len(cust_ids))
    rows = conn.execute(f"""
        SELECT o.customer_id, g.processor_name
        FROM orders o
        JOIN gateways g ON g.client_id = o.client_id AND g.gateway_id = o.processing_gateway_id
        WHERE o.client_id = 1
          AND o.derived_product_role = 'main_initial'
          AND o.order_status IN (2, 6, 8)
          AND o.customer_id IN ({placeholders})
        ORDER BY o.acquisition_date ASC
    """, cust_ids).fetchall()
    init_proc = {}
    for r in rows:
        if r['customer_id'] not in init_proc:
            init_proc[r['customer_id']] = r['processor_name']
    for o in orders:
        cid = o.get('customer_id')
        if cid and cid in init_proc:
            o['initial_processor'] = init_proc[cid]


def enrich_subscription_features(orders, conn):
    for o in orders:
        row = conn.execute("""
            SELECT consecutive_approvals, days_since_last_charge, days_since_initial,
                   lifetime_charges, lifetime_revenue, initial_amount, amount_ratio,
                   prior_declines_in_cycle
            FROM tx_features
            WHERE client_id = 1 AND sticky_order_id = ?
        """, [o['order_id']]).fetchone()
        if row:
            for key in ['consecutive_approvals', 'days_since_last_charge', 'days_since_initial',
                        'lifetime_charges', 'lifetime_revenue', 'initial_amount', 'amount_ratio',
                        'prior_declines_in_cycle']:
                o[key] = row[key] or 0


def score_order(order, gateways, active_gw_ids, sess, encoders):
    from datetime import datetime

    try:
        dt = datetime.strptime(order['acquisition_date'], '%Y-%m-%d %H:%M:%S')
        hour = dt.hour
        dow = (dt.weekday() + 1) % 7
    except:
        hour = 12
        dow = 0

    # These are initial orders (billing_cycle=0), cascaded and declined
    tx_class = 'initial'
    cycle_depth = 'C0'
    attempt_num = 2  # already tried twice (original + cascade)

    issuer = order.get('issuer_bank', 'Unknown') or 'Unknown'
    u_issuer = issuer.upper()
    if 'BANK OF AMERICA' in u_issuer:
        issuer = 'BANK OF AMERICA, NATIONAL ASSOCIATION'
    elif 'CITIBANK' in u_issuer or 'CITI BANK' in u_issuer:
        issuer = 'CITIBANK N.A.'
    elif 'JPMORGAN' in u_issuer or 'JP MORGAN' in u_issuer:
        issuer = 'JPMORGAN CHASE BANK N.A.'

    prev_decline = order.get('decline_reason') or 'UNKNOWN'
    init_proc = order.get('initial_processor', 'UNKNOWN') or 'UNKNOWN'
    if init_proc != 'UNKNOWN':
        init_proc = init_proc.strip().upper()

    results = []
    for gw_id in active_gw_ids:
        gw = gateways.get(gw_id)
        if not gw or not gw['processor_name']:
            continue

        proc = gw['processor_name'].strip().upper()
        bank = gw.get('bank_name') or 'UNKNOWN'
        mcc = gw.get('mcc_code') or 'UNKNOWN'

        features = {
            'processor_name': proc,
            'acquiring_bank': bank,
            'mcc_code': mcc,
            'issuer_bank': issuer,
            'card_brand': order.get('card_brand', 'UNKNOWN') or 'UNKNOWN',
            'card_type': order.get('bin_card_type', 'UNKNOWN') or 'UNKNOWN',
            'tx_class': tx_class,
            'cycle_depth': cycle_depth,
            'prev_decline_reason': prev_decline,
            'initial_processor': init_proc,
            'is_prepaid': order.get('bin_is_prepaid', 0) or 0,
            'amount': order['order_total'],
            'attempt_number': attempt_num,
            'hour_of_day': hour,
            'day_of_week': dow,
            'mid_velocity_daily': 200,
            'mid_velocity_weekly': 1400,
            'customer_history_on_proc': 1,
            'bin_velocity_weekly': 100,
            'consecutive_approvals': order.get('consecutive_approvals', 0),
            'days_since_last_charge': order.get('days_since_last_charge', 0),
            'days_since_initial': order.get('days_since_initial', 0),
            'lifetime_charges': order.get('lifetime_charges', 0),
            'lifetime_revenue': order.get('lifetime_revenue', 0),
            'initial_amount': order.get('initial_amount', 0),
            'amount_ratio': order.get('amount_ratio', 0),
            'prior_declines_in_cycle': order.get('prior_declines_in_cycle', 0),
        }

        encoded = []
        for col in CATEGORICAL_FEATURES:
            val = features[col]
            le = encoders[col]
            if val in le.classes_:
                encoded.append(le.transform([val])[0])
            else:
                if 'UNKNOWN' in le.classes_:
                    encoded.append(le.transform(['UNKNOWN'])[0])
                else:
                    encoded.append(0)

        for col in NUMERICAL_FEATURES:
            encoded.append(features[col] or 0)

        X = np.array([encoded], dtype=np.float32)
        raw = sess.run(None, {sess.get_inputs()[0].name: X})
        probs = raw[1]
        if isinstance(probs, list) and isinstance(probs[0], dict):
            prob = probs[0].get(1, probs[0].get('1', 0))
        elif hasattr(probs, 'shape') and len(probs.shape) == 2:
            prob = probs[0][1]
        else:
            prob = float(probs[0])

        results.append({
            'gw_id': gw_id,
            'gw_name': gw.get('gateway_alias', str(gw_id)),
            'processor': proc,
            'prob': prob,
        })

    results.sort(key=lambda x: x['prob'], reverse=True)

    # Short gateway names
    def short_gw(alias):
        if not alias:
            return '?'
        parts = alias.split('_')
        if len(parts) >= 2:
            return f"{parts[1]}({parts[-1].rstrip(')')})"
        return alias[:15]

    orig_gw_short = short_gw(order.get('original_gw_alias'))
    cascade_gw_short = short_gw(order.get('gateway_alias'))

    best = results[0] if results else None
    best_gw_short = short_gw(best['gw_name']) if best else '?'

    # Best untried
    untried = [s for s in results
               if s['gw_id'] != order['original_gateway_id'] and s['gw_id'] != order['gateway_id']]
    best_untried = untried[0] if untried else None

    return {
        'order_id': order['order_id'],
        'amount': order['order_total'],
        'orig_gw_short': orig_gw_short,
        'cascade_gw_short': cascade_gw_short,
        'best_gw_short': best_gw_short,
        'best_prob': best['prob'] if best else 0,
        'best_untried_gw': best_untried['gw_name'] if best_untried else 'N/A',
        'best_untried_gw_id': best_untried['gw_id'] if best_untried else None,
        'best_untried_prob': best_untried['prob'] if best_untried else 0,
        'all_scores': results,
    }


if __name__ == '__main__':
    main()
