"""
BinRoute AI — A/B Test Setup

Splits the 30 SWITCH orders into:
  - 20 TREATMENT (use AI gateway)
  - 10 CONTROL (keep original gateway)

Selection: stratified by lift size so both groups have a mix of
high-lift and low-lift orders. This prevents bias.

Outputs:
  - data/ab_test_plan.csv — full tracking sheet
  - Prints which orders to change and which to leave alone

Usage: py -3 scripts/ml/build_ab_test.py
"""

import os, csv, json
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data')
INPUT_CSV = os.path.join(DATA_DIR, 'queued_orders_dual_model.csv')
OUTPUT_CSV = os.path.join(DATA_DIR, 'ab_test_plan.csv')
OUTPUT_JSON = os.path.join(DATA_DIR, 'models', 'ab_test_snapshot.json')

def main():
    print("=" * 70)
    print("BinRoute AI — A/B Test Setup")
    print("=" * 70)

    # Load recommendations
    rows = []
    with open(INPUT_CSV, 'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    switch_orders = [r for r in rows if r['Action'] == 'SWITCH']
    keep_orders = [r for r in rows if r['Action'] == 'KEEP']

    print(f"\n  Total orders: {len(rows)}")
    print(f"  KEEP (AI agrees): {len(keep_orders)}")
    print(f"  SWITCH (AI disagrees): {len(switch_orders)}")

    # Sort SWITCH by lift descending
    switch_orders.sort(key=lambda x: float(x['Lift pp']), reverse=True)

    # Stratified split: alternate assignment to ensure both groups
    # get a mix of high-lift and low-lift orders
    treatment = []  # Use AI gateway
    control = []    # Keep original gateway

    for i, order in enumerate(switch_orders):
        # Every 3rd order goes to control (gives ~10 control, ~20 treatment)
        if i % 3 == 2:
            control.append(order)
        else:
            treatment.append(order)

    print(f"\n  TREATMENT (AI gateway): {len(treatment)} orders")
    print(f"  CONTROL (keep original): {len(control)} orders")

    # Treatment avg lift vs control avg lift (should be similar if stratified well)
    t_avg = sum(float(r['Lift pp']) for r in treatment) / len(treatment)
    c_avg = sum(float(r['Lift pp']) for r in control) / len(control)
    print(f"  Treatment avg predicted lift: {t_avg:.1f}pp")
    print(f"  Control avg predicted lift: {c_avg:.1f}pp (not applied)")

    # === TREATMENT: Orders to change ===
    print(f"\n{'='*70}")
    print(f"TREATMENT — Change these {len(treatment)} orders to AI gateway:")
    print(f"{'='*70}")
    print(f"\n  {'Sub ID':<10} {'Customer':<22} {'FROM GW':<10} {'TO GW':<10} {'Lift':>7}")
    print(f"  {'-'*62}")
    for r in treatment:
        print(f"  {r['Sub ID']:<10} {r['Customer']:<22} {r['Current GW ID']:<10} {r['AI Best GW ID']:<10} {float(r['Lift pp']):>+6.1f}%")

    # === CONTROL: Orders to leave alone ===
    print(f"\n{'='*70}")
    print(f"CONTROL — Leave these {len(control)} orders on original gateway:")
    print(f"{'='*70}")
    print(f"\n  {'Sub ID':<10} {'Customer':<22} {'Keep GW':<10} {'AI Would':>10} {'Missed Lift':>12}")
    print(f"  {'-'*68}")
    for r in control:
        print(f"  {r['Sub ID']:<10} {r['Customer']:<22} {r['Current GW ID']:<10} {r['AI Best GW ID']:>10} {float(r['Lift pp']):>+11.1f}%")

    # === KEEP orders (no change needed) ===
    print(f"\n{'='*70}")
    print(f"KEEP — No change ({len(keep_orders)} orders, AI agrees with current routing)")
    print(f"{'='*70}")
    print(f"\n  {'Sub ID':<10} {'Customer':<22} {'GW':<10} {'AI %':>7}")
    print(f"  {'-'*52}")
    for r in keep_orders:
        print(f"  {r['Sub ID']:<10} {r['Customer']:<22} {r['Current GW ID']:<10} {float(r['AI Best %']):>6.1f}%")

    # === Build tracking CSV ===
    ab_rows = []
    for r in keep_orders:
        ab_rows.append({
            'sub_id': r['Sub ID'],
            'customer': r['Customer'],
            'card': r['Card'],
            'issuer': r['Issuer'],
            'amount': r['Amount'],
            'tx_class': r['TX Class'],
            'model': r['Model Used'],
            'group': 'KEEP',
            'original_gw_id': r['Current GW ID'],
            'original_gw': r['Current GW'],
            'assigned_gw_id': r['Current GW ID'],
            'assigned_gw': r['Current GW'],
            'ai_best_gw_id': r['AI Best GW ID'],
            'ai_best_gw': r['AI Best GW'],
            'ai_predicted_pct': r['AI Best %'],
            'original_predicted_pct': r['Current %'],
            'lift_pp': r['Lift pp'],
            'actual_outcome': '',
            'actual_gateway_used': '',
            'notes': '',
        })
    for r in treatment:
        ab_rows.append({
            'sub_id': r['Sub ID'],
            'customer': r['Customer'],
            'card': r['Card'],
            'issuer': r['Issuer'],
            'amount': r['Amount'],
            'tx_class': r['TX Class'],
            'model': r['Model Used'],
            'group': 'TREATMENT',
            'original_gw_id': r['Current GW ID'],
            'original_gw': r['Current GW'],
            'assigned_gw_id': r['AI Best GW ID'],
            'assigned_gw': r['AI Best GW'],
            'ai_best_gw_id': r['AI Best GW ID'],
            'ai_best_gw': r['AI Best GW'],
            'ai_predicted_pct': r['AI Best %'],
            'original_predicted_pct': r['Current %'],
            'lift_pp': r['Lift pp'],
            'actual_outcome': '',
            'actual_gateway_used': '',
            'notes': '',
        })
    for r in control:
        ab_rows.append({
            'sub_id': r['Sub ID'],
            'customer': r['Customer'],
            'card': r['Card'],
            'issuer': r['Issuer'],
            'amount': r['Amount'],
            'tx_class': r['TX Class'],
            'model': r['Model Used'],
            'group': 'CONTROL',
            'original_gw_id': r['Current GW ID'],
            'original_gw': r['Current GW'],
            'assigned_gw_id': r['Current GW ID'],
            'assigned_gw': r['Current GW'],
            'ai_best_gw_id': r['AI Best GW ID'],
            'ai_best_gw': r['AI Best GW'],
            'ai_predicted_pct': r['AI Best %'],
            'original_predicted_pct': r['Current %'],
            'lift_pp': r['Lift pp'],
            'actual_outcome': '',
            'actual_gateway_used': '',
            'notes': '',
        })

    # Write CSV
    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ab_rows[0].keys())
        writer.writeheader()
        writer.writerows(ab_rows)
    print(f"\n  Tracking CSV: {OUTPUT_CSV}")

    # Save snapshot JSON for automated verification later
    snapshot = {
        'created_at': datetime.now().isoformat(),
        'total_orders': len(rows),
        'treatment_count': len(treatment),
        'control_count': len(control),
        'keep_count': len(keep_orders),
        'treatment_sub_ids': [r['Sub ID'] for r in treatment],
        'control_sub_ids': [r['Sub ID'] for r in control],
        'keep_sub_ids': [r['Sub ID'] for r in keep_orders],
        'treatment_avg_predicted_lift': round(t_avg, 1),
        'control_avg_predicted_lift': round(c_avg, 1),
    }
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(snapshot, f, indent=2)
    print(f"  Snapshot JSON: {OUTPUT_JSON}")

    # === What to measure after processing ===
    print(f"\n{'='*70}")
    print(f"AFTER PROCESSING — How to verify")
    print(f"{'='*70}")
    print(f"""
  Once orders process, run a sync and then compare:

  1. TREATMENT approval rate vs CONTROL approval rate
     - Treatment should be higher if AI routing works
     - With 20 vs 10 orders, even a 2-3 order difference is meaningful

  2. AI calibration: for each order, compare predicted % vs actual outcome
     - Orders with >70% predicted should mostly approve
     - Orders with <30% predicted should mostly decline

  3. The money question:
     - Treatment: {len(treatment)} orders, avg lift {t_avg:.1f}pp
     - If AI is right, ~{int(len(treatment) * t_avg/100)} more approvals than control rate
""")

    print("Done!")


if __name__ == '__main__':
    main()
