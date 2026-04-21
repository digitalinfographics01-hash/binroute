"""
Pre-training data audit — run on server before ML training.
Checks data quality, feature completeness, exclusions, and potential biases.
"""
import sqlite3
import sys
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
for arg in sys.argv:
    if arg.startswith('--db='):
        DB_PATH = arg.split('=', 1)[1]

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

print("=" * 75)
print("  BinRoute Pre-Training Data Audit")
print("=" * 75)
print(f"  DB: {os.path.abspath(DB_PATH)}")

# ── 1. Transaction attempts overview ──
print("\n[1] Transaction Attempts Overview")
c.execute("SELECT COUNT(*) FROM transaction_attempts")
total = c.fetchone()[0]
print(f"  Total rows: {total:,}")

c.execute("SELECT client_id, COUNT(*) as cnt FROM transaction_attempts GROUP BY client_id ORDER BY client_id")
print("\n  By client:")
for row in c.fetchall():
    print(f"    Client {row[0]}: {row[1]:,}")

c.execute("SELECT feature_version, COUNT(*) FROM transaction_attempts GROUP BY feature_version ORDER BY feature_version")
print("\n  By feature_version:")
for row in c.fetchall():
    print(f"    v{row[0]}: {row[1]:,}")

# ── 2. Model target distribution ──
print("\n[2] Model Target Distribution")
c.execute("SELECT model_target, COUNT(*) as cnt, ROUND(100.0*SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr FROM transaction_attempts WHERE feature_version >= 3 GROUP BY model_target ORDER BY cnt DESC")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]:,} ({row[2]}% approved)")

# ── 3. Derived product role distribution ──
print("\n[3] Derived Product Role Distribution")
c.execute("SELECT derived_product_role, COUNT(*) as cnt FROM transaction_attempts WHERE feature_version >= 3 GROUP BY derived_product_role ORDER BY cnt DESC")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]:,}")

# ── 4. Excluded gateways check ──
print("\n[4] Excluded Gateways in Training Data")
c.execute("""
    SELECT ta.gateway_id, g.gateway_descriptor, g.exclude_from_analysis, COUNT(*) as cnt
    FROM transaction_attempts ta
    LEFT JOIN gateways g ON ta.gateway_id = g.gateway_id AND ta.client_id = g.client_id
    WHERE g.exclude_from_analysis = 1
    GROUP BY ta.gateway_id, g.gateway_descriptor
    ORDER BY cnt DESC
""")
rows = c.fetchall()
if rows:
    print("  WARNING — excluded gateways found in transaction_attempts:")
    for row in rows:
        print(f"    Gateway {row[0]} ({row[1]}): {row[3]:,} rows")
else:
    print("  OK — no excluded gateways in training data")

# ── 5. Payfac check ──
print("\n[5] Payfac (Gateway 192) Check")
c.execute("SELECT client_id, COUNT(*) FROM transaction_attempts WHERE gateway_id = 192 GROUP BY client_id")
rows = c.fetchall()
if rows:
    print("  WARNING — Payfac rows in transaction_attempts:")
    for row in rows:
        print(f"    Client {row[0]}: {row[1]:,}")
else:
    print("  OK — no Payfac rows")

c.execute("SELECT initial_was_payfac, COUNT(*) FROM transaction_attempts WHERE feature_version >= 3 GROUP BY initial_was_payfac")
print("\n  initial_was_payfac distribution:")
for row in c.fetchall():
    print(f"    {row[0]}: {row[1]:,}")

# ── 6. Decline reason classes ──
print("\n[6] Decline Reason Classes")
c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='decline_reason_classes'")
if c.fetchone():
    c.execute("SELECT decline_class, COUNT(*) FROM decline_reason_classes GROUP BY decline_class ORDER BY COUNT(*) DESC")
    print("  Classes defined:")
    for row in c.fetchall():
        print(f"    {row[0]}: {row[1]:,} reasons")

    c.execute("""
        SELECT drc.decline_class, COUNT(*) as cnt
        FROM transaction_attempts ta
        JOIN decline_reason_classes drc ON ta.decline_reason = drc.decline_reason
        WHERE ta.feature_version >= 3 AND ta.outcome = 'declined'
        GROUP BY drc.decline_class ORDER BY cnt DESC
    """)
    print("\n  Declined attempts by class:")
    for row in c.fetchall():
        print(f"    {row[0]}: {row[1]:,}")

    c.execute("""
        SELECT COUNT(*) FROM transaction_attempts ta
        JOIN decline_reason_classes drc ON ta.decline_reason = drc.decline_reason
        WHERE ta.feature_version >= 3 AND drc.decline_class IN ('customer_input', 'system_decline')
    """)
    to_filter = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM transaction_attempts WHERE feature_version >= 3 AND model_target != 'excluded'")
    trainable = c.fetchone()[0]
    print(f"\n  Will filter (customer_input + system_decline): {to_filter:,} of {trainable:,} ({100*to_filter/trainable:.1f}%)")
else:
    print("  WARNING — decline_reason_classes table does NOT exist")
    print("  customer_input/system_decline filtering will NOT work")

# ── 7. NULL feature check ──
print("\n[7] NULL Feature Check (feature_version >= 3)")
critical_features = [
    'processor_name', 'acquiring_bank', 'issuer_bank', 'card_brand', 'card_type',
    'mid_age_days', 'mid_velocity_daily', 'mid_velocity_weekly',
    'bin_velocity_weekly', 'customer_history_on_proc',
    'initial_processor', 'last_approved_processor', 'parent_declined_processor',
    'model_target', 'derived_product_role', 'outcome',
]
for feat in critical_features:
    c.execute(f"SELECT COUNT(*) FROM transaction_attempts WHERE feature_version >= 3 AND {feat} IS NULL")
    nulls = c.fetchone()[0]
    if nulls > 0:
        pct = 100 * nulls / total
        flag = "WARNING" if pct > 5 else "note"
        print(f"  {flag}: {feat} has {nulls:,} NULLs ({pct:.1f}%)")

# ── 8. Test BINs check ──
print("\n[8] Test BIN Check")
test_bins = ['144444', '777777', '444444', '411111', '000000', '666666', '518426']
c.execute(f"SELECT COUNT(*) FROM transaction_attempts WHERE cc_first_6 IN ({','.join('?' for _ in test_bins)})", test_bins)
test_count = c.fetchone()[0]
if test_count > 0:
    print(f"  WARNING — {test_count:,} test BIN rows in transaction_attempts")
    c.execute(f"SELECT cc_first_6, COUNT(*) FROM transaction_attempts WHERE cc_first_6 IN ({','.join('?' for _ in test_bins)}) GROUP BY cc_first_6", test_bins)
    for row in c.fetchall():
        print(f"    BIN {row[0]}: {row[1]:,}")
else:
    print("  OK — no test BINs")

# ── 9. Date range per client ──
print("\n[9] Date Range Per Client")
c.execute("""
    SELECT client_id, MIN(acquisition_date) as first, MAX(acquisition_date) as last, COUNT(*) as cnt
    FROM transaction_attempts WHERE feature_version >= 3
    GROUP BY client_id ORDER BY client_id
""")
for row in c.fetchall():
    print(f"  Client {row[0]}: {row[1][:10]} to {row[2][:10]} ({row[3]:,} rows)")

# ── 10. Approval rate by client x model_target ──
print("\n[10] Approval Rate by Client x Model Target")
c.execute("""
    SELECT client_id, model_target,
        COUNT(*) as cnt,
        ROUND(100.0*SUM(CASE WHEN outcome='approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr
    FROM transaction_attempts WHERE feature_version >= 3 AND model_target != 'excluded'
    GROUP BY client_id, model_target ORDER BY client_id, model_target
""")
print(f"  {'Client':>8} {'Target':>18} {'Count':>10} {'Appr%':>7}")
print(f"  {'-'*48}")
for row in c.fetchall():
    print(f"  {row[0]:>8} {row[1]:>18} {row[2]:>10,} {row[3]:>6.1f}%")

# ── 11. Upsell data check ──
print("\n[11] Upsell Rows Per Client")
c.execute("""
    SELECT client_id,
        SUM(CASE WHEN derived_product_role LIKE '%upsell%' THEN 1 ELSE 0 END) as upsell_rows,
        COUNT(*) as total
    FROM transaction_attempts WHERE feature_version >= 3
    GROUP BY client_id ORDER BY client_id
""")
for row in c.fetchall():
    pct = 100 * row[1] / row[2] if row[2] else 0
    print(f"  Client {row[0]}: {row[1]:,} upsell rows ({pct:.1f}% of {row[2]:,})")

# ── 12. Duplicate check ──
print("\n[12] Duplicate Check")
c.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id || '-' || attempt_seq) FROM transaction_attempts WHERE feature_version >= 3")
dupes = c.fetchone()[0]
if dupes > 0:
    print(f"  WARNING — {dupes:,} duplicate order_id+attempt_seq combinations")
else:
    print("  OK — no duplicates")

print("\n" + "=" * 75)
print("  Audit Complete")
print("=" * 75)

conn.close()
