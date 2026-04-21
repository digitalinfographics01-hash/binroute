#!/usr/bin/env python3
"""
P4.2 — Training-vs-inference feature distribution audit.

Compares the mean/std of each numerical feature between:
  - Training data (from transaction_attempts)
  - Recent inference snapshots (from shadow_decisions.feature_snapshot_json)

Flags any feature where |train_mean - infer_mean| / train_std > 0.5
(i.e., inference distribution has shifted by more than half a standard deviation).

Usage:
  python3 scripts/ml/audit-feature-distributions.py [--db data/binroute.db]

Exit code 1 if any feature is flagged.
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = ROOT / 'data' / 'binroute.db'

# Numerical features the initial model uses (post-P1.1: customer_history_on_proc removed).
INITIAL_NUMERICAL = [
    'is_prepaid', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily',
    'bin_velocity_weekly', 'mid_age_days',
    'bin_approval_rate', 'bin_proc_approval_rate',
    'te_acquiring_bank', 'bin_approval_7d', 'bin_approval_30d',
    'amount_vs_bin_avg', 'is_near_payday',
]

SHIFT_THRESHOLD = 0.5  # flag if |delta_mean| / train_std > this


def load_training_stats(conn):
    """Compute mean/std per feature from transaction_attempts (feature_version >= 3, initial only)."""
    df = pd.read_sql_query(
        """
        SELECT mid_velocity_daily, bin_velocity_weekly, mid_age_days,
               bin_approval_rate, bin_proc_approval_rate, te_acquiring_bank,
               bin_approval_7d, bin_approval_30d, amount_vs_bin_avg,
               is_prepaid, hour_of_day, day_of_week, is_near_payday
          FROM transaction_attempts
         WHERE feature_version >= 3
           AND model_target = 'initial'
           AND derived_product_role LIKE '%main%'
        """,
        conn,
    )
    if df.empty:
        print('ERROR: No training rows found in transaction_attempts')
        sys.exit(1)

    stats = {}
    for col in INITIAL_NUMERICAL:
        if col not in df.columns:
            print(f'  WARN: {col} not in transaction_attempts — skipping')
            continue
        vals = pd.to_numeric(df[col], errors='coerce').dropna()
        stats[col] = {
            'mean': float(vals.mean()) if len(vals) > 0 else 0.0,
            'std': float(vals.std()) if len(vals) > 1 else 1.0,
            'count': len(vals),
        }
    return stats


def load_inference_stats(conn, min_rows=10):
    """Extract feature values from recent shadow_decisions feature_snapshot_json."""
    rows = conn.execute(
        """
        SELECT feature_snapshot_json
          FROM shadow_decisions
         WHERE feature_snapshot_json IS NOT NULL
         ORDER BY request_received_at DESC
         LIMIT 200
        """
    ).fetchall()

    if len(rows) < min_rows:
        print(f'  Only {len(rows)} shadow rows with feature_snapshot_json (need {min_rows}+)')
        print('  Skipping inference distribution check — not enough data yet.')
        return None

    # Parse daemon_payload.candidates from each snapshot to get per-candidate features.
    records = []
    for (json_str,) in rows:
        try:
            snap = json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            continue
        payload = snap.get('daemon_payload') or {}
        candidates = payload.get('candidates') or []
        for c in candidates:
            records.append(c)

    if not records:
        print('  No candidate records extracted from snapshots.')
        return None

    df = pd.DataFrame(records)
    stats = {}
    for col in INITIAL_NUMERICAL:
        if col not in df.columns:
            continue
        vals = pd.to_numeric(df[col], errors='coerce').dropna()
        if len(vals) == 0:
            continue
        stats[col] = {
            'mean': float(vals.mean()),
            'std': float(vals.std()) if len(vals) > 1 else 0.0,
            'count': len(vals),
        }
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=str(DEFAULT_DB))
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f'ERROR: DB not found at {db_path}')
        sys.exit(1)

    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)

    print('=== Training feature distributions ===')
    train_stats = load_training_stats(conn)
    for feat, s in train_stats.items():
        print(f'  {feat:30s}  mean={s["mean"]:8.4f}  std={s["std"]:8.4f}  n={s["count"]}')

    print('\n=== Inference feature distributions ===')
    infer_stats = load_inference_stats(conn)

    flagged = []
    if infer_stats:
        for feat, s in infer_stats.items():
            print(f'  {feat:30s}  mean={s["mean"]:8.4f}  std={s["std"]:8.4f}  n={s["count"]}')

        print('\n=== Distribution shift check ===')
        for feat in INITIAL_NUMERICAL:
            if feat not in train_stats or feat not in infer_stats:
                continue
            t = train_stats[feat]
            i = infer_stats[feat]
            if t['std'] == 0:
                shift = 0.0
            else:
                shift = abs(t['mean'] - i['mean']) / t['std']

            status = 'FLAGGED' if shift > SHIFT_THRESHOLD else 'ok'
            if shift > SHIFT_THRESHOLD:
                flagged.append(feat)
            print(f'  {feat:30s}  shift={shift:.3f}  [{status}]')
    else:
        print('  (skipped — insufficient shadow data)')

    conn.close()

    print(f'\n=== Result: {len(flagged)} features flagged ===')
    if flagged:
        for f in flagged:
            print(f'  - {f}')
        print('\nFAILED — feature distributions have shifted beyond threshold.')
        sys.exit(1)
    else:
        print('\nPASSED — all features within tolerance (or insufficient inference data to check).')
        sys.exit(0)


if __name__ == '__main__':
    main()
