"""
Persistent AI scoring daemon for /api/route (Stage 0 shadow mode).

Loads data/models/five_model_initial.pkl once at boot and serves POST /score
requests from the Node routing endpoint on 127.0.0.1:5001. Target latency:
<20 ms p95 per request.

The daemon owns feature engineering. Node passes:
  {
    client_id, bin, amount, sales_type, request_at,
    bin_features: { issuer_bank, card_brand, card_type, is_prepaid },
    candidates: [
      { gateway_id, processor_name, acquiring_bank, mcc_code, mid_age_days }
    ]
  }

The daemon fills in runtime features (BIN approval rates, velocity, time
features, amount_vs_bin_avg, target-encoded acquiring bank, etc.) from its
own cached pandas frames. Returns per-candidate scores:
  { model_version, scores: [{ gateway_id, score }] }

Runtime caches refresh on SIGHUP (or on a POST /reload call). A nightly cron
can signal the daemon after retrain.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from flask import Flask, jsonify, request
from sklearn.preprocessing import LabelEncoder

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MODEL = ROOT / 'data' / 'models' / 'five_model_initial.pkl'
DEFAULT_DB = ROOT / 'data' / 'binroute.db'

# Features the current V1 initial model expects (see data/models/four_model_results.json).
# Note: train_four_models.py treats client_id as CATEGORICAL for the initial model.
# Order matters: training concatenates encoded_categoricals + numerical, so we must
# match that order exactly when building the prediction matrix.
INITIAL_CATEGORICAL = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank_grouped', 'card_brand', 'card_type',
    'billing_state', 'client_id',
]
INITIAL_NUMERICAL = [
    # Order MUST match MODEL_CONFIGS['initial']['numerical'] in train_four_models.py.
    # customer_history_on_proc REMOVED in P1.1 — unknown at BIN-entry time (no
    # customer_id), zeroing it creates train/serve skew worse than losing ~5% signal.
    'is_prepaid', 'hour_of_day', 'day_of_week',
    'mid_velocity_daily',
    'bin_velocity_weekly', 'mid_age_days',
    'bin_approval_rate', 'bin_proc_approval_rate',
    'te_acquiring_bank', 'bin_approval_7d', 'bin_approval_30d',
    'amount_vs_bin_avg', 'is_near_payday',
]
FEATURE_COLUMNS = INITIAL_CATEGORICAL + INITIAL_NUMERICAL

# Top issuer banks by volume — matches the grouping used in training. The
# threshold (min 500 txns) was locked in session 2026-04-13. Everything else
# gets bucketed to 'OTHER'.
# This set is refreshed whenever caches reload.
_TOP_ISSUERS: set[str] = set()

app = Flask(__name__)

_state_lock = threading.RLock()
_state: dict = {
    'model': None,
    'model_version': None,
    'loaded_at': None,
    'feature_order': None,
    'encoders': {},            # col -> sklearn LabelEncoder (categoricals)
    # runtime caches
    'bin_rates': {},          # bin -> {approval_rate, sample_size}
    'bin_proc_rates': {},     # (bin, processor) -> approval_rate
    'bin_avg_amount': {},     # bin -> avg order_total
    'te_acq_bank': {},        # acquiring_bank -> target-encoded approval rate
    'top_issuers': set(),
    # Priors for warming-up MIDs: median mid_age_days / velocity by (proc, bank).
    # Used to substitute when a candidate has is_warming_up=1 or mid_age_days < 30,
    # so the AI doesn't under-rank a new MID just because it's young.
    'mid_age_by_pb': {},      # (processor, bank) -> median age_days of mature MIDs
    'mid_vel_by_pb': {},      # (processor, bank) -> median mid_velocity_daily of mature MIDs
    # P1.2: BIN event timestamps for computing bin_velocity_weekly at request time.
    # bin -> sorted np.array of unix-second timestamps (last 35 days).
    'bin_recent_events': {},
    # P1.4: BIN approval timestamps for computing bin_approval_7d/30d at request time.
    # bin -> sorted np.array of unix-second timestamps for APPROVED orders (last 35 days).
    'bin_recent_approvals': {},
    # P1.3: MID event timestamps for computing mid_velocity_daily at request time.
    # gateway_id -> sorted np.array of unix-second timestamps (last 7 days).
    'mid_recent_events': {},
}


# ---------------------------------------------------------------------------
# Model + cache loading
# ---------------------------------------------------------------------------

def _load_model(model_path: Path) -> None:
    if not model_path.exists():
        raise FileNotFoundError(f'model pickle not found: {model_path}')
    loaded = joblib.load(model_path)
    # train_four_models.py exports a dict like {'model': <booster>, 'features': [...], 'version': '...'}
    # We defensively accept bare model objects too.
    if isinstance(loaded, dict) and 'model' in loaded:
        model = loaded['model']
        features = loaded.get('features') or FEATURE_COLUMNS
        version = loaded.get('version') or model_path.name
    else:
        model = loaded
        features = FEATURE_COLUMNS
        version = model_path.name

    with _state_lock:
        _state['model'] = model
        _state['feature_order'] = list(features)
        _state['model_version'] = f'{version}@{datetime.fromtimestamp(model_path.stat().st_mtime, tz=timezone.utc).isoformat()}'
        _state['loaded_at'] = datetime.now(tz=timezone.utc).isoformat()
    print(f'[scoring_daemon] model loaded: {model_path.name} ({len(features)} features)', flush=True)


def _refresh_caches(db_path: Path) -> None:
    """Build the runtime lookup caches from the SQLite DB. Called on boot and on SIGHUP."""
    if not db_path.exists():
        print(f'[scoring_daemon] WARN: DB not found at {db_path} — caches will be empty', flush=True)
        with _state_lock:
            _state['bin_rates'] = {}
            _state['bin_proc_rates'] = {}
            _state['bin_avg_amount'] = {}
            _state['te_acq_bank'] = {}
            _state['top_issuers'] = set()
        return

    t0 = time.time()
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=5.0)
    try:
        # Build test-filter clause based on which columns actually exist on orders.
        # Older local DBs may be missing is_test_cc; Hostinger prod has both.
        order_cols = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
        filters = []
        if 'is_test_cc' in order_cols:
            filters.append('o.is_test_cc = 0')
        if 'is_internal_test' in order_cols:
            filters.append('o.is_internal_test = 0')
        # ORDER_FILTER references orders alias o.
        test_filter_o = (' AND ' + ' AND '.join(filters)) if filters else ''
        # For queries that don't alias the orders table (standalone FROM orders).
        test_filter_plain = test_filter_o.replace('o.', '')

        # BIN-level approval rate (all time, across all clients) — top-line rate feature.
        bin_rates = pd.read_sql_query(
            f'''
            SELECT bin, approved * 1.0 / NULLIF(total, 0) AS rate, total AS sample_size
              FROM (
                SELECT o.cc_first_6 AS bin,
                       SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) AS approved,
                       COUNT(*) AS total
                  FROM orders o
                 WHERE o.cc_first_6 IS NOT NULL
                   {test_filter_o}
                 GROUP BY o.cc_first_6
                 HAVING total >= 10
              ) s
            ''',
            conn,
        )

        # BIN x processor approval rate.
        bin_proc = pd.read_sql_query(
            f'''
            SELECT o.cc_first_6 AS bin,
                   g.processor_name AS processor,
                   SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) * 1.0 /
                     NULLIF(COUNT(*), 0) AS rate
              FROM orders o
              LEFT JOIN gateways g ON g.client_id = o.client_id AND g.gateway_id = o.gateway_id
             WHERE o.cc_first_6 IS NOT NULL
               AND g.processor_name IS NOT NULL
               {test_filter_o}
             GROUP BY o.cc_first_6, g.processor_name
            HAVING COUNT(*) >= 5
            ''',
            conn,
        )

        # BIN average amount.
        bin_amt = pd.read_sql_query(
            f'''
            SELECT cc_first_6 AS bin, AVG(order_total) AS avg_amount
              FROM orders o
             WHERE cc_first_6 IS NOT NULL
               {test_filter_o}
             GROUP BY cc_first_6
            ''',
            conn,
        )

        # Target-encoded acquiring bank (approval rate per bank).
        te_bank = pd.read_sql_query(
            f'''
            SELECT g.bank_name AS bank,
                   SUM(CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END) * 1.0 /
                     NULLIF(COUNT(*), 0) AS rate
              FROM orders o
              JOIN gateways g ON g.client_id = o.client_id AND g.gateway_id = o.gateway_id
             WHERE g.bank_name IS NOT NULL
               {test_filter_o}
             GROUP BY g.bank_name
            HAVING COUNT(*) >= 100
            ''',
            conn,
        )

        # Per (processor, bank) priors for warming-up MIDs: median mid_age_days
        # and median mid_velocity_daily of MATURE (≥90d old) gateways. When a new
        # MID is scored, we substitute these priors for its low raw values so the
        # AI treats it like an "average" MID on that processor×bank, not a brand
        # new one with zero velocity.
        mid_priors = pd.read_sql_query(
            f'''
            WITH mature AS (
              SELECT g.gateway_id,
                     g.processor_name AS processor,
                     g.bank_name AS bank,
                     CAST(julianday('now') - julianday(g.gateway_created) AS INTEGER) AS age_days
                FROM gateways g
               WHERE g.gateway_created IS NOT NULL
                 AND julianday('now') - julianday(g.gateway_created) >= 90
                 AND (g.exclude_from_analysis = 0 OR g.exclude_from_analysis IS NULL)
            ),
            vel AS (
              SELECT gateway_id,
                     client_id,
                     COUNT(*) * 1.0 /
                       NULLIF(julianday(MAX(acquisition_date)) - julianday(MIN(acquisition_date)), 0) AS v_daily
                FROM orders o
               WHERE gateway_id IS NOT NULL
                 AND acquisition_date IS NOT NULL
                 {test_filter_plain}
               GROUP BY gateway_id, client_id
               HAVING COUNT(*) >= 30
            )
            SELECT m.processor, m.bank, m.age_days, COALESCE(v.v_daily, 0) AS v_daily
              FROM mature m
              LEFT JOIN vel v ON v.gateway_id = m.gateway_id
            ''',
            conn,
        )

        # Top issuer banks (volume >= 500) for issuer_bank_grouped.
        top_iss = pd.read_sql_query(
            f'''
            SELECT b.issuer_bank AS issuer, COUNT(*) AS n
              FROM orders o
              JOIN bin_lookup b ON b.bin = o.cc_first_6
             WHERE b.issuer_bank IS NOT NULL
               {test_filter_o}
             GROUP BY b.issuer_bank
            HAVING n >= 500
            ''',
            conn,
        )

        # P1.2 + P1.4: Recent BIN events for bin_velocity_weekly and bin_approval_7d/30d.
        # Pull last 35 days of orders with timestamps. We store two caches:
        #   bin_recent_events   — all attempts (for velocity count)
        #   bin_recent_approvals — approved only (for windowed approval rates)
        # At request time: np.searchsorted gives O(log n) window counts.
        bin_events_df = pd.read_sql_query(
            f'''
            SELECT o.cc_first_6 AS bin,
                   strftime('%s', o.acquisition_date) AS ts,
                   CASE WHEN o.order_status IN (2,6,8) THEN 1 ELSE 0 END AS approved
              FROM orders o
             WHERE o.cc_first_6 IS NOT NULL
               AND o.acquisition_date >= datetime('now', '-35 days')
               AND o.acquisition_date IS NOT NULL
               {test_filter_o}
             ORDER BY o.cc_first_6, o.acquisition_date
            ''',
            conn,
        )
        bin_events_df['ts'] = pd.to_numeric(bin_events_df['ts'], errors='coerce')
        bin_events_df = bin_events_df.dropna(subset=['ts'])

        # Build per-BIN sorted arrays of timestamps.
        bin_recent_events: dict[str, np.ndarray] = {}
        bin_recent_approvals: dict[str, np.ndarray] = {}
        if not bin_events_df.empty:
            for bin_val, grp in bin_events_df.groupby('bin'):
                ts_arr = grp['ts'].values.astype(np.float64)
                bin_recent_events[bin_val] = ts_arr  # already sorted by ORDER BY
                approved_mask = grp['approved'].values == 1
                if approved_mask.any():
                    bin_recent_approvals[bin_val] = ts_arr[approved_mask]

        # P1.3: MID event timestamps for mid_velocity_daily at request time.
        # Training semantics: count of same-day same-MID attempts. We cache the
        # last 7 days of per-gateway timestamps and count today's events at request time.
        mid_events_df = pd.read_sql_query(
            f'''
            SELECT o.gateway_id AS gw,
                   strftime('%s', o.acquisition_date) AS ts
              FROM orders o
             WHERE o.gateway_id IS NOT NULL
               AND o.acquisition_date >= datetime('now', '-7 days')
               AND o.acquisition_date IS NOT NULL
               {test_filter_o}
             ORDER BY o.gateway_id, o.acquisition_date
            ''',
            conn,
        )
        mid_events_df['ts'] = pd.to_numeric(mid_events_df['ts'], errors='coerce')
        mid_events_df = mid_events_df.dropna(subset=['ts'])

        mid_recent_events: dict[int, np.ndarray] = {}
        if not mid_events_df.empty:
            for gw_id, grp in mid_events_df.groupby('gw'):
                mid_recent_events[int(gw_id)] = grp['ts'].values.astype(np.float64)

        # Rebuild label encoders for categorical columns by pulling distinct values
        # from tx_features (same source + preprocessing training used). sklearn's
        # LabelEncoder sorts input via np.unique, so as long as we feed the same
        # distinct set, we reproduce training's label mapping deterministically.
        encoder_cols_in_tx = ['processor_name', 'acquiring_bank', 'mcc_code',
                              'issuer_bank', 'card_brand', 'card_type',
                              'billing_state', 'client_id']
        encoders = {}
        tx_cols = {row[1] for row in conn.execute("PRAGMA table_info(tx_features)").fetchall()}
        for col in encoder_cols_in_tx:
            if col not in tx_cols:
                continue
            vals = pd.read_sql_query(
                f'SELECT DISTINCT {col} AS v FROM tx_features',
                conn,
            )['v']
            vals = vals.fillna('UNKNOWN').astype(str)
            vals = pd.concat([vals, pd.Series(['UNKNOWN'])], ignore_index=True).unique()
            le = LabelEncoder()
            le.fit(vals)
            encoders[col] = le

        # Map training's `issuer_bank` encoder onto the daemon's `issuer_bank_grouped`
        # feature — training applied grouping first, so the values the encoder saw
        # were already {top_issuers ∪ 'OTHER'}. Re-fit on the grouped top set here.
        if 'issuer_bank' in encoders:
            grouped_vals = list(set(top_iss['issuer'].dropna().astype(str).tolist())
                                | {'OTHER', 'UNKNOWN'})
            le_grouped = LabelEncoder()
            le_grouped.fit(sorted(grouped_vals))
            encoders['issuer_bank_grouped'] = le_grouped
    finally:
        conn.close()

    with _state_lock:
        _state['bin_rates'] = {
            row.bin: {'rate': float(row.rate) if pd.notna(row.rate) else None,
                      'sample_size': int(row.sample_size)}
            for row in bin_rates.itertuples(index=False)
        }
        _state['bin_proc_rates'] = {
            (row.bin, row.processor): float(row.rate) if pd.notna(row.rate) else None
            for row in bin_proc.itertuples(index=False)
        }
        _state['bin_avg_amount'] = {
            row.bin: float(row.avg_amount) if pd.notna(row.avg_amount) else None
            for row in bin_amt.itertuples(index=False)
        }
        _state['te_acq_bank'] = {
            row.bank: float(row.rate) if pd.notna(row.rate) else None
            for row in te_bank.itertuples(index=False)
        }
        _state['top_issuers'] = set(top_iss['issuer'].dropna().astype(str).tolist())
        _state['encoders'] = encoders

        # Per (processor, bank) medians for new-MID substitution.
        if not mid_priors.empty:
            age_med = mid_priors.groupby(['processor', 'bank'])['age_days'].median()
            vel_med = mid_priors.groupby(['processor', 'bank'])['v_daily'].median()
            _state['mid_age_by_pb'] = {
                (str(k[0]), str(k[1])): float(v) for k, v in age_med.items()
                if pd.notna(v)
            }
            _state['mid_vel_by_pb'] = {
                (str(k[0]), str(k[1])): float(v) for k, v in vel_med.items()
                if pd.notna(v)
            }
        else:
            _state['mid_age_by_pb'] = {}
            _state['mid_vel_by_pb'] = {}

        # P1.2 + P1.4: BIN event caches.
        _state['bin_recent_events'] = bin_recent_events
        _state['bin_recent_approvals'] = bin_recent_approvals
        # P1.3: MID event cache.
        _state['mid_recent_events'] = mid_recent_events

    dt = time.time() - t0
    print(
        f'[scoring_daemon] caches refreshed in {dt:.2f}s: '
        f'{len(_state["bin_rates"])} BIN rates, '
        f'{len(_state["bin_proc_rates"])} BIN×proc, '
        f'{len(_state["te_acq_bank"])} banks, '
        f'{len(_state["top_issuers"])} top issuers, '
        f'{len(_state["mid_age_by_pb"])} (proc,bank) MID priors, '
        f'{len(_state["bin_recent_events"])} BIN event series, '
        f'{len(_state["mid_recent_events"])} MID event series, '
        f'{len(_state["encoders"])} label encoders',
        flush=True,
    )


# ---------------------------------------------------------------------------
# Feature assembly
# ---------------------------------------------------------------------------

def _is_near_payday(dt: datetime) -> int:
    d = dt.day
    return 1 if (d <= 3 or (13 <= d <= 17) or d >= 28) else 0


def _assemble_features(payload: dict) -> pd.DataFrame:
    """Build one row per candidate with the full feature vector V1 expects."""
    client_id = int(payload.get('client_id') or 0)
    bin_str = str(payload.get('bin') or '')
    amount = payload.get('amount')
    bin_feats = payload.get('bin_features') or {}
    candidates = payload.get('candidates') or []
    req_at = payload.get('request_at')

    try:
        dt = datetime.fromisoformat(req_at.replace('Z', '+00:00')) if req_at else datetime.now(tz=timezone.utc)
    except Exception:
        dt = datetime.now(tz=timezone.utc)

    hour_of_day = dt.hour
    day_of_week = dt.weekday()  # Mon=0 .. Sun=6; matches training code's day_of_week convention
    near_payday = _is_near_payday(dt)
    is_weekend = 1 if day_of_week >= 5 else 0

    with _state_lock:
        bin_rates = _state['bin_rates']
        bin_proc_rates = _state['bin_proc_rates']
        bin_avg = _state['bin_avg_amount']
        te_bank = _state['te_acq_bank']
        top_issuers = _state['top_issuers']
        mid_age_by_pb = _state['mid_age_by_pb']
        mid_vel_by_pb = _state['mid_vel_by_pb']
        bin_recent_events = _state['bin_recent_events']
        bin_recent_approvals = _state['bin_recent_approvals']
        mid_recent_events = _state['mid_recent_events']

    issuer = bin_feats.get('issuer_bank')
    issuer_grouped = issuer if issuer in top_issuers else 'OTHER'
    card_brand = bin_feats.get('card_brand')
    card_type = bin_feats.get('card_type')
    is_prepaid = int(bin_feats.get('is_prepaid') or 0)

    bin_rate = bin_rates.get(bin_str, {}).get('rate')
    bin_avg_amt = bin_avg.get(bin_str)
    amt_vs_bin = None
    if amount is not None and bin_avg_amt is not None and bin_avg_amt > 0:
        try:
            amt_vs_bin = float(amount) / float(bin_avg_amt)
        except Exception:
            amt_vs_bin = None

    # P1.2: bin_velocity_weekly — count of same-BIN events in trailing 7 days.
    # Matches training semantics: sliding window [now - 7d, now).
    now_ts = dt.timestamp()
    ts_7d_ago = now_ts - 7 * 86400
    ts_30d_ago = now_ts - 30 * 86400
    bin_events = bin_recent_events.get(bin_str)
    if bin_events is not None and len(bin_events) > 0:
        # Count events in [now - 7d, now) using binary search.
        bin_vel_weekly = int(np.searchsorted(bin_events, now_ts, side='left')
                            - np.searchsorted(bin_events, ts_7d_ago, side='left'))
    else:
        bin_vel_weekly = 0

    # P1.4: bin_approval_7d / bin_approval_30d — real windowed rates.
    # Matches training: approval_rate = approved / total in the window.
    # Fall back to all-time bin_rate only when window has < 5 samples.
    def _windowed_bin_rate(ts_cutoff):
        if bin_events is None or len(bin_events) == 0:
            return bin_rate  # all-time fallback
        total_in_window = int(np.searchsorted(bin_events, now_ts, side='left')
                              - np.searchsorted(bin_events, ts_cutoff, side='left'))
        if total_in_window < 5:
            return bin_rate  # insufficient samples, fall back to all-time
        bin_approved = bin_recent_approvals.get(bin_str)
        if bin_approved is None or len(bin_approved) == 0:
            approved_in_window = 0
        else:
            approved_in_window = int(np.searchsorted(bin_approved, now_ts, side='left')
                                     - np.searchsorted(bin_approved, ts_cutoff, side='left'))
        return approved_in_window / total_in_window

    bin_rate_7d = _windowed_bin_rate(ts_7d_ago)
    bin_rate_30d = _windowed_bin_rate(ts_30d_ago)

    rows = []
    for c in candidates:
        proc = c.get('processor_name')
        acq_bank = c.get('acquiring_bank')
        bin_proc_rate = bin_proc_rates.get((bin_str, proc))
        te = te_bank.get(acq_bank)

        # New-MID handling: if the MID is warming up (explicit flag) or young
        # (<30 days), substitute mid_age_days + mid_velocity_daily with the
        # median values for same (processor, bank). This prevents the AI from
        # systematically downranking new MIDs just because age=0. Falls back
        # to raw values when no prior exists.
        raw_age = c.get('mid_age_days')
        gw_id = c.get('gateway_id')
        is_warming = 1 if c.get('is_warming_up') else 0
        use_prior = is_warming == 1 or raw_age is None or (isinstance(raw_age, (int, float)) and raw_age < 30)

        # P1.3: mid_velocity_daily — count of same-day same-MID attempts.
        # Training semantics: events on the same calendar day for this gateway.
        start_of_day_ts = now_ts - (dt.hour * 3600 + dt.minute * 60 + dt.second)
        mid_events = mid_recent_events.get(int(gw_id)) if gw_id is not None else None
        if mid_events is not None and len(mid_events) > 0:
            mid_vel_computed = int(np.searchsorted(mid_events, now_ts, side='left')
                                   - np.searchsorted(mid_events, start_of_day_ts, side='left'))
        else:
            mid_vel_computed = 0

        if use_prior:
            prior_age = mid_age_by_pb.get((str(proc), str(acq_bank)))
            prior_vel = mid_vel_by_pb.get((str(proc), str(acq_bank)))
            mid_age_final = prior_age if prior_age is not None else (raw_age if raw_age is not None else 0)
            mid_vel_final = prior_vel if prior_vel is not None else mid_vel_computed
        else:
            mid_age_final = raw_age
            mid_vel_final = mid_vel_computed

        rows.append({
            'gateway_id': c.get('gateway_id'),
            'processor_name': proc,
            'acquiring_bank': acq_bank,
            'mcc_code': c.get('mcc_code'),
            'issuer_bank_grouped': issuer_grouped,
            'card_brand': card_brand,
            'card_type': card_type,
            'billing_state': None,            # unknown at BIN-entry time
            'client_id': client_id,
            'is_prepaid': is_prepaid,
            'hour_of_day': hour_of_day,
            'day_of_week': day_of_week,
            'mid_velocity_daily': mid_vel_final,  # prior-substituted when warming
            'bin_velocity_weekly': bin_vel_weekly,
            'mid_age_days': mid_age_final,    # prior-substituted when warming
            'bin_approval_rate': bin_rate,
            'bin_proc_approval_rate': bin_proc_rate,
            'te_acquiring_bank': te,
            'bin_approval_7d': bin_rate_7d,
            'bin_approval_30d': bin_rate_30d,
            'amount_vs_bin_avg': amt_vs_bin,
            'is_near_payday': near_payday,
            'is_weekend': is_weekend,
        })
    return pd.DataFrame(rows)


def _encode_with_fallback(le: LabelEncoder, values: pd.Series) -> np.ndarray:
    """Transform with fallback to the 'UNKNOWN' class for unseen values."""
    known = set(le.classes_)
    safe = values.fillna('UNKNOWN').astype(str).map(lambda v: v if v in known else 'UNKNOWN')
    # If 'UNKNOWN' itself isn't in classes_ (shouldn't happen — we force-added it),
    # fall back to the first class to avoid a ValueError.
    fallback = 'UNKNOWN' if 'UNKNOWN' in known else le.classes_[0]
    safe = safe.where(safe.isin(known), fallback)
    return le.transform(safe)


def _predict(df: pd.DataFrame) -> np.ndarray:
    with _state_lock:
        model = _state['model']
        encoders = _state['encoders'] or {}
    if model is None:
        raise RuntimeError('model not loaded')

    X = df.copy()
    # Build the matrix in the EXACT training order: encoded categoricals first, then numerical.
    cols = []
    for col in INITIAL_CATEGORICAL:
        if col in encoders:
            X[col + '_enc'] = _encode_with_fallback(encoders[col], X[col])
        else:
            # No encoder available — hash to a stable int as a weak fallback.
            X[col + '_enc'] = X[col].fillna('UNKNOWN').astype(str).map(lambda v: hash(v) & 0xFFFF)
        cols.append(col + '_enc')
    for col in INITIAL_NUMERICAL:
        X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0.0)
        cols.append(col)

    Xmat = X[cols].values.astype(np.float32)

    # LightGBM / sklearn-style interface: predict_proba for class 1 (approved).
    if hasattr(model, 'predict_proba'):
        probs = model.predict_proba(Xmat)
        # binary model: column 1 = positive class (approved)
        return probs[:, 1] if probs.ndim == 2 and probs.shape[1] >= 2 else probs.ravel()
    # Booster-style with .predict returning raw probabilities
    preds = model.predict(Xmat)
    return np.asarray(preds).ravel()


# ---------------------------------------------------------------------------
# HTTP handlers
# ---------------------------------------------------------------------------

@app.route('/health', methods=['GET'])
def health():
    with _state_lock:
        return jsonify({
            'ok': _state['model'] is not None,
            'model_version': _state['model_version'],
            'loaded_at': _state['loaded_at'],
            'cache_sizes': {
                'bin_rates': len(_state['bin_rates']),
                'bin_proc_rates': len(_state['bin_proc_rates']),
                'bin_avg_amount': len(_state['bin_avg_amount']),
                'te_acq_bank': len(_state['te_acq_bank']),
                'top_issuers': len(_state['top_issuers']),
                'bin_recent_events': len(_state['bin_recent_events']),
                'bin_recent_approvals': len(_state['bin_recent_approvals']),
                'mid_recent_events': len(_state['mid_recent_events']),
            },
        })


@app.route('/score', methods=['POST'])
def score():
    t0 = time.time()
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception as e:
        return jsonify({'error': f'bad_json: {e}'}), 400

    candidates = payload.get('candidates') or []
    if not candidates:
        return jsonify({'model_version': _state.get('model_version'), 'scores': []})

    try:
        df = _assemble_features(payload)
        preds = _predict(df)
    except Exception as e:
        return jsonify({'error': f'score_failed: {e}'}), 500

    scores = []
    for i in range(len(df)):
        gw = df.iloc[i]['gateway_id']
        # numpy int64 / pandas scalars aren't JSON-serializable in stdlib json.
        gw_int = int(gw) if gw is not None and pd.notna(gw) else None
        scores.append({'gateway_id': gw_int, 'score': float(preds[i])})
    dt_ms = int((time.time() - t0) * 1000)
    return jsonify({
        'model_version': _state.get('model_version'),
        'scores': scores,
        'latency_ms': dt_ms,
    })


@app.route('/reload', methods=['POST'])
def reload_caches_endpoint():
    db_path = Path(os.environ.get('BINROUTE_DB', str(DEFAULT_DB)))
    model_path = Path(os.environ.get('BINROUTE_INITIAL_MODEL', str(DEFAULT_MODEL)))
    _load_model(model_path)
    _refresh_caches(db_path)
    return jsonify({'ok': True, 'reloaded_at': datetime.now(tz=timezone.utc).isoformat()})


# SIGHUP -> reload caches + model (for systemd graceful reload)
def _install_signal_handlers(model_path: Path, db_path: Path):
    def handler(signum, frame):
        print(f'[scoring_daemon] SIG {signum} received — reloading', flush=True)
        try:
            _load_model(model_path)
            _refresh_caches(db_path)
        except Exception as e:
            print(f'[scoring_daemon] reload failed: {e}', flush=True)
    try:
        signal.signal(signal.SIGHUP, handler)
    except Exception:
        # SIGHUP isn't available on Windows — fine, use /reload HTTP endpoint instead.
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default=os.environ.get('SCORING_DAEMON_HOST', '127.0.0.1'))
    ap.add_argument('--port', type=int, default=int(os.environ.get('SCORING_DAEMON_PORT', '5001')))
    ap.add_argument('--model', default=os.environ.get('BINROUTE_INITIAL_MODEL', str(DEFAULT_MODEL)))
    ap.add_argument('--db', default=os.environ.get('BINROUTE_DB', str(DEFAULT_DB)))
    args = ap.parse_args()

    model_path = Path(args.model)
    db_path = Path(args.db)

    _load_model(model_path)
    _refresh_caches(db_path)
    _install_signal_handlers(model_path, db_path)

    # Warmup: run one dummy prediction so the first real request isn't paying the
    # LightGBM lazy-init / sklearn wrapper JIT cost. Saves ~1.5s on the first call.
    try:
        warmup_payload = {
            'client_id': 0,
            'bin': '000000',
            'amount': 1.0,
            'sales_type': 'INITIALS',
            'request_at': datetime.now(tz=timezone.utc).isoformat(),
            'bin_features': {'issuer_bank': None, 'card_brand': None, 'card_type': None, 'is_prepaid': 0},
            'candidates': [{'gateway_id': 0, 'processor_name': 'WARMUP',
                            'acquiring_bank': None, 'mcc_code': None, 'mid_age_days': None}],
        }
        _predict(_assemble_features(warmup_payload))
        print('[scoring_daemon] warmup prediction complete', flush=True)
    except Exception as e:
        print(f'[scoring_daemon] warmup skipped: {e}', flush=True)

    print(f'[scoring_daemon] listening on http://{args.host}:{args.port}', flush=True)
    # threaded=True so SQL queries inside /reload don't block /score.
    app.run(host=args.host, port=args.port, threaded=True, debug=False, use_reloader=False)


if __name__ == '__main__':
    main()
