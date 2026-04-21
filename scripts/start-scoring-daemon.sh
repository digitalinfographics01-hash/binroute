#!/usr/bin/env bash
# Start the Python scoring daemon. Intended for use as a systemd ExecStart
# on the Hostinger VPS. Locally you can run this directly.
#
# Systemd unit template (save as /etc/systemd/system/binroute-scoring.service):
#
#   [Unit]
#   Description=BinRoute Initial-Routing Scoring Daemon
#   After=network.target
#
#   [Service]
#   Type=simple
#   WorkingDirectory=/opt/binroute
#   ExecStart=/opt/binroute/scripts/start-scoring-daemon.sh
#   ExecReload=/bin/kill -HUP $MAINPID
#   Restart=on-failure
#   RestartSec=3
#   Environment=SCORING_DAEMON_HOST=127.0.0.1
#   Environment=SCORING_DAEMON_PORT=5001
#   StandardOutput=append:/var/log/binroute-scoring.log
#   StandardError=append:/var/log/binroute-scoring.log
#
#   [Install]
#   WantedBy=multi-user.target
#
# Then: systemctl daemon-reload && systemctl enable --now binroute-scoring
#       systemctl reload binroute-scoring   # refresh caches + model after retrain

set -euo pipefail

# Default to the repo root two dirs up from this file.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PY="${PYTHON:-python3}"
HOST="${SCORING_DAEMON_HOST:-127.0.0.1}"
PORT="${SCORING_DAEMON_PORT:-5001}"
MODEL="${BINROUTE_INITIAL_MODEL:-$REPO_ROOT/data/models/five_model_initial.pkl}"
DB="${BINROUTE_DB:-$REPO_ROOT/data/binroute.db}"

cd "$REPO_ROOT"
exec "$PY" scripts/ml/scoring_daemon.py \
  --host "$HOST" \
  --port "$PORT" \
  --model "$MODEL" \
  --db "$DB"
