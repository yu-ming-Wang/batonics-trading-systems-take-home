#!/usr/bin/env bash
set -euo pipefail

# ============================
# Feed / Network (prefer compose envs)
# ============================
# Prefer FEED_HOST (from docker-compose/.env). Fall back to HOST (legacy), then streamer-control.
FEED_HOST="${FEED_HOST:-${HOST:-streamer-control}}"
FEED_PORT="${FEED_PORT:-9000}"
WS_PORT="${WS_PORT:-8080}"

# ============================
# Engine params
# ============================
DEPTH="${DEPTH:-50}"
SNAPSHOT_EVERY="${SNAPSHOT_EVERY:-1000}"
MAX_MSGS="${MAX_MSGS:--1}"
PUSH_MS="${PUSH_MS:-50}"

# ============================
# Output / Logs
# ============================
FEED_ENABLED="${FEED_ENABLED:-1}"
FEED_PATH="${FEED_PATH:-/shared/snapshots_feed.jsonl}"
BENCH_LOG_PATH="${BENCH_LOG_PATH:-/shared/benchmarks.jsonl}"

# ============================
# Postgres
# ============================
PG_CONNINFO="${PG_CONNINFO:-host=db port=5432 dbname=batonic user=postgres password=postgres}"

echo "[engine] connecting feed: ${FEED_HOST}:${FEED_PORT} (will retry until up)"
echo "[engine] ws_port=${WS_PORT} push_ms=${PUSH_MS} depth=${DEPTH} snapshot_every=${SNAPSHOT_EVERY} max_msgs=${MAX_MSGS}"
echo "[engine] feed_enabled=${FEED_ENABLED} feed_path=${FEED_PATH}"
echo "[engine] bench_log_path=${BENCH_LOG_PATH}"
echo "[engine] pg_conninfo=${PG_CONNINFO}"

# Ensure output dirs exist (won't fail if already exist)
mkdir -p "$(dirname "$FEED_PATH")" || true
mkdir -p "$(dirname "$BENCH_LOG_PATH")" || true

export FEED_ENABLED FEED_PATH BENCH_LOG_PATH PG_CONNINFO

exec /app/tcp_main_ws \
  "$FEED_HOST" "$FEED_PORT" "$WS_PORT" \
  "$DEPTH" "$SNAPSHOT_EVERY" "$MAX_MSGS" "$PUSH_MS"
