#!/usr/bin/env bash
set -e

# ===== Config =====
API_DIR="$(cd "$(dirname "$0")/../api" && pwd)"
STREAMER_DIR="$(cd "$(dirname "$0")/../streamer" && pwd)"

API_PORT=8001          
STREAMER_CTRL_PORT=7000

# localhost Postgres
export PG_CONNINFO="host=127.0.0.1 port=5432 dbname=batonic user=postgres password=postgres"

# API 透過這個去找 streamer_control
export STREAMER_CTRL_URL="http://127.0.0.1:${STREAMER_CTRL_PORT}"

echo "🚀 Starting local dev stack..."
echo "   - streamer_control @ :${STREAMER_CTRL_PORT}"
echo "   - api_server       @ :${API_PORT}"
echo ""

# ===== Start streamer_control =====
cd "$STREAMER_DIR"
uvicorn streamer_control:app \
  --host 127.0.0.1 \
  --port ${STREAMER_CTRL_PORT} \
  --reload \
  > /tmp/streamer_control.log 2>&1 &

STREAMER_PID=$!
echo "✅ streamer_control started (pid=${STREAMER_PID})"

# ===== Start api_server =====
cd "$API_DIR"
uvicorn api_server:app \
  --host 127.0.0.1 \
  --port ${API_PORT} \
  --reload \
  > /tmp/api_server.log 2>&1 &

API_PID=$!
echo "✅ api_server started (pid=${API_PID})"

echo ""
echo "🌐 Endpoints:"
echo "   - API health:      http://127.0.0.1:${API_PORT}/health"
echo "   - Streamer health: http://127.0.0.1:${STREAMER_CTRL_PORT}/health"
echo ""
ech
