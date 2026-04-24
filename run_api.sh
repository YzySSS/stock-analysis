#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

HOST="${API_HOST:-0.0.0.0}"
PORT="${API_PORT:-8000}"

exec python3 -m uvicorn app.api.main:app --host "$HOST" --port "$PORT" --reload
