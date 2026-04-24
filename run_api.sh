#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

HOST="${API_HOST:-0.0.0.0}"
PORT="${API_PORT:-8000}"

export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"

exec .venv/bin/python -m uvicorn app.api.main:app --app-dir "$PROJECT_ROOT" --host "$HOST" --port "$PORT" --reload
