#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

DAILY_JOB="0 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_kline_daily_update.py >> $LOG_DIR/daily_kline_increment.log 2>&1"
BACKFILL_JOB="15 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_kline_history_backfill.py >> $LOG_DIR/daily_kline_backfill.log 2>&1"
FUNDAMENTAL_JOB="40 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_fundamental_daily_update.py >> $LOG_DIR/fundamental_sync.log 2>&1"

CURRENT_CRON="$(crontab -l 2>/dev/null || true)"
FILTERED_CRON="$(printf '%s\n' "$CURRENT_CRON" | grep -v 'run_kline_daily_update.py' | grep -v 'run_kline_history_backfill.py' | grep -v 'run_fundamental_daily_update.py' || true)"
NEW_CRON="$(printf '%s\n%s\n%s\n%s\n' "$FILTERED_CRON" "$DAILY_JOB" "$BACKFILL_JOB" "$FUNDAMENTAL_JOB" | awk '!seen[$0]++')"

printf '%s\n' "$NEW_CRON" | crontab -
echo "Cron installed."
crontab -l | awk "!seen[$0]++"
