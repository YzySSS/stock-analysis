#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

DAILY_JOB="0 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_kline_daily_update.py >> $LOG_DIR/daily_kline_increment.log 2>&1"
BACKFILL_JOB="15 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_kline_history_backfill.py >> $LOG_DIR/daily_kline_backfill.log 2>&1"
FUNDAMENTAL_JOB="40 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_fundamental_daily_update.py --batch-size 500 >> $LOG_DIR/fundamental_sync.log 2>&1"
VALUATION_JOB="50 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_valuation_daily_update.py --batch-size 500 >> $LOG_DIR/valuation_sync.log 2>&1"
STATUS_SNAPSHOT_JOB="5 3 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/refresh_stock_status_snapshot.py >> $LOG_DIR/stock_status_snapshot.log 2>&1"
FACTOR_INPUT_JOB="20 3 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_factor_input_daily_update.py --recent-trade-days 5 --batch-size 500 >> $LOG_DIR/factor_input_daily_update.log 2>&1"
MARKET_CONTEXT_JOB="35 3 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_market_context_daily_update.py >> $LOG_DIR/market_context_daily_update.log 2>&1"
SENTIMENT_JOB="50 3 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_sentiment_daily_update.py --limit 80 >> $LOG_DIR/stock_sentiment_daily_update.log 2>&1"
REALTIME_SNAPSHOT_JOB="* 9-15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_realtime_snapshot_update.py --retention-days 1 >> $LOG_DIR/stock_realtime_snapshot_update.log 2>&1"
MARKET_FUND_FLOW_JOB="*/3 9-15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_market_fund_flow_update.py --retention-days 1 >> $LOG_DIR/market_fund_flow_update.log 2>&1"

CURRENT_CRON="$(crontab -l 2>/dev/null || true)"
FILTERED_CRON="$(printf '%s\n' "$CURRENT_CRON" | grep -v 'run_kline_daily_update.py' | grep -v 'run_kline_history_backfill.py' | grep -v 'run_fundamental_daily_update.py' | grep -v 'run_valuation_daily_update.py' | grep -v 'refresh_stock_status_snapshot.py' | grep -v 'run_factor_input_daily_update.py' | grep -v 'run_market_context_daily_update.py' | grep -v 'run_sentiment_daily_update.py' | grep -v 'run_realtime_snapshot_update.py' | grep -v 'run_market_fund_flow_update.py' || true)"
NEW_CRON="$(printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n' "$FILTERED_CRON" "$DAILY_JOB" "$BACKFILL_JOB" "$FUNDAMENTAL_JOB" "$VALUATION_JOB" "$STATUS_SNAPSHOT_JOB" "$FACTOR_INPUT_JOB" "$MARKET_CONTEXT_JOB" "$SENTIMENT_JOB" "$REALTIME_SNAPSHOT_JOB" "$MARKET_FUND_FLOW_JOB" | awk '!seen[$0]++')"

printf '%s\n' "$NEW_CRON" | crontab -
echo "Cron installed."
crontab -l | awk '!seen[$0]++'
