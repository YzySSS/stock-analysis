#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

MODE="${1:-install}"
if [[ "$MODE" != "install" && "$MODE" != "--print-only" ]]; then
  echo "Usage: $0 [--print-only]" >&2
  exit 2
fi

EOD_REALTIME_BACKFILL_JOB="10 15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/backfill_kline_from_realtime_snapshot.py --min-valid-rows 4500 --log-task >> $LOG_DIR/daily_kline_realtime_eod_backfill.log 2>&1"
REALTIME_LIFECYCLE_JOB="20 15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_realtime_lifecycle.py --apply >> $LOG_DIR/stock_realtime_lifecycle.log 2>&1"
STOCK_BASIC_JOB="30 1 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_stock_basic_sync.py >> $LOG_DIR/stock_basic_sync.log 2>&1"
ADJ_FACTOR_JOB="10 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_adj_factor_daily_update.py --recent-trade-days 5 >> $LOG_DIR/adj_factor_daily_update.log 2>&1"
MONEYFLOW_DAILY_JOB="20 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_moneyflow_daily_update.py --recent-trade-days 5 >> $LOG_DIR/moneyflow_daily_update.log 2>&1"
CHIP_DAILY_JOB="30 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_chip_daily_update.py --recent-trade-days 5 >> $LOG_DIR/chip_daily_update.log 2>&1"
DAILY_JOB="0 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_kline_daily_update.py >> $LOG_DIR/daily_kline_increment.log 2>&1"
BACKFILL_JOB="15 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_kline_history_backfill.py >> $LOG_DIR/daily_kline_backfill.log 2>&1"
FUNDAMENTAL_JOB="40 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_fundamental_daily_update.py --batch-size 500 >> $LOG_DIR/fundamental_sync.log 2>&1"
VALUATION_JOB="50 2 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_valuation_daily_update.py --batch-size 500 >> $LOG_DIR/valuation_sync.log 2>&1"
PE_BAIDU_BACKFILL_JOB="10 3 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/backfill_pe_from_baidu_valuation.py --limit 500 --workers 8 >> $LOG_DIR/pe_baidu_valuation_backfill.log 2>&1"
STATUS_SNAPSHOT_JOB="5 3 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/refresh_stock_status_snapshot.py >> $LOG_DIR/stock_status_snapshot.log 2>&1"
FACTOR_INPUT_JOB="20 3 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_factor_input_daily_update.py --recent-trade-days 5 --batch-size 500 >> $LOG_DIR/factor_input_daily_update.log 2>&1
30 18 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_factor_input_daily_update.py --recent-trade-days 5 --batch-size 500 >> $LOG_DIR/factor_input_daily_update.log 2>&1"
STRATEGY_FACTOR_CI_JOB="45 3 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_strategy_factor_ci_daily_update.py --horizon-days 1 >> $LOG_DIR/strategy_factor_ci_daily_update.log 2>&1
50 15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_strategy_factor_ci_daily_update.py --horizon-days 1 >> $LOG_DIR/strategy_factor_ci_daily_update.log 2>&1"
MARKET_CONTEXT_JOB="35 3 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_market_context_daily_update.py >> $LOG_DIR/market_context_daily_update.log 2>&1"
MARKET_TIMING_JOB="40 3 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_market_timing_daily_update.py >> $LOG_DIR/market_timing_daily_update.log 2>&1
35 15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_market_timing_daily_update.py >> $LOG_DIR/market_timing_daily_update.log 2>&1"
SENTIMENT_JOB="50 3 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_sentiment_daily_update.py --limit 80 >> $LOG_DIR/stock_sentiment_daily_update.log 2>&1"
STOCK_STATUS_PIT_JOB="35 4 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_stock_status_pit_backfill.py --stages lifecycle,names,suspensions,market-data --suspension-recent-trade-days 10 --pending-market-only --pause-seconds 0.05 >> $LOG_DIR/stock_status_pit_backfill.log 2>&1"
FUNDAMENTAL_PIT_JOB="40 4 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_fundamental_pit_backfill.py --recent-periods 8 --pause-seconds 0.1 >> $LOG_DIR/fundamental_pit_backfill.log 2>&1"
DATA_QUALITY_JOB="55 4 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_data_quality_audit.py >> $LOG_DIR/data_quality_audit.log 2>&1
45 18 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_data_quality_audit.py >> $LOG_DIR/data_quality_audit.log 2>&1"
MARKET_OPINION_JOB="*/15 9-15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_market_opinion_update.py --lookback-days 30 --sleep-seconds 0.5 >> $LOG_DIR/market_opinion_update.log 2>&1"
MARKET_OPINION_LIFECYCLE_JOB="5 16 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_market_opinion_lifecycle.py --apply >> $LOG_DIR/market_opinion_lifecycle.log 2>&1"
REALTIME_SNAPSHOT_JOB="* 9-15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_realtime_snapshot_update.py >> $LOG_DIR/stock_realtime_snapshot_update.log 2>&1"
PORTFOLIO_ETF_QUOTE_JOB="*/5 9-15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_portfolio_etf_quote_update.py --days 90 >> $LOG_DIR/portfolio_etf_quote_update.log 2>&1"
MARKET_FUND_FLOW_JOB="*/3 9-15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_market_fund_flow_update.py --retention-days 1 >> $LOG_DIR/market_fund_flow_update.log 2>&1"
THS_CONCEPT_HOT_JOB="*/30 9-15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_ths_concept_hot_update.py --retention-days 3 >> $LOG_DIR/ths_concept_hot_update.log 2>&1"
REALTIME_MONEYFLOW_JOB="*/5 9-15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_realtime_moneyflow_update.py --retention-days 1 >> $LOG_DIR/stock_realtime_moneyflow_update.log 2>&1"
STOCK_POPULARITY_JOB="*/5 9-15 * * 1-5 cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_stock_popularity_update.py --retention-days 3 >> $LOG_DIR/stock_popularity_update.log 2>&1"
JOB_RETENTION_JOB="15 4 * * * cd $PROJECT_ROOT && PYTHONPATH=$PROJECT_ROOT $PYTHON_BIN scripts/run_job_retention.py --apply >> $LOG_DIR/job_retention.log 2>&1"

CURRENT_CRON="$(crontab -l 2>/dev/null || true)"
FILTERED_CRON="$(printf '%s\n' "$CURRENT_CRON" | grep -v 'backfill_kline_from_realtime_snapshot.py' | grep -v 'run_realtime_lifecycle.py' | grep -v 'run_market_opinion_lifecycle.py' | grep -v 'run_stock_basic_sync.py' | grep -v 'run_adj_factor_daily_update.py' | grep -v 'run_moneyflow_daily_update.py' | grep -v 'run_chip_daily_update.py' | grep -v 'run_kline_daily_update.py' | grep -v 'run_kline_history_backfill.py' | grep -v 'run_fundamental_daily_update.py' | grep -v 'run_valuation_daily_update.py' | grep -v 'backfill_pe_from_baidu_valuation.py' | grep -v 'refresh_stock_status_snapshot.py' | grep -v 'run_factor_input_daily_update.py' | grep -v 'run_strategy_factor_ci_daily_update.py' | grep -v 'run_market_context_daily_update.py' | grep -v 'run_market_timing_daily_update.py' | grep -v 'run_sentiment_daily_update.py' | grep -v 'run_stock_status_pit_backfill.py' | grep -v 'run_fundamental_pit_backfill.py' | grep -v 'run_data_quality_audit.py' | grep -v 'run_market_opinion_update.py' | grep -v 'run_realtime_snapshot_update.py' | grep -v 'run_portfolio_etf_quote_update.py' | grep -v 'run_market_fund_flow_update.py' | grep -v 'run_ths_concept_hot_update.py' | grep -v 'run_realtime_moneyflow_update.py' | grep -v 'run_stock_popularity_update.py' | grep -v 'run_job_retention.py' || true)"
NEW_CRON="$(printf '%s\n' "$FILTERED_CRON" "$EOD_REALTIME_BACKFILL_JOB" "$REALTIME_LIFECYCLE_JOB" "$STOCK_BASIC_JOB" "$ADJ_FACTOR_JOB" "$MONEYFLOW_DAILY_JOB" "$CHIP_DAILY_JOB" "$DAILY_JOB" "$BACKFILL_JOB" "$FUNDAMENTAL_JOB" "$VALUATION_JOB" "$PE_BAIDU_BACKFILL_JOB" "$STATUS_SNAPSHOT_JOB" "$FACTOR_INPUT_JOB" "$STRATEGY_FACTOR_CI_JOB" "$MARKET_CONTEXT_JOB" "$MARKET_TIMING_JOB" "$SENTIMENT_JOB" "$STOCK_STATUS_PIT_JOB" "$FUNDAMENTAL_PIT_JOB" "$DATA_QUALITY_JOB" "$JOB_RETENTION_JOB" "$MARKET_OPINION_JOB" "$MARKET_OPINION_LIFECYCLE_JOB" "$REALTIME_SNAPSHOT_JOB" "$PORTFOLIO_ETF_QUOTE_JOB" "$MARKET_FUND_FLOW_JOB" "$THS_CONCEPT_HOT_JOB" "$REALTIME_MONEYFLOW_JOB" "$STOCK_POPULARITY_JOB" | awk 'NF && !seen[$0]++')"

if [[ "$MODE" == "--print-only" ]]; then
  printf '%s\n' "$NEW_CRON"
  exit 0
fi

(cd "$PROJECT_ROOT" && PYTHONPATH="$PROJECT_ROOT" "$PYTHON_BIN" -m app.orchestration.migrate --check >/dev/null)
printf '%s\n' "$NEW_CRON" | crontab -
echo "Cron installed."
crontab -l | awk '!seen[$0]++'
