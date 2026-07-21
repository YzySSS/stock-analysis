from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.mysql_lock import acquire_mysql_advisory_lock, release_mysql_advisory_lock
from app.shared.strategy_loader import StrategyLoader
from app.shared.task_log import TaskRunLogger
from app.stock_selection.forward_observation import (
    ForwardObservationRepository,
    ForwardObservationService,
    ForwardProtocolSpec,
    strategy_config_hash,
)
from app.stock_selection.run_tasks import SelectionRunService


TASK_NAME = "strategy_forward_observation_submit"
LOCK_NAME = "strategy_forward_observation_submit_lock"
PROTOCOL_ID = "a_share_sentiment_v0_4_2_after_close_v1"
STRATEGY_ID = "a_share_sentiment"
STRATEGY_VERSION = "0.4.2"
IMMUTABLE_TAG = "a-share-sentiment-v0.4.2"
EXECUTION_TIME = "16:20:00"
STARTED_ON = "2026-07-21"
BASE_REQUEST = {
    "strategy_id": STRATEGY_ID,
    "instrument_type": "stock",
    "market_board": None,
    "limit": 3,
    "max_picks": 3,
    "score_threshold": 60.0,
    "save": False,
}
FROZEN_SOURCE_PATHS = (
    "app/strategies/service.py",
    "app/strategies/capability.py",
    "app/strategies/active/thematic_strategies.py",
    "app/strategies/registry/strategies.yaml",
    "app/strategies/registry/configs/a_share_sentiment.yaml",
    "app/stock_selection/selector.py",
    "app/stock_selection/repository.py",
    "app/stock_selection/trade_plan.py",
    "app/stock_selection/deepseek_sentiment_rerank.py",
    "app/stock_selection/sentiment_refresh.py",
    "app/shared/strategy_loader.py",
    "app/shared/sentiment_scoring.py",
    "app/shared/market_opinion_taxonomy.py",
    "app/data_ingestion/market_opinion_repository.py",
    "app/data_ingestion/market_opinion_semantics.py",
    "app/data_ingestion/news_provider.py",
    "app/data_ingestion/intraday_bar_sync.py",
    "scripts/run_market_opinion_update.py",
)


def _git_output(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def verify_immutable_source(tag: str = IMMUTABLE_TAG) -> str:
    dirty = _git_output("status", "--porcelain", "--untracked-files=no")
    if dirty:
        raise RuntimeError("formal forward observation requires a clean tracked worktree")
    head = _git_output("rev-parse", "HEAD")
    tag_commit = _git_output("rev-list", "-n", "1", tag)
    if not tag_commit:
        raise RuntimeError(f"immutable strategy tag not found: {tag}")
    try:
        _git_output("merge-base", "--is-ancestor", tag_commit, head)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"immutable strategy tag {tag} is not an ancestor of deployed HEAD") from exc
    changed_paths = _git_output("diff", "--name-only", tag_commit, "--", *FROZEN_SOURCE_PATHS)
    if changed_paths:
        raise RuntimeError(
            f"immutable strategy source drift since {tag}: {changed_paths.replace(chr(10), ', ')}"
        )
    return tag_commit


def build_protocol_spec(implementation_commit: str) -> ForwardProtocolSpec:
    loader = StrategyLoader()
    meta = loader.get_strategy_meta(STRATEGY_ID)
    config = loader.load_config(STRATEGY_ID)
    actual_version = str(meta.get("version") or "")
    threshold = float((config.get("selection") or {}).get("score_threshold") or 0)
    max_picks = int((config.get("selection") or {}).get("max_picks") or 0)
    if actual_version != STRATEGY_VERSION:
        raise RuntimeError(f"strategy version drift: expected {STRATEGY_VERSION}, got {actual_version}")
    if threshold != BASE_REQUEST["score_threshold"] or max_picks != BASE_REQUEST["max_picks"]:
        raise RuntimeError(
            f"selection protocol drift: expected threshold=60/max_picks=3, got {threshold:g}/{max_picks}"
        )
    snapshot: dict[str, Any] = {
        "strategy_meta": meta,
        "strategy_config": config,
        "methodology": {
            "signal_clock": "after_close_16_20_asia_shanghai",
            "entry_rule": "first_tradable_open_after_signal_date",
            "exit_rules": {
                "1d": "entry_day_close",
                "3d": "third_trading_day_close",
                "5d": "fifth_trading_day_close",
                "20d": "twentieth_trading_day_close",
            },
            "price_accounting": "adj_factor_total_return_with_explicit_raw_fallback",
            "sample_policy": "retain_every_scheduled_day_including_zero_pick_days",
            "parameter_policy": "no_parameter_changes_until_minimum_sample_reached",
            "ai_policy": "record_actual_progressive_ai_or_local_fallback_and_report_segments",
            "preliminary_read_rule": "20 successful trade days or 50 candidates",
            "validation_caveat": "prospective observation is evidence, not formal strategy validation",
        },
    }
    return ForwardProtocolSpec(
        protocol_id=PROTOCOL_ID,
        strategy_id=STRATEGY_ID,
        strategy_version=STRATEGY_VERSION,
        protocol_version="prospective_after_close_v1",
        execution_time=EXECUTION_TIME,
        timezone="Asia/Shanghai",
        entry_rule="next_tradable_open",
        horizons=(1, 3, 5, 20),
        benchmark_codes=("000300.SH", "000905.SH", "000852.SH"),
        minimum_observation_days=20,
        minimum_candidate_count=50,
        immutable_tag=IMMUTABLE_TAG,
        implementation_commit=implementation_commit,
        strategy_config_hash=strategy_config_hash(meta, config),
        ai_policy="progressive_recorded_fallback",
        strategy_snapshot=snapshot,
        request=BASE_REQUEST,
        started_on=STARTED_ON,
    )


def submit_observation(
    *,
    today: date,
    repository: ForwardObservationRepository,
    selection_service: SelectionRunService,
    protocol_spec: ForwardProtocolSpec,
) -> dict[str, Any]:
    repository.ensure_protocol(protocol_spec)
    latest_trade_date = repository.latest_data_trade_date()
    if latest_trade_date != today:
        return {
            "status": "skipped",
            "reason": "latest_daily_kline_is_not_today",
            "today": today.isoformat(),
            "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
            "protocol_id": protocol_spec.protocol_id,
        }

    observation_id = f"asent_fwd_{today.strftime('%Y%m%d')}_v042"
    observation = repository.reserve_observation(
        observation_id=observation_id,
        protocol_id=protocol_spec.protocol_id,
        signal_trade_date=today,
        request=protocol_spec.request,
    )
    if observation.get("status") in {"success", "failed"}:
        return {
            "status": "deduplicated",
            "observation_id": observation_id,
            "observation_status": observation.get("status"),
            "selection_run_id": observation.get("selection_run_id"),
        }

    existing_run = repository.find_selection_run_for_observation(observation_id)
    if existing_run:
        repository.attach_selection_run(observation_id, str(existing_run.get("run_id")))
        ForwardObservationService(repository=repository).reconcile_open_observations()
        refreshed = repository.get_observation(observation_id) or {}
        return {
            "status": "recovered",
            "observation_id": observation_id,
            "observation_status": refreshed.get("status"),
            "selection_run_id": refreshed.get("selection_run_id"),
        }

    run = selection_service.submit(
        {
            **protocol_spec.request,
            "forward_protocol_id": protocol_spec.protocol_id,
            "forward_observation_id": observation_id,
        }
    )
    selection_run_id = str(run.get("run_id") or "")
    if not selection_run_id:
        raise RuntimeError("selection service did not return a run_id")
    repository.attach_selection_run(observation_id, selection_run_id)
    return {
        "status": "queued",
        "protocol_id": protocol_spec.protocol_id,
        "observation_id": observation_id,
        "signal_trade_date": today.isoformat(),
        "selection_run_id": selection_run_id,
        "deduplicated": bool(run.get("deduplicated")),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit the frozen A-share sentiment forward observation.")
    parser.add_argument("--dry-run", action="store_true", help="verify immutable source and protocol without database writes")
    args = parser.parse_args()

    implementation_commit = verify_immutable_source()
    spec = build_protocol_spec(implementation_commit)
    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "dry_run_ok",
                    "protocol_id": spec.protocol_id,
                    "strategy_id": spec.strategy_id,
                    "strategy_version": spec.strategy_version,
                    "implementation_commit": spec.implementation_commit,
                    "strategy_config_hash": spec.strategy_config_hash,
                    "request": spec.request,
                },
                ensure_ascii=False,
                default=str,
            )
        )
        return

    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock_handle is None:
        print(json.dumps({"status": "skipped", "reason": "lock_unavailable"}, ensure_ascii=False))
        return
    logger = TaskRunLogger()
    task_run_id = f"strategy_forward_observation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metadata = {
        "protocol_id": spec.protocol_id,
        "strategy_id": spec.strategy_id,
        "strategy_version": spec.strategy_version,
        "implementation_commit": spec.implementation_commit,
    }
    try:
        logger.start(TASK_NAME, task_run_id, metadata)
        result = submit_observation(
            today=date.today(),
            repository=ForwardObservationRepository(),
            selection_service=SelectionRunService(),
            protocol_spec=spec,
        )
        status = "success" if result.get("status") != "failed" else "failed"
        logger.finish(TASK_NAME, task_run_id, status, f"forward observation {result.get('status')}", result)
        print(json.dumps(result, ensure_ascii=False, default=str))
    except Exception as exc:
        logger.finish(TASK_NAME, task_run_id, "failed", str(exc), metadata)
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock_handle)
        if release_error:
            print(f"warning: failed to release {LOCK_NAME}: {release_error}", file=sys.stderr)


if __name__ == "__main__":
    main()
