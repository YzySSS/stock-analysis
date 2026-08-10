from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.mysql_lock import (
    acquire_mysql_advisory_lock,
    release_mysql_advisory_lock,
)
from app.shared.task_log import TaskRunLogger
from app.stock_selection.automatic_observation import (
    AutomaticObservationCampaignService,
)


TASK_NAME = "automatic_strategy_observation"
LOCK_NAME = "automatic_strategy_observation_lock"


def _git_output(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def verify_deployed_strategy_tags(
    service: AutomaticObservationCampaignService,
    *,
    today: date,
) -> dict[str, Any]:
    dirty = _git_output("status", "--porcelain", "--untracked-files=no")
    if dirty:
        raise RuntimeError(
            "automatic strategy observation requires a clean tracked worktree"
        )
    head = _git_output("rev-parse", "HEAD")
    policies = service.policies(today=today)
    tags = sorted(
        {
            tag
            for policy in policies
            for tag in (
                policy.baseline_immutable_tag,
                policy.candidate_immutable_tag,
            )
        }
    )
    resolved = {}
    for tag in tags:
        tag_commit = _git_output("rev-list", "-n", "1", tag)
        if not tag_commit:
            raise RuntimeError(f"immutable strategy tag not found: {tag}")
        try:
            _git_output("merge-base", "--is-ancestor", tag_commit, head)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"immutable strategy tag {tag} is not an ancestor of deployed HEAD"
            ) from exc
        resolved[tag] = tag_commit
    return {
        "head": head,
        "policies": policies,
        "tags": resolved,
    }


def wait_for_call_auction_snapshot(
    service: AutomaticObservationCampaignService,
    *,
    today: date,
    policies,
    wait_seconds: int,
    poll_seconds: int,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(0, int(wait_seconds))
    while True:
        readiness = service.quote_readiness(today=today, policies=policies)
        if readiness.get("ready"):
            return readiness
        if time.monotonic() >= deadline:
            return readiness
        time.sleep(max(1, min(int(poll_seconds), 30)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run mandatory paired strategy observations at "
            "09:25 without writing manual selection_result rows"
        )
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--wait-seconds", type=int, default=90)
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--date", help="override local date (YYYY-MM-DD)")
    args = parser.parse_args(argv)
    if args.wait_seconds < 0:
        parser.error("--wait-seconds must be non-negative")
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")

    today = date.fromisoformat(args.date) if args.date else date.today()
    service = AutomaticObservationCampaignService()
    deployment = verify_deployed_strategy_tags(service, today=today)
    policies = deployment["policies"]
    policy_payload = [
        {
            "campaign_id": policy.campaign_id,
            "baseline_strategy_id": policy.baseline_strategy_id,
            "baseline_strategy_version": policy.baseline_strategy_version,
            "candidate_strategy_id": policy.candidate_strategy_id,
            "candidate_strategy_version": policy.candidate_strategy_version,
            "start_on": policy.start_on.isoformat(),
            "target_trade_days": policy.target_trade_days,
            "execution_time": policy.execution_time,
            "entry_rule": policy.entry_rule,
            "engine": policy.engine,
        }
        for policy in policies
    ]
    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "dry_run_ok",
                    "today": today.isoformat(),
                    "head": deployment["head"],
                    "tags": deployment["tags"],
                    "policies": policy_payload,
                    "selection_result_written": False,
                },
                ensure_ascii=False,
                default=str,
            )
        )
        return 0
    if today.weekday() >= 5:
        print(
            json.dumps(
                {
                    "status": "skipped",
                    "reason": "weekend",
                    "today": today.isoformat(),
                },
                ensure_ascii=False,
            )
        )
        return 0
    eligible = [policy for policy in policies if today >= policy.start_on]
    if not eligible:
        print(
            json.dumps(
                {
                    "status": "scheduled",
                    "today": today.isoformat(),
                    "policies": policy_payload,
                },
                ensure_ascii=False,
            )
        )
        return 0

    lock_handle = acquire_mysql_advisory_lock(LOCK_NAME, timeout_seconds=0)
    if lock_handle is None:
        print(
            json.dumps(
                {"status": "skipped", "reason": "lock_unavailable"},
                ensure_ascii=False,
            )
        )
        return 0

    logger = TaskRunLogger()
    task_run_id = (
        f"{TASK_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    )
    metadata = {
        "today": today.isoformat(),
        "head": deployment["head"],
        "policies": policy_payload,
        "selection_result_written": False,
    }
    try:
        logger.start(TASK_NAME, task_run_id, metadata)
        pending_policies = service.policies_requiring_snapshot(
            today=today,
            policies=eligible,
        )
        if not pending_policies:
            result = service.run(
                today=today,
                implementation_commit=deployment["head"],
            )
            result["call_auction_readiness"] = {
                "ready": True,
                "reason": "no_pending_campaigns",
            }
            logger.finish(
                TASK_NAME,
                task_run_id,
                "success",
                "automatic observation campaigns already complete or deduplicated",
                result,
            )
            print(json.dumps(result, ensure_ascii=False, default=str))
            return 0
        readiness = wait_for_call_auction_snapshot(
            service,
            today=today,
            policies=pending_policies,
            wait_seconds=args.wait_seconds,
            poll_seconds=args.poll_seconds,
        )
        if not readiness.get("ready"):
            result = {
                "status": "skipped",
                "reason": "call_auction_snapshot_not_ready",
                "readiness": readiness,
                "selection_result_written": False,
            }
            logger.finish(
                TASK_NAME,
                task_run_id,
                "success",
                "automatic observation skipped: call auction snapshot not ready",
                result,
            )
            print(json.dumps(result, ensure_ascii=False, default=str))
            return 0
        result = service.run(
            today=today,
            implementation_commit=deployment["head"],
        )
        result["call_auction_readiness"] = readiness
        logger.finish(
            TASK_NAME,
            task_run_id,
            "success",
            "automatic paired strategy observation completed",
            result,
        )
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    except Exception as exc:
        logger.finish(
            TASK_NAME,
            task_run_id,
            "failed",
            str(exc)[:500],
            metadata,
        )
        raise
    finally:
        release_error = release_mysql_advisory_lock(lock_handle)
        if release_error:
            print(
                f"warning: failed to release {LOCK_NAME}: {release_error}",
                file=sys.stderr,
            )


if __name__ == "__main__":
    raise SystemExit(main())
