from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import time

from app.jobs.durable_tasks import DurableTaskService
from app.jobs.worker_runtime import WorkerRuntimeHeartbeat


logger = logging.getLogger("durable-task-worker")
_STOP = False


def _handle_stop(signum, frame) -> None:  # noqa: ANN001
    global _STOP
    _STOP = True
    logger.info("received signal %s, stopping after current iteration", signum)


def run_worker(poll_seconds: float = 3.0, once: bool = False) -> None:
    service = DurableTaskService()
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    logger.info("durable task worker started worker_id=%s poll_seconds=%s once=%s", worker_id, poll_seconds, once)
    runtime = WorkerRuntimeHeartbeat("durable_task", worker_id, metadata={"poll_seconds": poll_seconds})
    runtime.start()
    try:
        while not _STOP:
            recovered = service.recover_stale()
            if recovered.total:
                logger.warning(
                    "durable task stale recovery requeued=%s failed=%s cancelled=%s",
                    recovered.requeued,
                    recovered.failed,
                    recovered.cancelled,
                )
            task_id = service.claim_next(worker_id)
            if not task_id:
                if once:
                    logger.info("no queued durable task, exiting")
                    return
                time.sleep(poll_seconds)
                continue
            runtime.set_running(task_id)
            logger.info("claimed durable task %s", task_id)
            try:
                outcome = service.run_claimed(task_id, worker_id)
            except Exception:
                logger.exception("unhandled failure while processing durable task %s", task_id)
            else:
                if outcome == "requeued":
                    logger.warning("durable task %s requeued after transient failure", task_id)
                else:
                    logger.info("finished durable task %s", task_id)
            finally:
                runtime.set_idle()
            if once:
                return
    finally:
        runtime.stop()
    logger.info("durable task worker stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Analysis durable API task worker")
    parser.add_argument("--poll-seconds", type=float, default=3.0)
    parser.add_argument("--once", action="store_true", help="process at most one queued task then exit")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    run_worker(poll_seconds=args.poll_seconds, once=args.once)


if __name__ == "__main__":
    main()
