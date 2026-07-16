from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import time

from app.stock_selection.run_tasks import SelectionRunService
from app.jobs.worker_runtime import WorkerRuntimeHeartbeat


logger = logging.getLogger("selection-worker")
_STOP = False


def _handle_stop(signum, frame) -> None:  # noqa: ANN001
    global _STOP
    _STOP = True
    logger.info("received signal %s, stopping after current iteration", signum)


def run_worker(poll_seconds: float = 3.0, once: bool = False) -> None:
    service = SelectionRunService()
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    logger.info("selection worker started worker_id=%s poll_seconds=%s once=%s", worker_id, poll_seconds, once)
    runtime = WorkerRuntimeHeartbeat("selection", worker_id, metadata={"poll_seconds": poll_seconds})
    runtime.start()
    try:
        while not _STOP:
            recovered = service.recover_stale_running_runs()
            if recovered.total:
                logger.warning(
                    "selection stale recovery requeued=%s failed=%s cancelled=%s",
                    recovered.requeued,
                    recovered.failed,
                    recovered.cancelled,
                )
            run_id = service.claim_next_queued_run(worker_id=worker_id)
            if not run_id:
                if once:
                    logger.info("no queued selection task, exiting")
                    return
                time.sleep(poll_seconds)
                continue
            runtime.set_running(run_id)
            logger.info("claimed selection task %s", run_id)
            try:
                service.run_claimed(run_id, worker_id)
            except Exception:
                logger.exception("unhandled failure while processing selection task %s", run_id)
            else:
                logger.info("finished selection task %s", run_id)
            finally:
                runtime.set_idle()
            if once:
                return
    finally:
        runtime.stop()
    logger.info("selection worker stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Analysis selection worker")
    parser.add_argument("--poll-seconds", type=float, default=3.0)
    parser.add_argument("--once", action="store_true", help="process at most one queued selection task then exit")
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
