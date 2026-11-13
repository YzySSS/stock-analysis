"""Post-commit display invalidation, outside frozen research source identity."""
from __future__ import annotations
import logging
from app.shared.task_log import TaskRunLogger as BaseTaskRunLogger

logger = logging.getLogger(__name__)


class TaskRunLogger(BaseTaskRunLogger):
    def finish(self, task_name, run_id, status, message=None, metadata=None, error_code=None):
        super().finish(task_name, run_id, status, message, metadata, error_code)
        # Partial/failed runs can have committed useful rows as well. Never
        # claim readiness: invalidate display results and let live builders decide.
        if status in {'skipped', 'running'}:
            return
        try:
            from app.display_cache.service import invalidate
            invalidate('market', 'dataset', 'system')
        except Exception:
            logger.warning('post-commit display invalidation unavailable task=%s', task_name)
