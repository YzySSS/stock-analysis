from __future__ import annotations

import logging
import math
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

from app.shared.cache import get_cache_backend

PREFIX = 'display:v1'
logger = logging.getLogger(__name__)
_LOCKS: dict[str, threading.Lock] = {}
_LOCK_GUARD = threading.Lock()
MAX_PAYLOAD_BYTES = 1024 * 1024
# Fixed allowlist: no user query strings, unbounded key cardinality or private portfolios.
SCOPES = {
    'dashboard': ('market', 'tracking'),
    'dataset': ('dataset',),
    'tracking': ('market', 'tracking'),
    'tracking-filters': ('tracking',),
    'system': ('system',),
}
TTLS = {'dashboard': 60, 'dataset': 60, 'tracking': 30, 'tracking-filters': 60, 'system': 60}
REFRESH = {'dashboard': 15, 'dataset': 30, 'tracking': 15, 'tracking-filters': 30, 'system': 30}


def _day() -> str:
    return datetime.now(ZoneInfo('Asia/Shanghai')).date().isoformat()


def _lock(name: str) -> threading.Lock:
    with _LOCK_GUARD:
        return _LOCKS.setdefault(name, threading.Lock())


def _usable(backend) -> bool:
    diag = backend.diagnostics()
    return diag.get('backend') != 'disabled' and not (
        diag.get('backend') == 'redis' and diag.get('status') != 'ready'
    )


def revisions(name: str) -> dict:
    backend = get_cache_backend()
    return {scope: backend.get(f'{PREFIX}:revision:{scope}') for scope in SCOPES[name]}


def invalidate(*scopes: str) -> None:
    """Call only after commit. Token mismatch makes old projections unreadable.

    Updating a single fixed key per scope also coalesces bursts of updates.
    No key scans, global cache clears, queue/lock keys or synchronous SQL.
    """
    backend = get_cache_backend()
    for scope in set(scopes):
        if scope not in {'market', 'dataset', 'tracking', 'system'}:
            raise ValueError('unknown display scope')
        backend.set(f'{PREFIX}:revision:{scope}', uuid.uuid4().hex, ttl_seconds=86400)


def entry(name: str) -> dict | None:
    backend = get_cache_backend()
    value = backend.get(f'{PREFIX}:{name}')
    if not isinstance(value, dict) or not _usable(backend):
        return None
    built_at = value.get('built_at')
    if not isinstance(built_at, (float, int)) or not math.isfinite(built_at):
        return None
    age = time.time() - built_at
    if not 0 <= age < TTLS[name] or value.get('day') != _day():
        return None
    if value.get('revisions') != revisions(name) or not _usable(backend):
        return None
    return value if isinstance(value.get('payload'), dict) else None


@contextmanager
def lease(name: str, backend=None):
    """Redis owner-token lease, never a business/worker lock.

    On Redis failure we serve live SQL, without publishing shared projections.
    A 120s bound exceeds normal display builds; publication checks ownership
    again, so a stalled former owner cannot overwrite a successor.
    """
    backend = backend or get_cache_backend()
    client = backend._redis_client() if hasattr(backend, '_redis_client') else None
    key, token = f'{PREFIX}:lease:{name}', uuid.uuid4().hex
    acquired = True
    if client is not None:
        try:
            acquired = bool(client.set(backend._key(key), token, nx=True, ex=120))
        except Exception:
            acquired = False
    def owns():
        if not acquired:
            return False
        if client is None:
            return _usable(backend)
        try:
            raw = client.get(backend._key(key))
            return raw in (token, token.encode())
        except Exception:
            return False
    try:
        yield owns
    finally:
        if client is not None and acquired:
            try:
                client.eval("if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end", 1, backend._key(key), token)
            except Exception:
                pass  # Expiry releases it; never delete someone else's lease.


def builder(name: str) -> Callable[[], dict]:
    if name == 'dashboard':
        from app.api.routes.dashboard import _build_dashboard_summary
        return lambda: _build_dashboard_summary(8, compact=True)
    if name == 'dataset':
        from app.api.strategy_display import _query_display_dataset_snapshot
        return _query_display_dataset_snapshot
    if name == 'tracking':
        from app.api.routes.tracking import _tracking_payload
        return lambda: _tracking_payload(limit=10, compact=True, include_runs=False)
    if name == 'tracking-filters':
        from app.api.routes.tracking import _build_tracking_filters
        return lambda: _build_tracking_filters('stock', None)
    if name == 'system':
        from app.api.routes.system import _build_system_status
        return _build_system_status
    raise ValueError('unknown display projection')


def refresh(name: str, load: Callable[[], dict] | None = None, *, only_if_missing: bool = False) -> dict | None:
    with _lock(name), lease(name) as owns:
        if only_if_missing:
            cached = entry(name)
            if cached:
                return cached
        if not owns():
            return None
        before = revisions(name)
        day = _day()
        started = time.time()
        payload = (load or builder(name))()
        # Metadata describes the source snapshot build, never rewrites source dates.
        value = {'payload': payload, 'built_at': started, 'day': day, 'revisions': before}
        if before == revisions(name) and day == _day() and owns():
            from app.shared.cache import _encode
            if len(_encode(value)) <= MAX_PAYLOAD_BYTES:
                get_cache_backend().set(f'{PREFIX}:{name}', value, ttl_seconds=TTLS[name])
                return value
        return None


def get(name: str, load: Callable[[], dict] | None = None) -> dict:
    cached = entry(name)
    if cached:
        return cached['payload']
    # Local single-flight plus cross-process lease. Recheck after local wait.
    with _lock('request:' + name):
        cached = entry(name)
        if cached:
            return cached['payload']
        value = refresh(name, load, only_if_missing=True)
        if value:
            return value['payload']
        # Another process may own a long provider/SQL build. Wait at most 1s,
        # then a live uncached read keeps Redis optional; never serve expired data.
        for _ in range(10):
            cached = entry(name)
            if cached:
                return cached['payload']
            if not _usable(get_cache_backend()):
                break
            time.sleep(.1)
        return (load or builder(name))()


def warm_cycle() -> dict:
    result = {}
    for name in SCOPES:
        try:
            cached = entry(name)
            if not cached or time.time() - cached['built_at'] >= REFRESH[name]:
                result[name] = 'published' if refresh(name) else 'superseded_or_busy'
            else:
                result[name] = 'fresh'
        except Exception as exc:
            result[name] = type(exc).__name__
            logger.warning('display warm failed name=%s error=%s', name, type(exc).__name__)
    # External providers are last; one serial lane, seven fixed keys.
    try:
        from app.dashboard.index_market import warm_index_cache
        result['indices'] = warm_index_cache()
    except Exception as exc:
        result['indices'] = type(exc).__name__
    get_cache_backend().set(f'{PREFIX}:warmer-status', {'at': time.time(), 'results': result}, ttl_seconds=180)
    return result


class DisplayWarmer:
    def __init__(self):
        self.stop_event = threading.Event()
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self.run, name='display-cache-warmer', daemon=True)
        self.thread.start()

    def run(self):
        while not self.stop_event.is_set():
            try:
                # Cross-process suppression for future multiple API workers.
                with lease('warm-cycle') as owns:
                    if owns():
                        warm_cycle()
            except Exception as exc:
                logger.warning('display warmer unavailable error=%s', type(exc).__name__)
            self.stop_event.wait(5)

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=2)


def diagnostics() -> dict:
    backend = get_cache_backend()
    result = {}
    for name in SCOPES:
        value = entry(name)
        result[name] = {
            'usable': value is not None,
            'age_seconds': round(time.time() - value['built_at'], 2) if value else None,
            'max_age_seconds': TTLS[name],
            'refresh_seconds': REFRESH[name],
            'source_day_guard': 'Asia/Shanghai',
        }
    return {'projections': result, 'warmer': backend.get(f'{PREFIX}:warmer-status')}
