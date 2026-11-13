from __future__ import annotations
import threading
import unittest
from unittest.mock import patch, MagicMock
from app.display_cache import service as cache
from app.display_cache.events import TaskRunLogger
from app.shared.cache import InMemoryCacheBackend, RedisCacheBackend


class AtomicRedis:
    def __init__(self):
        self.data = {}
        self.fail = False
    def ping(self):
        if self.fail: raise ConnectionError('offline')
    def get(self, key):
        self.ping()
        return self.data.get(key)
    def set(self, key, value, ex=None, nx=False):
        self.ping()
        if nx and key in self.data: return False
        self.data[key] = value
        return True
    def eval(self, script, n, key, token):
        if self.data.get(key) == token:
            del self.data[key]
            return 1
        return 0


class DisplayCacheTests(unittest.TestCase):
    def setUp(self):
        self.backend = InMemoryCacheBackend()
        self.patch = patch.object(cache, 'get_cache_backend', return_value=self.backend)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_revision_change_immediately_rejects_old_payload_without_broad_clear(self):
        self.backend.set('unrelated:queue', {'id': 2})
        cache.refresh('dashboard', lambda: {'date': '2026-11-12', 'price': 1})
        self.assertEqual(cache.get('dashboard')['price'], 1)
        cache.invalidate('market')
        self.assertIsNone(cache.entry('dashboard'))
        self.assertEqual(cache.get('dashboard', lambda: {'price': 2}), {'price': 2})
        self.assertEqual(self.backend.get('unrelated:queue'), {'id': 2})

    def test_source_update_during_build_cannot_publish_old_revision(self):
        def build():
            cache.invalidate('dataset')
            return {'old': True}
        self.assertIsNone(cache.refresh('dataset', build))
        self.assertIsNone(cache.entry('dataset'))

    def test_midnight_and_clock_rollback_do_not_revive_yesterday(self):
        with patch.object(cache, '_day', return_value='2026-11-12'):
            cache.refresh('dataset', lambda: {'snapshot_now': '2026-11-12 23:59:50'})
        with patch.object(cache, '_day', return_value='2026-11-13'):
            self.assertIsNone(cache.entry('dataset'))
        value = self.backend.get('display:v1:dataset')
        value['built_at'] = float('nan')
        self.backend.set('display:v1:dataset', value)
        self.assertIsNone(cache.entry('dataset'))

    def test_short_ttl_is_not_extended_by_hits_or_failed_builds(self):
        with patch.object(cache.time, 'time', return_value=1000):
            cache.refresh('dataset', lambda: {'ready': True})
        with patch.object(cache.time, 'time', return_value=1060):
            with self.assertRaises(RuntimeError):
                cache.get('dataset', lambda: (_ for _ in ()).throw(RuntimeError('db offline')))
        self.assertEqual(self.backend.get('display:v1:dataset')['built_at'], 1000)

    def test_redis_outage_does_not_serve_process_local_stale_fallback(self):
        client = AtomicRedis()
        backend = RedisCacheBackend(redis_url=None, client=client, fallback=InMemoryCacheBackend())
        with patch.object(cache, 'get_cache_backend', return_value=backend):
            cache.refresh('dataset', lambda: {'ready': True})
            client.fail = True
            self.assertIsNone(cache.entry('dataset'))
            self.assertEqual(cache.get('dataset', lambda: {'ready': False}), {'ready': False})
            client.fail = False
            backend._retry_at = 0
            cache.refresh('dataset', lambda: {'ready': False})
            self.assertEqual(cache.get('dataset'), {'ready': False})

    def test_lease_is_single_owner_and_old_owner_cannot_delete_new_owner(self):
        client = AtomicRedis()
        backend = RedisCacheBackend(redis_url=None, client=client)
        with patch.object(cache, 'get_cache_backend', return_value=backend):
            with cache.lease('dataset') as owns:
                self.assertTrue(owns())
                with cache.lease('dataset') as second:
                    self.assertFalse(second())
                key = backend._key('display:v1:lease:dataset')
                client.data[key] = b'successor'
                self.assertFalse(owns())
            self.assertEqual(client.data[key], b'successor')

    def test_large_projection_is_not_stored(self):
        self.assertIsNone(cache.refresh('dataset', lambda: {'huge': 'x' * (cache.MAX_PAYLOAD_BYTES + 1)}))
        self.assertIsNone(cache.entry('dataset'))

    def test_independent_prewarmer_failure_does_not_stop_other_projections(self):
        def fake_builder(name):
            if name == 'dataset': return lambda: (_ for _ in ()).throw(RuntimeError('offline'))
            return lambda: {'name': name}
        with patch.object(cache, 'builder', side_effect=fake_builder), patch('app.dashboard.index_market.warm_index_cache', return_value={}):
            result = cache.warm_cycle()
        self.assertEqual(result['dataset'], 'RuntimeError')
        self.assertEqual(result['tracking'], 'published')
        self.assertEqual(cache.entry('tracking')['payload'], {'name': 'tracking'})

    def test_events_run_only_after_successful_log_commit_and_are_best_effort(self):
        logger = TaskRunLogger()
        with patch('app.display_cache.events.BaseTaskRunLogger.finish') as finish, patch.object(cache, 'invalidate') as invalidate:
            logger.finish('daily_kline', 'r', 'success')
            finish.assert_called_once()
            invalidate.assert_called_once_with('market', 'dataset', 'system')
        with patch('app.display_cache.events.BaseTaskRunLogger.finish', side_effect=RuntimeError('commit failed')), patch.object(cache, 'invalidate') as invalidate:
            with self.assertRaises(RuntimeError): logger.finish('daily_kline', 'r', 'success')
            invalidate.assert_not_called()
        with patch('app.display_cache.events.BaseTaskRunLogger.finish'), patch.object(cache, 'invalidate', side_effect=RuntimeError('redis failed')):
            logger.finish('daily_kline', 'r', 'success')  # Business task stays successful.

    def test_warmer_stop_is_bounded_without_waiting_on_provider(self):
        warmer = cache.DisplayWarmer()
        warmer.thread = MagicMock()
        warmer.stop()
        self.assertTrue(warmer.stop_event.is_set())
        warmer.thread.join.assert_called_once_with(timeout=2)


if __name__ == '__main__':
    unittest.main()
