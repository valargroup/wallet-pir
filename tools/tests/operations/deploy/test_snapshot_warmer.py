"""Runtime validation preserves public inputs and waits for durable cache files."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest.mock import Mock, patch

import publisher_fixture  # Registers the operations package for direct discovery.
from wallet_pir_ops.deploy import snapshot_warmer as warmer


class SnapshotFilesTests(unittest.TestCase):
    def test_reusing_cache_files_preserves_the_fresh_snapshot_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            previous, fresh = root / 'previous', root / 'fresh'
            previous.mkdir()
            fresh.mkdir()
            (previous / 'active.json').write_text('old authority')
            (previous / 'shards.json').write_text('old map')
            (fresh / 'shards.json').write_text('fresh map')
            name = 'a' * 64 + '-' + 'b' * 64 + '.runtime'
            (previous / name).write_bytes(b'completed cache file')
            self.assertEqual(warmer.reuse(previous, fresh)['reused_runtimes'], 1)
            self.assertTrue((fresh / name).samefile(previous / name))
            self.assertEqual((fresh / 'shards.json').read_text(), 'fresh map')
            self.assertFalse((fresh / 'active.json').exists())

    def test_readiness_port_must_belong_to_the_query_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process = root / '123'
            (process / 'fd').mkdir(parents=True)
            (process / 'net').mkdir()
            (process / 'fd/5').symlink_to('socket:[456]')
            table = process / 'net/tcp'
            table.write_text('header\n 0: 0100007F:%04X 0 0A 0 0 0 0 0 456\n' % 18990)
            self.assertTrue(warmer.owns_listener(123, 18990, root))
            self.assertFalse(warmer.owns_listener(123, 18991, root))
            table.write_text(table.read_text().replace('456', '789'))
            self.assertFalse(warmer.owns_listener(123, 18990, root))

    def test_pins_survive_source_collection_and_seed_only_completed_runtimes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, pinned, active = (root / name for name in ('source', 'pin', 'active'))
            source.mkdir()
            active.mkdir()
            (source / 'shards.json').write_text('{"public":true}')
            name = 'a' * 64 + '-' + 'b' * 64 + '.runtime'
            (source / name).write_bytes(b'completed runtime')
            (source / 'unfinished.partial').write_bytes(b'not committed')
            warmer.pin(source, pinned)
            self.assertTrue((source / name).samefile(pinned / name))
            self.assertFalse((pinned / 'unfinished.partial').exists())
            (source / name).unlink()
            record = root / 'active.json'
            record.write_text(json.dumps({'directory': str(active)}))
            before = record.read_bytes()
            for _ in range(2):
                report = warmer.seed(pinned, record, root)
                self.assertEqual(report['linked_runtimes'], 1)
            self.assertEqual((active / name).read_bytes(), b'completed runtime')
            self.assertEqual(record.read_bytes(), before)
            self.assertFalse((active / 'shards.json').exists())
            self.assertEqual([p.name for p in active.iterdir()], [name])

    def test_refuses_links_and_recursive_pins(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            source.mkdir()
            with self.assertRaisesRegex(ValueError, 'outside its source'):
                warmer.pin(source, source / 'recursive')
            (source / 'outside').symlink_to(root)
            with self.assertRaisesRegex(ValueError, 'symbolic link'):
                warmer.pin(source, root / 'pin')


class SnapshotDeadlineTests(unittest.TestCase):
    def run_warmup(self, shards, seconds_per_shard):
        with tempfile.TemporaryDirectory() as directory:
            specification = {'work': directory, 'pin': directory,
                             'section': 'transparent_history', 'identity': {'shards': shards},
                             'memory_bytes': 1024, 'disk_bytes': 1024,
                             'map_sha256': 'map', 'binary_sha256': 'binary', 'binary': '/query'}
            now = 0
            waiting = True

            def readiness(*args):
                nonlocal now, waiting
                if waiting:
                    now += seconds_per_shard
                    waiting = False
                    return None
                waiting = True
                return {'ready': True}

            with patch.object(warmer.time, 'monotonic', side_effect=lambda: now), \
                    patch.object(warmer.time, 'sleep'), \
                    patch.object(warmer, 'assignment', return_value={}), \
                    patch.object(warmer, 'ready_report', side_effect=readiness), \
                    patch.object(warmer.subprocess, 'Popen'), \
                    patch.object(warmer, 'stop_child') as stop:
                try:
                    return warmer.warm(specification)
                finally:
                    self.assertGreater(stop.call_count, 0)

    def test_multiple_healthy_shards_can_take_more_than_fifteen_minutes(self):
        self.assertEqual(self.run_warmup(3, 400), {'validated': 3, 'complete': True})

    def test_a_single_stalled_shard_still_stops_after_fifteen_minutes(self):
        with self.assertRaisesRegex(RuntimeError, 'warming deadline exceeded'):
            self.run_warmup(1, 901)

    def test_total_preparation_still_has_a_one_hour_limit(self):
        with self.assertRaisesRegex(RuntimeError, 'warming deadline exceeded'):
            self.run_warmup(10, 400)


class SnapshotReadinessTests(unittest.TestCase):
    def setUp(self):
        self.executable = b'the qualified executable'
        self.specification = {'map_sha256': 'a' * 64,
                              'binary_sha256': hashlib.sha256(self.executable).hexdigest()}
        self.report = {
            'ready': True, 'prewarm_finished': True, 'prewarm_failed': 0,
            'warm_runtimes': 3, 'target_runtimes': 3, 'mode': 'warm', 'assigned_shards': 1,
            'assignment_sha256': 'assignment', **self.specification,
        }
        self.metrics = ('transparent_shard_disk_write_failures_total 0\n'
                        'transparent_shard_disk_save_pending 0\n')
        self.child = Mock(pid=123)
        self.child.poll.return_value = None

    def check(self):
        def response(url, timeout):
            reply = io.BytesIO(self.metrics.encode() if url.endswith('/metrics') else json.dumps(self.report).encode())
            reply.status = 200
            return reply
        with patch.object(warmer.urllib.request, 'urlopen', side_effect=response), \
                patch.object(warmer, 'owns_listener', return_value=True), \
                patch('builtins.open', return_value=io.BytesIO(self.executable)):
            return warmer.ready_report(self.specification, 'assignment', self.child)

    def test_waits_until_the_runtime_cache_finishes_saving(self):
        self.metrics = self.metrics.replace('pending 0', 'pending 1')
        self.assertIsNone(self.check())
        self.metrics = self.metrics.replace('pending 1', 'pending 0')
        self.assertEqual(self.check()['warm_runtimes'], 3)

    def test_rejects_cache_write_failures_and_missing_persistence_metrics(self):
        self.metrics = self.metrics.replace('total 0', 'total 1')
        with self.assertRaisesRegex(RuntimeError, 'persist every'):
            self.check()
        self.metrics = ''
        with self.assertRaisesRegex(RuntimeError, 'persist every'):
            self.check()

    def test_loaded_only_or_another_assignment_cannot_qualify(self):
        self.report['mode'] = 'loaded-only'
        with self.assertRaisesRegex(RuntimeError, 'exact assignment'):
            self.check()
        self.report['mode'] = 'warm'
        self.report['assignment_sha256'] = 'another assignment'
        with self.assertRaisesRegex(RuntimeError, 'exact assignment'):
            self.check()

    def test_partial_runtime_coverage_and_replaced_executables_are_rejected(self):
        self.report['warm_runtimes'] = 2
        with self.assertRaisesRegex(RuntimeError, 'staged identity'):
            self.check()
        self.report['warm_runtimes'] = 3
        self.executable = b'a replaced executable'
        with self.assertRaisesRegex(RuntimeError, 'executable changed'):
            self.check()

    def test_a_terminal_not_ready_http_response_fails_without_waiting_for_timeout(self):
        self.report.update(ready=False, prewarm_failed=1, reason='prewarm finished short of its target')
        error = urllib.error.HTTPError('http://localhost/v1/ready', 503, 'not ready', {},
                                       io.BytesIO(json.dumps(self.report).encode()))
        with patch.object(warmer.urllib.request, 'urlopen', side_effect=error), \
                patch.object(warmer, 'owns_listener', return_value=True):
            with self.assertRaisesRegex(RuntimeError, 'finished without full readiness'):
                warmer.ready_report(self.specification, 'assignment', self.child)


if __name__ == '__main__':
    unittest.main()
