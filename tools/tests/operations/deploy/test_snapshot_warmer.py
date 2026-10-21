"""Snapshot reuse preserves public inputs and checks the full archive capacity."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

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
            self.assertEqual(warmer.reuse(previous, fresh)['runtime_bytes'], len(b'completed cache file'))
            self.assertTrue((fresh / name).samefile(previous / name))
            self.assertEqual((fresh / 'shards.json').read_text(), 'fresh map')
            self.assertFalse((fresh / 'active.json').exists())


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


class SnapshotCapacityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        identity = dict.fromkeys(('shard_schema', 'map_sha256', 'network', 'genesis_hash'), 'identity')
        identity.update(shards=3, start_height=0, covered_through=99, recent_from_shard=3)
        self.specification = {'work': str(self.root), 'pin': str(self.root),
                              'section': 'transparent_history', 'identity': identity,
                              'map_sha256': 'map', 'binary_sha256': 'binary', 'binary': '/query',
                              'verification_cache_bytes': 44 << 30,
                              'memory_bytes': 36 << 30, 'disk_bytes': (34 + 32) << 30}

    def test_native_verification_checks_every_shard_with_the_archive_budget(self):
        sizing = {'used_bytes': 34 << 30, 'missing_bytes': 8 << 30}

        def verify(command, **kwargs):
            self.assertEqual(command[-1], '--verify-only')
            config = json.loads(Path(command[command.index('--config') + 1]).read_text())
            settings = config['transparent_history']['settings']
            self.assertEqual(settings['cache_bytes'], 44 << 30)
            self.assertEqual(settings['runtime_cache_max_bytes'], (34 + 32) << 30)
            assigned = json.loads(Path(settings['assignment']).read_text())
            self.assertEqual(assigned['workers'][0]['shards'], [0, 1, 2])
            self.assertEqual(assigned['unassigned'], [])
            self.assertNotIn('retain_revisions', settings)
            return subprocess.CompletedProcess(command, 0, json.dumps({'verified': True, 'runtime_cache': sizing}), '')

        with patch.object(warmer.subprocess, 'run', side_effect=verify):
            self.assertEqual(warmer.verify_history_capacity(self.specification), sizing)

    def test_insufficient_cache_capacity_refuses_without_starting_a_server(self):
        failure = subprocess.CompletedProcess([], 1, '', 'runtime cache budget exhausted')
        with patch.object(warmer.subprocess, 'run', return_value=failure):
            with self.assertRaisesRegex(RuntimeError, 'complete history cache does not fit'):
                warmer.verify_history_capacity(self.specification)
        self.assertIn('budget exhausted', (self.root / 'capacity.log').read_text())


if __name__ == '__main__':
    unittest.main()
