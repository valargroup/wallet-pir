"""Snapshot reuse preserves public inputs without loading another server."""
import json
import os
from pathlib import Path
import tempfile
import unittest

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

    def test_comparison_requires_each_pinned_inode_and_size(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, pinned = root / 'source', root / 'pin'
            (source / 'nested').mkdir(parents=True)
            (source / 'shards.json').write_text('{"public":true}')
            (source / 'nested' / 'table.bin').write_bytes(b'immutable table')
            warmer.pin(source, pinned)
            # Files the pin does not hold, such as a later restore record, are not compared.
            (source / 'history-restore.json').write_text('{}')
            self.assertEqual(warmer.same(source, pinned), {'compared': 2, 'same': True, 'differs': []})
            replacement = root / 'replacement'
            replacement.write_bytes(b'immutable table')
            os.replace(replacement, source / 'nested' / 'table.bin')
            (source / 'shards.json').unlink()
            self.assertEqual(warmer.same(source, pinned),
                             {'compared': 2, 'same': False, 'differs': ['nested/table.bin', 'shards.json']})
            with self.assertRaisesRegex(ValueError, 'regular directories'):
                warmer.same(source, root / 'missing')

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


if __name__ == '__main__':
    unittest.main()
