"""Snapshot reuse preserves public inputs without loading another server."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'lib'))
from wallet_pir_ops.deploy import snapshot_warmer as warmer  # noqa: E402


class SnapshotFilesTests(unittest.TestCase):
    def test_object_aliases_pin_regular_files_and_detect_replaced_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, pinned, objects = root / 'candidate', root / 'pin', root / '.objects'
            (source / 'segments').mkdir(parents=True)
            objects.mkdir()
            body = b'published immutable artifact'
            target = objects / hashlib.sha256(body).hexdigest()
            target.write_bytes(body)
            for alias in (source / 'runtime', source / 'segments' / 'table.bin'):
                alias.symlink_to(os.path.relpath(target, alias.parent))
            self.assertEqual(warmer.pin(source, pinned), {'pinned_files': 2})
            self.assertFalse((pinned / 'runtime').is_symlink())
            self.assertTrue((pinned / 'runtime').samefile(target))
            self.assertTrue(warmer.same(source, pinned)['same'])
            replacement = root / 'replacement'
            replacement.write_bytes(body)
            os.replace(replacement, target)
            self.assertFalse(warmer.same(source, pinned)['same'])
            target.unlink()
            self.assertEqual((pinned / 'runtime').read_bytes(), body)

    def test_pin_refuses_escaping_and_indirect_object_aliases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, objects = root / 'source', root / '.objects'
            source.mkdir()
            objects.mkdir()
            target = objects / ('a' * 64)
            target.write_bytes(b'body')
            outside = root / ('b' * 64)
            outside.write_bytes(b'outside')
            alias = source / 'runtime'
            for index, link in enumerate((str(target), '../' + outside.name, '../.objects/../' + outside.name)):
                alias.symlink_to(link)
                with self.assertRaisesRegex(ValueError, 'unsupported object alias'):
                    warmer.pin(source, root / ('pin-' + str(index)))
                alias.unlink()
            target.unlink()
            target.symlink_to(outside)
            alias.symlink_to('../.objects/' + target.name)
            with self.assertRaisesRegex(ValueError, 'unsupported object alias'):
                warmer.pin(source, root / 'indirect-pin')

    def test_replacement_during_linking_cannot_create_a_valid_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, objects = root / 'source', root / '.objects'
            source.mkdir()
            objects.mkdir()
            target = objects / ('a' * 64)
            target.write_bytes(b'first')
            (source / 'runtime').symlink_to('../.objects/' + target.name)
            replacement = root / 'replacement'
            replacement.write_bytes(b'other')
            link = os.link
            def replace_then_link(*args, **kwargs):
                os.replace(replacement, target)
                link(*args, **kwargs)
            with mock.patch.object(warmer.os, 'link', side_effect=replace_then_link):
                with self.assertRaisesRegex(ValueError, 'changed while it was pinned'):
                    warmer.pin(source, root / 'pin')

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
