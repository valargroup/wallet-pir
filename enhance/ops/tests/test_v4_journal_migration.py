"""Offline packing migration preserves records and rejects unsafe source state."""
import fcntl
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    'migration', Path(__file__).parents[1] / 'scripts/migrate-v4-journal.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class MigrationTests(unittest.TestCase):
    def fixture(self, root):
        source = root / 'source'
        source.mkdir()
        (source / 'journal.lock').touch()
        (source / 'records.bin').write_bytes(bytes(range(256)) * 5 + b'x' * 194)
        manifest = {
            'version': 7, 'table': 'enhance', 'record_bytes': 737,
            'records_per_row': 29, 'tree_size': 2,
            'blocks': [{'height': 10, 'hash': 'a' * 64,
                        'first_position': 0, 'action_count': 2}],
        }
        (source / 'manifest.json').write_text(json.dumps(manifest))
        return source

    def test_preserves_source_records_and_blocks_without_artifacts(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = self.fixture(root)
            before = (source / 'manifest.json').read_bytes()
            destination = root / 'new'
            (source / 'compiled-cache').write_bytes(b'not migrated')
            receipt = module.migrate(source, destination)
            self.assertEqual((source / 'manifest.json').read_bytes(), before)
            self.assertEqual((source / 'records.bin').read_bytes(),
                             (destination / 'records.bin').read_bytes())
            self.assertEqual(json.loads((destination / 'manifest.json').read_text()),
                             dict(json.loads(before), records_per_row=33))
            self.assertEqual(receipt['records'], 2)
            self.assertFalse((destination / 'compiled-cache').exists())
            with self.assertRaises(ValueError):
                module.migrate(source, destination)

    def test_locked_or_inconsistent_source_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = self.fixture(root)
            with (source / 'journal.lock').open('r+b') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaises(BlockingIOError):
                    module.migrate(source, root / 'locked')
            (source / 'records.bin').write_bytes(b'short')
            with self.assertRaises(ValueError):
                module.migrate(source, root / 'short')
            self.assertFalse((root / 'short').exists())

    def test_bad_positions_format_and_symlinks_are_rejected(self):
        for change in ({'first_position': 1}, {'action_count': -1}, {'hash': 'invalid'}):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                source = self.fixture(root)
                path = source / 'manifest.json'
                manifest = json.loads(path.read_text())
                manifest['blocks'][0].update(change)
                path.write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    module.migrate(source, root / 'new')
                self.assertFalse((root / 'new').exists())
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = self.fixture(root)
            path = source / 'manifest.json'
            path.rename(source / 'original.json')
            path.symlink_to(source / 'original.json')
            with self.assertRaises(ValueError):
                module.migrate(source, root / 'new')
