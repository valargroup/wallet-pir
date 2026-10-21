import importlib.util
import json
import os
import pathlib
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    'reclaim', pathlib.Path(__file__).parents[1] / 'transparent/scripts/transparent-provider-reclaim-scan.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Tests(unittest.TestCase):
    def test_counts_shared_inodes_once_and_only_unretained_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp).resolve()
            objects = root / 'objects'
            objects.mkdir()
            names = ['%064x' % n for n in range(6)]
            for name in names:
                (objects / name).write_bytes(os.urandom(4096))
            revision = 'a' * 64
            (root / '.sources' / revision).mkdir(parents=True)
            os.link(objects / names[0], root / '.sources' / revision / 'rows.bin')
            for n in (1, 2, 10):
                stage = root / ('staging-%d-100' % n)
                stage.mkdir()
                os.link(objects / names[n if n < 10 else 3], stage / 'shards.json')
            # The active tree is older than the newest stage and links the catalog.
            os.symlink('../.sources/' + revision, root / 'staging-2-100' / revision)
            (root / 'active.json').write_text(json.dumps({'directory': str(root / 'staging-2-100')}))
            for n in (1, 2, 10):
                os.utime(root / ('staging-%d-100' % n), ns=(n * 1000000000, n * 1000000000))
            before = {p: p.stat().st_mtime_ns for p in root.rglob('*') if not p.is_symlink()}

            report = m.scan(str(root), 1)

            self.assertEqual(report['retained'], ['staging-10-100', 'staging-2-100'])
            self.assertEqual(report['retained_catalog_revisions'], 1)
            # Objects 0, 2 and 3 are linked by retained trees; 1 only by staging-1,
            # 4 and 5 by nothing.
            self.assertEqual(report['reachable_bytes'], 3 * 4096)
            self.assertEqual(report['reclaimable_bytes'], 3 * 4096)
            self.assertEqual(report['allocated_bytes'], 6 * 4096)
            self.assertEqual(before, {p: p.stat().st_mtime_ns for p in root.rglob('*') if not p.is_symlink()})

    def provider(self, root, copied=False):
        objects = root / 'objects'
        objects.mkdir()
        stage = root / 'staging-1-100'
        stage.mkdir()
        cached = objects / ('a' * 64)
        cached.write_bytes(b'map' * 4096)
        if copied:
            (stage / 'shards.json').write_bytes(cached.read_bytes())
        else:
            os.link(cached, stage / 'shards.json')
        (root / 'active.json').write_text(json.dumps({'directory': str(stage)}))
        return objects, stage

    def test_preserves_partial_foreign_and_externally_linked_cache_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp).resolve()
            objects, _ = self.provider(root)
            for name in ('b' * 64 + '.partial-1-2', 'notes', 'c' * 64):
                (objects / name).write_bytes(b'keep' * 2048)
            os.link(objects / ('c' * 64), root / 'external-link')
            os.symlink(root / 'external-link', objects / ('d' * 64))

            report = m.scan(str(root), 7)

            self.assertEqual(report['reclaimable_bytes'], 0)
            self.assertGreater(report['preserved_other_bytes'], 0)
            self.assertFalse(report['object_collection_skipped'])

    def test_copy_fallback_keeps_cache_but_collects_unused_tree_and_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp).resolve()
            objects, _ = self.provider(root, copied=True)
            old = root / 'staging-0-100'
            old.mkdir()
            (old / 'rows').write_bytes(b'old' * 4096)
            catalog = root / '.sources' / ('b' * 64)
            catalog.mkdir(parents=True)
            (catalog / 'rows').write_bytes(b'rows' * 4096)
            (objects / ('e' * 64)).write_bytes(b'cached' * 4096)
            expected = sum(p.stat().st_blocks * 512 for p in (old / 'rows', catalog / 'rows'))

            report = m.scan(str(root), 0)

            self.assertTrue(report['object_collection_skipped'])
            self.assertEqual(report['reclaimable_bytes'], expected)
            self.assertGreater(report['preserved_other_bytes'], 0)

    def test_counts_bytes_only_after_all_links_are_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp).resolve()
            objects, _ = self.provider(root)
            old = root / 'staging-0-100'
            old.mkdir()
            doomed = objects / ('b' * 64)
            doomed.write_bytes(b'obsolete' * 4096)
            os.link(doomed, old / 'one')
            os.link(doomed, old / 'two')
            held = objects / ('c' * 64)
            held.write_bytes(b'external' * 4096)
            os.link(held, old / 'held')
            os.link(held, root / 'outside-collected-directories')
            expected = doomed.stat().st_blocks * 512

            report = m.scan(str(root), 0)

            self.assertEqual(report['reclaimable_bytes'], expected)
            self.assertGreater(report['preserved_other_bytes'], 0)

    def test_mtime_retention_and_conservative_catalog_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp).resolve()
            _, active = self.provider(root)
            revision = 'b' * 64
            (active / revision).write_text('malformed reference still protects catalog')
            catalog = root / '.sources' / revision
            catalog.mkdir(parents=True)
            (catalog / 'rows').write_bytes(b'keep' * 4096)
            for name, modified in [('staging-2-100', 20), ('staging-99-100', 10)]:
                tree = root / name
                tree.mkdir()
                (tree / 'rows').write_bytes(b'rows' * 4096)
                os.utime(tree, ns=(modified * 1000000000, modified * 1000000000))
            report = m.scan(str(root), 1)
            self.assertEqual(report['retained'], ['staging-1-100', 'staging-2-100'])
            self.assertEqual(report['retained_catalog_revisions'], 1)
            self.assertTrue(report['retention_is_estimate'])
            with self.assertRaisesRegex(ValueError, 'nonnegative'):
                m.scan(str(root), -1)


if __name__ == '__main__':
    unittest.main()
