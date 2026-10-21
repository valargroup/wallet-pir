import importlib.util
import json
import os
import pathlib
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    'reclaim', pathlib.Path(__file__).with_name('transparent-provider-reclaim-scan.py'))
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


if __name__ == '__main__':
    unittest.main()
