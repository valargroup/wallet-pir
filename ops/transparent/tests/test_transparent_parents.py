"""Deployment staging rejects changed archives and corrupt evaluated bodies."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('stage', Path(__file__).parents[1] / 'scripts/stage-transparent-parents.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class StageTests(unittest.TestCase):
    def test_stage_and_reject_changed_or_corrupt_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            child = {'shard_id': 0, 'sealed': True, 'geometry': 'archive-wide', 'start_height': 0, 'end_height': 10}
            live = {'genesis_hash': '00'*32, 'profile': 'test', 'shards': [child]}
            (root/'map.json').write_text(json.dumps(live))
            body = b'\0'
            digest = hashlib.sha256(body).hexdigest()
            (root/'artifacts').mkdir()
            artifact = root/'artifacts'/f'{digest}.bin'
            artifact.write_bytes(body)
            parent = {'genesis_hash': live['genesis_hash'], 'profile': 'test', 'm': 100, 'p': 6,
                      'children': [dict(child)], 'filter_hash': digest, 'bytes': len(body)}
            manifest = {'schema': 'transparent-parent-evaluation-v1', 'parents': [parent]}
            candidate = root/'candidate.json'
            candidate.write_text(json.dumps(manifest))
            result = m.stage(candidate, (root/'map.json').as_uri(), root/'good')
            self.assertEqual(result['covered_children'], 1)
            self.assertEqual((root/'good/archive-wide.json').read_bytes(), candidate.read_bytes())
            artifact.write_bytes(b'bad')
            with self.assertRaises(ValueError):
                m.stage(candidate, (root/'map.json').as_uri(), root/'bad-body')
            self.assertFalse((root/'bad-body').exists())
            artifact.write_bytes(body)
            live['shards'][0]['end_height'] = 11
            (root/'map.json').write_text(json.dumps(live))
            with self.assertRaises(ValueError):
                m.stage(candidate, (root/'map.json').as_uri(), root/'bad-map')
            self.assertFalse((root/'bad-map').exists())


if __name__ == '__main__':
    unittest.main()
