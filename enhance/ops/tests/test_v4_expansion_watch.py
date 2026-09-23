"""The live test watcher may act only after a new, pinned capacity request."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/v4-isolated-expansion.py'
spec = importlib.util.spec_from_file_location('v4_isolated_expansion_watch', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def health(remaining, requested=None):
    request = {'id': requested, 'target_groups': 2, 'registered': False,
               'requested_at': 100, 'boundary_records': 5406720}
    return {'generation': 1, 'registered_groups': 1,
            'capacity': {'observation': {'records': 5406720 - remaining * 33, 'at': 100},
                         'remaining_rows': remaining, 'effective_rows_per_second': 1,
                         'readiness_seconds': 21600, 'burst_rows': 4096,
                         'requested': requested,
                         'requests': {requested: request} if requested else {}}}


class WatchTests(unittest.TestCase):
    def test_one_crossing_triggers_the_pinned_pair(self):
        with tempfile.TemporaryDirectory() as temp:
            trace = Path(temp) / 'capacity.jsonl'
            with patch.object(module, 'health', side_effect=[health(25697), health(25696, 'successor-5-pair-2')]), \
                    patch.object(module.time, 'sleep'):
                module.watch_request('http://127.0.0.1:8280', 'successor-5-pair-2', 2, trace, 30, 1)
            rows = [json.loads(line) for line in trace.read_text().splitlines()]
            self.assertEqual([row['event'] for row in rows], ['sample', 'sample', 'provision-start'])
            self.assertGreater(rows[0]['remaining_rows'], rows[0]['threshold_rows'])
            self.assertEqual(rows[1]['remaining_rows'], rows[1]['threshold_rows'])

    def test_rejects_existing_or_different_request(self):
        for responses in ([health(25696, 'successor-5-pair-2')],
                          [health(25697), health(25696, 'successor-5-pair-3')]):
            with self.subTest(responses=responses), tempfile.TemporaryDirectory() as temp:
                with patch.object(module, 'health', side_effect=responses), \
                        patch.object(module.time, 'sleep'):
                    with self.assertRaisesRegex(ValueError, 'pinned request'):
                        module.watch_request('http://127.0.0.1:8280', 'successor-5-pair-2', 2,
                                             Path(temp) / 'capacity.jsonl', 30, 1)

    def test_preserves_outside_sample_across_fixture_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            trace = Path(temp) / 'capacity.jsonl'
            with patch.object(module, 'health', side_effect=[
                    health(25697), urllib.error.URLError('fixture restart'),
                    health(25696, 'successor-5-pair-2')]), patch.object(module.time, 'sleep'):
                module.watch_request('http://127.0.0.1:8280', 'successor-5-pair-2', 2, trace, 30, 1)
            self.assertEqual([json.loads(line)['event'] for line in trace.read_text().splitlines()],
                             ['sample', 'health-unavailable', 'sample', 'provision-start'])


if __name__ == '__main__':
    unittest.main()
