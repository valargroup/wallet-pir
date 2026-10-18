"""Fictional timing and raw RPC fixtures; no production qualification."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[3]/'ops/lib'), str(HERE.parents[1]/'lib')]
import activity_oracle_anchors as M


class Anchors(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(); self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name).resolve(); self.count = 0
        self.hashes = {h: format(h+100, '064x') for h in [*range(16), 3500738]}
        self.native = dict(started_unix=100, ended_unix=200, started_monotonic=10, ended_monotonic=20)
        self.owner = dict(started_unix=80, ended_unix=220, started_monotonic=8, ended_monotonic=22)

    def ref(self, value):
        self.count += 1
        raw = json.dumps(value).encode(); path = self.root/str(self.count)
        path.write_bytes(raw); path.chmod(0o400)
        return dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest())

    def fixture(self):
        attempts, timings = [], []
        for index, (unix, mono) in enumerate(((90,9), (150,15), (210,21))):
            calls = [dict(id=h, method='getblockhash', params=[h]) for h in self.hashes]
            replies = [dict(id=h, result=v, error=None) for h,v in self.hashes.items()]
            attempts.append(dict(status=200, request=self.ref(calls), response=self.ref(replies)))
            timings.append(dict(attempt_index=index, started_unix=unix, ended_unix=unix+.5,
                                started_monotonic=mono, ended_monotonic=mono+.05))
        return attempts, timings

    def run_fixture(self, attempts, timings, check=lambda:None):
        return M.verify(attempts, timings, self.hashes, self.native, self.owner, check)

    def test_all_sampled_hashes_surround_reader_in_both_clocks(self):
        attempts, timings = self.fixture(); result = self.run_fixture(attempts, timings)
        self.assertEqual(set(result['before'].values()), {0})
        self.assertEqual(set(result['after'].values()), {2})
        self.assertEqual(len(result['canonical_hashes']), 17)

    def test_in_reader_anchor_cannot_replace_after_proof(self):
        attempts, timings = self.fixture()
        with self.assertRaisesRegex(ValueError, 'coverage'):
            self.run_fixture(attempts[:2], timings[:2])
        timings[2].update(started_monotonic=19, ended_monotonic=19.5)
        with self.assertRaisesRegex(ValueError, 'coverage'):
            self.run_fixture(attempts, timings)

    def test_missing_height_wrong_hash_id_transport_and_tampered_bytes_refuse(self):
        for kind in ('missing', 'hash', 'id', 'status', 'tamper'):
            attempts, timings = self.fixture()
            responses = M.decode(attempts[2]['response'])
            if kind == 'missing': responses.pop()
            if kind == 'hash': responses[0]['result'] = 'f'*64
            if kind == 'id': responses[0]['id'] = responses[1]['id']
            attempts[2]['response'] = self.ref(responses)
            if kind == 'status': attempts[2]['status'] = 500
            if kind == 'tamper':
                path = Path(attempts[2]['response']['path'])
                path.chmod(0o600); path.write_text('{}'); path.chmod(0o400)
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                self.run_fixture(attempts, timings)

    def test_timing_gaps_order_overlap_nonfinite_and_owner_escape_refuse(self):
        for kind in ('missing', 'index', 'overlap', 'nonfinite', 'escape', 'boolean'):
            attempts, timings = self.fixture()
            if kind == 'missing': timings.pop()
            if kind == 'index': timings[1]['attempt_index'] = 0
            if kind == 'overlap': timings[1]['started_monotonic'] = 9.01
            if kind == 'nonfinite': timings[1]['ended_unix'] = float('nan')
            if kind == 'escape': timings[2]['ended_monotonic'] = 23
            if kind == 'boolean': timings[0]['started_unix'] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                self.run_fixture(attempts, timings)

    def test_resource_deadline_callback_and_changed_native_interval_refuse(self):
        attempts, timings = self.fixture()
        def refuse(): raise ValueError('fictional resource floor')
        with self.assertRaisesRegex(ValueError, 'fictional resource floor'):
            self.run_fixture(attempts, timings, refuse)
        native = copy.deepcopy(self.native); native['ended_unix'] = 230
        with self.assertRaisesRegex(ValueError, 'escapes'):
            M.verify(attempts, timings, self.hashes, native, self.owner, lambda:None)


if __name__ == '__main__': unittest.main()
