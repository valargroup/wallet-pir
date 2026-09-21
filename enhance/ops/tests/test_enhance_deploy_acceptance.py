"""Exercise the actual deployment predicate, including exact-release operator waivers."""
import json
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[3]

class DeploymentAcceptance(unittest.TestCase):
    def setUp(self):
        programs = subprocess.check_output([str(ROOT / 'enhance/ops/scripts/deploy-enhance-pir.sh'), 'jq-programs']).decode()
        self.predicate = dict(row.split('\t', 1) for row in programs.split('\0') if row)['JQ_ENHANCE_RELEASE_ACCEPTED']
        self.revision = 'a' * 40
        self.receipt = dict(revision=self.revision, worker_size='c-4', shards_per_group=3,
                            acceptance='operator', waive_qualification=True, waive_initial_observation=True,
                            authorized_by='test operator', authorized_at='2026-09-14T00:00:00Z', reason='test waiver')

    def accepted(self):
        return subprocess.run(['jq', '-e', '--arg', 'revision', self.revision, self.predicate],
                              input=json.dumps(self.receipt), text=True, capture_output=True).returncode == 0

    def test_explicit_waiver_does_not_require_claiming_passed_tests(self):
        self.assertTrue(self.accepted())
        self.receipt['revision'] = 'b' * 40
        self.assertFalse(self.accepted())

    def test_partial_or_unattributed_waivers_fail(self):
        for key in ('waive_qualification', 'waive_initial_observation', 'authorized_by', 'authorized_at', 'reason'):
            original = self.receipt.pop(key)
            self.assertFalse(self.accepted(), key)
            self.receipt[key] = original
        self.receipt['reason'] = '   '
        self.assertFalse(self.accepted())

    def test_qualified_receipts_still_require_all_evidence(self):
        self.receipt = dict(revision=self.revision, worker_size='c-4', shards_per_group=3,
                            passed=True, full_capacity=True, failover=True, online_append=True, memory=True,
                            seconds=21600, publications=300)
        self.assertTrue(self.accepted())
        self.receipt['seconds'] = 21599
        self.assertFalse(self.accepted())

if __name__ == '__main__':
    unittest.main()
