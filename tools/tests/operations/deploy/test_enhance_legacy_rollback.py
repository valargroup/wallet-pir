"""A recorded legacy CUDA predecessor keeps its own readiness contract."""
import copy
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'tools/lib'))
from tools.enhance.lib.deploy_prerequisites import Guard
from wallet_pir_ops.deploy.engine import Deployer


class LegacyRollback(unittest.TestCase):
    def setUp(self):
        self.digest = 'a' * 64
        self.record = {
            'host': 'gpu', 'unit': 'worker.service', 'role': 'worker', 'phase': 'restoring',
            'previous': {'exe_sha256': self.digest, 'exec_binary': '/release/enhance-pir-server'},
            'verify': {'health': 'http://worker/health', 'ready': {'field': 'ready', 'equals': True},
                       'health_equals': {'protocol': 'ironwood-enhance-pir-v9-native-two-mask-m29',
                                         'matvec.matvec_backend': 'cuda', 'matvec.cuda_device': 0}},
        }
        self.health = {'binary_sha256': self.digest,
                       'protocol': 'ironwood-enhance-pir-v9-native-two-mask-m29',
                       'matvec': {'matvec_backend': 'cuda', 'cuda_device': 0},
                       'published': [1, 2], 'published_manifest_digests': {'1': 'b' * 64, '2': 'c' * 64},
                       'resident_database_bytes': 1024}
        self.process = {'active_state': 'active', 'exe_sha256': self.digest}
        self.deployer = object.__new__(Deployer)
        self.deployer.ex = SimpleNamespace(probe_unit=lambda *_: self.process,
                                          http_get=lambda *_: (200, json.dumps(self.health)))
        self.deployer.service = SimpleNamespace(roles={
            'worker': SimpleNamespace(product_guard=lambda: Guard())})

    def check(self, expected=None):
        return self.deployer.check(self.record, expected or self.digest)[0]

    def test_restored_pinned_cuda_holdings_pass(self):
        self.assertTrue(self.check())

    def test_forward_activation_cannot_use_old_readiness(self):
        self.record['phase'] = 'verifying'
        self.assertFalse(self.check())

    def test_another_binary_or_process_cannot_use_old_readiness(self):
        for changed in ('process', 'reported', 'expected'):
            with self.subTest(changed=changed):
                self.setUp()
                if changed == 'process': self.process['exe_sha256'] = 'd' * 64
                if changed == 'reported': self.health['binary_sha256'] = 'd' * 64
                self.assertFalse(self.check('d' * 64 if changed == 'expected' else None))

    def test_explicit_unready_is_never_replaced(self):
        self.health['ready'] = False
        self.assertFalse(self.check())

    def test_autonomous_predecessor_still_requires_ready(self):
        self.record['previous']['exec_binary'] = '/release/pir-query-server'
        self.assertFalse(self.check())

    def test_missing_or_inconsistent_holdings_refuse(self):
        changes = ({'published': []}, {'published': [1, 1]}, {'published': [True]},
                   {'published_manifest_digests': {'1': 'b' * 64}},
                   {'published_manifest_digests': {'1': 'invalid', '2': 'c' * 64}},
                   {'resident_database_bytes': 0})
        original = copy.deepcopy(self.health)
        for change in changes:
            with self.subTest(change=change):
                self.health = original | change
                self.assertFalse(self.check())

    def test_backend_and_protocol_checks_still_apply(self):
        for change in ({'protocol': 'different'}, {'matvec': {'matvec_backend': 'cpu', 'cuda_device': 0}}):
            with self.subTest(change=change):
                self.setUp()
                self.health.update(change)
                self.assertFalse(self.check())

    def test_current_candidate_passes_only_with_normal_readiness(self):
        self.record['phase'] = 'verifying'
        self.process['exe_sha256'] = self.health['binary_sha256'] = 'd' * 64
        self.health['ready'] = True
        self.assertTrue(self.check('d' * 64))


if __name__ == '__main__':
    unittest.main()
