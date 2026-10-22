"""CUDA target restrictions, readiness and verification on repeat deployments."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch, Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_wallet_pir_deploy import Fleet, OLD_SHA, NEW_SHA, ENHANCE, ROOT, SERVICES
from wallet_pir_ops.deploy import descriptors, units
from wallet_pir_ops.deploy.remote import SSHExecutor


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / file)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


deploy = module('cuda_deploy', 'tools/enhance/scripts/deploy-cuda.py')
verify = module('cuda_verify', 'tools/enhance/scripts/verify-cuda-deployment.py')


class CoordinatorPreflight(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.args = SimpleNamespace(ssh_key=self.root/'key', known_hosts=self.root/'hosts',
                                    inventory=self.root/'inventory', state_dir=self.root/'state')
        for file in [self.args.ssh_key, self.args.known_hosts, self.args.inventory]:
            file.write_text('isolated fixture')
            file.chmod(0o600)
        self.args.state_dir.mkdir(mode=0o700)
        original = Path.lstat
        self.stat = lambda path: SimpleNamespace(st_mode=original(path).st_mode, st_uid=0)
        self.addCleanup(patch.stopall)
        patch.object(deploy.shutil, 'which', return_value='/fixture/tool').start()

    def check(self):
        with patch.object(Path, 'lstat', self.stat):
            deploy.preflight_inputs(self.args)

    def test_private_inputs_are_accepted_without_writing_evidence(self):
        before = sorted(self.root.iterdir())
        self.check()
        self.assertEqual(sorted(self.root.iterdir()), before)

    def test_broad_key_and_state_permissions_fail(self):
        for path in [self.args.ssh_key, self.args.state_dir]:
            path.chmod(0o755)
            with self.assertRaisesRegex(ValueError, 'permissions|private'):
                self.check()
            path.chmod(0o700 if path.is_dir() else 0o600)

    def test_locked_preparation_tightens_existing_ledger_without_replacing_it(self):
        path = self.args.state_dir
        retained = path / 'transaction.json'
        retained.write_text('retained receipt')
        before = (path.stat().st_ino, retained.stat().st_ino)
        path.chmod(0o755)
        held = Mock()
        original = deploy.os.fstat
        with patch.object(deploy.os, 'fstat', side_effect=lambda fd: SimpleNamespace(st_uid=0, st_mode=original(fd).st_mode)):
            deploy.prepare_state_directory(path, held)
        held.verify.assert_called_once()
        self.assertEqual(path.stat().st_mode & 0o777, 0o700)
        self.assertEqual((path.stat().st_ino, retained.stat().st_ino), before)
        self.assertEqual(retained.read_text(), 'retained receipt')
        self.check()

    def test_state_preparation_refuses_lost_lock_and_untrusted_directories(self):
        path = self.args.state_dir
        path.chmod(0o755)
        held = Mock()
        held.verify.side_effect = RuntimeError('lost lock')
        with self.assertRaisesRegex(RuntimeError, 'lost lock'):
            deploy.prepare_state_directory(path, held)
        self.assertEqual(path.stat().st_mode & 0o777, 0o755)
        held = Mock()
        for owner, mode in [(1, 0o40755), (0, 0o40777)]:
            with patch.object(deploy.os, 'fstat', return_value=SimpleNamespace(st_uid=owner, st_mode=mode)):
                with self.assertRaisesRegex(ValueError, 'root-owned'):
                    deploy.prepare_state_directory(path, held)
            self.assertEqual(path.stat().st_mode & 0o777, 0o755)
        path.rmdir()
        path.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError):
            deploy.prepare_state_directory(path, held)

    def test_symlink_non_owner_and_missing_capability_fail(self):
        self.args.ssh_key.unlink()
        self.args.ssh_key.symlink_to(self.args.inventory)
        with self.assertRaisesRegex(ValueError, 'regular file'):
            self.check()
        self.args.ssh_key.unlink()
        self.args.ssh_key.write_text('fixture')
        self.args.ssh_key.chmod(0o600)
        with patch.object(Path, 'lstat', new=lambda path: SimpleNamespace(st_mode=0o100600, st_uid=1)):
            with self.assertRaisesRegex(ValueError, 'root-owned'):
                deploy.preflight_inputs(self.args)
        with patch.object(deploy.shutil, 'which', return_value=None):
            with self.assertRaisesRegex(ValueError, 'ssh and scp'):
                self.check()


class CudaReadiness(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.worker_url = 'http://gpu'
        self.config = {'router_url': 'http://router'}
        self.worker_ready = [False, True]
        self.router_ready = True
        self.identity = {'pid': 7, 'oom': 0, 'restarts': 0}

    def fetch(self, url):
        if url.startswith(self.worker_url):
            ready = self.worker_ready.pop(0) if len(self.worker_ready) > 1 else self.worker_ready[0]
            return json.dumps({'protocol': deploy.PROTOCOL, 'active': 'generation-11', 'ready': ready,
                               'matvec': {'matvec_backend': 'cuda', 'cuda_device': 0}})
        return json.dumps({'active': 'ab' * 32, 'ready': self.router_ready})

    def wait(self, budget=60, **extra):
        def sleep(seconds):
            self.now += seconds
        return verify.wait_for_readiness(self.worker_url, self.config, lambda: self.identity,
                                         {'pid': 7, 'oom': 0, 'restarts': 0}, budget,
                                         fetcher=self.fetch, clock=lambda: self.now, sleep=sleep, **extra)

    def test_restarted_worker_may_recover_within_the_shared_budget(self):
        worker, router, remaining = self.wait(recover=True)
        self.assertTrue(worker['ready'] and router['ready'])
        self.assertEqual(self.now, 2)
        self.assertEqual(remaining, 58)

    def test_recovery_cannot_exceed_the_shared_budget(self):
        self.worker_ready = [False]
        with self.assertRaisesRegex(ValueError, 'shared readiness budget'):
            self.wait(budget=10, recover=True)
        self.assertEqual(self.now, 10)

    def test_post_query_readiness_loss_and_identity_drift_are_immediate_failures(self):
        self.worker_ready = [True]
        self.router_ready = False
        with self.assertRaisesRegex(ValueError, 'router is not ready'):
            self.wait()
        self.assertEqual(self.now, 0)
        self.router_ready = True
        self.identity = {'pid': 8, 'oom': 0, 'restarts': 0}
        with self.assertRaisesRegex(ValueError, 'identity, OOM or restart'):
            self.wait()


class CudaInventory(unittest.TestCase):
    def document(self, known_hosts):
        return {'lock': {'type': 'pinned_host', 'machine_id': 'fake-coordinator'}, 'ssh': {
            'mode': 'pinned', 'key': '/key', 'known_hosts': str(known_hosts),
            'known_hosts_sha256': hashlib.sha256(known_hosts.read_bytes()).hexdigest()},
            'hosts': {'coordinator': {'address': '192.0.2.1'}, 'enhance-pir-gpu-01': {
                'address': '192.0.2.2', 'user': 'paperspace', 'sudo': True, 'jump': 'coordinator'}},
            'services': {'enhance': {'roles': {'worker': [{'host': 'enhance-pir-gpu-01',
                'unit': 'enhance-pir-gpu-worker.service', 'vars': {'listen': '192.0.2.2:8091'},
                'health_equals': deploy.EXPECTED}]}, 'cuda_validation': {'origin': 'https://example.test',
                'oracle': '/oracle.json', 'router_url': 'http://192.0.2.3:8093'}}}}

    def test_target_restrictions_and_pinned_jump(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            known_hosts = root / 'known_hosts'
            known_hosts.write_text('verified keys\n')
            document = self.document(known_hosts)
            path = root / 'inventory.json'
            path.write_text(json.dumps(document))
            inventory = descriptors.load_inventory(path)
            deploy.validate_inventory(inventory, SERVICES['enhance'])
            argv = SSHExecutor(inventory).transport('enhance-pir-gpu-01')
            self.assertEqual(argv[-1], 'paperspace@192.0.2.2')
            self.assertTrue(any('ProxyCommand=' in value and 'root@192.0.2.1' in value for value in argv))
            for change in ['cpu', 'extra', 'wrong_backend']:
                bad = copy.deepcopy(document)
                role = bad['services']['enhance']['roles']['worker'][0]
                if change == 'cpu':
                    role['host'] = 'coordinator'
                elif change == 'extra':
                    bad['services']['enhance']['roles']['coordinator'] = [{'host': 'coordinator'}]
                else:
                    role['health_equals']['matvec.matvec_backend'] = 'cpu'
                path.write_text(json.dumps(bad))
                with self.assertRaises(ValueError):
                    deploy.validate_inventory(descriptors.load_inventory(path), SERVICES['enhance'])
            known_hosts.write_text('changed keys\n')
            with self.assertRaises(ValueError):
                SSHExecutor(inventory).transport('enhance-pir-gpu-01')

    def test_verifier_rejects_cpu_unready_and_warmup_failures(self):
        report = {'protocol': deploy.PROTOCOL, 'exact_answer_oracle': True, 'completed': 60,
                  'succeeded': 60, 'incorrect_answers': 0, 'errors': {}, 'unstarted_arrivals': 0,
                  'warmup_incorrect_answers': 0, 'warmup_errors': {}}
        verify.validate_report(report)
        report['warmup_errors'] = {'503': 1}
        with self.assertRaises(ValueError):
            verify.validate_report(report)
        worker = {'protocol': deploy.PROTOCOL, 'matvec': {'matvec_backend': 'cuda', 'cuda_device': 0},
                  'active': 'generation-1', 'ready': True}
        router = {'active': 'ab' * 32, 'ready': True}
        verify.validate_health(worker, router)
        for name, change in {'cpu': {'matvec': {'matvec_backend': 'cpu', 'cuda_device': None}},
                             'unready': {'ready': False}, 'no publication': {'active': None}}.items():
            with self.subTest(name), self.assertRaises(ValueError):
                verify.validate_health({**worker, **change}, router)
        with self.assertRaisesRegex(ValueError, 'router is not ready'):
            verify.validate_health(worker, {**router, 'ready': False})


if __name__ == '__main__':
    unittest.main()
