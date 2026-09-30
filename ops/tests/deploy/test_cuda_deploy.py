"""CUDA target restrictions, readiness and verification on repeat deployments."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_wallet_pir_deploy import Fleet, OLD_SHA, NEW_SHA, ENHANCE, ROOT, SERVICES
from wallet_pir_ops.deploy import descriptors
from wallet_pir_ops.deploy.remote import SSHExecutor


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / file)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


deploy = module('cuda_deploy', 'enhance/ops/scripts/deploy-cuda.py')
verify = module('cuda_verify', 'enhance/ops/scripts/verify-cuda-deployment.py')


class CudaFleet(Fleet):
    def test_repeat_validation_checks_exact_answers_without_restarting(self):
        runner = self.enhance_fleet()
        result = runner.deploy(OLD_SHA, verify_noop=True)
        self.assertEqual(result.status, 'committed')
        self.assertEqual(self.restarts(), [])
        self.assertEqual(len(self.fake.runs), 1)

    def test_nested_health_and_strict_scalar_types(self):
        runner = self.enhance_fleet()
        target = runner.targets[0]
        record = {'host': target.host, 'unit': target.unit, 'verify': {
            'health': target.url(target.role.health), 'health_equals': deploy.EXPECTED}}
        health = {'protocol': deploy.PROTOCOL, 'matvec': {'matvec_backend': 'cuda', 'cuda_device': 0}}
        with patch.object(self.fake, 'http_get', return_value=(200, json.dumps(health))):
            self.assertTrue(runner.check(record, OLD_SHA)[0])
        for bad in [True, 1, None]:
            health['matvec']['cuda_device'] = bad
            with patch.object(self.fake, 'http_get', return_value=(200, json.dumps(health))):
                self.assertFalse(runner.check(record, OLD_SHA)[0])

    def test_backend_mismatch_rolls_back_selected_target(self):
        runner = self.enhance_fleet()
        runner.targets = runner.targets[:1]
        runner.targets[0].health_equals['matvec.matvec_backend'] = 'cuda'
        original = self.fake.http_get
        def health(host, url, timeout=5):
            status, body = original(host, url, timeout)
            document = json.loads(body)
            state = self.fake.host(host).units[runner.targets[0].unit]
            document['matvec'] = {'matvec_backend': 'cpu' if state['exe'] == NEW_SHA else 'cuda'}
            return status, json.dumps(document)
        with patch.object(self.fake, 'http_get', side_effect=health):
            with self.assertRaisesRegex(Exception, 'not verified'):
                runner.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(self.fake.probe_unit(*ENHANCE[0])['exe_sha256'], OLD_SHA)
        self.assertTrue(all(host == ENHANCE[0][0] for host, _ in self.restarts()))


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
                'oracle': '/oracle.json', 'router_url': 'http://192.0.2.3:8093',
                'coordinator_url': 'http://127.0.0.1:8080'}}}}

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

    def test_verifier_rejects_cpu_routing_and_warmup_failures(self):
        report = {'protocol': deploy.PROTOCOL, 'exact_answer_oracle': True, 'completed': 60,
                  'succeeded': 60, 'incorrect_answers': 0, 'errors': {}, 'unstarted_arrivals': 0,
                  'warmup_incorrect_answers': 0, 'warmup_errors': {}}
        verify.validate_report(report)
        report['warmup_errors'] = {'503': 1}
        with self.assertRaises(ValueError):
            verify.validate_report(report)
        worker = {'protocol': deploy.PROTOCOL, 'matvec': {'matvec_backend': 'cuda', 'cuda_device': 0},
                  'published': [1]}
        router = {'protocol': deploy.PROTOCOL, 'ready': True, 'preferred_workers': {'http://gpu': False}}
        with self.assertRaises(ValueError):
            verify.validate_health(worker, router, 'http://gpu')


if __name__ == '__main__':
    unittest.main()
