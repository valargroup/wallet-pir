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


deploy = module('cuda_deploy', 'enhance/ops/scripts/deploy-cuda.py')
verify = module('cuda_verify', 'enhance/ops/scripts/verify-cuda-deployment.py')


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


class FeeOracleRepair(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.current, self.replacement = self.root/'oracle.json', self.root/'new.json'
        self.evidence = self.root/'evidence'
        self.evidence.mkdir()
        def row(fee=None):
            raw = bytearray(653)
            if fee is not None:
                raw[640] = 4
                raw[645:653] = fee.to_bytes(8,'little')
            return raw.hex()
        self.old = [{'position':0,'record_hex':row()}, {'position':33,'record_hex':row(15000)}]
        self.new = [{'position':0,'record_hex':row(20000)}, self.old[1]]
        self.current.write_text(json.dumps(self.old))
        self.replacement.write_text(json.dumps(self.new))
        self.current.chmod(0o600)
        self.replacement.chmod(0o600)
        self.writes = []
        original = Path.lstat
        self.owner_stat = lambda path: SimpleNamespace(st_uid=0,st_mode=original(path).st_mode)

    def repair(self, expected=None, held=None):
        def write(path, data, mode):
            self.writes.append(path)
            Path(path).write_bytes(data)
            Path(path).chmod(mode)
        digest = expected or hashlib.sha256(self.replacement.read_bytes()).hexdigest()
        with patch.object(Path, 'lstat', self.owner_stat):
            return deploy.repair_reviewed_fee_oracle(self.current,self.replacement,digest,
                                                     held or Mock(),self.evidence,write)

    def test_fee_only_repair_preserves_cases_and_retains_prior_bytes(self):
        original = self.current.read_bytes()
        expected = self.replacement.read_bytes()
        result = self.repair()
        self.assertEqual(self.current.read_bytes(), expected)
        self.assertEqual((self.evidence/'oracle-before.json').read_bytes(),original)
        self.assertEqual((self.evidence/'oracle-after.json').read_bytes(),expected)
        self.assertEqual(result['positions'],2)
        self.assertEqual(result['fee_metadata_updates'],1)
        self.assertEqual(self.current.stat().st_mode & 0o777,0o600)

    def test_oracle_repair_refuses_ciphertext_expiry_known_fee_and_coverage_changes(self):
        original = self.current.read_bytes()
        for case in ['ciphertext','expiry','known-fee','position','drop-case']:
            bad = copy.deepcopy(self.new)
            if case in ['ciphertext','expiry']:
                raw = bytearray.fromhex(bad[0]['record_hex'])
                raw[0 if case=='ciphertext' else 641] ^= 1
                bad[0]['record_hex'] = raw.hex()
            elif case=='known-fee':
                raw = bytearray.fromhex(bad[1]['record_hex']);raw[645] ^= 1
                bad[1]['record_hex'] = raw.hex()
            elif case=='position':
                bad[0]['position'] = 1
            else:
                bad.pop()
            self.replacement.write_text(json.dumps(bad))
            with self.assertRaises(Exception):self.repair()
            self.assertEqual(self.current.read_bytes(),original)
            self.assertEqual(self.writes,[])

    def test_oracle_repair_refuses_wrong_digest_and_lost_lock(self):
        original = self.current.read_bytes()
        with self.assertRaisesRegex(Exception,'reviewed SHA-256'):self.repair(expected='0'*64)
        held = Mock();held.verify.side_effect = RuntimeError('lost lock')
        with self.assertRaisesRegex(RuntimeError,'lost lock'):self.repair(held=held)
        self.assertEqual(self.current.read_bytes(),original)
        self.assertEqual(self.writes,[])
        self.assertEqual(list(self.evidence.iterdir()),[])


class CudaFleet(Fleet):
    def reviewed_state(self):
        runner = self.enhance_fleet()
        runner.targets = runner.targets[:1]
        runner.capture_baseline()
        target = runner.targets[0]
        current = copy.deepcopy(self.fake.probe_unit(target.host, target.unit))
        old_binary = units.split_exec(units.exec_start(units.effective(
            [current['fragment_text']] + [d['text'] for d in current['drop_ins']])))[1]
        new_binary = runner.service.release_binary(NEW_SHA)
        current['exe_sha256'] = NEW_SHA
        current['fragment_text'] = current['fragment_text'].replace(old_binary, new_binary)
        for item in current['drop_ins']:
            item['text'] = item['text'].replace(old_binary, new_binary)
        evidence = self.dir/'evidence'
        evidence.mkdir()
        return runner, current, evidence

    def test_reviewed_baseline_adoption_retains_old_bytes_and_ledger(self):
        runner, current, evidence = self.reviewed_state()
        original = runner.baseline_path.read_bytes()
        pointer = self.state/'latest-enhance.json'
        pointer.write_text('retained transaction pointer')
        with patch.object(runner, 'probe', return_value={runner.targets[0].key: current}):
            result = deploy.adopt_reviewed_baseline(runner, current, NEW_SHA, Mock(), evidence)
        self.assertEqual((evidence/'baseline-before.json').read_bytes(), original)
        self.assertEqual(result['previous_baseline_sha256'], hashlib.sha256(original).hexdigest())
        self.assertEqual(pointer.read_text(), 'retained transaction pointer')
        self.assertEqual(json.loads(runner.baseline_path.read_text())['targets'][runner.targets[0].key]['exe_sha256'], NEW_SHA)
        self.assertEqual(self.restarts(), [])
        self.assertIsNone(deploy.adopt_reviewed_baseline(runner, current, NEW_SHA, Mock(), evidence))

    def test_baseline_adoption_requires_the_explicit_matching_running_hash(self):
        runner, current, evidence = self.reviewed_state()
        original = runner.baseline_path.read_bytes()
        self.assertIsNone(deploy.adopt_reviewed_baseline(runner, current, None, Mock(), evidence))
        with self.assertRaisesRegex(Exception, 'expected-current-sha256'):
            deploy.adopt_reviewed_baseline(runner, current, OLD_SHA, Mock(), evidence)
        self.assertEqual(runner.baseline_path.read_bytes(), original)
        self.assertEqual(list(evidence.iterdir()), [])

    def test_reviewed_baseline_adoption_refuses_argument_limit_and_file_drift(self):
        runner, current, evidence = self.reviewed_state()
        original = runner.baseline_path.read_bytes()
        for change in ['argument', 'limit', 'file']:
            bad = copy.deepcopy(current)
            if change == 'argument':
                binary = runner.service.release_binary(NEW_SHA)
                bad['fragment_text'] = bad['fragment_text'].replace(binary, binary + ' --unreviewed')
                for item in bad['drop_ins']:
                    item['text'] = item['text'].replace(binary, binary + ' --unreviewed')
            elif change == 'limit':
                bad['fragment_text'] += 'MemoryMax=4G\n'
            else:
                bad['fragment_path'] += '.changed'
            with self.assertRaisesRegex(Exception, 'executable path|unit file paths'):
                deploy.adopt_reviewed_baseline(runner, bad, NEW_SHA, Mock(), evidence)
            self.assertEqual(runner.baseline_path.read_bytes(), original)
        self.assertEqual(list(evidence.iterdir()), [])

    def test_reviewed_baseline_adoption_refuses_a_lost_lock(self):
        runner, current, evidence = self.reviewed_state()
        original = runner.baseline_path.read_bytes()
        held = Mock()
        held.verify.side_effect = RuntimeError('lost lock')
        with self.assertRaisesRegex(RuntimeError, 'lost lock'):
            deploy.adopt_reviewed_baseline(runner, current, NEW_SHA, held, evidence)
        self.assertEqual(runner.baseline_path.read_bytes(), original)
        self.assertEqual(list(evidence.iterdir()), [])

    def test_gpu_rollback_refuses_paths_outside_its_canonical_ledger(self):
        runner = self.enhance_fleet()
        with patch.object(deploy.Journal, 'load') as load:
            with self.assertRaisesRegex(Exception, 'canonical GPU transaction'):
                deploy.rollback_verified_gpu(runner, '../other-ledger/transaction', Mock())
            load.assert_not_called()

    def test_gpu_rollback_refuses_a_transaction_for_other_targets(self):
        runner = self.enhance_fleet()
        runner.targets = runner.targets[:1]
        foreign = SimpleNamespace(hosts=[{'host':'unrelated','unit':'other.service'}])
        with patch.object(deploy.Journal, 'load', return_value=foreign), patch.object(runner, 'rollback') as rollback:
            with self.assertRaisesRegex(Exception, 'exactly the selected GPU'):
                deploy.rollback_verified_gpu(runner, 'enhance-explicit-transaction', Mock())
            rollback.assert_not_called()

    def test_gpu_rollback_cannot_pass_when_public_verification_fails(self):
        runner = self.enhance_fleet()
        runner.targets = runner.targets[:1]
        target = runner.targets[0]
        journal = SimpleNamespace(hosts=[{'host':target.host,'unit':target.unit,
                                        'previous':{'exe_sha256':OLD_SHA}}])
        runner.inventory.services['enhance']['exact_check'] = {
            'host':'coordinator','argv':['verifier','--sha256',NEW_SHA],'timeout':60}
        with patch.object(deploy.Journal, 'load', return_value=journal), \
                patch.object(runner, 'rollback', return_value=journal), \
                patch.object(runner, 'run_exact_check', side_effect=ValueError('public oracle failed')):
            with self.assertRaisesRegex(ValueError, 'public oracle failed'):
                deploy.rollback_verified_gpu(runner, 'enhance-explicit-transaction', Mock())

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
