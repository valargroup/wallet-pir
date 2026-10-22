"""Artifact conversion cannot strand old authority behind new executables."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from fake_fleet import FakeFleet, sha256
from wallet_pir_ops.deploy import artifact_cutover as A, descriptors
from wallet_pir_ops.deploy.engine import DeployError, mutation_fence
from wallet_pir_ops.deploy.remote import production_lock


class Fleet(FakeFleet):
    def __init__(self):
        super().__init__()
        self.fail_exact = False
        self.unknown = False
        self.events = []
        self.interrupt_stop = False

    def systemctl(self, host, *arguments):
        super().systemctl(host, *arguments)
        if arguments[0] == 'stop' and self.interrupt_stop:
            self.interrupt_stop = False
            raise KeyboardInterrupt('lost cutover owner after completed stop')

    def http_get(self, host, url, timeout=5):
        if url == 'https://publisher.example/.well-known/pir-service-discovery':
            return 200, '{"service":"pir-publisher","version":"2.0.0"}'
        return super().http_get(host, url, timeout)

    def run(self, host, argv, timeout):
        if len(argv) > 2 and argv[1] == 'artifact-checksum':
            self.events.append(argv[2])
            if argv[2] in ('verify-prepared', 'install-prepared'):
                return 0, '{"verified_files":1}'
            argument = lambda name: argv[argv.index(name) + 1]
            if argv[2] == 'prepare-record':
                value = json.loads(self.read(host, argument('--input')))
                if '--reverse' in argv and self.unknown:
                    return 1, 'latest publication has no verified reverse translation'
                value['algorithm'] = 'sha256' if '--reverse' in argv else 'blake3'
                self.put(host, argument('--output'), json.dumps(value))
                return 0, '{}'
            if argv[2] == 'install-record':
                target = argument('--destination')
                if self.sha256(host, target) != argument('--destination-sha256'):
                    return 1, 'record changed'
                self.put(host, target, self.host(host).files[argument('--input')])
                return 0, '{}'
        if argv[0] == '/bin/exact' and self.fail_exact:
            self.fail_exact = False
            value = json.loads(self.read(host, '/state/current.json'))
            value.update(revision=108, eligible=False, revoked=['new withdrawal'])
            self.put(host, '/state/current.json', json.dumps(value))
            return 1, 'encrypted answer mismatch'
        return super().run(host, argv, timeout)


class ArtifactCutoverTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.state = Path(self.directory.name)
        self.ex = Fleet()
        self.registry = {}
        services = {}
        groups = []
        for index, name in enumerate(['publisher', *sorted(A.SERVICES - {'publisher'})]):
            unit = name + '.service'
            health = f'http://127.0.0.1:{8000 + index}/health'
            role = descriptors.Role('server', unit, 'exec-drop-in', health)
            service = descriptors.Service(name, '/opt/' + name, 'service', ('server',), {'server':role})
            self.registry[name] = service
            services[name] = {'roles':{'server':[{'host':'coordinator'}]},
                              'exact_check':{'host':'coordinator', 'argv':['/bin/exact']}}
            self.ex.install('coordinator', unit, '[Service]\nExecStart=/old/' + name + '\n',
                            files={'/old/' + name:('old-' + name).encode()})
            self.ex.endpoints[('coordinator', health)] = unit
            candidate = self.state / name
            candidate.write_bytes(('new-' + name).encode())
            groups.append({'service':name, 'targets':['server'], 'binary':str(candidate),
                           'binary_sha256':sha256(candidate.read_bytes()), 'binary_bytes':candidate.stat().st_size})
        self.inventory = descriptors.Inventory({'coordinator':{}}, {'mode':'config'},
                                               {'type':'remote','host':'coordinator'}, services)
        self.ex.put('coordinator', '/helper', b'helper')
        self.ex.put('coordinator', '/state/current.json', json.dumps({'algorithm':'sha256',
                    'revision':92, 'eligible':True, 'revoked':[]}))
        helper = {'host':'coordinator','helper':'/helper','helper_sha256':sha256(b'helper')}
        self.request = {'schema':'artifact-checksum-cutover/1','source_sha':'a'*40,'groups':groups,
            'preparations':[{**helper,'root':'/state','stage':'/stage'}],
            'records':[{**helper,'kind':'transparent-state','input':'/state/current.json',
                        'output':'/stage/current.json','preparations':['/stage/prepared.json']}],
            'discovery':{'host':'coordinator','url':'https://publisher.example/.well-known/pir-service-discovery'}}
        self.target = self.make_target()
        for _, deployer in self.target.groups:
            deployer.refresh_baseline()

    def make_target(self, request=None):
        return A.Cutover(request or self.request, self.inventory, self.registry, self.ex, self.state,
                         poll_seconds=0)

    def test_conversion_precedes_publisher_then_consumers_and_repeated_rollback_is_safe(self):
        result = self.target.deploy(self.target.plan()['plan_sha256'])
        self.assertEqual(result['status'], 'committed')
        restarts = [detail[1] for _, operation, detail in self.ex.log
                    if operation == 'systemctl' and detail[0] == 'restart']
        self.assertEqual(restarts[0], 'publisher.service')
        self.assertEqual(json.loads(self.ex.read('coordinator', '/state/current.json'))['algorithm'], 'blake3')
        self.assertIsNone(self.ex.read('coordinator', A.FENCE))
        self.assertEqual(self.target.rollback(result['id'])['status'], 'rolled-back')
        before = len(self.ex.log)
        self.target.rollback(result['id'])
        self.assertEqual(len(self.ex.log), before)

    def test_failed_acceptance_keeps_new_withdrawal_when_old_binaries_are_restored(self):
        self.ex.fail_exact = True
        with self.assertRaisesRegex(DeployError, 'exact-answer'):
            self.target.deploy(self.target.plan()['plan_sha256'])
        current = json.loads(self.ex.read('coordinator', '/state/current.json'))
        self.assertEqual(current, {'algorithm':'sha256','revision':108,'eligible':False,'revoked':['new withdrawal']})
        for name in A.SERVICES:
            self.assertEqual(self.ex.probe_unit('coordinator', name + '.service')['exe_sha256'],
                             sha256(('old-' + name).encode()))

    def test_unknown_reverse_translation_refuses_before_stopping_candidate(self):
        result = self.target.deploy(self.target.plan()['plan_sha256'])
        self.ex.unknown = True
        before = len(self.ex.log)
        with self.assertRaisesRegex(DeployError, 'reverse translation'):
            self.target.rollback(result['id'])
        self.assertFalse(any(op == 'systemctl' for _, op, _ in self.ex.log[before:]))

    def test_incomplete_fleet_and_later_deployments_are_refused(self):
        request = copy.deepcopy(self.request)
        request['groups'].pop()
        with self.assertRaisesRegex(DeployError, 'every affected'):
            self.make_target(request)
        self.ex.put('coordinator', A.FENCE, '{"transaction":"unfinished"}')
        with self.assertRaisesRegex(DeployError, 'unfinished'):
            mutation_fence(self.inventory, self.ex)

    def test_lost_owner_is_recovered_by_a_new_runner_without_restoring_old_state(self):
        self.ex.interrupt_stop = True
        with self.assertRaises(KeyboardInterrupt):
            self.target.deploy(self.target.plan()['plan_sha256'])
        self.assertIsNotNone(self.ex.read('coordinator', A.FENCE))
        with self.assertRaisesRegex(Exception, 'unfinished'):
            with production_lock(self.inventory, self.ex):
                self.fail('another deployment acquired a fenced production lock')
        result = self.make_target().rollback()
        self.assertEqual(result['status'], 'rolled-back')
        self.assertIsNone(self.ex.read('coordinator', A.FENCE))
        for name in A.SERVICES:
            self.assertEqual(self.ex.probe_unit('coordinator', name + '.service')['exe_sha256'],
                             sha256(('old-' + name).encode()))


if __name__ == '__main__':
    unittest.main()
