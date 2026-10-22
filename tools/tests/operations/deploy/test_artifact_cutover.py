"""Artifact conversion cannot strand old authority behind new executables."""
import copy
import itertools
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


CONFIG = '/etc/wallet-pir/publisher.json'
INCIDENT = '/state/integrity-failures.json'


def publisher_config(root='/state', enabled=True):
    return json.dumps({'management_listen':'127.0.0.1:18080', 'transparent_history':{'enabled':enabled,
        'settings':{'publication_root':root, 'listen':'10.0.0.1:8096'}}})
# As the current publisher serializes a failure: tagged checksums.
CURRENT_INCIDENT = json.dumps([{'path':'a/directory.0.bin',
    'expected_checksum':{'algorithm':'blake3', 'digest':'77'*32},
    'found_checksum':{'algorithm':'blake3', 'digest':'88'*32},
    'inventory_checksum':{'algorithm':'blake3', 'digest':'99'*32},
    'map_sha256':'44'*32, 'reason':'body checksum differs'}])
PREDECESSOR_INCIDENT = json.dumps([{'path':'a/directory.0.bin', 'expected_sha256':'11'*32,
    'found_sha256':None, 'inventory_sha256':'33'*32, 'map_sha256':'44'*32, 'reason':'body checksum differs'}])


class Fleet(FakeFleet):
    def __init__(self):
        super().__init__()
        self.fail_exact = False
        self.unknown = False
        self.events = []
        self.interrupt_stop = False
        self.interrupt_reverse = False
        self.incident_on_stop = None
        self.interrupt_restart = False

    def systemctl(self, host, *arguments):
        if arguments[0] == 'restart' and self.interrupt_restart:
            self.interrupt_restart = False
            raise KeyboardInterrupt('lost rollback owner while resuming the current release')
        super().systemctl(host, *arguments)
        if arguments[0] == 'stop' and self.incident_on_stop and arguments[1] == 'publisher.service':
            # The validator records a failure while the publisher shuts down.
            self.put(host, INCIDENT, self.incident_on_stop)
            self.incident_on_stop = None
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
            if argv[2] in ('verify-prepared', 'install-prepared', 'verify-predecessors', 'restore-predecessors'):
                return 0, '{"verified_files":1}'
            argument = lambda name: argv[argv.index(name) + 1]
            if argv[2] == 'prepare':
                self.put(host, argument('--stage') + '/prepared.json', '{}')
                return 0, '{"inventory":{"checksum":{"algorithm":"blake3","digest":"' + 'a'*64 + '"},"bytes":2}}'
            if argv[2] == 'prepare-record':
                value = json.loads(self.read(host, argument('--input')))
                if '--reverse' in argv and self.unknown:
                    return 1, 'latest publication has no verified reverse translation'
                if '--reverse' in argv and value['algorithm'] != 'blake3':
                    # Like the real converter: reverse input needs tagged checksums.
                    return 1, 'reverse input is not a current-format record'
                value['algorithm'] = 'sha256' if '--reverse' in argv else 'blake3'
                self.put(host, argument('--output'), json.dumps(value))
                return 0, '{}'
            if argv[2] == 'install-record':
                target = argument('--destination')
                if self.sha256(host, target) != argument('--destination-sha256'):
                    return 1, 'record changed'
                self.put(host, target, self.host(host).files[argument('--input')])
                if self.interrupt_reverse and json.loads(self.read(host, target))['algorithm'] == 'sha256':
                    self.interrupt_reverse = False
                    raise KeyboardInterrupt('lost rollback owner after reverse record installation')
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
            # The publisher's aggregate readiness, which a retained incident holds false.
            ready = {'field':'ready', 'equals':True} if name == 'publisher' else None
            role = descriptors.Role('server', unit, 'exec-drop-in', health, ready=ready)
            service = descriptors.Service(name, '/opt/' + name, 'service', ('server',), {'server':role})
            self.registry[name] = service
            services[name] = {'roles':{'server':[{'host':'coordinator'}]},
                              'exact_check':{'host':'coordinator', 'argv':['/bin/exact']}}
            arguments = ' --config ' + CONFIG if name == 'publisher' else ''
            self.ex.install('coordinator', unit, '[Service]\nExecStart=/old/' + name + arguments + '\n',
                            files={'/old/' + name:('old-' + name).encode()})
            self.ex.endpoints[('coordinator', health)] = unit
            candidate = self.state / name
            candidate.write_bytes(('new-' + name).encode())
            groups.append({'service':name, 'targets':['server'], 'binary':str(candidate),
                           'binary_sha256':sha256(candidate.read_bytes()), 'binary_bytes':candidate.stat().st_size})
        self.inventory = descriptors.Inventory({'coordinator':{}}, {'mode':'config'},
                                               {'type':'remote','host':'coordinator'}, services)
        self.ex.put('coordinator', '/helper', b'helper')
        self.ex.put('coordinator', CONFIG, publisher_config())
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
        # Each poll advances a second, so a readiness deadline expires at once.
        return A.Cutover(request or self.request, self.inventory, self.registry, self.ex, self.state,
                         poll_seconds=0, clock=itertools.count().__next__, sleep=lambda _: None)

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

    def test_preparation_keeps_predecessors_running_and_requires_a_fresh_activation_plan(self):
        group, deployer = self.target.groups[0]
        entry = self.request['preparations'][0]
        entry.update(helper=deployer.release_binary(group['binary_sha256']),
                     helper_sha256=group['binary_sha256'], source={'product':'status',
                     'inventory':'{}','inventory_sha256':sha256(b'{}'),'checkpoints':'/stage/checkpoints'})
        before = self.target.plan()['plan_sha256']
        self.assertEqual(self.target.prepare(before)['status'], 'prepared')
        self.assertNotEqual(self.target.preflight()['plan_sha256'], before)
        self.assertFalse(any(op == 'systemctl' for _,op,_ in self.ex.log))
        self.assertEqual(self.ex.read('coordinator', '/stage/predecessor-input.json'), '{}')
        self.assertIsNone(self.ex.read('coordinator', A.FENCE))

    def test_failed_acceptance_keeps_new_withdrawal_when_old_binaries_are_restored(self):
        self.ex.fail_exact = True
        with self.assertRaisesRegex(DeployError, 'exact-answer'):
            self.target.deploy(self.target.plan()['plan_sha256'])
        current = json.loads(self.ex.read('coordinator', '/state/current.json'))
        self.assertEqual(current, {'algorithm':'sha256','revision':108,'eligible':False,'revoked':['new withdrawal']})
        for name in A.SERVICES:
            self.assertEqual(self.ex.probe_unit('coordinator', name + '.service')['exe_sha256'],
                             sha256(('old-' + name).encode()))

    def test_interrupted_reverse_installation_resumes_and_keeps_newer_withdrawal(self):
        for window in ('before-install', 'after-install'):
            with self.subTest(window=window):
                self.setUp()
                result = self.target.deploy(self.target.plan()['plan_sha256'])
                advanced = {'algorithm':'blake3', 'revision':108, 'eligible':False, 'revoked':['new withdrawal']}
                self.ex.put('coordinator', '/state/current.json', json.dumps(advanced))
                if window == 'after-install':
                    self.ex.interrupt_reverse = True
                else:
                    def lost(entry, evidence):
                        raise KeyboardInterrupt('lost rollback owner before reverse record installation')
                    self.target.install_record = lost
                with self.assertRaises(KeyboardInterrupt):
                    self.target.rollback(result['id'])
                for name in A.SERVICES:
                    self.assertFalse(self.ex.probe_unit('coordinator', name + '.service')['main_pid'])
                self.assertEqual(self.make_target().rollback(result['id'])['status'], 'rolled-back')
                current = json.loads(self.ex.read('coordinator', '/state/current.json'))
                self.assertEqual(current, {**advanced, 'algorithm':'sha256'})
                for name in A.SERVICES:
                    self.assertEqual(self.ex.probe_unit('coordinator', name + '.service')['exe_sha256'],
                                     sha256(('old-' + name).encode()))

    def test_unjournaled_predecessor_format_record_is_not_accepted_as_reversed(self):
        result = self.target.deploy(self.target.plan()['plan_sha256'])
        foreign = {'algorithm':'sha256', 'revision':108, 'eligible':True, 'revoked':[]}
        self.ex.put('coordinator', '/state/current.json', json.dumps(foreign))
        before = len(self.ex.log)
        with self.assertRaisesRegex(DeployError, 'current-format'):
            self.target.rollback(result['id'])
        self.assertFalse(any(op == 'systemctl' for _, op, _ in self.ex.log[before:]))

    def test_unknown_reverse_translation_refuses_before_stopping_candidate(self):
        result = self.target.deploy(self.target.plan()['plan_sha256'])
        self.ex.unknown = True
        before = len(self.ex.log)
        with self.assertRaisesRegex(DeployError, 'reverse translation'):
            self.target.rollback(result['id'])
        self.assertFalse(any(op == 'systemctl' for _, op, _ in self.ex.log[before:]))

    def assert_serving(self, prefix):
        for name in A.SERVICES:
            state = self.ex.probe_unit('coordinator', name + '.service')
            self.assertEqual((state['active_state'], state['exe_sha256']),
                             ('active', sha256((prefix + name).encode())))

    def test_current_format_incident_refuses_rollback_before_stopping(self):
        result = self.target.deploy(self.target.plan()['plan_sha256'])
        self.ex.put('coordinator', INCIDENT, CURRENT_INCIDENT)
        before = len(self.ex.log)
        with self.assertRaisesRegex(DeployError, 'predecessor cannot read.*roll forward'):
            self.target.rollback(result['id'])
        self.assertFalse(any(op in ('systemctl', 'write') for _, op, _ in self.ex.log[before:]))
        self.assertEqual(self.ex.read('coordinator', INCIDENT), CURRENT_INCIDENT)
        self.assert_serving('new-')
        for document in ('[{}]', '{}', 'not json'):
            self.ex.put('coordinator', INCIDENT, document)
            with self.assertRaisesRegex(DeployError, 'predecessor cannot read'):
                self.make_target().rollback(result['id'])
        self.assert_serving('new-')

    def test_predecessor_format_incident_survives_rollback_unchanged(self):
        result = self.target.deploy(self.target.plan()['plan_sha256'])
        self.ex.put('coordinator', INCIDENT, PREDECESSOR_INCIDENT)
        self.assertEqual(self.target.rollback(result['id'])['status'], 'rolled-back')
        self.assertEqual(self.ex.read('coordinator', INCIDENT), PREDECESSOR_INCIDENT)
        self.assert_serving('old-')

    def test_incident_recorded_during_shutdown_resumes_current_release(self):
        for lost in (False, True):
            with self.subTest(lost_runner=lost):
                self.setUp()
                # The retained incident keeps the resumed publisher unready.
                result = self.deploy_with_unready_publisher()
                advanced = {'algorithm':'blake3', 'revision':108, 'eligible':False, 'revoked':['new withdrawal']}
                self.ex.put('coordinator', '/state/current.json', json.dumps(advanced))
                self.ex.incident_on_stop = CURRENT_INCIDENT
                self.ex.interrupt_restart = lost
                if lost:
                    with self.assertRaises(KeyboardInterrupt):
                        self.target.rollback(result['id'])
                    # A new runner finds writers stopped and the record stable.
                    self.target = self.make_target()
                with self.assertRaisesRegex(DeployError, 'resumed the current release.*roll forward'):
                    self.target.rollback(result['id'])
                self.assert_serving('new-')
                self.assertEqual(self.ex.read('coordinator', INCIDENT), CURRENT_INCIDENT)
                self.assertEqual(json.loads(self.ex.read('coordinator', '/state/current.json')), advanced)
                self.assertNotIn('restore-predecessors', self.ex.events)
                journal = A.Journal.load(self.state, A.NAME, result['id'])
                self.assertEqual(journal.status, 'committed')
                self.assertNotIn('rollback', journal.data)
                # Once the current release clears the failure, rollback completes.
                self.ex.put('coordinator', INCIDENT, '[]')
                self.assertEqual(self.make_target().rollback(result['id'])['status'], 'rolled-back')
                self.assertEqual(json.loads(self.ex.read('coordinator', '/state/current.json')),
                                 {**advanced, 'algorithm':'sha256'})
                self.assert_serving('old-')

    def deploy_with_unready_publisher(self):
        """A committed cutover whose publisher then reports aggregate ready=false."""
        result = self.target.deploy(self.target.plan()['plan_sha256'])
        self.ex.unhealthy.add(('coordinator', self.request['groups'][0]['binary_sha256']))
        status, body = self.ex.http_get('coordinator', 'http://127.0.0.1:8000/health')
        self.assertEqual((status, json.loads(body)['ready']), (200, False))
        return result

    def test_resume_refuses_dead_or_wrong_executable_and_a_retry_recovers(self):
        release = self.target.groups[0][1].release_binary(self.request['groups'][0]['binary_sha256'])
        for case in ('dead', 'wrong'):
            with self.subTest(case=case):
                self.setUp()
                result = self.deploy_with_unready_publisher()
                self.ex.incident_on_stop = CURRENT_INCIDENT
                if case == 'dead':
                    self.ex.failing_restart.add(('coordinator', 'publisher.service'))
                    reason = 'unit is failed'
                else:
                    original = self.ex.host('coordinator').files[release]
                    self.ex.put('coordinator', release, b'another publisher')
                    reason = 'running executable is ' + sha256(b'another publisher')
                with self.assertRaisesRegex(DeployError, 'not verified.*' + reason):
                    self.target.rollback(result['id'])
                # Consumers wait for the publisher; the journal still owns recovery.
                self.assertFalse(self.ex.probe_unit('coordinator', 'status.service')['main_pid'])
                self.assertEqual(A.Journal.load(self.state, A.NAME, result['id']).status, 'rolling-back')
                if case == 'dead':
                    self.ex.failing_restart.clear()
                else:
                    self.ex.put('coordinator', release, original)
                with self.assertRaisesRegex(DeployError, 'resumed the current release'):
                    self.make_target().rollback(result['id'])
                self.assert_serving('new-')
                self.assertEqual(self.ex.read('coordinator', INCIDENT), CURRENT_INCIDENT)
                self.assertEqual(A.Journal.load(self.state, A.NAME, result['id']).status, 'committed')

    def refuses_before_stopping(self, result, pattern='predecessor cannot read'):
        before = len(self.ex.log)
        with self.assertRaisesRegex(DeployError, pattern):
            self.make_target().rollback(result['id'])
        self.assertFalse(any(op in ('systemctl', 'write') for _, op, _ in self.ex.log[before:]))
        self.assert_serving('new-')

    def test_history_incident_records_follow_the_publisher_configuration(self):
        # The request names no incident and its preparation has no product.
        self.assertNotIn('source', self.request['preparations'][0])
        self.ex.put('coordinator', CONFIG, publisher_config('/srv/history'))
        result = self.make_target().deploy(self.make_target().plan()['plan_sha256'])
        pinned = A.Journal.load(self.state, A.NAME, result['id']).data['incidents']
        self.assertEqual(pinned, [{'host':'coordinator', 'path':'/srv/history/integrity-failures.json'}])
        # A clean record on another same-host path proves nothing.
        self.ex.put('coordinator', INCIDENT, '[]')
        self.ex.put('coordinator', '/srv/history/integrity-failures.json', CURRENT_INCIDENT)
        self.refuses_before_stopping(result)
        # An on-disk root move cannot prove what the running publisher loaded.
        self.ex.put('coordinator', CONFIG, publisher_config('/srv/moved'))
        self.refuses_before_stopping(result, 'publisher configuration changed')
        self.ex.put('coordinator', '/srv/history/integrity-failures.json', '[]')
        self.ex.put('coordinator', '/srv/moved/integrity-failures.json', CURRENT_INCIDENT)
        self.refuses_before_stopping(result, 'publisher configuration changed')
        # Without a readable configuration nothing proves which roots exist.
        self.ex.remove('coordinator', '/srv/moved/integrity-failures.json')
        self.ex.remove('coordinator', CONFIG)
        self.refuses_before_stopping(result, 'cannot derive Transparent History incident records')
        # Disabling it on disk also does not prove the running process stopped using it.
        self.ex.put('coordinator', CONFIG, publisher_config('/srv/moved', enabled=False))
        self.refuses_before_stopping(result, 'publisher configuration changed')
        self.ex.put('coordinator', CONFIG, publisher_config('/srv/history'))
        self.assertEqual(self.make_target().rollback(result['id'])['status'], 'rolled-back')
        self.assert_serving('old-')

    def test_publisher_without_its_configuration_cannot_plan(self):
        self.ex.put('coordinator', CONFIG, 'not json')
        with self.assertRaisesRegex(DeployError, 'not a readable publisher configuration'):
            self.target.plan()
        request = {**copy.deepcopy(self.request), 'incidents':[]}
        path = self.state / 'request.json'
        path.write_text(json.dumps(request))
        with self.assertRaisesRegex(DeployError, 'request fields differ'):
            A.load_request(path, sha256(path.read_bytes()))

    def test_publisher_configuration_drift_refuses_before_interruption(self):
        result = self.target.deploy(self.target.plan()['plan_sha256'])
        # A running invocation can still use B while the file already names C.
        # Neither that edit nor disabling History proves which root it loaded.
        for document in (publisher_config('/srv/b'), publisher_config('/srv/c'),
                         publisher_config('/state', enabled=False)):
            with self.subTest(document=document):
                self.ex.put('coordinator', CONFIG, document)
                self.refuses_before_stopping(result, 'publisher configuration changed')
        self.ex.put('coordinator', CONFIG, publisher_config())
        self.assertEqual(self.make_target().rollback(result['id'])['status'], 'rolled-back')
        self.assert_serving('old-')

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
