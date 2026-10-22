"""Status HTTPS cutover preserves other products and restores failed attempts."""
import copy
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.status.lib.https_cutover import Cutover, CADDY, INVENTORY, UNITS, WINDOW, configuration, edge, load_request
from tools.tests.operations.deploy.fake_fleet import FakeFleet
from wallet_pir_ops import durable
from wallet_pir_ops.deploy.descriptors import Inventory
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.transaction import Journal


def sha(data):
    return hashlib.sha256(data).hexdigest()


class Host(FakeFleet):
    def __init__(self):
        super().__init__()
        self.fail_new_checks = False
        self.old_publisher = None
        self.directories = set()

    def systemctl(self, host, *args):
        if args == ('reload', 'caddy.service'):
            self._mutate(host, 'systemctl', args)
            if self.host(host).units['caddy.service']['active'] != 'active':
                raise RuntimeError('Caddy is not active')
            return
        if args[0] == 'start':
            self._mutate(host, 'systemctl', args)
            self._start(host, args[1])
            return
        super().systemctl(host, *args)

    def run(self, host, argv, timeout):
        self.runs.append((host, list(argv)))
        if argv == ['cat', '/etc/machine-id']:
            return 0, ('1' if host == 'coordinator' else '2') * 32
        if argv[:3] == ['stat', '-c', '%a:%u']:
            return 0, '644:0'
        if argv[0] == 'python3':
            return 0, 'present' if len(argv) > 4 and argv[3] in self.directories else 'absent'
        if argv[:2] == ['install', '-d']:
            self.directories.add(argv[-1])
            return 0, ''
        if argv[0] == 'systemctl':
            self.systemctl(host, *argv[1:])
            return 0, ''
        if argv[0] == 'exact':
            new = self.host('coordinator').units['pir-publisher.service']['exe'] != self.old_publisher
            return (1, 'wrong encrypted answer') if new and self.fail_new_checks else (0, 'exact answers')
        raise AssertionError(argv)


class CutoverTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.host = Host()
        self.clock = 0
        self.locked = False
        self.checks = {name: {'host': 'coordinator', 'argv': ['exact', name], 'timeout': 10}
                       for name in ('enhance', 'status', 'transparent_history', 'transaction_details')}
        self.inventory = Inventory(hosts={'coordinator': {}, 'status': {}}, ssh={},
                                   lock={'type': 'remote', 'host': 'coordinator'},
                                   services={'publisher': {'exact_check': {'products': self.checks}}})
        self.request = {'schema': 'status-https-cutover/1', 'source_sha': 'a' * 40,
                        'public_origin': 'https://wallet.example', 'machine_ids': {'coordinator': '1' * 32, 'status': '2' * 32},
                        'roles': {}, 'checks': self.checks}
        for name, unit in UNITS.items():
            host = 'coordinator' if name == 'publisher' else 'status'
            binary_name = 'pir-publisher' if name == 'publisher' else 'pir-query-server'
            old, new = (name + '-old').encode(), (name + '-new').encode()
            old_binary = '/opt/status-pir/releases/' + sha(old) + '/' + binary_name
            new_binary = '/opt/status-pir/releases/' + sha(new) + '/' + binary_name
            path = '/etc/wallet-pir/publisher.json' if name == 'publisher' else '/etc/status-pir/' + name + '.json'
            if name == 'publisher':
                before = {'management_listen': '127.0.0.1:18080', 'enhance': {'untouched': True},
                          'status': {'enabled': True, 'settings': {'publisher': {'state_dir': '/srv/status', 'salt_hex': 'ab' * 32,
                                     'private_listen': '127.0.0.1:8480', 'window_blocks': 64,
                                     'worker_origin': 'http://127.0.0.1:8481', 'router_origin': 'http://127.0.0.1:8482'}}}}
                after = copy.deepcopy(before)
                for field in ('worker_origin', 'router_origin'):
                    after['status']['settings']['publisher'].pop(field)
            else:
                before = {'management_listen': '127.0.0.1:18481' if name == 'worker' else '127.0.0.1:18482',
                          'status': {'enabled': True, 'settings': {'role': name, 'network_hex': 'ab' * 32,
                                     'listen': '127.0.0.1:8481' if name == 'worker' else '127.0.0.1:8482',
                                     'state_dir': '/srv/status-' + name, 'artifact_origin': 'http://127.0.0.1:8495',
                                     'worker_origin': 'http://127.0.0.1:8481'}}}
                after = copy.deepcopy(before)
                settings = after['status']['settings']
                settings.pop('artifact_origin')
                settings['publisher'] = self.request['public_origin']
                settings['state_dir'] += '-https'
            text = json.dumps(before)
            self.host.put(host, new_binary, new)
            self.host.put(host, path, text)
            self.host.install(host, unit, '[Service]\nUser=root\nExecStart=' + old_binary + ' --config ' + path + '\n', files={old_binary: old})
            self.request['roles'][name] = {'host': host, 'unit': unit, 'binary': new_binary, 'binary_sha256': sha(new),
                                           'config_path': path, 'config_sha256': sha(text.encode()), 'configuration': after}
            port = '8480/control/active' if name == 'publisher' else ('8481' if name == 'worker' else '8482') + '/control/health'
            self.host.endpoints[(host, 'http://127.0.0.1:' + port)] = unit
        self.host.old_publisher = self.host.probe_unit('coordinator', UNITS['publisher'])['exe_sha256']
        self.host.put('coordinator', CADDY, 'wallet.example {\n handle /v1/status/* {\n reverse_proxy 127.0.0.1:8492\n }\n handle /v1/enhance/* { reverse_proxy 127.0.0.1:8080 }\n}\n')
        self.host.install('coordinator', 'caddy.service', '[Service]\nExecStart=/usr/bin/caddy\n', files={'/usr/bin/caddy': b'caddy'})
        installed = {'services': {'publisher': self.inventory.services['publisher'], 'status': {'roles': {
            name: [{'host': 'status', 'vars': {'artifact_origin': 'http://127.0.0.1:8495'}}] for name in ('worker', 'router')}}}}
        self.host.put('coordinator', INVENTORY, json.dumps(installed))
        self.before = {(host, path): data for host, h in self.host.hosts.items() for path, data in h.files.items()}
        self.cutover = Cutover(self.inventory, self.host, Path(self.root.name), self.request, lock=self.lock,
                               clock=lambda: self.clock, sleep=self.sleep)
        self.fence = patch('tools.status.lib.https_cutover.mutation_fence')
        self.fence.start()
        self.addCleanup(self.fence.stop)

    def sleep(self, seconds):
        self.clock += seconds

    @contextmanager
    def lock(self):
        self.assertFalse(self.locked)
        self.locked = True
        try:
            yield self
        finally:
            self.locked = False

    def verify(self):
        self.assertTrue(self.locked)

    def test_plan_is_read_only_and_success_preserves_wallet_routes(self):
        mark = len(self.host.log)
        plan = self.cutover.preflight()
        self.assertEqual(len(self.host.log), mark)
        result = self.cutover.deploy(plan['plan_sha256'])
        self.assertEqual(result['phase'], 'committed')
        text = self.host.read('coordinator', CADDY)
        self.assertIn('handle /provider/status/v1/*', text)
        self.assertIn('handle /v1/enhance/* { reverse_proxy 127.0.0.1:8080 }', text)
        self.cutover.rollback(result['transaction'])
        self.assertEqual(self.host.read('coordinator', CADDY).encode(), self.before[('coordinator', CADDY)])
        self.assertTrue(self.host.directories, 'rollback must retain consumer safety state')

    def test_wrong_answers_restore_previous_binaries_and_configuration(self):
        self.host.fail_new_checks = True
        with self.assertRaisesRegex(DeployError, 'wrong encrypted answer'):
            self.cutover.deploy(self.cutover.plan()['plan_sha256'])
        self.assertEqual(Journal.load(Path(self.root.name), 'status-https').status, 'rolled-back')
        for name, role in self.request['roles'].items():
            self.assertEqual(self.host.read(role['host'], role['config_path']).encode(), self.before[(role['host'], role['config_path'])])
            self.assertNotEqual(self.host.probe_unit(role['host'], role['unit'])['exe_sha256'], role['binary_sha256'])

    def interrupted_stopped_candidate(self):
        plan = self.cutover.plan()
        self.host.fail_new_checks = True
        with patch.object(self.cutover, 'restore', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.cutover.deploy(plan['plan_sha256'])
        for role in self.request['roles'].values():
            self.host.systemctl(role['host'], 'stop', role['unit'])
        return plan

    def test_resume_reapplies_partial_restoration_and_starts_candidate(self):
        plan = self.interrupted_stopped_candidate()
        journal = Journal.load(Path(self.root.name), 'status-https')
        item = journal.data['plan']['files'][0]
        self.host.write(item['host'], item['path'], item['before'].encode(), item['mode'])
        self.host.fail_new_checks = False
        self.assertEqual(self.cutover.resume(plan['plan_sha256'])['phase'], 'committed')
        for role in self.request['roles'].values():
            self.assertEqual(self.host.probe_unit(role['host'], role['unit'])['exe_sha256'], role['binary_sha256'])

    def test_resume_failure_holds_candidate_without_restoring_it(self):
        plan = self.interrupted_stopped_candidate()
        with self.assertRaisesRegex(DeployError, 'wrong encrypted answer'):
            self.cutover.resume(plan['plan_sha256'])
        self.assertEqual(Journal.load(Path(self.root.name), 'status-https').status, 'held')
        for role in self.request['roles'].values():
            self.assertEqual(self.host.probe_unit(role['host'], role['unit'])['exe_sha256'], role['binary_sha256'])

    def test_resume_refuses_foreign_file_before_starting_services(self):
        plan = self.interrupted_stopped_candidate()
        role = self.request['roles']['worker']
        self.host.write(role['host'], role['config_path'], b'foreign configuration', 0o644)
        with self.assertRaisesRegex(DeployError, 'foreign managed-file'):
            self.cutover.resume(plan['plan_sha256'])
        for role in self.request['roles'].values():
            self.assertEqual(self.host.probe_unit(role['host'], role['unit'])['active_state'], 'inactive')

    def test_twenty_minute_failure_starts_rollback_without_erasing_new_state(self):
        role = self.request['roles']['worker']
        self.host.unhealthy.add((role['host'], role['binary_sha256']))
        with self.assertRaisesRegex(DeployError, 'deadline'):
            self.cutover.deploy(self.cutover.plan()['plan_sha256'])
        self.assertGreaterEqual(self.clock, WINDOW)
        self.assertEqual(Journal.load(Path(self.root.name), 'status-https').status, 'rolled-back')
        self.assertTrue(self.host.directories)

    def test_last_answer_check_cannot_commit_after_recovery_deadline(self):
        original = self.host.run

        def delayed(host, argv, timeout=30):
            result = original(host, argv, timeout)
            installed = self.host.probe_unit('coordinator', UNITS['publisher'])['exe_sha256']
            if argv == ['exact', 'transaction_details'] and installed != self.host.old_publisher:
                self.clock += WINDOW
            return result

        self.host.run = delayed
        with self.assertRaisesRegex(DeployError, 'recovery window expired'):
            self.cutover.deploy(self.cutover.plan()['plan_sha256'])
        self.assertEqual(Journal.load(Path(self.root.name), 'status-https').status, 'rolled-back')

    def test_slow_stops_restore_without_installing_expired_candidates(self):
        original = self.host.systemctl
        original_run = self.host.run
        stops = 0
        candidate_starts = []
        stop_timeouts = []

        def bounded(host, argv, timeout):
            if argv[:2] == ['systemctl', 'stop']:
                stop_timeouts.append(timeout)
            return original_run(host, argv, timeout)

        def delayed(host, *args):
            nonlocal stops
            original(host, *args)
            if args[0] == 'stop' and stops < 3:
                stops += 1
                self.clock += 401
            if args[0] == 'start':
                installed = self.host.probe_unit(host, args[1])['exe_sha256']
                if any(role['unit'] == args[1] and role['binary_sha256'] == installed
                       for role in self.request['roles'].values()):
                    candidate_starts.append(args[1])

        self.host.systemctl = delayed
        self.host.run = bounded
        with self.assertRaisesRegex(DeployError, 'deadline|recovery window expired'):
            self.cutover.deploy(self.cutover.plan()['plan_sha256'])
        self.assertEqual(candidate_starts, [])
        self.assertEqual(stop_timeouts, [600, 600, 398])
        self.assertEqual(Journal.load(Path(self.root.name), 'status-https').status, 'rolled-back')

    def test_expiration_while_saving_intent_prevents_forward_service_changes(self):
        original = self.cutover.save
        original_control = self.host.systemctl
        forward_controls = []

        def expired(journal, status=None):
            original(journal, status)
            if journal.status == 'preparing' and journal.data.get('intent') == 'stop publisher':
                self.clock = WINDOW

        def tracked(host, *args):
            if Journal.load(Path(self.root.name), 'status-https').status == 'preparing':
                forward_controls.append(args)
            return original_control(host, *args)

        self.cutover.save = expired
        self.host.systemctl = tracked
        with self.assertRaisesRegex(DeployError, 'recovery window expired'):
            self.cutover.deploy(self.cutover.plan()['plan_sha256'])
        self.assertEqual(forward_controls, [])
        self.assertEqual(Journal.load(Path(self.root.name), 'status-https').status, 'rolled-back')

    def test_expiration_at_file_install_restores_before_installing_more_files(self):
        original = self.host.write
        installed = []

        def delayed(host, path, data, mode):
            original(host, path, data, mode)
            if path in {role['config_path'] for role in self.request['roles'].values()}:
                if Journal.load(Path(self.root.name), 'status-https').status == 'preparing':
                    installed.append(path)
                    self.clock = WINDOW

        self.host.write = delayed
        with self.assertRaisesRegex(DeployError, 'recovery window expired'):
            self.cutover.deploy(self.cutover.plan()['plan_sha256'])
        self.assertEqual(installed, ['/etc/wallet-pir/publisher.json'])
        self.assertEqual(Journal.load(Path(self.root.name), 'status-https').status, 'rolled-back')

    def test_foreign_configuration_and_policy_changes_refuse_before_stopping(self):
        plan = self.cutover.plan()
        self.host.put('coordinator', CADDY, 'foreign change')
        with self.assertRaises(DeployError):
            self.cutover.deploy(plan['plan_sha256'])
        self.assertTrue(all(self.host.probe_unit(r['host'], r['unit'])['active_state'] == 'active' for r in self.request['roles'].values()))

    def test_parser_binds_request_and_refuses_unknown_fields(self):
        path = Path(self.root.name) / 'request.json'
        path.write_text(json.dumps(self.request))
        parsed = load_request(path, sha(path.read_bytes()))
        self.assertEqual(parsed, self.request)
        self.request['unknown'] = True
        path.write_text(json.dumps(self.request))
        with self.assertRaises(DeployError):
            load_request(path, sha(path.read_bytes()))
        with self.assertRaises(DeployError):
            load_request(path, '0' * 64)

    def test_parser_accepts_installed_publisher_probe_timeouts(self):
        for check, timeout in zip(self.checks.values(), (210, 180, 420, 420)):
            check['timeout'] = timeout
        path = Path(self.root.name) / 'request.json'
        path.write_text(json.dumps(self.request))
        self.assertEqual(load_request(path, sha(path.read_bytes())), self.request)
        installed = json.loads(self.host.read('coordinator', INVENTORY))
        installed['services']['publisher']['exact_check']['products'] = self.checks
        self.host.put('coordinator', INVENTORY, json.dumps(installed))
        self.cutover.preflight()
        for timeout in (0, 901, True):
            self.checks['status']['timeout'] = timeout
            path.write_text(json.dumps(self.request))
            with self.assertRaisesRegex(DeployError, 'invalid exact check'):
                load_request(path, sha(path.read_bytes()))

    def test_long_probe_is_capped_by_remaining_recovery_time(self):
        self.checks['status']['timeout'] = 420
        original = self.host.run
        timeouts = []

        def record(host, argv, timeout):
            if argv[0] == 'exact':
                timeouts.append(timeout)
            return original(host, argv, timeout)

        self.host.run = record
        self.cutover.checks(deadline=30)
        self.assertEqual(timeouts, [10, 30, 10, 10])
