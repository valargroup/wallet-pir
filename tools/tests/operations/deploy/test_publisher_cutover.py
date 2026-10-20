"""Ownership transfer, crash recovery and first installation use the real deploy executor boundary."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops import durable
from wallet_pir_ops.deploy import descriptors, publisher
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError
from fake_fleet import FakeFleet, sha256

BINARY = b'unified publisher executable'
HISTORY = {'data_dir': '/srv/transparent-activity/full-v3/journal',
           'publication_root': '/srv/transparent-activity/full-v12/publications',
           'initial_publication': '/srv/transparent-activity/full-v12/publications/initial',
           'recent_from': 1, 'recent_geometry': 'decoupled-2k-8k', 'archive_geometry': 'decoupled-32k-32k',
           'rpc_url': 'http://127.0.0.1:8232', 'rpc_cookie': '/root/.cache/zakura/.cookie',
           'fleet_command': '/opt/history/fleet.py', 'fleet_config': '/opt/history/fleet.json',
           'listen': '10.0.0.1:8096', 'source_sha': 'retained-source', 'follow_journal': False,
           'ship_runtimes': False, 'distribute_prepared': False, 'http_only': False}
DISPLAY = {'root': '/srv/txid-display-genesis/v4/root', 'journal': '/srv/txid-display-genesis/journal-v3',
           'workers': '/opt/display/workers.json', 'rpc_url': 'http://127.0.0.1:8232',
           'rpc_cookie': '/root/.cache/zakura/.cookie', 'poll_ms': 500, 'status_listen': '127.0.0.1:8098',
           'trust_sealed': False, 'ship_runtimes': False, 'http_only': False, 'max_recent_records': None}


class Coordinator(FakeFleet):
    def __init__(self):
        super().__init__()
        self.memory = 48 << 30
        self.crash_on = None
        self.fail_probe = False
        self.refuse_configuration = False
        self.clock_seconds = 0
        self.effects = []
        self.lock_held = False

    def publisher_unlocked(self, host, paths):
        if self.lock_held:
            raise DeployError('another writer retains its lock')
        assert all(self.probe_unit(host, name)['main_pid'] == 0 for name in (*publisher.OLD, publisher.UNIT))

    def probe_unit(self, host, name):
        value = super().probe_unit(host, name)
        state = self.host(host).units.get(name, {})
        value['unit_file_state'] = 'enabled' if state.get('enabled', True) else 'disabled'
        return value

    def publisher_resources(self, host, paths):
        files = self.host(host).files
        return {'machine_id': 'a' * 32, 'memory_available': self.memory, 'memory_total': 64 << 30,
                'files': {p: {'mode': 0o600} if p in files else None for p in paths},
                'disks': {p: {'available': 80 << 30, 'total': 100 << 30} for p in paths}}

    def systemctl(self, host, *args):
        self.effects.append(args)
        if args[0] == 'start':
            args = ('restart', *args[1:])
        super().systemctl(host, *args)
        if self.crash_on == args:
            self.crash_on = None
            raise RemoteError('reply lost after completed service mutation')

    def http_get(self, host, url, timeout=5):
        active = lambda name: self.host(host).units.get(name, {}).get('active') == 'active'
        if ':18080/' in url:
            return (200, json.dumps({'ready': True})) if active(publisher.UNIT) else (503, '{}')
        if ':8098/' in url:
            return 200, json.dumps({'phase': 'running', 'mode': 'live', 'halted': None, 'stalled': False})
        return 200, json.dumps({'phase': 'serving'})

    def run(self, host, argv, timeout):
        self.runs.append((host, argv))
        if '--check' in argv:
            configuration = json.loads(self.read(host, argv[argv.index('--config') + 1]))
            if self.refuse_configuration or set(configuration) != {'management_listen', *publisher.PRODUCTS}:
                return 1, 'unknown product configuration'
        elif self.fail_probe:
            return 1, 'encrypted answer mismatch'
        return 0, 'independent exact answer verified'

    def clock(self):
        return self.clock_seconds

    def sleep(self, seconds):
        self.clock_seconds += seconds


class PublisherCutoverTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.binary = self.root / 'pir-publisher'
        self.binary.write_bytes(BINARY)
        self.ex = Coordinator()
        self.ex.install('coordinator', publisher.OLD[0],
                        '[Service]\nExecStart=/bin/history --config ' + publisher.HISTORY + '\n',
                        files={'/bin/history': b'history'})
        flags = ' '.join('--' + k.replace('_', '-') + ' ' + str(DISPLAY[k])
                         for k in ('root', 'journal', 'workers', 'rpc_url', 'rpc_cookie', 'status_listen'))
        self.ex.install('coordinator', publisher.OLD[1], '[Service]\nExecStart=/bin/display run ' + flags +
                        ' --mode replay-then-live --blocks-per-step 25 --step-interval-ms 2000\n',
                        files={'/bin/display': b'display'})
        self.ex.install('coordinator', publisher.LOAD,
                        '[Service]\nExecStart=/bin/load --controller-unit transparent-v12-publish-controller\n',
                        files={'/bin/load': b'load'})
        self.ex.put('coordinator', publisher.HISTORY, json.dumps(HISTORY))
        self.ex.put('coordinator', publisher.SAMPLER, json.dumps({'targets': [
            {'sources': ['transparent-publisher'], 'unit': publisher.OLD[0]},
            {'sources': ['txid-controller'], 'unit': publisher.OLD[1]}]}))
        document = json.loads((ROOT / 'tools/enhance/deploy/deploy-inventory.example.json').read_text())
        document['services'] = {name: value for name, value in document['services'].items()
                                if name in ('transparent-history', 'transparent-display')}
        for service in document['services'].values():
            service['roles'].pop('controller', None)
        document['services']['publisher'] = {
            'roles': {'server': [{'host': 'coordinator', 'vars': {'management_listen': '127.0.0.1:18080'}}]},
            'managed_products': publisher.PRODUCTS,
            'exact_check': {'host': 'coordinator', 'argv': ['/bin/check-both']}}
        document['ancillary'] = {name: value for name, value in document['ancillary'].items()
                                  if set(value['consumers']) & {'transparent-history', 'transparent-display'}}
        for value in document['ancillary'].values():
            value['consumers'] = [name for name in value['consumers'] if name in document['services']]
        document['lock'] = {'type': 'remote', 'host': 'coordinator'}
        self.request = {'schema': publisher.SCHEMA, 'host': 'coordinator', 'machine_id': 'a' * 32,
            'release': {'git_sha': 'b' * 40, 'binary_sha256': sha256(BINARY), 'binary_bytes': len(BINARY)},
            'configuration': {'management_listen': '127.0.0.1:18080',
                              'transparent_history': {'enabled': True, 'settings': copy.deepcopy(HISTORY)},
                              'transaction_details': {'enabled': True, 'settings': copy.deepcopy(DISPLAY)}},
            'inventory': document,
            'checks': {name: {'host': 'coordinator', 'argv': ['/bin/check-' + name], 'timeout': 20}
                       for name in publisher.PRODUCTS}}
        self.target = self.make_target()
        self.ex.log.clear()

    def make_target(self):
        path = self.root / 'request.json'
        path.write_text(json.dumps(self.request))
        request, inventory = publisher.load_request(path, publisher.sha(path.read_text()))
        return publisher.Cutover(request, inventory, self.ex, self.root,
                                 clock=self.ex.clock, sleep=self.ex.sleep)

    def deploy(self):
        return self.target.deploy(self.binary, self.target.plan()['plan_sha256'])

    def test_first_install_and_rollback_restore_ownership_and_absent_files(self):
        initial = {p: self.ex.read('coordinator', p) for p in (publisher.SAMPLER, publisher.UNIT_PATH, publisher.CONFIG)}
        journal = self.deploy()
        self.assertEqual(journal['phase'], 'committed')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['active_state'], 'active')
        for name in publisher.OLD:
            self.assertEqual(self.ex.probe_unit('coordinator', name)['active_state'], 'inactive')
            self.assertEqual(self.ex.probe_unit('coordinator', name)['unit_file_state'], 'disabled')
        stop_indices = [self.ex.effects.index(('stop', name)) for name in publisher.OLD]
        self.assertLess(max(stop_indices), self.ex.effects.index(('start', publisher.UNIT)))
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        for name in publisher.OLD:
            self.assertEqual(self.ex.probe_unit('coordinator', name)['active_state'], 'active')
        for path, text in initial.items():
            self.assertEqual(self.ex.read('coordinator', path), text)
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')

    def test_plan_and_preflight_capture_do_not_mutate(self):
        self.target.plan()
        self.assertEqual(self.ex.mutations(), [])

    def test_stale_plan_refuses_before_staging(self):
        plan = self.target.plan()
        self.ex.put('coordinator', publisher.SAMPLER, json.dumps({'targets': [
            {'sources': ['transparent-publisher', 'txid-controller'], 'unit': publisher.OLD[0]}]}))
        with self.assertRaisesRegex(DeployError, 'stale'):
            self.target.deploy(self.binary, plan['plan_sha256'])
        self.assertEqual(self.ex.mutations(), [])

    def test_configuration_without_the_compiled_product_never_stops_old_owners(self):
        self.ex.refuse_configuration = True
        with self.assertRaisesRegex(DeployError, 'refuses its configuration'):
            self.deploy()
        self.assertFalse(any(args[0] == 'stop' for args in self.ex.effects))
        self.target.rollback()
        self.assertTrue(all(self.ex.probe_unit('coordinator', name)['active_state'] == 'active' for name in publisher.OLD))

    def test_lost_stop_reply_is_reconciled_by_restoring_original_owners(self):
        self.ex.crash_on = ('stop', publisher.OLD[0])
        with self.assertRaises(RemoteError):
            self.deploy()
        self.assertEqual(self.target.latest()['intent'], 'stop ' + publisher.OLD[0])
        with self.assertRaisesRegex(DeployError, 'unfinished'):
            self.target.plan()
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertTrue(all(self.ex.probe_unit('coordinator', name)['active_state'] == 'active' for name in publisher.OLD))

    def test_exact_probe_failure_remains_uncommitted_until_restoration(self):
        self.ex.fail_probe = True
        with self.assertRaisesRegex(DeployError, 'encrypted-answer probe failed'):
            self.deploy()
        self.assertEqual(self.target.latest()['phase'], 'verifying')
        self.ex.fail_probe = False
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')

    def test_changed_managed_file_refuses_rollback_before_stopping_anyone(self):
        self.deploy()
        self.ex.put('coordinator', publisher.CONFIG, 'foreign configuration')
        count = len(self.ex.effects)
        with self.assertRaisesRegex(DeployError, 'outside transaction'):
            self.target.rollback()
        self.assertEqual(len(self.ex.effects), count)

    def test_running_publisher_can_be_updated_and_restored(self):
        self.deploy()
        before = self.ex.read('coordinator', publisher.CONFIG)
        second = self.deploy()
        self.assertEqual(second['plan']['units'][publisher.UNIT]['active_state'], 'active')
        self.target.rollback()
        self.assertEqual(self.ex.read('coordinator', publisher.CONFIG), before)
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['active_state'], 'active')
        self.assertTrue(all(self.ex.probe_unit('coordinator', name)['active_state'] == 'inactive' for name in publisher.OLD))

    def test_unknown_arguments_and_ownership_changes_are_refused(self):
        self.request['configuration']['transaction_details']['settings']['poll_ms'] = 99
        with self.assertRaisesRegex(DeployError, 'display configuration changes'):
            self.make_target().plan()
        self.request['inventory']['services']['publisher']['managed_products'] = ['transparent_history']
        with self.assertRaisesRegex(DeployError, 'managed products differ'):
            self.make_target()

    def test_host_identity_and_resource_floor_refuse_before_staging(self):
        self.ex.memory = 1 << 30
        with self.assertRaisesRegex(DeployError, '20 percent'):
            self.target.plan()
        self.assertEqual(self.ex.mutations(), [])

    def test_another_writer_lock_prevents_starting_the_unified_owner(self):
        self.ex.lock_held = True
        with self.assertRaisesRegex(DeployError, 'retains its lock'):
            self.deploy()
        self.assertNotIn(('start', publisher.UNIT), self.ex.effects)


if __name__ == '__main__':
    unittest.main()
