"""Cache activation converges both monitoring processes and resumes without touching serving units."""
import argparse
import contextlib
import functools
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(Path(__file__).parent))
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops import monitoring, monitoring_adoption, monitoring_certs, monitoring_roles  # noqa: E402
from wallet_pir_ops.deploy import cache_data, descriptors, remote  # noqa: E402
from wallet_pir_ops.deploy.engine import Deployer  # noqa: E402


class VolumeMonitoring(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / 'state'
        self.state.mkdir()
        self.worker = {'host_id': 'txid-01', 'address': '10.0.0.4'}
        self.inventory = self.root / 'inventory.json'
        self.inventory.write_text(json.dumps({
            'hosts': {'coordinator': {}, 'txid-01': {}}, 'ssh': {'mode': 'config'},
            'lock': {'type': 'remote', 'host': 'coordinator'},
            'services': {'apm': {'roles': {'central': [{'host': 'coordinator'}]}},
                         'apm-sidecar': {'roles': {'observer': [
                             {'host': 'txid-01', 'install': True, 'settings': self.worker}]}},
                         'transparent-display': {'roles': {'worker': [{'host': 'txid-01'}]}}}}))
        self.fake = FakeFleet()
        self.loaded = {}
        start = self.fake._start

        def started(host, unit):
            start(host, unit)
            path = monitoring.CONFIG if unit == monitoring_certs.SIDECAR_UNIT else monitoring_roles.APM_OBSERVERS
            self.loaded[host, unit] = json.loads(self.fake.read(host, path))
        self.fake._start = started
        self.fake.endpoints['txid-01', monitoring_certs.SIDECAR_HEALTH] = monitoring_certs.SIDECAR_UNIT
        self.fake.health_shapes['txid-01', monitoring_certs.SIDECAR_UNIT] = lambda ready, exe: {
            'ready': ready, 'binary_sha256': exe,
            'config_digest': monitoring.config_digest(self.loaded['txid-01', monitoring_certs.SIDECAR_UNIT])}
        self.fake.product_check = lambda host, argv: (0, 'configuration accepted')
        services = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
        binary = self.root / 'pir-apm-sidecar'
        binary.write_bytes(b'sidecar')
        deployer = Deployer(services['apm-sidecar'], descriptors.load_inventory(self.inventory), self.fake,
                            self.state, out=lambda _: None)
        deployer.capture_baseline()
        deployer.deploy(sha256(b'sidecar'), str(binary), skip_exact_check=True)
        collector = monitoring.observers(json.loads(self.inventory.read_text()), mode='shadow',
                                          roster='/etc/pir/roster.json',
                                          roster_snapshot={'unit': 'transparent-recent.service'})
        self.old_revision = collector['revision']
        self.fake.put('coordinator', monitoring_roles.APM_OBSERVERS, json.dumps(collector))
        apm_binary = '/opt/pir-apm/releases/' + sha256(b'apm') + '/pir-apm'
        self.fake.put('coordinator', apm_binary, b'apm')
        rendered = monitoring_roles.apm({'slack': False, 'chain_cookie': None},
                                        release=str(Path(apm_binary).parent),
                                        template=services['apm'].roles['central'].template.read_text())
        self.fake.install('coordinator', monitoring_certs.COLLECTOR_UNIT, rendered.unit)
        self.fake.endpoints['coordinator', monitoring_certs.COLLECTOR_READY] = monitoring_certs.COLLECTOR_UNIT

        def collector_health(ready, exe):
            loaded = self.loaded['coordinator', monitoring_certs.COLLECTOR_UNIT]
            collected = self.loaded['txid-01', monitoring_certs.SIDECAR_UNIT]
            valid = int(loaded['hosts'][0]['config_digest'] == monitoring.config_digest(collected))
            return {'ready': ready and bool(valid), 'revision': loaded['revision'], 'expected_hosts': 1,
                    'valid_hosts': valid, 'evaluated_at': time.time()}
        self.fake.health_shapes['coordinator', monitoring_certs.COLLECTOR_UNIT] = collector_health
        request = {'host': 'txid-01', 'unit': 'transparent-txid-display-worker.service',
                   'configuration': '/etc/query.json', 'volume_id': 'v1', 'volume_name': 'cache', 'size_gib': 50}
        (self.state / 'volume-1.json').write_text(json.dumps({'phase': 'attached', 'volume_id': 'v1',
            'plan': {'request': {'host': 'txid-01', 'name': 'cache', 'size_gib': 50}}}))
        self.summary = {'phase': 'serving-started', 'transaction': 'cache-data-t1',
                        'active_cache': '/srv/cache', 'device': {'uuid': 'abcd-0001'}}
        run = self.fake.run
        self.storage_runs = []

        def execute(host, argv, timeout=90):
            if argv[:2] == ['python3', '-c'] and argv[-1] == 'cache-data-t1':
                self.storage_runs.append(argv)
                return 0, json.dumps(self.summary)
            return run(host, argv, timeout)
        self.fake.run = execute
        path = self.root / 'request.json'
        path.write_text(json.dumps(request))
        self.args = argparse.Namespace(request=path, request_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                                       inventory=str(self.inventory), state_dir=str(self.state),
                                       command='cache-data-activate', transaction='cache-data-t1')
        self.fake.log.clear()
        self.adopt = functools.partial(monitoring_adoption.adopt_volume, ready_timeout=0)

    def dispatch(self):
        with mock.patch.object(monitoring_adoption, 'adopt_volume', self.adopt), contextlib.redirect_stdout(io.StringIO()):
            return cache_data.dispatch(self.args, self.fake)

    def test_a_stopped_central_apm_leaves_monitoring_pending_before_changing_the_sidecar(self):
        self.fake.systemctl('coordinator', 'stop', monitoring_certs.COLLECTOR_UNIT)
        self.fake.log.clear()
        with self.assertRaisesRegex(cache_data.DeployError, 'monitoring adoption remains pending'):
            self.dispatch()
        self.assertFalse([entry for entry in self.fake.log if entry[1] == 'systemctl'])
        receipt = json.loads((self.state / 'cache-data-t1.json').read_text())
        self.assertEqual(receipt['monitoring']['status'], 'pending')

    def test_activation_updates_running_sidecar_and_collector_under_one_lock(self):
        result = self.dispatch()
        self.assertEqual((result['phase'], result['monitoring']['status']), ('serving-started', 'complete'))
        sidecar = self.loaded['txid-01', monitoring_certs.SIDECAR_UNIT]
        collector = self.loaded['coordinator', monitoring_certs.COLLECTOR_UNIT]
        self.assertEqual(sidecar['required_volumes'], [{'path': '/srv/cache', 'uuid': 'abcd-0001'}])
        self.assertEqual(collector['hosts'][0]['config_digest'], monitoring.config_digest(sidecar))
        self.assertNotEqual(collector['revision'], self.old_revision)
        self.assertEqual(collector['roster_snapshot'], {'unit': 'transparent-recent.service'})
        self.assertEqual(collector['mode'], 'shadow')
        self.args.command = 'cache-data-reconcile'
        self.assertEqual(self.dispatch()['monitoring']['status'], 'complete')
        self.assertEqual(len(self.storage_runs), 1)
        self.assertFalse([detail for _, op, detail in self.fake.log if op == 'systemctl'
                          and any('transparent-' in value for value in detail)])

    def test_lost_lock_during_collector_update_resumes_both_and_keeps_serving_cache(self):
        def interrupt(host, operation, detail):
            if operation == 'write' and detail == monitoring_roles.APM_OBSERVERS:
                self.fake.lock.lost = True
        self.fake.observer = interrupt
        with self.assertRaisesRegex(cache_data.DeployError, 'monitoring adoption remains pending'):
            self.dispatch()
        receipt = json.loads((self.state / 'cache-data-t1.json').read_text())
        self.assertEqual(receipt['phase'], 'monitoring-pending')
        self.assertIn('sidecar_transaction', receipt['monitoring'])
        self.fake.observer = None
        self.args.command = 'cache-data-reconcile'
        self.assertEqual(self.dispatch()['monitoring']['status'], 'complete')
        self.assertEqual(len(self.storage_runs), 1)
        collector = self.loaded['coordinator', monitoring_certs.COLLECTOR_UNIT]
        self.assertEqual(collector['hosts'][0]['config_digest'], monitoring.config_digest(
            self.loaded['txid-01', monitoring_certs.SIDECAR_UNIT]))

    def interrupted_restart_recovers(self, target):
        start = self.fake._start

        def interrupted(host, unit):
            if unit == target:
                self.fake.host(host).units[unit].update(active='failed', pid=0, exe=None)
                self.fake.lock.lost = True
                raise remote.LockHeld('restart acknowledgment lost with the process stopped')
            start(host, unit)
        self.fake._start = interrupted
        with self.assertRaisesRegex(cache_data.DeployError, 'monitoring adoption remains pending'):
            self.dispatch()
        self.fake._start = start
        host = 'coordinator' if target == monitoring_certs.COLLECTOR_UNIT else 'txid-01'
        self.assertEqual(self.fake.probe_unit(host, target)['active_state'], 'failed')
        self.args.command = 'cache-data-reconcile'
        self.assertEqual(self.dispatch()['monitoring']['status'], 'complete')
        self.assertEqual(self.fake.probe_unit(host, target)['active_state'], 'active')
        self.assertEqual(len(self.storage_runs), 1)

    def test_reconcile_recovers_its_own_interrupted_collector_restart(self):
        self.interrupted_restart_recovers(monitoring_certs.COLLECTOR_UNIT)

    def test_reconcile_recovers_its_own_interrupted_sidecar_restart(self):
        self.interrupted_restart_recovers(monitoring_certs.SIDECAR_UNIT)

    def test_reconcile_retries_a_failed_sidecar_preflight_before_any_unit_was_changed(self):
        self.fake.product_check = lambda host, argv: (1, 'configuration check refused')
        with self.assertRaisesRegex(cache_data.DeployError, 'monitoring adoption remains pending'):
            self.dispatch()
        self.fake.product_check = lambda host, argv: (0, 'configuration accepted')
        self.args.command = 'cache-data-reconcile'
        self.assertEqual(self.dispatch()['monitoring']['status'], 'complete')
        self.assertEqual(len(self.storage_runs), 1)
