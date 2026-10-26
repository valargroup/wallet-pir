"""Contract cutovers operate on processes and accept each enabled product."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT / 'tools/lib'), str(Path(__file__).parent), str(Path(__file__).parent.parent)]
from fake_fleet import FakeFleet, sha256
from test_wallet_pir_deploy import worker_settings, status_settings, TOKEN
from wallet_pir_ops.deploy import descriptors, fleet_release, units
from wallet_pir_ops.query_role import PRODUCTS as NODE_PRODUCTS
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.fleet_transaction import FleetTransaction
from wallet_pir_ops.deploy.remote import RemoteError

ORIGIN = 'https://publisher.example:443'
PRODUCTS = list(NODE_PRODUCTS)
REGISTRY = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')


def report(name, major):
    return {'schema': 'wallet-pir-contract-info/1', 'executable': name,
            'contract_version': '%d.0.0' % major, 'contract_major': major,
            'products': PRODUCTS, 'backends': ['cuda']}


class Hosts(FakeFleet):
    def publisher_resources(self, host, paths):
        return {'machine_id': hashlib.md5(host.encode()).hexdigest(), 'files': {}, 'disks': {},
                'memory_total': 64 << 30, 'memory_available': 32 << 30}

    def publisher_unlocked(self, host, paths):
        self.log.append((host, 'writers-joined', paths))

    def publisher_major(self):
        state = self.host('publisher').units['pir-publisher.service']
        if not state['pid']:
            return None
        return json.loads(self.contract_reports[state['exe']])['contract_major']

    def http_get(self, host, url, timeout=5):
        if url.endswith('/.well-known/pir-service-discovery'):
            major = self.publisher_major()
            return (503, '') if major is None else (200, json.dumps({'service': 'pir-publisher',
                                                                     'version': '%d.0.0' % major}))
        return super().http_get(host, url, timeout)

    def _start(self, host, unit):
        super()._start(host, unit)
        if unit == 'pir-publisher.service':
            return
        state = self.host(host).units[unit]
        if state['pid']:
            major = json.loads(self.contract_reports[state['exe']])['contract_major']
            if major != self.publisher_major():
                raise AssertionError('consumer started against incompatible publisher')


class FleetTransactions(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.ex = Hosts()
        self.ex.product_check = lambda host, argv: (0, 'static configuration accepted')
        self.old_pub, self.old_query = sha256(b'old publisher'), sha256(b'old query')
        self.pub_key = 'publisher/pir-publisher.service'
        self.query_key = 'query/wallet-pir-query.service'
        self.status_key = 'worker/status-worker.service'
        self.time = 1000.

    def fixture(self, *, unified=True, distributed=True, major=2, selected=None, colocated=False):
        self.ex = Hosts()
        self.ex.product_check = lambda host, argv: (0, 'static configuration accepted')
        settings = json.loads((ROOT / 'tools/deploy/query-server/query-node.example.json').read_text())
        settings.pop('receiver', None)
        settings['enhance']['synchronization']['publisher_origin'] = ORIGIN
        for product in PRODUCTS[1:]:
            settings[product]['publisher'] = ORIGIN
        document = {'hosts': {name: {} for name in ('publisher', 'query', 'worker')},
            'ssh': {'mode': 'config'}, 'lock': {'type': 'remote', 'host': 'publisher'},
            'services': {'publisher': {'managed_products': PRODUCTS,
                'exact_check': {'products': {p: {'host': 'query', 'argv': ['probe', p], 'timeout': 10} for p in PRODUCTS}},
                'roles': {'server': [{'host': 'publisher', 'vars': {'management_listen': '127.0.0.1:18080'}}]}}}}
        if unified:
            document['services']['query-server'] = {'roles': {'combined': [{'host': 'query',
                'settings': settings, 'vars': {'management_listen': settings['management_listen']}}]}}
        if distributed:
            worker = status_settings('worker')
            worker['publisher'] = ORIGIN
            host = 'query' if colocated else 'worker'
            self.status_key = host + '/status-worker.service'
            document['services']['status'] = {'roles': {'worker': [{'host': host, 'settings': worker}]}}
        self.inventory_path = self.root / 'inventory.json'
        self.inventory_path.write_text(json.dumps(document))
        self.inventory = descriptors.load_inventory(self.inventory_path)
        self.old_data = {'pir-publisher': b'old publisher', 'pir-query-server': b'old query'}
        self.new_data = {'pir-publisher': b'candidate publisher ' + str(major).encode(),
                         'pir-query-server': b'candidate query ' + str(major).encode()}
        artifacts = {}
        for name, data in self.new_data.items():
            path = self.root / name
            path.write_bytes(data)
            artifacts[name] = str(path)
        self.binaries = artifacts
        self.pub_config = {'management_listen': '127.0.0.1:18080',
                           **{p: {'enabled': True, 'settings': {}} for p in PRODUCTS}}
        self.pub_config['enhance']['settings'] = {'data_dir': '/srv/publisher/enhance'}
        self.pub_config['status']['settings'] = {'publisher': {'state_dir': '/srv/publisher/status'}}
        self.pub_config['transparent_history']['settings'] = {'data_dir': '/srv/publisher/history/journal',
                                                             'publication_root': '/srv/publisher/history/publications'}
        self.pub_config['transaction_details']['settings'] = {'journal': '/srv/publisher/details/journal',
                                                             'root': '/srv/publisher/details/publications'}
        selected = selected or ([self.pub_key] + ([self.query_key] if unified else []) +
                                ([self.status_key] if distributed else []))
        self.request = {'schema': 'fleet-release/1', 'source': {'revision': 'a' * 40, 'submitted_unix': 999.},
            'inventory_sha256': hashlib.sha256(self.inventory_path.read_bytes()).hexdigest(),
            'executables': {name: {'sha256': sha256(data), 'contract': report(name, major)}
                            for name, data in self.new_data.items()},
            'select': selected, 'publishers': {ORIGIN: self.pub_key},
            'probes': {p: {'host': 'publisher', 'argv': ['probe', p], 'restore_argv': ['probe', p]} for p in PRODUCTS},
            'process_probes': {self.query_key: {p: {'host': 'query', 'argv': ['node-probe', p],
                                                 'restore_argv': ['node-probe', p]} for p in PRODUCTS}}
                            if unified else {}}
        for name in self.old_data:
            self.ex.contract_reports[sha256(self.old_data[name])] = json.dumps(report(name, 1))
            self.ex.contract_reports[sha256(self.new_data[name])] = json.dumps(report(name, major))
        # Install publisher before consumers: their real startup requirement is
        # enforced by the fake, including during restoration.
        for service_name in ('publisher', 'query-server', 'status'):
            if service_name not in document['services']:
                continue
            service = REGISTRY[service_name]
            for target in descriptors.targets(service, self.inventory):
                name = target.role.binary or service.binary
                sha = sha256(self.old_data[name])
                executable = service.release_binary(sha, name)
                if service_name == 'publisher':
                    text = '[Service]\nExecStart=' + executable + ' --config /etc/wallet-pir/publisher.json\n'
                    files = {'/etc/wallet-pir/publisher.json': json.dumps(self.pub_config)}
                else:
                    rendered = target.role.render(target.settings, service.release_dir(sha))
                    text, files = rendered.unit, rendered.files
                files = dict(files)
                for path in units.effective([text]).get('Service', {}).get('EnvironmentFile', []):
                    files[path.lstrip('-')] = 'TEST_ONLY=1\n'
                self.ex.install(target.host, target.unit, text, files={executable: self.old_data[name], **files})
                for endpoint in (target.url(target.role.health), target.url(target.role.ready_url)):
                    if endpoint:
                        self.ex.endpoints[(target.host, endpoint)] = target.unit
        self.ex.log.clear()
        self.target = self.new_target()

    def new_target(self):
        return FleetTransaction(REGISTRY, self.inventory, self.request['inventory_sha256'], self.request,
                                self.ex, self.root / 'state', now=lambda: self.time,
                                clock=lambda: self.time, sleep=self.sleep)

    def sleep(self, seconds):
        self.time += seconds

    def prepare(self):
        from wallet_pir_ops.deploy.remote import production_lock
        with production_lock(self.inventory, self.ex) as self.target.lock:
            self.target.stage(self.binaries)
        return self.target.inspect()

    def test_all_topologies_cross_the_publisher_barrier_and_restore(self):
        for unified, distributed in ((True, False), (False, True), (True, True)):
            with self.subTest(unified=unified, distributed=distributed):
                self.fixture(unified=unified, distributed=distributed)
                plan = self.prepare()
                self.assertEqual(plan['problems'], [])
                result = self.target.deploy(plan['plan_sha256'])
                self.assertEqual(result['phase'], 'committed')
                if unified:
                    starts = [event for host, op, event in self.ex.log if host == 'query' and op == 'systemctl'
                              and tuple(event[:2]) == ('restart', 'wallet-pir-query.service')]
                    self.assertEqual(len(starts), 1)
                    self.assertTrue(all(self.query_key + '#' + p in self.target.data['checks'] for p in PRODUCTS))
                restored = self.new_target().recover('rollback')
                self.assertEqual(restored['phase'], 'rolled-back')
                self.assertEqual(self.ex.publisher_major(), 1)

    def test_publisher_only_compatible_update_preserves_query_invocations(self):
        self.fixture(major=1, selected=[self.pub_key])
        before = self.ex.probe_unit('query', 'wallet-pir-query.service')['invocation_id']
        plan = self.prepare()
        self.target.deploy(plan['plan_sha256'])
        self.assertEqual(self.ex.probe_unit('query', 'wallet-pir-query.service')['invocation_id'], before)

    def test_each_product_failure_restores_complete_release(self):
        for product in PRODUCTS:
            with self.subTest(product=product):
                self.fixture()
                plan = self.prepare()
                run = self.ex.run
                def refuse(host, argv, timeout):
                    if argv == ['node-probe', product] and self.ex.publisher_major() == 2:
                        return 1, 'wrong encrypted answer'
                    return run(host, argv, timeout)
                self.ex.run = refuse
                with self.assertRaisesRegex(DeployError, 'encrypted-answer probe failed'):
                    self.target.deploy(plan['plan_sha256'])
                self.assertEqual(self.target.data['phase'], 'rolled-back')
                self.assertEqual(self.ex.publisher_major(), 1)
                del self.ex.run

    def test_staging_failure_never_stops_a_service(self):
        self.fixture()
        self.ex.self_check_fails.add(sha256(self.new_data['pir-query-server']))
        with self.assertRaisesRegex(DeployError, 'self-check'):
            self.prepare()
        self.assertFalse([event for _, op, event in self.ex.log if op == 'systemctl'])

    def test_colocated_roles_transfer_the_query_executable_once(self):
        self.fixture(colocated=True)
        plan = self.prepare()
        uploads = [(host, path) for host, operation, path in self.ex.log
                   if operation == 'upload' and path.endswith('/pir-query-server')]
        self.assertEqual(len(uploads), 1)
        self.assertEqual(len([1 for _, operation, _ in self.ex.log if operation == 'release-copy']), 1)
        self.assertEqual(self.target.deploy(plan['plan_sha256'])['phase'], 'committed')

    def test_missing_predecessor_binary_refuses_recovery_before_stopping(self):
        self.fixture()
        plan = self.prepare()
        self.target.deploy(plan['plan_sha256'])
        self.ex.remove('query', self.target.record(self.query_key)['previous']['exec_binary'])
        self.ex.log.clear()
        with self.assertRaisesRegex(DeployError, 'predecessor executable'):
            self.new_target().recover('rollback')
        self.assertFalse([event for _, op, event in self.ex.log if op == 'systemctl'])

    def test_plan_drift_refuses_before_stopping(self):
        self.fixture()
        plan = self.prepare()
        self.ex.systemctl('query', 'restart', 'wallet-pir-query.service')
        self.ex.log.clear()
        with self.assertRaisesRegex(DeployError, 'stale'):
            self.target.deploy(plan['plan_sha256'])
        self.assertFalse([event for _, op, event in self.ex.log if op == 'systemctl'])

    def test_actual_candidate_metadata_and_unified_probe_coverage_are_required(self):
        self.fixture()
        self.ex.contract_reports[sha256(self.new_data['pir-query-server'])] = json.dumps(report('pir-query-server', 1))
        with self.assertRaisesRegex(DeployError, 'report differs'):
            self.prepare()
        self.fixture()
        del self.request['process_probes'][self.query_key]['status']
        with self.assertRaisesRegex(DeployError, 'every enabled product'):
            self.prepare()

    def test_newly_enabled_product_cannot_escape_unified_acceptance(self):
        self.fixture()
        path = '/etc/wallet-pir-query/query-server.json'
        document = json.loads(self.ex.read('query', path))
        document['status']['enabled'] = False
        self.ex.put('query', path, json.dumps(document))
        probe = self.request['process_probes'][self.query_key].pop('status')
        with self.assertRaisesRegex(DeployError, 'every enabled product'):
            self.prepare()
        self.request['process_probes'][self.query_key]['status'] = probe
        plan = self.prepare()
        self.target.deploy(plan['plan_sha256'])
        self.ex.log.clear()
        self.new_target().recover('rollback')
        self.assertFalse(json.loads(self.ex.read('query', path))['status']['enabled'])

    def test_lost_start_reply_is_restored_from_remote_journal(self):
        self.fixture()
        plan = self.prepare()
        systemctl = self.ex.systemctl
        def lost(host, *args):
            systemctl(host, *args)
            if host == 'query' and args[0] == 'restart' and self.ex.publisher_major() == 2:
                raise RemoteError('lost start reply')
        self.ex.systemctl = lost
        with self.assertRaisesRegex(RemoteError, 'lost start'):
            self.target.deploy(plan['plan_sha256'])
        self.assertEqual(self.target.data['phase'], 'rolled-back')
        self.assertEqual(self.new_target().recover('rollback')['phase'], 'rolled-back')

    def test_foreign_configuration_blocks_recovery_without_stopping(self):
        self.fixture()
        plan = self.prepare()
        self.target.deploy(plan['plan_sha256'])
        self.ex.put('query', '/etc/wallet-pir-query/query-server.json', 'foreign configuration')
        self.ex.log.clear()
        with self.assertRaisesRegex(DeployError, 'changed'):
            self.new_target().recover('rollback')
        self.assertFalse([event for _, op, event in self.ex.log if op == 'systemctl'])

    def test_timing_miss_is_reported_without_restarting_healthy_services(self):
        self.fixture()
        plan = self.prepare()
        self.time += 240
        result = self.target.deploy(plan['plan_sha256'])
        self.assertEqual(result['phase'], 'committed')
        self.assertFalse(result['timing']['under_180_seconds'])
        self.assertFalse(result['performance_qualified'])

    def test_unified_acceptance_can_finish_while_an_independent_worker_warms(self):
        self.fixture()
        plan = self.prepare()
        run = self.ex.run
        worker_sha = self.request['executables']['pir-query-server']['sha256']
        self.ex.unhealthy.add(('worker', worker_sha))
        early = []
        def accept(host, argv, timeout):
            if argv[0] == 'node-probe':
                early.append(True)
                self.ex.unhealthy.discard(('worker', worker_sha))
            return run(host, argv, timeout)
        self.ex.run = accept
        self.assertEqual(self.target.deploy(plan['plan_sha256'])['phase'], 'committed')
        self.assertTrue(early)

    def test_verify_does_not_restart_or_extend_original_deployment_time(self):
        self.fixture()
        plan = self.prepare()
        original = self.target.deploy(plan['plan_sha256'])
        self.time += 600
        self.ex.log.clear()
        verified = self.new_target().recover('verify')
        self.assertEqual(original['timing']['source_to_accepted'], verified['timing']['source_to_accepted'])
        self.assertEqual(original['timing']['product_gaps'], verified['timing']['product_gaps'])
        self.assertFalse([event for _, op, event in self.ex.log if op == 'systemctl'])

    def test_cuda_is_checked_for_a_unified_process_before_shutdown(self):
        self.fixture()
        original_run = self.ex.run
        configuration = '/etc/wallet-pir-query/query-server.json'
        document = json.loads(self.ex.read('query', configuration))
        document['enhance']['settings']['matvec'] = {'matvec_backend': 'cuda', 'cuda_device': 1}
        self.ex.put('query', configuration, json.dumps(document))
        # Keep the reviewed desired configuration consistent with the installed
        # settings; CUDA selection does not change the process sandbox.
        self.inventory.services['query-server']['roles']['combined'][0]['settings']['enhance']['matvec'] = \
            document['enhance']['settings']['matvec']
        def unavailable(host, argv, timeout):
            if 'check-backend' in argv:
                return 1, 'device unavailable'
            return original_run(host, argv, timeout)
        self.ex.run = unavailable
        with self.assertRaisesRegex(DeployError, 'CUDA backend check failed'):
            self.prepare()
        self.assertFalse([event for _, op, event in self.ex.log if op == 'systemctl'])

    def test_failed_restoration_keeps_both_errors_and_can_be_reconciled(self):
        self.fixture()
        plan = self.prepare()
        self.ex.failing_restart.add(('publisher', 'pir-publisher.service'))
        with self.assertRaisesRegex(DeployError, 'restoration failed'):
            self.target.deploy(plan['plan_sha256'])
        self.assertEqual(self.target.data['phase'], 'rollback-failed')
        self.assertIn('deploy_error', self.target.data['errors'])
        self.assertIn('rollback_error', self.target.data['errors'])
        self.ex.failing_restart.clear()
        self.assertEqual(self.new_target().recover('reconcile')['phase'], 'rolled-back')

    def test_warming_predecessor_is_not_restarted_on_recovery_retry(self):
        self.fixture()
        plan = self.prepare()
        self.ex.unhealthy.add(('query', self.old_query))
        run = self.ex.run
        def reject_candidate(host, argv, timeout):
            if argv[0] == 'node-probe' and self.ex.publisher_major() == 2:
                return 1, 'incorrect answer'
            return run(host, argv, timeout)
        self.ex.run = reject_candidate
        with self.assertRaisesRegex(DeployError, 'restoration failed'):
            self.target.deploy(plan['plan_sha256'])
        before = self.ex.probe_unit('query', 'wallet-pir-query.service')['invocation_id']
        self.ex.unhealthy.clear()
        self.assertEqual(self.new_target().recover('reconcile')['phase'], 'rolled-back')
        self.assertEqual(self.ex.probe_unit('query', 'wallet-pir-query.service')['invocation_id'], before)

    def test_unfinished_parent_fences_legacy_deployments(self):
        from wallet_pir_ops.deploy.engine import mutation_fence
        self.fixture()
        plan = self.prepare()
        self.ex.failing_restart.add(('publisher', 'pir-publisher.service'))
        with self.assertRaises(DeployError):
            self.target.deploy(plan['plan_sha256'])
        with self.assertRaisesRegex(DeployError, 'unfinished fleet release'):
            mutation_fence(self.inventory, self.ex)

    def test_parallel_failure_drains_started_operations_before_rollback(self):
        self.fixture()
        plan = self.prepare()
        query_started, worker_failed, release = threading.Event(), threading.Event(), threading.Event()
        original = self.ex.systemctl
        failures = []
        def control(host, *args):
            original(host, *args)
            if args[0] != 'restart' or self.ex.publisher_major() != 2:
                return
            if host == 'query':
                query_started.set()
                if not release.wait(5):
                    raise RuntimeError('test did not release outstanding start')
            if host == 'worker':
                worker_failed.set()
                raise RemoteError('worker start reply lost')
        self.ex.systemctl = control
        def deploy():
            try:
                self.target.deploy(plan['plan_sha256'])
            except BaseException as error:
                failures.append(error)
        thread = threading.Thread(target=deploy, daemon=True)
        thread.start()
        try:
            self.assertTrue(query_started.wait(3))
            self.assertTrue(worker_failed.wait(3))
            self.assertNotEqual(self.target.data['phase'], 'rolling-back')
            self.assertEqual(self.ex.publisher_major(), 2)
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertTrue(failures)
        self.assertEqual(self.target.data['phase'], 'rolled-back')

    def test_data_root_changes_refuse_before_service_changes(self):
        self.fixture(major=1, selected=[self.query_key])
        entry = self.inventory.services['query-server']['roles']['combined'][0]
        entry['settings']['enhance']['data_dir'] = '/srv/wallet-pir-query/other-enhance'
        with self.assertRaisesRegex(DeployError, 'persistent data roots|installed unit policy'):
            self.prepare()
        self.assertFalse([event for _, op, event in self.ex.log if op == 'systemctl'])

    def test_cli_prepare_deploy_status_and_rollback_use_one_transaction(self):
        from wallet_pir_ops.deploy import cli
        self.fixture()
        request_file = self.root / 'request.json'
        request_file.write_text(json.dumps(self.request))
        pin = ['--request', str(request_file), '--request-sha256', sha256(request_file.read_bytes())]
        base = ['--inventory', str(self.inventory_path), '--state-dir', str(self.root / 'state')]
        lines = []
        binaries = [argument for variant, path in self.binaries.items() for argument in ('--binary', variant + '=' + path)]
        self.assertEqual(cli.main([*base, 'fleet-release-prepare', *pin, *binaries], executor=self.ex, out=lines.append), 0)
        planned = json.loads(lines[-1])
        self.assertEqual(cli.main([*base, 'fleet-release-deploy', *pin, '--expect-plan-sha256', planned['plan_sha256']],
                                 executor=self.ex, out=lines.append), 0)
        deployed = json.loads(lines[-1])
        self.assertEqual(deployed['phase'], 'committed')
        self.assertEqual(cli.main([*base, 'fleet-release-status', *pin], executor=self.ex, out=lines.append), 0)
        self.assertEqual(json.loads(lines[-1])['transaction'], deployed['transaction'])
        self.assertEqual(cli.main([*base, 'fleet-release-rollback', *pin], executor=self.ex, out=lines.append), 0)
        self.assertEqual(json.loads(lines[-1])['phase'], 'rolled-back')


if __name__ == '__main__':
    unittest.main()
