"""External worker roles: rendered whole from inventory settings, installed fresh, rolled back to absence.

Uses the repository's descriptors, product adapters and templates; only hosts are fake.
"""
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors, units  # noqa: E402
from wallet_pir_ops.deploy.engine import Deployer, DeployError  # noqa: E402
from wallet_pir_ops.deploy.lifecycle import Lifecycle  # noqa: E402

SERVICES = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
BINARY = b'receiver-directory with serve-publication'
SHA = sha256(BINARY)
NEXT = b'receiver-directory, a later revision'
NEXT_SHA = sha256(NEXT)
UNIT = 'receiver-serve-publication.service'
CONFIG = '/etc/receiver-serve/config.json'
SETTINGS = {
    'config_path': CONFIG, 'publication_record': '/etc/receiver-serve/record/publication.json',
    'publication_root': '/srv/receiver-serve/publications', 'state_dir': '/var/lib/receiver-serve',
    'bind': '127.0.0.1:18390', 'rpc_urls': ['http://127.0.0.1:8232'], 'no_auth': True,
    'limits': {'MemoryMax': '6G'},
}
HEALTH = 'http://127.0.0.1:18390/v1/receiver/health'
HISTORY_WORKER = {'listen': '127.0.0.1:8094', 'state_dir': '/var/lib/history-worker',
                  'publisher': 'https://publisher.example', 'publisher_network': 'main',
                  'publisher_genesis': '00' * 32}


class ExternalWorkers(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = Path(self.dir.name)
        self.state = root / 'state'
        self.binary, self.next = root / 'receiver-directory', root / 'receiver-directory-next'
        self.binary.write_bytes(BINARY)
        self.next.write_bytes(NEXT)
        self.fake = FakeFleet()
        self.fake.host('edge-01')
        self.fake.host('coordinator')
        self.fake.endpoints[('edge-01', HEALTH)] = UNIT
        self.fake.health_shapes[('edge-01', UNIT)] = lambda ready, exe: {'serving': 'ab' * 32 if ready else None}
        self.checks = []
        def product_check(host, argv):
            self.checks.append(argv)
            config = argv[argv.index('--config') + 1]
            # The check reads the staged copy, not the live path, before anything is replaced.
            text = self.fake.read(host, config)
            return (0, 'verified') if text and json.loads(text)['bind'] else (1, 'no configuration')
        self.fake.product_check = product_check
        self.inventory = root / 'inventory.json'
        self.write_inventory()

    def tearDown(self):
        self.dir.cleanup()

    def write_inventory(self, install=True, settings=None, extra=None):
        entry = {'host': 'edge-01', 'vars': {'listen': '127.0.0.1:18390'}, 'settings': settings or SETTINGS}
        if install is not None:
            entry['install'] = install
        document = {
            'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
            'hosts': {'edge-01': {}, 'coordinator': {}},
            'services': {'receiver': {'roles': {'serve-publication': [entry]}}, **(extra or {})},
        }
        self.inventory.write_text(json.dumps(document))

    def deployer(self, service='receiver', only=None):
        now = [0.0]
        def sleep(seconds):
            now[0] += seconds
        return Deployer(SERVICES[service], descriptors.load_inventory(self.inventory), self.fake, self.state,
                        lock=lambda: self.fake.hold_lock(*('coordinator', descriptors.LOCK_PATH)),
                        sleep=sleep, clock=lambda: now[0], out=lambda _: None, only=only)

    def installed(self):
        state = self.fake.probe_unit('edge-01', UNIT)
        return state['load_state'], state['active_state'], state['exe_sha256']

    def test_a_fresh_worker_installs_from_an_absent_baseline_and_rolls_back_to_absence(self):
        deployer = self.deployer()
        baseline = deployer.capture_baseline()
        self.assertEqual(baseline['targets']['serve-publication@edge-01']['load_state'], 'not-found')
        result = Lifecycle(deployer).inspect('preflight', SHA, str(self.binary))
        self.assertEqual(result.problems, ())
        self.assertEqual(result.targets[0]['action'], 'install')
        self.assertEqual({c['path'] for c in result.targets[0]['changes']},
                         {'/etc/systemd/system/' + UNIT, CONFIG})
        self.assertEqual(self.fake.log, [], 'plan and preflight change nothing')

        journal = deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        self.assertEqual(journal.status, 'committed')
        self.assertEqual(self.installed(), ('loaded', 'active', SHA))
        self.assertTrue(self.fake.host('edge-01').units[UNIT]['enabled'])
        unit = units.effective([self.fake.read('edge-01', '/etc/systemd/system/' + UNIT)])['Service']
        self.assertEqual(units.split_exec(unit['ExecStart'][-1])[1],
                         '/opt/receiver-pir/releases/%s/receiver-directory' % SHA)
        self.assertEqual(unit['MemoryMax'], '6G')
        self.assertIn('/var/lib/receiver-serve', unit['ReadWritePaths'])
        self.assertIn('/srv/receiver-serve/publications', unit['ReadOnlyPaths'])
        self.assertNotIn('EnvironmentFile', unit, 'no partner key or other credential by default')
        self.assertEqual(json.loads(self.fake.read('edge-01', CONFIG))['publication_record'],
                         SETTINGS['publication_record'])
        # The product's own check ran on the staged release against a staged copy.
        (argv,) = self.checks
        self.assertEqual(argv[:3], ['/opt/receiver-pir/releases/%s/receiver-directory' % SHA,
                                    'serve-publication', '--check'])
        self.assertIn('/transactions/%s/' % journal.id, argv[-1])
        self.assertIn('/var/lib/receiver-serve', self.fake.host('edge-01').dirs)

        # Nothing to do the second time.
        self.assertIsNone(self.deployer().deploy(SHA, str(self.binary), skip_exact_check=True))

        self.deployer().rollback(journal.id)
        self.assertEqual(self.installed(), ('not-found', 'inactive', None))
        self.assertIsNone(self.fake.read('edge-01', CONFIG))
        self.assertIn('/var/lib/receiver-serve', self.fake.host('edge-01').dirs, 'state is kept')

    def test_an_absent_unit_is_refused_without_an_explicit_install(self):
        self.write_inventory(install=None)
        deployer = self.deployer()
        deployer.capture_baseline()
        plans, problems = deployer.assess(SHA, str(self.binary))
        self.assertTrue(any('"install": true' in p for p in problems), problems)

    def test_a_refused_product_check_changes_no_unit(self):
        self.fake.product_check = lambda host, argv: (1, 'publication record: directory must be absolute')
        deployer = self.deployer()
        deployer.capture_baseline()
        with self.assertRaisesRegex(DeployError, 'product check refused'):
            deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        written = [d for _, op, d in self.fake.log if op == 'write']
        self.assertTrue(all('/transactions/' in path for path in written), written)
        self.assertEqual(self.installed(), ('not-found', 'inactive', None))

    def test_a_worker_that_does_not_start_is_rolled_back_to_absence(self):
        self.fake.failing_restart.add(('edge-01', UNIT))
        deployer = self.deployer()
        deployer.capture_baseline()
        with self.assertRaises(DeployError):
            deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        self.assertEqual(self.installed(), ('not-found', 'inactive', None))
        self.assertIsNone(self.fake.read('edge-01', CONFIG))

    def test_a_fresh_install_never_overwrites_an_operator_file(self):
        self.fake.put('edge-01', CONFIG, '{"operator": true}\n')
        deployer = self.deployer()
        deployer.capture_baseline()
        _, problems = deployer.assess(SHA, str(self.binary))
        self.assertTrue(any('does not overwrite' in p for p in problems), problems)

    def test_updates_and_configuration_changes_roll_back_to_the_previous_worker(self):
        deployer = self.deployer()
        deployer.capture_baseline()
        deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        first = self.fake.read('edge-01', CONFIG)
        self.write_inventory(settings={**SETTINGS, 'poll_seconds': 5})
        journal = self.deployer().deploy(NEXT_SHA, str(self.next), skip_exact_check=True)
        self.assertEqual(journal.hosts[0]['action'], 'restart')
        self.assertEqual(self.installed(), ('loaded', 'active', NEXT_SHA))
        self.assertEqual(json.loads(self.fake.read('edge-01', CONFIG))['poll_seconds'], 5)
        self.deployer().rollback(journal.id)
        self.assertEqual(self.installed(), ('loaded', 'active', SHA))
        self.assertEqual(self.fake.read('edge-01', CONFIG), first)

    def test_the_report_separates_process_identity_publication_and_readiness(self):
        deployer = self.deployer()
        deployer.capture_baseline()
        record = {'directory': '/srv/receiver-serve/publications/a', 'revision': 'cd' * 32}
        self.fake.health_shapes[('edge-01', UNIT)] = lambda ready, exe: {
            'identity': {'binary_sha256': exe, 'incarnation': 'i1'}, 'serving': 'ab' * 32, 'epoch': 0,
            'publication': {'assigned': record, 'loaded': record, 'preparing': None, 'refusal': None}}
        output = []
        base = ['--inventory', str(self.inventory), '--state-dir', str(self.state)]
        self.assertEqual(cli.main(base + ['worker-report', 'receiver'], executor=self.fake, out=output.append), 0)
        (absent,) = json.loads(output[-1])['workers']
        self.assertEqual((absent['process']['alive'], absent['process']['load_state']), (False, 'not-found'))
        self.assertEqual(absent['publication'], 'unobserved')
        deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        self.fake.log.clear()
        cli.main(base + ['worker-report', 'receiver', '--only', 'serve-publication'],
                 executor=self.fake, out=output.append)
        (worker,) = json.loads(output[-1])['workers']
        self.assertEqual(self.fake.log, [], 'the report changes nothing')
        self.assertTrue(worker['process']['alive'])
        self.assertTrue(worker['executable']['matches_process'])
        self.assertEqual(worker['publication']['loaded'], record)
        self.assertEqual(worker['readiness']['verified'], True)
        self.assertTrue(worker['encrypted_correctness'].startswith('unobserved'))

    def test_settings_and_install_belong_to_role_mode_roles_only(self):
        document = json.loads(self.inventory.read_text())
        document['services']['receiver']['roles']['server'] = [
            {'host': 'edge-01', 'install': True, 'vars': {'listen': '127.0.0.1:18380'}}]
        self.inventory.write_text(json.dumps(document))
        with self.assertRaisesRegex(descriptors.DescriptorError, 'install is a boolean'):
            descriptors.targets(SERVICES['receiver'], descriptors.load_inventory(self.inventory))
        self.write_inventory(settings={**SETTINGS, 'near_key': 'x'})
        _, problems = self.deployer().assess(SHA, str(self.binary), require_baseline=False)
        self.assertTrue(any('unknown role settings' in p for p in problems), problems)

    def test_product_services_admit_the_shared_engine_only_for_external_workers(self):
        history = {'transparent-history': {'roles': {
            'worker': [{'host': 'edge-01', 'vars': {'listen': '127.0.0.1:8093'}}],
            'external-worker': [{'host': 'coordinator', 'install': True, 'vars': {'listen': '127.0.0.1:8094'},
                                 'settings': HISTORY_WORKER}]}}}
        self.write_inventory(extra=history)
        base = ['--inventory', str(self.inventory), '--state-dir', str(self.state), '--json']
        output = []
        code = cli.main(base + ['plan', 'transparent-history', '--binary', str(self.binary)],
                        executor=self.fake, out=output.append)
        self.assertEqual(code, 1)
        self.assertIn('product recipes', output[-1])
        output.clear()
        code = cli.main(base + ['plan', 'transparent-history', '--binary', str(self.binary),
                                '--only', 'worker'], executor=self.fake, out=output.append)
        self.assertEqual(code, 1, 'the product\'s own worker stays with its recipes')
        output.clear()
        code = cli.main(base + ['plan', 'transparent-history', '--binary', str(self.binary),
                                '--only', 'external-worker'], executor=self.fake, out=output.append)
        self.assertEqual(code, 0, output)
        (target,) = json.loads(output[-1])['targets']
        self.assertEqual(target['action'], 'install')


class ProviderLifecycle(unittest.TestCase):
    setUp = ExternalWorkers.setUp
    tearDown = ExternalWorkers.tearDown
    write_inventory = ExternalWorkers.write_inventory
    deployer = ExternalWorkers.deployer
    def setUp(self):
        ExternalWorkers.setUp(self)
        settings = json.loads((ROOT / 'tools/transparent/deploy/history-provider.example.json').read_text())
        self.write_inventory(extra={'transparent-history': {'roles': {'provider': [{
            'host': 'edge-01', 'vars': {'listen': settings['listen']},
            'settings': settings, 'install': True}]}}})

    def test_provider_refuses_generic_cli_mutation_before_running_a_loader(self):
        base = ['--inventory', str(self.inventory), '--state-dir', str(self.state), '--json']
        for command in ('plan', 'preflight', 'deploy'):
            with self.subTest(command=command):
                output = []
                result = cli.main(base + [command, 'transparent-history', '--only', 'provider',
                                         '--binary', str(self.binary)], executor=self.fake, out=output.append)
                self.assertEqual(result, 1, output)
                self.assertIn('ordinary binary deployment', output[-1])
                self.assertEqual(self.fake.log, [])
                self.assertEqual(self.checks, [])

    def test_direct_lifecycle_refuses_exclusive_state_before_staging(self):
        deployer = self.deployer('transparent-history', only=['provider'])
        deployer.capture_baseline()
        result = Lifecycle(deployer).inspect('preflight', SHA, str(self.binary), stage=True)
        self.assertTrue(any('operator-managed' in p for p in result.problems), result)
        self.assertEqual(self.fake.log, [])
        self.assertEqual(self.checks, [])
        with self.assertRaisesRegex(DeployError, 'operator-managed'):
            deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        from wallet_pir_ops.deploy.transaction import Journal
        journal = Journal.create(self.state, 'transparent-history', SHA, None,
                                 [{'role': 'provider', 'host': 'edge-01',
                                   'unit': 'transparent-history-provider.service'}], {})
        with self.assertRaisesRegex(DeployError, 'operator-managed'):
            deployer.rollback(journal.id)
        self.assertEqual(self.fake.log, [])
        self.assertEqual(self.checks, [])

    def test_existing_worker_transaction_rolls_back_with_provider_in_inventory(self):
        document = json.loads(self.inventory.read_text())
        document['services']['transparent-history']['roles']['external-worker'] = [{
            'host': 'edge-01', 'install': True, 'vars': {'listen': '127.0.0.1:8094'},
            'settings': HISTORY_WORKER}]
        self.inventory.write_text(json.dumps(document))
        unit = 'transparent-history-worker.service'
        self.fake.endpoints[('edge-01', 'http://127.0.0.1:8094/v1/ready')] = unit
        self.fake.health_shapes[('edge-01', unit)] = lambda ready, exe: {'ready': ready}
        deployer = self.deployer('transparent-history', only=['external-worker'])
        deployer.capture_baseline()
        journal = deployer.deploy(SHA, str(self.binary), skip_exact_check=True)
        self.assertEqual(journal.status, 'committed')
        self.fake.log.clear()
        output = []
        code = cli.main(['--inventory', str(self.inventory), '--state-dir', str(self.state),
                         'rollback', 'transparent-history', '--transaction', journal.id],
                        executor=self.fake, out=output.append,
                        lock=lambda: self.fake.hold_lock('coordinator', descriptors.LOCK_PATH))
        self.assertEqual(code, 0, output)
        self.assertEqual(self.fake.probe_unit('edge-01', unit)['load_state'], 'not-found')
        self.assertNotIn('/var/lib/transparent-provider', self.fake.host('edge-01').dirs)
        self.assertEqual(self.checks, [], 'neither role runs a loader against live state')

    def test_provider_private_report_still_works_without_a_deploy(self):
        output = []
        code = cli.main(['--inventory', str(self.inventory), 'worker-report', 'transparent-history',
                         '--only', 'provider'], executor=self.fake, out=output.append)
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output[-1])['workers'][0]['synchronization'], 'unobserved')
        self.assertEqual(self.fake.log, [])


class TwoEnhanceWorkers(unittest.TestCase):
    """Two external Enhance workers on one host each keep the config they were given."""

    def test_install_writes_a_config_and_management_port_per_unit(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        directory = Path(root.name)
        binary = directory / 'pir-query-server'
        binary.write_bytes(b'pir-query-server for two external workers')
        digest = sha256(binary.read_bytes())
        workers = (
            ('enhance-pir-worker-0.service', '127.0.0.1:8091', '/srv/enhance-worker/0'),
            ('enhance-pir-worker-1.service', '127.0.0.1:8092', '/srv/enhance-worker/1'),
        )
        inventory = directory / 'inventory.json'
        inventory.write_text(json.dumps({
            'lock': {'type': 'remote', 'host': 'local'}, 'ssh': {'mode': 'config'},
            'hosts': {'local': {}},
            'services': {'enhance': {'roles': {'external-worker': [
                {'host': 'local', 'install': True, 'unit': unit, 'vars': {'listen': listen},
                 'settings': {'listen': listen, 'data_dir': data, 'synchronization': {
                     'publisher_origin': 'https://publisher.example', 'shards': [0],
                     'download_budget_bytes': 4 << 30, 'cache_limit_bytes': 4 << 30}}}
                for unit, listen, data in workers]}}},
        }))
        fake = FakeFleet()
        for unit, listen, _data in workers:
            fake.endpoints[('local', 'http://%s/internal/health' % listen)] = unit
        now = [0.0]

        def sleep(seconds):
            now[0] += seconds

        deployer = Deployer(SERVICES['enhance'], descriptors.load_inventory(inventory), fake, directory / 'state',
                            lock=lambda: fake.hold_lock('local', descriptors.LOCK_PATH),
                            sleep=sleep, clock=lambda: now[0], out=lambda _line: None)
        deployer.capture_baseline()
        journal = deployer.deploy(digest, str(binary), skip_exact_check=True)
        self.assertEqual(journal.status, 'committed')
        configs = []
        for unit, listen, data in workers:
            text = fake.read('local', '/etc/systemd/system/' + unit)
            command = units.exec_start(units.effective([text]))
            (path,) = [item for item in command.split() if item.endswith('.json')]
            self.assertNotEqual(path, '/etc/enhance-pir/external-worker.json')
            document = json.loads(fake.read('local', path))
            self.assertEqual(document['enhance']['settings']['listen'], listen)
            self.assertEqual(document['enhance']['settings']['data_dir'], data)
            configs.append((path, document['management_listen']))
        self.assertEqual(len({path for path, _port in configs}), 2)
        self.assertEqual(len({port for _path, port in configs}), 2)
        self.assertNotIn('127.0.0.1:18091', {port for _path, port in configs})


if __name__ == '__main__':
    unittest.main()
