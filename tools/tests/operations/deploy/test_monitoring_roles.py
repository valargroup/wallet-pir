"""The central APM and the independent monitor as rendered roles.

Each is installed on a fresh host, updated, observed, rolled back, and adopted
from the legacy unit an existing host runs, with the repository's descriptors,
renderers and templates; only hosts are fake. Adoption's rollback must bring
back the legacy unit, its drop-ins and its credential launcher.
"""
import dataclasses
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops import monitoring_certs, monitoring_roles  # noqa: E402
from wallet_pir_ops.deploy import descriptors, units  # noqa: E402
from wallet_pir_ops.deploy.engine import Deployer, DeployError  # noqa: E402

SERVICES = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
# The opt-in legacy descriptor: the same `apm` service and journal sequence, role `server`.
LEGACY_APM = descriptors.load_descriptors(ROOT / 'tools/enhance/deploy/apm.toml')['apm']
HOOK = 'https://hooks.slack.com/services/T0/B0/never-rendered'
# Every loader the descriptors accept on a legacy unit.
LAUNCHERS = {tuple(path for path, _ in role.adopt_launcher or role.launcher)
             for service in (SERVICES['apm'], SERVICES['monitor'], LEGACY_APM) for role in service.roles.values()}
APM_SETTINGS = {'environment': {'PIR_APM_ALERT_MODE': 'shadow', 'PIR_APM_SCRAPE_URL': 'http://127.0.0.1:8080',
                                'PIR_APM_PEER_STATUS_URL': 'https://monitor-pir.valargroup.dev/monitor-status',
                                'PIR_APM_STATUS_TOPOLOGY': 'Enhance host → CPU router + worker'}}
MONITOR_SETTINGS = {'environment': {
    'PIR_MONITOR_ORIGIN': 'https://enhance-pir.valargroup.dev',
    'PIR_MONITOR_APM_STATUS_URL': 'https://enhance-pir.valargroup.dev/apm/monitor-status',
    'PIR_MONITOR_ORACLE': '/etc/pir-monitor/oracle.json', 'PIR_MONITOR_ORACLE_SHA256': '0' * 64},
    'chain_rpc_cookie': True,
    'service_probes': [{'service': 'receiver', 'command': ['/opt/pir-monitor/receiver-probe'],
                        'timeout_seconds': 30}]}
CASES = {
    'apm': {'role': 'central', 'host': 'coordinator', 'unit': 'pir-apm.service', 'binary': 'pir-apm',
            'settings': APM_SETTINGS, 'legacy': ROOT / 'tools/enhance/deploy/pir-apm.service',
            'legacy_drop_ins': {'pir-apm-observability.conf': ROOT / 'tools/enhance/deploy/pir-apm-observability.conf',
                                '80-observers.conf': ROOT / 'tools/enhance/deploy/pir-apm-observers.conf'}},
    'monitor': {'role': 'checker', 'host': 'monitor', 'unit': 'pir-monitor.service', 'binary': 'pir-monitor',
                'settings': MONITOR_SETTINGS, 'legacy': ROOT / 'tools/enhance/deploy/pir-monitor.service',
                'legacy_drop_ins': {'90-service-quality.conf':
                                    ROOT / 'tools/enhance/deploy/pir-monitor-service-quality.conf'}},
}


class LaunchedFleet(FakeFleet):
    """Systemd that reports the executable a credential launcher execs, as the kernel does."""

    def _start(self, host, unit):
        super()._start(host, unit)
        state = self.host(host).units[unit]
        for launcher in LAUNCHERS:
            if state['active'] == 'active' and units.uses_launcher(state['exec_start'], launcher):
                state['exe'] = self.sha256(host, units.split_exec(state['exec_start'], launcher)[1])


def effective(fleet, host, unit):
    return units.effective(text for _, text in fleet._disk(host, unit))


class RoleCase:
    """One service's fake host, inventory and deployer for a role under test."""

    def __init__(self, test, name):
        case = CASES[name]
        self.test, self.name, self.case = test, name, case
        self.root = Path(test.dir.name) / name
        self.root.mkdir()
        self.service = SERVICES[name]
        self.role = self.service.roles[case['role']]
        self.launcher = tuple(path for path, _ in self.role.adopt_launcher)
        self.fleet = test.fleet
        self.host, self.unit = case['host'], case['unit']
        self.fleet.host(self.host)
        self.fleet.endpoints[(self.host, self.role.health)] = self.unit
        if self.role.ready_url:
            self.fleet.endpoints[(self.host, self.role.ready_url)] = self.unit
        if self.role.ready and 'nonempty' in self.role.ready:
            field = self.role.ready['field']
            self.fleet.health_shapes[(self.host, self.unit)] = lambda ready, exe: {field: 7 if ready else 0}
        self.settings = case['settings']

    def binary(self, contents):
        path = self.root / ('%s-%s' % (self.case['binary'], sha256(contents)[:8]))
        path.write_bytes(contents)
        return path

    def inventory(self, role=None, settings=None):
        entry = {'host': self.host}
        if (role or self.case['role']) == self.case['role']:
            entry.update(install=True, settings=settings or self.settings)
        path = self.root / 'inventory.json'
        path.write_text(json.dumps({'lock': {'type': 'remote', 'host': self.host}, 'ssh': {'mode': 'config'},
                                    'hosts': {self.host: {}},
                                    'services': {self.name: {'roles': {role or self.case['role']: [entry]}}}}))
        return descriptors.load_inventory(path)

    def deployer(self, **inventory):
        now = [0.0]

        def sleep(seconds):
            now[0] += seconds
        return Deployer(self.service, self.inventory(**inventory), self.fleet, self.root / 'state',
                        lock=lambda: self.fleet.hold_lock(self.host, descriptors.LOCK_PATH),
                        sleep=sleep, clock=lambda: now[0], out=lambda _: None)

    def deploy(self, contents, **options):
        deployer = self.deployer(**options.pop('inventory', {}))
        deployer.capture_baseline()
        return deployer, deployer.deploy(sha256(contents), str(self.binary(contents)), skip_exact_check=True,
                                         **options)

    def install_legacy(self):
        """The host as it runs today: the legacy unit, its drop-ins and the credential launcher."""
        legacy = '/opt/legacy/%s' % self.case['binary']
        drop_ins = [('/etc/systemd/system/%s.d/%s' % (self.unit, name), path.read_text())
                    for name, path in self.case['legacy_drop_ins'].items()]
        drop_ins.append(('/etc/systemd/system/%s.d/%s' % (self.unit, units.MANAGED_DROP_IN),
                         units.managed_drop_in(' '.join(self.launcher) + ' ', legacy, '')))
        self.fleet.install(self.host, self.unit, self.case['legacy'].read_text(), drop_ins, files={
            self.launcher[0]: b'python', self.launcher[1]: b'credential loader', legacy: b'legacy release',
            '/etc/pir-monitor/monitor.env': b'PIR_MONITOR_ORIGIN=https://enhance-pir.valargroup.dev\n'})
        self.test.assertEqual(self.fleet.probe_unit(self.host, self.unit)['exe_sha256'], sha256(b'legacy release'))
        return drop_ins

    def pins(self):
        return tuple((path, self.fleet.sha256(self.host, path)) for path in self.launcher)

    def pin_launcher(self, service=None):
        """Pin this host's loader bytes; the descriptors' production pins name the live loader's."""
        service = service or self.service
        field = 'adopt_launcher' if service is self.service else 'launcher'
        roles = {name: dataclasses.replace(role, **{field: self.pins()}) for name, role in service.roles.items()}
        return dataclasses.replace(service, roles=roles)


class RenderedRoles(unittest.TestCase):
    def test_probe_settings_match_the_monitor_service_names_and_timeout_bounds(self):
        for service in ('status', 'transparent', 'receiver', 'txid'):
            for timeout in (1, 45):
                self.assertEqual(json.loads(monitoring_roles._probes([
                    {'service': service, 'command': ['/bin/true'], 'timeout_seconds': timeout}]))[0]['service'], service)
        for service, timeout in (('custom', 30), ('receiver', 0), ('receiver', 46), ('receiver', 300)):
            with self.subTest(service=service, timeout=timeout), self.assertRaises(ValueError):
                monitoring_roles._probes([{'service': service, 'command': ['/bin/true'], 'timeout_seconds': timeout}])

    def test_units_reference_scoped_credentials_and_never_a_webhook(self):
        rendered = monitoring_roles.apm(APM_SETTINGS, release='/opt/pir-apm/releases/abc',
                                        template=SERVICES['apm'].roles['central'].template.read_text())
        service = units.effective([rendered.unit])['Service']
        self.assertEqual(units.exec_start(units.effective([rendered.unit])), '/opt/pir-apm/releases/abc/pir-apm')
        self.assertEqual(sorted(service['LoadCredential']), sorted(
            ['%s:%s/%s' % (n, monitoring_certs.COLLECTOR_CREDENTIALS, n) for n in monitoring_certs.COLLECTOR_FILES]
            + ['slack-webhook:/etc/pir-apm/alerting/slack-webhook', 'chain-cookie:/root/.cache/zakura/.cookie']))
        self.assertEqual(service['Environment']['PIR_APM_SLACK_WEBHOOK_FILE'], 'slack-webhook')
        self.assertEqual(service['Environment']['PIR_APM_OBSERVERS'], '/etc/pir-apm/observers.json')
        self.assertEqual(service['Environment']['PIR_APM_STATUS_TOPOLOGY'], 'Enhance host → CPU router + worker')
        self.assertNotIn('EnvironmentFile', service)
        self.assertNotIn('hooks.slack.com', rendered.unit)
        monitor = monitoring_roles.monitor(MONITOR_SETTINGS, release='/opt/pir-monitor/releases/abc',
                                           template=SERVICES['monitor'].roles['checker'].template.read_text())
        service = units.effective([monitor.unit])['Service']
        self.assertEqual(service['User'], 'pir-monitor')
        self.assertEqual(sorted(service['LoadCredential']), [
            'chain-rpc-cookie:/etc/pir-monitor/credentials/chain-rpc-cookie',
            'slack-webhook:/etc/pir-monitor/credentials/slack-webhook'])
        self.assertEqual(json.loads(monitor.files[monitoring_roles.MONITOR_PROBES]),
                         MONITOR_SETTINGS['service_probes'])
        self.assertEqual(service['Environment']['PIR_MONITOR_SERVICE_PROBES_CONFIG'], monitoring_roles.MONITOR_PROBES)

    def test_settings_cannot_carry_secrets_or_override_rendered_paths(self):
        template = SERVICES['apm'].roles['central'].template.read_text()
        for environment in ({'PIR_APM_SLACK_WEBHOOK_URL': HOOK}, {'PIR_APM_PUBLIC_URL': HOOK},
                            {'PIR_APM_OBSERVERS': '/tmp/other.json'}, {'PIR_APM_ALERT_MODE': 'loud'},
                            {'PIR_MONITOR_ORIGIN': 'https://x'}, {'PIR_APM_TITLE': 'a"b'},
                            {'PIR_APM_TITLE': '$HOME'}, {'PIR_APM_TITLE': 'a\nExecStart=/bin/sh'}):
            with self.subTest(environment=environment), self.assertRaises(ValueError):
                monitoring_roles.apm({'environment': environment}, release='/opt/pir-apm/releases/a',
                                     template=template)
        with self.assertRaises(ValueError):
            monitoring_roles.apm({'credentials': ['/etc/default/pir-apm'], 'unknown': 1},
                                 release='/opt/pir-apm/releases/a', template=template)
        template = SERVICES['monitor'].roles['checker'].template.read_text()
        incomplete = dict(MONITOR_SETTINGS['environment'])
        del incomplete['PIR_MONITOR_ORACLE_SHA256']
        for settings in ({'environment': incomplete},
                         dict(MONITOR_SETTINGS, service_probes=[{'service': 'x', 'command': ['probe'],
                                                                 'timeout_seconds': 5}]),
                         dict(MONITOR_SETTINGS, environment=dict(MONITOR_SETTINGS['environment'],
                                                                 PIR_MONITOR_LISTEN='0.0.0.0:3003'))):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                monitoring_roles.monitor(settings, release='/opt/pir-monitor/releases/a', template=template)


class FreshHosts(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.fleet = LaunchedFleet()

    def test_install_update_status_and_rollback_on_a_fresh_host(self):
        for name in CASES:
            with self.subTest(service=name):
                case = RoleCase(self, name)
                _, installed = case.deploy(b'release one')
                self.assertEqual(installed.status, 'committed')
                self.assertEqual(installed.hosts[0]['action'], 'install')
                state = self.fleet.probe_unit(case.host, case.unit)
                self.assertEqual((state['active_state'], state['exe_sha256']), ('active', sha256(b'release one')))
                self.assertTrue(self.fleet.host(case.host).units[case.unit]['enabled'])
                unit = self.fleet.read(case.host, '/etc/systemd/system/' + case.unit)
                self.assertNotIn('hooks.slack.com', unit)
                self.assertIn('slack-webhook', ''.join(effective(self.fleet, case.host, case.unit)['Service']
                                                        ['LoadCredential']))
                # Idempotent: the same release and settings change nothing.
                self.assertIsNone(case.deployer().deploy(sha256(b'release one'), skip_exact_check=True))
                # A settings change alone restarts with the new unit, reviewed as unit drift;
                # so does a new release.
                changed = dict(case.settings, slack=False)
                with self.assertRaisesRegex(DeployError, 'differs from the live one'):
                    case.deploy(b'release one', inventory={'settings': changed})
                _, update = case.deploy(b'release one', inventory={'settings': changed}, allow_drift=True)
                self.assertEqual(update.hosts[0]['action'], 'restart')
                self.assertNotIn('LoadCredential=slack-webhook', self.fleet.read(
                    case.host, '/etc/systemd/system/' + case.unit))
                deployer, upgrade = case.deploy(b'release two', inventory={'settings': changed})
                self.assertEqual(self.fleet.probe_unit(case.host, case.unit)['exe_sha256'], sha256(b'release two'))
                status = deployer.status()
                self.assertEqual(status['targets'][0]['binary_sha256'], sha256(b'release two'))
                self.assertEqual((status['transaction'], status['phase']), (upgrade.id, 'committed'))
                # Rollbacks walk back, latest first, to absence.
                for journal, expected in ((upgrade, b'release one'), (update, b'release one')):
                    deployer.rollback(journal.id)
                    self.assertEqual(self.fleet.probe_unit(case.host, case.unit)['exe_sha256'], sha256(expected))
                self.assertIn('LoadCredential=slack-webhook', self.fleet.read(
                    case.host, '/etc/systemd/system/' + case.unit))
                deployer.rollback(installed.id)
                self.assertEqual(self.fleet.probe_unit(case.host, case.unit)['load_state'], 'not-found')

    def test_a_role_that_never_becomes_ready_is_removed_again(self):
        for name in CASES:
            with self.subTest(service=name):
                case = RoleCase(self, name)
                # A missing credential source fails the start, as systemd's LoadCredential does.
                self.fleet.failing_restart.add((case.host, case.unit))
                with self.assertRaises(DeployError):
                    case.deploy(b'broken')
                self.assertEqual(self.fleet.probe_unit(case.host, case.unit)['load_state'], 'not-found')


class LegacyAdoption(unittest.TestCase):
    """An existing APM or monitor host moves to the rendered role and back."""

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.fleet = LaunchedFleet()

    def test_adoption_needs_explicit_acceptance_and_rolls_back_to_the_legacy_unit(self):
        for name in CASES:
            with self.subTest(service=name):
                case = RoleCase(self, name)
                drop_ins = case.install_legacy()
                case.service = case.pin_launcher()
                before = {path: self.fleet.read(case.host, path) for path, _ in drop_ins}
                fragment = self.fleet.read(case.host, '/etc/systemd/system/' + case.unit)
                with self.assertRaisesRegex(DeployError, 'pass --retire-historical to adopt it'):
                    case.deploy(b'rendered release')
                with self.assertRaisesRegex(DeployError, 'differs from the live one beyond the binary'):
                    case.deploy(b'rendered release', retire_historical=True)
                deployer, journal = case.deploy(b'rendered release', retire_historical=True, allow_drift=True)
                self.assertEqual(journal.status, 'committed')
                self.assertEqual(journal.hosts[0]['launcher'], [{'path': p, 'sha256': d} for p, d in case.pins()])
                self.assertTrue(any('ExecStart' in line for line in journal.hosts[0]['drift']))
                state = self.fleet.probe_unit(case.host, case.unit)
                self.assertEqual((state['exe_sha256'], state['drop_ins']), (sha256(b'rendered release'), []))
                for path, _ in drop_ins:
                    self.assertIsNone(self.fleet.read(case.host, path), 'retired into the transaction')
                start = units.exec_start(effective(self.fleet, case.host, case.unit))
                self.assertFalse(units.uses_launcher(start, case.launcher), 'the rendered unit loads no launcher')
                # A later deploy is an ordinary update of the rendered role.
                self.assertIsNone(case.deployer().deploy(sha256(b'rendered release'), skip_exact_check=True))
                # The legacy rollback path: the adopting transaction restores it whole.
                deployer.rollback(journal.id)
                self.assertEqual(self.fleet.probe_unit(case.host, case.unit)['exe_sha256'], sha256(b'legacy release'))
                self.assertEqual(self.fleet.read(case.host, '/etc/systemd/system/' + case.unit), fragment)
                self.assertEqual({path: self.fleet.read(case.host, path) for path, _ in drop_ins}, before)

    def test_unpinned_launcher_and_a_changed_loader_are_refused_without_changes(self):
        case = RoleCase(self, 'monitor')
        case.install_legacy()
        # The descriptor's production pins do not match this host's loader bytes.
        with self.assertRaisesRegex(DeployError, 'credential launcher changed'):
            case.deploy(b'rendered release', retire_historical=True, allow_drift=True)
        self.assertEqual(self.fleet.probe_unit(case.host, case.unit)['exe_sha256'], sha256(b'legacy release'))
        case.service = case.pin_launcher()
        _, journal = case.deploy(b'rendered release', retire_historical=True, allow_drift=True)
        self.fleet.put(case.host, case.launcher[1], b'changed loader')
        with self.assertRaisesRegex(DeployError, 'credential launcher changed'):
            case.deployer().rollback(journal.id)
        self.assertEqual(self.fleet.probe_unit(case.host, case.unit)['exe_sha256'], sha256(b'rendered release'))

    def test_the_legacy_descriptor_deploys_again_after_the_adoption_is_rolled_back(self):
        case = RoleCase(self, 'apm')
        case.install_legacy()
        case.service = case.pin_launcher()
        deployer, adoption = case.deploy(b'rendered release', retire_historical=True, allow_drift=True)
        # While the rendered role is current, the legacy descriptor cannot replace it.
        legacy = case.pin_launcher(LEGACY_APM)
        rendered, case.service = case.service, legacy
        with self.assertRaises(DeployError):
            case.deploy(b'legacy update', inventory={'role': 'server'})
        case.service = rendered
        deployer.rollback(adoption.id)
        case.service = legacy
        _, journal = case.deploy(b'legacy update', inventory={'role': 'server'})
        start = units.exec_start(effective(self.fleet, case.host, case.unit))
        self.assertTrue(start.startswith(' '.join(case.launcher) + ' /opt/pir-apm/releases/'))
        self.assertEqual(self.fleet.probe_unit(case.host, case.unit)['exe_sha256'], sha256(b'legacy update'))
        self.assertEqual(journal.hosts[0]['role'], 'server')


if __name__ == '__main__':
    unittest.main()
