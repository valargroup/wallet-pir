"""External worker roles render real arguments with inputs read-only and state separately writable."""
import json
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from wallet_pir_ops.deploy import descriptors, units  # noqa: E402
from worker_settings import SETTINGS  # noqa: E402
from tools.transparent.tests.test_history_provider_role import ProviderRole, assert_history_config  # noqa: E402,F401

REGISTRY = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
RELEASE = '/opt/example/releases/' + '0' * 64


def paths(value):
    return [p.strip('"') for p in (value if isinstance(value, list) else [value]) for p in p.split()]


class WorkerRoles(unittest.TestCase):
    def test_rendered_units_run_the_release_with_inputs_read_only(self):
        for (service, role), settings in SETTINGS.items():
            with self.subTest(service=service):
                spec = REGISTRY[service]
                result = spec.roles[role].render(settings, RELEASE)
                effective = units.effective([result.unit])['Service']
                command = units.exec_start(units.effective([result.unit]))
                binary = units.split_exec(command)[1]
                self.assertEqual(binary, '%s/%s' % (RELEASE, spec.roles[role].binary or spec.binary))
                if service == 'enhance':
                    self.assertIn('--config /etc/enhance-pir/%s.json' % role, command)
                writable = paths(effective.get('ReadWritePaths', ''))
                readonly = paths(effective.get('ReadOnlyPaths', ''))
                self.assertEqual(list(result.directories), writable)
                for read in readonly:
                    self.assertFalse(any(PurePosixPath(read).is_relative_to(w) or PurePosixPath(w).is_relative_to(read)
                                         for w in writable), (read, writable))
                for path in result.files:
                    self.assertTrue(any(PurePosixPath(path).is_relative_to(r) for r in readonly), path)
                self.assertIn('strict', str(effective.get('ProtectSystem')))

    def test_templates_carry_no_fleet_address_or_provider_credential(self):
        for service in REGISTRY.values():
            for role in service.roles.values():
                if role.mode != 'role':
                    continue
                text = role.template.read_text()
                self.assertIsNone(re.search(r'\b10\.\d+\.\d+\.\d+|EnvironmentFile|NEAR', text), role.template)

    def test_budgets_and_credentials_come_from_settings_only(self):
        result = REGISTRY['enhance'].roles['external-worker'].render(SETTINGS[('enhance', 'external-worker')], RELEASE)
        effective = units.effective([result.unit])['Service']
        self.assertEqual(effective['MemoryMax'], '7G')
        self.assertEqual(effective['EnvironmentFile'], ['/etc/enhance-worker/internal-token.env'])
        for bad in ({'limits': {'ExecStartPre': '/bin/sh'}}, {'limits': {'MemoryMax': '7G\nUser=root'}},
                    {'near_key': 'x'}, {'credentials': ['relative.env']}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                REGISTRY['enhance'].roles['external-worker'].render(
                    {**SETTINGS[('enhance', 'external-worker')], **bad}, RELEASE)

    def test_inputs_cannot_sit_inside_state(self):
        settings = {**SETTINGS[('transparent-history', 'external-worker')],
                    'publisher_ca': '/var/lib/history-worker/publisher-ca.pem'}
        with self.assertRaisesRegex(ValueError, 'overlaps'):
            REGISTRY['transparent-history'].roles['external-worker'].render(settings, RELEASE)


    def test_transparent_roles_run_the_host_over_one_generated_section(self):
        configs = {('transparent-history', 'external-worker'): '/etc/transparent-pir/history-worker.json',
                   ('transparent-history', 'https-recent'): '/etc/transparent-pir/history-https-recent.json',
                   ('transparent-history', 'provider'): '/etc/transparent-pir/history-provider.json',
                   ('transparent-display', 'external-worker'): '/etc/transparent-pir/display-worker.json'}
        managements = set()
        for (service, role), path in configs.items():
            with self.subTest(service=service, role=role):
                settings = SETTINGS[(service, role)]
                result = REGISTRY[service].roles[role].render(settings, RELEASE)
                command = shlex.split(units.exec_start(units.effective([result.unit])))
                self.assertEqual(command, ['%s/pir-query-server' % RELEASE, '--config', path])
                readonly = paths(units.effective([result.unit])['Service'].get('ReadOnlyPaths', ''))
                self.assertIn(path, readonly)
                section = 'transaction_details' if service == 'transparent-display' else 'transparent_history'
                body = assert_history_config(self, result.files[path], settings['listen'], section)
                managements.add(json.loads(result.files[path])['management_listen'])
                for name, value in settings.items():
                    if isinstance(value, int) and not isinstance(value, bool):
                        self.assertIs(type(body[name]), int, name)
                        self.assertEqual(body[name], value)
                if role in ('provider', 'https-recent'):
                    self.assertEqual(result.check, ('--config', path, '--verify-only'))
                else:
                    # `--verify-only` with a publisher would synchronize into live state.
                    self.assertEqual(result.check, ())
        self.assertEqual(len(managements), len(configs))

    def test_external_transparent_workers_render_the_current_https_consumer(self):
        expected = {
            'transparent-history': ('/etc/transparent-pir/history-worker.json', 'transparent_history',
                                    {'listen': '127.0.0.1:8093', 'shard_dir': '/var/lib/history-worker',
                                     'publisher': 'https://publisher.example.invalid', 'publisher_network': 'main',
                                     'publisher_genesis': '00' * 32, 'cache_bytes': 8 << 30}),
            'transparent-display': ('/etc/transparent-pir/display-worker.json', 'transaction_details',
                                    {'listen': '127.0.0.1:8095', 'role': 'combined',
                                     'state_dir': '/var/lib/display-worker',
                                     'publisher': 'https://publisher.example.invalid', 'publisher_network': 'main',
                                     'publisher_genesis': '00' * 32}),
        }
        for service, (path, section, body) in expected.items():
            with self.subTest(service=service):
                role = REGISTRY[service].roles['external-worker']
                result = role.render(SETTINGS[(service, 'external-worker')], RELEASE)
                self.assertEqual(set(result.files), {path})
                self.assertEqual(json.loads(result.files[path])[section]['settings'], body)
                self.assertEqual(list(result.directories), [body.get('state_dir', body.get('shard_dir'))])
                for bad in ({'publisher': 'http://publisher.example.invalid'}, {'publisher_network': 'test'},
                            {'publisher_genesis': 'AB' * 32}, {'control_socket': '/run/x.sock'}):
                    with self.subTest(bad=bad), self.assertRaises(ValueError):
                        role.render({**SETTINGS[(service, 'external-worker')], **bad}, RELEASE)

    def test_status_roles_refuse_the_removed_cuda_setting(self):
        with self.assertRaisesRegex(ValueError, 'unknown role settings'):
            REGISTRY['status'].roles['external-worker'].render(
                {**SETTINGS[('status', 'external-worker')], 'cuda': False}, RELEASE)

    def test_two_external_workers_on_one_host_do_not_share_a_config(self):
        role = REGISTRY['enhance'].roles['external-worker']
        base = SETTINGS[('enhance', 'external-worker')]
        alone = role.render(base, RELEASE)
        first = role.render({**base, 'listen': '127.0.0.1:8091', 'data_dir': '/srv/enhance-worker/0'},
                            RELEASE, 'enhance-pir-worker-0.service')
        second = role.render({**base, 'listen': '127.0.0.1:8092', 'data_dir': '/srv/enhance-worker/1'},
                             RELEASE, 'enhance-pir-worker-1.service')
        alone_path = '/etc/enhance-pir/external-worker.json'
        self.assertIn('--config ' + alone_path, units.exec_start(units.effective([alone.unit])))
        self.assertEqual(json.loads(alone.files[alone_path])['management_listen'], '127.0.0.1:18091')
        paths = []
        ports = []
        for rendered, listen, data in (
                (first, '127.0.0.1:8091', '/srv/enhance-worker/0'),
                (second, '127.0.0.1:8092', '/srv/enhance-worker/1')):
            command = units.exec_start(units.effective([rendered.unit]))
            (path,) = [item for item in command.split() if item.endswith('.json')]
            self.assertNotEqual(path, alone_path)
            document = json.loads(rendered.files[path])
            self.assertEqual(document['enhance']['settings']['listen'], listen)
            self.assertEqual(document['enhance']['settings']['data_dir'], data)
            paths.append(path)
            ports.append(document['management_listen'])
        self.assertEqual(len(set(paths)), 2)
        self.assertEqual(len(set(ports)), 2)
        self.assertNotIn('127.0.0.1:18091', ports)


class StatusInstalledRoles(unittest.TestCase):
    def settings(self, role):
        return dict(network_hex='08' * 32, state_dir='/var/lib/status-' + role,
                    listen='127.0.0.1:' + ('8481' if role == 'worker' else '8482'),
                    publisher='https://publisher.example', worker_origin='http://127.0.0.1:8481',
                    **({'query_listen': '127.0.0.1:8484'} if role == 'router' else {}))

    def test_deployment_owns_configuration_and_runs_its_loader_before_restart(self):
        for role in ('worker', 'router'):
            with self.subTest(role=role):
                spec = REGISTRY['status'].roles[role]
                rendered = spec.render(self.settings(role), RELEASE)
                path = '/etc/status-pir/' + role + '.json'
                document = json.loads(rendered.files[path])
                effective = units.effective([rendered.unit])['Service']
                self.assertEqual(spec.mode, 'role')
                self.assertEqual(document['status']['settings'], {'role': role, **self.settings(role)})
                self.assertEqual(rendered.check, ('--config', path, '--check'))
                self.assertIn(path, paths(effective['ReadOnlyPaths']))
                self.assertEqual(paths(effective['ReadWritePaths']), ['/var/lib/status-' + role])
                self.assertEqual(effective['MemoryMax'], '12G' if role == 'worker' else '3G')

    def test_inventory_example_renders_https_status_consumers(self):
        inventory = json.loads((ROOT / 'tools/enhance/deploy/deploy-inventory.example.json').read_text())
        for role in ('worker', 'router'):
            with self.subTest(role=role):
                settings = inventory['services']['status']['roles'][role][0]['settings']
                rendered = REGISTRY['status'].roles[role].render(settings, RELEASE)
                document = json.loads(rendered.files['/etc/status-pir/' + role + '.json'])
                actual = document['status']['settings']
                self.assertEqual(actual['publisher'], 'https://enhance.example')
                self.assertNotIn('artifact_origin', actual)
                self.assertEqual(actual['worker_origin'], 'http://127.0.0.1:8481')
                self.assertEqual(actual['listen'], '127.0.0.1:' + ('8481' if role == 'worker' else '8482'))
                if role == 'router':
                    self.assertEqual(actual['query_listen'], '127.0.0.1:8484')

    def test_invalid_network_or_non_loopback_peers_refuse_before_deployment(self):
        for role in ('worker', 'router'):
            bad_settings = [{'network_hex': 'ab'}, {'network_hex': 'FF' * 32},
                            {'listen': '0.0.0.0:8481'}, {'worker_origin': 'http://10.0.0.1:8481'},
                            {'publisher': 'http://publisher.example'}, {'listen': '127.0.0.1:65536'}]
            if role == 'worker':
                bad_settings.append({'query_listen': '127.0.0.1:8484'})
            for bad in bad_settings:
                with self.subTest(role=role, bad=bad), self.assertRaises(ValueError):
                    REGISTRY['status'].roles[role].render({**self.settings(role), **bad}, RELEASE)


class RolesCommand(unittest.TestCase):
    def run_roles(self, *arguments):
        return subprocess.run([sys.executable, str(ROOT / 'tools/scripts/wallet-pir-roles.py'), *arguments],
                              capture_output=True, text=True, timeout=30)

    def test_explicit_service_and_role_render_a_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = Path(directory) / 'settings.json'
            settings.write_text(json.dumps(SETTINGS[('receiver', 'serve-publication')]))
            result = self.run_roles('--service', 'receiver', '--role', 'serve-publication',
                                    '--settings', str(settings), '--release', RELEASE)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('/etc/receiver-serve/config.json', json.loads(result.stdout)['files'])
            refused = self.run_roles('--service', 'receiver', '--role', 'server', '--settings', str(settings),
                                     '--release', RELEASE)
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn('keep their installed units', refused.stderr)


if __name__ == '__main__':
    unittest.main()
