"""External worker roles render real arguments with inputs read-only and state separately writable."""
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from wallet_pir_ops.deploy import descriptors, units  # noqa: E402
from worker_settings import SETTINGS  # noqa: E402
from tools.transparent.tests.test_history_provider_role import ProviderRole  # noqa: E402,F401

REGISTRY = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
RELEASE = '/opt/example/releases/' + '0' * 64


def paths(value):
    return [p.strip('"') for p in (value if isinstance(value, list) else [value]) for p in p.split()]


class WorkerRoles(unittest.TestCase):
    def test_every_product_registers_one_rendered_optional_worker(self):
        rendered = {(s, r.name) for s, service in REGISTRY.items()
                    for r in service.roles.values() if r.mode == 'role' and r.optional}
        self.assertEqual(rendered, set(SETTINGS))
        serving = {r.name for r in REGISTRY['enhance'].roles.values() if r.mode == 'role' and not r.optional}
        self.assertEqual(serving, {'worker', 'packing-router', 'query-ingress'})
        for service, role in SETTINGS:
            self.assertTrue(REGISTRY[service].roles[role].optional)
            self.assertTrue(REGISTRY[service].roles[role].owns_unit)

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
        settings = {**SETTINGS[('transparent-history', 'external-worker')], 'shard_dir': '/var/lib/history-worker/set'}
        with self.assertRaisesRegex(ValueError, 'overlaps'):
            REGISTRY['transparent-history'].roles['external-worker'].render(settings, RELEASE)

    def test_product_checks_are_the_services_own_loaders(self):
        check = lambda service, role, settings=None: REGISTRY[service].roles[role].render(
            settings or SETTINGS[(service, role)], RELEASE).check
        self.assertEqual(check('receiver', 'serve-publication')[:2], ('serve-publication', '--check'))
        self.assertEqual(check('transparent-history', 'external-worker')[-1], '--verify-only')
        # No staged inputs, so no check is manufactured.
        self.assertEqual(check('enhance', 'external-worker'), ())
        self.assertEqual(check('status', 'external-worker'), ())
        self.assertEqual(check('transparent-display', 'external-worker'), ())
        initial = {**SETTINGS[('transparent-display', 'external-worker')], 'publication_dir': '/srv/display/publications/c0'}
        self.assertEqual(check('transparent-display', 'external-worker', initial)[-1], '--verify-only')


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

    def test_the_publisher_invocation_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / 'controller.json'
            config.write_text(json.dumps(dict(data_dir='/srv/history/journal', publication_root='/srv/publications',
                                              initial_publication='/srv/publications/initial')))
            common = ['--config', str(config), '--config-path', '/etc/history/controller.json',
                      '--state-dir', '/srv/history-state']
            legacy = self.run_roles(*common)
            explicit = self.run_roles('--service', 'transparent-history', '--role', 'publisher', *common)
            self.assertEqual(legacy.returncode, 0, legacy.stderr)
            self.assertEqual(legacy.stdout, explicit.stdout)


if __name__ == '__main__':
    unittest.main()
