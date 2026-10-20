"""Standalone role policy: explicit origins, private administration and strict loader."""
import copy
import ipaddress
import json
from pathlib import Path
import shlex
import sys
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(ROOT))
from tools.transparent.lib.roles import history_provider, report  # noqa: E402
from wallet_pir_ops.deploy import units  # noqa: E402

CONFIG = '/etc/transparent-pir/history-provider.json'


def assert_history_config(test, text, listen, section='transparent_history'):
    """One enabled product section, a distinct loopback management listener, never verify_only."""
    document = json.loads(text)
    test.assertEqual(set(document), {'management_listen', section})
    management = document['management_listen']
    test.assertTrue(ipaddress.ip_address(management.rsplit(':', 1)[0]).is_loopback, management)
    test.assertNotEqual(management, listen)
    test.assertIs(document[section]['enabled'], True)
    settings = document[section]['settings']
    test.assertEqual(settings['listen'], listen)
    test.assertNotIn('verify_only', settings)
    return settings


class ProviderRole(unittest.TestCase):
    def setUp(self):
        self.settings = json.loads((ROOT / 'tools/transparent/deploy/history-provider.example.json').read_text())

    def test_public_example_renders_only_history_with_identical_install_and_restart_checks(self):
        role = history_provider(self.settings, release='/opt/provider/release')
        service = units.effective([role.unit])['Service']
        start = shlex.split(units.exec_start(units.effective([role.unit])))
        self.assertEqual(start, ['/opt/provider/release/pir-query-server', '--config', CONFIG])
        self.assertEqual(shlex.split(service['ExecStartPre'][0]), start + ['--verify-only'])
        self.assertEqual(role.check, ('--config', CONFIG, '--verify-only'))
        self.assertEqual(role.directories, (self.settings['state_dir'],))
        self.assertEqual(set(role.files), {CONFIG})
        self.assertIn(CONFIG, [p for line in service['ReadOnlyPaths'] for p in shlex.split(line)])
        settings = assert_history_config(self, role.files[CONFIG], self.settings['listen'])
        self.assertEqual(settings['shard_dir'], self.settings['state_dir'])
        for name in ('publisher', 'archive_upstream', 'publisher_network', 'publisher_genesis'):
            self.assertEqual(settings[name], self.settings[name])
        for name in ('cache_bytes', 'bootstrap_max_bytes', 'query_slots', 'query_waiters', 'body_bytes',
                     'query_deadline_secs', 'upload_deadline_secs'):
            self.assertIs(type(settings[name]), int, name)
            self.assertEqual(settings[name], self.settings[name])
        for name in ('control_socket', 'active_record', 'assignment', 'runtime_cache_dir'):
            self.assertNotIn(name, settings)
        self.assertNotIn('EnvironmentFile', service)
        self.assertEqual(service['User'], 'transparent-provider')

    def test_invalid_or_ambiguous_inputs_refused_before_install(self):
        for patch in ({'listen': '0.0.0.0:8092'}, {'listen': '127.0.0.1:0'},
                      {'publisher': 'http://publisher.example.invalid'},
                      {'archive_upstream': 'https://user:secret@archive.example.invalid'},
                      {'archive_upstream': 'https://archive.example.invalid/forward'},
                      {'publisher_genesis': 'wrong'}, {'publisher_network': 'other'},
                      {'publisher_ca': '/var/lib/transparent-provider/root.pem'},
                      {'state_dir': '/opt/provider/release/state'},
                      {'assignment': '/srv/fleet/assignment.json'}, {'query_slots': False}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                history_provider({**self.settings, **patch}, release='/opt/provider/release')
        missing = copy.deepcopy(self.settings)
        del missing['archive_upstream']
        with self.assertRaises(ValueError):
            history_provider(missing, release='/opt/provider/release')

    def test_report_preserves_independent_observations_and_unknowns(self):
        health = {'synchronization': {'state': 'refused'}, 'verified_publication': {'map_sha256': 'ab' * 32},
                  'runtime_readiness': {'warm': True}, 'active_coverage': [{'local': False}],
                  'archive_reachability': {'state': 'refused'}}
        observed = report('transparent-history', 'provider', health, 200)
        for key, value in health.items():
            self.assertEqual(observed[key], value)
        self.assertTrue(observed['readiness']['ready'])
        self.assertEqual(report('transparent-history', 'provider', None, None)['archive_reachability'], 'unobserved')

    def test_tls_example_preserves_same_origin_display_and_private_administration(self):
        text = (ROOT / 'tools/transparent/deploy/history-provider.caddy').read_text()
        self.assertIn('/v2/filters/shards', text)
        self.assertIn('/v2/shards', text)
        self.assertIn('@display path /v2/txid /v2/txid/*', text)
        self.assertIn('reverse_proxy {$TRANSPARENT_DISPLAY_UPSTREAM}', text)
        self.assertLess(text.index('handle @display'), text.index('respond 404'))
        self.assertNotIn('/v1/', text)
        self.assertNotIn('/metrics', text)
        self.assertIn('respond 404', text)


if __name__ == '__main__':
    unittest.main()
