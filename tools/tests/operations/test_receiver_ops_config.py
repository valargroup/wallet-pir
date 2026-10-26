"""Receiver root roles, provider edge, release bundle and sandbox agree offline."""
import json
from pathlib import Path
import re
import sys
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops.deploy import descriptors, units
from wallet_pir_ops import query_role, release
from tools.receiver.lib import roles

QUERY = {
    'config_path': '/etc/receiver/query.json', 'state_dir': '/srv/wallet-pir-query/receiver',
    'listen': '127.0.0.1:18380', 'management_listen': '127.0.0.1:18381',
    'publisher': 'https://publisher.example', 'genesis': [1] * 32, 'activation_height': 100,
    'publisher_ca': '/etc/receiver/ca.pem',
}


class ReceiverOpsConfig(unittest.TestCase):
    def test_shared_deployment_replaces_one_off_cutover_commands(self):
        from wallet_pir_ops.deploy import cli
        help_text = cli.parser().format_help()
        self.assertIn('publisher-cutover-deploy', help_text)
        self.assertIn('fleet-release-deploy', help_text)
        self.assertNotIn('receiver-routing-', help_text)
        self.assertNotIn('query-node-provision', help_text)

    def test_query_uses_https_only_and_preserves_capacity_and_sandbox(self):
        rendered = roles.query(QUERY, release='/opt/receiver/releases/test')
        config = json.loads(rendered.files[QUERY['config_path']])
        self.assertEqual(config['receiver']['settings']['publisher'], QUERY['publisher'])
        self.assertEqual(config['public_listen'], QUERY['listen'])
        service = units.effective([rendered.unit])['Service']
        self.assertEqual(units.exec_start(units.effective([rendered.unit])),
                         '/opt/receiver/releases/test/pir-query-server --config /etc/receiver/query.json')
        self.assertEqual(service['MemoryHigh'], '6G')
        self.assertEqual(service['MemoryMax'], '7G')
        self.assertEqual(service['MemorySwapMax'], '0')
        self.assertIn(QUERY['state_dir'], service['ReadWritePaths'])
        self.assertIn(QUERY['publisher_ca'], service['ReadOnlyPaths'])
        self.assertNotIn('EnvironmentFile', service)
        self.assertEqual(rendered.check, ('--config', QUERY['config_path'], '--check'))
        for field in ('rpc_urls', 'cookie', 'no_auth', 'publication_record', 'near_key'):
            with self.subTest(field=field), self.assertRaises(ValueError):
                roles.query({**QUERY, field: 'refused'}, release='/release')
        for origin in ('http://publisher.example', 'https://user@publisher.example',
                       'https://publisher.example/path', 'https://publisher.example?x=1'):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                roles.query({**QUERY, 'publisher': origin}, release='/release')

    def test_existing_store_is_owned_by_root_publisher(self):
        document = json.loads((ROOT / 'tools/receiver/deploy/receiver-publisher.example.json').read_text())
        settings = {'config_path': '/etc/receiver/publisher.json',
                    'management_listen': document['management_listen'], 'chain': document['chain'],
                    'receiver': document['receiver']['settings']}
        rendered = roles.publisher(settings, release='/opt/receiver/releases/test')
        config = json.loads(rendered.files[settings['config_path']])
        self.assertEqual(config['receiver']['settings']['data_dir'], '/srv/receiver-pir/index')
        self.assertNotIn('rpc_url', config['receiver']['settings'])
        self.assertIn('rpc_url', config['chain'])
        self.assertIn('/pir-publisher --config ', units.exec_start(units.effective([rendered.unit])))
        service = units.effective([rendered.unit])['Service']
        self.assertEqual(service['User'], 'receiver-pir')
        self.assertEqual(service['MemoryMax'], '7G')
        self.assertIn('near-', service['EnvironmentFile'][0])

    def test_all_five_products_share_the_root_query_listener(self):
        settings = json.loads((ROOT / 'tools/deploy/query-server/query-node.example.json').read_text())
        rendered = query_role.combined(settings, release='/release')
        document = json.loads(next(iter(rendered.files.values())))
        self.assertTrue(all(document[name]['enabled'] for name in
                            ('enhance', 'status', 'transparent_history', 'transaction_details', 'receiver')))
        self.assertEqual(document['receiver']['settings']['publisher'],
                         settings['receiver']['publisher'])
        self.assertNotIn('rpc_url', document['receiver']['settings'])

    def test_registry_and_release_name_only_supported_entrypoints(self):
        service = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')['receiver']
        self.assertEqual(service.binary, 'pir-publisher')
        self.assertEqual(service.roles['server'].unit, 'receiver-pir.service')
        self.assertEqual(service.roles['query'].binary, 'pir-query-server')
        self.assertNotIn('serve-publication', service.roles)
        self.assertEqual(release.BINARIES['receiver-pir'],
                         ['pir-publisher', 'pir-query-server', 'receiver-probe'])
        manifest = tomllib.loads((ROOT / 'src/receiver/receiver-indexer/Cargo.toml').read_text())
        self.assertEqual([item['name'] for item in manifest['bin']], ['receiver-probe'])
        for path in release.FILES['receiver-pir']:
            self.assertTrue((ROOT / path).is_file(), path)

    def test_edge_exposes_wallet_and_provider_namespaces_only(self):
        text = (ROOT / 'tools/receiver/digitalocean/Caddyfile').read_text()
        wallet = re.search(r'@wallet path (.*)', text).group(1).split()
        expected = {prefix + operation for prefix in ('/receiver/v1', '/v1/receiver')
                    for operation in ('/inventory', '/inventory/*', '/query', '/public/*',
                                      '/shard-filters/*', '/block-filters/*')}
        self.assertEqual(set(wallet), expected)
        provider = re.search(r'@provider path (.*)', text).group(1).split()
        self.assertEqual(provider, ['/provider/receiver/v1/*'])
        self.assertRegex(text, r'handle @provider\s*\{\s*reverse_proxy \{\$RECEIVER_PUBLISHER_UPSTREAM\}')
        self.assertRegex(text, r'handle /\.well-known/pir-service-discovery\s*\{\s*reverse_proxy \{\$RECEIVER_PUBLISHER_UPSTREAM\}')
        self.assertNotIn('127.0.0.1:1838', text)
        self.assertIn('header_up Host {upstream_hostport}', text)
        self.assertNotIn('/v1/receiver/health', wallet)
        self.assertIn('reverse_proxy {$RECEIVER_QUERY_UPSTREAM}', text)
        self.assertRegex(text, r'handle\s*\{\s*respond 404')


if __name__ == '__main__':
    unittest.main()
