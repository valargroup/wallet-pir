"""The unified deployment reuses product validation and owns one bounded unit."""
import copy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from wallet_pir_ops.deploy import descriptors, units
from wallet_pir_ops.query_role import CONFIG
from worker_settings import SETTINGS

RELEASE = '/opt/wallet-pir-query/releases/' + '0' * 64


def settings():
    enhance = copy.deepcopy(SETTINGS['enhance', 'combined'])
    enhance.pop('credentials')
    return {
        'listen': '127.0.0.1:8090', 'management_listen': '127.0.0.1:18081',
        'credentials': ['/etc/wallet-pir-query/internal-token.env'],
        'enhance': enhance,
        'status': copy.deepcopy(SETTINGS['status', 'combined']),
        'transparent_history': {**SETTINGS['transparent-history', 'external-worker'],
                                'state_dir': '/srv/wallet-pir-query/history', 'full_holdings': True},
        'transaction_details': {**SETTINGS['transparent-display', 'external-worker'],
                                'state_dir': '/srv/wallet-pir-query/details'},
    }


class QueryRole(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.role = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')['query-server'].roles['combined']

    def render(self, value):
        return self.role.render(value, RELEASE)

    def test_products_share_one_listener_but_keep_separate_state_and_https_sources(self):
        value = settings()
        value['enhance']['data_dir'] = '/srv/wallet-pir-query/enhance'
        value['status']['state_dir'] = '/srv/wallet-pir-query/status'
        value['status']['publisher_ca'] = '/etc/wallet-pir-query/publisher-ca.crt'
        result = self.render(value)
        config = json.loads(result.files[CONFIG])
        self.assertEqual(config['public_listen'], value['listen'])
        self.assertEqual(set(config) - {'public_listen', 'management_listen'},
                         {'enhance', 'status', 'transparent_history', 'transaction_details'})
        self.assertEqual(config['enhance']['settings']['role'], 'combined')
        self.assertEqual(config['status']['settings']['role'], 'combined')
        self.assertTrue(config['transparent_history']['settings']['full_holdings'])
        self.assertNotIn('archive_upstream', config['transparent_history']['settings'])
        self.assertEqual(len(result.directories), 4)
        service = units.effective([result.unit])['Service']
        self.assertEqual(service['MemoryMax'], '160G')
        self.assertEqual(service['MemorySwapMax'], '0')
        self.assertIn('/etc/wallet-pir-query/publisher-ca.crt', service['ReadOnlyPaths'])
        self.assertEqual(result.check, ('--config', CONFIG, '--check'))
        self.assertEqual(units.split_exec(units.exec_start(units.effective([result.unit])))[1],
                         RELEASE + '/pir-query-server')

    def test_installer_settings_serve_all_five_products(self):
        value = json.loads((ROOT / 'tools/deploy/query-server/install-node.json').read_text())
        config = json.loads(self.render(value).files[CONFIG])
        self.assertEqual(set(config) - {'public_listen', 'management_listen'},
                         {'enhance', 'status', 'transparent_history', 'transaction_details', 'receiver'})
        self.assertEqual(config['receiver']['settings']['publisher'], 'https://receiver-pir.valargroup.dev')

    def test_invalid_compositions_are_refused_before_deployment(self):
        baseline = settings()
        baseline['enhance']['data_dir'] = '/srv/wallet-pir-query/enhance'
        baseline['status']['state_dir'] = '/srv/wallet-pir-query/status'
        mutations = [
            lambda s: s.update(receiver={}),
            lambda s: s.update(listen='0.0.0.0:8090'),
            lambda s: s.update(management_listen=s['listen']),
            lambda s: s['status'].update(query_listen='127.0.0.1:8099'),
            lambda s: s['status'].update(listen=s['transaction_details']['listen']),
            lambda s: s['status'].update(state_dir=s['enhance']['data_dir']),
            lambda s: s['status'].update(state_dir='/var/lib/status'),
            lambda s: s['status'].update(publisher='http://publisher.example'),
            lambda s: s['transparent_history'].update(full_holdings=False),
            lambda s: s['transparent_history'].update(archive_upstream='https://archive.example'),
            lambda s: s['enhance'].update(credentials=['/etc/ignored.env']),
            lambda s: s.update(config_path='/srv/wallet-pir-query/history/config.json'),
        ]
        for mutate in mutations:
            value = copy.deepcopy(baseline)
            mutate(value)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.render(value)


if __name__ == '__main__':
    unittest.main()
