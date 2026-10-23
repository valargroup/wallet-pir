"""An approved cache volume is created once, in the existing host's project."""
import copy
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops import durable
from wallet_pir_ops.deploy import cache_volume
from wallet_pir_ops.deploy.engine import DeployError

REQUEST = {'host': 'details', 'project_id': 'project', 'droplet_id': 42,
           'region': 'ams3', 'name': 'wallet-pir-details-data', 'size_gib': 500}


class Provider:
    def __init__(self):
        self.volumes = []
        self.writes = []
        self.team = 'Valargroup'
        self.resources = [{'urn': 'do:droplet:42'}]

    def droplet(self, identifier):
        return {'id': 42, 'name': 'details', 'region': {'slug': 'ams3'},
                'networks': {'v4': [{'ip_address': '10.0.0.18'}]}}

    def get(self, path):
        if path == '/v2/account':
            return {'account': {'team': {'name': self.team}}}
        if path == '/v2/projects/project':
            return {'project': {'name': 'wallet-pir'}}
        if '/resources?' in path:
            return {'resources': self.resources}
        if path.startswith('/v2/volumes?'):
            return {'volumes': self.volumes}
        if path == '/v2/volumes/volume':
            return {'volume': self.volumes[0]}
        raise AssertionError(path)

    def write(self, method, path, body):
        self.writes.append((method, path, copy.deepcopy(body)))
        if path == '/v2/volumes':
            volume = {'id': 'volume', 'name': body['name'], 'description': body['description'],
                      'filesystem_type': body['initial_filesystem_type'], 'size_gigabytes': body['size_gigabytes'],
                      'droplet_ids': []}
            self.volumes.append(volume)
            return {'volume': volume}
        if path == '/v2/volumes/volume/actions':
            self.volumes[0]['droplet_ids'] = [body['droplet_id']]
            return {'action': {'id': 9, 'status': 'completed'}}
        if path == '/v2/projects/project/resources':
            return {'resources': []}
        raise AssertionError(path)


class VolumeTests(unittest.TestCase):
    def setUp(self):
        self.api = Provider()
        self.inventory = SimpleNamespace(hosts={'details': {'address': '10.0.0.18'}})

    def plan(self):
        return cache_volume.review(REQUEST, self.inventory, self.api)

    def test_wrong_account_project_and_host_refuse_before_creation(self):
        self.api.team = 'Other'
        with self.assertRaisesRegex(DeployError, 'Valargroup'):
            self.plan()
        self.api.team = 'Valargroup'
        self.api.resources = []
        with self.assertRaisesRegex(DeployError, 'approved project'):
            self.plan()
        self.api.resources = [{'urn': 'do:droplet:42'}]
        self.inventory.hosts['details']['address'] = '10.0.0.19'
        with self.assertRaisesRegex(DeployError, 'pinned inventory'):
            self.plan()
        self.assertEqual(self.api.writes, [])

    def test_retry_reuses_volume_after_creation_and_after_attachment(self):
        plan = self.plan()
        receipts = []
        cache_volume.provision(plan, self.api, lambda state: receipts.append(copy.deepcopy(state)))
        result = cache_volume.provision(plan, self.api, lambda state: receipts.append(copy.deepcopy(state)))
        self.assertEqual(result['phase'], 'attached')
        self.assertEqual(len([entry for entry in self.api.writes if entry[1] == '/v2/volumes']), 1)
        self.assertEqual(len([entry for entry in self.api.writes if entry[1].endswith('/actions')]), 1)
        self.assertEqual(self.api.volumes[0]['size_gigabytes'], 500)
        self.assertIn('created', [state['phase'] for state in receipts])

    def test_foreign_existing_volume_cannot_be_reused_or_attached(self):
        self.api.volumes = [{'id': 'foreign', 'name': REQUEST['name'], 'description': 'another owner',
                             'size_gigabytes': 500, 'filesystem_type': 'ext4'}]
        with self.assertRaisesRegex(DeployError, 'approved candidate'):
            cache_volume.provision(self.plan(), self.api, lambda state: None)
        self.assertEqual(self.api.writes, [])

    def test_lost_create_response_is_recovered_by_name_and_request_identity(self):
        plan = self.plan()
        self.api.volumes = [{'id': 'volume', 'name': REQUEST['name'],
                             'description': 'Wallet PIR Details query cache; request ' + durable.digest(REQUEST),
                             'filesystem_type': 'ext4', 'size_gigabytes': 500, 'droplet_ids': []}]
        result = cache_volume.provision(plan, self.api, lambda state: None)
        self.assertEqual(result['phase'], 'attached')
        self.assertFalse(any(path == '/v2/volumes' for _, path, _ in self.api.writes))


if __name__ == '__main__':
    unittest.main()
