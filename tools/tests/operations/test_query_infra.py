"""Only the approved additive candidate may pass initial provisioning review."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'lib'))
from wallet_pir_ops.query_infra import HOSTNAME, PREFIX, RESOURCES, SIZE, initial_plan

PROJECT = '00000000-0000-0000-0000-000000000001'


def plan():
    values = {
        'digitalocean_droplet.query': {'size': SIZE, 'region': 'ams3', 'name': 'wallet-pir-query-01'},
        'digitalocean_volume.data': {'size': 1024, 'name': 'wallet-pir-query-01-data', 'region': 'ams3'},
        'digitalocean_project_resources.query': {'project': PROJECT},
        'cloudflare_dns_record.query': {'name': HOSTNAME, 'type': 'A', 'proxied': False},
    }
    return {'complete': True, 'resource_changes': [
        {'mode': 'managed', 'address': PREFIX + name,
         'change': {'actions': ['create'], 'after': values.get(name, {})}}
        for name in sorted(RESOURCES)]}


class ProvisionPlan(unittest.TestCase):
    def test_only_the_additive_candidate_is_accepted(self):
        value = plan()
        value['resource_changes'].append({'mode': 'managed', 'address': 'existing.host',
                                         'change': {'actions': ['no-op']}})
        self.assertEqual(len(initial_plan(value, PROJECT)), 6)

    def test_unrelated_changes_replacements_and_incomplete_plans_are_refused(self):
        mutations = [
            lambda p: p.update(complete=False),
            lambda p: p.update(resource_drift=[{}]),
            lambda p: p.update(deferred_changes=[{}]),
            lambda p: p['resource_changes'].pop(),
            lambda p: p['resource_changes'].append(copy.deepcopy(p['resource_changes'][0])),
            lambda p: p['resource_changes'][0]['change'].update(actions=['delete', 'create']),
            lambda p: p['resource_changes'].append({'mode': 'managed', 'address': 'existing.host',
                                                  'change': {'actions': ['update']}}),
        ]
        for mutate in mutations:
            value = plan()
            mutate(value)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                initial_plan(value, PROJECT)

    def test_capacity_project_and_public_dns_cannot_be_changed(self):
        for name, field, wrong in [
            ('digitalocean_droplet.query', 'size', 'm-32vcpu-256gb'),
            ('digitalocean_droplet.query', 'region', 'nyc3'),
            ('digitalocean_volume.data', 'size', 2048),
            ('digitalocean_project_resources.query', 'project', 'another-project'),
            ('cloudflare_dns_record.query', 'name', 'wallet-pir.valargroup.dev'),
            ('cloudflare_dns_record.query', 'proxied', True),
            ('digitalocean_volume.data', 'size', '1024'),
        ]:
            value = plan()
            for resource in value['resource_changes']:
                if resource['address'] == PREFIX + name:
                    resource['change']['after'][field] = wrong
            with self.subTest(resource=name, field=field), self.assertRaises(ValueError):
                initial_plan(value, PROJECT)


if __name__ == '__main__':
    unittest.main()
