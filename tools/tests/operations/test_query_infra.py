"""Only an approved query node's additive creation may pass provisioning review."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'lib'))
from wallet_pir_ops.query_infra import NODES, RESOURCES, initial_plan, prefix

PROJECT = '00000000-0000-0000-0000-000000000001'


PREFIX = prefix('01')


def plan(node='01'):
    name = 'wallet-pir-query-' + node
    values = {
        'digitalocean_droplet.query': {'size': NODES[node]['size'], 'region': 'ams3', 'name': name},
        'digitalocean_volume.data': {'size': NODES[node]['volume_gib'], 'name': name + '-data', 'region': 'ams3'},
        'digitalocean_project_resources.query': {'project': PROJECT},
        'cloudflare_dns_record.query': {'name': name + '.valargroup.dev', 'type': 'A', 'proxied': False},
    }
    return {'complete': True, 'resource_changes': [
        {'mode': 'managed', 'address': prefix(node) + resource,
         'change': {'actions': ['create'], 'after': values.get(resource, {})}}
        for resource in sorted(RESOURCES)]}


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

    def test_a_noop_cannot_hide_a_fleet_move_or_import(self):
        for extra in ({'previous_address': 'legacy.host'},
                      {'change': {'actions': ['no-op'], 'importing': {'id': 'existing-id'}}}):
            value = plan()
            value['resource_changes'].append({'mode': 'managed', 'address': 'existing.host',
                                             'change': {'actions': ['no-op']}, **extra})
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                initial_plan(value, PROJECT)

    def test_the_replacement_node_has_its_own_smaller_capacity_and_name(self):
        self.assertEqual(len(initial_plan(plan('02'), PROJECT, '02')), 6)
        self.assertTrue(all(address.startswith('module.query_serving_02[0].')
                            for address in initial_plan(plan('02'), PROJECT, '02')))
        # Each node's plan is refused under the other node's identity.
        for value, node in ((plan('01'), '02'), (plan('02'), '01')):
            with self.subTest(node=node), self.assertRaises(ValueError):
                initial_plan(value, PROJECT, node)
        for field, wrong in (('size', NODES['01']['size']), ('name', 'wallet-pir-query-01')):
            value = plan('02')
            for resource in value['resource_changes']:
                if resource['address'].endswith('digitalocean_droplet.query'):
                    resource['change']['after'][field] = wrong
            with self.subTest(field=field), self.assertRaises(ValueError):
                initial_plan(value, PROJECT, '02')

    def test_adding_the_second_node_cannot_touch_the_first(self):
        value = plan('02')
        value['resource_changes'].append({'mode': 'managed', 'address': PREFIX + 'digitalocean_droplet.query',
                                         'change': {'actions': ['no-op']}})
        initial_plan(value, PROJECT, '02')
        value['resource_changes'][-1]['change']['actions'] = ['update']
        with self.assertRaisesRegex(ValueError, 'unrelated'):
            initial_plan(value, PROJECT, '02')


if __name__ == '__main__':
    unittest.main()
