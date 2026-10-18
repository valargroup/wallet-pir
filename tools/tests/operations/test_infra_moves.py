"""Exercise refusal of actions, changed values, unknowns and missing identities."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'lib'))
from wallet_pir_ops.infra_layout import compare, destination

OLD = 'digitalocean_droplet.transparent_archive["transparent-pir-archive-01"]'
NEW = 'module.transparent.' + OLD
MOVES = {'digitalocean_droplet.transparent_archive': 'module.transparent.digitalocean_droplet.transparent_archive',
         'digitalocean_droplet.transparent_archive[0]': OLD}


def plan(address, previous=None):
    values = {'id': '12345', 'name': 'unchanged', 'user_data': 'unchanged cloud-init'}
    resource = {'address': address, 'mode': 'managed', 'values': values}
    change = {'address': address, 'mode': 'managed', 'change': {
        'actions': ['no-op'], 'before': values, 'after': values, 'after_unknown': {}}}
    if previous:
        change['previous_address'] = previous
    return {'complete': True, 'planned_values': {'root_module': {'resources': [resource]},
            'outputs': {'host': {'value': 'unchanged', 'sensitive': False}}}, 'resource_changes': [change]}


class MoveComparison(unittest.TestCase):
    def test_preserves_ids_values_and_outputs(self):
        self.assertEqual(compare(plan(OLD), plan(NEW, OLD), MOVES),
                         {'resources': 1, 'moved_instances': 1, 'provider_actions': 0})
        self.assertEqual(destination('digitalocean_droplet.transparent_archive[0]', MOVES), NEW)

    def test_rejects_every_provider_action(self):
        for actions in (['create'], ['update'], ['delete'], ['delete', 'create'], ['read']):
            for which in (0, 1):
                plans = [plan(OLD), plan(NEW, OLD)]
                plans[which]['resource_changes'][0]['change']['actions'] = actions
                with self.subTest(actions=actions, which=which), self.assertRaises(ValueError):
                    compare(*plans, MOVES)

    def test_rejects_incomplete_changed_or_undeclared_results(self):
        mutations = [lambda p: p.update(variables={'different': {'value': True}}),
                     lambda p: p.update(complete=False),
                     lambda p: p.update(resource_drift=[{}]),
                     lambda p: p.update(deferred_changes=[{}]),
                     lambda p: p['resource_changes'][0].update(previous_address='unrelated.resource'),
                     lambda p: p['resource_changes'][0].pop('previous_address'),
                     lambda p: p['resource_changes'].clear(),
                     lambda p: p['resource_changes'].append(copy.deepcopy(p['resource_changes'][0])),
                     lambda p: p['planned_values']['root_module']['resources'][0]['values'].update(id='different'),
                     lambda p: p['planned_values']['root_module']['resources'][0]['values'].update(user_data='changed'),
                     lambda p: p['resource_changes'][0]['change'].update(after_unknown={'nested': [True]}),
                     lambda p: p['planned_values']['outputs']['host'].update(value='changed')]
        for mutate in mutations:
            after = copy.deepcopy(plan(NEW, OLD))
            mutate(after)
            with self.assertRaises(ValueError):
                compare(plan(OLD), after, MOVES)


if __name__ == '__main__':
    unittest.main()
