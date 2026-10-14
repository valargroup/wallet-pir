"""Complete-fleet validation of the operator inventory, and that product inventories keep working."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'ops/lib'))
from wallet_pir_ops.deploy import descriptors  # noqa: E402
from wallet_pir_ops.fleet import Fleet  # noqa: E402

EXAMPLE = json.loads((ROOT / 'enhance/ops/deploy/deploy-inventory.example.json').read_text())
SERVICES = descriptors.load_descriptors(ROOT / 'ops/deploy/services.toml')


class FleetInventoryTests(unittest.TestCase):
    def fleet(self, change=None):
        document = copy.deepcopy(EXAMPLE)
        if change:
            change(document)
        with tempfile.NamedTemporaryFile('w', suffix='.json') as handle:
            json.dump(document, handle)
            handle.flush()
            return Fleet(descriptors.load_inventory(handle.name), SERVICES)

    def refused(self, change, message, complete=True):
        problems = self.fleet(change).problems(complete)
        self.assertTrue(any(message in p for p in problems), problems)

    def test_the_example_is_a_complete_configured_fleet(self):
        view = self.fleet().describe(complete=True)
        self.assertEqual(view['view'], 'configured')
        self.assertEqual({r['service'] for r in view['roles']}, set(SERVICES))
        for name, service in SERVICES.items():
            self.assertEqual({r['role'] for r in view['roles'] if r['service'] == name}, set(service.order))
        writers = [(r['service'], r['role']) for r in view['roles'] if 'journal-writer' in r['responsibilities']]
        self.assertEqual(sorted(writers), [('transparent-display', 'controller'), ('transparent-history', 'controller')])
        kinds = {(a['kind'], c) for a in view['ancillary'] for c in a['consumers']}
        self.assertIn(('supervision', 'status'), kinds)
        self.assertIn(('routing', 'transparent-history'), kinds)
        self.assertTrue(all(a['operations'] == ['inspect'] for a in view['ancillary']))
        drivers = {r['service']: r['operations'] for r in view['roles']}
        self.assertNotIn('deploy', drivers['transparent-history'])
        self.assertIn('deploy', drivers['enhance'])
        cuda = [r for r in view['roles'] if r['artifact_kind'] == 'enhance-pir-native-cuda']
        self.assertEqual([r['host'] for r in cuda], ['enhance-pir-gpu-01'])
        self.assertEqual(sorted(view['retired_hosts'])[:1], ['transparent-archive-03'])

    def test_required_roles_services_responsibilities_and_ancillary(self):
        self.refused(lambda d: d['services']['enhance']['roles'].pop('packing-router'), 'required roles missing')
        self.refused(lambda d: d['services'].pop('receiver'), 'services missing')
        self.refused(lambda d: d['services']['transparent-history']['roles']['controller'][0].pop('responsibilities'),
                     'exactly one controller must hold journal-writer')
        def two_writers(d):
            roles = d['services']['transparent-display']['roles']
            roles['controller'].append({'host': 'coordinator', 'unit': 'second-display-controller.service',
                                        'vars': {'listen': '127.0.0.1:8510'}, 'responsibilities': ['journal-writer']})
        self.refused(two_writers, 'exactly one controller must hold journal-writer')
        def writer_on_worker(d):
            d['services']['transparent-history']['roles']['worker'][0]['responsibilities'] = ['journal-writer']
        self.refused(writer_on_worker, 'responsibilities must be among []')
        self.refused(lambda d: d['ancillary'].pop('control-sessions'), 'status: no ancillary supervision')

    def test_ownership_is_unique_per_machine_and_unit(self):
        def alias(d):
            d['hosts']['coordinator-alias'] = {'address': d['hosts']['coordinator']['address']}
            d['services']['status']['roles']['controller'][0]['host'] = 'coordinator-alias'
            d['services']['status']['roles']['controller'][0]['unit'] = 'enhance-pir-server.service'
        self.refused(alias, 'both own enhance-pir-server.service', complete=False)
        def shared_edge(d):
            d['ancillary']['second-edge'] = {'host': 'coordinator', 'unit': 'caddy.service', 'kind': 'routing',
                                             'consumers': ['status']}
        self.refused(shared_edge, 'both own caddy.service', complete=False)
        def case(d):
            d['hosts']['Coordinator'] = {'address': '192.0.2.99'}
        self.refused(case, 'differ only in case', complete=False)

    def test_connection_retired_and_unknown_hosts(self):
        with self.assertRaisesRegex(descriptors.DescriptorError, 'an address per host'):
            self.fleet(lambda d: d['hosts']['worker-01'].pop('address'))
        def retired_role(d):
            d['services']['enhance']['roles']['worker'][0]['host'] = 'transparent-recent-01'
        self.refused(retired_role, 'is retired (2026-10-14)', complete=False)
        def retired_ancillary(d):
            d['ancillary']['transparent-public-router']['host'] = 'transparent-recent-02'
        self.refused(retired_ancillary, 'is retired', complete=False)
        self.refused(lambda d: d['ancillary']['receiver-edge'].update(host='nowhere'), 'unknown host', complete=False)
        for change, message in ((lambda d: d['retired_hosts'].update({'worker-01': {}}), 'both live and retired'),
                                (lambda d: d['hosts']['router-01'].update(jump='transparent-recent-01'), 'retired host'),
                                (lambda d: d['lock'].update(host='transparent-recent-01'), 'retired')):
            with self.assertRaisesRegex(descriptors.DescriptorError, message):
                self.fleet(change)

    def test_artifact_variants_are_explicit_for_cuda(self):
        def implicit(d):
            d['services']['enhance']['roles']['worker'][2].pop('artifact_kind')
        self.refused(implicit, 'only an explicit CUDA artifact_kind')
        def cpu_host(d):
            d['hosts']['enhance-pir-gpu-01'].pop('accelerator')
        self.refused(cpu_host, 'only a CUDA host runs one')
        def unverified(d):
            d['services']['enhance']['roles']['worker'][2].pop('health_equals')
        self.refused(unverified, 'matvec.matvec_backend')

    def test_operations_and_controller_owned_fields(self):
        self.refused(lambda d: d['ancillary']['coordinator-edge'].update(operations=['inspect', 'restart']),
                     'unsupported operations', complete=False)
        self.refused(lambda d: d['ancillary']['coordinator-edge'].update(consumers=['nothing']), 'unknown consumer',
                     complete=False)
        def membership(d):
            d['services']['transparent-history']['roles']['worker'][0]['shards'] = [1, 2]
        self.refused(membership, 'belong to the product controller')
        self.assertEqual(self.fleet(membership).problems(), [])

    def test_a_product_inventory_stays_usable_for_its_own_operations(self):
        def enhance_only(d):
            d['services'] = {'enhance': d['services']['enhance']}
            d.pop('ancillary')
            d.pop('retired_hosts')
            for entry in d['services']['enhance']['roles']['worker']:
                entry.pop('artifact_kind', None)
        fleet = self.fleet(enhance_only)
        self.assertEqual(fleet.problems(), [])
        self.assertTrue(fleet.describe()['roles'])
        self.assertTrue(any('services missing' in p for p in fleet.problems(complete=True)))
        targets = descriptors.targets(SERVICES['enhance'], fleet.inventory)
        self.assertEqual(len(targets), 6)


if __name__ == '__main__':
    unittest.main()
