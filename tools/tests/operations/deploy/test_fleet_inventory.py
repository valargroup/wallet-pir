"""Complete-fleet validation of the operator inventory, and that product inventories keep working."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops import pinned_ssh  # noqa: E402
from wallet_pir_ops.deploy import descriptors  # noqa: E402
from wallet_pir_ops.deploy.remote import SSHExecutor  # noqa: E402
from wallet_pir_ops.fleet import Fleet  # noqa: E402

EXAMPLE = json.loads((ROOT / 'tools/enhance/deploy/deploy-inventory.example.json').read_text())
SERVICES = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')


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
        managed = {descriptors.PUBLISHER_PRODUCTS[p] for p in EXAMPLE['services']['publisher']['managed_products']}
        for name, service in SERVICES.items():
            # Optional external workers are additions a complete fleet need not have.
            self.assertEqual({r['role'] for r in view['roles'] if r['service'] == name},
                             {r for r in service.order if not service.roles[r].optional and (name, r) not in managed})
        writers = [(r['service'], r['role']) for r in view['roles'] if 'journal-writer' in r['responsibilities']]
        self.assertEqual(writers, [])
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
        # Two history workers on one host keep distinct keys.
        keys = [t.key for t in descriptors.targets(SERVICES['transparent-history'], self.fleet().inventory)]
        self.assertIn('worker@transparent-v12-01:transparent-v12-archive.service', keys)
        self.assertEqual(len(keys), len(set(keys)))

    def test_unified_process_has_its_own_deploy_descriptor(self):
        services = descriptors.load_descriptors(ROOT / 'tools/deploy/publisher/services.toml')
        service = services['publisher']
        self.assertEqual(service.binary, 'pir-publisher')
        self.assertEqual(service.order, ('server',))
        self.assertTrue(service.roles['server'].owns_unit)

    def test_unified_publisher_refuses_standalone_owners_and_missing_checks(self):
        document = copy.deepcopy(EXAMPLE)
        descriptors.publisher_ownership(document['services'])
        document['services']['publisher'].pop('exact_check')
        with self.assertRaisesRegex(descriptors.DescriptorError, 'independent exact_check'):
            descriptors.publisher_ownership(document['services'])
        for service, role in [('enhance', 'coordinator'), ('status', 'controller')]:
            with self.subTest(service=service):
                self.assertNotIn(role, SERVICES[service].roles)
                def retired(d):
                    d['services'].pop('publisher')
                    d['services'][service]['roles'][role] = [{'host': 'coordinator'}]
                self.refused(retired, 'unknown '+service+' roles', complete=False)

    def test_each_managed_product_rejects_its_existing_standalone_owner(self):
        for product, service, role in (
            ('enhance', 'enhance', 'coordinator'),
            ('status', 'status', 'controller'),
            ('transparent_history', 'transparent-history', 'controller'),
            ('transaction_details', 'transparent-display', 'controller'),
            ('receiver', 'receiver', 'server'),
        ):
            with self.subTest(product=product):
                document = copy.deepcopy(EXAMPLE)
                document['services']['publisher'] = {
                    'managed_products': [product],
                    'roles': {'server': [{'host': 'coordinator'}]},
                    'exact_check': {'host': 'coordinator', 'argv': ['/opt/probes/all-products']},
                }
                document['services'][service]['roles'][role] = [{'host': 'coordinator'}]
                with self.assertRaisesRegex(descriptors.DescriptorError, 'both own publishing'):
                    descriptors.publisher_ownership(document['services'])
                document['services'][service]['roles'].pop(role)
                descriptors.publisher_ownership(document['services'])

    def test_required_roles_services_responsibilities_and_ancillary(self):
        self.refused(lambda d: d['services']['enhance']['roles'].pop('packing-router'), 'required roles missing')
        self.refused(lambda d: d['services'].pop('receiver'), 'services missing')
        def historical(d, service):
            d['services'].pop('publisher')
            d['services'][service]['roles']['controller'] = [
                {'host': 'coordinator', 'vars': {'listen': '127.0.0.1:8510'}}]
        def missing_writer(d):
            historical(d, 'transparent-history')
        self.refused(missing_writer, 'exactly one controller must hold journal-writer')
        def two_writers(d):
            historical(d, 'transparent-display')
            roles = d['services']['transparent-display']['roles']
            roles['controller'][0]['responsibilities'] = ['journal-writer']
            roles['controller'].append({'host': 'coordinator', 'unit': 'second-display-controller.service',
                                        'vars': {'listen': '127.0.0.1:8511'}, 'responsibilities': ['journal-writer']})
        self.refused(two_writers, 'exactly one controller must hold journal-writer')
        def writer_on_worker(d):
            d['services']['transparent-history']['roles']['worker'][0]['responsibilities'] = ['journal-writer']
        self.refused(writer_on_worker, 'responsibilities must be among []')
        self.refused(lambda d: d['ancillary'].pop('control-sessions'), 'status: no ancillary supervision')

    def test_ownership_is_unique_per_machine_and_unit(self):
        def alias(d):
            d['hosts']['coordinator-alias'] = {'address': d['hosts']['coordinator']['address']}
            d['services']['status']['roles']['router'][0]['host'] = 'coordinator-alias'
            d['services']['status']['roles']['router'][0]['unit'] = 'pir-publisher.service'
        self.refused(alias, 'both own pir-publisher.service', complete=False)
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
        self.assertEqual(len(targets), 5)


    def test_hosts_may_use_their_own_identity_and_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shared, product = root / 'known_hosts', root / 'product_known_hosts'
            shared.write_text('192.0.2.10 ssh-ed25519 AAAA\n')
            product.write_text('192.0.2.20 ssh-ed25519 BBBB\n')
            def change(d):
                d['ssh'].update(key=str(root / 'fallback'), known_hosts=str(shared),
                                known_hosts_sha256=pinned_ssh.sha256(shared.read_bytes()))
                d['hosts']['transparent-router-01'].update(key=str(root / 'product-key'), known_hosts=str(product),
                                                           known_hosts_sha256=pinned_ssh.sha256(product.read_bytes()))
            executor = SSHExecutor(self.fleet(change).inventory)
            router = ' '.join(executor.raw_transport('transparent-router-01'))
            coordinator = ' '.join(executor.raw_transport('coordinator'))
            # The executor resolves paths; on macOS the temporary root is a link (/var -> /private/var).
            resolved = root.resolve()
            self.assertIn('-i %s' % (resolved / 'product-key'), router)
            self.assertIn('UserKnownHostsFile=%s' % (resolved / product.name), router)
            self.assertIn('-i %s' % (resolved / 'fallback'), coordinator)
            self.assertIn('UserKnownHostsFile=%s' % (resolved / shared.name), coordinator)
            product.write_text('192.0.2.20 ssh-ed25519 CCCC\n')
            with self.assertRaisesRegex(ValueError, 'changed'):
                executor.raw_transport('transparent-router-01')
        with self.assertRaisesRegex(descriptors.DescriptorError, 'together with its known_hosts_sha256'):
            self.fleet(lambda d: d['hosts']['worker-01'].update(known_hosts='/etc/wallet-pir/known_hosts'))


if __name__ == '__main__':
    unittest.main()
