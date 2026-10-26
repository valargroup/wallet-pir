"""A combined node commits only after independent checks pass for every product."""
import copy
import json
import unittest

from test_wallet_pir_deploy import Fleet, ROOT, SERVICES, NEW_SHA, OLD_SHA, OLD
from wallet_pir_ops.deploy import descriptors
from wallet_pir_ops.deploy.engine import DeployError


PRODUCTS = ('enhance', 'status', 'transparent_history', 'transaction_details', 'receiver')


class QueryAcceptance(Fleet):
    def query_fleet(self):
        settings = json.loads((ROOT / 'tools/deploy/query-server/query-node.example.json').read_text())
        checks = {product: {'host': 'coordinator' if product != 'transaction_details' else 'receiver-01',
                            'argv': ['/usr/local/bin/check-' + product, '{transaction}'], 'timeout': 120}
                  for product in PRODUCTS}
        document = json.loads(self.inventory_path.read_text())
        document['services']['query-server'] = {
            'roles': {'combined': [{'host': 'worker-01', 'settings': settings,
                                    'vars': {'management_listen': settings['management_listen']}}]},
            'exact_check': {'products': checks}}
        self.inventory_path.write_text(json.dumps(document))
        self.inventory = descriptors.load_inventory(self.inventory_path)
        service = SERVICES['query-server']
        for target in descriptors.targets(service, self.inventory):
            release = service.release_dir(OLD_SHA)
            rendered = target.role.render(target.settings, release)
            self.fake.install(target.host, target.unit, rendered.unit, (),
                              {release + '/pir-query-server': OLD, **rendered.files})
        self.fake.product_check = lambda host, argv: (0, 'configuration accepted')
        deployer = self.deployer('query-server')
        deployer.capture_baseline()
        self.fake.log.clear()
        return deployer

    def test_all_products_run_on_their_independent_hosts_before_commit(self):
        deployer = self.query_fleet()
        journal = deployer.deploy(NEW_SHA, self.binary)
        self.assertEqual(journal.status, 'committed')
        calls = [(host, argv) for host, argv in self.fake.runs if argv[0].startswith('/usr/local/bin/check-')]
        self.assertEqual(calls, [('coordinator' if product != 'transaction_details' else 'receiver-01',
                                 ['/usr/local/bin/check-' + product, journal.id]) for product in PRODUCTS])

    def assert_product_failure_rolls_back(self, product):
        deployer = self.query_fleet()
        run = self.fake.run
        def reject_product(host, argv, timeout):
            if argv[0] == '/usr/local/bin/check-' + product:
                return 1, 'decrypted answer differs from the independent fixture'
            return run(host, argv, timeout)
        self.fake.run = reject_product
        with self.assertRaisesRegex(DeployError, 'exact-answer check failed'):
            deployer.deploy(NEW_SHA, self.binary)
        self.assertEqual(self.fake.probe_unit('worker-01', 'wallet-pir-query.service')['exe_sha256'], OLD_SHA)

    def test_enhance_failure_rolls_the_node_back(self):
        self.assert_product_failure_rolls_back('enhance')

    def test_status_failure_rolls_the_node_back(self):
        self.assert_product_failure_rolls_back('status')

    def test_history_failure_rolls_the_node_back(self):
        self.assert_product_failure_rolls_back('transparent_history')

    def test_details_failure_rolls_the_node_back(self):
        self.assert_product_failure_rolls_back('transaction_details')

    def test_receiver_failure_rolls_the_node_back(self):
        self.assert_product_failure_rolls_back('receiver')

    def test_refused_acceptance_host_prevents_all_deployment_mutations(self):
        deployer = self.query_fleet()
        self.fake.refuse_identity.add('receiver-01')
        with self.assertRaisesRegex(DeployError, 'not accepted by receiver-01; nothing was changed'):
            deployer.deploy(NEW_SHA, self.binary)
        self.assertEqual(self.fake.log, [])

    def test_missing_unknown_or_malformed_product_checks_are_refused(self):
        self.query_fleet()
        original = self.inventory.services['query-server']['exact_check']
        invalid = [{'host': 'coordinator', 'argv': ['check'], 'timeout': 10}]
        # None is permitted by parsing, but deploy itself refuses the missing gate.
        with self.assertRaisesRegex(DeployError, 'skip-exact-check'):
            self.inventory.services['query-server']['exact_check'] = None
            self.deployer('query-server').deploy(NEW_SHA, self.binary)
        for product in PRODUCTS:
            missing = copy.deepcopy(original)
            del missing['products'][product]
            invalid.append(missing)
        extra = copy.deepcopy(original)
        extra['products']['unknown-product'] = copy.deepcopy(extra['products']['enhance'])
        invalid.append(extra)
        for field, value in [('host', 'unknown'), ('host', []), ('argv', []), ('argv', ['']),
                             ('timeout', True), ('timeout', 0), ('timeout', 901)]:
            bad = copy.deepcopy(original)
            bad['products']['enhance'][field] = value
            invalid.append(bad)
        for check in invalid:
            with self.subTest(check=check):
                self.inventory.services['query-server']['exact_check'] = check
                with self.assertRaises(descriptors.DescriptorError):
                    descriptors.exact_check(SERVICES['query-server'], self.inventory)

    def test_publisher_checks_also_verify_all_acceptance_hosts(self):
        check = {'products': {'status': {'host': 'coordinator'},
                              'enhance': {'host': 'coordinator'},
                              'transparent_history': {'host': 'receiver-01'}}}
        self.assertEqual(descriptors.exact_check_hosts(check), ['coordinator', 'receiver-01'])
        self.assertEqual(descriptors.exact_check_hosts(None), [])


if __name__ == '__main__':
    unittest.main()
