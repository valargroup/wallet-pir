"""Chain reader graph rules: active edges only, product isolation, every consumer."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('chain_graph', ROOT / 'tools/ci/chain_graph.py')
graph = importlib.util.module_from_spec(spec)
spec.loader.exec_module(graph)


class RpcDependencyTests(unittest.TestCase):
    def graph(self, active=False, kind=None, name='zakura-chain'):
        return {
            'packages': [{'id': 'consumer', 'name': 'consumer'}, {'id': 'node', 'name': name}],
            'resolve': {'root': 'consumer', 'nodes': [
                {'id': 'consumer', 'deps': [{'pkg': 'node', 'dep_kinds': [{'kind': kind}]}] if active else []},
                {'id': 'node', 'deps': []},
            ]},
        }

    def test_inactive_node_dependency_is_allowed(self):
        self.assertEqual(graph.rpc_dependency_problems(self.graph()), [])

    def test_runtime_or_build_node_or_database_dependency_is_rejected(self):
        for name in ('zakura-chain', 'zakura-state', 'librocksdb-sys'):
            for kind in (None, 'build'):
                with self.subTest(name=name, kind=kind):
                    self.assertTrue(graph.rpc_dependency_problems(self.graph(True, kind, name)))

    def test_fixture_dependency_does_not_enter_rpc_build(self):
        self.assertEqual(graph.rpc_dependency_problems(self.graph(True, 'dev')), [])

    def test_decoding_consumers_reject_active_transport_only(self):
        for name in ('tokio', 'reqwest', 'axum', 'hyper'):
            self.assertTrue(graph.transport_dependency_problems(self.graph(True, None, name), 'product'))
            self.assertEqual(graph.transport_dependency_problems(self.graph(False, None, name), 'product'), [])
            self.assertEqual(graph.transport_dependency_problems(self.graph(True, 'dev', name), 'product'), [])


class ProductDependencyTests(unittest.TestCase):
    def graph(self, features, receiver=False):
        packages = [{'id': 'adapter', 'name': 'zakura-server-chain'}]
        if receiver:
            packages.append({'id': 'receiver', 'name': 'zakura-receiver-crypto'})
        return {'packages': packages, 'resolve': {'nodes': [{'id': 'adapter', 'features': features}]}}

    def test_each_product_is_isolated_and_receiver_recovery_is_optional(self):
        for product in graph.PRODUCTS:
            self.assertEqual(graph.product_dependency_problems(self.graph([product], product == 'receiver'), product), [])
        self.assertTrue(graph.product_dependency_problems(self.graph(['transparent'], True), 'transparent'))

    def test_testing_cannot_activate_another_product(self):
        self.assertEqual(graph.product_dependency_problems(self.graph(['testing']), None), [])
        self.assertTrue(graph.product_dependency_problems(self.graph(['testing', 'receiver']), None))
        self.assertTrue(graph.product_dependency_problems(self.graph(['transparent', 'receiver']), 'transparent'))

    def test_exactly_one_reader_is_required(self):
        self.assertTrue(graph.product_dependency_problems({'packages': [], 'resolve': {'nodes': []}}, None))


class LocalDependencyTests(unittest.TestCase):
    def test_reader_reaches_no_other_repository_package(self):
        packages = [{'name': 'kind-consumer', 'source': None}, {'name': 'zakura-server-chain', 'source': None},
                    {'name': 'serde', 'source': 'registry+https://github.com/rust-lang/crates.io-index'}]
        self.assertEqual(graph.local_dependency_problems({'packages': packages}), [])
        packages.append({'name': 'transparent-shared', 'source': None})
        self.assertTrue(graph.local_dependency_problems({'packages': packages}))


class ConsumerTests(unittest.TestCase):
    def test_every_consumer_and_product_selection_is_exercised(self):
        consumers = {kind: options for kind, options, _ in graph.consumers()}
        self.assertNotIn('testing', consumers['server-production'])
        self.assertIn('features = ["testing"]', consumers['server-fixtures'])
        self.assertIn('"receiver"', consumers['server'])
        for product in graph.PRODUCTS:
            self.assertIn(f'features = ["{product}"]', consumers['product-' + product])
            self.assertIn(f'features = ["{product}", "testing"]', consumers['product-' + product + '-testing'])
        # The opt-in RocksDB reader is resolved and type-checked on its own.
        self.assertIn('features = ["rocksdb"]', consumers['server-rocksdb'])
        self.assertNotIn('rocksdb', consumers['server-production'] + consumers['server'])

    def test_rules_follow_each_consumer(self):
        node = {'packages': [{'id': 'consumer', 'name': 'consumer', 'source': None},
                             {'id': 'adapter', 'name': 'zakura-server-chain', 'source': None},
                             {'id': 'node', 'name': 'zakura-chain', 'source': 'git+x'},
                             {'id': 'tokio', 'name': 'tokio', 'source': 'registry+x'}],
                'resolve': {'root': 'consumer', 'nodes': [
                    {'id': 'consumer', 'deps': [{'pkg': 'adapter', 'dep_kinds': [{'kind': None}]}]},
                    {'id': 'adapter', 'features': [], 'deps': [{'pkg': p, 'dep_kinds': [{'kind': None}]}
                                                                for p in ('node', 'tokio')]},
                    {'id': 'node', 'deps': []}, {'id': 'tokio', 'deps': []}]}}
        problems = lambda kind: [e for rule in graph.checks(kind) for e in rule(node)]
        self.assertTrue(any('node/database' in e for e in problems('server-production')))
        self.assertTrue(any('runtime/HTTP' in e for e in problems('server-fixtures')))
        self.assertTrue(any('runtime/HTTP' in e for e in problems('product-status-testing')))
        self.assertFalse(any('node/database' in e for e in problems('product-status-testing')))
        self.assertEqual(problems('server-rocksdb'), [])

    def test_consumer_type_checks_keep_flags_and_cap_lints(self):
        with patch.dict(graph.os.environ, {'RUSTFLAGS': '-D warnings'}):
            env = graph.consumer_environment(Path('/t'))
        self.assertEqual(env['RUSTFLAGS'], '-D warnings --cap-lints=warn')
        self.assertEqual(env['CARGO_TARGET_DIR'], '/t')

    def test_main_reports_violations_without_compiling(self):
        def metadata(manifest, offline):
            return {'packages': [{'id': 'c', 'name': manifest.parent.name + '-consumer', 'source': None},
                                 {'id': 'x', 'name': 'pir-runtime', 'source': None}],
                    'resolve': {'root': 'c', 'nodes': [{'id': 'c', 'deps': []}, {'id': 'x', 'deps': []}]}}
        with patch.object(graph, 'metadata', side_effect=metadata), \
                patch.object(graph.subprocess, 'run') as run, \
                self.assertRaisesRegex(SystemExit, 'repository package pir-runtime'):
            graph.main([])
        run.assert_not_called()


class FullSelectionTests(unittest.TestCase):
    def test_shared_runs_three_reader_selections_with_rocksdb_limited_to_its_targets(self):
        import sys
        sys.path.insert(0, str(ROOT / 'tools/ci'))
        import full_packages
        groups = {'shared': ['pir-control', graph.READER], 'enhance': [], 'transparent': [], 'receiver': []}
        commands = [c for c in full_packages.commands('shared', groups) if graph.READER in c]
        self.assertEqual(len(commands), 3)
        decoding, rpc, rocksdb = (c[c.index('--features') + 1] for c in commands)
        self.assertNotIn('rpc', decoding)
        self.assertNotIn('testing-node', decoding)
        for product in graph.PRODUCTS:
            self.assertIn(f'{graph.READER}/{product}', rpc)
        self.assertIn(f'{graph.READER}/rocksdb', rocksdb)
        self.assertEqual(commands[2][commands[2].index('--lib'):commands[2].index('--features')],
                         ['--lib', '--test', 'state'])


if __name__ == '__main__':
    unittest.main()
