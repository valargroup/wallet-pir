"""Fleet release planning over distributed, unified and mixed topologies."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops.deploy import descriptors, fleet_release  # noqa: E402

REGISTRY = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')
ORIGIN = 'https://publisher.example:443'
PUBLISHER = 'pub-01/pir-publisher.service'
UNIFIED = 'query-01/wallet-pir-query.service'
STATUS = 'status-01/status-worker.service'
GPU = 'gpu-01/enhance-pir-worker.service'
ROUTER = 'router-01/enhance-pir-packing-router.service'
QUERY_PRODUCTS = ['enhance', 'status', 'transparent_history', 'transaction_details']


def sync():
    return {'synchronization': {'publisher_origin': 'https://publisher.example'}}


def inventory_document(unified=True, distributed=True):
    hosts = {name: {'address': '10.0.0.%d' % number} for number, name in
             enumerate(['pub-01', 'query-01', 'status-01', 'gpu-01', 'router-01'], 1)}
    services = {'publisher': {'managed_products': QUERY_PRODUCTS, 'exact_check': {'host': 'pub-01', 'argv': ['x']},
                              'roles': {'server': [{'host': 'pub-01',
                                                    'vars': {'management_listen': '127.0.0.1:18080'}}]}}}
    if unified:
        services['query-server'] = {'roles': {'combined': [{'host': 'query-01', 'vars': {'management_listen': '127.0.0.1:18090'},
                                                            'settings': {'listen': '127.0.0.1:8443',
                                                                         'enhance': sync(),
                                                                         'status': {'publisher': ORIGIN},
                                                                         'transparent_history': {'publisher': ORIGIN},
                                                                         'transaction_details': {'publisher': ORIGIN}}}]}}
    if distributed:
        services['status'] = {'roles': {'worker': [{'host': 'status-01', 'settings': {'publisher': ORIGIN}}]}}
        services['enhance'] = {'roles': {
            'worker': [{'host': 'gpu-01', 'artifact_kind': 'enhance-pir-native-cuda', 'vars': {'listen': '127.0.0.1:1'},
                        'settings': sync()}],
            'packing-router': [{'host': 'router-01', 'vars': {'control_listen': '127.0.0.1:2'}, 'settings': {}}]}}
    return {'hosts': hosts, 'ssh': {'mode': 'config'}, 'lock': {'type': 'remote', 'host': 'pub-01'},
            'services': services}


def load(document):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'inventory.json'
        path.write_text(json.dumps(document))
        return descriptors.load_inventory(path)


def report(executable, major, products, backends=()):
    return {'schema': 'wallet-pir-contract-info/1', 'executable': executable,
            'contract_version': '%d.0.0' % major, 'contract_major': major,
            'products': list(products), 'backends': list(backends)}


def executables(major, query_products=QUERY_PRODUCTS):
    return {'pir-publisher': {'sha256': 'a' * 64, 'contract': report('pir-publisher', major, QUERY_PRODUCTS)},
            'pir-query-server': {'sha256': 'b' * 64, 'contract': report('pir-query-server', major, query_products)},
            'enhance-pir-native-cuda': {'sha256': 'c' * 64,
                                        'contract': report('pir-query-server', major, ['enhance'], ['cuda'])}}


def request(select, major=1, **overrides):
    document = {'schema': fleet_release.SCHEMA, 'source': {'revision': 'f' * 40, 'submitted_unix': 1000},
                'inventory_sha256': 'e' * 64, 'executables': executables(major), 'select': select,
                'publishers': {ORIGIN: PUBLISHER},
                'probes': {product: {'host': 'pub-01', 'argv': ['probe', product]}
                           for product in [*QUERY_PRODUCTS, 'receiver']}}
    document.update(overrides)
    return document


def observations(keys, publisher_major=1, contract_major=1):
    return {'processes': {key: {'exe_sha256': '0' * 64, 'contract_major': contract_major, 'previous_staged': True}
                          for key in keys},
            'publishers': {ORIGIN: {'major': publisher_major}}}


ALL = [PUBLISHER, UNIFIED, STATUS, GPU, ROUTER]


class FleetReleasePlanTests(unittest.TestCase):
    def plan(self, select, document=None, observed=None, **overrides):
        inventory = load(document or inventory_document())
        return fleet_release.plan(REGISTRY, inventory, 'e' * 64, request(select, **overrides),
                                  observations(ALL) if observed is None else observed)

    def test_unified_node_is_one_process_with_its_enabled_products(self):
        planned = self.plan([UNIFIED])
        self.assertEqual(planned['problems'], [])
        self.assertEqual(planned['kind'], 'compatible')
        unified = next(p for p in planned['processes'] if p['key'] == UNIFIED)
        self.assertEqual(unified['products'], sorted(QUERY_PRODUCTS))
        self.assertEqual(unified['reads'], [ORIGIN])
        self.assertEqual(planned['waves']['start'], [[UNIFIED]])
        self.assertEqual(sorted(planned['probes']), sorted(QUERY_PRODUCTS))
        self.assertEqual([p['key'] for p in planned['processes'] if p['action'] == 'change'], [UNIFIED])

    def test_compatible_publisher_only_release_keeps_every_consumer(self):
        planned = self.plan([PUBLISHER])
        self.assertEqual(planned['problems'], [])
        self.assertEqual(planned['waves'], {'stop': [[PUBLISHER]], 'start': [[PUBLISHER]]})

    def test_breaking_release_refuses_each_omitted_consumer(self):
        for topology, consumers in (('unified', [UNIFIED]), ('distributed', [STATUS, GPU]),
                                    ('mixed', [UNIFIED, STATUS, GPU])):
            with self.subTest(topology=topology):
                document = inventory_document(unified=topology != 'distributed',
                                              distributed=topology != 'unified')
                present = [key for key in ALL if key in {PUBLISHER, *consumers} or key == ROUTER and topology != 'unified']
                planned = self.plan([PUBLISHER], document, observations(present), major=2)
                self.assertEqual(planned['kind'], 'breaking')
                for consumer in consumers:
                    self.assertTrue(any(consumer in problem and 'not part of the release' in problem
                                        for problem in planned['problems']), (consumer, planned['problems']))
                complete = self.plan([PUBLISHER, *consumers], document, observations(present), major=2)
                self.assertEqual(complete['problems'], [])
                self.assertEqual(complete['waves']['stop'][-1], [PUBLISHER])
                self.assertEqual(complete['waves']['start'][0], [PUBLISHER])

    def test_breaking_waves_start_routers_after_workers(self):
        planned = self.plan(ALL, major=2)
        self.assertEqual(planned['problems'], [])
        self.assertEqual(planned['waves']['stop'], [[GPU, UNIFIED, ROUTER, STATUS], [PUBLISHER]])
        self.assertEqual(planned['waves']['start'], [[PUBLISHER], [GPU, UNIFIED, STATUS], [ROUTER]])

    def test_consumer_candidate_must_match_unchanged_publisher(self):
        planned = self.plan([STATUS], major=2)
        self.assertTrue(any('would not match publisher' in problem for problem in planned['problems']))

    def test_unified_candidate_missing_an_enabled_product_refuses(self):
        built = executables(1, ['enhance', 'status'])
        planned = self.plan([UNIFIED], executables=built)
        self.assertTrue(any('does not compile enabled products' in problem and 'transparent_history' in problem
                            for problem in planned['problems']))

    def test_cuda_worker_needs_a_cuda_candidate(self):
        built = executables(1)
        built['enhance-pir-native-cuda']['contract']['backends'] = []
        planned = self.plan([GPU], executables=built)
        self.assertTrue(any('no CUDA backend' in problem for problem in planned['problems']))

    def test_invalid_or_disagreeing_metadata_refuses(self):
        built = executables(1)
        built['pir-query-server']['contract']['contract_major'] = 2
        self.assertTrue(any('disagrees' in problem for problem in self.plan([UNIFIED], executables=built)['problems']))
        built = executables(1)
        built['pir-query-server']['contract']['executable'] = 'pir-publisher'
        self.assertTrue(any('names executable' in problem
                            for problem in self.plan([UNIFIED], executables=built)['problems']))

    def test_unknown_publisher_major_refuses_publisher_change(self):
        observed = observations(ALL, publisher_major=None)
        planned = self.plan([PUBLISHER], observed=observed)
        self.assertTrue(any('current contract major is unknown' in problem for problem in planned['problems']))

    def test_duplicate_declaration_of_one_process_refuses(self):
        document = inventory_document()
        document['hosts']['query-alias'] = {'address': '10.0.0.2'}
        document['services']['query-server']['roles']['combined'].append(
            {**copy.deepcopy(document['services']['query-server']['roles']['combined'][0]), 'host': 'query-alias'})
        planned = self.plan([PUBLISHER], document)
        self.assertTrue(any('declared 2 times' in problem for problem in planned['problems']), planned['problems'])

    def test_unreviewed_publisher_origin_refuses(self):
        document = inventory_document()
        document['services']['status']['roles']['worker'][0]['settings']['publisher'] = 'https://other.example'
        planned = self.plan([STATUS], document)
        self.assertTrue(any('no reviewed publisher process serves' in problem for problem in planned['problems']))

    def test_missing_recovery_artifact_or_probe_refuses(self):
        observed = observations(ALL)
        observed['processes'][UNIFIED]['previous_staged'] = False
        self.assertTrue(any('not staged for recovery' in problem
                            for problem in self.plan([UNIFIED], observed=observed)['problems']))
        planned = self.plan([UNIFIED], probes={'enhance': {'host': 'pub-01', 'argv': ['x']}})
        self.assertTrue(any('no independent encrypted-answer probe for status' in problem
                            for problem in planned['problems']))

    def test_stale_inventory_and_plan_digest_refuse(self):
        inventory = load(inventory_document())
        planned = fleet_release.plan(REGISTRY, inventory, 'd' * 64, request([UNIFIED]), observations(ALL))
        self.assertTrue(any('inventory changed since review' in problem for problem in planned['problems']))
        reviewed = self.plan([UNIFIED])
        self.assertIsNone(fleet_release.stale(reviewed['plan_sha256'], self.plan([UNIFIED])))
        drifted = observations(ALL)
        drifted['processes'][UNIFIED]['exe_sha256'] = '9' * 64
        self.assertIn('stale', fleet_release.stale(reviewed['plan_sha256'], self.plan([UNIFIED], observed=drifted)))

    def test_request_shape_is_checked_first(self):
        self.assertTrue(fleet_release.plan(REGISTRY, load(inventory_document()), 'e' * 64,
                                           {'schema': 'other'}, {})['problems'])
        bad = request([UNIFIED], publishers={'https://publisher.example': PUBLISHER})
        self.assertTrue(fleet_release.validate_request(bad))


if __name__ == '__main__':
    unittest.main()
