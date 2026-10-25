"""Receiver routing changes preserve wallet URLs and recover an interrupted cutover."""
import json
from pathlib import Path
import tempfile
import unittest

from tools.receiver.lib.routing import CONFIG, Routing, candidate, digest
from tools.tests.operations.test_status_routing import Host, LostRunner
from wallet_pir_ops.deploy import descriptors
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.transaction import Journal

PUBLISHER = '''receiver-pir.valargroup.dev {
 @wallet path /receiver/v1/inventory /v1/receiver/inventory /v1/receiver/query
 handle @wallet {
  reverse_proxy 10.70.0.11:18380
 }
 handle {
  respond 404
 }
}
'''
QUERY = '''wallet-pir-query-01.valargroup.dev {
 @wallet path /enhance/v1/* /status/v1/* /transparent/v1/* /v2/*
 handle @wallet {
  reverse_proxy 127.0.0.1:8090
 }
 handle /receiver/v1/* {
  respond "Receiver serving integration is not installed" 503
 }
 handle {
  respond 404
 }
}
'''
REQUEST = dict(schema='receiver-routing/1', phase='distribution', host='coordinator', machine_id='1' * 32,
               query_host='query', query_origin='https://wallet-pir-query-01.valargroup.dev',
               query_sha256='2' * 64, publisher_listen='10.70.0.11:18380', receiver_address='161.35.182.172')


class ReceiverRouting(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        path = Path(self.temp.name) / 'inventory.json'
        path.write_text(json.dumps(dict(hosts={'coordinator': {}, 'query': {}}, ssh={'mode': 'config'},
            lock={'type': 'remote', 'host': 'coordinator'}, services={'receiver': {
                'roles': {'server': [{'host': 'coordinator', 'vars': {'management_listen': '127.0.0.1:18381'}}]},
                'exact_check': {'host': 'coordinator', 'argv': ['exact'], 'timeout': 10}}})))
        self.inventory = descriptors.load_inventory(path)
        self.host = Host()
        self.host.files[CONFIG] = PUBLISHER.encode()

    def routing(self, phase):
        return Routing(self.inventory, self.host, self.temp.name, dict(REQUEST, phase=phase), lock=self.host.lock)

    def deploy(self, routing):
        return routing.deploy(digest(json.dumps(routing.plan()[0], sort_keys=True)))

    def test_distribution_preparation_preserves_the_old_wallet_upstream_and_rolls_back(self):
        routing = self.routing('distribution')
        result = self.deploy(routing)
        after = self.host.read('coordinator', CONFIG)
        self.assertIn('/provider/receiver/v1/*', after)
        self.assertIn('handle @wallet {\n  reverse_proxy 10.70.0.11:18380', after)
        self.assertEqual(candidate(after, routing.request), after)
        routing.rollback(result['transaction'])
        self.assertEqual(self.host.read('coordinator', CONFIG), PUBLISHER)

    def test_query_ingress_keeps_other_products_and_both_receiver_prefixes(self):
        request = dict(REQUEST, phase='query')
        after = candidate(QUERY, request)
        self.assertIn('/enhance/v1/* /status/v1/* /transparent/v1/* /v2/* /receiver/v1/* /v1/receiver/*', after)
        self.assertNotIn('integration is not installed', after)
        self.assertIn('trusted_proxies 161.35.182.172/32', after)
        self.assertEqual(candidate(after, request), after)
        with self.assertRaises(DeployError):
            candidate(QUERY.replace('respond "Receiver serving integration is not installed" 503', 'reverse_proxy other:80'), request)

    def test_wallet_switch_runs_the_exact_probe_and_failure_restores_the_old_upstream(self):
        self.host.files[CONFIG] = candidate(PUBLISHER, REQUEST).encode()
        before = self.host.files[CONFIG]
        routing = self.routing('wallet')
        self.host.failure = 'exact'
        with self.assertRaises(DeployError):
            self.deploy(routing)
        self.assertEqual(self.host.files[CONFIG], before)
        result = self.deploy(routing)
        self.assertEqual(result['phase'], 'committed')
        self.assertIn('reverse_proxy https://wallet-pir-query-01.valargroup.dev', self.host.read('coordinator', CONFIG))
        self.host.failure = 'exact'
        with self.assertRaises(DeployError):
            self.deploy(routing)

    def test_changed_plan_foreign_edits_and_interruption_do_not_lose_the_predecessor(self):
        routing = self.routing('distribution')
        plan = digest(json.dumps(routing.plan()[0], sort_keys=True))
        self.host.files[CONFIG] += b'# another operator\n'
        with self.assertRaisesRegex(DeployError, 'plan changed'):
            routing.deploy(plan)
        self.assertEqual(self.host.writes, [])
        self.host.files[CONFIG] = PUBLISHER.encode()
        self.host.failure = 'lost-runner'
        with self.assertRaises(LostRunner):
            self.deploy(routing)
        journal = Journal.load(self.temp.name, routing.service_name)
        self.assertEqual(journal.status, 'installing')
        routing.rollback(journal.id)
        self.assertEqual(self.host.read('coordinator', CONFIG), PUBLISHER)
        result = self.deploy(routing)
        self.host.files[CONFIG] += b'# another operator\n'
        with self.assertRaisesRegex(DeployError, 'outside this transaction'):
            routing.rollback(result['transaction'])
        self.assertTrue(self.host.files[CONFIG].endswith(b'# another operator\n'))


if __name__ == '__main__':
    unittest.main()
