"""The publisher's private control change must deploy and restore as a group."""
import contextlib
import io
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import publisher, publisher_serving
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class ServingTests(PublisherFixture, unittest.TestCase):
    def test_json_response_is_not_mixed_with_progress(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            print(json.dumps(self.deploy()))
        self.assertEqual(json.loads(output.getvalue())['phase'], 'committed')

    def test_deploy_and_restore_keep_matching_control_versions(self):
        result = self.deploy()
        self.assertEqual(result['phase'], 'committed')
        edge = self.ex.read('coordinator', publisher_serving.CADDY)
        self.assertIn('handle /v1/enhance/*', edge)
        self.assertEqual(edge.count(publisher_serving.DISCOVERY), 1)
        self.assertIn('respond 404', edge)
        self.assertNotIn('127.0.0.1:8080', edge)
        self.assertTrue(all(r['phase'] == 'verified' for r in result['serving_journal']['hosts']))
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.read('coordinator', publisher_serving.CADDY), result['plan']['serving']['edge_before'])
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_subsequent_update_keeps_the_installed_wallet_routes(self):
        self.deploy()
        edge = self.ex.read('coordinator', publisher_serving.CADDY)
        self.request['serving']['caddy_sha256'] = publisher.sha(edge)
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.assertEqual(self.deploy()['phase'], 'committed')
        self.assertEqual(self.ex.read('coordinator', publisher_serving.CADDY), edge)

    def test_foreign_discovery_route_is_refused(self):
        edge = self.ex.read('coordinator', publisher_serving.CADDY)
        foreign = publisher_serving.DISCOVERY.replace('127.0.0.1:18080', '127.0.0.1:8080')
        self.ex.put('coordinator', publisher_serving.CADDY,
                    edge.replace(publisher_serving.WALLET, publisher_serving.WALLET + foreign))
        self.request['serving']['caddy_sha256'] = publisher.sha(
            self.ex.read('coordinator', publisher_serving.CADDY))
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.effects.clear()
        with self.assertRaisesRegex(DeployError, 'unknown publisher discovery route'):
            self.target.plan()
        self.assertEqual(self.ex.effects, [])

    def test_retained_cutover_journal_restores_its_saved_wallet_routes(self):
        result = self.deploy()
        # A retained transaction can predate today's route shape. Recovery reads
        # its saved bytes instead of regenerating an obsolete migration plan.
        old_edge = 'example.test {\n' + publisher_serving.FALLBACK + '\n}\n'
        result['plan']['serving']['edge_before'] = old_edge
        path = publisher.ROOT_DIR + '/transactions/' + result['id'] + '/journal.json'
        self.ex.put('coordinator', path, json.dumps(result))
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.read('coordinator', publisher_serving.CADDY), old_edge)

    def test_lost_role_restart_reply_restores_original_group(self):
        self.ex.crash_on = ('restart', 'enhance-pir-query-ingress.service')
        with self.assertRaises(RemoteError):
            self.deploy()
        # Recover using only the durable coordinator journal, with no runner journal.
        for path in self.root.glob('publisher-cutover/*.json'):
            path.unlink()
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

    def test_foreign_route_change_blocks_restore_before_any_stop(self):
        self.deploy()
        self.ex.put('coordinator', publisher_serving.CADDY, 'foreign route')
        self.ex.effects.clear()
        with self.assertRaisesRegex(DeployError, 'outside the transaction'):
            self.target.rollback()
        self.assertEqual(self.ex.effects, [])

    def test_role_drift_invalidates_plan_before_stopping(self):
        plan = self.target.plan()
        self.ex.install('router-01', 'enhance-pir-packing-router.service', '[Service]\nExecStart=/bin/old-serving changed\n')
        with self.assertRaisesRegex(DeployError, 'stale'):
            self.target.deploy(self.binary, plan['plan_sha256'], self.serving_binary)
        self.assertEqual(self.ex.effects, [])

    def test_product_policy_changes_are_still_refused(self):
        self.request['configuration']['enhance']['settings']['sealed_shards'] += 1
        target = self.make_target()
        with self.assertRaisesRegex(DeployError, 'preserve every product'):
            target.plan()

    def test_staged_role_check_failure_never_stops_running_group(self):
        original = self.ex.run
        def refuse(host, argv, timeout):
            if '--check' in argv and argv[0].endswith('/pir-query-server'):
                return 1, 'invalid role configuration'
            return original(host, argv, timeout)
        self.ex.run = refuse
        with self.assertRaisesRegex(DeployError, 'product check refused'):
            self.deploy()
        self.assertFalse(any(effect[0] in ('stop', 'restart') for effect in self.ex.effects))
        self.ex.run = original
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')


if __name__ == '__main__':
    unittest.main()
