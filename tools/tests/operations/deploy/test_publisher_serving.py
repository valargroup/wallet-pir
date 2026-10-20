"""The publisher's private control change must deploy and restore as a group."""
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from test_publisher_expansion import ExpansionFixture, ExpandedCoordinator
from fake_fleet import FakeFleet
from wallet_pir_ops.deploy import publisher, publisher_products, publisher_serving
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class Coordinator(ExpandedCoordinator):
    def run(self, host, argv, timeout):
        if argv[:2] == ['stat', '-c']:
            return 0, '644:0:0\n'
        return super().run(host, argv, timeout)

    def systemctl(self, host, *args):
        if args[0] == 'reload' and args[1] == 'caddy.service':
            self.effects.append(args)
            return
        super().systemctl(host, *args)

    def http_get(self, host, url, timeout=5):
        if (host, url) in self.endpoints:
            status, body = FakeFleet.http_get(self, host, url, timeout)
            value = json.loads(body)
            # The private control versions deliberately cannot interoperate.
            executable = self.host(host).units[self.endpoints[(host, url)]]['exe']
            new_publisher = self.probe_unit('coordinator', publisher.UNIT)['exe_sha256'] == self.new_publisher
            value['ready'] = value['ready'] and ((executable == self.new_serving) == new_publisher)
            return status, json.dumps(value)
        return super().http_get(host, url, timeout)


class ServingTests(ExpansionFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.baseline.deploy()
        self.target.deploy(self.binary, self.target.plan()['plan_sha256'])
        self.ex.__class__ = Coordinator
        self.old_publisher = publisher.file_sha256(self.binary)
        self.binary.write_bytes(b'publication-only publisher')
        self.request['release'].update(binary_sha256=publisher.file_sha256(self.binary), binary_bytes=self.binary.stat().st_size)
        self.serving_binary = self.root / 'pir-query-server'
        self.serving_binary.write_bytes(b'query-only serving')
        self.request['schema'] = publisher.SERVING_SCHEMA
        self.request['configuration'] = publisher_products.publication_only(self.request['configuration'])
        edge = 'example.test {\n' + publisher_serving.FALLBACK + '\n}\n'
        self.ex.put('coordinator', publisher_serving.CADDY, edge)
        self.ex.install('coordinator', 'caddy.service', '[Service]\nExecStart=/bin/caddy run --config /etc/caddy/Caddyfile\n', files={'/bin/caddy': b'caddy'})
        self.request['serving'] = {'git_sha': self.request['release']['git_sha'],
                                  'binary_sha256': publisher.file_sha256(self.serving_binary),
                                  'binary_bytes': self.serving_binary.stat().st_size, 'caddy_sha256': publisher.sha(edge)}
        self.ex.new_publisher = self.request['release']['binary_sha256']
        self.ex.new_serving = self.request['serving']['binary_sha256']
        for role in publisher_serving.ROLES:
            entry = self.request['inventory']['services']['enhance']['roles'][role][0]
            host = entry['host']
            unit = 'enhance-pir-' + role + '.service'
            self.ex.install(host, unit, '[Service]\nExecStart=/bin/old-serving ' + role + '\n', files={'/bin/old-serving': b'old serving'})
            for path in entry['settings']['credentials']:
                self.ex.put(host, path, '')
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        for target in self.target.serving.deployer.targets:
            self.ex.endpoints[(target.host, target.url(target.role.health))] = target.unit
        self.ex.effects.clear()

    def deploy(self):
        return self.target.deploy(self.binary, self.target.plan()['plan_sha256'], self.serving_binary)

    def test_deploy_and_restore_keep_matching_control_versions(self):
        result = self.deploy()
        self.assertEqual(result['phase'], 'committed')
        self.assertIn('handle /v1/enhance/*', self.ex.read('coordinator', publisher_serving.CADDY))
        self.assertTrue(all(r['phase'] == 'verified' for r in result['serving_journal']['hosts']))
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertNotIn('handle /v1/enhance/*', self.ex.read('coordinator', publisher_serving.CADDY))
        self.assertEqual(self.ex.probe_unit('coordinator', publisher.UNIT)['exe_sha256'], self.old_publisher)

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
