"""One cutover owns all serving roles and preserves the rollback handoff."""
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import enhance_https, publisher
from wallet_pir_ops.deploy.cutover_guard import Decision
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class EnhanceTests(PublisherFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.request['schema'] = enhance_https.SCHEMA
        origin = 'https://pir.example.test'
        inventory = self.request['inventory']
        roles = inventory['services']['enhance']['roles']
        roles['worker'] = [r for r in roles['worker'] if r['settings'].get('matvec_backend') != 'cuda']
        for entries in roles.values():
            for entry in entries:
                entry['settings']['synchronization']['publisher_origin'] = origin
        settings = {'artifact_listen': '127.0.0.1:8084', 'listen': '127.0.0.1:8080',
            'data_dir': '/srv/enhance', 'zakura_rpc_url': 'http://127.0.0.1:8232',
            'zakura_cookie': '/run/credentials/node-cookie', 'poll_seconds': 1, 'prevout_cache_outputs': 1000}
        installed = copy.deepcopy(self.request['configuration'])
        installed['enhance']['settings'].update(settings)
        installed['enhance']['settings'].pop('artifact_listen')
        self.ex.put('coordinator', publisher.CONFIG, json.dumps(installed))
        self.request['configuration']['enhance']['settings'] = settings
        progress = {name: {'host': 'coordinator', 'url': url, 'field': field} for name, url, field in [
            ('enhance', origin + '/v1/enhance/init', 'anchor_height'),
            ('status', 'https://status.example.test/v1/status/init', 'anchor_height'),
            ('transparent_history', 'http://10.0.0.1:8096/v1/status', 'public_height'),
            ('transaction_details', 'http://127.0.0.1:8098/v1/status', 'published.tip')]}
        self.progress_urls = {entry['url']: entry['field'] for entry in progress.values()}
        self.request['enhance_https'] = {'origin': origin, 'publisher_budget_bytes': 1 << 30,
                                        'cuda_release': None, 'progress': progress}
        self.target = self.make_target()
        self.target.serving.binary = self.serving_binary
        self.ex.clock_seconds = 1000
        guard = Decision(self.root, 'publisher-fixture', clock=self.ex.clock)
        guard.create('a' * 64)
        guard.started('fixture', 123)
        self.target.supervision = guard
        self.handoffs = []
        self.progress_count = {}
        original_run, original_http = self.ex.run, self.ex.http_get

        def run(host, argv, timeout):
            if 'handoff-https-state' in argv:
                rollback = '--rollback' in argv
                self.handoffs.append(rollback)
                return 0, json.dumps({'rollback': rollback, 'recovery_epoch': 4 if rollback else 3})
            return original_run(host, argv, timeout)

        def http(host, url, timeout=5):
            if url in self.progress_urls:
                count = self.progress_count.get(url, 0) + 1
                self.progress_count[url] = count
                body = count
                for field in reversed(self.progress_urls[url].split('.')):
                    body = {field: body}
                return 200, json.dumps(body)
            return original_http(host, url, timeout)

        self.ex.run, self.ex.http_get = run, http
        for target in self.target.serving.deployer.targets:
            if self.ex.probe_unit(target.host, target.unit)['load_state'] == 'not-found':
                self.ex.install(target.host, target.unit, '[Service]\nExecStart=/bin/old-serving worker\n',
                                files={'/bin/old-serving': b'old serving'})
            for path in target.settings.get('credentials', []):
                self.ex.put(target.host, path, '')
            self.ex.endpoints[(target.host, target.url(target.role.health))] = target.unit
        self.ex.effects.clear()

    def test_all_roles_advance_then_restore_after_a_floor_handoff(self):
        result = self.deploy()
        self.assertEqual(result['phase'], 'committed')
        self.assertEqual(self.handoffs, [False])
        self.assertEqual(set(result['product_progress']['after']), set(self.request['checks']))
        self.assertIn(enhance_https.PROVIDER, self.ex.read('coordinator', '/etc/caddy/Caddyfile'))
        # Explicit operator rollback remains available after successful deploy.
        self.target.supervision = None
        restored = self.target.rollback()
        self.assertEqual(restored['phase'], 'rolled-back')
        self.assertEqual(self.handoffs, [False, True])
        for records in restored['enhance_journals'].values():
            self.assertTrue(all(r['phase'] == 'restored' for r in records['hosts']))

    def test_unsupervised_deploy_refuses_before_any_stop(self):
        self.target.supervision = None
        with self.assertRaisesRegex(DeployError, 'independent supervised runner'):
            self.deploy()
        self.assertEqual(self.ex.effects, [])

    def test_other_product_settings_cannot_be_changed(self):
        self.target.request['configuration']['status']['settings']['cookie'] = '/another-cookie'
        with self.assertRaisesRegex(DeployError, 'every other product'):
            self.target.plan()
        self.assertEqual(self.ex.effects, [])

    def test_lost_handoff_reply_still_requires_rollback_floor_transfer(self):
        original = self.ex.run

        def fail(host, argv, timeout):
            result = original(host, argv, timeout)
            if 'handoff-https-state' in argv and '--rollback' not in argv:
                raise RemoteError('lost reply after the offline state transfer')
            return result

        self.ex.run = fail
        with self.assertRaisesRegex(RemoteError, 'lost reply'):
            self.deploy()
        self.target.supervision = None
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.handoffs, [False, True])


if __name__ == '__main__':
    unittest.main()
