"""Prove that adding products restores the pre-existing mixed deployment exactly."""
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parent))
import test_publisher_cutover as baseline_tests
from wallet_pir_ops.deploy import publisher, publisher_products as products
from wallet_pir_ops.deploy.engine import DeployError
from wallet_pir_ops.deploy.remote import RemoteError


class ExpandedCoordinator(baseline_tests.Coordinator):
    def run(self, host, argv, timeout):
        if '--check' in argv and not self.refuse_configuration:
            return 0, 'all configured native products compiled'
        return super().run(host, argv, timeout)


class ExpansionTests(unittest.TestCase):
    def setUp(self):
        baseline = baseline_tests.PublisherCutoverTests()
        baseline.setUp()
        self.addCleanup(baseline.doCleanups)
        baseline.deploy()
        self.baseline = baseline
        self.ex = ExpandedCoordinator()
        self.ex.__dict__.update(baseline.ex.__dict__)
        self.request = copy.deepcopy(baseline.request)
        self.root = baseline.root
        self.binary = baseline.binary
        self.ex.install('coordinator', publisher.ADOPTED[0],
                        '[Service]\nEnvironmentFile=/etc/pir/credentials.env\nExecStart=/bin/enhance --sealed-shards 6 coordinator '
                        '--data-dir /srv/enhance --worker-config /etc/enhance/workers.json '
                        '--zakura-cookie /run/node/cookie\n', files={'/bin/enhance': b'enhance', '/etc/pir/credentials.env': b''})
        self.ex.install('coordinator', publisher.ADOPTED[1],
                        '[Service]\nExecStart=/bin/status serve-distributed --config ' + products.STATUS_CONFIG +
                        ' --cookie /run/node/cookie --public-listen 127.0.0.1:8489\n', files={'/bin/status': b'status'})
        self.ex.put('coordinator', '/etc/enhance/workers.json', '{}')
        self.ex.put('coordinator', products.STATUS_CONFIG, json.dumps({'state_dir': '/srv/status', 'public_enabled': True}))
        self.request['schema'] = publisher.EXPANDED_SCHEMA
        states = {name: self.ex.probe_unit('coordinator', name) for name in publisher.ADOPTED}
        self.request['configuration']['enhance'] = {'enabled': True, 'settings': products.enhance_settings(states[publisher.ADOPTED[0]])}
        self.request['configuration']['status'] = {'enabled': True, 'settings': products.status_settings(states[publisher.ADOPTED[1]], lambda p: self.ex.read('coordinator', p))}
        self.request['checks'].update({name: {'host': 'coordinator', 'argv': ['/bin/check-' + name], 'timeout': 20} for name in ('enhance', 'status')})
        # Retain complete serving roles from the documented fleet.
        source = json.loads((publisher.ROOT / 'tools/enhance/deploy/deploy-inventory.example.json').read_text())
        inventory = self.request['inventory']
        inventory['hosts'].update(source['hosts'])
        for name, role in [('enhance', 'coordinator'), ('status', 'controller')]:
            inventory['services'][name] = copy.deepcopy(source['services'][name])
            inventory['services'][name]['roles'].pop(role)
        inventory['ancillary'] = {k: v for k, v in source['ancillary'].items()
                                  if set(v['consumers']) <= set(inventory['services'])}
        inventory['services']['publisher']['managed_products'] = publisher.EXPANDED_PRODUCTS
        self.target = self.make_target()
        self.ex.effects.clear()

    def make_target(self):
        path = self.root / 'expanded.json'
        path.write_text(json.dumps(self.request))
        request, inventory = publisher.load_request(path, publisher.sha(path.read_text()))
        return publisher.Cutover(request, inventory, self.ex, self.root, clock=self.ex.clock, sleep=self.ex.sleep)

    def test_deploy_restart_and_rollback_preserve_the_original_mixed_owners(self):
        before = self.ex.read('coordinator', publisher.CONFIG)
        self.target.deploy(self.binary, self.target.plan()['plan_sha256'])
        self.assertIn('MemoryMax=40G', self.ex.read('coordinator', publisher.UNIT_PATH))
        self.assertIn('EnvironmentFile=/etc/pir/credentials.env', self.ex.read('coordinator', publisher.UNIT_PATH))
        self.assertTrue(all(self.ex.probe_unit('coordinator', n)['active_state'] == 'inactive' for n in publisher.ADOPTED))
        self.assertEqual(self.target.restart()['phase'], 'committed')
        self.assertEqual(self.target.rollback()['phase'], 'rolled-back')
        self.assertEqual(self.ex.read('coordinator', publisher.CONFIG), before)
        self.assertTrue(all(self.ex.probe_unit('coordinator', n)['active_state'] == 'active' for n in (*publisher.ADOPTED, publisher.UNIT)))

    def test_changed_product_policy_is_refused_before_any_stop(self):
        self.request['configuration']['enhance']['settings']['sealed_shards'] = 7
        with self.assertRaisesRegex(DeployError, 'more than process ownership'):
            self.make_target().plan()
        self.assertEqual(self.ex.effects, [])

    def test_changed_input_invalidates_the_reviewed_plan(self):
        plan = self.target.plan()
        self.ex.put('coordinator', '/etc/enhance/workers.json', '{"changed":true}')
        with self.assertRaisesRegex(DeployError, 'stale'):
            self.target.deploy(self.binary, plan['plan_sha256'])
        self.assertEqual(self.ex.effects, [])

    def test_lost_stop_reply_restores_all_original_owners(self):
        self.ex.crash_on = ('stop', publisher.ADOPTED[0])
        with self.assertRaises(RemoteError):
            self.target.deploy(self.binary, self.target.plan()['plan_sha256'])
        self.target.rollback()
        self.assertTrue(all(self.ex.probe_unit('coordinator', n)['active_state'] == 'active' for n in (*publisher.ADOPTED, publisher.UNIT)))

    def test_unknown_predecessor_argument_is_refused(self):
        state = self.ex.probe_unit('coordinator', publisher.ADOPTED[0])
        state['fragment_text'] = state['fragment_text'].rstrip() + ' --unknown-policy 1\n'
        with self.assertRaisesRegex(DeployError, 'unhandled'):
            products.enhance_settings(state)


if __name__ == '__main__':
    unittest.main()
