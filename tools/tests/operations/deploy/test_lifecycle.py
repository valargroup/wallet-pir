"""Public lifecycle results preserve deployment gates and product ownership."""
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_wallet_pir_deploy import ENHANCE, Fleet, NEW_SHA, SERVING_SELECTORS, SERVICES, ROOT  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors  # noqa: E402
from wallet_pir_ops.deploy.lifecycle import Lifecycle  # noqa: E402
from wallet_pir_ops.fleet import Fleet as ConfiguredFleet  # noqa: E402


def historical_cli(*args, **kwargs):
    """Exercise CLI recovery against the captured pre-cutover service registry."""
    with patch.object(descriptors, 'load_descriptors', return_value=SERVICES):
        return cli.main(*args, **kwargs)


class LifecycleTests(Fleet):
    def test_staging_preflight_refuses_a_missing_baseline_before_upload(self):
        deployer = self.enhance_fleet()
        deployer.baseline_path.unlink()
        result = Lifecycle(deployer).inspect('preflight', NEW_SHA, self.binary,
                                            retire_historical=True, stage=True)
        self.assertTrue(any('no baseline' in reason for reason in result.problems))
        self.assertFalse(any(op in ('write', 'upload', 'mkdir', 'systemctl', 'rename', 'remove')
                             for _, op, _ in self.fake.log))

    def test_structured_deploy_status_and_rollback_keep_the_transaction(self):
        lifecycle = Lifecycle(self.enhance_fleet())
        result = lifecycle.deploy(NEW_SHA, self.binary, retire_historical=True)
        self.assertEqual(result.phase, 'committed')
        self.assertTrue(result.transaction)
        status = lifecycle.status()
        self.assertEqual(status.transaction, result.transaction)
        self.assertEqual(len(status.targets), len(ENHANCE))
        self.assertTrue(all(t['binary_sha256'] == NEW_SHA for t in status.targets))
        rollback = lifecycle.rollback(result.transaction)
        self.assertEqual(rollback.phase, 'rolled-back')
        self.assertEqual(rollback.transaction, result.transaction)

    def test_json_cli_emits_one_result_instead_of_progress_text(self):
        self.enhance_fleet()
        output = []
        selected = [item for role in SERVING_SELECTORS for item in ('--only', role)]
        code = historical_cli(['--inventory', str(self.inventory_path), '--state-dir', str(self.state),
                         '--json', 'plan', 'enhance', '--binary', str(self.binary), '--retire-historical', *selected],
                        executor=self.fake, out=output.append)
        self.assertEqual(code, 0, output)
        self.assertEqual(len(output), 1)
        self.assertEqual(json.loads(output[0])['operation'], 'plan')
        self.assertEqual(len(json.loads(output[0])['targets']), len(ENHANCE))
        self.assertEqual(self.fake.log, [])

    def test_product_publications_cannot_use_the_binary_deploy_driver(self):
        inventory = json.loads(self.inventory_path.read_text())
        inventory['services']['transparent-history'] = {'roles': {'worker': [
            {'host': 'worker-01', 'vars': {'listen': '127.0.0.1:8093'}}]}}
        self.inventory_path.write_text(json.dumps(inventory))
        output = []
        code = historical_cli(['--inventory', str(self.inventory_path), 'deploy', 'transparent-history',
                         '--binary', str(self.binary)], executor=self.fake, out=output.append)
        self.assertEqual(code, 1)
        self.assertIn('product recipes', output[0])
        self.assertEqual(self.fake.log, [])


class ReadOnlyStatusTests(Fleet):
    def test_status_reads_only_selected_targets_and_reports_unreachable_ones(self):
        self.enhance_fleet()
        output = []
        code = historical_cli(['--inventory', str(self.inventory_path), '--state-dir', str(self.state), '--json',
                         'status', 'enhance', '--only', 'worker'], executor=self.fake, out=output.append)
        result = json.loads(output[0])
        self.assertEqual((code, [t['host'] for t in result['targets']]), (0, ['worker-01', 'worker-02']))
        def unreachable(host, unit):
            raise RuntimeError('ssh: connect to host worker-02: timed out')
        self.fake.probe_unit, original = unreachable, self.fake.probe_unit
        output.clear()
        historical_cli(['--inventory', str(self.inventory_path), '--state-dir', str(self.state), '--json',
                  'status', 'enhance', '--only', 'worker@worker-02'], executor=self.fake, out=output.append)
        self.fake.probe_unit = original
        self.assertIn('timed out', json.loads(output[0])['targets'][0]['error'])
        self.assertEqual(self.fake.log, [])

    def test_ancillary_status_covers_every_unit_read_only(self):
        inventory = json.loads(self.inventory_path.read_text())
        inventory['ancillary'] = {'edge': {'host': 'coordinator', 'unit': 'caddy.service', 'kind': 'routing',
                                           'consumers': ['enhance']},
                                  'gone': {'host': 'worker-01', 'unit': 'missing.service', 'kind': 'routing',
                                           'consumers': ['enhance']}}
        self.inventory_path.write_text(json.dumps(inventory))
        self.fake.install('coordinator', 'caddy.service', '[Service]\nExecStart=/usr/bin/caddy run\n',
                          files={'/usr/bin/caddy': b'caddy'})
        self.fake.log.clear()
        output = []
        code = historical_cli(['--inventory', str(self.inventory_path), 'ancillary-status'],
                        executor=self.fake, out=output.append)
        units = {u['name']: u for u in json.loads(output[0])['ancillary']}
        self.assertEqual((code, units['edge']['active_state'], units['gone']['load_state']),
                         (0, 'active', 'not-found'))
        self.assertEqual(self.fake.log, [])


class FleetViewTests(unittest.TestCase):
    def test_one_view_covers_each_product_without_ssh(self):
        fleet = ConfiguredFleet.load(ROOT / 'tools/enhance/deploy/deploy-inventory.example.json')
        view = fleet.describe()
        self.assertEqual(view['view'], 'configured')
        self.assertEqual({r['service'] for r in view['roles']}, set(fleet.services))
        self.assertEqual({r['lifecycle_driver'] for r in view['roles']},
                         {'binary', 'transparent-history', 'txid-display'})

    def test_two_products_cannot_own_the_same_host_unit(self):
        fleet = ConfiguredFleet.load(ROOT / 'tools/enhance/deploy/deploy-inventory.example.json')
        worker = fleet.inventory.services['transparent-history']['roles']['worker'][0]
        worker.update(host='worker-01', unit='enhance-pir-worker.service')
        with self.assertRaisesRegex(ValueError, 'both own enhance-pir-worker.service'):
            fleet.describe()


if __name__ == '__main__':
    unittest.main()
