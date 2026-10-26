"""Public lifecycle results preserve deployment gates and product ownership."""
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_wallet_pir_deploy import ENHANCE, Fleet, NEW_SHA, ROOT  # noqa: E402
from wallet_pir_ops.deploy import cli  # noqa: E402
from wallet_pir_ops.deploy.lifecycle import Lifecycle  # noqa: E402
from wallet_pir_ops.fleet import Fleet as ConfiguredFleet  # noqa: E402


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


class ReadOnlyStatusTests(Fleet):
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
        code = cli.main(['--inventory', str(self.inventory_path), 'ancillary-status'],
                        executor=self.fake, out=output.append)
        units = {u['name']: u for u in json.loads(output[0])['ancillary']}
        self.assertEqual((code, units['edge']['active_state'], units['gone']['load_state']),
                         (0, 'active', 'not-found'))
        self.assertEqual(self.fake.log, [])


class FleetViewTests(unittest.TestCase):
    def test_two_products_cannot_own_the_same_host_unit(self):
        fleet = ConfiguredFleet.load(ROOT / 'tools/enhance/deploy/deploy-inventory.example.json')
        worker = fleet.inventory.services['transparent-history']['roles']['worker'][0]
        worker.update(host='worker-01', unit='enhance-pir-worker.service')
        with self.assertRaisesRegex(ValueError, 'both own enhance-pir-worker.service'):
            fleet.describe()


if __name__ == '__main__':
    unittest.main()
