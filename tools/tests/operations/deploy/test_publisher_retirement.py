"""Approved state retirement preserves the running publisher and its baseline."""
import json
import unittest
from unittest.mock import patch

from publisher_fixture import PublisherFixture
from wallet_pir_ops.deploy import publisher_retirement as retirement
from wallet_pir_ops.deploy.engine import DeployError


class RetirementTests(PublisherFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.request['release']['binary_sha256'] = self.old_publisher
        self.target = self.make_target()
        self.roots = {root: {'device': 1, 'inode': index + 10, 'bytes': 1 << 30}
                      for index, root in enumerate(retirement.ROOTS)}
        self.baseline = {'id': 'publisher-existing', 'phase': 'committed', 'request': self.request}
        original = self.ex.run
        def run(host, argv, timeout):
            if argv[:3] == ['python3', '-c', retirement.INSPECT]:
                return 0, json.dumps(self.roots)
            if argv[:2] == ['python3', '-c'] and 'shutil.rmtree(p)' in argv[2]:
                self.ex.runs.append((host, argv))
                self.roots[argv[3]] = None
                return 0, ''
            return original(host, argv, timeout)
        self.addCleanup(patch.stopall)
        patch.object(self.ex, 'run', side_effect=run).start()
        patch.object(self.target, 'latest', return_value=self.baseline).start()

    def test_plan_and_preflight_are_read_only_and_name_only_approved_roots(self):
        for operation in ('retire-state-plan', 'retire-state-preflight'):
            plan = retirement.dispatch(self.target, operation)
            self.assertEqual(set(plan['plan']['roots']), set(retirement.ROOTS))
            self.assertTrue(plan['irreversible'])
        self.assertEqual(self.ex.mutations(), [])

    def test_changed_plan_is_refused_before_deletion(self):
        plan = retirement.dispatch(self.target, 'retire-state-plan')
        self.roots[retirement.ROOTS[0]]['inode'] += 1
        with self.assertRaisesRegex(DeployError, 'plan is stale'):
            retirement.dispatch(self.target, 'retire-state-deploy', plan['plan_sha256'])
        self.assertEqual(self.ex.mutations(), [])

    def test_active_or_rollback_configuration_cannot_use_retired_state(self):
        self.baseline['request'] = {'configuration': {'data_dir': retirement.ROOTS[0]}}
        with self.assertRaisesRegex(DeployError, 'rollback publisher uses'):
            retirement.dispatch(self.target, 'retire-state-plan')
        self.assertEqual(self.ex.mutations(), [])

    def test_deletion_records_intent_without_stopping_the_publisher(self):
        plan = retirement.dispatch(self.target, 'retire-state-plan')
        result = retirement.dispatch(self.target, 'retire-state-deploy', plan['plan_sha256'])
        self.assertEqual(result['phase'], 'retired')
        self.assertEqual(result['completed'], list(retirement.ROOTS))
        self.assertTrue(all(value is None for value in self.roots.values()))
        self.assertEqual(self.ex.effects, [])
        receipts = [p for p in self.ex.host(self.target.host).files if '/retirements/' in p]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(json.loads(self.ex.read(self.target.host, receipts[0])), result)


if __name__ == '__main__':
    unittest.main()
