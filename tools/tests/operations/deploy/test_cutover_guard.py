"""The saved deadline arbitrates real forward progress and recovery races."""
import json
import multiprocessing
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops.deploy.cutover_guard import Decision, services
from wallet_pir_ops.deploy.engine import DeployError


def decide(directory, action, output):
    guard = Decision(directory, 'publisher-fixture', clock=lambda: 1_700_000_001)
    try:
        if action == 'accept':
            guard.accept({'products': {'enhance': {'ready': True, 'advanced': True, 'exact': True}}})
            output.put('accepted')
        else:
            output.put('rollback' if guard.claim_rollback(runner_active=False) else 'refused')
    except DeployError:
        output.put('refused')


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.now = 1_700_000_000.75
        self.root = Path(self.directory.name)
        self.guard = Decision(self.root, 'publisher-fixture', clock=lambda: self.now)
        self.guard.create('a' * 64)
        self.guard.started('fixture', 123)

    def evidence(self):
        return {'products': {name: {'ready': True, 'advanced': True, 'exact': True}
                             for name in ('enhance', 'status', 'transparent_history', 'transaction_details')}}

    def test_retry_and_process_restart_cannot_extend_the_original_hour(self):
        deadline = self.guard.arm()
        self.now += 900
        restarted = Decision(self.root, 'publisher-fixture', clock=lambda: self.now)
        self.assertEqual(restarted.arm(), deadline)
        self.now = deadline
        with self.assertRaisesRegex(DeployError, 'deadline'):
            restarted.guard()
        self.assertTrue(restarted.claim_rollback(runner_active=True))
        self.now += 30
        self.assertTrue(restarted.claim_rollback(runner_active=False))
        self.assertEqual(json.loads(restarted.path.read_text())['rollback_started_unix'], deadline)
        with self.assertRaises(DeployError):
            restarted.accept(self.evidence())
        restarted.restored()
        self.assertFalse(restarted.claim_rollback(runner_active=False))

    def test_timely_complete_acceptance_prevents_deadline_rollback(self):
        deadline = self.guard.arm()
        self.now = deadline - 0.1
        self.guard.accept(self.evidence())
        self.now = deadline + 1
        self.assertFalse(self.guard.claim_rollback(runner_active=False))

    def test_missing_product_advancement_cannot_disarm_recovery(self):
        self.guard.arm()
        evidence = self.evidence()
        evidence['products']['status']['advanced'] = False
        with self.assertRaisesRegex(DeployError, 'advancement'):
            self.guard.accept(evidence)
        self.assertTrue(self.guard.claim_rollback(runner_active=False))

    def test_deadline_and_watcher_race_cannot_accept_after_fencing(self):
        deadline = self.guard.arm()
        self.now = deadline
        self.assertTrue(self.guard.claim_rollback(runner_active=True))
        with self.assertRaises(DeployError):
            self.guard.accept(self.evidence())
        with self.assertRaises(DeployError):
            self.guard.arm()

    def test_running_predeadline_controller_is_not_interrupted(self):
        self.guard.arm()
        self.assertFalse(self.guard.claim_rollback(runner_active=True))
        self.guard.guard()

    def test_reboot_requires_recovery_even_when_wall_clock_moved_backwards(self):
        original = Decision(self.root, 'publisher-fixture', clock=lambda: self.now,
                            boot_id='first-boot', elapsed=lambda: 10)
        original.arm()
        restarted = Decision(self.root, 'publisher-fixture', clock=lambda: self.now - 100,
                             boot_id='next-boot', elapsed=lambda: 1)
        with self.assertRaisesRegex(DeployError, 'deadline'):
            restarted.guard()
        self.assertTrue(restarted.claim_rollback(runner_active=False))

    def test_dead_controller_is_recovered_even_before_the_deadline(self):
        self.guard.arm()
        self.assertTrue(self.guard.claim_rollback(runner_active=False))
        with self.assertRaises(DeployError):
            self.guard.guard()

    def test_separate_processes_cannot_both_accept_and_claim_rollback(self):
        self.guard.arm()
        context = multiprocessing.get_context('fork')
        output = context.Queue()
        processes = [context.Process(target=decide, args=(str(self.root), action, output))
                     for action in ('accept', 'rollback')]
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=5)
            self.assertEqual(process.exitcode, 0)
        results = [output.get(timeout=1) for _ in processes]
        self.assertIn('refused', results)
        self.assertEqual(len(set(results) & {'accepted', 'rollback'}), 1)

    def test_wall_clock_moving_backwards_cannot_extend_a_boots_deadline(self):
        elapsed = [100.0]
        guard = Decision(self.root, 'publisher-fixture', clock=lambda: self.now,
                         boot_id='same-boot', elapsed=lambda: elapsed[0])
        guard.arm()
        self.now -= 7200
        elapsed[0] += 3600
        with self.assertRaisesRegex(DeployError, 'deadline'):
            guard.guard()
        self.assertTrue(guard.claim_rollback(runner_active=True))

    def test_units_own_their_process_group_and_restart_failed_rollback(self):
        files = services('publisher-fixture', ['/usr/bin/python3', '/opt/ops/cli.py', 'run'],
                         ['/usr/bin/python3', '/opt/ops/cli.py', 'watchdog'])
        forward = files['wallet-pir-cutover-publisher-fixture.service']
        rollback = files['wallet-pir-cutover-publisher-fixture-rollback.service']
        self.assertIn('OnFailure=wallet-pir-cutover-publisher-fixture-rollback.service', forward)
        self.assertIn('KillMode=control-group', forward)
        self.assertIn('TimeoutStopSec=30', forward)
        self.assertIn('Restart=on-failure', rollback)
        self.assertIn('WantedBy=multi-user.target', rollback)


if __name__ == '__main__':
    unittest.main()
