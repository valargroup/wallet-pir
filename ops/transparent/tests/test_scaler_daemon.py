"""The scaler daemon: files it writes, modes, locking, recovery, never crashing."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from scaler_fixtures import NOW, OPS, POLICY
from scaler import metrics as M
from test_scaler_metrics import render_worker
from test_scaler_signals import inventory, observed, record

SPEC = importlib.util.spec_from_file_location('fleet_scaler', OPS / 'scripts' / 'transparent-fleet-scaler.py')
DAEMON = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DAEMON)
DAEMON.log = lambda event, **fields: None

CONTRACT_STATUS = {'schema', 'updated_unix', 'mode', 'heartbeat_unix', 'decision', 'desired_recent',
                   'serving_recent', 'offered_qps', 'holds', 'operation', 'budget', 'forecast'}


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


class DaemonTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.state = Path(temp.name, 'state')
        self.dir = Path(temp.name, 'scaler')
        self.state.mkdir()
        (self.dir / 'journal' / 'done').mkdir(parents=True)
        self.clock = Clock(NOW)
        self.qps = 5.0
        self.write_policy('observe')
        self.write_fleet()
        self.scaler = self.make()

    def make(self):
        return DAEMON.Scaler(self.state, self.dir, 'http://publisher/v1/status', clock=self.clock,
                             fetch=self.fetch, fetch_status=self.fetch_status)

    def write_policy(self, mode, **extra):
        (self.dir / 'policy.json').write_text(json.dumps({**POLICY, 'mode': mode, **extra}))

    def write_fleet(self):
        (self.state / 'inventory.json').write_text(json.dumps(inventory(
            record('a0', role='archive-owner'), record('r1'), record('r2'))))
        self.write_membership()

    def write_membership(self):
        members = {'a0': observed(role='archive-owner'), 'r1': observed(), 'r2': observed()}
        for value in members.values():
            value['observed_unix'] = self.clock.t - 1
        (self.state / 'membership.json').write_text(json.dumps(
            {'schema': 1, 'updated_unix': self.clock.t - 0.5, 'members': members, 'routing_generation': 1}))

    def fetch(self, url, timeout):
        elapsed = self.clock.t - (NOW - 1000)
        queries = int(self.qps * elapsed)
        return M.parse(render_worker(queries=queries, rejections=0, overloads=0, deadline=0,
                                     busy=int(elapsed * 1e6 * 0.2), success={0.05: queries}))

    def fetch_status(self, url, timeout):
        return {'phase': 'serving', 'public_height': 100, 'node_height': 100, 'freshness_seconds': 20.0,
                'ready_replicas': 2}

    def warm_up(self, scaler=None, seconds=360):
        scaler = scaler or self.scaler
        for _ in range(int(seconds / 15) + 1):
            self.write_membership()
            scaler.scrape()
            self.clock.t += 15
        self.clock.t -= 15

    def status(self):
        return json.loads((self.dir / 'status.json').read_text())

    def test_observe_cycle_writes_state_status_and_log_but_no_request(self):
        self.warm_up()
        decision = self.scaler.cycle()
        self.assertEqual(decision['action'], 'hold')
        status = self.status()
        self.assertLessEqual(CONTRACT_STATUS, set(status))
        self.assertEqual(status['schema'], 1)
        self.assertEqual(status['mode'], 'observe')
        self.assertEqual(status['serving_recent'], 2)
        self.assertEqual(status['desired_recent'], 2)
        self.assertAlmostEqual(status['offered_qps'], 10.0)
        self.assertIsNone(status['operation'])
        self.assertEqual(status['awaiting_operator'], [])
        self.assertEqual(status['orphans'], [])
        self.assertEqual(status['budget'], {'actions_left': 6, 'destroys_left': 2, 'monthly_cost_usd': 96.0})
        self.assertIn('days_to_recent_budget', status['forecast'])
        self.assertNotIn('recommendation', status)
        self.assertEqual(status['updated_unix'], self.clock.t)
        self.assertIn('steady', status['decision']['reason'])
        self.assertFalse((self.dir / 'request.json').exists())
        state = json.loads((self.dir / 'state.json').read_text())
        self.assertEqual(state['schema'], 1)
        self.assertEqual(len(state['forecast_samples']), 1)
        log = (self.dir / 'decisions.jsonl').read_text().splitlines()
        self.assertEqual(len(log), 1)
        self.assertFalse(json.loads(log[0])['request_written'])
        self.clock.t += 15
        self.scaler.heartbeat()
        status = self.status()
        self.assertEqual((status['heartbeat_unix'], status['updated_unix']), (self.clock.t, self.clock.t))
        self.assertEqual(status['decision']['decided_unix'], self.clock.t - 15)

    def test_recommend_publishes_the_recommendation(self):
        self.write_policy('recommend', scale_out_confirm_seconds=0)
        self.qps = 10.0  # 20 qps over two replicas: 4 wanted
        self.warm_up()
        self.scaler.cycle()
        status = self.status()
        self.assertEqual(status['recommendation']['action'], 'scale_out')
        self.assertEqual(status['recommendation']['count'], 2)
        self.assertFalse((self.dir / 'request.json').exists())

    def test_act_writes_one_request_and_then_waits_for_the_actuator(self):
        self.write_policy('act', scale_out_confirm_seconds=0)
        self.qps = 10.0  # 20 qps over two replicas: 4 wanted
        self.warm_up()
        decision = self.scaler.cycle()
        request = json.loads((self.dir / 'request.json').read_text())
        self.assertEqual(request['schema'], 1)
        self.assertEqual(request['decision_id'], decision['decision_id'])
        self.assertEqual((request['action'], request['count']), ('scale_out', 2))
        self.assertNotIn('member', request)
        state = json.loads((self.dir / 'state.json').read_text())
        self.assertEqual(state['request']['decision_id'], request['decision_id'])
        self.clock.t += 60
        self.write_membership()
        self.scaler.scrape()
        decision = self.scaler.cycle()
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('awaiting the actuator', decision['reason'])
        self.assertEqual(json.loads((self.dir / 'request.json').read_text()), request)
        self.assertEqual(self.status()['budget']['actions_left'], 5)

    def test_act_dry_also_writes_requests(self):
        self.write_policy('act-dry', scale_out_confirm_seconds=0)
        self.qps = 10.0  # 20 qps over two replicas: 4 wanted
        self.warm_up()
        self.scaler.cycle()
        self.assertTrue((self.dir / 'request.json').exists())

    def test_bad_inputs_hold_without_crashing(self):
        self.warm_up()
        (self.state / 'membership.json').write_text('{')
        (self.dir / 'journal' / 'operation.json').write_text('[')
        (self.dir / 'policy.json').write_text('nope')
        decision = self.scaler.cycle()
        self.assertEqual(decision['action'], 'hold')
        status = self.status()
        self.assertEqual(status['mode'], 'invalid')
        self.assertTrue(status['holds'])
        (self.dir / 'policy.json').unlink()
        decision = self.scaler.cycle()
        self.assertIn('policy', decision['reason'])

    def test_internal_errors_become_a_hold_status(self):
        self.warm_up()
        stop = threading.Event()
        with patch.object(DAEMON.D, 'decide', side_effect=RuntimeError('boom')):
            self.scaler.run(stop, once=True)
        status = self.status()
        self.assertEqual(status['decision']['action'], 'hold')
        self.assertIn('scaler error: RuntimeError: boom', status['decision']['reason'])
        self.assertEqual(status['orphans'], [])

    def test_single_instance_lock(self):
        first = DAEMON.lock(self.dir)
        self.addCleanup(first.close)
        self.assertIsNone(DAEMON.lock(self.dir))
        self.assertEqual(DAEMON.main(['--state-dir', str(self.state), '--scaler-dir', str(self.dir), '--once']), 1)

    def test_corrupt_state_is_kept_aside_and_budget_rebuilt_from_the_log(self):
        self.write_policy('act', scale_out_confirm_seconds=0)
        self.qps = 10.0  # 20 qps over two replicas: 4 wanted
        self.warm_up()
        self.scaler.cycle()
        (self.dir / 'state.json').write_text('{"schema": 1, "actions": [')
        scaler = self.make()
        self.clock.t += 60
        self.warm_up(scaler, 30)
        scaler.cycle()
        self.assertTrue(list(self.dir.glob('state.json.corrupt-*')))
        state = json.loads((self.dir / 'state.json').read_text())
        self.assertEqual(len(state['actions']), 1)
        self.assertEqual(self.status()['budget']['actions_left'], 5)

    def test_decision_log_rotates_daily_and_keeps_fourteen(self):
        for day in range(16):
            (self.dir / f'decisions-202609{day + 1:02d}.jsonl').write_text('{}\n')
        path = self.dir / 'decisions.jsonl'
        path.write_text('{"old": true}\n')
        os.utime(path, (NOW - 86400, NOW - 86400))
        self.scaler.append_log(NOW, {'new': True})
        rotated = sorted(p.name for p in self.dir.glob('decisions-*.jsonl'))
        self.assertEqual(len(rotated), 14)
        self.assertIn(f'decisions-{DAEMON.utc_day(NOW - 86400)}.jsonl', rotated)
        self.assertEqual(path.read_text(), '{"new": true}\n')

    def test_main_once_runs_one_cycle(self):
        code = DAEMON.main(['--state-dir', str(self.state), '--scaler-dir', str(self.dir), '--once',
                            '--publisher-url', 'http://127.0.0.1:9/v1/status'])
        self.assertEqual(code, 0)
        status = self.status()
        self.assertEqual(status['decision']['action'], 'hold')


if __name__ == '__main__':
    unittest.main()
