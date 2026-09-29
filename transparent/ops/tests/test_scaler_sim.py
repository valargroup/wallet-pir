"""Seeded fleet simulation of the scaler: invariants over 200+ scenarios.

sim/fleet.py models the fleet (queueing latency, the per-publication tail
rebuild stall, provisioning and warm time, faults) and a journaled actuator;
sim/check.py checks the invariants; sim/scenarios.py is the suite. The
mutation tests below break the scaler on purpose and require the checker to
notice, so a green suite means something.
"""
import concurrent.futures
import copy
import os
import sys
import time
import unittest

import scaler_fixtures  # noqa: F401 - puts transparent/ops on the path
from scaler import decide as D
from sim import traces as T
from sim.check import run
from sim.fleet import HOUR, MINUTE, T0, Scenario
from sim.scenarios import run_entry, suite


def workers():
    try:
        return max(1, int(os.environ.get('TRANSPARENT_SIM_WORKERS', min(4, os.cpu_count() or 1))))
    except ValueError:
        return 1


def run_all(count):
    if workers() == 1:
        return [run_entry(i) for i in range(count)]
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers()) as pool:
            return list(pool.map(run_entry, range(count), chunksize=4))
    except (OSError, NotImplementedError, concurrent.futures.process.BrokenProcessPool):
        return [run_entry(i) for i in range(count)]


def with_policy(**changes):
    """A scaler deciding under a different policy than the checker enforces."""
    def decide(snapshot, policy, state, now):
        changed = copy.deepcopy(policy)
        for key, value in changes.items():
            changed[key] = {**changed[key], **value} if isinstance(value, dict) else value
        return D.decide(snapshot, changed, state, now)
    return decide


def blind_to_publisher(snapshot, policy, state, now):
    publisher = {**snapshot['publisher'], 'serving': True, 'age_seconds': 0.0, 'lag_seconds': 0.0, 'error': None}
    return D.decide({**snapshot, 'publisher': publisher}, policy, state, now)


def forgets_operations(snapshot, policy, state, now):
    return D.decide({**snapshot, 'operation': None, 'done_decision_ids': []}, policy,
                    {**state, 'request': None, 'last_scale_out_complete_unix': None}, now)


def never_scales_in(snapshot, policy, state, now):
    decision, new = D.decide(snapshot, policy, state, now)
    if decision['action'] == 'scale_in':
        return D.decide(snapshot, {**policy, 'mode': 'observe'}, state, now)
    return decision, new


def renamed(action, member):
    def decide(snapshot, policy, state, now):
        decision, new = D.decide(snapshot, policy, state, now)
        if decision['action'] == action:
            decision['member'] = member
        return decision, new
    return decide


def naive_p99(snapshot, policy, state, now):
    """Acts on the window's plain p99, as a scaler ignorant of the rebuild stall would."""
    load = {**snapshot['load'], 'p99_seconds': snapshot['load']['p99_window_seconds']}
    return D.decide({**snapshot, 'load': load}, policy, state, now)


def publisher_outage(fleet):
    fleet.windows['publisher_down'].append((T0 + HOUR - 30, T0 + HOUR + 20 * MINUTE))


class SuiteTest(unittest.TestCase):
    def test_every_scenario_keeps_every_invariant(self):
        scenarios = suite()
        self.assertGreaterEqual(len(scenarios), 200)
        started = time.monotonic()
        results = run_all(len(scenarios))
        elapsed = time.monotonic() - started
        failures = [(r['name'], r['violations'][:5]) for r in results if r['violations']]
        self.assertEqual(failures, [])
        actions = {}
        flags = set()
        for r in results:
            for action in r['requests']:
                actions[action] = actions.get(action, 0) + 1
            flags.update(r['flags'])
        hours = sum(r['hours'] for r in results)
        print(f'\nscaler simulation: {len(results)} scenarios, {hours:.0f} virtual hours, {actions}, '
              f'{elapsed:.1f} s on {workers()} processes', file=sys.stderr)
        # The suite exercises every kind of action and the alarming paths.
        self.assertTrue(all(actions.get(a, 0) > 10 for a in ('scale_out', 'scale_in', 'replace')), actions)
        for flag in ('capacity model drift', 'correlated failures', 'awaiting operator',
                     'daily destroy budget exhausted'):
            self.assertIn(flag, flags)
        self.assertTrue(any(f.startswith('operation op-') and f.endswith('fenced') for f in flags), flags)


class BehaviourTest(unittest.TestCase):
    def test_step_reaction_within_twenty_minutes(self):
        for before, after, want in ((4, 18, 4), (4, 24, 5), (8, 18, 4)):
            result = run(Scenario(7, T.Step(before, after, HOUR), 2))
            self.assertEqual(result.violations, [])
            reached = next(t for t, n in result.serving if t >= T0 + HOUR and n >= want)
            self.assertLessEqual(reached - (T0 + HOUR), 20 * MINUTE, (before, after))

    def test_scale_back_toward_the_floor(self):
        result = run(Scenario(8, T.UpDown(3, 24, 45 * MINUTE, 165 * MINUTE), 7))
        self.assertEqual(result.violations, [])
        ins = [e for e in result.requests if e[2] == 'scale_in']
        self.assertGreaterEqual(len(ins), 2)
        self.assertGreaterEqual(ins[0][0] - (T0 + 165 * MINUTE), 3600)  # hold and cooldown
        self.assertLess(result.final_serving, result.max_serving)

    def test_rebuild_stalls_never_trip_the_p99_backstop(self):
        for qps in (4, 8, 12):
            for seed in range(3):
                result = run(Scenario(40 + seed, T.Flat(qps), 3))
                self.assertEqual(result.violations, [])
                self.assertNotIn('capacity model drift', result.flags)
                self.assertTrue(all('backstop' not in e[4] for e in result.requests), result.requests)
                # The stall is really there: the window's plain p99 is in the seconds.
                naive = run(Scenario(40 + seed, T.Flat(qps), 3), decide=naive_p99)
                self.assertIn('capacity model drift', naive.flags)
                self.assertEqual(naive.max_serving, 6)

    def test_deterministic(self):
        scenario = suite()[-1]
        self.assertEqual(run(scenario).events, run(scenario).events)


class MutationTest(unittest.TestCase):
    """Deliberately broken scalers; the checker must object to each."""

    def assertCaught(self, text, scenario, decide, setup=None):
        result = run(scenario, decide=decide, setup=setup)
        self.assertTrue(any(text in v for v in result.violations), result.violations[:5])

    def test_acting_on_a_stale_publisher(self):
        self.assertCaught('stale publisher status', Scenario(1, T.Step(4, 24, HOUR), 2.5), blind_to_publisher,
                          publisher_outage)

    def test_second_operation_while_one_is_open(self):
        self.assertCaught('requested while operation', Scenario(2, T.Step(4, 40, HOUR), 2), forgets_operations)

    def test_flapping(self):
        self.assertCaught('flapping', Scenario(3, T.Spike(3, 30, (HOUR,), 600), 3),
                          with_policy(scale_in={'hold_seconds': 0}, cooldown_in_seconds=1))

    def test_daily_budgets(self):
        scenario = Scenario(4, T.Sawtooth(2, 30, 30 * MINUTE, 30 * MINUTE), 8)
        decide = with_policy(daily_actions=100, daily_destroys=100, scale_in={'hold_seconds': 600},
                             cooldown_in_seconds=600)
        self.assertCaught('actions in 24 h', scenario, decide)
        self.assertCaught('destroy budget exceeded', scenario, decide)

    def test_cost_cap(self):
        self.assertCaught('a month', Scenario(5, T.Flat(60), 2),
                          with_policy(monthly_cost_cap_usd=10000, max_recent=12, max_step=10))

    def test_slow_reaction(self):
        self.assertCaught('20 min later', Scenario(6, T.Step(4, 24, HOUR), 2.5), with_policy(max_step=1))

    def test_never_scaling_back(self):
        self.assertCaught('no scale-in within', Scenario(7, T.UpDown(3, 24, 45 * MINUTE, 165 * MINUTE), 5.5),
                          never_scales_in)

    def test_static_or_archive_victims(self):
        updown = Scenario(8, T.UpDown(3, 24, 45 * MINUTE, 165 * MINUTE), 5.5)
        self.assertCaught('names static member', updown, renamed('scale_in', 'transparent-pir-recent-01'))
        self.assertCaught('names archive member', updown, renamed('scale_in', 'transparent-pir-archive-01'))


if __name__ == '__main__':
    unittest.main()
