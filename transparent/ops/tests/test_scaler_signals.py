"""Snapshots: file inputs, load over the window, unknowns, resets, publisher lag."""
import json
import math
from pathlib import Path
import tempfile
import unittest

from scaler_fixtures import D, NOW, POLICY
from scaler import metrics as M
from scaler import signals as S
from test_scaler_metrics import BUCKETS, render_worker

INF = math.inf


def inventory(*members):
    return {'schema': S.INVENTORY_SCHEMA, 'revision': 3, 'updated_by': 'test', 'updated_unix': NOW,
            'partition': {'ranges': []}, 'members': list(members)}


def record(member_id, role='recent-replica', origin='static', intent='enrolled', **extra):
    return {'id': member_id, 'role': role, 'group': 'recent' if role == 'recent-replica' else 'a0',
            'origin': origin, 'intent': intent, 'ssh_host': '10.0.0.1',
            'upstream': f'{member_id}:8093', **extra}


def observed(state='serving', routed=True, rendered=None, age=1.0, role='recent-replica'):
    return {'role': role, 'state': state, 'routed': routed, 'rendered': routed if rendered is None else rendered,
            'warm': state == 'serving', 'observed_unix': NOW - age, 'transport_failures': 0}


def inputs(members=None, records=None, **extra):
    records = records if records is not None else [record('a0', role='archive-owner'), record('r1'), record('r2')]
    members = members if members is not None else {'a0': observed(role='archive-owner'), 'r1': observed(),
                                                   'r2': observed()}
    value = {'errors': {}, 'inventory': inventory(*records),
             'membership': {'schema': 1, 'updated_unix': NOW - 0.5, 'members': members, 'routing_generation': 3},
             'maintenance': None, 'withdrawn': {'withdrawn': False}, 'operation': None, 'operation_mtime': None,
             'disabled': False, 'done': []}
    value.update(extra)
    return value


def latency(fast, slow=0.0, slow_le=5.0):
    """Cumulative success buckets: `fast` queries at 25 ms, `slow` ones at `slow_le`."""
    out = []
    for le in BUCKETS + (INF,):
        out.append((le, (fast if le >= 0.025 else 0.0) + (slow if le >= slow_le else 0.0)))
    return tuple(out)


def feed(collector, member_id, seconds=360, qps=5.0, rej=0.0, overload=0.0, deadline=0.0, start=1.0,
         t0=None, step=15.0, busy=0.5, stall_every=None, stall_slow=0.0, warm=None):
    """Samples every `step` s ending at NOW: `qps` successes, fast; optional stalls add slow ones."""
    t0 = NOW - seconds if t0 is None else t0
    n = int(round((NOW - t0) / step))
    fast = slow = 0.0
    for i in range(n + 1):
        t = t0 + i * step
        if i:
            fast += qps * step
            if stall_every and i % stall_every == 0:
                slow += stall_slow
        counters = {'queries': fast + slow, 'queue_rejections': rej * i * step, 'overloads': overload * i * step,
                    'deadline_exceeded': deadline * i * step, 'slot_busy_us': busy * 2 * i * step * 1e6,
                    'prewarm_ops': 0.0}
        gauges = {'slots': 2.0, 'warm_runtimes': (warm(i) if warm else 10.0), 'cache_resident_bytes': 4e9,
                  'cache_budget_bytes': 5e9, 'cgroup_memory_bytes': 6e9, 'cgroup_memory_max_bytes': 7e9}
        collector.record_sample(member_id, M.Sample(t=t, start=start, counters=counters, gauges=gauges,
                                                    latency=latency(fast, slow), complete=True))


def publisher(collector, t=NOW - 3, public=100, node=100, phase='serving'):
    collector.record_publisher({'phase': phase, 'public_height': public, 'node_height': node,
                                'freshness_seconds': 20.0, 'ready_replicas': 2}, t)


def healthy_collector():
    collector = S.Collector()
    feed(collector, 'r1')
    feed(collector, 'r2')
    publisher(collector)
    return collector


class InputTests(unittest.TestCase):
    def test_read_inputs_from_files(self):
        with tempfile.TemporaryDirectory() as temp:
            state, scaler = Path(temp, 'state'), Path(temp, 'scaler')
            (scaler / 'journal' / 'done').mkdir(parents=True)
            state.mkdir()
            (state / 'membership.json').write_text(json.dumps({'schema': 1, 'members': {}}))
            (state / 'inventory.json').write_text('{not json')
            (state / 'maintenance.json').write_text(json.dumps({'enabled': True}))
            (scaler / 'journal' / 'operation.json').write_text(json.dumps({'id': 'op', 'phase': 'planned'}))
            (scaler / 'journal' / 'done' / 'op-1.json').write_text(json.dumps({'decision_id': 'd-1'}))
            (scaler / 'journal' / 'done' / 'op-2.json').write_text('garbage')
            (scaler / 'disabled').write_text('')
            value = S.read_inputs(state, scaler)
            self.assertEqual(value['membership'], {'schema': 1, 'members': {}})
            self.assertIn('invalid JSON', value['errors']['inventory'])
            self.assertEqual(value['maintenance'], {'enabled': True})
            self.assertIsNone(value['withdrawn'])
            self.assertEqual(value['operation']['id'], 'op')
            self.assertIsNotNone(value['operation_mtime'])
            self.assertEqual([d['decision_id'] for d in value['done']], ['d-1'])
            self.assertTrue(value['disabled'])

    def test_inventory_validation(self):
        good = inventory(record('a0', role='archive-owner'), record('r1'))
        self.assertEqual(set(S.validate_inventory(good)), {'a0', 'r1'})
        for bad in (inventory(record('a0', role='archive-owner', origin='elastic')),
                    inventory(record('r1'), record('r1')), inventory(record('r1', intent='gone')),
                    inventory(record('r 1')), inventory(record('r1', role='owner')), {'schema': 'x'},
                    {**good, 'revision': '3'}):
            with self.assertRaises(ValueError):
                S.validate_inventory(bad)

    def test_operation_summary(self):
        self.assertIsNone(S.operation_summary(None, None, NOW))
        op = S.operation_summary({'id': 'op-1', 'action': 'scale_out', 'phase': 'applying', 'started_unix': NOW - 30,
                                  'deadline_unix': NOW - 1, 'decision_id': 'd'}, None, NOW)
        self.assertEqual((op['age_seconds'], op['deadline_exceeded'], op['fenced']), (30.0, True, False))
        self.assertTrue(S.operation_summary({'id': 'x', 'phase': 'fenced'}, NOW - 5, NOW)['fenced'])
        self.assertEqual(S.operation_summary({'id': 'x', 'fenced': True}, NOW - 5, NOW)['age_seconds'], 5.0)
        self.assertIsNone(S.operation_summary({'id': 'x', 'phase': 'done'}, None, NOW))
        with self.assertRaises(ValueError):
            S.operation_summary(['x'], None, NOW)

    def test_targets_are_recent_members_in_membership_with_an_upstream(self):
        value = inputs()
        value['membership']['members']['r9'] = observed()
        self.assertEqual(S.Collector.targets(value), [('r1', 'http://r1:8093/metrics'),
                                                      ('r2', 'http://r2:8093/metrics')])


class SnapshotTests(unittest.TestCase):
    def snap(self, collector=None, value=None, policy=None):
        return (collector or healthy_collector()).snapshot(value or inputs(), policy or POLICY, NOW)

    def test_healthy_snapshot(self):
        snap = self.snap()
        self.assertEqual(snap['errors'], [])
        self.assertFalse(snap['maintenance'])
        self.assertFalse(snap['withdrawn'])
        self.assertAlmostEqual(snap['membership']['age_seconds'], 0.5)
        self.assertEqual(snap['inventory']['total_members'], 3)
        self.assertEqual(snap['publisher']['lag_seconds'], 0.0)
        self.assertTrue(snap['publisher']['serving'])
        r1 = snap['members']['r1']
        self.assertTrue(r1['serving'])
        self.assertTrue(r1['static'])
        self.assertEqual(r1['sample_age_seconds'], 0.0)
        self.assertIsNone(snap['members']['a0']['sample_age_seconds'])
        load = snap['load']
        self.assertAlmostEqual(load['offered_qps'], 10.0)
        self.assertAlmostEqual(load['offered_qps_recent'], 10.0)
        self.assertEqual(load['error_ratio'], 0.0)
        self.assertEqual(load['rejections'], 0.0)
        self.assertAlmostEqual(load['utilization'], 0.5)
        self.assertAlmostEqual(load['p99_seconds'], 0.01 + 0.015 * 0.99)  # inside (10 ms, 25 ms]
        self.assertEqual(load['latency_intervals'], 20)

    def test_rates_errors_and_rejections_over_the_window(self):
        collector = S.Collector()
        feed(collector, 'r1', qps=4.0, rej=1.0, overload=0.5, deadline=0.5)
        feed(collector, 'r2', qps=6.0)
        publisher(collector)
        load = self.snap(collector)['load']
        self.assertAlmostEqual(load['offered_qps'], 11.0)  # successes + queue rejections
        self.assertAlmostEqual(load['errors_per_second'], 2.0)
        self.assertAlmostEqual(load['error_ratio'], 2.0 / 12.0)
        self.assertAlmostEqual(load['rejections'], 2.0 * 300)

    def test_periodic_stalls_do_not_move_the_sustained_p99(self):
        collector = S.Collector()
        # 5 qps per replica; every fifth interval (75 s) 10 queries per replica take 5 s: 2.6%
        # of the window, as the live tail-rebuild stall.
        feed(collector, 'r1', stall_every=5, stall_slow=10.0)
        feed(collector, 'r2', stall_every=5, stall_slow=10.0)
        publisher(collector)
        load = self.snap(collector)['load']
        self.assertGreater(load['p99_window_seconds'], 1.4)
        self.assertLess(load['p99_seconds'], 0.1)
        # Overload that slows every interval does move it.
        collector = S.Collector()
        feed(collector, 'r1', stall_every=1, stall_slow=3.0)
        feed(collector, 'r2', stall_every=1, stall_slow=3.0)
        publisher(collector)
        self.assertGreater(self.snap(collector)['load']['p99_seconds'], 1.4)

    def test_serving_member_without_samples_makes_load_unknown(self):
        collector = S.Collector()
        feed(collector, 'r1')
        publisher(collector)
        snap = self.snap(collector)
        self.assertIsNone(snap['load']['offered_qps'])
        self.assertEqual(snap['load']['unknown'], ['r2'])
        self.assertEqual(snap['members']['r2']['metrics_error'], 'not scraped')
        self.assertIsNone(snap['members']['r2']['sample_age_seconds'])
        decision, _ = D.decide(snap, {**POLICY, 'mode': 'act'}, D.initial_state(), NOW)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('stale signal: r2', decision['reason'])

    def test_stale_sample_is_no_sample(self):
        collector = S.Collector()
        feed(collector, 'r1')
        feed(collector, 'r2', seconds=600, t0=NOW - 660)  # last sample 60 s ago
        collector.histories['r2'].samples = [s for s in collector.histories['r2'].samples if s.t <= NOW - 60]
        publisher(collector)
        snap = self.snap(collector)
        self.assertEqual(snap['members']['r2']['sample_age_seconds'], 60.0)
        self.assertIsNone(snap['members']['r2']['window'])
        self.assertIsNone(snap['load']['offered_qps'])

    def test_counter_reset_drops_the_worker_until_it_has_a_window(self):
        collector = S.Collector()
        feed(collector, 'r1')
        feed(collector, 'r2', t0=NOW - 360, seconds=360)
        reset = M.Sample(t=NOW + 15, start=99.0, counters={'queries': 1.0, 'queue_rejections': 0.0,
                                                           'overloads': 0.0, 'deadline_exceeded': 0.0,
                                                           'slot_busy_us': 0.0, 'prewarm_ops': 0.0},
                         gauges={'slots': 2.0}, latency=latency(1.0), complete=True)
        self.assertTrue(collector.record_sample('r2', reset))
        publisher(collector, t=NOW + 10)
        snap = collector.snapshot(inputs(), POLICY, NOW + 15)
        self.assertIsNone(snap['members']['r2']['window'])
        self.assertEqual(snap['members']['r2']['resets'], 1)
        self.assertIsNone(snap['load']['offered_qps'])

    def test_unknown_files_are_none_not_false(self):
        value = inputs(errors={'maintenance': 'maintenance.json: invalid JSON'})
        snap = self.snap(value=value)
        self.assertIsNone(snap['maintenance'])
        self.assertIn('maintenance.json: invalid JSON', snap['errors'])
        snap = self.snap(value=inputs(withdrawn={'withdrawn': 'yes'}))
        self.assertIsNone(snap['withdrawn'])
        snap = self.snap(value=inputs(maintenance={'enabled': True}))
        self.assertTrue(snap['maintenance'])
        snap = self.snap(value=inputs(membership=None))
        self.assertIsNone(snap['membership'])
        self.assertIn('membership missing', snap['errors'])
        snap = self.snap(value=inputs(inventory={'schema': 'wrong'}))
        self.assertIsNone(snap['inventory'])
        self.assertTrue(any('inventory invalid' in e for e in snap['errors']))

    def test_retired_members_leave_and_unknown_origin_is_static(self):
        records = [record('a0', role='archive-owner'), record('r1'), record('r2'),
                   record('r3', origin='elastic', intent='retired')]
        members = {'a0': observed(role='archive-owner'), 'r1': observed(), 'r2': observed(), 'r4': observed()}
        collector = healthy_collector()
        snap = self.snap(collector, inputs(members=members, records=records))
        self.assertNotIn('r3', snap['members'])
        self.assertEqual(snap['inventory']['total_members'], 4)
        self.assertTrue(snap['members']['r4']['static'])
        self.assertFalse(snap['members']['r4']['in_inventory'])

    def test_draining_member_is_not_serving_but_carries_traffic_when_rendered(self):
        records = [record('a0', role='archive-owner'), record('r1'), record('r2', origin='elastic', intent='draining')]
        snap = self.snap(value=inputs(records=records))
        self.assertFalse(snap['members']['r2']['serving'])
        self.assertIn('r2', snap['load']['members'])

    def test_publisher_lag_tracking(self):
        collector = healthy_collector()
        publisher(collector, t=NOW - 200, public=100, node=101)
        publisher(collector, t=NOW - 3, public=100, node=102)
        snap = self.snap(collector)
        self.assertEqual(snap['publisher']['lag_seconds'], 200.0)
        publisher(collector, t=NOW - 2, public=101, node=102)
        self.assertEqual(self.snap(collector)['publisher']['lag_seconds'], 2.0)  # a new lag
        publisher(collector, t=NOW - 1, public=102, node=102)
        self.assertEqual(self.snap(collector)['publisher']['lag_seconds'], 0.0)
        collector.record_publisher(None, NOW, 'URLError: refused')
        self.assertEqual(self.snap(collector)['publisher']['error'], 'URLError: refused')
        fresh = S.Collector()
        self.assertIsNone(fresh.snapshot(inputs(), POLICY, NOW)['publisher']['serving'])
        publisher(collector, phase='starting')
        self.assertFalse(self.snap(collector)['publisher']['serving'])

    def test_warm_progress_is_tracked_within_one_process(self):
        collector = S.Collector()
        feed(collector, 'r1', warm=lambda i: float(i))
        feed(collector, 'r2')
        publisher(collector)
        snap = self.snap(collector)
        self.assertEqual(snap['members']['r1']['last_progress_unix'], NOW)
        self.assertIsNone(snap['members']['r2']['last_progress_unix'])
        # A restart is not progress.
        restarted = M.Sample(t=NOW + 15, start=2.0, counters=dict(collector.histories['r2'].latest().counters),
                             gauges={'slots': 2.0, 'warm_runtimes': 50.0}, latency=latency(1.0), complete=True)
        collector.record_sample('r2', restarted)
        self.assertNotIn('r2', collector.progress)

    def test_membership_state_advance_is_progress(self):
        collector = healthy_collector()
        members = {'a0': observed(role='archive-owner'), 'r1': observed(), 'r2': observed('booting', routed=False)}
        collector.snapshot(inputs(members=members), POLICY, NOW)
        members['r2'] = observed('warming', routed=False)
        snap = collector.snapshot(inputs(members=members), POLICY, NOW + 1)
        self.assertEqual(snap['members']['r2']['last_progress_unix'], NOW + 1)
        self.assertFalse(snap['members']['r2']['attesting'])


class ScrapeTests(unittest.TestCase):
    def test_scrape_records_samples_failures_and_publisher(self):
        def fetch(url, timeout):
            if 'r2' in url:
                raise OSError('connection refused')
            return M.parse(render_worker())
        collector = S.Collector()
        collector.scrape([('r1', 'http://r1:8093/metrics'), ('r2', 'http://r2:8093/metrics')], NOW, fetch=fetch,
                         publisher_url='http://127.0.0.1:8094/v1/status',
                         fetch_status=lambda url, timeout: {'phase': 'serving', 'public_height': 5, 'node_height': 5})
        self.assertEqual(collector.histories['r1'].latest().counters['queries'], 100)
        self.assertIn('connection refused', collector.histories['r2'].last_error)
        self.assertEqual(collector.publisher['public_height'], 5)

    def test_scrape_survives_garbage(self):
        collector = S.Collector()
        collector.scrape([('r1', 'u')], NOW, fetch=lambda url, timeout: M.parse('a{ 1\n'),
                         publisher_url='p', fetch_status=lambda url, timeout: ['not', 'a', 'dict'])
        self.assertIn('ParseError', collector.histories['r1'].last_error)
        self.assertIsNotNone(collector.publisher_error)


if __name__ == '__main__':
    unittest.main()
