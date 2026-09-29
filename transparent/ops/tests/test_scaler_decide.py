"""decide(): every hold, trigger, cooldown, budget and protection rule."""
import copy
import random
import unittest

from scaler_fixtures import ARCHIVE, D, NOW, elastic, fleet, load, member, policy, run, snapshot


def hot(**overrides):
    """A snapshot that would scale out 3 without any hold (offered 40 qps)."""
    return snapshot(load=load(offered=40.0), **overrides)


def fast(**overrides):
    return policy(scale_out_confirm_seconds=0, **overrides)


class HoldTests(unittest.TestCase):
    def assertHold(self, snap, text, pol=None, state=None):
        decision, new = run(snap, pol or fast(), state)
        self.assertEqual(decision['action'], 'hold', decision)
        self.assertIn(text, decision['reason'])
        self.assertIn(text, ' '.join(new['summary']['holds']))
        self.assertNotIn('decision_id', decision)
        return decision, new

    def test_unheld_hot_snapshot_scales_out(self):
        decision, _ = run(hot(), fast())
        self.assertEqual((decision['action'], decision['count']), ('scale_out', 3))

    def test_input_error(self):
        self.assertHold(hot(errors=['membership: invalid JSON']), 'input: membership: invalid JSON')

    def test_kill_switch(self):
        self.assertHold(hot(disabled=True), 'kill switch')

    def test_paused(self):
        self.assertHold(hot(), 'paused until', pol=fast(paused_until_unix=NOW + 60))
        decision, _ = run(hot(), fast(paused_until_unix=NOW - 1))
        self.assertEqual(decision['action'], 'scale_out')

    def test_maintenance_and_unknown_maintenance(self):
        self.assertHold(hot(maintenance=True), 'fleet maintenance')
        self.assertHold(hot(maintenance=None), 'maintenance state unknown')

    def test_withdrawn_and_unknown_withdrawal(self):
        self.assertHold(hot(withdrawn=True), 'publication withdrawn')
        self.assertHold(hot(withdrawn=None), 'withdrawal state unknown')

    def test_membership_stale_or_unknown(self):
        self.assertHold(hot(membership={'age_seconds': 46.0}), 'membership stale')
        self.assertHold(hot(membership=None), 'membership unknown')
        self.assertHold(hot(membership={'age_seconds': None}), 'membership unknown')
        self.assertHold(hot(membership={'age_seconds': -30.0}), 'membership stale')

    def test_inventory_unknown(self):
        self.assertHold(hot(inventory=None), 'inventory unknown')

    def test_publisher_unknown_stale_not_serving_or_behind(self):
        base = snapshot()['publisher']
        self.assertHold(hot(publisher={**base, 'serving': None, 'error': 'refused'}), 'publisher status unknown')
        self.assertHold(hot(publisher={**base, 'age_seconds': 60.0}), 'publisher status stale')
        self.assertHold(hot(publisher={**base, 'serving': False, 'phase': 'starting'}), 'publisher not serving')
        self.assertHold(hot(publisher={**base, 'lag_seconds': 181.0}), 'behind the node')
        self.assertHold(hot(publisher={**base, 'lag_seconds': None}), 'public publication age unknown')
        decision, _ = run(hot(publisher={**base, 'lag_seconds': 179.0}), fast())
        self.assertEqual(decision['action'], 'scale_out')

    def test_journal_unreadable(self):
        self.assertHold(hot(journal_ok=False), 'actuator journal unreadable')

    def test_open_operation(self):
        operation = {'id': 'op-1', 'action': 'scale_out', 'phase': 'bootstrapping', 'age_seconds': 120.0,
                     'fenced': False, 'deadline_exceeded': False, 'decision_id': None}
        self.assertHold(hot(operation=operation), 'actuator operation open: op-1')

    def test_fenced_operation_holds_and_flags(self):
        operation = {'id': 'op-1', 'action': 'scale_out', 'phase': 'applying', 'age_seconds': 500.0,
                     'fenced': True, 'deadline_exceeded': True, 'decision_id': None}
        decision, _ = self.assertHold(hot(operation=operation), 'actuator operation fenced')
        self.assertTrue(any('fenced' in f for f in decision['flags']))
        self.assertTrue(any('deadline' in f for f in decision['flags']))

    def test_request_awaiting_actuator(self):
        _, state = run(hot(), fast())
        self.assertHold(hot(), 'awaiting the actuator', state=state)

    def test_stale_serving_member(self):
        members = fleet()
        members['transparent-pir-recent-02']['sample_age_seconds'] = 46.0
        self.assertHold(hot(members=members), 'stale signal: transparent-pir-recent-02 (metrics')
        members = fleet()
        members['transparent-pir-recent-02']['window'] = None
        members['transparent-pir-recent-02']['metrics_error'] = 'incomplete metrics'
        self.assertHold(hot(members=members), 'stale signal: transparent-pir-recent-02 (metrics: incomplete')
        members = fleet()
        members['transparent-pir-recent-01']['observed_age_seconds'] = None
        self.assertHold(hot(members=members), 'stale signal: transparent-pir-recent-01 (membership')
        members = fleet()
        members['transparent-pir-recent-01']['sample_age_seconds'] = None
        self.assertHold(hot(members=members), 'stale signal: transparent-pir-recent-01')

    def test_stale_non_serving_member_does_not_hold(self):
        members = fleet(5, serving=False, attesting=False)
        key = 'transparent-pir-recent-05'
        members[key]['sample_age_seconds'] = None
        members[key]['window'] = None
        decision, _ = run(snapshot(members=members, load=load(offered=21.4)), fast())
        self.assertEqual(decision['action'], 'scale_out')
        self.assertEqual(decision['count'], 2)  # 5 wanted; the booting member counts as pending

    def test_load_unknown_holds_scaling(self):
        snap = snapshot(load={**load(), 'offered_qps': None, 'unknown': ['transparent-pir-recent-01']})
        self.assertHold(snap, 'load unknown: transparent-pir-recent-01')

    def test_invalid_policy(self):
        for bad in (fast(min_recent=1), fast(mode='yolo'), fast(schema=2), None, fast(headroom=0.5),
                    fast(max_recent=40), fast(capacity_qps_per_replica=0), fast(backstop={'p99_seconds': 'x'}),
                    fast(prices_usd_monthly={'s-4vcpu-8gb': -1})):
            decision, state = D.decide(hot(), bad, D.initial_state(), NOW)
            self.assertEqual(decision['action'], 'hold')
            self.assertIn('policy invalid', decision['reason'])


class ScaleOutTests(unittest.TestCase):
    def test_capacity_model_deficit_and_max_step(self):
        # 21.4 qps × 1.5 / 8 = 4.01 → 5 wanted, 2 serving → +3.
        decision, state = run(snapshot(load=load(offered=21.4)), fast())
        self.assertEqual((decision['action'], decision['count']), ('scale_out', 3))
        self.assertEqual(state['summary']['desired_recent'], 5)
        # 40 qps wants 8 → clamped to max_recent 6; the step is capped at 3.
        decision, state = run(snapshot(load=load(offered=40.0)), fast())
        self.assertEqual(decision['count'], 3)
        self.assertEqual(state['summary']['desired_recent'], 6)
        decision, _ = run(snapshot(load=load(offered=12.0)), fast())
        self.assertEqual(decision['count'], 1)  # 12 × 1.5 / 8 = 2.25 → 3

    def test_recent_rate_sizes_a_rising_load(self):
        decision, _ = run(snapshot(load=load(offered=6.0, recent=21.0)), fast())
        self.assertEqual(decision['count'], 2)  # max(6, 21) × 1.5 / 8 = 3.9 → 4

    def test_confirmation_period(self):
        pol = policy()
        decision, state = run(hot(), pol)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('confirming', decision['reason'])
        decision, state = run(hot(), pol, state, NOW + 30)
        self.assertEqual(decision['action'], 'hold')
        decision, state = run(hot(), pol, state, NOW + 60)
        self.assertEqual(decision['action'], 'scale_out')
        # A dip resets the confirmation.
        _, state = run(hot(), pol)
        _, state = run(snapshot(), pol, state, NOW + 30)
        decision, _ = run(hot(), pol, state, NOW + 60)
        self.assertEqual(decision['action'], 'hold')

    def test_pending_members_count_toward_current(self):
        members = fleet(5, serving=False, attesting=False, progress=NOW - 10)
        decision, _ = run(snapshot(members=members, load=load(offered=21.4)), fast())
        self.assertEqual(decision['count'], 2)

    def test_error_backstop_adds_one_and_flags_drift(self):
        snap = snapshot(load=load(offered=4.0, error_ratio=0.1))
        decision, state = run(snap, fast())
        self.assertEqual(decision['action'], 'hold')
        decision, state = run(snap, fast(), state, NOW + 119)
        self.assertEqual(decision['action'], 'hold')
        decision, state = run(snap, fast(), state, NOW + 120)
        self.assertEqual((decision['action'], decision['count']), ('scale_out', 1))
        self.assertTrue(any('capacity model drift' in f for f in decision['flags']))

    def test_p99_backstop_adds_one_and_flags_drift(self):
        snap = snapshot(load=load(offered=4.0, p99=2.0))
        _, state = run(snap, fast())
        decision, state = run(snap, fast(), state, NOW + 299)
        self.assertEqual(decision['action'], 'hold')
        decision, _ = run(snap, fast(), state, NOW + 300)
        self.assertEqual((decision['action'], decision['count']), ('scale_out', 1))
        self.assertIn('backstop: p99', decision['reason'])
        self.assertTrue(any('capacity model drift' in f for f in decision['flags']))

    def test_backstop_breach_resets_when_unknown(self):
        snap = snapshot(load=load(offered=4.0, p99=2.0))
        _, state = run(snap, fast())
        _, state = run(snapshot(load={**load(), 'offered_qps': None}), fast(), state, NOW + 200)
        decision, _ = run(snap, fast(), state, NOW + 300)
        self.assertEqual(decision['action'], 'hold')

    def test_backstop_with_model_deficit_does_not_flag_drift(self):
        snap = snapshot(load=load(offered=21.4, p99=2.0))
        _, state = run(snap, policy())
        decision, _ = run(snap, policy(), state, NOW + 300)
        self.assertEqual(decision['count'], 3)
        self.assertIn('backstop: p99', decision['reason'])
        self.assertFalse(any('drift' in f for f in decision['flags']))

    def test_cooldown_after_scale_out_completion(self):
        state = D.initial_state()
        state['last_scale_out_complete_unix'] = NOW - 899
        decision, _ = run(hot(), fast(), state)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('scale-out cooldown', decision['reason'])
        state['last_scale_out_complete_unix'] = NOW - 900
        decision, _ = run(hot(), fast(), state)
        self.assertEqual(decision['action'], 'scale_out')

    def test_max_recent(self):
        members = fleet(3, 4, 5, 6)
        decision, _ = run(hot(members=members), fast())
        self.assertIn('steady: desired 6, serving 6', decision['reason'])
        slow = snapshot(members=members, load=load(offered=40.0, p99=2.0))
        _, state = run(slow, fast())
        decision, _ = run(slow, fast(), state, NOW + 300)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('at max_recent 6', decision['reason'])
        self.assertTrue(any('capacity ceiling' in f for f in decision['flags']))
        members = fleet(3, 4, 5)
        decision, _ = run(hot(members=members), fast())
        self.assertEqual(decision['count'], 1)

    def test_cost_cap_limits_and_refuses(self):
        # Two static recent replicas cost 96; a 150 cap leaves room for one more.
        decision, state = run(hot(), fast(monthly_cost_cap_usd=150))
        self.assertEqual(decision['count'], 1)
        self.assertEqual(state['summary']['budget']['monthly_cost_usd'], 96.0)
        decision, _ = run(hot(), fast(monthly_cost_cap_usd=100))
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('monthly cost cap', decision['reason'])
        self.assertTrue(any('monthly cost cap' in f for f in decision['flags']))

    def test_cost_counts_every_non_retired_recent_member_by_size(self):
        members = fleet(5)
        members['transparent-pir-recent-05']['size'] = 's-8vcpu-16gb'
        members['transparent-pir-recent-09'] = member(origin='elastic', intent='quarantined', serving=False,
                                                      size='s-4vcpu-8gb')
        pol = fast(prices_usd_monthly={'s-4vcpu-8gb': 48.0, 's-8vcpu-16gb': 96.0})
        _, state = run(hot(members=members), pol)
        self.assertEqual(state['summary']['budget']['monthly_cost_usd'], 48 * 2 + 96 + 48)

    def test_unknown_price_refuses_additions(self):
        members = fleet(5)
        members['transparent-pir-recent-05']['size'] = 'gpu-h100'
        decision, _ = run(hot(members=members), fast())
        self.assertEqual(decision['action'], 'hold')
        self.assertTrue(any('price unknown' in f for f in decision['flags']))

    def test_daily_action_budget_is_rolling(self):
        state = D.initial_state()
        state['actions'] = [{'unix': NOW - 86399 + i, 'action': 'scale_out', 'count': 1, 'member': None,
                             'decision_id': str(i), 'destroy': False} for i in range(6)]
        decision, new = run(hot(), fast(), state)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('daily action budget exhausted', decision['reason'])
        self.assertEqual(new['summary']['budget']['actions_left'], 0)
        decision, _ = run(hot(), fast(), state, NOW + 2)
        self.assertEqual(decision['action'], 'scale_out')  # the oldest action left the window

    def test_inventory_member_limit(self):
        decision, _ = run(hot(inventory={'revision': 9, 'total_members': 32}), fast())
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('inventory would exceed 32', decision['reason'])
        decision, _ = run(hot(inventory={'revision': 9, 'total_members': 31}), fast())
        self.assertEqual(decision['count'], 1)

    def test_non_improvement_holds_further_scale_outs(self):
        state = D.initial_state()
        state['pending_eval'] = {'complete_unix': NOW - 600, 'baseline': {'p99': 2.0, 'error_ratio': 0.0}}
        state['last_scale_out_complete_unix'] = NOW - 600
        bad = snapshot(load=load(offered=21.4, p99=2.2))
        decision, state = run(bad, fast(), state)
        self.assertIsNotNone(state['pending_eval'])
        decision, state = run(bad, fast(), state, NOW + 300)
        self.assertIsNone(state['pending_eval'])
        self.assertIsNotNone(state['no_improvement_until'])
        self.assertTrue(any('did not improve' in f for f in decision['flags']))
        decision, state = run(bad, fast(), state, NOW + 1000)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('did not improve', decision['reason'])
        # Recovery below the backstop clears the hold.
        decision, state = run(snapshot(load=load(offered=21.4, p99=0.5)), fast(), state, NOW + 1100)
        self.assertIsNone(state['no_improvement_until'])
        self.assertEqual(decision['action'], 'scale_out')

    def test_demand_that_outgrew_the_scale_out_is_not_judged(self):
        state = D.initial_state()
        state['pending_eval'] = {'complete_unix': NOW - 900,
                                 'baseline': {'p99': 2.0, 'error_ratio': 0.0, 'per_replica_qps': 4.0}}
        state['last_scale_out_complete_unix'] = NOW - 900
        # 21.4 qps over 2 serving is 10.7 per replica, more than the 4.0 before.
        decision, state = run(snapshot(load=load(offered=21.4, p99=2.2)), fast(), state)
        self.assertIsNone(state['pending_eval'])
        self.assertIsNone(state['no_improvement_until'])
        self.assertEqual(decision['action'], 'scale_out')
        state = D.initial_state()
        state['pending_eval'] = {'complete_unix': NOW - 900,
                                 'baseline': {'p99': 2.0, 'error_ratio': 0.0, 'per_replica_qps': 20.0}}
        _, state = run(snapshot(load=load(offered=21.4, p99=2.2)), fast(), state)
        self.assertIsNotNone(state['no_improvement_until'])

    def test_improvement_clears_the_evaluation(self):
        state = D.initial_state()
        state['pending_eval'] = {'complete_unix': NOW - 60, 'baseline': {'p99': 2.0, 'error_ratio': 0.0}}
        _, state = run(snapshot(load=load(p99=1.6)), fast(), state)
        self.assertIsNone(state['pending_eval'])
        self.assertIsNone(state['no_improvement_until'])


class ScaleInTests(unittest.TestCase):
    def state(self, below=3600):
        state = D.initial_state()
        state['below_since'] = NOW - below
        return state

    def test_victim_is_highest_ordinal_elastic(self):
        members = fleet(5, 10, 7)
        decision, _ = run(snapshot(members=members, load=load(offered=1.0)), fast(), self.state())
        self.assertEqual((decision['action'], decision['member']), ('scale_in', 'transparent-pir-recent-10'))

    def test_one_replica_at_a_time_then_cooldown(self):
        members = fleet(5, 6)
        decision, state = run(snapshot(members=members, load=load(offered=1.0)), fast(), self.state())
        self.assertEqual(decision['member'], 'transparent-pir-recent-06')
        self.assertNotIn('count', decision)
        state['request'] = None  # consumed and completed
        state['last_complete_unix'] = NOW + 600
        members.pop('transparent-pir-recent-06')
        state['below_since'] = NOW - 7200
        decision, _ = run(snapshot(members=members, load=load(offered=1.0)), fast(), state, NOW + 3000)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('cooldown after last action', decision['reason'])
        decision, _ = run(snapshot(members=members, load=load(offered=1.0)), fast(), state, NOW + 4200)
        self.assertEqual(decision['member'], 'transparent-pir-recent-05')

    def test_static_members_are_never_removed(self):
        members = fleet()
        members['transparent-pir-recent-03'] = member()
        members['transparent-pir-recent-04'] = member()
        decision, _ = run(snapshot(members=members, load=load(offered=0.5)), fast(), self.state())
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('no elastic member to remove', decision['reason'])

    def test_never_below_two_serving(self):
        members = fleet(5)
        decision, _ = run(snapshot(members=members, load=load(offered=0.0)), fast(), self.state())
        self.assertEqual(decision['member'], 'transparent-pir-recent-05')
        members = fleet(5)
        members['transparent-pir-recent-02'] = member(serving=False, attesting=False, progress=NOW)
        decision, _ = run(snapshot(members=members, load=load(offered=0.0)), fast(), self.state())
        self.assertEqual(decision['action'], 'hold')  # desired 2 = serving 2, and a member warms

    def test_hold_time(self):
        members = fleet(5)
        decision, _ = run(snapshot(members=members, load=load(offered=1.0)), fast(), self.state(below=3599))
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('scale-in: desired 2 < 3 for 3599', decision['reason'])
        _, state = run(snapshot(members=members, load=load(offered=1.0)), fast())
        self.assertEqual(state['below_since'], NOW)
        _, state = run(snapshot(members=members, load=load(offered=30.0)), fast(), state, NOW + 60)
        self.assertIsNone(state['below_since'])

    def test_rejections_and_p99_block(self):
        members = fleet(5)
        decision, _ = run(snapshot(members=members, load=load(offered=1.0, rejections=1.0)), fast(), self.state())
        self.assertIn('rejections in window: 1', decision['reason'])
        decision, _ = run(snapshot(members=members, load=load(offered=1.0, p99=0.8)), fast(), self.state())
        self.assertIn('p99 0.80 s >= 0.8 s', decision['reason'])
        idle = load(offered=0.0, p99=None, latency_count=0.0)
        decision, _ = run(snapshot(members=members, load=idle), fast(), self.state())
        self.assertEqual(decision['action'], 'scale_in')  # no queries is not an unknown p99
        unknown = load(offered=0.0, p99=None, latency_count=None)
        decision, _ = run(snapshot(members=members, load=unknown), fast(), self.state())
        self.assertIn('p99 unknown', decision['reason'])

    def test_warming_booting_draining_or_failed_blocks(self):
        for extra in ({'serving': False, 'attesting': False, 'progress': NOW},
                      {'intent': 'draining', 'serving': False, 'attesting': True}):
            members = fleet(5, 6)
            members['transparent-pir-recent-07'] = member(origin='elastic', size='s-4vcpu-8gb', **extra)
            decision, _ = run(snapshot(members=members, load=load(offered=1.0)), fast(), self.state())
            self.assertEqual(decision['action'], 'hold', extra)
            self.assertIn('members warming, booting, draining or failed: transparent-pir-recent-07',
                          decision['reason'])

    def test_destroy_budget(self):
        state = self.state()
        state['actions'] = [{'unix': NOW - 100 * i - 4000, 'action': 'scale_in', 'count': None,
                             'member': f'x{i}', 'decision_id': str(i), 'destroy': True} for i in range(2)]
        decision, new = run(snapshot(members=fleet(5), load=load(offered=1.0)), fast(), state)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('daily destroy budget exhausted', decision['reason'])
        self.assertEqual(new['summary']['budget']['destroys_left'], 0)

    def test_a_refused_request_spends_no_budget(self):
        # 2026-09-29: a refused replace, retried, was charged twice and left
        # the destroy budget at -1.
        state = self.state()
        state['actions'] = [{'unix': NOW - 100 * i - 4000, 'action': 'replace', 'count': None,
                             'member': 'x', 'decision_id': str(i), 'destroy': True} for i in range(2)]
        decision, new = run(snapshot(members=fleet(5), load=load(offered=1.0),
                                     refused_decision_ids=['0']), fast(), state)
        # Only the retried request counts, so this scale-in may still destroy.
        self.assertEqual(decision['action'], 'scale_in')
        self.assertEqual(new['summary']['budget']['destroys_left'], 0)
        self.assertEqual(new['summary']['budget']['actions_left'], 4)


class ReplaceTests(unittest.TestCase):
    def failing(self, member_id, since=900, members=None):
        state = D.initial_state()
        state['members'][member_id] = {'unhealthy_since': NOW - since, 'first_seen_unix': NOW - 5000}
        return state

    def test_failed_elastic_member_is_replaced(self):
        members = fleet(5)
        members['transparent-pir-recent-05'] = elastic(5, serving=False, attesting=False)[1]
        state = self.failing('transparent-pir-recent-05')
        decision, new = run(snapshot(members=members), fast(), state)
        self.assertEqual((decision['action'], decision['member']), ('replace', 'transparent-pir-recent-05'))
        self.assertEqual(new['actions'][-1]['destroy'], True)
        self.assertEqual(new['awaiting_operator'], [])

    def test_not_failed_before_the_threshold_or_with_progress(self):
        members = fleet(5)
        members['transparent-pir-recent-05'] = elastic(5, serving=False, attesting=False)[1]
        decision, _ = run(snapshot(members=members), fast(), self.failing('transparent-pir-recent-05', 899))
        self.assertEqual(decision['action'], 'hold')
        members['transparent-pir-recent-05']['last_progress_unix'] = NOW - 100
        decision, _ = run(snapshot(members=members), fast(), self.failing('transparent-pir-recent-05', 5000))
        self.assertEqual(decision['action'], 'hold')

    def test_failed_static_member_is_left_to_the_operator(self):
        members = fleet()
        members['transparent-pir-recent-02'] = member(serving=False, attesting=False)
        state = self.failing('transparent-pir-recent-02')
        decision, new = run(snapshot(members=members), fast(), state)
        self.assertEqual((decision['action'], decision['member']), ('replace', 'transparent-pir-recent-02'))
        self.assertFalse(new['actions'][-1]['destroy'])
        self.assertEqual(new['awaiting_operator'], ['transparent-pir-recent-02'])
        # It stays listed while quarantined, until an operator retires it.
        members['transparent-pir-recent-02']['intent'] = 'quarantined'
        new['request'] = None
        _, later = run(snapshot(members=members), fast(), new, NOW + 60)
        self.assertEqual(later['awaiting_operator'], ['transparent-pir-recent-02'])
        members.pop('transparent-pir-recent-02')
        _, later = run(snapshot(members=members), fast(), later, NOW + 120)
        self.assertEqual(later['awaiting_operator'], [])

    def test_observe_mode_does_not_list_awaiting_operator(self):
        members = fleet()
        members['transparent-pir-recent-02'] = member(serving=False, attesting=False)
        decision, new = run(snapshot(members=members), policy(mode='observe'),
                            self.failing('transparent-pir-recent-02'))
        self.assertEqual(decision['action'], 'replace')
        self.assertEqual(new['awaiting_operator'], [])
        self.assertEqual(new['actions'], [])

    def test_correlated_failures_hold_and_flag(self):
        members = fleet(5)
        members['transparent-pir-recent-05'] = elastic(5, serving=False, attesting=False)[1]
        members['transparent-pir-recent-02'] = member(serving=False, attesting=False, progress=NOW)
        state = self.failing('transparent-pir-recent-05')
        state['members']['transparent-pir-recent-02'] = {'unhealthy_since': NOW - 60, 'first_seen_unix': 0}
        decision, _ = run(snapshot(members=members), fast(), state)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('correlated failures', decision['reason'])
        self.assertTrue(any('correlated failures' in f for f in decision['flags']))

    def test_archive_unhealthy_counts_as_correlated_and_is_never_named(self):
        members = fleet()
        members['transparent-pir-recent-02'] = member(serving=False, attesting=False)
        members[ARCHIVE[0]] = member(role='archive-owner', serving=False, attesting=False)
        state = self.failing('transparent-pir-recent-02')
        state['members'][ARCHIVE[0]] = {'unhealthy_since': NOW - 5000, 'first_seen_unix': 0}
        decision, _ = run(snapshot(members=members), fast(), state)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('correlated failures', decision['reason'])
        # An archive owner alone is never a replacement candidate.
        members = fleet()
        members[ARCHIVE[1]] = member(role='archive-owner', serving=False, attesting=False)
        state = self.failing(ARCHIVE[1], since=10_000)
        decision, _ = run(snapshot(members=members), fast(), state)
        self.assertNotEqual(decision['action'], 'replace')

    def test_replace_respects_budgets_and_cost(self):
        members = fleet(5)
        members['transparent-pir-recent-05'] = elastic(5, serving=False, attesting=False)[1]
        state = self.failing('transparent-pir-recent-05')
        state['actions'] = [{'unix': NOW - 10 - i, 'action': 'scale_in', 'member': 'x', 'count': None,
                             'decision_id': str(i), 'destroy': True} for i in range(2)]
        decision, _ = run(snapshot(members=members), fast(), state)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('daily destroy budget exhausted', decision['reason'])
        decision, _ = run(snapshot(members=members), fast(monthly_cost_cap_usd=144),
                          self.failing('transparent-pir-recent-05'))
        self.assertIn('monthly cost cap', decision['reason'])

    def test_replacement_happens_with_unknown_load(self):
        members = fleet()
        members['transparent-pir-recent-02'] = member(serving=False, attesting=False)
        snap = snapshot(members=members, load={**load(), 'offered_qps': None})
        decision, _ = run(snap, fast(), self.failing('transparent-pir-recent-02'))
        self.assertEqual(decision['action'], 'replace')

    def test_unhealthy_tracking_needs_a_fresh_membership(self):
        members = fleet()
        members['transparent-pir-recent-02'] = member(serving=False, attesting=False)
        _, state = run(snapshot(members=members), fast())
        self.assertEqual(state['members']['transparent-pir-recent-02']['unhealthy_since'], NOW)
        members['transparent-pir-recent-02'] = member()
        _, state2 = run(snapshot(members=members, membership={'age_seconds': 100}), fast(), state, NOW + 60)
        self.assertEqual(state2['members']['transparent-pir-recent-02']['unhealthy_since'], NOW)
        _, state3 = run(snapshot(members=members), fast(), state2, NOW + 120)
        self.assertIsNone(state3['members']['transparent-pir-recent-02']['unhealthy_since'])


class ModeAndRequestTests(unittest.TestCase):
    def test_observe_and_recommend_record_nothing(self):
        for mode in ('observe', 'recommend'):
            decision, state = run(hot(), policy(mode=mode, scale_out_confirm_seconds=0))
            self.assertEqual(decision['action'], 'scale_out')
            self.assertNotIn('decision_id', decision)
            self.assertEqual(state['actions'], [])
            self.assertIsNone(state['request'])
            self.assertEqual(state['summary']['mode'], mode)

    def test_acting_modes_record_the_request(self):
        for mode in ('act-dry', 'act'):
            decision, state = run(hot(), policy(mode=mode, scale_out_confirm_seconds=0))
            self.assertRegex(decision['decision_id'], r'^[0-9a-f-]{36}$')
            self.assertEqual(state['request']['decision_id'], decision['decision_id'])
            self.assertEqual(state['actions'][-1]['decision_id'], decision['decision_id'])
            self.assertEqual(state['summary']['budget']['actions_left'], 5)

    def test_decision_ids_are_fresh(self):
        seen = set()
        state = D.initial_state('a' * 32)
        for i in range(5):
            decision, state = run(hot(), fast(daily_actions=100), state, NOW + 10_000 * i)
            state['request'] = None
            state['last_scale_out_complete_unix'] = None
            seen.add(decision['decision_id'])
        self.assertEqual(len(seen), 5)
        other, _ = run(hot(), fast(), D.initial_state('b' * 32))
        self.assertNotIn(other['decision_id'], seen)

    def test_request_lifecycle_through_the_journal(self):
        decision, state = run(hot(), fast())
        did = decision['decision_id']
        op = {'id': 'op-9', 'action': 'scale_out', 'phase': 'applying', 'age_seconds': 5.0, 'fenced': False,
              'deadline_exceeded': False, 'decision_id': did}
        decision, state = run(hot(operation=op), fast(), state, NOW + 20)
        self.assertTrue(state['request']['consumed'])
        self.assertIn('actuator operation open', decision['reason'])
        decision, state = run(hot(), fast(), state, NOW + 600)
        self.assertIsNone(state['request'])
        self.assertEqual(state['last_scale_out_complete_unix'], NOW + 600)
        self.assertIn('scale-out cooldown', decision['reason'])
        self.assertIsNone(state['pending_eval'])  # nothing breached: improved at once
        _, breached = run(hot(), fast())
        breached['request'] = {**breached['request'], 'consumed': True,
                               'baseline': {'p99': 2.0, 'error_ratio': 0.0}}
        _, breached = run(snapshot(load=load(offered=40.0, p99=2.0)), fast(), breached, NOW + 600)
        self.assertEqual(breached['pending_eval']['baseline']['p99'], 2.0)

    def test_completion_seen_in_done(self):
        decision, state = run(hot(), fast())
        _, state = run(hot(done_decision_ids=[decision['decision_id']]), fast(), state, NOW + 60)
        self.assertIsNone(state['request'])
        self.assertEqual(state['last_complete_unix'], NOW + 60)

    def test_unconsumed_request_expires_with_a_flag(self):
        first, state = run(hot(), fast())
        decision, state = run(hot(), fast(), state, NOW + 599)
        self.assertEqual(decision['action'], 'hold')
        decision, state = run(hot(), fast(), state, NOW + 601)
        self.assertTrue(any(f"request {first['decision_id']} not consumed" in f for f in decision['flags']))
        # The budget stays spent: an unconsumed request is never refunded.
        self.assertEqual(len(state['actions']), 2)
        self.assertNotEqual(state['request']['decision_id'], first['decision_id'])

    def test_operation_without_decision_id_counts_as_consumption(self):
        _, state = run(hot(), fast())
        op = {'id': 'op-9', 'action': 'scale_out', 'phase': 'planned', 'age_seconds': 10.0, 'fenced': False,
              'deadline_exceeded': False, 'decision_id': None}
        _, state = run(hot(operation=op), fast(), state, NOW + 30)
        self.assertTrue(state['request']['consumed'])
        old = {**op, 'age_seconds': 500.0}
        _, fresh = run(hot(), fast())
        _, fresh = run(hot(operation=old), fast(), fresh, NOW + 30)
        self.assertFalse(fresh['request']['consumed'])


class SingleArchiveOwnerTests(unittest.TestCase):
    """One archive owner holding the whole archive: the scaler acts on the
    recent tier exactly as with two, and the lone owner is still never named."""

    def test_scale_out_in_and_replace(self):
        decision, _ = run(snapshot(archive_owners=1, load=load(offered=40.0)), fast())
        self.assertEqual((decision['action'], decision['count']), ('scale_out', 3))
        state = D.initial_state()
        state['below_since'] = NOW - 3600
        members = fleet(5, 10, archive_owners=1)
        self.assertEqual([m for m in members if m in ARCHIVE], [ARCHIVE[0]])
        decision, _ = run(snapshot(members=members, load=load(offered=1.0)), fast(), state)
        self.assertEqual((decision['action'], decision['member']), ('scale_in', 'transparent-pir-recent-10'))
        members = fleet(5, archive_owners=1)
        members['transparent-pir-recent-05'] = elastic(5, serving=False, attesting=False)[1]
        state = D.initial_state()
        state['members']['transparent-pir-recent-05'] = {'unhealthy_since': NOW - 900, 'first_seen_unix': NOW - 5000}
        decision, _ = run(snapshot(members=members), fast(), state)
        self.assertEqual((decision['action'], decision['member']), ('replace', 'transparent-pir-recent-05'))

    def test_the_lone_owner_unhealthy_holds_and_is_never_named(self):
        members = fleet(archive_owners=1)
        members['transparent-pir-recent-02'] = member(serving=False, attesting=False)
        members[ARCHIVE[0]] = member(role='archive-owner', serving=False, attesting=False)
        state = D.initial_state()
        for m in ('transparent-pir-recent-02', ARCHIVE[0]):
            state['members'][m] = {'unhealthy_since': NOW - 5000, 'first_seen_unix': 0}
        decision, _ = run(snapshot(members=members), fast(), state)
        self.assertEqual(decision['action'], 'hold')
        self.assertIn('correlated failures', decision['reason'])
        members = fleet(archive_owners=1)
        members[ARCHIVE[0]] = member(role='archive-owner', serving=False, attesting=False)
        state = D.initial_state()
        state['members'][ARCHIVE[0]] = {'unhealthy_since': NOW - 10_000, 'first_seen_unix': NOW - 5000}
        decision, _ = run(snapshot(members=members), fast(), state)
        self.assertNotEqual(decision['action'], 'replace')


class PropertyTests(unittest.TestCase):
    def test_random_snapshots_never_name_archive_or_static_victims(self):
        for owners in (2, 1):
            with self.subTest(archive_owners=owners):
                self.random_snapshots(owners)

    def random_snapshots(self, owners):
        rng = random.Random(7)
        for _ in range(2000):
            members = {}
            for a in ARCHIVE[:owners]:
                members[a] = member(role='archive-owner', serving=rng.random() > 0.2)
            for n in range(1, rng.randint(2, 4) + 1):
                ok = rng.random() > 0.2
                members[f'transparent-pir-recent-{n:02d}'] = member(serving=ok, attesting=ok)
            for n in range(5, 5 + rng.randint(0, 5)):
                ok = rng.random() > 0.2
                members[f'transparent-pir-recent-{n:02d}'] = member(
                    origin='elastic', size='s-4vcpu-8gb', serving=ok, attesting=ok,
                    intent=rng.choice(['enrolled', 'enrolled', 'draining']))
            state = D.initial_state()
            state['below_since'] = NOW - rng.choice([0, 4000])
            for m in members:
                if rng.random() < 0.3:
                    state['members'][m] = {'unhealthy_since': NOW - rng.choice([10, 1000]), 'first_seen_unix': 0}
            snap = snapshot(members=members, load=load(offered=rng.choice([0.0, 3.0, 20.0, 60.0]),
                                                     p99=rng.choice([0.2, 1.0, 2.0])))
            decision, new = run(snap, fast(), state)
            if decision['action'] in ('scale_in', 'replace'):
                target = members[decision['member']]
                self.assertEqual(target['role'], 'recent-replica')
                self.assertNotIn(decision['member'], ARCHIVE)
            if decision['action'] == 'scale_in':
                self.assertFalse(target['static'])
                serving = sum(1 for v in members.values() if v['role'] == 'recent-replica' and v['serving'])
                self.assertGreaterEqual(serving - 1, 2)
            if decision['action'] == 'scale_out':
                self.assertLessEqual(new['summary']['budget']['monthly_cost_usd'] + 48 * decision['count'], 400)

    def test_decide_is_pure(self):
        snap = hot()
        state = D.initial_state()
        before = (copy.deepcopy(snap), copy.deepcopy(state))
        first = run(snap, fast(), state)
        second = run(snap, fast(), state)
        self.assertEqual(first, second)
        self.assertEqual((snap, state), before)


if __name__ == '__main__':
    unittest.main()
