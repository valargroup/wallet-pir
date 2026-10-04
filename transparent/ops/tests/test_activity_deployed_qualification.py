"""Fail-closed deployed qualification checks. Fixtures here never qualify anything.

Evaluators run against synthetic raw receipts in the native output formats;
owner lifecycle tests use real child processes, locks, signals and the shared
fence. Unit effects run against an in-memory systemd model. Live SSH, systemd,
HTTPS and the candidate executables are separate gates.
"""
import copy
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'ops/lib'))
from wallet_pir_ops import durable, schema_fence  # noqa: E402
from wallet_pir_ops.deploy import cli  # noqa: E402
SPEC = importlib.util.spec_from_file_location('deployed_qualification', ROOT/'transparent/ops/lib/activity_deployed_qualification.py')
Q = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(Q)

FIXTURE = 'f'*64


def sample(classes=None):
    classes = classes or list(Q.COMPOSITION[40])
    return {'genesis_hash':'0'*64, 'start_height':0, 'cutoff_height':10, 'anchor_height':20, 'anchor_hash':'1'*64,
            'clients':[{'class':c, 'scripts':['76a9'+'%02x' % i], 'required_from':0, 'expected_digest':'2'*64,
                        'journal_events':1} for i, c in enumerate(classes)]}


def request(kind='staged-load', **extra):
    value = {'version':1, 'kind':kind, 'source_sha':'a'*40, 'attempt':1,
             'transaction':'transparent-schema-1', 'recipe_sha256':'b'*64}
    value.update(extra)
    return value


def capacity_request(level=8, trial=1):
    return request('capacity', level=level, trial=trial, sample=sample())


def rate_lines(qps, seconds, processes=5, *, http=.2, recent=8, drop=0, error_then_success=0, fixture=FIXTURE):
    result = []
    for index in range(processes):
        events = [{'event':'ready', 'fixture_sha256':fixture, 'targets':[3, 3, 1, 1]}]
        for k in range(int(qps/processes*seconds)):
            geometry = 'recent-4k-8k' if k % 10 < recent else 'archive-wide'
            if index == 0 and k < error_then_success:
                events.append({'event':'error', 'logical_id':k, 'attempt':1, 'retry_scheduled':True})
            if index == 0 and k < drop:
                events.append({'event':'error', 'logical_id':k, 'attempt':1, 'retry_scheduled':False})
                continue
            events.append({'event':'query', 'logical_id':k, 'exact':True, 'geometry':geometry, 'http_seconds':http})
        result.append(events)
    return result


class Clock:
    """A controllable monotonic clock; sleeping advances it."""
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += max(0, seconds)


class ValidationTests(unittest.TestCase):
    def test_every_kind_accepts_only_closed_fields(self):
        for value in (request(), request('freshness'), capacity_request(),
                      request('fault', fault='router-restart'), request('fault', fault='client-reopen'),
                      request('fault', fault='recent-worker-loss', target='recent-01'),
                      request('fault', fault='rollback-redeploy', rolled_back_transaction='transparent-schema-0')):
            self.assertEqual(Q.validate(copy.deepcopy(value)), value)
        injected = [request(url='https://example.invalid'), request(argv=['/bin/sh']), request(unit='ssh.service'),
                    request(seconds=1), request('fault', fault='router-restart', target='router'),
                    request('fault', fault='recent-worker-loss'), request('fault', fault='signal-worker'),
                    request('fault', fault='rollback-redeploy', rolled_back_transaction='transparent-schema-1'),
                    request('capacity', level=10, trial=1, sample=sample()), request('capacity', level=8, trial=7, sample=sample()),
                    request('capacity', level=20, trial=1, sample=sample(list(Q.COMPOSITION[8]))),
                    dict(request(), version=True), dict(request(), source_sha='A'*40), request('shell')]
        for value in injected:
            with self.assertRaises(ValueError, msg=value):
                Q.validate(value)

    def test_sample_scripts_and_identities_are_bounded(self):
        bad = sample()
        bad['clients'][0]['scripts'] = ['not hex']
        with self.assertRaises(ValueError):
            Q.validate(request('capacity', level=8, trial=1, sample=bad))

    def test_plan_is_deterministic_and_binds_fixed_activity(self):
        value = capacity_request()
        first, second = Q.plan(value), Q.plan(copy.deepcopy(value))
        self.assertEqual(durable.digest(first), durable.digest(second))
        self.assertNotIn('sample', first['request'])
        self.assertEqual(first['request']['sample_sha256'], durable.digest(value['sample']))
        changed = copy.deepcopy(value); changed['sample']['clients'][0]['journal_events'] = 2
        self.assertNotEqual(durable.digest(Q.plan(changed)), durable.digest(first))
        self.assertEqual(first['activity']['composition'], Q.COMPOSITION[8])
        self.assertTrue(any('heavy continuation' in m for m in first['missing_assurance']))
        load = Q.plan(request())
        self.assertEqual([(s['qps'], s['seconds']) for s in load['activity']['stages']], [(5, 120), (20, 600)])
        self.assertEqual(load['activity']['origin'], 'https://transparent-pir.valargroup.dev')
        self.assertEqual(Q.plan(request('fault', fault='rollback-redeploy', rolled_back_transaction='transparent-schema-0'))
                         ['activity']['rollback_budget'], {'withdraw-origins':60, 'restore-v10':140, 'verify-rollback':300,
                                                           'reopen-v10':100, 'verify-service':140})
        self.assertEqual(sum(Q.ROLLBACK_BUDGET.values()), 740)
        self.assertIn('RuntimeMaxSec=2400', load['properties'])
        self.assertIn('TimeoutStopSec=90', load['properties'])
        self.assertEqual(load['bounds']['owner_margin_seconds'], Q.OWNER_MARGIN)
        interruption = Q.plan(request('fault', fault='publication-interruption'))
        self.assertEqual(interruption['activity']['action'], ['coordinator', 'stop-start'])
        self.assertFalse(any('mid-preparation' in m for m in interruption['missing_assurance']))

    def test_bounds_fit_their_owner_runtimes(self):
        # A fault's effect, 900 s recovery and post checks fit inside the owner deadline.
        for fault, seconds in Q.EFFECT_SECONDS.items():
            self.assertLess(Q.QUERY_PROBE_SECONDS+Q.WALLET_PROBE_SECONDS+seconds+Q.RECOVERY_SECONDS+Q.POST_SECONDS+600,
                            Q.RUNTIME['fault']-Q.OWNER_MARGIN, fault)
        longest = sum(Q.CAPACITY[k] for k in ('preparation_deadline_seconds', 'duration_seconds', 'recovery_deadline_seconds'))
        self.assertLess(longest+300+Q.CHILD_STOP_SECONDS+600, Q.RUNTIME['capacity']-Q.OWNER_MARGIN)
        self.assertLess(sum(s['seconds']+120 for s in Q.LOAD_STAGES)+Q.FRESHNESS['permit_seconds']+600,
                        Q.RUNTIME['staged-load']-Q.OWNER_MARGIN)
        self.assertGreater(Q.STOP_TIMEOUT, Q.CHILD_STOP_SECONDS)
        self.assertGreaterEqual(Q.REMOTE_BOUND['stop-start'], Q.UNIT_SECONDS['stop']+Q.LOSS_HOLD_SECONDS+Q.UNIT_SECONDS['start'])

    def test_targets_and_compositions_are_the_reviewed_values(self):
        self.assertEqual(Q.P95_TARGETS['small-active'], 5); self.assertEqual(Q.P95_TARGETS['restore-6m'], 10)
        self.assertEqual(Q.P95_TARGETS['multi-script'], 60); self.assertEqual(Q.P95_TARGETS['unused'], 15)
        self.assertEqual({l:sum(c.values()) for l, c in Q.COMPOSITION.items()}, {8:8, 20:20, 40:40})
        self.assertTrue(all(Q.HEAVY in c for c in Q.COMPOSITION.values()))
        scenario = json.loads((ROOT/'transparent/tools/transparent-loadtest/scenarios/mixed-20-sustained.json').read_text())
        self.assertEqual(Q.COMPOSITION[20], scenario['profiles'])


class DeadlineTests(unittest.TestCase):
    def test_one_shared_monotonic_bound(self):
        clock = Clock()
        with patch.object(Q.time, 'monotonic', clock), patch.object(Q.time, 'sleep', clock.sleep):
            deadline = Q.Deadline(100)
            child = deadline.child(500)
            self.assertEqual(child.end, deadline.end)
            deadline.need(100, 'whole')
            with self.assertRaisesRegex(Q.Budget, 'needs 101 s'):
                deadline.need(101, 'more')
            self.assertEqual(deadline.timeout(30), 30)
            clock.now += 90
            self.assertEqual(deadline.timeout(30), 10)
            with self.assertRaises(Q.Budget):
                deadline.sleep(20)
            with self.assertRaises(Q.Budget):
                deadline.timeout(1)

    def test_commands_are_capped_by_the_deadline(self):
        seen = []
        with patch.object(Q.H.Commands, 'run', lambda self, argv, *, data=None, timeout=30: seen.append(timeout) or b''):
            commands = Q.BoundedCommands(Q.Deadline(3))
            commands.run(['systemctl', 'show', 'x'], timeout=30)
            self.assertLessEqual(seen[-1], 3)
            Q.BoundedCommands().run(['systemctl'], timeout=60)
            self.assertEqual(seen[-1], 15)


class RateTests(unittest.TestCase):
    def evaluate(self, processes, qps=20, seconds=600):
        return Q.evaluate_rate(processes, qps, seconds, FIXTURE)

    def test_exact_twenty_qps_passes(self):
        result = self.evaluate(rate_lines(20, 600))
        self.assertEqual(result['status'], 'passed', result['failures'])
        self.assertEqual(result['exact_logical'], 12000)
        self.assertAlmostEqual(result['recent_fraction'], .8)

    def test_logical_failure_or_inexact_answer_fails(self):
        self.assertIn('logical query failures', self.evaluate(rate_lines(20, 600, drop=1))['failures'])
        inexact = rate_lines(20, 600); inexact[2][5]['exact'] = False
        self.assertIn('logical query failures', self.evaluate(inexact)['failures'])

    def test_recovered_retry_counts_as_transport_attempt(self):
        recovered = self.evaluate(rate_lines(20, 600, error_then_success=100))
        self.assertEqual(recovered['logical_failures'], 0)
        self.assertEqual(recovered['status'], 'passed')
        noisy = self.evaluate(rate_lines(20, 600, error_then_success=130))
        self.assertIn('transport attempt failures at or above 1 percent', noisy['failures'])

    def test_rate_latency_mix_and_fixture_gates(self):
        self.assertIn('completed rate below 95 percent of requested', self.evaluate(rate_lines(20, 500), 20, 600)['failures'])
        self.assertIn('latency gate failed', self.evaluate(rate_lines(20, 600, http=.7))['failures'])
        self.assertIn('recent/archive mix outside 80/20', self.evaluate(rate_lines(20, 600, recent=5))['failures'])
        self.assertEqual(self.evaluate(rate_lines(20, 600, fixture='e'*64))['status'], 'failed')
        self.assertEqual(self.evaluate([])['status'], 'failed')


class FreshnessTests(unittest.TestCase):
    def advance(self, window, now, *, public=5, replica=20, blocks=1, late=None):
        """New blocks every 75 s; node observations bracket each by one second."""
        for _ in range(blocks):
            now += 75
            window.node(window.tip, now-1, now-.9)  # the loop observes about every second
            window.node(window.tip+1, now, now+.1)
            delay = late if late else public
            window.serving('public', window.tip, now+delay-.1, now+delay)
            for name in window.replicas:
                window.serving(name, window.tip, now+replica-.1, now+replica)
            window.complete(now+delay)
        return now

    def window(self, replicas=('recent-1', 'recent-2'), gap=10**9):
        window = Q.FreshnessWindow(list(replicas), 0, dict(Q.FRESHNESS, gap_seconds=gap))
        window.node(100, 0, 0)
        return window

    def test_requires_six_hours_and_three_hundred_blocks(self):
        window = self.window()
        now = self.advance(window, 0, blocks=287)
        self.assertGreaterEqual(now, 21525)
        self.assertFalse(window.qualifies(21600))  # six hours, 287 blocks
        now = self.advance(window, now, blocks=13)
        self.assertIsNone(window.violation(now+30))
        self.assertTrue(window.qualifies(now+30))
        short = Q.FreshnessWindow(['recent-1'], 0); short.node(1, 0, 0)
        for _ in range(300):
            short.node(short.tip+1, 1, 1); short.serving('public', short.tip, 2, 2); short.serving('recent-1', short.tip, 2, 2)
        self.assertFalse(short.qualifies(100))

    def test_latency_is_bounded_from_the_last_observation_without_the_block(self):
        window = self.window(['recent-1'])
        window.node(100, 0, .1)           # tip without block 101
        window.node(101, 20, 20.1)        # discovered 20 s later
        window.serving('public', 101, 44, 45)
        # Discovery time would give 25 s; the conservative bound is 45 s.
        self.assertEqual(window.visible['public'][101], 45)
        self.assertIn('public freshness exceeded 30s at block 101', window.violation(45))
        first = Q.FreshnessWindow(['recent-1'], 0)
        first.node(500, 0, 1)             # the first observation has no prior bound
        self.assertEqual(first.births, {})

    def test_violation_resets_credit_and_preserves_failed_interval(self):
        window = self.window(['recent-1'])
        now = self.advance(window, 0, blocks=50)
        now = self.advance(window, now, late=31)
        reason = window.violation(now+31)
        self.assertIn('public freshness exceeded', reason)
        window.close(now+31, reason)
        self.assertEqual(window.failed[0]['blocks'], 51)
        self.assertEqual(window.blocks(), 0)
        self.assertEqual(window.started, now+31)
        later = self.advance(window, now+31, blocks=2)
        self.assertEqual(window.blocks(), 2)
        self.assertFalse(window.qualifies(later+21600))

    def test_observation_gaps_never_count_as_fresh(self):
        window = Q.FreshnessWindow(['recent-1'], 0)
        self.assertIsNone(window.complete(9))
        self.assertIn('observation gap of 11.0 s', window.complete(20))
        window.close(20, 'observation gap')
        self.assertEqual(window.failed[-1]['reason'], 'observation gap')
        self.assertEqual((window.started, window.last_complete), (20, 20))

    def test_overdue_replica_and_reorganization(self):
        window = self.window(['recent-1'])
        window.node(100, 99, 99.5); window.node(101, 100, 100.5); window.serving('public', 101, 101, 101)
        self.assertIsNone(window.violation(158))
        self.assertIn('recent-1 freshness exceeded 60s', window.violation(160))
        window = self.window(['recent-1'])
        window.node(101, 5, 5); window.node(102, 6, 6)
        window.node(99, 10, 10)
        self.assertEqual(window.failed[-1]['reason'], 'chain reorganized')
        self.assertTrue(window.permits() is False)


def write_lines(path, values):
    path.write_text(''.join(json.dumps(v)+'\n' for v in values))


def metric_text(start=100.0, current=1e9, limit=4e9):
    return ('transparent_shard_process_start_time_seconds %s\ntransparent_shard_cgroup_memory_current_bytes %s\n'
            'transparent_shard_cgroup_memory_max_bytes %s\ntransparent_shard_query_queue_depth{worker="a"} 3\n'
            'transparent_shard_revisions_held 2\n' % (start, current, limit))


class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def wallets(self, *, per_profile=20, seconds=2.0, heavy='exact', missing=0, t0=1000.0):
        wallets, identifier = [], 0
        for profile in Q.COMPOSITION[8]:
            for k in range(per_profile if profile != Q.HEAVY else 1):
                identifier += 1
                at = t0+k*30
                wallets.append({'type':'scheduled', 'id':identifier, 'at':at, 'profile':profile})
                wallets.append({'type':'started', 'id':identifier, 'at':at+.1})
                if identifier <= missing:
                    continue
                outcome = heavy if profile == Q.HEAVY else 'exact'
                wallets.append({'type':'outcome', 'id':identifier, 'at':at+.1+seconds, 'outcome':outcome,
                                'events_exact':outcome == 'exact'})
        return wallets

    def trial(self, *, wallets=None, refused=0, metrics=None, t0=1000.0, exit_code=0, stopped=None, extra='', **options):
        write_lines(self.directory/'wallets.ndjson', wallets if wallets is not None else self.wallets(t0=t0, **options))
        if extra:
            with (self.directory/'wallets.ndjson').open('a') as stream:
                stream.write(extra)
        requests = [{'type':'request', 'id':1, 'status':503 if i < refused else 200, 'bytes_up':10, 'bytes_down':20} for i in range(100)]
        write_lines(self.directory/'requests.ndjson', requests)
        write_lines(self.directory/'metrics.ndjson', metrics if metrics is not None else
                    [{'at':t0, 'target':'recent-1', 'text':metric_text()}, {'at':t0+5, 'target':'recent-1', 'text':metric_text()},
                     {'at':t0, 'target':'load-client', 'processes':[]}])
        return Q.evaluate_capacity(self.directory, 8, t0+3700, exit_code, stopped, ['recent-1'])

    def test_completed_exact_outcomes_only_count(self):
        result = self.trial()
        self.assertEqual(result['status'], 'passed', result['failures'])
        self.assertEqual(result['exact'], 141)
        self.assertEqual(result['profiles']['small-active']['p95_seconds'], 2.0)
        self.assertEqual(result['workers']['recent-1']['maximum_queue_depth'], 3)
        self.assertEqual(result['payload_bytes'], {'up':1000, 'down':2000})
        self.assertAlmostEqual(result['sustained_exact_per_second'], 141/3600)

    def test_nonzero_or_stopped_exit_fails_despite_complete_rows(self):
        for exit_code, stopped, message in ((1, None, 'loadtest did not exit 0 (exit 1)'), (-15, None, 'exit -15'),
                                            (None, None, 'exit None'), (0, 'freshness violation', 'stopped: freshness violation')):
            result = self.trial(exit_code=exit_code, stopped=stopped)
            self.assertEqual(result['status'], 'failed')
            self.assertTrue(any(message in f for f in result['failures']), result['failures'])
            self.assertFalse(result['complete'])
            self.assertIsNone(result['sustained_exact_per_second'])

    def test_duplicate_malformed_and_misordered_events_fail(self):
        base = self.wallets()
        cases = {
            'duplicate scheduled wallet event': base+[base[0]],
            'duplicate outcome wallet event': base+[base[2]],
            'wallet started before it was scheduled': [dict(e, at=e['at']-5) if e['type'] == 'started' and e['id'] == 1 else e for e in base],
            'wallet outcome precedes its start': [dict(e, at=e['at']-10) if e['type'] == 'outcome' and e['id'] == 1 else e for e in base],
            'malformed wallet event': base+[{'type':'progress', 'id':1, 'at':1}],
            'wallet event without a scheduled wallet': base+[{'type':'started', 'id':999, 'at':1001}],
        }
        for message, wallets in cases.items():
            result = self.trial(wallets=wallets)
            self.assertIn(message, result['failures'], message)
            self.assertEqual(result['status'], 'failed')
        garbled = self.trial(extra='{not json\n')
        self.assertIn('malformed wallet event', garbled['failures'])
        nan = self.trial(wallets=base+[{'type':'scheduled', 'id':500, 'at':float('nan'), 'profile':'unused'}])
        self.assertIn('malformed wallet event', nan['failures'])

    def test_scheduled_without_outcome_and_heavy_timeouts_fail(self):
        unterminated = self.trial(missing=3)
        self.assertIn('scheduled wallets without a terminal outcome', unterminated['failures'])
        self.assertEqual(unterminated['exact'], 138)
        heavy = self.trial(per_profile=2, heavy='timed_out')
        self.assertIn('failed or incomplete syncs above 5 percent', heavy['failures'])
        self.assertEqual(heavy['profiles'][Q.HEAVY]['outcomes'], {'timed_out':1})

    def test_503_restarts_memory_and_window_gates(self):
        self.assertIn('HTTP 503 attempts above 10 percent', self.trial(refused=11)['failures'])
        restarted = self.trial(metrics=[{'at':1, 'target':'recent-1', 'text':metric_text(100)},
                                        {'at':2, 'target':'recent-1', 'text':metric_text(200)}])
        self.assertIn('worker restarted during trial: recent-1', restarted['failures'])
        hot = self.trial(metrics=[{'at':1, 'target':'recent-1', 'text':metric_text(current=3.5e9)}])
        self.assertIn('worker cgroup memory above 80 percent of limit: recent-1', hot['failures'])
        absent = self.trial(metrics=[])
        self.assertIn('no metrics scrape for recent-1', absent['failures'])
        early = Q.evaluate_capacity(self.directory, 8, 1000+3599, 0, None, ['recent-1'])
        self.assertIn('trial did not complete its 60-minute sustained window', early['failures'])


def trial_result(level, trial, observations=40, seconds=1.0, status='passed', throughput=.5, heavy_timeouts=0):
    profiles = {name:{'terminal':observations, 'exact':observations, 'outcomes':{'exact':observations},
                      'seconds':[seconds]*observations} for name in Q.COMPOSITION[level]}
    profiles[Q.HEAVY]['outcomes'] = {'exact':observations-heavy_timeouts, 'timed_out':heavy_timeouts}
    return {'level':level, 'trial':trial, 'status':status, 'profiles':profiles, 'sustained_exact_per_second':throughput}


class DecisionTests(unittest.TestCase):
    def test_three_passing_trials_with_enough_observations(self):
        trials = [trial_result(8, n, throughput=.4+n/10) for n in (1, 2, 3)]
        decision = Q.capacity_decision(trials)
        self.assertEqual(decision['levels'][8]['status'], 'passed')
        self.assertAlmostEqual(decision['supported_exact_per_second'], .25)
        self.assertFalse(decision['qualified'])
        self.assertTrue(decision['missing_assurance'])
        self.assertEqual(decision['levels'][20]['status'], 'incomplete')

    def test_sparse_profiles_extend_and_slow_profiles_fail(self):
        sparse = Q.capacity_decision([trial_result(8, n, observations=30) for n in (1, 2, 3)])
        self.assertEqual(sparse['levels'][8]['status'], 'extend')
        self.assertIsNone(sparse['supported_exact_per_second'])
        slow = Q.capacity_decision([trial_result(8, n, seconds=6) for n in (1, 2, 3)])
        self.assertEqual(slow['levels'][8]['status'], 'failed')
        failed = Q.capacity_decision([trial_result(8, 1), trial_result(8, 2, status='failed'), trial_result(8, 3)])
        self.assertEqual(failed['levels'][8]['status'], 'failed')
        exhausted = Q.capacity_decision([trial_result(8, n, observations=10) for n in range(1, 7)])
        self.assertEqual(exhausted['levels'][8]['status'], 'failed')

    def test_escalation_and_extension_are_fail_closed(self):
        with self.assertRaisesRegex(ValueError, 'previous level'):
            Q.escalation([], 20, 1)
        with self.assertRaisesRegex(ValueError, 'sequential'):
            Q.escalation([trial_result(8, 1)], 8, 3)
        with self.assertRaisesRegex(ValueError, 'final'):
            Q.escalation([trial_result(8, n) for n in (1, 2, 3)], 8, 4)
        Q.escalation([trial_result(8, n, observations=30) for n in (1, 2, 3)], 8, 4)
        with self.assertRaisesRegex(ValueError, 'final'):
            Q.escalation([trial_result(8, 1, status='failed')], 8, 2)
        Q.escalation([trial_result(8, n) for n in (1, 2, 3)], 20, 1)
        interrupted = {'level':8, 'trial':1, 'status':'failed', 'profiles':{}}
        self.assertEqual(Q.capacity_decision([interrupted])['levels'][8]['status'], 'failed')


def recipe():
    return {'version':1, 'source_sha':'c'*40, 'publication_sha256':'d'*64, 'inputs':[], 'rollback_inputs':[],
            'preflight':[{'name':'product-preflight', 'argv':['/usr/bin/python3', 'w', 'schema-product-preflight', '--spec', '/s.json',
                          '--spec-sha256', 'e'*64], 'timeout':1800, 'read_only':True}],
            'steps':[{'name':n, 'argv':['x'], 'timeout':1800, 'read_only':False} for n in
                     ('preserve-v10', 'maintenance', 'stage-v11', 'activate-prewarm', 'align-origins', 'verify-canonical', 'resume-load', 'verify-service')],
            'rollback':[{'name':n, 'argv':['x'], 'timeout':t, 'read_only':False} for n, t in Q.ROLLBACK_BUDGET.items()]}


def journal(identifier, status, *, rollback=True, created=1000.0, value=None):
    value = value or recipe()
    events = [{'group':'steps', 'name':c['name'], 'status':'passed', 'exit_code':0, 'started':created+i, 'seconds':1}
              for i, c in enumerate(value['steps'])]
    if rollback:
        start = created+5000
        for c in value['rollback']:
            events.append({'group':'rollback', 'name':c['name'], 'status':'passed', 'exit_code':0, 'started':start, 'seconds':c['timeout']-10})
            start += c['timeout']-10
    return {'journal_version':1, 'id':identifier, 'recipe':value, 'recipe_sha256':durable.digest(value), 'status':status,
            'created':created, 'events':events}


class RollbackJournalTests(unittest.TestCase):
    def setUp(self):
        self.original = journal('transparent-schema-1', 'rolled-back')
        self.redeploy = journal('transparent-schema-2', 'committed', rollback=False, created=7000)
        self.sha = self.original['recipe_sha256']

    def verify(self, original=None, redeploy=None, latest='transparent-schema-2'):
        return Q.verify_rollback_redeploy(original or self.original, redeploy or self.redeploy, latest, self.sha)

    def test_real_rollback_then_same_recipe_redeploy(self):
        evidence = self.verify()
        self.assertEqual(evidence['rollback_seconds'], 690)
        self.assertEqual(evidence['redeploy_transaction'], 'transparent-schema-2')
        self.assertEqual(evidence['redeploy_committed_unix'], 7008)

    def test_refuses_fabricated_or_changed_rollbacks(self):
        cases = []
        failed = copy.deepcopy(self.original); failed['status'] = 'rollback-failed'; cases.append((failed, None, 'transparent-schema-2'))
        repaired = copy.deepcopy(self.original); repaired['recovery_programs'] = [{}]; cases.append((repaired, None, 'transparent-schema-2'))
        slow = copy.deepcopy(self.original); slow['events'][-1]['seconds'] = 141; cases.append((slow, None, 'transparent-schema-2'))
        missing = copy.deepcopy(self.original); missing['events'].pop(); cases.append((missing, None, 'transparent-schema-2'))
        skipped = copy.deepcopy(self.original); skipped['events'][9]['status'] = 'failed'; cases.append((skipped, None, 'transparent-schema-2'))
        budget = recipe(); budget['rollback'][2]['timeout'] = 400
        changed = journal('transparent-schema-1', 'rolled-back', value=budget); cases.append((changed, None, 'transparent-schema-2'))
        early = journal('transparent-schema-2', 'committed', rollback=False, created=5500); cases.append((None, early, 'transparent-schema-2'))
        other = recipe(); other['source_sha'] = '9'*40
        foreign = journal('transparent-schema-2', 'committed', rollback=False, created=7000, value=other); cases.append((None, foreign, 'transparent-schema-2'))
        cases.append((None, None, 'transparent-schema-3'))
        rolled = journal('transparent-schema-2', 'committed', created=7000); cases.append((None, rolled, 'transparent-schema-2'))
        for original, redeploy, latest in cases:
            with self.assertRaises(ValueError):
                self.verify(original, redeploy, latest)

    def test_committed_transaction_binding(self):
        Q.committed(self.redeploy, self.sha)
        for change in ({'status':'applying'}, {'recipe_sha256':'0'*64}, {'events':self.redeploy['events'][:-1]}):
            with self.assertRaises(ValueError):
                Q.committed(dict(self.redeploy, **change), self.sha)
        self.assertEqual(Q.spec_input(self.redeploy['recipe']), {'path':'/s.json', 'sha256':'e'*64})


class ProcessTests(unittest.TestCase):
    def test_identity_liveness_and_owned_sessions(self):
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], start_new_session=True)
        self.addCleanup(lambda: child.poll() is None and child.kill())
        identity = Q.process_identity(child.pid)
        self.assertEqual(identity['session'], child.pid)
        self.assertTrue(Q.alive(identity))
        self.assertFalse(Q.alive(dict(identity, start_ticks=identity['start_ticks']+1)))
        self.assertEqual(Q.session_members([child.pid]), [child.pid])
        self.assertLessEqual(abs(Q.started_unix(child.pid)-time.time()), 5)
        child.kill(); child.wait()
        self.assertFalse(Q.alive(identity))
        self.assertEqual(Q.session_members([child.pid]), [])

    def test_receipts_are_written_once_and_sealed(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'result.json'
            Q.write_once(path, {'status':'passed'})
            self.assertEqual(path.stat().st_mode & 0o777, 0o400)
            with self.assertRaises(FileExistsError):
                Q.write_once(path, {'status':'failed'})


class FakeCommands:
    """Owner-unit and quality-supervisor states for owner lifecycle tests."""
    active = 'inactive'
    main_pid = '0'
    empty = True
    listing = ''

    def __init__(self, deadline=None):
        self.deadline = deadline

    def state(self, unit):
        return {'ActiveState':self.active, 'MainPID':self.main_pid, 'NRestarts':'0', 'ControlGroup':''}

    def empty_cgroup(self, state):
        return self.empty

    def run(self, argv, *, data=None, timeout=30):
        return self.listing.encode() if 'list-units' in argv else b''

    def service_resources(self, unit, root):
        return {'memory_available':8, 'memory_total':10, 'disk_available':8, 'disk_total':10, 'oom':0, 'oom_kill':0,
                'restarts':'0', 'pid':'7'}


class FakeSystem:
    """An in-memory systemd: units, main PIDs, executables, queued jobs and failures."""
    def __init__(self, *units):
        self.units = {u:{'ActiveState':'active', 'MainPID':str(100+i)} for i, u in enumerate(units)}
        self.exe, self.calls, self.fail, self.hooks = {}, [], {}, {}
        self.next_pid, self.jobs, self.listing, self.ready = 500, '', '', {'ready':True}
        self.control_status = {'active':{'map_sha256':'1'*64}, 'preparing':None}
        self.deadline = None

    def state(self, unit):
        value = self.units.setdefault(unit, {'ActiveState':'inactive', 'MainPID':'0'})
        return {'SubState':'', 'NRestarts':'0', 'FragmentPath':'/etc/systemd/system/'+unit, 'DropInPaths':'',
                'ControlGroup':'/system.slice/'+unit, 'Result':'success', **value}

    def empty_cgroup(self, state):
        return state.get('MainPID') in ('0', '')

    def run(self, argv, *, data=None, timeout=30):
        self.calls.append(list(argv))
        if argv[:2] == ['systemctl', '--no-block']:
            action, unit = argv[2], argv[3]
            if action in self.hooks:
                self.hooks.pop(action)()
            if action in self.fail:
                self.fail[action] -= 1
                if self.fail[action] == 0:
                    self.fail.pop(action)
                raise subprocess.CalledProcessError(1, argv)
            if action == 'stop':
                self.units[unit] = {'ActiveState':'inactive', 'MainPID':'0'}
            else:
                self.next_pid += 1
                self.units[unit] = {'ActiveState':'active', 'MainPID':str(self.next_pid)}
            return b''
        if 'list-jobs' in argv:
            return self.jobs.encode()
        if 'list-units' in argv:
            return self.listing.encode()
        return b''

    def service_resources(self, unit, root):
        return {'memory_available':8, 'memory_total':10, 'disk_available':8, 'disk_total':10, 'oom':0, 'oom_kill':0,
                'restarts':'0', 'pid':self.state(unit)['MainPID']}

    def cache_observation(self):
        return {'ready':dict(self.ready, binary_sha256=self.ready.get('binary_sha256', 'e'*64))}

    def control(self):
        return self.control_status


REAL_READLINK = os.readlink


def readable_processes():
    """Tests run unprivileged: another user's executable link reads as absent, never as owned."""
    def readlink(path, *args, **kwargs):
        try:
            return REAL_READLINK(path, *args, **kwargs)
        except PermissionError:
            raise FileNotFoundError(path) from None
    return patch.object(Q.os, 'readlink', readlink)


def fake_processes(system):
    """Process identity and executables from the in-memory system, not /proc."""
    return (patch.object(Q, 'process_identity', lambda pid: {'pid':pid, 'start_ticks':pid, 'session':pid, 'boot_id':'fake'}),
            patch.object(Q, 'executable_sha256', lambda pid: system.exe.get(int(pid), 'e'*64)))


class OwnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root/'machine').write_text('c'*32)
        self.inventory = SimpleNamespace(lock={'type':'pinned_host', 'machine_id':'c'*32})
        FakeCommands.active, FakeCommands.main_pid, FakeCommands.empty, FakeCommands.listing = 'inactive', '0', True, ''
        for item in (patch.object(Q, 'OWNERS', self.root/'owners'), patch.object(schema_fence, 'INPUT_STAGING', self.root/'owners'),
                     patch.object(schema_fence, 'HOST_ACTIONS', self.root/'host-actions'),
                     patch.object(Q, 'REMOTE', self.root/'actions'),
                     patch.object(Q, 'ROOT', self.root/'qualification'), patch.object(Q, 'SOURCES', self.root/'sources'),
                     patch.object(Q.ProductionLock, 'PATH', self.root/'lock'),
                     patch.object(Q.ProductionLock, 'MACHINE_ID', self.root/'machine'),
                     patch.object(Q.ProductionLock, 'ROOT_UID', os.geteuid()), patch.object(Q, 'commands', FakeCommands),
                     patch.dict(os.environ)):
            item.start(); self.addCleanup(item.stop)
        self.assertFalse((schema_fence.SCHEMA_STATE/schema_fence.SCHEMA_POINTER).exists())
        self.observed = {'deployment':{'transaction':'transparent-schema-1', 'hosts':[]}, 'local':{}, 'remote':{}, 'ready':{},
                         'coordinator':{}, 'public':{}}
        self.spec = {'inventory':{'path':'/inventory.json', 'sha256':'1'*64}}

    def owner(self, value=None):
        value = value or request('freshness')
        q = Q.Qualification(self.inventory, value, durable.digest(value))
        return q

    def launch(self, q):
        def preflight(running=False):
            if not running:
                self.assertEqual(q.status()['status'], 'absent')
            q.spec = self.spec
            return self.observed
        launched = []
        with patch.object(q, 'preflight', preflight), \
                patch.object(Q.subprocess, 'run', lambda command, **_: launched.append(command)):
            q.run('run', q.plan_sha)
        return launched[0]

    def test_launch_requires_reviewed_plan_and_records_intent_before_systemd(self):
        q = self.owner()
        with self.assertRaisesRegex(ValueError, 'plan checksum'):
            q.run('run', '0'*64)
        command = self.launch(q)
        self.assertEqual(command[:3], ['/usr/bin/systemd-run', '--quiet', '--unit='+q.unit])
        self.assertIn('--property=TimeoutStopSec=90', command)
        self.assertIn('schema-qualify-exec', command)
        self.assertNotIn('--request', command)
        self.assertEqual(q.status()['status'], 'launching')
        self.assertEqual((q.directory/'request.json').stat().st_mode & 0o777, 0o400)
        self.assertEqual(Q.retained_request(q.sha), q.request)
        with self.assertRaisesRegex(ValueError, 'unfinished input staging owner'):
            schema_fence.local_schema_fence()
        with self.assertRaises(ValueError):
            self.launch(self.owner())

    def test_execute_runs_one_owner_and_stays_fenced_until_reconcile(self):
        q = self.owner()
        self.launch(q)
        runner = Q.Qualification(self.inventory, q.request, q.sha)
        def activity():
            self.assertLessEqual(runner.deadline.remaining(), Q.RUNTIME['freshness']-Q.OWNER_MARGIN)
            child = runner.spawn('client', [sys.executable, '-c', 'import os,sys; sys.exit(0 if os.environ["WALLET_PIR_PRODUCTION_LOCK_FDS"] else 1)'],
                                 subprocess.DEVNULL, subprocess.DEVNULL)
            self.assertEqual(child.wait(timeout=10), 0)
            return {'status':'passed', 'failures':[]}
        with patch.object(runner, 'preflight', lambda running=False: self.observed), \
                patch.object(runner, 'freshness_window', activity):
            runner.execute()
        record = runner.status()
        self.assertEqual(record['status'], 'finished')
        self.assertEqual(record['children'][0]['status'], 'exited')
        result = json.loads((q.directory/'result.json').read_text())
        self.assertEqual(result['status'], 'passed')
        self.assertFalse(result['qualified'])  # quality-alert assurance remains missing
        with self.assertRaises(ValueError):
            schema_fence.local_schema_fence()
        with self.assertRaisesRegex(ValueError, 'still alive'):
            runner.run('reconcile')  # this test process is the recorded owner
        exited = subprocess.Popen([sys.executable, '-c', 'pass']); identity = Q.process_identity(exited.pid); exited.wait()
        record = runner.status(); record['owner'] = identity; runner.save(record)  # the detached owner has exited
        reconciled = runner.run('reconcile')
        self.assertEqual(reconciled['status'], 'reconciled')
        self.assertEqual(reconciled['outcome'], 'passed')
        schema_fence.local_schema_fence()
        with self.assertRaisesRegex(ValueError, 'does not require'):
            runner.run('reconcile')

    def test_sigterm_records_an_interrupted_result(self):
        q = self.owner()
        self.launch(q)
        runner = Q.Qualification(self.inventory, q.request, q.sha)
        def activity():
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(5)
        with patch.object(runner, 'preflight', lambda running=False: self.observed), \
                patch.object(runner, 'freshness_window', activity):
            with self.assertRaises(Q.Interrupted):
                runner.execute()
        self.assertEqual(json.loads((q.directory/'result.json').read_text())['status'], 'interrupted')
        self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)

    def test_reconcile_waits_for_unit_and_owned_descendants(self):
        q = self.owner()
        self.launch(q)
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], start_new_session=True)
        self.addCleanup(lambda: child.poll() is None and child.kill())
        record = q.status()
        record.update(status='running', children=[{'name':'client', 'status':'running', 'identity':Q.process_identity(child.pid)},
                                                  {'name':'unidentified', 'status':'starting'}])
        q.save(record)
        FakeCommands.main_pid, FakeCommands.active = '42', 'active'
        with self.assertRaisesRegex(ValueError, 'unit still runs'):
            q.run('reconcile')
        FakeCommands.main_pid, FakeCommands.active, FakeCommands.empty = '0', 'inactive', False
        with self.assertRaisesRegex(ValueError, 'unit still runs'):
            q.run('reconcile')  # a child started before its identity was saved still holds the owner cgroup
        FakeCommands.empty = True
        with self.assertRaisesRegex(ValueError, 'still alive'):
            q.run('reconcile')
        os.killpg(child.pid, signal.SIGKILL); child.wait()
        reconciled = q.run('reconcile')
        self.assertEqual(reconciled['outcome'], 'interrupted')
        self.assertEqual(q.status()['reconciliation']['unidentified_children'], ['unidentified'])
        self.assertEqual(json.loads((q.directory/'result.json').read_text())['status'], 'interrupted')
        schema_fence.local_schema_fence()

    def test_reconcile_requires_lock_free_of_inheriting_children(self):
        q = self.owner()
        self.launch(q)
        with Q.ProductionLock(self.inventory.lock):
            with self.assertRaises(BlockingIOError):
                q.run('reconcile')

    def test_preflight_holds_the_global_lock(self):
        q = self.owner()
        with patch.object(q, 'preflight', lambda running=False: self.observed):
            with Q.ProductionLock(self.inventory.lock):
                with self.assertRaises(BlockingIOError):
                    q.run('preflight')
            self.assertEqual(q.run('preflight')['status'], 'ready')

    def test_an_unrestored_effect_fails_and_fences_the_result(self):
        q = self.owner(request('fault', fault='recent-worker-loss', target='recent-01'))
        self.launch(q)
        runner = Q.Qualification(self.inventory, q.request, q.sha)
        def activity():
            runner.record.setdefault('remote_actions', []).append({'host':'recent-01', 'operation':'stop-start', 'status':'unknown'})
            raise Q.Unknown('remote qualification act on recent-01 timed out; outcome unknown')
        with patch.object(runner, 'preflight', lambda running=False: self.observed), patch.object(runner, 'fault', activity):
            with self.assertRaises(Q.Unknown):
                runner.execute()
        result = json.loads((q.directory/'result.json').read_text())
        self.assertEqual((result['status'], result['fenced']), ('failed', 'owned unit effect requires reconcile restoration'))
        self.assertEqual(runner.status()['remote_actions'][0]['status'], 'unknown')

    def test_capacity_trials_include_interrupted_attempts(self):
        value = capacity_request()
        q = self.owner(value)
        self.launch(q)
        q.run('reconcile')
        trials = Q.reconciled_trials(value['transaction'])
        self.assertEqual([(t['level'], t['trial'], t['status']) for t in trials], [(8, 1, 'failed')])
        self.assertEqual(Q.summary_report(value['transaction'])['levels'][8]['status'], 'failed')
        with self.assertRaisesRegex(ValueError, 'final'):
            Q.escalation(trials, 8, 2)

    def test_unrestored_remote_action_stays_fenced_until_remote_restoration(self):
        q = self.owner(request('fault', fault='router-restart'))
        self.launch(q)
        record = q.status()
        record.update(status='finished', remote_actions=[{'host':'router', 'operation':'restart', 'status':'unknown'}])
        q.save(record)
        replies = [{'status':'failed'}, {'status':'restored'}]
        calls = []
        q.retained_hosts = lambda: None
        def remote(host, operation, action):
            calls.append((host, operation, action))
            return None, replies.pop(0)
        q.remote = remote
        with self.assertRaisesRegex(ValueError, 'not restored on router'):
            q.run('reconcile')
        with self.assertRaises(ValueError):
            schema_fence.local_schema_fence()
        self.assertEqual(q.run('reconcile')['status'], 'reconciled')
        self.assertEqual(calls, [('router', 'restart', 'reconcile')]*2)
        self.assertEqual(q.status()['remote_actions'][0]['status'], 'restored')
        schema_fence.local_schema_fence()

    def test_unrestored_local_publisher_is_restored_by_reconcile(self):
        q = self.owner(request('fault', fault='publication-interruption'))
        self.launch(q)
        system = FakeSystem(Q.PUBLISHER)
        identity, executable = fake_processes(system)
        with identity, executable:
            original = Q.unit_identity(system, Q.PUBLISHER)
            system.units[Q.PUBLISHER] = {'ActiveState':'inactive', 'MainPID':'0'}  # owner died after the stop
            record = q.status()
            record.update(status='finished', effect={'fault':'publication-interruption', 'status':'starting', 'unit':Q.PUBLISHER,
                                                     'original':original, 'phase':'stopped', 'restores':[]})
            q.save(record)
            with patch.object(Q, 'commands', lambda deadline=None: system):
                reconciled = q.run('reconcile')
        self.assertEqual(reconciled['status'], 'reconciled')
        effect = q.status()['effect']
        self.assertEqual(effect['status'], 'restored')
        self.assertIn('start_intent_unix', effect['restores'][0])
        self.assertEqual(system.units[Q.PUBLISHER]['ActiveState'], 'active')


class OwnerFindingTests(unittest.TestCase):
    """Fresh owner quiescence: terminal journals are not enough."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for item in (patch.object(schema_fence, 'INPUT_STAGING', self.root/'owners'),
                     patch.object(schema_fence, 'HOST_ACTIONS', self.root/'host-actions'),
                     patch.object(Q, 'REMOTE', self.root/'actions'), patch.object(Q, 'SCHEMA', self.root/'schema'),
                     patch.object(Q, 'PUBLICATION_JOB', self.root/'publication'),
                     patch.object(Q, 'OWNED_ROOTS', (self.root/'owned',)), readable_processes()):
            item.start(); self.addCleanup(item.stop)
        self.child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], start_new_session=True)
        self.addCleanup(lambda: self.child.poll() is None and self.child.kill())

    def host_action(self, record):
        (self.root/'host-actions/transparent-schema-1').mkdir(parents=True, exist_ok=True)
        durable.atomic_json(self.root/'host-actions/transparent-schema-1/r1.json', record)
        durable.atomic_json(self.root/'host-actions/latest.json', {'transaction':'transparent-schema-1', 'request_id':'r1'})

    def test_terminal_record_with_a_live_recorded_owner_is_not_quiescent(self):
        system = FakeCommands()
        self.host_action({'status':'passed', 'pid':self.child.pid, 'started_unix':time.time()+1})
        with self.assertRaisesRegex(ValueError, 'not quiescent'):
            Q.owner_findings(system)
        self.host_action({'status':'passed', 'pid':self.child.pid, 'started_unix':time.time()-3600})
        Q.owner_findings(system)  # that PID now names a later, unrelated process
        (self.root/'actions').mkdir()
        durable.atomic_json(self.root/('actions/'+'d'*64+'.json'), {'status':'restored', 'request_sha256':'d'*64,
                                                                  'owner':Q.process_identity(self.child.pid)})
        durable.atomic_json(self.root/'actions/latest.json', {'request_sha256':'d'*64})
        with self.assertRaisesRegex(ValueError, 'not quiescent'):
            Q.owner_findings(system)
        self.child.kill(); self.child.wait()
        findings = Q.owner_findings(system)
        self.assertEqual({r['namespace'] for r in findings['records']}, {'host-actions', 'qualification-actions'})

    def test_every_retained_record_not_only_the_latest_is_reconciled(self):
        system = FakeCommands()
        (self.root/'host-actions/transparent-schema-0').mkdir(parents=True)
        durable.atomic_json(self.root/'host-actions/transparent-schema-0/old.json',
                            {'status':'reconciled', 'pid':self.child.pid, 'started_unix':time.time()+1})
        self.host_action({'status':'passed'})  # the latest owner is terminal and dead
        with self.assertRaisesRegex(ValueError, "'owner', 'host-actions', %d" % self.child.pid):
            Q.owner_findings(system)
        durable.atomic_json(self.root/'host-actions/transparent-schema-0/old.json', {'status':'reconciled'})
        (self.root/'owners').mkdir()
        durable.atomic_json(self.root/('owners/'+'e'*64+'.json'), {'request_sha256':'e'*64, 'status':'staged',
                            'children':[{'name':'native', 'identity':Q.process_identity(self.child.pid)}]})
        durable.atomic_json(self.root/('owners/'+'f'*64+'.json'), {'request_sha256':'f'*64, 'status':'staged'})
        durable.atomic_json(self.root/'owners/latest.json', {'request_sha256':'f'*64})
        with self.assertRaisesRegex(ValueError, "'owner', 'input-staging'"):
            Q.owner_findings(system)
        durable.atomic_json(self.root/('owners/'+'e'*64+'.json'), {'request_sha256':'e'*64, 'status':'staged',
                            'pid':self.child.pid, 'process_start':Q.process_identity(self.child.pid)['start_ticks']})
        with self.assertRaisesRegex(ValueError, 'not quiescent'):
            Q.owner_findings(system)  # the upload owner format: PID with kernel start ticks
        (self.root/'schema').mkdir()
        durable.atomic_json(self.root/'schema/transparent-schema-1.json', {'journal_version':1, 'status':'committed',
                            'v10_reconciliation':{'preparations':[{'status':'running', 'pid':self.child.pid, 'started':time.time()}]}})
        os.unlink(self.root/('owners/'+'e'*64+'.json'))
        with self.assertRaisesRegex(ValueError, "'owner', 'schema'"):
            Q.owner_findings(system)  # a nested owner inside a terminal journal
        self.child.kill(); self.child.wait()
        findings = Q.owner_findings(system)
        self.assertEqual({r['namespace'] for r in findings['records']}, {'host-actions', 'input-staging', 'schema'})
        self.assertTrue(all(r['sha256'] for r in findings['records']))
        schema = next(r for r in findings['records'] if r['namespace'] == 'schema')
        self.assertEqual(schema['processes'][0]['live'], False)  # retained raw verdicts

    def test_escaped_descendant_of_a_dead_terminal_owner(self):
        self.child.kill(); self.child.wait()
        ready = self.root/'grandchild'
        parent = subprocess.Popen([sys.executable, '-c', 'import subprocess, sys, time; '
                                   'subprocess.Popen([sys.executable, "-c", "import pathlib, time; pathlib.Path(%r).write_text(\'x\'); time.sleep(60)"])' % str(ready)],
                                  start_new_session=True)
        identity = Q.process_identity(parent.pid)
        parent.wait()
        for _ in range(100):
            if ready.exists():
                break
            time.sleep(.05)
        self.host_action({'status':'passed', 'owner':identity})  # terminal, and its owner is dead
        survivors = Q.session_members([identity['session']])
        self.addCleanup(lambda: [os.kill(p, signal.SIGKILL) for p in Q.session_members([identity['session']])])
        self.assertTrue(survivors)
        with self.assertRaisesRegex(ValueError, "'escaped', %d" % survivors[0]):
            Q.owner_findings(FakeCommands())
        for pid in survivors:
            os.kill(pid, signal.SIGKILL)
        for _ in range(100):
            if not Q.session_members([identity['session']]):
                break
            time.sleep(.05)
        Q.owner_findings(FakeCommands())

    def test_a_process_from_an_owner_root_outside_any_unit_is_escaped(self):
        self.child.kill(); self.child.wait()
        (self.root/'owned').mkdir()
        (self.root/'owned/native.py').write_text('import time; time.sleep(60)')
        stray = subprocess.Popen([sys.executable, str(self.root/'owned/native.py')], start_new_session=True)
        self.addCleanup(lambda: stray.poll() is None and stray.kill())
        with self.assertRaisesRegex(ValueError, "'escaped', %d" % stray.pid):
            Q.owner_findings(FakeCommands())  # no record names it and it holds no lock
        stray.kill(); stray.wait()
        findings = Q.owner_findings(FakeCommands())
        self.assertEqual(findings['processes']['owned_roots'], [])

    def test_incomplete_or_unknown_scope_refuses(self):
        self.child.kill(); self.child.wait()
        (self.root/'owners').mkdir()
        for name in ('a', 'b'):
            durable.atomic_json(self.root/('owners/%s.json' % (name*64)), {'request_sha256':name*64, 'status':'staged'})
        with patch.object(Q, 'MAX_OWNER_RECORDS', 1):
            with self.assertRaisesRegex(ValueError, 'scope is incomplete'):
                Q.owner_findings(FakeCommands())
        (self.root/'owners/garbled.json').write_text('{not json')
        with self.assertRaisesRegex(ValueError, 'scope is unknown'):
            Q.owner_findings(FakeCommands())
        os.unlink(self.root/'owners/garbled.json')
        os.symlink(self.root/('owners/'+'a'*64+'.json'), self.root/'owners/link.json')
        with self.assertRaisesRegex(ValueError, 'not a plain file'):
            Q.owner_findings(FakeCommands())
        os.unlink(self.root/'owners/link.json')
        def denied(path, *args, **kwargs):
            raise PermissionError(path)
        with patch.object(Q.os, 'readlink', denied):
            with self.assertRaisesRegex(ValueError, 'process table is unreadable'):
                Q.owner_findings(FakeCommands())
        Q.owner_findings(FakeCommands())

    def test_unfinished_fence_and_live_owner_units(self):
        self.host_action({'status':'running'})
        with self.assertRaisesRegex(ValueError, 'unfinished remote host owner'):
            Q.owner_findings(FakeCommands())
        self.host_action({'status':'passed'})
        class Live(FakeCommands):
            listing = ('transparent-activity-full-publication-v11.service loaded active running x\n'
                       'transparent-activity-qualification-0123456789abcdef.service loaded active running y\n')
            main_pid, active = '9', 'active'
        with self.assertRaisesRegex(ValueError, 'transparent-activity-full-publication-v11'):
            Q.owner_findings(Live(), own_unit='transparent-activity-qualification-0123456789abcdef')
        class Exited(Live):
            main_pid, active = '0', 'active'
        findings = Q.owner_findings(Exited())
        self.assertEqual(len(findings['units']), 2)
        class Lingering(Exited):
            empty = False
        with self.assertRaisesRegex(ValueError, 'not quiescent'):
            Q.owner_findings(Lingering())

    def test_every_remote_host_is_reconciled_and_none_exempt(self):
        value = request('fault', fault='router-restart')
        q = Q.Qualification(SimpleNamespace(lock={'type':'pinned_host', 'machine_id':'c'*32}), value, durable.digest(value))
        q.hosts = [{'host':h, 'role':r} for h, r in (('coordinator', 'coordinator'), ('router', 'router'), ('r1', 'worker'),
                                                       ('r2', 'worker'), ('a1', 'worker'))]
        calls = []
        def remote(host, operation, action):
            calls.append((host, operation, action))
            if host == 'a1' and self.fail:
                raise ValueError('owner processes or units are not quiescent')
            return None, {'resources':{'memory_available':5, 'memory_total':10, 'disk_available':5, 'disk_total':10}}
        q.remote = remote
        self.fail = False
        self.assertEqual(set(q.reconcile_hosts('identity')), {'router', 'r1', 'r2', 'a1'})
        self.assertEqual(sorted(calls), sorted((h, 'identity', 'probe') for h in ('router', 'r1', 'r2', 'a1')))
        self.fail = True
        with self.assertRaisesRegex(ValueError, 'not quiescent'):
            q.reconcile_hosts('probe')


class RemoteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root/'machine').write_text('b'*32)
        self.system = FakeSystem(Q.H.WORKER, 'caddy.service')
        for item in (patch.object(Q, 'REMOTE', self.root/'actions'), patch.object(schema_fence, 'INPUT_STAGING', self.root/'owners'),
                     patch.object(schema_fence, 'HOST_ACTIONS', self.root/'host-actions'),
                     patch.object(Q.ProductionLock, 'PATH', self.root/'lock'),
                     patch.object(Q.ProductionLock, 'MACHINE_ID', self.root/'machine'),
                     patch.object(Q.ProductionLock, 'ROOT_UID', os.geteuid()), patch.object(Q, 'LOSS_HOLD_SECONDS', 0),
                     patch.object(Q.RemoteActor, 'identity', lambda self: None), *fake_processes(self.system),
                     patch.object(Q, 'SCHEMA', self.root/'schema'), patch.object(Q, 'PUBLICATION_JOB', self.root/'publication'),
                     readable_processes()):
            item.start(); self.addCleanup(item.stop)

    def remote(self, operation='restart', role='worker', **extra):
        return {'version':1, 'source_sha':'a'*40, 'qualification_sha256':'d'*64, 'host':'recent-01', 'role':role,
                'machine_id':'b'*32, 'coordinator_machine_id':'c'*32, 'transaction':'transparent-schema-1', 'operation':operation, **extra}

    def actor(self, operation='stop-start', **extra):
        return Q.RemoteActor(self.remote(operation, **extra), self.system)

    def test_remote_requests_are_closed(self):
        for value in (dict(self.remote(), unit='ssh.service'), self.remote(role='coordinator'),
                      self.remote('stop-start', 'router'), self.remote('await-preparation', 'router'), self.remote('kill'),
                      dict(self.remote(), machine_id='c'*32), dict(self.remote(), transaction='x')):
            with self.assertRaises(ValueError):
                Q.remote_validate(value)
        command = Q.remote_command(self.remote(), 'act')
        self.assertEqual(command[3:], ['schema-qualify-remote', '--action', 'act', '--request-sha256', durable.digest(self.remote())])
        self.assertEqual(Q.remote_command(self.remote(), 'act', sudo=True)[:3], ['sudo', '-n', '--'])
        with self.assertRaisesRegex(ValueError, 'read-only'):
            self.actor('restart').run('probe')
        with self.assertRaisesRegex(ValueError, 'unsupported remote action'):
            self.actor('probe').run('act')

    def test_intent_and_exact_service_precede_each_phase_and_never_replay(self):
        actor = self.actor()
        seen = []
        original = self.system.run
        def recording(argv, **options):
            if argv[:2] == ['systemctl', '--no-block']:
                record = json.loads(actor.path.read_text())
                seen.append((argv[2], record['phase'], record['original']['MainPID'], signal.getsignal(signal.SIGHUP)))
            return original(argv, **options)
        self.system.run = recording
        record = actor.run('act')
        self.assertEqual(record['status'], 'passed', record)
        self.assertEqual(seen, [('stop', 'stopping', '100', signal.SIG_IGN), ('start', 'starting', '100', signal.SIG_IGN)])
        self.assertEqual(record['phase'], 'verified')
        self.assertEqual(record['after']['MainPID'], '501')
        self.assertNotEqual(signal.getsignal(signal.SIGHUP), signal.SIG_IGN)
        with self.assertRaisesRegex(ValueError, 'never replay'):
            actor.run('act')
        restart = self.actor('restart', host='archive-01')
        self.assertEqual(restart.run('act')['status'], 'passed')

    def test_failed_start_is_restored_within_the_bound_or_stays_fenced(self):
        self.system.fail['start'] = 1
        record = self.actor().run('act')
        self.assertEqual(record['status'], 'restored')
        self.assertEqual(record['restores'][0]['phase'], 'starting')
        self.assertEqual(self.system.units[Q.H.WORKER]['ActiveState'], 'active')
        self.assertEqual(self.actor('probe').run('probe')['status'], 'passed')
        self.system.fail['start'] = 2
        failed = self.actor(host='recent-02').run('act')
        self.assertEqual(failed['status'], 'failed')
        self.assertEqual(self.system.units[Q.H.WORKER]['ActiveState'], 'inactive')
        with self.assertRaisesRegex(ValueError, 'unrestored'):
            self.actor('probe').run('probe')
        with self.assertRaisesRegex(ValueError, 'unrestored'):
            self.actor(host='recent-03').run('act')
        reconciled = self.actor(host='recent-02').run('reconcile')
        self.assertEqual(reconciled['status'], 'restored')
        self.assertEqual(len(reconciled['restores']), 2)
        self.assertEqual(self.system.units[Q.H.WORKER]['ActiveState'], 'active')
        self.assertEqual(self.actor('probe').run('probe')['status'], 'passed')

    def test_termination_mid_effect_restores_the_owned_service(self):
        actor = self.actor()
        self.system.hooks['start'] = lambda: os.kill(os.getpid(), signal.SIGTERM)
        record = actor.run('act')
        self.assertEqual(record['error_type'], 'Interrupted')
        self.assertEqual(record['status'], 'restored')
        self.assertEqual(self.system.units[Q.H.WORKER]['ActiveState'], 'active')
        self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)

    def test_reconcile_restores_only_owned_state_and_fences_foreign_state(self):
        actor = self.actor()
        Q.REMOTE.mkdir()
        original = Q.unit_identity(self.system, Q.H.WORKER)
        exited = subprocess.Popen([sys.executable, '-c', 'pass']); owner = Q.process_identity(exited.pid); exited.wait()
        def stale(phase, **extra):
            record = {'status':'running', 'request':actor.request, 'request_sha256':actor.sha, 'owner':owner,
                      'original':original, 'phase':phase, 'restores':[], **extra}
            durable.atomic_json(actor.path, record)
            durable.atomic_json(Q.REMOTE/'latest.json', {'request_sha256':actor.sha})
        stale('intent')
        self.system.units[Q.H.WORKER] = {'ActiveState':'inactive', 'MainPID':'0'}
        with self.assertRaisesRegex(ValueError, 'without an owned effect'):
            actor.run('reconcile')  # stopped by someone else: never started by this owner
        stale('stopping')
        self.system.units[Q.H.WORKER] = {'ActiveState':'active', 'MainPID':'100'}
        self.assertEqual(actor.run('reconcile')['status'], 'restored')  # the stop never took effect
        stale('stopped')
        self.system.units[Q.H.WORKER] = {'ActiveState':'active', 'MainPID':'777'}
        self.system.exe[777] = '9'*64
        with self.assertRaisesRegex(ValueError, 'differs from the owned pre-fault service'):
            actor.run('reconcile')  # a different executable is foreign
        stale('stopped')
        self.system.units[Q.H.WORKER] = {'ActiveState':'inactive', 'MainPID':'0'}
        self.system.ready = {'ready':False}
        with patch.object(Q, 'RECONCILE_SECONDS', 2):
            with self.assertRaises(Q.Budget):
                actor.run('reconcile')  # not ready yet: stays unrestored
        self.assertEqual(actor.status()['status'], 'running')
        self.system.ready = {'ready':True}
        self.assertEqual(actor.run('reconcile')['status'], 'restored')
        stale('stopped', restores=[{}, {}, {}])
        with self.assertRaisesRegex(ValueError, 'attempts exhausted'):
            actor.run('reconcile')
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        self.addCleanup(lambda: child.poll() is None and child.kill())
        with patch.object(Q, 'process_identity', lambda pid: {'pid':pid, 'start_ticks':int(Q.stat_fields(pid)[19]),
                                                               'session':0, 'boot_id':Q.boot_id()}):
            stale('stopped', owner=Q.process_identity(child.pid))
            with self.assertRaisesRegex(ValueError, 'still alive'):
                actor.run('reconcile')

    def test_absent_action_is_refused_for_good_and_lock_holders_fence(self):
        actor = self.actor()
        with Q.ProductionLock({'type':'pinned_host', 'machine_id':'b'*32}):
            with self.assertRaisesRegex(ValueError, 'still holds the host lock'):
                actor.run('reconcile')
            with self.assertRaisesRegex(ValueError, 'held by another owner'):
                actor.run('act')
            with self.assertRaisesRegex(ValueError, 'held by another owner'):
                self.actor('probe').run('probe')
        refused = actor.run('reconcile')
        self.assertEqual(refused['status'], 'refused')
        with self.assertRaisesRegex(ValueError, 'never replay'):
            actor.run('act')  # a delayed delivery after reconciliation
        self.assertEqual(self.system.calls, [c for c in self.system.calls if c[:2] != ['systemctl', '--no-block']])

    def test_read_only_preparation_and_identity_probes(self):
        self.system.control_status = {'active':{'map_sha256':'1'*64}, 'preparing':{'map_sha256':'2'*64, 'phase':'building'}}
        observed = self.actor('await-preparation').run('probe')
        self.assertEqual((observed['observed'], observed['control']['preparing']['map_sha256']), ('preparing', '2'*64))
        self.system.control_status = {'active':{'map_sha256':'1'*64}, 'preparing':None}
        with patch.object(Q, 'REMOTE_BOUND', dict(Q.REMOTE_BOUND, **{'await-preparation':1})):
            self.assertEqual(self.actor('await-preparation').run('probe')['status'], 'not-observed')
        self.assertEqual(self.actor('control-status').run('probe')['control']['active_map_sha256'], '1'*64)
        with patch.object(Q, 'rollback_identity', lambda transaction: {'transaction':transaction}):
            reply = self.actor('identity').run('probe')
        self.assertEqual(reply['rollback'], {'transaction':'transparent-schema-1'})
        self.assertIn('findings', reply)
        self.assertNotIn(['systemctl', '--no-block'], [c[:2] for c in self.system.calls])


FAKE_RATE = r"""
import json, sys, time
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
qps, seconds = int(args['--qps']), int(args['--seconds'])
fixture = json.load(open(args['--fixture']))
print(json.dumps({'event':'ready', 'fixture_sha256':fixture['sha'], 'targets':[1, 1, 1, 1]}), flush=True)
for k in range(qps*seconds):
    print(json.dumps({'event':'query', 'logical_id':k, 'exact':True, 'geometry':'recent' if k % 10 < 8 else 'archive',
                      'http_seconds':fixture['latency']}), flush=True)
"""


def ready(incarnation='i1', map_sha256='m'*64, binary='x'):
    return {'ready':True, 'binary_sha256':binary, 'map_sha256':map_sha256, 'incarnation':incarnation, 'started_unix':1,
            'worker_id':'recent-1', 'role':'recent-replica', 'assignment_sha256':'a'*64, 'worker_assignment_sha256':'w'*64}


def public(sha='m'*64, sealed=None, height=100):
    return {'sha256':sha, 'sealed':sealed if sealed is not None else {'0':'s0', '1':'s1'},
            'tail':{'shard_id':2, 'end_height':height, 'terminal_block_hash':'h', 'manifest_digest':'t'}}


class FlowTests(unittest.TestCase):
    """Owner activities with fake clients and no live service."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root/'machine').write_text('c'*32)
        (self.root/'rate.py').write_text(FAKE_RATE)
        (self.root/'rate').write_text('#!/bin/sh\nexec %s %s "$@"\n' % (sys.executable, self.root/'rate.py'))
        os.chmod(self.root/'rate', 0o755)
        for item in (patch.object(Q, 'ROOT', self.root/'qualification'), patch.object(Q, 'OWNERS', self.root/'owners'),
                     patch.object(Q.ProductionLock, 'PATH', self.root/'lock'), patch.object(Q.ProductionLock, 'MACHINE_ID', self.root/'machine'),
                     patch.object(Q.ProductionLock, 'ROOT_UID', os.geteuid()), patch.object(Q, 'artifact', lambda name: self.root/'rate'),
                     patch.dict(os.environ)):
            item.start(); self.addCleanup(item.stop)
        self.inventory = SimpleNamespace(lock={'type':'pinned_host', 'machine_id':'c'*32})

    def owner(self, value, latency=.1):
        q = Q.Qualification(self.inventory, value, durable.digest(value))
        q.directory.mkdir(parents=True)
        Q.OWNERS.mkdir(exist_ok=True)
        fixture = self.root/'fixture.json'
        fixture.write_text(json.dumps({'sha':FIXTURE, 'latency':latency}))
        q.spec = {'load':{'fixture':{'path':str(fixture), 'sha256':FIXTURE}}}
        q.hosts = [{'host':'coordinator', 'role':'coordinator', 'worker':None},
                   {'host':'recent-01', 'role':'worker', 'worker':{'id':'recent-1', 'role':'recent-replica'}}]
        if getattr(self, 'held', None):
            self.held.__exit__(None, None, None)  # one production lock holder at a time
        q.lock = self.held = Q.ProductionLock(self.inventory.lock).__enter__()
        self.addCleanup(q.lock.__exit__, None, None, None)
        q.record = {'kind':'deployed-qualification', 'request_sha256':q.sha, 'plan_sha256':q.plan_sha, 'status':'running', 'children':[]}
        q.save(q.record)
        q.health = lambda stream, allowed=(): None
        q.reconcile_hosts = lambda operation='probe': {}
        return q

    def fresh(self, q, violations):
        def observe(window, stream):
            if window.tip is None:
                window.node(1, 0, 0)
            window.node(window.tip+1, 1, 1); window.serving('public', window.tip, 2, 2); window.serving('recent-1', window.tip, 2, 2)
            return violations.pop(0) if violations else None
        q.observe_freshness = observe

    def test_staged_load_escalates_only_after_a_passing_stage(self):
        stages = ({'qps':5, 'seconds':10, 'processes':5}, {'qps':20, 'seconds':10, 'processes':5})
        with patch.object(Q, 'LOAD_STAGES', stages):
            q = self.owner(request()); self.fresh(q, [])
            result = q.staged_load()
            self.assertEqual(result['status'], 'passed', result['failures'])
            self.assertEqual([s['qps'] for s in result['stages']], [5, 20])
            self.assertTrue(all(c['status'] == 'exited' and c['exit_code'] == 0 for c in q.record['children']))
            self.assertIsNone(q.monitor)
            slow = self.owner(dict(request(), attempt=2), latency=.9); self.fresh(slow, [])
            refused = slow.staged_load()
            self.assertEqual(refused['status'], 'failed')
            self.assertEqual(len(refused['stages']), 1)
            self.assertIn('escalation refused', refused['failures'])
            self.assertTrue((slow.directory/'raw/stage-0-5qps/queries-4.jsonl').exists())
            self.assertFalse((slow.directory/'raw/stage-1-20qps').exists())
            short = self.owner(dict(request(), attempt=3)); self.fresh(short, [])
            short.deadline = Q.Deadline(100)
            with self.assertRaisesRegex(Q.Budget, 'staged load at 5 QPS'):
                short.staged_load()  # never starts a stage that cannot finish
            self.assertEqual(short.record['children'], [])

    def test_freshness_violation_refuses_staged_load(self):
        q = self.owner(request())
        q.observe_freshness = lambda window, stream: 'public freshness exceeded 30s at block 2'
        with self.assertRaisesRegex(ValueError, 'does not permit staged load'):
            q.staged_load()
        self.assertEqual(q.record['children'], [])

    def test_remote_health_runs_off_the_observation_path(self):
        q = self.owner(request())
        del q.health
        q.baseline = {'local':{'units':{}}, 'remote':{'router':{'resources':{'oom':0, 'oom_kill':0, 'restarts':'0', 'pid':'9'}}}}
        stream = open(os.devnull, 'w'); self.addCleanup(stream.close)
        sample = {'unix':1, 'memory_available':.5, 'disk_available':{'/':.5}, 'units':{}}
        with patch.object(Q, 'local_resources', lambda commands=None: sample), patch.object(Q, 'commands', FakeCommands):
            monitor = Q.RemoteMonitor(q)
            monitor.results = [(1, {'router':{'resources':{'oom':0, 'oom_kill':1, 'restarts':'0', 'pid':'9'}}})]
            q.monitor = monitor
            with self.assertRaisesRegex(ValueError, 'remote restart or OOM: router'):
                q.health(stream)
            monitor.error = 'Unknown: remote qualification probe on router timed out'
            with self.assertRaisesRegex(ValueError, 'remote owner reconciliation failed'):
                q.health(stream)
            monitor.error, monitor.last = None, time.monotonic()-Q.REMOTE_HEALTH_STALE-1
            with self.assertRaisesRegex(ValueError, 'stale'):
                q.health(stream)
            monitor.last = time.monotonic()
            q.health(stream)
        def failing(operation='probe'):
            raise Q.Unknown('lost')
        q.reconcile_hosts = failing
        with Q.RemoteMonitor(q) as running:
            running.thread.join(5)
            self.assertIn('Unknown: lost', running.take()[1])
        self.assertIsNone(q.monitor)

    def test_observations_retain_raw_responses_and_timing(self):
        q = self.owner(request('freshness'))
        q.hosts[1]['worker']['upstream'] = '10.0.0.1:8093'
        shards = json.dumps({'shards':[{'end_height':101, 'terminal_block_hash':'h101'}]}).encode()
        def node_raw(method, params, timeout=3):
            value = 101 if method == 'getblockcount' else 'h%d' % params[0]
            return value, json.dumps({'result':value}).encode()
        def fetch(url, timeout):
            return json.loads(shards), shards, None
        window = Q.FreshnessWindow(['recent-1'], time.monotonic())
        with patch.object(Q, 'node_raw', node_raw), patch.object(Q, 'fetch', fetch), \
                (q.directory/'freshness.jsonl').open('w') as stream:
            self.assertIsNone(q.observe_freshness(window, stream))
        line = json.loads((q.directory/'freshness.jsonl').read_text())
        self.assertEqual([c['call'] for c in line['calls']],
                         ['getblockcount', 'getblockhash', 'public', 'filters', 'getblockhash-public', 'recent-1', 'getblockhash-recent-1'])
        self.assertTrue(all(c['before'] <= c['after'] for c in line['calls']))
        for call in line['calls']:
            self.assertTrue((q.directory/'raw/responses'/call['response_sha256']).exists())
        def disagree(url, timeout):
            value = {'shards':[{'end_height':101 if 'transparent' in url else 100, 'terminal_block_hash':'h'}]}
            return value, json.dumps(value).encode(), None
        with patch.object(Q, 'node_raw', node_raw), patch.object(Q, 'fetch', disagree), open(os.devnull, 'w') as stream:
            with self.assertRaisesRegex(ValueError, 'origins disagree'):
                q.observe_freshness(window, stream)
        reads = []
        def racing(url, timeout):
            reads.append(url)
            height = 101 if len(reads) != 2 else 100  # the filter origin lags once, then agrees
            value = {'shards':[{'end_height':height, 'terminal_block_hash':'h%d' % height}]}
            return value, json.dumps(value).encode(), None
        with patch.object(Q, 'node_raw', node_raw), patch.object(Q, 'fetch', racing), open(os.devnull, 'w') as stream:
            self.assertIsNone(q.observe_freshness(window, stream))
        self.assertEqual(len(reads), 5)  # public, filters, both re-read, one replica

    def test_freshness_run_retains_failed_intervals_and_resets_on_errors(self):
        q = self.owner(request('freshness'))
        outcomes = [ValueError('public endpoint is not canonical'), 'observation gap of 12.0 s exceeds 10 s', None]
        def observe(window, stream):
            outcome = outcomes.pop(0) if outcomes else None
            if isinstance(outcome, Exception):
                raise outcome
            if window.tip is None:
                window.node(1, 0, 0)
            for _ in range(300):
                window.node(window.tip+1, 1, 1); window.serving('public', window.tip, 2, 2); window.serving('recent-1', window.tip, 2, 2)
            return outcome
        q.observe_freshness = observe
        with patch.object(Q, 'FRESHNESS', dict(Q.FRESHNESS, seconds=0)):
            result = q.freshness_window()
        self.assertEqual(result['status'], 'passed')
        intervals = [json.loads(l) for l in (q.directory/'raw/intervals.jsonl').read_text().splitlines()]
        self.assertEqual([i['reason'][:30] for i in intervals],
                         ['observation failed: ValueError', 'observation gap of 12.0 s exce'])
        self.assertEqual([i['blocks'] for i in intervals], [0, 300])  # the failed interval keeps its evidence
        self.assertEqual(result['freshness']['blocks'], 300)

    def fault_owner(self, value):
        q = self.owner(value)
        q.baseline = {'ready':{'recent-1':ready()}, 'public':public(), 'local':{'units':{}}, 'remote':{},
                      'coordinator':{'rollback':{'complete_sha256':'r'}}}
        q.remote_health = lambda stream, allowed=(): {}
        q.all_hosts = lambda operation, running: {'coordinator':{'rollback':{'complete_sha256':'r'}}}
        q.probe = lambda directory: {'query':'passed'}
        q.readiness = lambda: {'recent-1':ready()}
        q.public_map = lambda: public()
        q.hosts.append({'host':'router', 'role':'router', 'worker':None})
        q.deadline = Q.Deadline(Q.RUNTIME['fault']-Q.OWNER_MARGIN)
        return q

    def test_publication_interruption_lands_inside_a_preparation_and_restores_the_publisher(self):
        q = self.fault_owner(request('fault', fault='publication-interruption'))
        system = FakeSystem(Q.PUBLISHER)
        calls, order = [], []
        preparing = {'map_sha256':'2'*64, 'phase':'building'}
        def remote(host, operation, action):
            calls.append((host, operation, action))
            order.append(operation)
            if operation == 'await-preparation':
                return None, {'status':'passed', 'observed':'preparing', 'control':{'preparing':preparing, 'active_map_sha256':'1'*64}}
            state = system.units[Q.PUBLISHER]['ActiveState']
            order.append('publisher '+state)
            return None, {'status':'passed', 'control':{'active_map_sha256':self.after_map, 'preparing':None}}
        q.remote = remote
        self.after_map = '1'*64
        identity, executable = fake_processes(system)
        with identity, executable, patch.object(Q, 'commands', lambda deadline=None: system), \
                patch.object(Q, 'local_resources', lambda commands=None: {'units':{}}), patch.object(Q, 'node', lambda *a: 100):
            result = q.fault()
            self.assertEqual(result['status'], 'passed')
            self.assertEqual(order, ['await-preparation', 'control-status', 'publisher inactive'])
            effect = q.status()['effect']
            self.assertTrue(effect['inside_preparation'])
            self.assertEqual([p['phase'] for p in effect['phases']], ['stopping', 'stopped', 'starting', 'verified'])
            self.assertEqual(system.units[Q.PUBLISHER]['ActiveState'], 'active')
            self.assertTrue((q.directory/'raw/owners-before-effect.json').exists())
            late = self.fault_owner(dict(request('fault', fault='publication-interruption'), attempt=2))
            late.remote = remote
            self.after_map = '2'*64  # the preparation had already activated
            with self.assertRaisesRegex(ValueError, 'not proven inside'):
                late.fault()
            self.assertEqual(late.record['effect']['status'], 'passed')  # publisher restored; the fault failed
            self.assertEqual(system.units[Q.PUBLISHER]['ActiveState'], 'active')
            broken = self.fault_owner(dict(request('fault', fault='publication-interruption'), attempt=3))
            broken.remote = remote
            system.fail['start'] = 2
            with self.assertRaises(subprocess.CalledProcessError):
                broken.fault()
            self.assertEqual(broken.record['effect']['phase'], 'starting')
            self.assertEqual(broken.record['effect']['status'], 'starting')  # unrestored: stays fenced
            self.assertIn('restore_error', broken.record['effect'])

    def test_remote_fault_unknown_transport_stays_fenced(self):
        q = self.fault_owner(request('fault', fault='recent-worker-loss', target='recent-01'))
        def remote(host, operation, action):
            raise Q.Unknown('remote qualification act on recent-01 timed out; outcome unknown')
        q.remote = remote
        with self.assertRaises(Q.Unknown):
            q.fault()
        self.assertEqual(q.status()['remote_actions'], [{'host':'recent-01', 'operation':'stop-start', 'status':'unknown',
                                                         'intent_unix':q.record['remote_actions'][0]['intent_unix'],
                                                         'error':'remote qualification act on recent-01 timed out; outcome unknown'}])
        failed = self.fault_owner(dict(request('fault', fault='recent-worker-loss', target='recent-01'), attempt=2))
        failed.remote = lambda host, operation, action: (None, {'status':'restored', 'phase':'verified', 'error':'unit failed'})
        with self.assertRaisesRegex(ValueError, 'did not pass on recent-01: restored'):
            failed.fault()

    def test_recovery_is_bounded_by_one_deadline(self):
        clock = Clock()
        with patch.object(Q.time, 'monotonic', clock), patch.object(Q.time, 'sleep', clock.sleep):
            q = self.fault_owner(request('fault', fault='router-restart'))
            q.deadline = Q.Deadline(3000)
            raw = q.directory/'raw'; raw.mkdir()
            started = clock()
            def slow(directory):
                clock.now += 950
                return {'query':'passed'}
            q.probe = slow
            with self.assertRaisesRegex(ValueError, r'recovery completed after 900 seconds \(950.0 s\)'):
                q.recover(raw, started, ())
            self.assertGreater(q.deadline.remaining(), 1000)  # the owner bound is restored
            probes = []
            def failing(directory):
                probes.append(clock())
                clock.now += 600
                raise ValueError('canonical encrypted query probe failed')
            q.probe = failing
            started = clock()
            with self.assertRaisesRegex(ValueError, 'within 900 seconds'):
                q.recover(raw, started, ())
            self.assertEqual(len(probes), 1)  # another bounded attempt no longer fits after 600 s
            probes.clear()
            with self.assertRaisesRegex(ValueError, 'within 900 seconds: a recovery attempt needs 330 s'):
                q.recover(raw, clock()-600, ())  # the effect itself used 600 s
            self.assertEqual(probes, [])  # no attempt starts that cannot finish
            clock.now += 1
            q.probe = lambda directory: clock.sleep(100) or {'query':'passed'}
            started = clock()
            result = q.recover(raw, started, ())
            self.assertEqual(result['recovered_seconds'], 100)

    def test_recovery_binds_canonical_publication_not_a_stale_map(self):
        q = self.fault_owner(request('fault', fault='recent-worker-loss', target='recent-01'))
        advanced = public(sha='n'*64, sealed={'0':'s0', '1':'s1', '2':'s2'}, height=150)
        q.identity_check({'recent-1':ready('i2', 'n'*64)}, advanced, ('recent-1',))
        cases = [({'recent-1':ready('i2', 'm'*64)}, advanced, 'canonical public map'),
                 ({'recent-1':ready('i2', 'n'*64, binary='y')}, advanced, 'baseline identity'),
                 ({'recent-1':dict(ready('i2', 'n'*64), assignment_sha256='z'*64)}, advanced, 'baseline identity'),
                 ({'recent-1':ready('i2', 'n'*64)}, public(sha='n'*64, sealed={'0':'s0', '1':'changed'}), 'sealed publication'),
                 ({'recent-1':ready('i1', 'n'*64)}, advanced, 'did not restart')]
        for value, mapping, message in cases:
            with self.assertRaisesRegex(ValueError, message):
                q.identity_check(value, mapping, ('recent-1',))
        with self.assertRaisesRegex(ValueError, 'unexpected worker restart'):
            q.identity_check({'recent-1':ready('i2', 'n'*64)}, advanced, ())

    def test_fault_requires_quiescence_and_unchanged_rollback_baselines(self):
        q = self.fault_owner(request('fault', fault='router-restart'))
        q.remote = lambda host, operation, action: (None, {'status':'passed'})
        q.readiness = lambda: {'recent-1':ready()}
        with patch.object(Q, 'local_resources', lambda commands=None: {'units':{}}), patch.object(Q, 'commands', FakeCommands):
            result = q.fault()
            self.assertEqual(result['status'], 'passed')
            changed = self.fault_owner(dict(request('fault', fault='router-restart'), attempt=2))
            changed.remote = q.remote
            changed.all_hosts = lambda operation, running: {'coordinator':{'rollback':{'complete_sha256':'other'}}}
            with self.assertRaisesRegex(ValueError, 'retained rollback baseline changed: coordinator'):
                changed.fault()
            busy = self.fault_owner(dict(request('fault', fault='router-restart'), attempt=3))
            busy.record['children'].append({'name':'stray', 'status':'running', 'identity':Q.process_identity(os.getpid())})
            with self.assertRaisesRegex(ValueError, 'owned child is still running'):
                busy.fault()
            self.assertNotIn('effect', busy.record)  # no effect without quiescence


class HealthTests(unittest.TestCase):
    def sample(self, memory=.5, restarts='0', oom=0):
        return {'unix':1, 'memory_available':memory, 'disk_available':{'/':.5},
                'units':{Q.PUBLISHER:{'MainPID':'7', 'NRestarts':restarts, 'oom':oom, 'oom_kill':0}}}

    def test_headroom_restarts_oom_and_quality_supervisor(self):
        value = request()
        q = Q.Qualification(SimpleNamespace(lock={}), value, durable.digest(value))
        q.lock = SimpleNamespace(verify=lambda: None)
        q.baseline = {'local':self.sample(), 'remote':{'router':{'resources':{'oom':0, 'oom_kill':0, 'restarts':'0', 'pid':'9'}}}}
        stream = open(os.devnull, 'w'); self.addCleanup(stream.close)
        class Running(FakeCommands):
            main_pid, active = '5', 'active'
        cases = ((self.sample(memory=.19), FakeCommands, 'headroom'), (self.sample(restarts='1'), FakeCommands, 'NRestarts'),
                 (self.sample(oom=1), FakeCommands, 'oom'), (self.sample(), Running, 'quality supervisor'))
        for current, commands, message in cases:
            with patch.object(Q, 'local_resources', lambda commands=None: current), patch.object(Q, 'commands', commands):
                with self.assertRaisesRegex(ValueError, message):
                    q.health(stream)
        with patch.object(Q, 'local_resources', lambda commands=None: self.sample(restarts='1')), patch.object(Q, 'commands', FakeCommands):
            q.health(stream, allowed=(Q.PUBLISHER,))
            q.all_hosts = lambda operation, running: {'router':{'resources':{'oom':0, 'oom_kill':1, 'restarts':'0', 'pid':'9'}}}
            with self.assertRaisesRegex(ValueError, 'remote restart or OOM'):
                q.remote_health(stream)
            q.remote_health(stream, allowed=('router',))


class DeploymentTests(unittest.TestCase):
    """Binding to the exact committed candidate transaction."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.record = journal('transparent-schema-1', 'committed', rollback=False)
        self.policy = self.root/'policy.json'; self.policy.write_text(json.dumps({'mode':'observe'}))
        self.sample = self.root/'sample.json'; self.sample.write_text('{}')
        self.assignment = self.root/'assignment.json'
        self.assignment.write_text(json.dumps({'workers':[{'id':'r1', 'role':'recent-replica', 'upstream':'10.0.0.1:8093'},
                                                          {'id':'r2', 'role':'recent-replica', 'upstream':'10.0.0.2:8093'},
                                                          {'id':'a1', 'role':'archive-owner', 'upstream':'10.0.0.3:8093'}]}))
        def worker(identifier):
            return {'id':identifier, 'binary_sha256':'5'*64, 'map_sha256':'6'*64}
        self.spec = {'version':2, 'candidate_sha':Q.C.SOURCE_SHA, 'source_sha':'c'*40, 'publication_sha256':'d'*64,
                     'inventory':{'path':'/inventory.json', 'sha256':'1'*64},
                     'assignment':{'path':str(self.assignment), 'sha256':'2'*64},
                     'load':{'fixture':{'path':'/fixture.json', 'sha256':'3'*64}, 'policy':{'path':str(self.policy), 'sha256':'4'*64}},
                     'routing':{'recovery':{'v11':{'binary':str(Q.C.path('transparent-loadtest')), 'binary_sha256':Q.C.ARTIFACTS['transparent-loadtest'],
                                                   'sample':str(self.sample), 'sample_sha256':Q.checksum(self.sample)}}},
                     'hosts':[{'host':'coordinator', 'plan':{'role':'coordinator', 'machine_id':'c'*32, 'worker':None}},
                              {'host':'router', 'plan':{'role':'router', 'machine_id':'d'*32, 'worker':None}},
                              *({'host':h, 'plan':{'role':'worker', 'machine_id':m*32, 'worker':worker(h)}} for h, m in (('r1', 'e'), ('r2', 'f'), ('a1', '9')))]}
        lock = {'type':'pinned_host', 'machine_id':'c'*32}
        fake = SimpleNamespace(checked=lambda value: value['path'],
                               descriptors=SimpleNamespace(load_inventory=lambda path: SimpleNamespace(lock=lock)))
        self.records = {'transparent-schema-1':self.record}
        self.latest = 'transparent-schema-1'
        for item in (patch.object(Q, 'product', lambda: fake), patch.object(Q, 'latest_transaction', lambda: self.latest),
                     patch.object(Q, 'load_record', lambda identifier: self.records[identifier]),
                     patch.object(Q, 'candidate_spec', lambda record: ({'path':'/s.json', 'sha256':'e'*64}, self.spec))):
            item.start(); self.addCleanup(item.stop)
        self.inventory = SimpleNamespace(lock=lock)

    def qualification(self, **extra):
        value = request(**extra) if 'kind' not in extra else request(extra.pop('kind'), **extra)
        value['recipe_sha256'] = self.record['recipe_sha256']
        return Q.Qualification(self.inventory, value, durable.digest(value))

    def test_binds_committed_candidate_and_roles(self):
        observed = self.qualification().deployment()
        self.assertEqual(observed['transaction'], 'transparent-schema-1')
        self.assertEqual({h['host']:(h['worker'] or {}).get('role') for h in observed['hosts']},
                         {'coordinator':None, 'router':None, 'r1':'recent-replica', 'r2':'recent-replica', 'a1':'archive-owner'})

    def test_refuses_drifted_deployments(self):
        self.latest = 'transparent-schema-9'
        with self.assertRaisesRegex(ValueError, 'latest schema transaction'):
            self.qualification().deployment()
        self.latest = 'transparent-schema-1'
        self.policy.write_text(json.dumps({'mode':'act'}))
        with self.assertRaisesRegex(ValueError, 'observe-only'):
            self.qualification().deployment()
        self.policy.write_text(json.dumps({'mode':'observe'}))
        self.sample.write_text('{"changed":1}')
        with self.assertRaisesRegex(ValueError, 'recovery reader or sample'):
            self.qualification().deployment()
        self.sample.write_text('{}')
        self.spec['hosts'] = [h for h in self.spec['hosts'] if h['host'] != 'router']
        with self.assertRaisesRegex(ValueError, 'router'):
            self.qualification().deployment()

    def test_fault_targets_must_match_deployed_roles(self):
        q = self.qualification(kind='fault', fault='recent-worker-loss', target='a1')
        q.deployment()
        with self.assertRaisesRegex(ValueError, 'recent-replica'):
            q.kind_preflight()
        q = self.qualification(kind='fault', fault='archive-restart', target='a1')
        q.deployment(); q.kind_preflight()

    def test_rollback_redeploy_binds_both_journals(self):
        original = journal('transparent-schema-0', 'rolled-back', value=self.record['recipe'])
        redeploy = journal('transparent-schema-1', 'committed', rollback=False, created=7000, value=self.record['recipe'])
        self.records = {'transparent-schema-0':original, 'transparent-schema-1':redeploy}
        q = self.qualification(kind='fault', fault='rollback-redeploy', rolled_back_transaction='transparent-schema-0')
        q.deployment()
        self.assertEqual(q.rollback_evidence['rolled_back_transaction'], 'transparent-schema-0')
        self.records['transparent-schema-0'] = dict(original, status='rollback-failed')
        with self.assertRaises(ValueError):
            self.qualification(kind='fault', fault='rollback-redeploy', rolled_back_transaction='transparent-schema-0').deployment()


class RemoteCallTests(unittest.TestCase):
    """The coordinator half of a remote call: closed argv, bounded, unknown on loss."""
    def setUp(self):
        value = request('fault', fault='router-restart')
        self.q = Q.Qualification(SimpleNamespace(lock={'type':'pinned_host', 'machine_id':'c'*32}), value, durable.digest(value))
        self.q.hosts = [{'host':'router', 'role':'router', 'machine_id':'d'*32, 'worker':None}]
        self.q.ssh = SimpleNamespace(hosts={'router':{'machine_id':'d'*32}}, ssh={'mode':'config'})
        self.runs = []
        patcher = patch.object(Q, 'SSHExecutor', lambda inventory: SimpleNamespace(transport=lambda host: ['ssh', host]))
        patcher.start(); self.addCleanup(patcher.stop)

    def call(self, reply, code=0, raises=None):
        def run(argv, **options):
            self.runs.append((argv, options))
            if raises:
                raise raises
            return SimpleNamespace(returncode=code, stdout=reply)
        with patch.object(Q.subprocess, 'run', run):
            return self.q.remote('router', 'restart', 'act')

    def test_reply_identity_and_transport_loss(self):
        request_ = Q.remote_validate({'version':1, 'source_sha':'a'*40, 'qualification_sha256':self.q.sha, 'host':'router',
                                      'role':'router', 'machine_id':'d'*32, 'coordinator_machine_id':'c'*32,
                                      'transaction':'transparent-schema-1', 'operation':'restart'})
        good = json.dumps({'request_sha256':durable.digest(request_), 'status':'passed'}).encode()
        self.assertEqual(self.call(good)[1]['status'], 'passed')
        argv, options = self.runs[-1]
        self.assertEqual(argv[:4], ['ssh', '-oControlMaster=no', '-oControlPath=none', 'router'])
        self.assertIn('schema-qualify-remote --action act --request-sha256 '+durable.digest(request_), argv[-1])
        self.assertEqual(options['timeout'], Q.REMOTE_BOUND['restart']+Q.SSH_MARGIN+Q.RESTORE_SECONDS)
        with self.assertRaises(Q.Unknown):
            self.call(b'', code=255)
        with self.assertRaises(Q.Unknown):
            self.call(b'', raises=subprocess.TimeoutExpired('ssh', 1))
        with self.assertRaises(Q.Unknown):
            self.call(json.dumps({'request_sha256':'0'*64, 'status':'passed'}).encode())  # not this request's reply
        self.q.ssh.hosts['router']['machine_id'] = 'e'*32
        with self.assertRaisesRegex(ValueError, 'remote machine differs'):
            self.call(good)


class CliTests(unittest.TestCase):
    def test_closed_wrapper_commands(self):
        parser = cli.parser()
        args = parser.parse_args(['schema-qualify-run', '--request', 'r.json', '--request-sha256', 'a'*64, '--expect-plan-sha256', 'b'*64])
        self.assertEqual(args.expect_plan_sha256, 'b'*64)
        for argv in (['schema-qualify-run', '--request', 'r', '--request-sha256', 'a'*64],
                     ['schema-qualify-status', '--request-sha256', 'a'*64, '--host', 'router'],
                     ['schema-qualify-remote', '--action', 'shell', '--request-sha256', 'a'*64],
                     ['schema-qualify-run', '--request', 'r', '--request-sha256', 'a'*64, '--expect-plan-sha256', 'b'*64, '--url', 'x']):
            with self.assertRaises(SystemExit):
                with patch('sys.stderr'):
                    parser.parse_args(argv)

    def test_plan_through_wrapper_without_live_access(self):
        with tempfile.TemporaryDirectory() as temp:
            value = request('fault', fault='router-restart')
            path = Path(temp)/'request.json'
            path.write_text(json.dumps(value))
            inventory = Path(temp)/'inventory.json'
            inventory.write_text(json.dumps({'hosts':{'coordinator':{}}, 'ssh':{'mode':'config'},
                                             'lock':{'type':'pinned_host', 'machine_id':'c'*32}, 'services':{}}))
            output = []
            code = cli.main(['--inventory', str(inventory), 'schema-qualify-plan', '--request', str(path),
                             '--request-sha256', durable.digest(value)], out=output.append)
            self.assertEqual(code, 0, output)
            plan = json.loads(output[0])
            self.assertEqual(output[1], 'plan sha256: '+durable.digest(plan))
            self.assertEqual(plan['activity']['action'], ['router', 'restart'])
            bad = cli.main(['--inventory', str(inventory), 'schema-qualify-plan', '--request', str(path),
                            '--request-sha256', '0'*64], out=output.append)
            self.assertEqual(bad, 1)


if __name__ == '__main__':
    unittest.main()
