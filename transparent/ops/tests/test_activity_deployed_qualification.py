"""Fail-closed deployed qualification checks. Fixtures here never qualify anything.

Evaluators run against synthetic raw receipts in the native output formats;
owner lifecycle tests use real child processes, locks and the shared fence.
Live SSH, systemd, HTTPS and the candidate executables are separate gates.
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
        self.assertIn('RuntimeMaxSec=1800', load['properties'])

    def test_targets_and_compositions_are_the_reviewed_values(self):
        self.assertEqual(Q.P95_TARGETS['small-active'], 5); self.assertEqual(Q.P95_TARGETS['restore-6m'], 10)
        self.assertEqual(Q.P95_TARGETS['multi-script'], 60); self.assertEqual(Q.P95_TARGETS['unused'], 15)
        self.assertEqual({l:sum(c.values()) for l, c in Q.COMPOSITION.items()}, {8:8, 20:20, 40:40})
        self.assertTrue(all(Q.HEAVY in c for c in Q.COMPOSITION.values()))
        scenario = json.loads((ROOT/'transparent/tools/transparent-loadtest/scenarios/mixed-20-sustained.json').read_text())
        self.assertEqual(Q.COMPOSITION[20], scenario['profiles'])


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
        processes = rate_lines(20, 600); processes[2][5]['exact'] = False
        self.assertIn('logical query failures', self.evaluate(processes)['failures'])

    def test_recovered_retry_counts_as_transport_attempt(self):
        result = self.evaluate(rate_lines(20, 600, error_then_success=30))
        self.assertEqual(result['logical_failures'], 0)
        self.assertEqual(result['status'], 'passed')
        failed = self.evaluate(rate_lines(20, 600, error_then_success=200))
        self.assertIn('transport attempt failures at or above 1 percent', failed['failures'])

    def test_rate_latency_mix_and_fixture_gates(self):
        self.assertIn('completed rate below 95 percent of requested', self.evaluate(rate_lines(20, 500), 20, 600)['failures'])
        self.assertIn('latency gate failed', self.evaluate(rate_lines(20, 600, http=.7))['failures'])
        self.assertIn('recent/archive mix outside 80/20', self.evaluate(rate_lines(20, 600, recent=5))['failures'])
        self.assertEqual(self.evaluate(rate_lines(20, 600, fixture='e'*64))['status'], 'failed')
        self.assertEqual(self.evaluate([])['status'], 'failed')


class FreshnessTests(unittest.TestCase):
    def advance(self, window, now, *, public=5, replica=20, blocks=1, late=None):
        for _ in range(blocks):
            now += 75
            window.node(window.tip+1, now)
            delay = late if late else public
            window.serving('public', window.tip, now+delay)
            for name in window.replicas:
                window.serving(name, window.tip, now+replica)
        return now

    def test_requires_six_hours_and_three_hundred_blocks(self):
        window = Q.FreshnessWindow(['recent-1', 'recent-2'], 0)
        window.node(100, 0)
        now = self.advance(window, 0, blocks=287)
        self.assertGreaterEqual(now, 21525)
        self.assertFalse(window.qualifies(21600))  # six hours, 287 blocks
        now = self.advance(window, now, blocks=13)
        self.assertIsNone(window.violation(now+30))
        self.assertTrue(window.qualifies(now+30))
        short = Q.FreshnessWindow(['recent-1'], 0); short.node(1, 0)
        for _ in range(300):
            short.node(short.tip+1, 1); short.serving('public', short.tip, 2); short.serving('recent-1', short.tip, 2)
        self.assertFalse(short.qualifies(100))

    def test_violation_resets_credit_and_preserves_failed_interval(self):
        window = Q.FreshnessWindow(['recent-1'], 0)
        window.node(10, 0)
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

    def test_overdue_replica_and_reorganization(self):
        window = Q.FreshnessWindow(['recent-1'], 0)
        window.node(10, 0); window.node(11, 100); window.serving('public', 11, 101)
        self.assertIsNone(window.violation(159))
        self.assertIn('recent-1 freshness exceeded 60s', window.violation(161))
        window = Q.FreshnessWindow(['recent-1'], 0)
        window.node(10, 0); window.node(11, 5)
        window.node(9, 10)
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

    def trial(self, *, per_profile=20, seconds=2.0, heavy='exact', missing=0, refused=0, metrics=None, t0=1000.0):
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
        write_lines(self.directory/'wallets.ndjson', wallets)
        requests = [{'type':'request', 'id':1, 'status':503 if i < refused else 200, 'bytes_up':10, 'bytes_down':20} for i in range(100)]
        write_lines(self.directory/'requests.ndjson', requests)
        write_lines(self.directory/'metrics.ndjson', metrics if metrics is not None else
                    [{'at':t0, 'target':'recent-1', 'text':metric_text()}, {'at':t0+5, 'target':'recent-1', 'text':metric_text()},
                     {'at':t0, 'target':'load-client', 'processes':[]}])
        return Q.evaluate_capacity(self.directory, 8, t0+3700, True, ['recent-1'])

    def test_completed_exact_outcomes_only_count(self):
        result = self.trial()
        self.assertEqual(result['status'], 'passed', result['failures'])
        self.assertEqual(result['exact'], 141)
        self.assertEqual(result['profiles']['small-active']['p95_seconds'], 2.0)
        self.assertEqual(result['workers']['recent-1']['maximum_queue_depth'], 3)
        self.assertEqual(result['payload_bytes'], {'up':1000, 'down':2000})
        self.assertAlmostEqual(result['sustained_exact_per_second'], 141/3600)

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
        early = Q.evaluate_capacity(self.directory, 8, 1000+3599, True, ['recent-1'])
        self.assertIn('trial did not complete its 60-minute sustained window', early['failures'])
        stopped = Q.evaluate_capacity(self.directory, 8, 1000+3700, False, ['recent-1'])
        self.assertEqual(stopped['status'], 'failed')


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
    active = 'inactive'
    main_pid = '0'
    empty = True
    calls = []

    def state(self, unit):
        return {'ActiveState':self.active, 'MainPID':self.main_pid, 'NRestarts':'0', 'ControlGroup':''}

    def empty_cgroup(self, state):
        return self.empty

    def unit(self, action, *units):
        FakeCommands.calls.append((action, units))

    def service_resources(self, unit, root):
        return {'memory_available':8, 'memory_total':10, 'disk_available':8, 'disk_total':10, 'oom':0, 'oom_kill':0,
                'restarts':'0', 'pid':'7'}


class OwnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root/'machine').write_text('c'*32)
        self.inventory = SimpleNamespace(lock={'type':'pinned_host', 'machine_id':'c'*32})
        FakeCommands.active, FakeCommands.main_pid, FakeCommands.empty, FakeCommands.calls = 'inactive', '0', True, []
        for item in (patch.object(Q, 'OWNERS', self.root/'owners'), patch.object(schema_fence, 'INPUT_STAGING', self.root/'owners'),
                     patch.object(schema_fence, 'HOST_ACTIONS', self.root/'host-actions'),
                     patch.object(Q, 'ROOT', self.root/'qualification'), patch.object(Q, 'SOURCES', self.root/'sources'),
                     patch.object(Q.ProductionLock, 'PATH', self.root/'lock'),
                     patch.object(Q.ProductionLock, 'MACHINE_ID', self.root/'machine'),
                     patch.object(Q.ProductionLock, 'ROOT_UID', os.geteuid()), patch.object(Q.H, 'Commands', FakeCommands),
                     patch.dict(os.environ)):
            item.start(); self.addCleanup(item.stop)
        self.assertFalse((schema_fence.SCHEMA_STATE/schema_fence.SCHEMA_POINTER).exists())
        self.observed = {'deployment':{'transaction':'transparent-schema-1', 'hosts':[]}, 'local':{}, 'remote':{}, 'ready':{}}
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

    def test_reconcile_waits_for_unit_and_owned_descendants(self):
        q = self.owner()
        self.launch(q)
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], start_new_session=True)
        self.addCleanup(lambda: child.poll() is None and child.kill())
        record = q.status()
        record.update(status='running', children=[{'name':'client', 'status':'running', 'identity':Q.process_identity(child.pid)}])
        q.save(record)
        FakeCommands.main_pid, FakeCommands.active = '42', 'active'
        with self.assertRaisesRegex(ValueError, 'unit still runs'):
            q.run('reconcile')
        FakeCommands.main_pid, FakeCommands.active = '0', 'inactive'
        with self.assertRaisesRegex(ValueError, 'still alive'):
            q.run('reconcile')
        os.killpg(child.pid, signal.SIGKILL); child.wait()
        reconciled = q.run('reconcile')
        self.assertEqual(reconciled['outcome'], 'interrupted')
        self.assertEqual(json.loads((q.directory/'result.json').read_text())['status'], 'interrupted')
        schema_fence.local_schema_fence()

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
        q.hosts = [{'host':'recent-01', 'role':'worker', 'worker':{'id':'recent-1', 'role':'recent-replica'}}]
        if getattr(self, 'held', None):
            self.held.__exit__(None, None, None)  # one production lock holder at a time
        q.lock = self.held = Q.ProductionLock(self.inventory.lock).__enter__()
        self.addCleanup(q.lock.__exit__, None, None, None)
        q.record = {'kind':'deployed-qualification', 'request_sha256':q.sha, 'plan_sha256':q.plan_sha, 'status':'running', 'children':[]}
        q.save(q.record)
        q.health = lambda stream, allowed=(): None
        return q

    def fresh(self, q, violations):
        def observe(window, stream):
            if window.tip is None:
                window.node(1, 0)
            window.node(window.tip+1, 1); window.serving('public', window.tip, 2); window.serving('recent-1', window.tip, 2)
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
            slow = self.owner(dict(request(), attempt=2), latency=.9); self.fresh(slow, [])
            refused = slow.staged_load()
            self.assertEqual(refused['status'], 'failed')
            self.assertEqual(len(refused['stages']), 1)
            self.assertIn('escalation refused', refused['failures'])
            self.assertTrue((slow.directory/'raw/stage-0-5qps/queries-4.jsonl').exists())
            self.assertFalse((slow.directory/'raw/stage-1-20qps').exists())

    def test_freshness_violation_refuses_staged_load(self):
        q = self.owner(request())
        q.observe_freshness = lambda window, stream: 'public freshness exceeded 30s at block 2'
        with self.assertRaisesRegex(ValueError, 'does not permit staged load'):
            q.staged_load()
        self.assertEqual(q.record['children'], [])

    def test_fault_records_intent_before_effect_and_enforces_recovery_deadline(self):
        value = request('fault', fault='publication-interruption')
        q = self.owner(value)
        q.baseline = {'ready':{'recent-1':{'binary_sha256':'x', 'map_sha256':'y'}}}
        effects = []
        class Commands(FakeCommands):
            def unit(inner, action, *units):
                effects.append((action, units, json.loads(q.owner_path.read_text())['effect']['status']))
        q.probe = lambda directory: {'query':'passed'}
        q.readiness = lambda: {'recent-1':{'binary_sha256':'x', 'map_sha256':'y'}}
        tips = [{'end_height':99}, {'end_height':100}]
        with patch.object(Q.H, 'Commands', Commands), patch.object(Q, 'local_resources', lambda: {}), \
                patch.object(q, 'probe_all', lambda: {}), patch.object(Q, 'node', lambda method, params: 100), \
                patch.object(Q, 'tail', lambda url: tips.pop(0)), patch.object(Q.time, 'sleep', lambda _: None):
            result = q.fault()
        self.assertEqual(effects, [('restart', (Q.PUBLISHER,), 'starting')])
        self.assertEqual(result['recovery']['attempts'], 2)
        self.assertIn('caught up', json.loads((q.directory/'raw/recovery-01.error.json').read_text())['error'])
        self.assertEqual(result['status'], 'passed')
        self.assertLess(result['recovery']['recovered_seconds'], 900)
        late = self.owner(dict(value, attempt=2))
        late.baseline = q.baseline
        late.readiness = lambda: {'recent-1':{'binary_sha256':'changed', 'map_sha256':'y'}}
        late.probe = lambda directory: {'query':'passed'}
        with patch.object(Q.H, 'Commands', Commands), patch.object(Q, 'RECOVERY_SECONDS', 0), \
                patch.object(Q.time, 'sleep', lambda _: None), patch.object(Q, 'node', lambda method, params: 100):
            with self.assertRaisesRegex(ValueError, 'within 900 seconds'):
                late.fault()
        error = json.loads((late.directory/'raw/recovery-01.error.json').read_text())
        self.assertIn('original baseline identity not preserved', error['error'])


class HealthTests(unittest.TestCase):
    def sample(self, memory=.5, restarts='0', oom=0):
        return {'unix':1, 'memory_available':memory, 'disk_available':{'/':.5},
                'units':{Q.PUBLISHER:{'MainPID':'7', 'NRestarts':restarts, 'oom':oom, 'oom_kill':0}}}

    def test_headroom_restarts_oom_and_quality_supervisor(self):
        value = request()
        q = Q.Qualification(SimpleNamespace(lock={}), value, durable.digest(value))
        q.lock = SimpleNamespace(verify=lambda: None)
        q.baseline = {'local':self.sample(), 'remote':{'router':{'oom':0, 'oom_kill':0, 'restarts':'0', 'pid':'9'}}}
        q.probe_all = lambda: {'router':{'oom':0, 'oom_kill':0, 'restarts':'0', 'pid':'9'}}
        stream = open(os.devnull, 'w'); self.addCleanup(stream.close)
        class Running(FakeCommands):
            main_pid, active = '5', 'active'
        cases = ((self.sample(memory=.19), FakeCommands, 'headroom'), (self.sample(restarts='1'), FakeCommands, 'NRestarts'),
                 (self.sample(oom=1), FakeCommands, 'oom'), (self.sample(), Running, 'quality supervisor'))
        for current, commands, message in cases:
            with patch.object(Q, 'local_resources', lambda: current), patch.object(Q.H, 'Commands', commands):
                with self.assertRaisesRegex(ValueError, message):
                    q.health(stream)
        with patch.object(Q, 'local_resources', lambda: self.sample(restarts='1')), patch.object(Q.H, 'Commands', FakeCommands):
            q.health(stream, allowed=(Q.PUBLISHER,))
            q.next_remote = 0
            q.probe_all = lambda: {'router':{'oom':0, 'oom_kill':1, 'restarts':'0', 'pid':'9'}}
            with self.assertRaisesRegex(ValueError, 'remote restart or OOM'):
                q.health(stream, allowed=(Q.PUBLISHER,))


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


class RemoteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root/'machine').write_text('b'*32)
        FakeCommands.calls = []
        for item in (patch.object(Q, 'REMOTE', self.root/'actions'), patch.object(schema_fence, 'INPUT_STAGING', self.root/'owners'),
                     patch.object(schema_fence, 'HOST_ACTIONS', self.root/'host-actions'),
                     patch.object(Q.ProductionLock, 'PATH', self.root/'lock'),
                     patch.object(Q.ProductionLock, 'MACHINE_ID', self.root/'machine'),
                     patch.object(Q.ProductionLock, 'ROOT_UID', os.geteuid()), patch.object(Q, 'LOSS_HOLD_SECONDS', 0),
                     patch.object(Q.RemoteActor, 'identity', lambda self: None)):
            item.start(); self.addCleanup(item.stop)

    def remote(self, operation='restart', role='worker'):
        return {'version':1, 'source_sha':'a'*40, 'qualification_sha256':'d'*64, 'host':'recent-01', 'role':role,
                'machine_id':'b'*32, 'coordinator_machine_id':'c'*32, 'operation':operation}

    def test_remote_requests_are_closed(self):
        for value in (dict(self.remote(), unit='ssh.service'), self.remote(role='coordinator'),
                      self.remote('stop-start', 'router'), self.remote('kill'), dict(self.remote(), machine_id='c'*32)):
            with self.assertRaises(ValueError):
                Q.remote_validate(value)
        command = Q.remote_command(self.remote(), 'act')
        self.assertEqual(command[3:], ['schema-qualify-remote', '--action', 'act', '--request-sha256', durable.digest(self.remote())])

    def test_intent_precedes_effect_and_is_never_replayed(self):
        actor = Q.RemoteActor(self.remote('stop-start'), FakeCommands())
        seen = []
        class Recording(FakeCommands):
            active = 'active'
            def unit(inner, action, *units):
                seen.append((action, units, json.loads(actor.path.read_text())['status']))
        actor.commands = Recording()
        record = actor.run('act')
        self.assertEqual(record['status'], 'passed')
        self.assertEqual(seen, [('stop', (Q.H.WORKER,), 'running'), ('start', (Q.H.WORKER,), 'running')])
        with self.assertRaisesRegex(ValueError, 'never replay'):
            actor.run('act')
        with self.assertRaises(ValueError):
            Q.RemoteActor(self.remote(), FakeCommands()).run('probe')

    def test_failed_effect_is_retained_and_unfinished_owner_reconciles(self):
        class Failing(FakeCommands):
            def unit(self, action, *units):
                raise subprocess.CalledProcessError(1, 'systemctl')
        actor = Q.RemoteActor(self.remote(), Failing())
        with self.assertRaises(subprocess.CalledProcessError):
            actor.run('act')
        self.assertEqual(actor.status()['status'], 'failed')
        stale = Q.RemoteActor(dict(self.remote(), qualification_sha256='e'*64), FakeCommands())
        Q.REMOTE.mkdir(exist_ok=True)
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        durable.atomic_json(stale.path, {'status':'running', 'owner':Q.process_identity(child.pid)})
        durable.atomic_json(Q.REMOTE/'latest.json', {'request_sha256':stale.sha})
        with self.assertRaisesRegex(ValueError, 'unfinished remote'):
            Q.RemoteActor(self.remote('probe'), FakeCommands()).run('probe')
        with self.assertRaisesRegex(ValueError, 'still alive'):
            stale.run('reconcile')
        child.kill(); child.wait()
        self.assertEqual(stale.run('reconcile')['status'], 'reconciled')
        self.assertEqual(Q.RemoteActor(self.remote('probe'), FakeCommands()).run('probe')['status'], 'passed')


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
