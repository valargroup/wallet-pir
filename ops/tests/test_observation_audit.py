#!/usr/bin/env python3
"""Check the fleet observation acceptance audit against synthetic bundles."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    'audit', ROOT / 'ops/scripts/audit-transparent-observation.py')
audit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit)

WORKERS = ['transparent-pir-recent-01', 'transparent-pir-recent-02',
           'transparent-pir-recent-03', 'transparent-pir-recent-04',
           'transparent-pir-archive-01', 'transparent-pir-archive-02']
BINARY = 'a' * 64
ROSTER = 'b' * 64


class Args:
    """The parsed-argument shape the audit functions read."""

    def __init__(self, observation, **kw):
        self.observation = observation
        self.canary_result = kw.get('canary_result')
        self.upgrade_result = kw.get('upgrade_result')
        self.workers = kw.get('workers', 6)
        self.seconds = kw.get('seconds', audit.DEFAULT_SECONDS)
        self.blocks = kw.get('blocks', audit.DEFAULT_BLOCKS)
        self.freshness_seconds = kw.get('freshness_seconds', audit.DEFAULT_FRESHNESS)
        self.replica_freshness_seconds = kw.get(
            'replica_freshness_seconds', audit.DEFAULT_REPLICA_FRESHNESS)
        self.minimum_queries = kw.get('minimum_queries', audit.MINIMUM_EXACT_QUERIES)
        self.out = None


def facts(restarts=0, oom=0, available=0.5):
    total = 16 * 1024 ** 3
    return {
        'NRestarts': restarts, 'ExecMainStartTimestampMonotonic': 1,
        'oom': oom, 'oom_kill': 0, 'oom_group_kill': 0,
        'MemTotal': total, 'MemAvailable': int(total * available),
    }


def write_worker(root, name, *, passed=True, seconds=50000, blocks=320,
                 samples=10, public=5.0, replica=8.0, restarts=0, oom=0,
                 available=0.5, queries=None, reorg=False, result=True,
                 transport_failures=0, transport_recoveries=0):
    path = root / f'fleet-{name}'
    path.mkdir(parents=True)
    events = [
        {'event': 'routing_availability_baseline',
         'evidence': {'schema': 1, 'epoch': 'e', 'unavailable_events': 0,
                      'available': True}},
        {'event': 'start', 'worker': name, 'minimum_seconds': 43200,
         'minimum_blocks': 300, 'public_budget_seconds': 30.0,
         'replica_budget_seconds': 60.0},
    ]
    for index in range(samples):
        events.append({
            'event': 'worker',
            'facts': facts(restarts if index else 0, oom if index else 0,
                           available if index else 0.5),
            'ready': {'binary_sha256': BINARY, 'ready': True},
            'headless': {'helper_sha256': 'c' * 64},
            'storage': {'helper_sha256': 'd' * 64},
        })
    events.append({'event': 'block_visible', 'height': 1, 'seconds': public})
    events.append({'event': 'canary_block_visible', 'height': 1,
                   'seconds': replica})
    if reorg:
        events.append({'event': 'chain_reorganized', 'old': 1, 'new': 1})
    for _ in range(transport_failures):
        events.append({'event': 'http_transport_failure'})
    for _ in range(transport_recoveries):
        events.append({'event': 'http_transport_recovered'})
    (path / 'samples.ndjson').write_text(
        ''.join(json.dumps(e) + '\n' for e in events))

    if result:
        record = {'event': 'result', 'passed': passed, 'seconds': seconds,
                  'blocks': blocks}
        if passed:
            record |= {
                'worker': name, 'binary_sha256': BINARY, 'roster_sha256': ROSTER,
                'fleet_config_sha256': 'f' * 64, 'fleet_script_sha256': '9' * 64,
                'storage_helper_sha256': 'd' * 64,
                'headless_helper_sha256': 'c' * 64,
                'replica_blocks': blocks,
                'maximum_visibility_seconds': public,
            }
        else:
            record['error'] = 'connection reset'
        (path / 'result.json').write_text(json.dumps(record))

    if queries is not None:
        for index, entries in enumerate(queries):
            lines = [json.dumps(e) for e in entries]
            lines.append(json.dumps({'event': 'result', 'passed': True}))
            (path / f'query-{index}.ndjson').write_text('\n'.join(lines) + '\n')


def exact_queries(count, mismatches=0):
    rows = [{'event': 'query', 'exact': True} for _ in range(count)]
    rows += [{'event': 'query', 'exact': False} for _ in range(mismatches)]
    return rows


def build(tmp, **overrides):
    root = Path(tmp) / 'observation'
    root.mkdir()
    (root / 'provenance.json').write_text(json.dumps(
        {'binary_sha256': BINARY, 'roster_sha256': ROSTER}))
    (root / 'initial-ready.json').write_text(json.dumps({'ready': True}))
    for name in WORKERS:
        kw = dict(overrides.pop(name, {}))
        if name == 'transparent-pir-recent-01':
            kw.setdefault('queries', [exact_queries(2000), exact_queries(2000)])
        write_worker(root, name, **kw)
    return root


class AuditTest(unittest.TestCase):
    def run_audit(self, root, **kw):
        args = Args(root, **kw)
        blocking, review, collected = [], [], []
        for path in audit.worker_dirs(root):
            b, r, f = audit.audit_worker(path, args)
            blocking += b
            review += r
            collected.append(f)
        b, r = audit.audit_identity(root, collected, args)
        blocking += b
        review += r
        b, r, totals = audit.audit_queries(root, args)
        blocking += b
        review += r
        return blocking, review, totals

    def test_a_clean_run_has_no_blocking_findings(self):
        with tempfile.TemporaryDirectory() as tmp:
            blocking, _, _ = self.run_audit(build(tmp))
            self.assertEqual(blocking, [])

    def test_a_short_window_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-02': {'seconds': 25200}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('gate requires 43200' in b for b in blocking),
                            blocking)

    def test_a_relaxed_duration_admits_the_same_short_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-02': {'seconds': 25200}})
            blocking, _, _ = self.run_audit(root, seconds=25000)
            self.assertEqual(blocking, [])

    def test_relaxations_are_named_against_the_gate_and_the_launch(self):
        applied = {'seconds': 25200, 'blocks': 300, 'freshness_seconds': 30.0,
                   'replica_freshness_seconds': 75.0}
        launched = {'seconds': 43200, 'blocks': 300, 'freshness_seconds': 30.0,
                    'replica_freshness_seconds': 60.0}
        notes = ' | '.join(audit.relaxations(applied, launched))
        self.assertIn('seconds 25200 vs documented gate 43200 (weaker)', notes)
        self.assertIn('replica_freshness_seconds 75.0 vs documented gate 60.0 '
                      '(weaker)', notes)
        self.assertIn('launched requiring 43200', notes)

    def test_unmodified_thresholds_report_no_departure(self):
        applied = {'seconds': audit.DEFAULT_SECONDS, 'blocks': audit.DEFAULT_BLOCKS,
                   'freshness_seconds': audit.DEFAULT_FRESHNESS,
                   'replica_freshness_seconds': audit.DEFAULT_REPLICA_FRESHNESS}
        self.assertEqual(audit.relaxations(applied, applied), [])

    def test_a_missing_worker_result_blocks_as_cancelled(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-archive-01': {'result': False}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('never terminated' in b for b in blocking),
                            blocking)

    def test_a_failed_worker_result_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-03': {'passed': False}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('monitor failed' in b for b in blocking), blocking)

    def test_too_few_blocks_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-04': {'blocks': 120}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('gate requires 300 of each' in b for b in blocking),
                            blocking)

    def test_an_over_budget_replica_sample_is_not_called_a_catch_up_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-01': {
                'replica': 60.715,
                'queries': [exact_queries(2000), exact_queries(2000)]}})
            blocking, _, _ = self.run_audit(root)
            found = [b for b in blocking if 'replica sample' in b]
            self.assertTrue(found, blocking)
            self.assertIn('abandoning the read', found[0])
            self.assertIn('worker log', found[0])

    def test_public_freshness_breach_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-02': {'public': 31.0}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('public sample' in b for b in blocking), blocking)

    def test_a_restart_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-02': {'restarts': 1}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('NRestarts changed' in b for b in blocking),
                            blocking)

    def test_an_oom_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-archive-02': {'oom': 1}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('OOM recorded' in b for b in blocking), blocking)

    def test_low_memory_headroom_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-03': {'available': 0.12}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('available host memory fell' in b
                                for b in blocking), blocking)

    def test_a_mismatched_query_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-01': {
                'queries': [exact_queries(2000, mismatches=1),
                            exact_queries(2000)]}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('not exact' in b for b in blocking), blocking)

    def test_too_few_exact_queries_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-01': {
                'queries': [exact_queries(10), exact_queries(10)]}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('below the 1000 minimum' in b for b in blocking),
                            blocking)

    def test_disagreeing_binary_across_workers_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp)
            path = root / 'fleet-transparent-pir-recent-02/result.json'
            record = json.loads(path.read_text())
            record['binary_sha256'] = 'e' * 64
            path.write_text(json.dumps(record))
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('binary_sha256 disagrees' in b for b in blocking),
                            blocking)

    def test_a_worker_contradicting_its_own_attestation_blocks(self):
        # The result must not be able to claim a binary the worker never
        # attested through /v1/ready.
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp)
            path = root / 'fleet-transparent-pir-recent-03/result.json'
            record = json.loads(path.read_text())
            record['binary_sha256'] = 'e' * 64
            path.write_text(json.dumps(record))
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('but the result reports' in b for b in blocking),
                            blocking)

    def test_cross_stage_binary_mismatch_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp)
            canary = Path(tmp) / 'canary.json'
            canary.write_text(json.dumps(
                {'passed': True, 'binary_sha256': 'e' * 64,
                 'roster_sha256': ROSTER}))
            blocking, _, _ = self.run_audit(root, canary_result=canary)
            self.assertTrue(any('differs between the canary' in b
                                for b in blocking), blocking)

    def test_a_differing_fleet_config_across_stages_is_not_a_finding(self):
        # The canary and the full fleet legitimately run different
        # configurations; requiring agreement would manufacture a finding.
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp)
            canary = Path(tmp) / 'canary.json'
            canary.write_text(json.dumps(
                {'passed': True, 'binary_sha256': BINARY, 'roster_sha256': ROSTER,
                 'fleet_config_sha256': '1' * 64}))
            blocking, _, _ = self.run_audit(root, canary_result=canary)
            self.assertEqual(blocking, [])

    def test_a_failed_canary_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp)
            canary = Path(tmp) / 'canary.json'
            canary.write_text(json.dumps({'passed': False}))
            blocking, _, _ = self.run_audit(root, canary_result=canary)
            self.assertTrue(any('did not pass' in b for b in blocking), blocking)

    def test_an_unrecovered_transport_failure_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-archive-01': {
                'transport_failures': 3, 'transport_recoveries': 1}})
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('never recovered' in b for b in blocking),
                            blocking)

    def test_a_recovered_transport_failure_is_review_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-archive-01': {
                'transport_failures': 2, 'transport_recoveries': 2}})
            blocking, review, _ = self.run_audit(root)
            self.assertEqual(blocking, [])
            self.assertTrue(any('transport failure' in r for r in review), review)

    def test_a_reorg_is_review_not_blocking(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp, **{'transparent-pir-recent-04': {'reorg': True}})
            blocking, review, _ = self.run_audit(root)
            self.assertEqual(blocking, [])
            self.assertTrue(any('reorg reset' in r for r in review), review)

    def test_missing_routing_baseline_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = build(tmp)
            path = root / 'fleet-transparent-pir-recent-01/samples.ndjson'
            kept = [line for line in path.read_text().splitlines()
                    if 'routing_availability_baseline' not in line]
            path.write_text('\n'.join(kept) + '\n')
            blocking, _, _ = self.run_audit(root)
            self.assertTrue(any('no routing availability baseline' in b
                                for b in blocking), blocking)

    def test_the_real_failed_bundle_fails(self):
        # The strongest check available: a real observation with a known verdict.
        bundle = ROOT / ('docs/transparent-pir/evidence/'
                         'productionize-m1-http-retry-2026-09-12/'
                         'failed-observation-20260912T003121Z.tar.gz')
        if not bundle.exists():
            self.skipTest('failed observation bundle not present')
        import tarfile
        with tempfile.TemporaryDirectory() as tmp:
            with tarfile.open(bundle) as archive:
                archive.extractall(tmp, filter='data')
            blocking, _, totals = self.run_audit(Path(tmp) / 'observation')
            self.assertTrue(blocking)
            cancelled = [b for b in blocking if 'never terminated' in b]
            self.assertEqual(len(cancelled), 5, cancelled)
            self.assertTrue(any('monitor failed' in b for b in blocking), blocking)
            # Zero mismatches is a real property of that run, and must survive.
            for counts in totals.values():
                self.assertEqual(counts['mismatches'], 0)


if __name__ == '__main__':
    unittest.main()
