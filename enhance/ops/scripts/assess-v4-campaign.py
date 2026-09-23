#!/usr/bin/env python3
"""Audit campaign evidence against frozen bootstrap inputs; never issue qualification.

Exit zero means the automated evidence checks passed. Memory-model calibration,
peak resident guard, reclaim/SLO acceptance and workload-host identity still need
independent review. This command never provisions, registers or promotes workers.
"""
import argparse
import hashlib
import importlib.util
import json
import math
from pathlib import Path

spec = importlib.util.spec_from_file_location('v4_observer', Path(__file__).with_name('observe-v4-pair.py'))
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)
MIN_SECONDS = 21600
MIN_PUBLICATIONS = 300


def pairs(items):
    value = {}
    for key, item in items:
        if key in value:
            raise ValueError('duplicate JSON key')
        value[key] = item
    return value


def decode(data):
    return json.loads(data, object_pairs_hook=pairs, parse_constant=observer.reject_constant)


def read(path):
    data = path.read_bytes()
    if len(data) > 1024 * 1024:
        raise ValueError('oversized evidence document')
    return decode(data)


def trace(path, expected):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for line in handle:
            if len(line) > 1024 * 1024:
                raise ValueError('oversized evidence record')
            digest.update(line)
            yield decode(line)
    # Hash the very bytes consumed by the audit, not a separate pre-read that
    # could race a writer. No audit result is returned until this check finishes.
    if digest.hexdigest() != expected:
        raise ValueError('trace digest mismatch')


def integer(value):
    if type(value) is not int or value < 0:
        raise ValueError('expected nonnegative integer evidence')
    return value


def finite(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError('expected finite nonnegative evidence')
    return value


def assess(workload, observations):
    """Each observation supplies (manifest, trace path, validated targets, config)."""
    failures = []
    statistics = {}

    def check(condition, code):
        if not condition and code not in failures:
            failures.append(code)

    report = read(workload / 'exercise.json')
    check(report['status'] == 'recorded', 'workload_not_recorded')
    profile = report['profile']
    check(profile in ('active', 'sealed'), 'requires_full_size_profile')
    check(finite(report['measurement_seconds']) >= MIN_SECONDS, 'workload_shorter_than_six_hours')
    check(integer(report['publications']) >= MIN_PUBLICATIONS, 'fewer_than_300_publications')
    check(integer(report['background_correct_answers']) > 0 and integer(report['background_errors']) == 0, 'query_correctness_or_errors')
    check(integer(report['expired_refreshes']) > 0 and integer(report['exact_boundary_and_retained_probes']) > 0, 'missing_retention_or_boundary_probes')
    finite(report['background_p99_ms'])
    began, finished = integer(report['started_wall_ns']), integer(report['finished_wall_ns'])
    check(finished > began, 'invalid_workload_window')
    generations, stages, last_elapsed, max_publication = [], set(), -1., 0
    for publication in trace(workload / 'publications.jsonl', report['publications_sha256']):
        generation = integer(publication['manifest']['generation'])
        elapsed = finite(publication['elapsed_seconds'])
        check(generation > 0, 'invalid_publication_generation')
        check(not generations or generation == generations[-1] + 1, 'nonconsecutive_publications')
        check(elapsed > last_elapsed, 'nonmonotonic_publication_trace')
        last_elapsed = elapsed
        generations.append(generation)
        stages.add(publication['stage'])
        max_publication = max(max_publication, integer(publication['publication_ms']))
        placement = publication['placement']
        check(len(placement['groups']) == (1 if profile == 'active' else 2), 'assignment_group_count_differs')
        check(all(integer(g['shards']) <= (6 if g['role'] in ('SEALED_OPEN', 'SEALED_FULL') else 5) for g in placement['groups']), 'group_role_limit_exceeded')
        first = placement['groups'][0]
        expected = ('ACTIVE', 5) if profile == 'active' else ('SEALED_FULL', 6)
        check((first['role'], first['shards']) == expected, 'assignment_profile_differs')
        check(bool(placement['published_replica_counts']) and all(type(n) is int and n == 2 for n in placement['published_replica_counts'].values()), 'missing_complete_replication')
        check(placement['retained_generations'] == list(range(generation, max(0, generation - 5), -1)), 'retention_window_differs')
    check(len(generations) == report['publications'], 'publication_count_differs')
    check(last_elapsed <= report['measurement_seconds'], 'publication_outside_measurement')
    check(max_publication == report['max_fixture_publication_ms'], 'publication_timing_summary_differs')
    required = {'loan_growth', 'return', 'owned_8k', 'owned_16k', 'owned_near_32k', 'rewind_return'} if profile == 'active' else {'sealed_with_external_append'}
    check(required <= stages, 'missing_profile_transitions')
    check(len(observations) == (1 if profile == 'active' else 2), 'missing_worker_pair_observations')
    workload_host = report.get('workload_host_id_sha256')
    check(isinstance(workload_host, str) and len(workload_host) == 64 and all(c in '0123456789abcdef' for c in workload_host), 'missing_workload_host_identity')
    resource_ids, host_ids = set(), set()
    for manifest, path, targets, config in observations:
        check(manifest['status'] == 'recorded' and manifest.get('sample_version') == 2, 'observation_not_recorded_v2')
        check(manifest['targets'] == targets, 'observation_target_binding_differs')
        check(manifest['bootstrap_config_digest'] == observer.pair_module.journal_module.digest(config), 'observation_policy_binding_differs')
        check(integer(manifest['seconds_requested']) >= MIN_SECONDS and integer(manifest['elapsed_ns']) >= MIN_SECONDS * 10**9, 'observation_shorter_than_six_hours')
        interval = finite(manifest['interval_seconds'])
        if not 1 <= interval <= 10:
            raise ValueError('invalid observation interval')
        slot_count = math.ceil(manifest['seconds_requested'] / interval) + 1
        counts = {t['resource_id']: 0 for t in targets}
        errors, unavailable, inconsistent = 0, 0, 0
        per_worker = {}
        controller = manifest['controller_host_id_sha256']
        check(isinstance(controller, str) and len(controller) == 64 and all(c in '0123456789abcdef' for c in controller), 'missing_off_host_observer_identity')
        for target in targets:
            check(target['resource_id'] not in resource_ids, 'duplicate_worker_resource')
            resource_ids.add(target['resource_id'])
            check(target['expected']['binary_sha256'] == report['binary_sha256'], 'workload_binary_differs')
        by_id = {t['resource_id']: t for t in targets}
        for entry in trace(path, manifest['trace_sha256']):
            index = counts[entry['resource_id']]
            offset = min(round(index * interval * 1e9), manifest['seconds_requested'] * 10**9)
            check(index < slot_count and integer(entry['scheduled_offset_ns']) == offset, 'observation_slots_missing_extra_or_unordered')
            counts[entry['resource_id']] += 1
            sample = entry['sample']
            check(entry['worker'] == by_id[entry['resource_id']]['name'], 'trace_worker_name_differs')
            if sample['error'] is not None:
                errors += 1
                continue
            observer.bind_sample(sample, by_id[entry['resource_id']])
            check(sample['error'] is None, 'trace_bootstrap_binding_differs')
            check(sample['version'] == 2, 'trace_schema_differs')
            values = sample['runtime_metrics']['values']
            check(integer(values['memory_sample_available']) in (0, 1) and type(sample['runtime_metrics']['state_consistent']) is bool, 'invalid_runtime_flags')
            unavailable += values['memory_sample_available'] == 0
            inconsistent += not sample['runtime_metrics']['state_consistent']
            state = per_worker.setdefault(entry['resource_id'], {'identity': None, 'first_wall': None, 'last_wall': None,
                'events': None, 'max_cgroup_peak_bytes': 0, 'max_sampled_rss_plus_kernel_bytes': 0, 'available_samples': 0})
            host_id = sample['host_id_sha256']
            check(isinstance(host_id, str) and len(host_id) == 64 and all(c in '0123456789abcdef' for c in host_id), 'invalid_worker_host_identity')
            check(host_id != workload_host, 'workload_runs_on_worker_host')
            identity = (host_id, sample['boot_id'], integer(sample['main_pid']), integer(sample['main_start_ticks']))
            if state['identity'] is None:
                check(identity[0] not in host_ids and identity[0] != controller, 'workers_or_observer_share_host')
                host_ids.add(identity[0])
                state['identity'] = identity
            check(state['identity'] == identity, 'worker_restarted_during_capacity_campaign')
            wall = integer(sample['wall_time_ns'])
            check(state['last_wall'] is None or wall > state['last_wall'], 'nonmonotonic_worker_clock')
            state['first_wall'] = wall if state['first_wall'] is None else state['first_wall']
            state['last_wall'] = wall
            events = sample['memory.events']
            if state['events'] is not None:
                check(all(integer(events[k]) >= state['events'][k] for k in state['events']), 'memory_counters_reset')
                check(all(events[k] == state['events'][k] for k in ('oom', 'oom_kill')), 'worker_oom_event')
            state['events'] = {k: integer(events[k]) for k in ('high', 'max', 'oom', 'oom_kill')}
            peak = integer(sample['cgroup_bytes']['memory.peak'])
            check(peak <= sample['cgroup_bytes']['memory.max'], 'cgroup_peak_exceeds_hard_limit')
            check(bool(sample['process_memory']) and any(p['pid'] == sample['main_pid'] for p in sample['process_memory']), 'missing_process_memory')
            resident = sum(integer(p['bytes']['Rss']) for p in sample['process_memory']) + integer(sample['memory.stat']['kernel'])
            check(resident <= 7 * 1024**3 - 512 * 1024**2, 'observed_resident_guard_violation')
            state['max_cgroup_peak_bytes'] = max(state['max_cgroup_peak_bytes'], peak)
            state['max_sampled_rss_plus_kernel_bytes'] = max(state['max_sampled_rss_plus_kernel_bytes'], resident)
            state['available_samples'] += values['memory_sample_available'] == 1
        check(all(n == slot_count for n in counts.values()), 'observation_slots_missing_or_extra')
        check(sum(counts.values()) == manifest['records'], 'observation_count_differs')
        check(errors == manifest['error_samples'] == 0, 'observation_errors')
        check(unavailable == manifest['unavailable_memory_samples'] and inconsistent == manifest['inconsistent_runtime_samples'], 'observation_runtime_counts_differ')
        for resource, state in per_worker.items():
            check(state['first_wall'] <= began and state['last_wall'] >= finished, 'observation_does_not_cover_initialization_and_workload')
            check(state['available_samples'] > 0, 'no_runtime_memory_samples')
            statistics[resource] = state
        check(set(per_worker) == set(by_id), 'worker_has_no_valid_samples')
    return {'kind': 'enhance-v4-campaign-assessment', 'qualification': 'unqualified',
            'status': 'evidence_checks_failed' if failures else 'evidence_checks_passed',
            'failures': failures, 'profile': profile, 'worker_statistics': statistics,
            'query_p99_ms': report['background_p99_ms'], 'max_publication_ms': max_publication,
            'unproven_gates': ['peak resident guard between observations', 'overhead-model calibration',
                'hard-cap, pressure and reclaim acceptance', 'cross-host clock alignment and host identity provenance',
                'latency and open-loop capacity acceptance', 'protocol and wallet conformance']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workload', type=Path, required=True)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--pair', nargs=3, action='append', required=True, metavar=('JOURNAL_DIR', 'BOOTSTRAP_POLICY', 'OBSERVATION_DIR'))
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        raise SystemExit('assessment output must be new')
    try:
        observations = []
        for journal, policy, observation in args.pair:
            operation = read(Path(journal) / 'journal.json')['operation']
            config = read(Path(policy))
            targets = observer.observed_targets(operation, config, args.bundle)
            directory = Path(observation)
            observations.append((read(directory / 'manifest.json'), directory / 'hardware.jsonl', targets, config))
        result = assess(args.workload, observations)
    except (ValueError, KeyError, TypeError, IndexError, OSError, ArithmeticError):
        result = {'kind': 'enhance-v4-campaign-assessment', 'qualification': 'unqualified',
                  'status': 'evidence_checks_failed', 'failures': ['invalid_or_incomplete_evidence']}
    observer.pair_module.journal_module.atomic(args.out, result)
    print(json.dumps({k: result[k] for k in ('status', 'qualification')}))
    raise SystemExit(0 if result['status'] == 'evidence_checks_passed' else 1)


if __name__ == '__main__':
    main()
