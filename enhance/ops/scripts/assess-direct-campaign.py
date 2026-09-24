#!/usr/bin/env python3
"""Audit an isolated exercise with two direct-deployment worker traces.

Exit zero means the automated checks passed, never that hardware is qualified.
The traces are collected on the workers; host provenance, clock alignment and
unsampled peaks still require independent operator review.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path


def load(name):
    path = Path(__file__).with_name(name)
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


campaign = load('assess-campaign.py')
samples = load('summarize-samples.py')


def assess(workload, workers):
    report, failures, began, finished, profile, policy, maximum, workload_host = campaign.audit_workload(workload)

    def check(condition, code):
        if not condition and code not in failures:
            failures.append(code)

    check(len(workers) == 2, 'requires_two_physical_workers')
    hosts, names, summaries = set(), set(), []
    for directory, policy_path in workers:
        manifest = campaign.read(directory / 'manifest.json')
        bound = campaign.read(policy_path)
        trace = directory / 'samples.jsonl'
        digest = hashlib.sha256(policy_path.read_bytes()).hexdigest()
        data_dir = bound.get('data_dir')
        check(bound.get('kind') == 'enhance-direct-worker-sampling-v1'
              and isinstance(data_dir, str)
              and data_dir.startswith('/srv/enhance-pir-v6/qualification/')
              and bound.get('campaign_profile') == profile
              and Path(data_dir).name.startswith(f'{profile}-worker')
              and bound.get('sealed_shards') == policy['sealed_shards'],
              'isolated_direct_policy_missing')
        check(manifest.get('status') == 'recorded' and manifest.get('direct_policy_sha256') == digest,
              'worker_observation_not_recorded_or_bound')
        check(manifest.get('interval_seconds') == 1 and manifest.get('seconds_requested', 0) >= 21600,
              'worker_observation_cadence_or_duration')
        check(manifest.get('started_wall_ns', began + 1) <= began
              and manifest.get('finished_wall_ns', finished - 1) >= finished,
              'worker_observation_does_not_cover_exercise')
        check(manifest.get('errors') == 0, 'worker_observation_errors')
        summary = samples.summarize(trace, max_gap_seconds=3, window=(began, finished))
        summaries.append(summary)
        check(summary['trace_sha256'] == manifest.get('samples_sha256')
              and summary['records'] == manifest.get('samples'),
              'worker_trace_digest_or_count_differs')
        check(not summary['findings'] and summary['error_samples'] == 0,
              'worker_trace_findings')
        check(len(summary['identities']) == 1, 'worker_identity_changed')
        if len(summary['identities']) == 1:
            identity = summary['identities'][0]
            host = identity['host_id_sha256']
            check(host not in hosts and host != workload_host, 'physical_workers_not_distinct_from_workload')
            hosts.add(host)
            check(identity['source_revision'] == bound.get('revision')
                  and identity['binary_sha256'] == bound.get('binary_sha256') == report['binary_sha256'],
                  'worker_or_workload_binary_differs')
        name = bound.get('worker_name')
        check(isinstance(name, str) and name not in names, 'worker_names_not_distinct')
        names.add(name)
        consumed = hashlib.sha256()
        with trace.open('rb') as handle:
            while line := handle.readline(1024 * 1024 + 1):
                if len(line) > 1024 * 1024 or not line.endswith(b'\n'):
                    raise ValueError('oversized or incomplete worker trace')
                consumed.update(line)
                sample = json.loads(line, object_pairs_hook=campaign.pairs,
                                    parse_constant=campaign.observer.reject_constant)['sample']
                if sample.get('error') is not None:
                    continue
                check(sample.get('identity_source') == 'direct-release-policy'
                      and sample.get('identity_policy_sha256') == digest
                      and sample.get('worker_data_dir') == bound.get('data_dir')
                      and sample.get('hostname') == name
                      and sample.get('bootstrap_manifest_sha256') == bound.get('manifest_sha256')
                      and sample.get('worker_private_ipv4') == bound.get('private_ipv4')
                      and sample.get('worker_private_port') == bound.get('private_port'),
                      'worker_sample_policy_binding_differs')
                service = sample['service']
                check(service.get('ActiveState') == 'active' and service.get('NRestarts') == '0'
                      and service.get('MainPID') == str(sample['main_pid']),
                      'worker_service_restarted_or_changed')
                limits = bound['limits']
                cgroup = sample['cgroup_bytes']
                check(cgroup['memory.high'] == limits['memory_high_bytes']
                      and cgroup['memory.max'] == limits['memory_max_bytes']
                      and cgroup['memory.swap.max'] == limits['memory_swap_max_bytes'],
                      'worker_limits_differ')
                check(sample['worker']['protocol'] == 'ironwood-enhance-pir-v6',
                      'worker_protocol_differs')
        check(consumed.hexdigest() == summary['trace_sha256'], 'worker_trace_changed_during_assessment')
    return {'kind': 'enhance-direct-campaign-assessment', 'qualification': 'unqualified',
            'status': 'evidence_checks_failed' if failures else 'evidence_checks_passed',
            'failures': failures, 'profile': profile, 'worker_summaries': summaries,
            'query_p99_ms': report['background_p99_ms'], 'max_publication_ms': maximum,
            'unproven_gates': ['Worker-local trace and host identity provenance',
                               'cross-host clock alignment', 'resident peaks between samples',
                               'overhead-model calibration, pressure and reclaim acceptance',
                               'latency and open-loop capacity acceptance',
                               'protocol and wallet conformance']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workload', type=Path, required=True)
    parser.add_argument('--worker', nargs=2, action='append', required=True,
                        metavar=('OBSERVATION_DIR', 'DIRECT_POLICY'))
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        parser.error('assessment output must be new')
    try:
        result = assess(args.workload, [(Path(directory), Path(policy)) for directory, policy in args.worker])
    except (ValueError, KeyError, TypeError, IndexError, OSError, ArithmeticError):
        result = {'kind': 'enhance-direct-campaign-assessment', 'qualification': 'unqualified',
                  'status': 'evidence_checks_failed', 'failures': ['invalid_or_incomplete_evidence']}
    args.out.write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    print(json.dumps({key: result[key] for key in ('status', 'qualification')}))
    raise SystemExit(0 if result['status'] == 'evidence_checks_passed' else 1)


if __name__ == '__main__':
    main()
