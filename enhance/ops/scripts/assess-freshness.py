#!/usr/bin/env python3
"""Conservatively assess five-minute publication coverage from local observations."""
import argparse
import hashlib
import json
from pathlib import Path


def unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate trace field')
        result[key] = value
    return result


def reject_constant(_value):
    raise ValueError('nonfinite trace value')


def assess(samples, start_ns, end_ns, interval_seconds=10, limit_seconds=300):
    if not 0 < start_ns < end_ns or not 1 <= interval_seconds <= 60 or limit_seconds <= 0:
        raise ValueError('invalid assessment window or cadence')
    gap_limit = 2 * interval_seconds * 10**9
    limit = limit_seconds * 10**9
    findings = set()
    pending = []
    last_wall = last_tip = None
    before_start = after_end = False
    initial_uncovered = False
    observed_tips = passed = ambiguous = 0
    worst_upper_ns = 0
    for sample in samples:
        wall = sample.get('wall_time_ns')
        if type(wall) is not int or wall < 0 or last_wall is not None and wall <= last_wall:
            raise ValueError('nonmonotonic or invalid observation clock')
        if last_wall is not None and wall - last_wall > gap_limit and start_ns <= wall <= end_ns + limit:
            findings.add('sampling_gap')
        before_start |= wall <= start_ns
        after_end |= wall >= end_ns + limit
        if 'error' not in sample or sample['error'] is not None and not isinstance(sample['error'], str):
            raise ValueError('invalid observation error field')
        if sample['error'] is not None:
            findings.add('collection_error')
            last_wall = wall
            continue
        tip, anchor, generation = (sample.get(k) for k in ('node_tip', 'published_anchor', 'generation'))
        if any(type(v) is not int or v < 0 for v in (tip, anchor, generation)):
            raise ValueError('invalid observed height or generation')
        if anchor > tip or any(type(sample.get(k)) is not bool for k in
                               ('ingestion_failed', 'publication_blocked')):
            raise ValueError('invalid publication state')
        if sample.get('ingestion_failed') or sample.get('publication_blocked'):
            findings.add('reported_ingestion_or_publication_problem')
        if last_tip is not None and tip < last_tip:
            findings.add('reorg_requires_hash_review')
            pending.clear()
        if wall < start_ns:
            initial_uncovered = anchor < tip
        if last_tip is not None and tip > last_tip and start_ns <= wall <= end_ns:
            # The new tip became available after the previous sample and no
            # later than this one. Use the earlier time for a safe lag bound.
            pending.append((tip, last_wall, wall))
            observed_tips += 1
        remaining = []
        for height, earliest, seen in pending:
            if anchor >= height:
                upper = wall - earliest
                worst_upper_ns = max(worst_upper_ns, upper)
                if upper <= limit:
                    passed += 1
                else:
                    ambiguous += 1
            else:
                if wall - seen > limit:
                    findings.add('definite_freshness_failure')
                remaining.append((height, earliest, seen))
        pending = remaining
        last_wall, last_tip = wall, tip
    if not before_start or not after_end:
        findings.add('window_or_resolution_tail_missing')
    if initial_uncovered:
        findings.add('initial_uncovered_tip')
    if pending:
        findings.add('unresolved_tip')
    if not observed_tips:
        findings.add('no_new_node_tip_observed')
    if ambiguous:
        findings.add('lag_bound_inconclusive')
    status = ('freshness_observation_passed' if not findings else
              'freshness_failed' if 'definite_freshness_failure' in findings else 'evidence_incomplete')
    return {'kind': 'enhance-freshness-assessment', 'qualification': 'unqualified',
            'status': status, 'window_wall_ns': [start_ns, end_ns],
            'sampled_tip_advances': observed_tips, 'conservatively_bounded': passed,
            'ambiguous_tip_advances': ambiguous, 'max_conservative_lag_seconds': worst_upper_ns / 1e9,
            'findings': sorted(findings),
            'limits': {'freshness_seconds': limit_seconds, 'maximum_sample_gap_seconds': 2 * interval_seconds},
            'limitations': ['Tip availability is bounded by the prior sample; exact node arrival time is unknown.',
                            'Height-only observations require separate review across reorgs.',
                            'This assesses freshness only, not wallet correctness or hardware qualification.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('trace', type=Path)
    parser.add_argument('--window', nargs=2, type=int, required=True, metavar=('START_NS', 'END_NS'))
    parser.add_argument('--interval-seconds', type=int, default=10)
    args = parser.parse_args()
    digest = hashlib.sha256()
    samples = []
    with args.trace.open('rb') as handle:
        while line := handle.readline(1024 * 1024 + 1):
            if len(line) > 1024 * 1024 or not line.endswith(b'\n'):
                raise ValueError('oversized or incomplete observation line')
            digest.update(line)
            samples.append(json.loads(line, object_pairs_hook=unique, parse_constant=reject_constant))
    report = assess(samples, *args.window, args.interval_seconds)
    report['trace_sha256'] = digest.hexdigest()
    report['samples'] = len(samples)
    print(json.dumps(report, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
