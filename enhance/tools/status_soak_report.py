#!/usr/bin/env python3
"""Report completed diagnostic soak measurements without granting release approval."""
import argparse
import json
import math
from pathlib import Path


def report(directory):
    summaries = [json.loads(line) for line in (directory / 'summary.jsonl').read_text().splitlines() if line.strip()]
    if len(summaries) != 1:
        raise ValueError('one completed summary is required')
    summary = summaries[0]
    protocol_ok = all(summary.get(k) == v for k, v in {
        'protocol': 'status-pir-v2-q48', 'rows': 8192, 'slots': 256,
        'slot_bytes': 40, 'row_bytes': 12288, 'columns': 6144,
        'database_bytes': 100663296,
    }.items())
    counts = {'correct': 0, 'error': 0, 'unstarted': 0}
    latencies = []
    identities = set()
    for line in (directory / 'requests.jsonl').read_text().splitlines():
        row = json.loads(line)
        if row['arrival'] in identities:
            raise ValueError('duplicate arrival')
        identities.add(row['arrival'])
        counts[row['result']] += 1
        if row['result'] == 'correct':
            latencies.append(row['completed_ms'] - row['scheduled_ms'])
    latencies.sort()
    p99 = latencies[math.ceil(len(latencies) * .99) - 1] if latencies else None
    covered = identities == set(range(summary['offered']))
    result = {
        'protocol_and_geometry_passed': protocol_ok,
        'six_hour_schedule_passed': summary['seconds'] >= 21600 and summary['offered'] >= 432000
            and summary['run_ended_ms'] - summary['run_started_ms'] >= 21600000,
        'arrival_coverage_passed': covered,
        'availability_passed': covered and counts['correct'] == summary['offered']
            and counts['error'] == 0 and counts['unstarted'] == 0,
        'latency_basis': 'scheduled_to_completed',
        'successful_request_p99_ms': p99,
        'successful_request_p99_passed': p99 is not None and p99 <= 1000,
        'counts': counts,
        'production_qualified': False,
        'remaining_evidence': [
            'complete independent publication and supersession oracle',
            'joint resource evidence from both hosts',
            'shared coordinator cutover and restricted HTTPS rehearsal',
            'live APM integration and independent protocol review',
        ],
        'run_started_ms': summary['run_started_ms'],
        'run_ended_ms': summary['run_ended_ms'],
    }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    result = report(args.directory)
    (args.directory / 'measurement-report.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
