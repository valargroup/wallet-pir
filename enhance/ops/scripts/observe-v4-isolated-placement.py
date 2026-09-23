#!/usr/bin/env python3
"""Record fixture placement transitions without copying private coordinator state."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.request


def get(origin, path):
    with urllib.request.urlopen(origin + path, timeout=10) as response:
        data = response.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ValueError('oversized coordinator response')
    return json.loads(data)


def observe(state_path, origin):
    state = json.loads(state_path.read_text())
    health = get(origin, '/v1/health')
    init = get(origin, '/v1/enhance/init')
    if init['generation'] != health['generation']:
        raise ValueError('health and init generations differ')
    capacity = health['capacity']
    forecast = {key: capacity.get(key) for key in
                ('requested', 'remaining_rows', 'effective_rows_per_second',
                 'readiness_seconds', 'burst_rows')}
    forecast['requests'] = [
        {key: request.get(key) for key in
         ('id', 'target_groups', 'requested_at', 'boundary_records', 'registered')}
        for request in sorted(capacity.get('requests', {}).values(), key=lambda r: r['id'])
    ]
    return {'at_ns': time.time_ns(), 'generation': init['generation'],
            'records': init['coverage']['records'],
            'shards': [{'id': shard['id'], 'state': shard['state'],
                        'global_row_start': shard['global_row_start'], 'records': shard['records']}
                       for shard in init['coverage']['shards']],
            'assignments': state['assignments'],
            'capacity': forecast,
            'registered_groups': health['registered_groups'],
            'published_replica_counts': health['published_replica_counts'],
            'blocked_reason': health.get('blocked_reason')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--origin', default='http://127.0.0.1:8280')
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=1800)
    parser.add_argument('--interval', type=float, default=5)
    args = parser.parse_args()
    if args.origin != 'http://127.0.0.1:8280' or args.seconds < 1 or not 1 <= args.interval <= 30:
        parser.error('observe only the isolated loopback coordinator with a positive duration')
    os.umask(0o077)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    digest = hashlib.sha256()
    samples = 0
    errors = 0
    with args.out.open('x') as output:
        while time.monotonic() - started < args.seconds:
            began = time.monotonic()
            try:
                value = observe(args.state, args.origin)
            except Exception as error:
                value = {'at_ns': time.time_ns(), 'error': type(error).__name__}
                errors += 1
            line = json.dumps(value, sort_keys=True, allow_nan=False) + '\n'
            output.write(line)
            output.flush()
            digest.update(line.encode())
            samples += 1
            time.sleep(max(0, args.interval - (time.monotonic() - began)))
    print(json.dumps({'samples': samples, 'errors': errors,
                      'sha256': digest.hexdigest(), 'qualification': 'unqualified'}))


if __name__ == '__main__':
    main()
