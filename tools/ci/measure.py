#!/usr/bin/env python3
"""Measure explicit check populations without storing commands or raw output.

Use a stable committed checkout. Warm and cold are labels supplied by the
operator; this tool never deletes caches to manufacture a cold sample.
"""
import argparse
import json
import math
import platform
from pathlib import Path
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--category', choices=['docs', 'ops', 'leaf', 'shared', 'dependency', 'artifact'], required=True)
    parser.add_argument('--population', choices=['warm', 'cold'], required=True)
    parser.add_argument('--runs', type=int, default=20)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command or args.runs < 1:
        parser.error('positive --runs and a command after -- are required')
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    dirty = subprocess.check_output(['git', 'status', '--porcelain'], text=True)
    if dirty:
        parser.error('measure a stable committed checkout')
    rows = []
    with args.output.open('x') as out:
        for _ in range(args.runs):
            start = time.monotonic()
            result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            elapsed = time.monotonic() - start
            current = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
            unchanged = not subprocess.check_output(['git', 'status', '--porcelain'], text=True)
            rows.append({'seconds': round(elapsed, 3), 'exit': result.returncode,
                         'snapshot_valid': current == sha and unchanged})
            data = {'sha': sha, 'category': args.category, 'population': args.population, 'platform': platform.platform(),
                    'architecture': platform.machine(), 'runs': rows}
            successes = sorted(r['seconds'] for r in rows if r['exit'] == 0 and r['snapshot_valid'])
            data['successful_runs'] = len(successes)
            data['p95_seconds'] = successes[math.ceil(len(successes)*.95)-1] if len(successes) >= 20 else None
            out.seek(0); out.truncate(); json.dump(data, out, indent=2); out.flush()
            if not rows[-1]['snapshot_valid']:
                raise SystemExit('snapshot changed; measurement invalid')
    print(json.dumps(data, indent=2))
    raise SystemExit(0 if all(r['exit'] == 0 for r in rows) else 1)


if __name__ == '__main__':
    main()
