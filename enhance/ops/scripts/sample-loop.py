#!/usr/bin/env python3
"""Persist timed worker samples, including failures; never issue qualification."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import signal
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seconds', type=int, required=True)
    parser.add_argument('--interval', type=float, default=1)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 172800 or not 1 <= args.interval <= 10:
        parser.error('use 1..172800 seconds and a 1..10 second interval')
    script = Path(__file__).with_name('sample-worker.py')
    spec = importlib.util.spec_from_file_location('worker_sample', script)
    sampler = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sampler)
    args.output.mkdir(parents=True, exist_ok=False)
    began = time.monotonic()
    state = {'status': 'running', 'qualification': 'unqualified',
             'sampler_sha256': hashlib.sha256(script.read_bytes()).hexdigest(),
             'started_wall_ns': time.time_ns(), 'seconds_requested': args.seconds,
             'interval_seconds': args.interval, 'samples': 0, 'errors': 0}
    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    for name in (signal.SIGINT, signal.SIGTERM):
        signal.signal(name, stop)
    manifest = args.output / 'manifest.json'
    manifest.write_text(json.dumps(state, indent=2) + '\n')
    digest = hashlib.sha256()
    with (args.output / 'samples.jsonl').open('x') as handle:
        while not stopping and time.monotonic() - began < args.seconds:
            started = time.monotonic()
            try:
                sample = sampler.collect()
            except Exception:
                sample = {'kind': 'enhance-hardware-sample', 'version': sampler.SAMPLE_VERSION,
                          'wall_time_ns': time.time_ns(), 'error': 'collection_failed'}
            entry = {'elapsed_seconds': started - began, 'sample': sample}
            line = json.dumps(entry, sort_keys=True, allow_nan=False) + '\n'
            digest.update(line.encode())
            handle.write(line)
            handle.flush()
            state['samples'] += 1
            state['errors'] += sample['error'] is not None
            remaining = min(args.interval - (time.monotonic() - started),
                            args.seconds - (time.monotonic() - began))
            if remaining > 0:
                time.sleep(remaining)
    state.update(status='interrupted' if stopping else 'recorded',
                 elapsed_seconds=time.monotonic() - began, finished_wall_ns=time.time_ns(),
                 samples_sha256=digest.hexdigest())
    temporary = manifest.with_suffix('.tmp')
    temporary.write_text(json.dumps(state, indent=2) + '\n')
    temporary.replace(manifest)


if __name__ == '__main__':
    main()
