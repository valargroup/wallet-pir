#!/usr/bin/env python3
"""Record local node availability and published PIR coverage without exposing RPC credentials."""
import argparse
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import time


def read_json(port, path, body=None, cookie=None):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    headers = {}
    if body is not None:
        headers['Content-Type'] = 'application/json'
        headers['Authorization'] = 'Basic ' + base64.b64encode(cookie).decode('ascii')
    try:
        connection.request('POST' if body is not None else 'GET', path, body=body, headers=headers)
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError('local endpoint did not return 200')
        payload = response.read(1024 * 1024 + 1)
        if len(payload) > 1024 * 1024:
            raise ValueError('oversized local response')
        return json.loads(payload)
    finally:
        connection.close()


def sample(cookie_path):
    result = {'wall_time_ns': time.time_ns(), 'node_tip': None, 'published_anchor': None,
              'generation': None, 'ingestion_failed': None, 'publication_blocked': None, 'error': None}
    try:
        request = json.dumps({'jsonrpc': '1.0', 'id': 'qualification',
                              'method': 'getblockcount', 'params': []}).encode()
        rpc = read_json(8232, '/', request, cookie_path.read_bytes().strip())
        health = read_json(8080, '/v1/health')
        tip, anchor, generation = rpc['result'], health['anchor_height'], health['generation']
        if rpc['error'] is not None or any(type(v) is not int or v < 0 for v in (tip, anchor, generation)):
            raise ValueError('invalid local chain or coordinator response')
        result.update(node_tip=tip, published_anchor=anchor, generation=generation,
                      ingestion_failed=health['ingestion_error'] is not None,
                      publication_blocked=health['blocked_reason'] is not None)
    except (OSError, ValueError, KeyError, TypeError, http.client.HTTPException) as error:
        result['error'] = type(error).__name__
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seconds', type=int, required=True)
    parser.add_argument('--interval', type=int, default=10)
    parser.add_argument('--cookie', type=Path, default=Path('/root/.cache/zakura/.cookie'))
    args = parser.parse_args()
    if not 1 <= args.seconds <= 172800 or not 1 <= args.interval <= 60:
        parser.error('use 1..172800 seconds and a 1..60 second interval')
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    script_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    started = time.monotonic()
    state = {'kind': 'enhance-freshness-observation', 'qualification': 'unqualified',
             'status': 'running', 'started_wall_ns': time.time_ns(), 'seconds_requested': args.seconds,
             'interval_seconds': args.interval, 'script_sha256': script_hash, 'samples': 0, 'errors': 0}
    manifest = args.output / 'manifest.json'
    manifest.write_text(json.dumps(state, indent=2) + '\n')
    digest = hashlib.sha256()
    trace = args.output / 'samples.jsonl'
    with trace.open('x') as handle:
        trace.chmod(0o600)
        while time.monotonic() - started < args.seconds:
            began = time.monotonic()
            entry = sample(args.cookie)
            line = json.dumps(entry, sort_keys=True, allow_nan=False) + '\n'
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
            digest.update(line.encode())
            state['samples'] += 1
            state['errors'] += entry['error'] is not None
            remaining = min(args.interval - (time.monotonic() - began),
                            args.seconds - (time.monotonic() - started))
            if remaining > 0:
                time.sleep(remaining)
    state.update(status='recorded', finished_wall_ns=time.time_ns(),
                 elapsed_seconds=time.monotonic() - started, samples_sha256=digest.hexdigest())
    temporary = manifest.with_suffix('.tmp')
    temporary.write_text(json.dumps(state, indent=2) + '\n')
    temporary.replace(manifest)


if __name__ == '__main__':
    main()
