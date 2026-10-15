#!/usr/bin/env python3
"""The benchmark's fleet adapter: one local recent replica.

The publication controller runs this as its `fleet_command`, with the
adapter config path as its argument and one request as JSON on stdin, as it
runs `transparent-live-fleet.py`. It stages exactly as that adapter's
`stage` does, on local disk instead of over SSH: plan the assignment once
per map, list the worker's files with `shard-assign files` (with
`--runtimes` when the controller offers prepared runtimes), hard-link every
revision directory and runtime file the worker's active publication already
holds, copy the rest with `rsync -a --ignore-existing --stats`, write the
assignment once, then send the worker `prepare` over its control socket.
Activation, invalidation and withdrawal are forwarded the same way.

Every stage is logged with its duration, the bytes rsync sent and the
worker's reply, one JSON object per line, to the configured log.
"""
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time


def log(config, **event):
    event['unix'] = time.time()
    with open(config['log'], 'a') as stream:
        stream.write(json.dumps(event) + '\n')


def control(config, command, timeout):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(config['worker_socket'])
        sock.sendall(json.dumps(command).encode() + b'\n')
        reply = b''
        while not reply.endswith(b'\n'):
            chunk = sock.recv(1 << 16)
            if not chunk:
                break
            reply += chunk
    value = json.loads(reply)
    if not value.get('ok'):
        raise RuntimeError(f"worker refused {command['operation']}: {value.get('error')}")
    return value['result']


def run(args):
    return subprocess.run([str(a) for a in args], check=True, capture_output=True).stdout


def transfer_bytes(stats):
    match = re.search(rb'^Total bytes sent: ([0-9,.]+)', stats, re.MULTILINE)
    return int(re.sub(rb'[,.]', b'', match.group(1))) if match else None


def assignment_for(config, digest, source, recent_from):
    path = Path(config['state_dir']) / (digest + '.assignment.json')
    if not path.exists():
        partial = path.with_name(path.name + '.partial')
        run([config['assign_binary'], 'plan', '--shard-dir', source, '--roster', config['roster'],
             '--recent-from-height', recent_from, '--headroom', '0.05',
             '--out-assignment', partial, '--source-sha', 'bench'])
        os.replace(partial, path)
    return path


def prepare(config, req):
    digest = req['map_sha256']
    if not re.fullmatch('[0-9a-f]{64}', digest):
        raise ValueError('invalid map digest')
    source = Path(req['directory']).resolve(strict=True)
    runtimes = bool(req.get('runtimes'))
    if runtimes and Path(req['runtimes']).resolve(strict=True) != source:
        raise ValueError('prepared runtimes must be files of the publication directory')
    timings = {}
    started = time.monotonic()
    assignment = assignment_for(config, digest, source, req['recent_from'])
    timings['plan'] = time.monotonic() - started
    started = time.monotonic()
    status = control(config, {'operation': 'status'}, 5)
    before = status['active']
    control(config, {'operation': 'collect'}, 60)
    timings['collect'] = time.monotonic() - started
    started = time.monotonic()
    files = run([config['assign_binary'], 'files', '--shard-dir', source, '--assignment', assignment,
                 '--worker-id', config['worker_id']] + (['--runtimes'] if runtimes else [])).decode().splitlines()
    timings['inventory'] = time.monotonic() - started
    started = time.monotonic()
    remote = Path(config['worker_root']) / digest
    remote.mkdir(parents=True, exist_ok=True)
    old = Path(before['directory'])
    linked = 0
    for name in sorted({f.split('/')[0] for f in files if '/' in f}):
        if not re.fullmatch('[0-9a-f]{64}', name):
            raise ValueError('unexpected artifact path')
        if not (remote / name).exists() and (old / name).is_dir():
            subprocess.run(['cp', '-al', str(old / name), str(remote / name)], check=True)
            linked += 1
    for name in files:
        if '/' in name or not name.endswith('.runtime'):
            continue
        if not re.fullmatch('[0-9a-f]{64}-[0-9a-f]{64}[.]runtime', name):
            raise ValueError('unexpected runtime path')
        if not (remote / name).exists() and (old / name).is_file():
            os.link(old / name, remote / name)
            linked += 1
    timings['hardlink'] = time.monotonic() - started
    started = time.monotonic()
    listing = Path(config['state_dir']) / (digest + '.files')
    listing.write_text('\n'.join(files) + '\n')
    stats = run(['rsync', '-a', '--ignore-existing', '--stats', '--files-from', listing,
                 str(source) + '/', str(remote) + '/'])
    transferred = transfer_bytes(stats)
    timings['transfer'] = time.monotonic() - started
    target = remote / 'assignment.json'
    if not target.exists():
        shutil.copyfile(assignment, target.with_suffix('.next'))
        os.replace(target.with_suffix('.next'), target)
    publication = {'directory': str(remote), 'assignment': str(target), 'map_sha256': digest}
    if runtimes:
        publication['runtimes'] = str(remote)
    started = time.monotonic()
    reply = control(config, {'operation': 'prepare', 'expected': before['map_sha256'],
                             'publication': publication}, 120)
    timings['prepare'] = time.monotonic() - started
    worker = {'expected': before['map_sha256'], 'publication': publication,
              'transferred_bytes': transferred, 'linked': linked,
              'runtimes': reply.get('runtimes'), 'loading_seconds': reply.get('loading_seconds'),
              'warming_seconds': reply.get('warming_seconds'), 'stage_seconds': timings}
    log(config, operation='prepare', map_sha256=digest, **worker)
    return {'ok': True, 'workers': {config['worker_id']: worker}, 'assignment': str(assignment)}


def activate(config, req):
    worker = req['prepared']['workers'][config['worker_id']]
    started = time.monotonic()
    control(config, {'operation': 'activate', 'expected': worker['expected'],
                     'map_sha256': req['map_sha256']}, 25)
    log(config, operation='activate', map_sha256=req['map_sha256'], seconds=time.monotonic() - started)
    return {'ok': True, 'upstreams': [config['worker_upstream']], 'recent_replicas': 1}


def invalidate(config, req):
    status = control(config, {'operation': 'status'}, 5)
    control(config, {'operation': 'invalidate', 'expected': status['active']['map_sha256'],
                     'from_height': req['from_height']}, 25)
    log(config, operation='invalidate', from_height=req['from_height'])
    return {'ok': True, 'retained_publication': False}


def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    req = json.load(sys.stdin)
    handlers = {'prepare': prepare, 'activate': activate, 'invalidate': invalidate,
                'withdraw': lambda config, req: {'ok': True}}
    try:
        reply = handlers[req['operation']](config, req)
    except Exception as error:  # reported to the controller, which retries
        log(config, operation=req.get('operation'), error=str(error))
        raise
    print(json.dumps(reply))


if __name__ == '__main__':
    main()
