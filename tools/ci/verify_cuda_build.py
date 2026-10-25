#!/usr/bin/env python3
"""Bind a CUDA release to Cargo's compiler output and the query-server executable.

Reads the query-server build's JSON messages, checks that binary and its
evaluator were compiled with CUDA and the native protocol, then runs the
packaged executable's evaluator check. No GPU is needed. The publisher's
build-info command is not consulted: it does not report CUDA. The revision
recorded here is the caller's build label; this binary does not print one.
Only after these checks may the caller write the CUDA ABI metadata.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


# Feature names Cargo must report for the packaged binary and its evaluator.
REQUIRED = {
    'pir-query-server': frozenset({'enhance', 'cuda', 'native-reinspiring'}),
    'enhance_shared': frozenset({'cuda', 'native-reinspiring'}),
    'pir_cuda': frozenset({'cuda'}),
}


def verify(messages, target, revision):
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise ValueError('CUDA build revision must be a full lowercase commit SHA')
    artifacts = []
    for line in messages.read_text().splitlines():
        if not line.startswith('{'):
            continue
        value = json.loads(line)
        if value.get('reason') == 'compiler-artifact':
            artifacts.append(value)
    for name, needed in REQUIRED.items():
        matching = [a for a in artifacts if a['target']['name'] == name]
        observed = [{'features': a.get('features'), 'executable': a.get('executable'),
                     'fresh': a.get('fresh')} for a in matching]
        print('CUDA_COMPILER_ARTIFACT ' + json.dumps({'target': name, 'artifacts': observed}), flush=True)
        features = set(matching[0].get('features') or []) if len(matching) == 1 else set()
        if len(matching) != 1 or not needed <= features:
            raise ValueError(f'{name}: Cargo did not produce exactly one CUDA artifact: {observed}')
    server = next(a for a in artifacts if a['target']['name'] == 'pir-query-server')
    binary = target / 'release/pir-query-server'
    if server.get('executable') is None or Path(server['executable']).resolve() != binary.resolve():
        raise ValueError('packaging path differs from the executable Cargo produced')
    # CPU is the default. Drop a caller-supplied CUDA selection so this probe
    # does not load a driver; compiler features already proved the CUDA build.
    env = os.environ.copy()
    env.pop('ENHANCE_MATVEC_BACKEND', None)
    env.pop('ENHANCE_CUDA_DEVICE', None)
    try:
        subprocess.check_output([str(binary.resolve()), 'check-backend'], timeout=30,
                                env=env, stderr=subprocess.STDOUT)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        detail = (getattr(error, 'output', b'') or b'').decode(errors='replace').strip()
        raise ValueError(f'query server evaluator check failed: {detail}') from error
    return {
        'cuda': True, 'native_reinspiring': True, 'source_revision': revision,
        'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--messages', type=Path, required=True)
    parser.add_argument('--target', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    args = parser.parse_args()
    print('CUDA_EXECUTABLE_VERIFIED ' + json.dumps(verify(args.messages, args.target, args.revision)))


if __name__ == '__main__':
    main()
