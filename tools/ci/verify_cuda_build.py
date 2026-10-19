#!/usr/bin/env python3
"""Bind a CUDA release to Cargo's compiler output and executable capabilities.

Reads the build's JSON messages, verifies the server and evaluator features,
then runs the exact executable that packaging will consume. No GPU is needed.
Only after these checks may the caller write the CUDA ABI metadata.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def verify(messages, target, revision):
    artifacts = []
    for line in messages.read_text().splitlines():
        if not line.startswith('{'):
            continue
        value = json.loads(line)
        if value.get('reason') == 'compiler-artifact':
            artifacts.append(value)
    for name in ('enhance-pir-server', 'enhance_query_server'):
        matching = [a for a in artifacts if a['target']['name'] == name]
        observed = [{'features': a.get('features'), 'executable': a.get('executable'),
                     'fresh': a.get('fresh')} for a in matching]
        print('CUDA_COMPILER_ARTIFACT ' + json.dumps({'target': name, 'artifacts': observed}), flush=True)
        if len(matching) != 1 or 'cuda' not in matching[0]['features']:
            raise ValueError(f'{name}: Cargo did not produce exactly one CUDA artifact: {observed}')
    server = next(a for a in artifacts if a['target']['name'] == 'enhance-pir-server')
    binary = target / 'release/enhance-pir-server'
    if Path(server['executable']).resolve() != binary.resolve():
        raise ValueError('packaging path differs from the executable Cargo produced')
    info = json.loads(subprocess.check_output([str(binary.resolve()), 'build-info'], timeout=30))
    expected = {'cuda': True, 'native_reinspiring': True, 'source_revision': revision}
    if info != expected:
        raise ValueError(f'compiled server capabilities differ: expected {expected}, observed {info}')
    return {**info, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--messages', type=Path, required=True)
    parser.add_argument('--target', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    args = parser.parse_args()
    print('CUDA_EXECUTABLE_VERIFIED ' + json.dumps(verify(args.messages, args.target, args.revision)))


if __name__ == '__main__':
    main()
