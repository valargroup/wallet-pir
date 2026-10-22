#!/usr/bin/env python3
"""Exercise independently synchronizing local processes under encrypted load.

This produces local evidence only. Full-size and hardware qualification use
separate gates. No installed services or cloud resources are touched.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile


def provenance(bin_dir):
    candidate = bin_dir / 'candidate.json'
    if candidate.exists():
        metadata = json.loads(candidate.read_text())
        if (metadata.get('kind') not in ('enhance-pir', 'enhance-pir-native')
                or metadata.get('qualification') != 'unqualified'
                or metadata.get('protocol_revision') != {
                    'enhance-pir': 'ironwood-enhance-pir-v7',
                    'enhance-pir-native': 'ironwood-enhance-pir-v9-native-two-mask-m29'
                }.get(metadata.get('kind'))
                or metadata.get('schema_version') != 11
                or not isinstance(metadata.get('source_dirty'), bool)
                or not re.fullmatch('[0-9a-f]{40}', metadata.get('source_revision', ''))):
            raise ValueError('invalid candidate provenance')
        return {'revision': metadata['source_revision'], 'dirty': metadata['source_dirty'],
                'revision_source': 'candidate-metadata'}
    try:
        result = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True)
    except FileNotFoundError:
        return {'revision': None, 'dirty': None, 'revision_source': 'unavailable'}
    if result.returncode:
        return {'revision': None, 'dirty': None, 'revision_source': 'unavailable'}
    status = subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True)
    return {'revision': result.stdout.strip(), 'dirty': bool(status.stdout) if status.returncode == 0 else None,
            'revision_source': 'checkout'}



def main():
    from local_fleet import Fleet, Publications, fetch
    from fixture_node import PROFILES
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin-dir', type=Path, default=Path('target/release-fast'))
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=30)
    parser.add_argument('--concurrency', default='1,2,4,8')
    parser.add_argument('--open-loop', action='store_true')
    parser.add_argument('--require-publications', type=int, default=0)
    parser.add_argument('--profile', choices=PROFILES, default='smoke')
    parser.add_argument('--block-interval', type=float, default=10)
    parser.add_argument('--ready-timeout', type=int, default=120)
    args = parser.parse_args()
    concurrencies = [int(value) for value in args.concurrency.split(',')]
    if args.seconds < 1 or args.require_publications < 0 or not concurrencies or min(concurrencies) < 1:
        parser.error('duration and concurrency must be positive')
    if args.open_loop and 2 not in concurrencies:
        parser.error('--open-loop needs a concurrency=2 baseline')
    if not math.isfinite(args.block_interval) or args.block_interval <= 0 or not 1 <= args.ready_timeout <= 1800:
        parser.error('block interval must be finite and positive; readiness timeout must be 1..1800 seconds')
    args.out.mkdir(parents=True, exist_ok=False)
    binaries = args.bin_dir.resolve()
    load = binaries / 'enhance-pir-load-test'
    manifest = {'kind': 'local-smoke-not-hardware-qualification', 'platform': platform.platform(),
                **provenance(binaries), 'binaries': {
                    name: hashlib.sha256((binaries / name).read_bytes()).hexdigest()
                    for name in ['pir-query-server', 'pir-publisher', 'enhance-pir-load-test']},
                'seconds_per_step': args.seconds, 'profile': args.profile,
                'fixture': {**PROFILES[args.profile], 'block_interval': args.block_interval}, 'results': []}
    with tempfile.TemporaryDirectory(prefix='enhance-pull-') as directory:
        fleet = Fleet(binaries, directory, args.out, profile=args.profile,
                      block_interval=args.block_interval,
                      sealed_shards=7 if args.profile == 'seven-sealed' else 6)
        publications = None
        try:
            fleet.start_roles()
            fleet.start_publisher()
            manifest['initial_manifest'] = fleet.wait_ready(args.ready_timeout)
            origin, operator = fleet.origin, 'http://' + fleet.operator
            manifest['load_start_manifest'] = fetch(origin + '/v1/enhance/init')
            publications = Publications(origin, manifest['load_start_manifest'])
            for concurrency in map(int, args.concurrency.split(',')):
                report = args.out / f'load-c{concurrency}.json'
                command = [str(load),  '--oracle', str(fleet.oracle), '--server', origin,
                    '--parallelism', str(concurrency), '--warmup', '2s', '--duration', f'{args.seconds}s',
                    '--seed', '20260922', '--max-error-rate', '0', '--json-out', str(report)]
                (args.out / f'load-c{concurrency}.command.json').write_text(json.dumps(command, indent=2))
                with (args.out / f'load-c{concurrency}.log').open('w') as output:
                    result = subprocess.run(command, stdout=output, stderr=output, timeout=args.seconds + 180)
                manifest['results'].append({'concurrency': concurrency, 'exit_code': result.returncode,
                                            'report': report.name, 'health': fetch(operator + '/v1/health')})
                print(f'concurrency={concurrency} exit={result.returncode}', flush=True)
                if result.returncode:
                    raise RuntimeError('load driver failed; inspect retained evidence')
            if args.open_loop:
                baseline = json.loads((args.out / 'load-c2.json').read_text())['correct_queries_per_second']
                for fraction in [0.5, 0.75, 1.0, 1.25]:
                    report = args.out / f'open-loop-{fraction}.json'
                    command = [str(load),  '--oracle', str(fleet.oracle), '--server', origin,
                        '--parallelism', '8', '--rate', str(baseline * fraction), '--warmup', '2s',
                        '--duration', f'{args.seconds}s', '--seed', '20260922', '--max-error-rate', '0',
                        '--json-out', str(report)]
                    (args.out / f'open-loop-{fraction}.command.json').write_text(json.dumps(command, indent=2))
                    with (args.out / f'open-loop-{fraction}.log').open('w') as output:
                        result = subprocess.run(command, stdout=output, stderr=output, timeout=args.seconds + 180)
                    manifest['results'].append({'offered_fraction': fraction, 'exit_code': result.returncode,
                                                'report': report.name, 'health': fetch(operator + '/v1/health')})
                    print(f'offered_fraction={fraction} exit={result.returncode}', flush=True)
                    if result.returncode:
                        raise RuntimeError('open-loop driver failed; inspect retained evidence')
            manifest['final_manifest'] = fetch(origin + '/v1/enhance/init')
            publications.close()
            observed = publications.report()
            (args.out / 'publications.json').write_text(json.dumps(observed, indent=2) + '\n')
            published = observed['observed_publications']
            manifest['publication_gate'] = {'required': args.require_publications,
                                            'observed': published,
                                            'passed': published >= args.require_publications}
            if published < args.require_publications:
                raise RuntimeError('insufficient completed publications during load; inspect retained evidence')

        finally:
            if publications is not None:
                publications.close()
                (args.out / 'publications.json').write_text(json.dumps(publications.report(), indent=2) + '\n')
            fleet.close()
            manifest['resources'] = fleet.resource_report()
            source = Path(directory) / 'publisher'
            for name in ['source-mode']:
                if (source / name).exists():
                    shutil.copyfile(source / name, args.out / name)
            (args.out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()
