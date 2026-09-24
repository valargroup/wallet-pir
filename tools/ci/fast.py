#!/usr/bin/env python3
"""Select affected workspace packages and execute the fast feedback gate.

Unknown/build-config changes broaden selection. Full CI owns full integration tests,
examples, doctests and all-feature lint; this gate never qualifies a release.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]


def select(packages, paths):
    names = {p['name'] for p in packages}
    roots = {p['name']: Path(p['manifest_path']).parent.relative_to(ROOT).as_posix() + '/' for p in packages}
    selected = set()
    for path in paths:
        if path.endswith('.md') or path.startswith(('docs/', 'evidence/', 'enhance/evidence/', 'transparent/evidence/')):
            continue
        if path in ('Cargo.toml', 'Cargo.lock', 'Makefile') or path.startswith(('.cargo/', '.github/', 'tools/ci/')) or path.endswith('/Cargo.toml'):
            return names
        owner = next((name for name, root in roots.items() if path.startswith(root)), None)
        if owner:
            selected.add(owner)
        elif path.startswith(('ops/', 'tools/', 'enhance/ops/', 'transparent/ops/', 'transparent/tools/filters/', 'transparent/tools/parent-filters/')):
            continue
        else:
            return names
    while True:
        expanded = selected | {p['name'] for p in packages if any(d['name'] in selected for d in p['dependencies'])}
        if expanded == selected:
            return selected
        selected = expanded


def run(command, *, env=None):
    start = time.monotonic()
    print('+ ' + ' '.join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, env=env)
    elapsed = time.monotonic() - start
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as out:
            out.write(f'| `{command[0]} {command[1]}` | {elapsed:.2f}s | {result.returncode} |\n')
    result.check_returncode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', default='')
    parser.add_argument('--head', default='HEAD')
    parser.add_argument('--select-only', action='store_true')
    parser.add_argument('--github-output', action='store_true')
    args = parser.parse_args()
    metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--no-deps', '--format-version', '1'], cwd=ROOT))
    packages = metadata['packages']
    paths = []
    if args.base and set(args.base) != {'0'}:
        paths = subprocess.check_output(['git', 'diff', '--name-only', '--no-renames', '-z', args.base, args.head], cwd=ROOT).decode().split('\0')
        selected = select(packages, [p for p in paths if p])
    else:
        selected = {p['name'] for p in packages}
    print('Affected packages: ' + ', '.join(sorted(selected)), flush=True)
    if args.github_output:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write(f'rust={str(bool(selected)).lower()}\n')
    if args.select_only:
        return
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as out:
            out.write('## Fast checks\n\nSelected packages: ' + ', '.join(sorted(selected)) + '\n\n| Stage | Duration | Exit |\n|---|---:|---:|\n')
    docs_only = bool(paths) and all(not p or p.endswith('.md') or p.startswith(('docs/', 'evidence/', 'enhance/evidence/', 'transparent/evidence/')) for p in paths)
    if docs_only:
        run(['make', 'check-docs'])
        return

    def rust_checks():
        run(['cargo', 'fmt', '--all', '--check'])
        libraries = sorted(p['name'] for p in packages if p['name'] in selected and any('lib' in t['kind'] for t in p['targets']))
        binary_only = sorted(selected - set(libraries))
        # --lib avoids compiling integration executables just to filter them out.
        if libraries:
            command = ['cargo', 'test', '--locked', '--profile', 'release-fast', '--lib', *sum((['-p', p] for p in libraries), [])]
            run([*command, '--no-run'])
            slow = json.loads((ROOT / 'tools/ci/slow-tests.json').read_text())
            print('Full-CI-only library cases: ' + ', '.join(slow), flush=True)
            # Independent service budgets do not cap concurrent test fixtures.
            # Keep real-crypto fixtures serial within the fast runner's 6-GiB cap.
            run([*command, '--', *sum((['--skip', name] for name in slow), [])],
                env=dict(os.environ, RUST_TEST_THREADS='1'))
        if 'enhance-pir-server' in selected:
            run(['python3', 'tools/ci/enhance_tests.py', '--tier', 'fast'])
        if binary_only:
            run(['cargo', 'check', '--locked', '--profile', 'release-fast', '--bins', *sum((['-p', p] for p in binary_only), [])])

    # Independent helper checks overlap Rust compilation/execution. Every
    # future is observed; an early failure never hides another task's result.
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(run, ['make', 'check-docs', 'check-tools']),
                   pool.submit(run, ['make', '-j4', 'check-ops']),
                   pool.submit(run, ['make', 'check-reports'])]
        if selected:
            futures.append(pool.submit(rust_checks))
        failures = []
        for future in futures:
            try:
                future.result()
            except subprocess.CalledProcessError as error:
                failures.append(error)
        if failures:
            raise failures[0]


if __name__ == '__main__':
    main()
