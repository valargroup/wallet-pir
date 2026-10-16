#!/usr/bin/env python3
"""Select cumulative service changes and build only their release artifacts."""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import urllib.error

from fast import ROOT, FULL_GROUPS, HELPERS, plan, workspace_packages
from stage import run

sys.path.insert(0, str(ROOT / 'ops/lib'))
from wallet_pir_ops import release  # noqa: E402

FAMILIES = {'ops': 'Operations', 'receiver': 'Receiver',
            'transparent': 'Transparent', 'enhance': 'Enhance'}
KINDS = {'ops': ['wallet-pir-ops'], 'receiver': ['receiver-pir'],
         'transparent': ['transparent-filter', 'transparent-shard', 'transparent-publisher',
                         'transparent-txid-display'],
         'enhance': ['enhance-pir', 'enhance-pir-native', 'status-pir']}


def history(repo):
    try:
        return release.api(f'repos/{repo}/actions/workflows/ci-qualify.yml/runs?per_page=100')['workflow_runs']
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        return []


def baseline(repo, sha, family, runs):
    """Only an ancestor with a successful family release can bound the diff."""
    for candidate in runs:
        if candidate.get('head_sha') == sha or not release.service_run(candidate, candidate.get('head_sha'), repo):
            continue
        jobs = release.api(f'repos/{repo}/actions/runs/{candidate["id"]}/attempts/{candidate["run_attempt"]}/jobs?per_page=100')['jobs']
        if not release.service_jobs(jobs, family):
            continue
        comparison = release.api(f'repos/{repo}/compare/{candidate["head_sha"]}...{sha}')
        if comparison['status'] == 'ahead':
            return candidate['head_sha']
    return None


def selection(repo, sha, requested='affected'):
    release.check_sha(sha)
    packages = workspace_packages()
    runs = history(repo) if requested == 'affected' else []
    result = {}
    for family in FAMILIES:
        base = baseline(repo, sha, family, runs) if requested == 'affected' else None
        comparison = release.api(f'repos/{repo}/compare/{base}...{sha}') if base else None
        # GitHub caps the compare endpoint at 300 files. Never infer an empty
        # or partial diff when coverage is uncertain; qualify the whole family.
        paths = ([name for file in comparison['files'] for name in
                  (file['filename'], file.get('previous_filename', file['filename']))]
                 if comparison and len(comparison['files']) < 300 else None)
        affected = plan(packages, paths) if paths is not None else None
        selected = (requested in ('all', family) if requested != 'affected' else
                    affected is None or family in affected['groups'])
        result[family] = {'selected': selected, 'base': base}
    return result


def checks(family, service_only=False):
    if family == 'common':
        groups = {'ops', 'enhance_infra', 'transparent_infra'}
    elif family == 'shared':
        groups = {'shared'}
    elif service_only:
        groups = set() if family == 'ops' else {family}
    else:
        groups = {'ops', 'enhance_infra', 'transparent_infra'} if family == 'ops' else {
            'ops', 'shared', family, 'transparent_infra' if family == 'transparent' else 'enhance_infra'}
    with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
        for group in sorted(FULL_GROUPS):
            out.write(f'{group}={str(group in groups).lower()}\n')
        out.write(f'helpers={json.dumps(sorted(HELPERS) if 'ops' in groups else [])}\n')


def verify(family, sha, output):
    with tempfile.TemporaryDirectory() as directory:
        for kind in KINDS[family]:
            release.extract(output / kind / (kind + '.tar.gz'), Path(directory) / kind, sha, kind)


def build(family):
    if family == 'ops':
        return
    command = ['cargo', 'build', '--locked', '--release']
    if family == 'receiver':
        command += ['-p', 'receiver-indexer', '--bin', 'receiver-directory', '--bin', 'receiver-probe']
    elif family == 'transparent':
        command += ['-p', 'transparent-filter-server', '-p', 'transparent-shard-server']
        for name in sorted({name for kind in KINDS[family] for name in release.BINARIES[kind]}):
            command += ['--bin', name]
    else:
        command += ['-p', 'enhance-pir-server', '-p', 'enhance-pir', '-p', 'enhance-pir-load-test', '-p', 'pir-apm']
        for name in release.BINARIES['enhance-pir']:
            command += ['--bin', name]
        command += ['--features', 'enhance-pir/cli']
    run(command, stage=f'compile:release-{family}')


def assemble(family, sha, target, output, native=False):
    kinds = ['enhance-pir-native', 'status-pir'] if native else KINDS[family]
    for kind in kinds:
        if not native and kind in ('enhance-pir-native', 'status-pir'):
            continue
        release.assemble(sha, target, output / kind, kind)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    select = sub.add_parser('select')
    select.add_argument('--sha', required=True)
    select.add_argument('--family', choices=['affected', 'all', *FAMILIES], default='affected')
    check = sub.add_parser('checks')
    check.add_argument('--family', choices=[*FAMILIES, 'common', 'shared'], required=True)
    check.add_argument('--service-only', action='store_true')
    for command in ('build', 'assemble', 'verify'):
        child = sub.add_parser(command)
        child.add_argument('--family', choices=FAMILIES, required=True)
        if command in ('assemble', 'verify'):
            child.add_argument('--sha', required=True)
            child.add_argument('--target', type=Path, default=Path(os.environ.get('CARGO_TARGET_DIR', 'target')))
            child.add_argument('--output', type=Path, default=Path('release-bundles'))
            child.add_argument('--native', action='store_true')
    args = parser.parse_args()
    if args.command == 'select':
        result = selection(os.environ['GITHUB_REPOSITORY'], args.sha, args.family)
        print(json.dumps(result, indent=2))
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            for family, value in result.items():
                out.write(f'{family}={str(value["selected"]).lower()}\n')
    elif args.command == 'checks':
        checks(args.family, args.service_only)
    elif args.command == 'verify':
        verify(args.family, args.sha, args.output)
    elif args.command == 'build':
        build(args.family)
    else:
        assemble(args.family, args.sha, args.target, args.output, args.native)


if __name__ == '__main__':
    main()
