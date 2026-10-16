#!/usr/bin/env python3
"""Select cumulative service changes and build only their release artifacts."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

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


def checks(family):
    groups = {'ops', 'enhance_infra', 'transparent_infra'} if family == 'ops' else {
        'ops', 'shared', family, 'transparent_infra' if family == 'transparent' else 'enhance_infra'}
    with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
        for group in sorted(FULL_GROUPS):
            out.write(f'{group}={str(group in groups).lower()}\n')
        out.write(f'helpers={json.dumps(sorted(HELPERS))}\n')


def background(repo, sha):
    if release.api(f'repos/{repo}/commits/main')['sha'] == sha:
        run([sys.executable, 'tools/ci/fast.py', '--all', '--select-only', '--github-output'], stage='selection')
        return
    print('Superseded automatic regression: no native work selected')
    with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
        for group in sorted(FULL_GROUPS):
            out.write(f'{group}=false\n')
        out.write('helpers=[]\n')


def cancel_regressions(repo, sha):
    """Cancel only obsolete automatic regressions, never pinned runs or deploys."""
    runs = release.api(f'repos/{repo}/actions/workflows/ci-full.yml/runs?per_page=100')['workflow_runs']
    for candidate in runs:
        if (candidate.get('event') not in ('push', 'workflow_run') or candidate.get('head_branch') != 'main'
                or candidate.get('status') not in ('queued', 'in_progress', 'waiting')
                or candidate.get('head_sha') == sha):
            continue
        request = urllib.request.Request(
            os.environ.get('GITHUB_API_URL', 'https://api.github.com')
            + f'/repos/{repo}/actions/runs/{candidate["id"]}/cancel', method='POST',
            headers={'Authorization': 'Bearer ' + (os.environ.get('GH_TOKEN') or os.environ['GITHUB_TOKEN']),
                     'Accept': 'application/vnd.github+json'})
        try:
            with urllib.request.urlopen(request, timeout=30):
                pass
        except urllib.error.HTTPError as error:
            # Completion between list and cancel is harmless. All other errors
            # are visible and fail the selector rather than silently building a backlog.
            if error.code != 409:
                raise


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
    for command in ('cancel-regressions', 'background'):
        child = sub.add_parser(command)
        child.add_argument('--sha', required=True)
    check = sub.add_parser('checks')
    check.add_argument('--family', choices=FAMILIES, required=True)
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
    elif args.command == 'cancel-regressions':
        cancel_regressions(os.environ['GITHUB_REPOSITORY'], args.sha)
    elif args.command == 'background':
        background(os.environ['GITHUB_REPOSITORY'], args.sha)
    elif args.command == 'checks':
        checks(args.family)
    elif args.command == 'verify':
        verify(args.family, args.sha, args.output)
    elif args.command == 'build':
        build(args.family)
    else:
        assemble(args.family, args.sha, args.target, args.output, args.native)


if __name__ == '__main__':
    main()
