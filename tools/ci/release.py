#!/usr/bin/env python3
"""Assemble, locate and verify exact-SHA release artifacts from full CI."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import urllib.request
import tarfile

ROOT = Path(__file__).resolve().parents[2]
BINARIES = {
    'enhance-pir': ['enhance-pir-server', 'enhance-pir-cli', 'enhance-pir-load-test'],
    'transparent-filter': ['transparent-filter-server'],
    'transparent-shard': ['transparent-shard-server', 'shard-assign', 'shard-prune'],
    'transparent-publisher': ['transparent-publish-controller', 'transparent-shard-server', 'shard-control', 'shard-assign'],
}
FILES = {
    'enhance-pir': ['enhance/ops/scripts/test-local.py', 'enhance/ops/scripts/bootstrap-worker.py',
        'enhance/ops/scripts/sample-worker.py',
        'enhance/ops/scripts/summarize-samples.py', 'enhance/ops/scripts/assess-campaign.py',
        'enhance/ops/deploy/workers.example.json'],
    'transparent-filter': ['transparent/ops/deploy/transparent-filter-server.service'],
    'transparent-shard': ['transparent/ops/deploy/transparent-shard-server.service', 'transparent/ops/deploy/transparent-Caddyfile'],
    'transparent-publisher': [],
}


def check_sha(sha):
    if not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('release revision must be a full lowercase commit SHA')


def api(path):
    token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    if token:
        request = urllib.request.Request(
            os.environ.get('GITHUB_API_URL', 'https://api.github.com') + '/' + path,
            headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json',
                     'X-GitHub-Api-Version': '2022-11-28'})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    return json.loads(subprocess.check_output(['gh', 'api', path]))


def qualified(run, sha):
    return (run['head_sha'] == sha and run['head_branch'] == 'main'
            and run['event'] in ('push', 'workflow_dispatch')
            and run['status'] == 'completed' and run['conclusion'] == 'success'
            and run['path'] == '.github/workflows/ci-full.yml')


def resolve(sha, kind):
    check_sha(sha)
    repo = os.environ['GITHUB_REPOSITORY']
    # Eligibility is independently checked by each deploy workflow. This is an
    # additional provenance gate: a successful fast/PR run cannot supply binaries.
    runs = api(f'repos/{repo}/actions/workflows/ci-full.yml/runs?head_sha={sha}&status=success&per_page=100')['workflow_runs']
    for run in runs:
        if not qualified(run, sha):
            continue
        artifacts = api(f'repos/{repo}/actions/runs/{run["id"]}/artifacts?per_page=100')['artifacts']
        if any(a['name'] == f'{kind}-{sha}' and not a['expired'] for a in artifacts):
            with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
                out.write(f'run-id={run["id"]}\n')
            return
    raise ValueError(f'no qualified, unexpired {kind} artifact for {sha}; run CI full on main first')


def assemble(sha, target, output, kind=None):
    check_sha(sha)
    dirty = bool(subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=normal'], cwd=ROOT))
    output.mkdir(parents=True, exist_ok=False)
    selected = {kind: BINARIES[kind]} if kind else BINARIES
    for kind, binaries in selected.items():
        directory = output / kind
        directory.mkdir()
        for name in binaries:
            shutil.copy2(target / 'release' / name, directory / name)
            (directory / name).chmod(0o755)
        for source in FILES[kind]:
            shutil.copy2(ROOT / source, directory / Path(source).name)
        if kind == 'enhance-pir':
            (directory / 'candidate.json').write_text(json.dumps({
                'kind': kind, 'schema_version': 11, 'protocol_revision': 'ironwood-enhance-pir-v6',
                'qualification': 'unqualified', 'source_revision': sha, 'source_dirty': dirty,
            }, indent=2) + '\n')
        (directory / 'revision').write_text(sha + '\n')
        (directory / 'SHA256SUMS').write_text(''.join(
            f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n'
            for p in sorted(directory.iterdir())))
        with tarfile.open(output / f'{kind}.tar.gz', 'w:gz') as archive:
            for path in sorted(directory.iterdir()):
                archive.add(path, arcname=path.name)


def extract(archive_path, destination, sha, kind):
    check_sha(sha)
    # Flat regular files only; no archive-controlled paths, links or duplicate
    # members. Preserve executability explicitly (Actions zip downloads do not).
    with tarfile.open(archive_path, 'r:gz') as archive:
        members = archive.getmembers()
        names = [m.name for m in members]
        if len(names) != len(set(names)) or any(not m.isfile() or Path(m.name).name != m.name or m.name in ('.', '..') for m in members):
            raise ValueError('release archive must contain unique flat regular files')
        payload = {m.name: archive.extractfile(m).read() for m in members}
    required = set(BINARIES[kind]) | {Path(p).name for p in FILES[kind]} | {'revision', 'SHA256SUMS'}
    if kind == 'enhance-pir':
        required.add('candidate.json')
    if set(payload) != required:
        raise ValueError('release archive contents differ from the required artifact inventory')
    if payload['revision'].decode().strip() != sha:
        raise ValueError('release revision mismatch')
    checksums = {}
    for line in payload['SHA256SUMS'].decode().splitlines():
        match = re.fullmatch(r'([0-9a-f]{64})  ([A-Za-z0-9_.-]+)', line)
        if not match or match[2] in checksums:
            raise ValueError('invalid or duplicate checksum entry')
        checksums[match[2]] = match[1]
    if set(checksums) != set(payload) - {'SHA256SUMS'}:
        raise ValueError('checksum inventory mismatch')
    for name, digest in checksums.items():
        if hashlib.sha256(payload[name]).hexdigest() != digest:
            raise ValueError(f'checksum mismatch: {name}')
    if kind == 'enhance-pir':
        candidate = json.loads(payload['candidate.json'])
        if (candidate.get('kind') != kind or candidate.get('source_revision') != sha
                or candidate.get('schema_version') != 11
                or candidate.get('protocol_revision') != 'ironwood-enhance-pir-v6'
                or candidate.get('qualification') != 'unqualified'
                or not isinstance(candidate.get('source_dirty'), bool)):
            raise ValueError('invalid candidate metadata; qualification is a separate gate')
    destination.mkdir(parents=True, exist_ok=False)
    for name, data in payload.items():
        path = destination / name
        path.write_bytes(data)
        # Ops scripts are launched through their interpreters or installed by
        # deployment; binaries must be directly executable on download.
        path.chmod(0o755 if name in BINARIES[kind] or name.endswith(('.py', '.sh')) else 0o644)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['assemble', 'resolve', 'extract'])
    parser.add_argument('--sha', required=True)
    parser.add_argument('--kind', choices=BINARIES)
    parser.add_argument('--target', type=Path, default=Path(os.environ.get('CARGO_TARGET_DIR', 'target')))
    parser.add_argument('--output', type=Path, default=Path('artifact'))
    parser.add_argument('--archive', type=Path)
    args = parser.parse_args()
    if args.command == 'assemble':
        head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        if head != args.sha:
            raise ValueError('cannot label a build with a different checkout revision')
        assemble(args.sha, args.target, args.output, args.kind)
    elif args.command == 'resolve':
        resolve(args.sha, args.kind)
    else:
        extract(args.archive, args.output, args.sha, args.kind)


if __name__ == '__main__':
    main()
