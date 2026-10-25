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

from .paths import ROOT
BINARIES = {
    'pir-publisher': ['pir-publisher'],
    'wallet-pir-ops': [],
    # pir-apm is the Enhance/Status/Transparent dashboard and alerting sidecar;
    # it has no native variant.
    'enhance-pir': ['pir-publisher', 'pir-query-server', 'enhance-pir-cli', 'enhance-pir-load-test', 'pir-apm'],
    # Same binaries built with `native-reinspiring`; they serve an incompatible
    # protocol and come from a separate target directory.
    'enhance-pir-native': ['pir-publisher', 'pir-query-server', 'enhance-pir-cli', 'enhance-pir-load-test'],
    # GPU hosts run this query server, not the publisher. The clients are diagnostics.
    'enhance-pir-native-cuda': ['pir-query-server', 'enhance-pir-cli', 'enhance-pir-load-test'],
    # Transparent history and txid display processes run pir-query-server
    # (features `transparent-history,transparent-detail`) with `--config`; the
    # admin tools come from transparent-tools.
    'transparent-shard': ['pir-query-server', 'shard-assign', 'shard-prune'],
    'transparent-publisher': ['pir-publisher', 'pir-query-server', 'shard-assign'],
    # Tiered txid display proof of concept beside history; deployed only by
    # `wallet-pir-deploy.py txid-display-*` (tools/txid/lib/txid_display_poc.py).
    'transparent-txid-display': ['pir-query-server', 'txid-inventory', 'pir-publisher',
                                 'txid-quality-canary'],
    # Status publisher and the query server its roles run, from the native target
    # like `enhance-pir-native`; tools/scripts/wallet-pir-deploy.py renders its units.
    'status-pir': ['pir-publisher', 'pir-query-server'],
    # The receiver directory's indexer and server, which
    # tools/scripts/wallet-pir-deploy.py deploys and whose `probe` is the deploy's
    # exact check, and the standalone probe pir-monitor runs.
    'receiver-pir': ['receiver-directory', 'receiver-probe'],
}
FILES = {
    'pir-publisher': ['tools/deploy/publisher/pir-publisher.service',
                      'tools/deploy/publisher/publisher.example.json',
                      'tools/deploy/publisher/services.toml'],
    'wallet-pir-ops': [],
    'enhance-pir': ['tools/enhance/scripts/test-local.py',
        'tools/enhance/scripts/local_fleet.py', 'tools/enhance/scripts/fixture_node.py',
        'src/pir-publisher/tests/support/distribution_tls.py',
        'tools/pir-e2e-tests/tests/fixtures/ironwood-fee-expiry.hex',
        'tools/pir-e2e-tests/tests/fixtures/ironwood-canonical-record-0.hex',
        'tools/pir-e2e-tests/tests/fixtures/ironwood-canonical-record-1.hex',
        'tools/enhance/deploy/workers.example.json'],
    'transparent-shard': [],
    'transparent-publisher': [],
    'transparent-txid-display': ['tools/txid/deploy/transparent-txid-display-worker.service.in',
                                 'tools/txid/deploy/transparent-txid-display-controller.service.in',
                                 'tools/txid/deploy/txid-display-routes.caddy.in',
                                 'tools/txid/scripts/txid-display-fleet.py',
                                 'tools/txid/scripts/txid-display-observe.py'],
    'status-pir': ['tools/status/deploy/status-worker.service.in', 'tools/status/deploy/status-router.service.in',
                   'tools/status/deploy/status-worker.example.json', 'tools/status/deploy/status-router.example.json'],
    'receiver-pir': ['tools/receiver/digitalocean/receiver-pir.service.in', 'tools/receiver/digitalocean/Caddyfile',
                     'tools/receiver/digitalocean/cloud-init.yaml'],
}
FILES['enhance-pir-native'] = FILES['enhance-pir'] + ['tools/enhance/deploy/native-tag-integration.md']
# Kinds built with their own features and target directory, so only assembled
# when requested explicitly.
FILES['enhance-pir-native-cuda'] = FILES['enhance-pir-native'] + [
    'tools/enhance/scripts/cuda-smoke.py', 'tools/enhance/scripts/verify-cuda-deployment.py',
]
# The operations package needs no compiled binary: full CI assembles and tests
# it in its own job, so publishing it never waits on Rust compilation.
ON_REQUEST = ('pir-publisher', 'enhance-pir-native', 'enhance-pir-native-cuda', 'status-pir', 'wallet-pir-ops')
# Build-time protocol identity recorded in candidate.json and re-checked on extract.
PROTOCOLS = {
    'pir-publisher': 'wallet-pir-publisher-v1',
    'enhance-pir': 'ironwood-enhance-pir-v7',
    'enhance-pir-native': 'ironwood-enhance-pir-v9-native-two-mask-m29',
    'enhance-pir-native-cuda': 'ironwood-enhance-pir-v9-native-two-mask-m29',
}

# ABI/build identity is checked separately from live GPU qualification.
CUDA_BUILD = {
    'cuda': True, 'cpu_target': 'x86-64-v3', 'os': 'ubuntu-22.04',
    'glibc': '2.35', 'rust': '1.97.1', 'target': 'x86_64-unknown-linux-gnu',
}


def check_cuda_build(metadata):
    if metadata != CUDA_BUILD or metadata.get('cuda') is not True:
        raise ValueError('invalid CUDA build metadata')


def check_sha(sha):
    if not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('release revision must be a full lowercase commit SHA')


from .github_api import api  # noqa: E402


def qualified(run, sha):
    return (run['head_sha'] == sha and run['head_branch'] == 'main'
            and run['event'] in ('push', 'workflow_dispatch')
            and run['status'] == 'completed' and run['conclusion'] == 'success'
            and run['path'] == '.github/workflows/ci-full.yml')


def service_family(kind):
    if kind == 'wallet-pir-ops':
        return 'ops'
    if kind == 'receiver-pir':
        return 'receiver'
    if kind.startswith('transparent-'):
        return 'transparent'
    if kind in ('enhance-pir', 'enhance-pir-native', 'enhance-pir-native-cuda', 'status-pir', 'pir-publisher'):
        return 'enhance'
    raise ValueError('unknown release artifact kind')


def service_run(run, sha, repo):
    return (run.get('head_sha') == sha and run.get('head_branch') == 'main'
            and run.get('event') in ('push', 'workflow_dispatch')
            and run.get('path') == '.github/workflows/ci-qualify.yml'
            and (run.get('head_repository') or {}).get('full_name') == repo
            and isinstance(run.get('run_attempt'), int))


def service_jobs(jobs, family, cuda=False):
    prefix = {'ops': 'Operations', 'receiver': 'Receiver', 'transparent': 'Transparent',
              'enhance': 'Enhance'}[family]
    common = any(job.get('name', '').startswith('Common / ') for job in jobs)
    required = [f'{prefix} / {name}' for name in (
        ['Select full checks', 'Full checks complete'] +
        (['Prepare native CUDA release artifact (Ubuntu 22.04)'] if cuda else
         ['Build candidate release artifacts', 'Prepare qualified release artifacts']))]
    service = {'ops': [], 'receiver': ['Receiver lint and tests'],
               'enhance': ['Enhance lint', 'Enhance tests'],
               'transparent': ['Transparent lint', 'Transparent tests']}[family]
    required += [f'{prefix} / {name}' for name in service]
    infrastructure = (['Validate Enhance infrastructure', 'Validate transparent elastic infrastructure']
                      if common or family == 'ops' else
                      ['Validate transparent elastic infrastructure' if family == 'transparent'
                       else 'Validate Enhance infrastructure'])
    shared = ['Shared lint and tests'] if not common and family != 'ops' else []
    if common and family != 'ops':
        required += [f'Shared / {name}' for name in
                     ['Select full checks', 'Shared lint and tests', 'Full checks complete']]
    common_prefix = 'Common' if common else prefix
    required += [f'{common_prefix} / {name}' for name in (
        ['Select full checks', 'Full checks complete'] if common else []) +
        ['Validate selected helper checks', *shared, *infrastructure]]
    for name in set(required):
        # Reusable publication jobs can be skipped under the same display name
        # as the caller's successful publisher. Require one executed gate;
        # duplicate executed jobs, failures and missing gates still refuse.
        matching = [job for job in jobs if job.get('name') == name and job.get('conclusion') != 'skipped']
        if (len(matching) != 1 or matching[0].get('status') != 'completed'
                or matching[0].get('conclusion') != 'success'):
            return False

    return True


def resolve(sha, kind):
    check_sha(sha)
    repo = os.environ['GITHUB_REPOSITORY']
    family = service_family(kind)
    runs = api(f'repos/{repo}/actions/workflows/ci-qualify.yml/runs?head_sha={sha}&per_page=100')['workflow_runs']
    for run in runs:
        if not service_run(run, sha, repo):
            continue
        jobs = api(f'repos/{repo}/actions/runs/{run["id"]}/attempts/{run["run_attempt"]}/jobs?per_page=100')['jobs']
        if not service_jobs(jobs, family, kind == 'enhance-pir-native-cuda'):
            continue
        artifacts = api(f'repos/{repo}/actions/runs/{run["id"]}/artifacts?per_page=100')['artifacts']
        matching = [a for a in artifacts if a['name'] == f'{kind}-{sha}' and not a['expired']]
        if len(matching) == 1 and re.fullmatch(r'sha256:[0-9a-f]{64}', matching[0].get('digest', '')):
            with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
                out.write(f'run-id={run["id"]}\nartifact-digest={matching[0]["digest"]}\n')
            return
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
    raise ValueError(f'no qualified, unexpired {kind} artifact for {sha}; qualify its service on main first')


def assemble(sha, target, output, kind=None, build_metadata=None):
    check_sha(sha)
    dirty = bool(subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=normal'], cwd=ROOT))
    if kind == 'enhance-pir-native-cuda':
        check_cuda_build(build_metadata)
    output.mkdir(parents=True, exist_ok=False)
    selected = {kind: BINARIES[kind]} if kind else {k: v for k, v in BINARIES.items() if k not in ON_REQUEST}
    for kind, binaries in selected.items():
        if kind == 'wallet-pir-ops':
            from .bundle import assemble as assemble_operations
            assemble_operations(output / 'wallet-pir-ops.tar.gz', sha, root=ROOT)
            continue
        directory = output / kind
        directory.mkdir()
        for name in binaries:
            shutil.copy2(target / 'release' / name, directory / name)
            (directory / name).chmod(0o755)
        for source in FILES[kind]:
            shutil.copy2(ROOT / source, directory / Path(source).name)
        if kind in PROTOCOLS:
            (directory / 'candidate.json').write_text(json.dumps({
                'kind': kind, 'schema_version': 11, 'protocol_revision': PROTOCOLS[kind],
                'qualification': 'unqualified', 'source_revision': sha, 'source_dirty': dirty,
            }, indent=2) + '\n')
        if kind == 'enhance-pir-native-cuda':
            (directory / 'build.json').write_text(json.dumps(build_metadata, indent=2) + '\n')
        (directory / 'revision').write_text(sha + '\n')
        (directory / 'SHA256SUMS').write_text(''.join(
            f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n'
            for p in sorted(directory.iterdir())))
        with tarfile.open(output / f'{kind}.tar.gz', 'w:gz') as archive:
            for path in sorted(directory.iterdir()):
                archive.add(path, arcname=path.name)


def extract(archive_path, destination, sha, kind):
    check_sha(sha)
    if kind == 'wallet-pir-ops':
        from .bundle import extract as extract_operations
        return extract_operations(archive_path, destination, sha)
    # Flat regular files only; no archive-controlled paths, links or duplicate
    # members. Preserve executability explicitly (Actions zip downloads do not).
    with tarfile.open(archive_path, 'r:gz') as archive:
        members = archive.getmembers()
        names = [m.name for m in members]
        if len(names) != len(set(names)) or any(not m.isfile() or Path(m.name).name != m.name or m.name in ('.', '..') for m in members):
            raise ValueError('release archive must contain unique flat regular files')
        payload = {m.name: archive.extractfile(m).read() for m in members}
    required = set(BINARIES[kind]) | {Path(p).name for p in FILES[kind]} | {'revision', 'SHA256SUMS'}
    if kind in PROTOCOLS:
        required.add('candidate.json')
    if kind == 'enhance-pir-native-cuda':
        required.add('build.json')
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
    if kind in PROTOCOLS:
        candidate = json.loads(payload['candidate.json'])
        if (candidate.get('kind') != kind or candidate.get('source_revision') != sha
                or candidate.get('schema_version') != 11
                or candidate.get('protocol_revision') != PROTOCOLS[kind]
                or candidate.get('qualification') != 'unqualified'
                or not isinstance(candidate.get('source_dirty'), bool)):
            raise ValueError('invalid candidate metadata; qualification is a separate gate')
    if kind == 'enhance-pir-native-cuda':
        check_cuda_build(json.loads(payload['build.json']))
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
    parser.add_argument('--build-metadata', type=Path)
    args = parser.parse_args()
    if args.command == 'assemble':
        head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        if head != args.sha:
            raise ValueError('cannot label a build with a different checkout revision')
        metadata = json.loads(args.build_metadata.read_text()) if args.build_metadata else None
        assemble(args.sha, args.target, args.output, args.kind, metadata)
    elif args.command == 'resolve':
        resolve(args.sha, args.kind)
    else:
        extract(args.archive, args.output, args.sha, args.kind)


if __name__ == '__main__':
    main()
