#!/usr/bin/env python3
"""Select CUDA without Cargo or a CUDA installation.

Compare designated integration contracts and the backend's locked dependency
closure at both revisions. Workspace membership and unrelated lock entries do
not require CUDA. Missing comparison inputs fail closed, with a visible reason.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parents[2]
BACKEND = 'src/shared/pir-cuda/'
CONTRACTS = (
    'src/enhance/enhance-query-server/src/matvec',
    'src/enhance/enhance-query-server/src/runtime',
    'src/enhance/enhance-query-server/src/worker',
    'src/status/status-query-server/src/backend.rs',
    'src/status/status-query-server/src/generation.rs',
    'src/status/status-query-server/src/distributed.rs',
    'src/shared/pir-runtime/src/evaluation.rs',
    'src/pir-query-server/tests/operator.rs',
    'src/pir-query-server/src/main.rs',
    'src/pir-query-server/src/config.rs',
    'tools/pir-e2e-tests/tests/http.rs',
    'tools/pir-e2e-tests/tests/support/encrypted.rs',
    'src/status/status-query-server/tests/status_wallet.rs',
)
TOOLS = {
    'tools/ci/cuda_changes.py', 'tools/tests/test_cuda_changes.py',
    'tools/ci/cuda_boundary.py', 'tools/tests/test_verify_cuda_build.py',
    'tools/ci/feedback_base.py', 'tools/tests/test_feedback_base.py',
    'tools/ci/backend_features.py', 'tools/ci/test_matrix.py',
    'tools/ci/build-cuda.sh',
    'tools/ci/verify_cuda_build.py', 'tools/ci/qualification.py',
    'tools/ci/cargo_cache.py', '.github/workflows/ci-cuda.yml',
    '.github/actions/verify-qualification/action.yml',
    '.github/actions/qualify-artifacts/action.yml',
}
WORKFLOWS = {'.github/workflows/ci.yml', '.github/workflows/ci-full.yml',
             '.github/workflows/ci-qualify.yml', '.github/workflows/ci-matrix-audit.yml'}
FORWARDERS = {
    'src/enhance/enhance-query-server/Cargo.toml',
    'src/status/status-query-server/Cargo.toml',
    'src/status/status-publisher/Cargo.toml', 'src/pir-query-server/Cargo.toml',
    'tools/pir-e2e-tests/Cargo.toml',
}


def locked_backend(lock):
    """Return complete lock records reachable from the CUDA implementation.

    Cargo qualifies ambiguous dependency names with a version and source. Refuse
    an ambiguous edge rather than treating an incomplete graph as unaffected.
    """
    packages = lock['package']
    pending = [p for p in packages if p['name'] in {'pir-cuda', 'ipir-sp', 'simplepir-kernel'}]
    if not any(p['name'] == 'simplepir-kernel' for p in pending):
        raise ValueError('CUDA kernel is absent from the lockfile')
    seen = {}
    while pending:
        package = pending.pop()
        identity = (package['name'], package['version'], package.get('source', ''))
        if identity in seen:
            continue
        seen[identity] = package
        for edge in package.get('dependencies', []):
            match = re.fullmatch(r'([^ ]+)(?: ([^ ]+))?(?: \((.+)\))?', edge)
            if match is None:
                raise ValueError(f'unknown lock dependency: {edge}')
            name, version, source = match.groups()
            candidates = [p for p in packages if p['name'] == name
                          and (version is None or p['version'] == version)
                          and (source is None or p.get('source') == source)]
            if len(candidates) != 1:
                raise ValueError(f'cannot resolve locked CUDA dependency: {edge}')
            pending.extend(candidates)
    return [seen[key] for key in sorted(seen)]


def manifest_contract(data):
    """Feature forwarding and direct evaluator identities affect CUDA builds."""
    result = {'features': data.get('features', {})}
    for kind in ('dependencies', 'build-dependencies', 'dev-dependencies'):
        result[kind] = {name: value for name, value in data.get(kind, {}).items()
                        if name in {'pir-cuda', 'ipir-sp', 'simplepir-kernel'}
                        or isinstance(value, dict) and any(
                            feature == 'cuda' for feature in value.get('features', []))}
    result['target'] = data.get('target', {})
    return result


def workflow_contract(contents):
    # Entire CUDA job bodies matter, including their setup and conditions. Other
    # jobs only contribute the lines that pass CUDA selection between workflows.
    jobs = re.split(r'(?=^  [a-z][\w-]*:\n)', contents, flags=re.M)
    return '\n'.join(section if re.match(r'  [\w-]*cuda[\w-]*:', section)
                     else '\n'.join(line for line in section.splitlines()
                                    if 'cuda' in line.lower()) for section in jobs)


def select(paths, read_before, read_after):
    reasons = []
    try:
        for path in paths:
            if path.startswith(BACKEND) or path.startswith(CONTRACTS) or path in TOOLS:
                reasons.append(f'{path}: CUDA implementation or integration contract')
            elif path.startswith('.cargo/') or path in {'rust-toolchain', 'rust-toolchain.toml'}:
                reasons.append(f'{path}: compiler or build configuration')
            elif path in FORWARDERS:
                if manifest_contract(tomllib.loads(read_before(path))) != manifest_contract(tomllib.loads(read_after(path))):
                    reasons.append(f'{path}: evaluator dependencies or feature forwarding changed')
            elif path in WORKFLOWS:
                if workflow_contract(read_before(path)) != workflow_contract(read_after(path)):
                    reasons.append(f'{path}: CUDA workflow contract changed')
            elif path == 'Makefile':
                if workflow_contract(read_before(path)) != workflow_contract(read_after(path)):
                    reasons.append('Makefile: CUDA commands changed')
            elif path == 'tools/ci/test-matrix.json':
                def cuda_targets(contents):
                    return {name: shard for name, shard in json.loads(contents).items()
                            if any(key.startswith('cuda:') for key in shard['targets'])}
                if cuda_targets(read_before(path)) != cuda_targets(read_after(path)):
                    reasons.append(f'{path}: CUDA qualification targets changed')
        if {'Cargo.toml', 'Cargo.lock'} & set(paths):
            before = locked_backend(tomllib.loads(read_before('Cargo.lock')))
            after = locked_backend(tomllib.loads(read_after('Cargo.lock')))
            if before != after:
                reasons.append('Cargo.lock: CUDA backend dependency graph changed')
            names = {p['name'] for p in [*before, *after]}
            def build_contract(contents):
                data = tomllib.loads(contents)
                workspace = dict(data.get('workspace', {}))
                workspace.pop('members', None)
                workspace.pop('default-members', None)
                if 'dependencies' in workspace:
                    workspace['dependencies'] = {
                        name: spec for name, spec in workspace['dependencies'].items()
                        if (spec.get('package', name) if isinstance(spec, dict) else name) in names}
                return {'workspace': workspace, 'profile': data.get('profile', {}),
                        'patch': {source: {name: spec for name, spec in entries.items() if name in names}
                                  for source, entries in data.get('patch', {}).items()}}
            if build_contract(read_before('Cargo.toml')) != build_contract(read_after('Cargo.toml')):
                reasons.append('Cargo.toml: CUDA dependency patches or build settings changed')
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        reasons.append(f'CUDA comparison is incomplete ({type(error).__name__}); require coverage')
    return {'cuda': bool(reasons), 'reasons': reasons}


def comparison(base, head='HEAD', *, local=False, root=ROOT):
    if not base or set(base) == {'0'}:
        return {'cuda': True, 'reasons': ['no comparison revision; require CUDA coverage']}
    def git(*args):
        return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.DEVNULL).decode()
    try:
        paths = set(git('diff', '--name-only', '--no-renames', '-z', base, head).split('\0')) - {''}
        if local:
            paths.update(git('diff', '--name-only', '--no-renames', '-z', head).split('\0'))
            paths.update(git('ls-files', '--others', '--exclude-standard', '-z').split('\0'))
            paths.discard('')
        return select(sorted(paths), lambda path: git('show', f'{base}:{path}'),
                      lambda path: (root / path).read_text() if local else git('show', f'{head}:{path}'))
    except subprocess.CalledProcessError:
        return {'cuda': True, 'reasons': ['comparison revision unavailable; require CUDA coverage']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', default='')
    parser.add_argument('--head', default='HEAD')
    parser.add_argument('--local', action='store_true')
    parser.add_argument('--github-output', action='store_true')
    args = parser.parse_args()
    result = comparison(args.base, args.head, local=args.local)
    print(json.dumps(result, indent=2))
    if args.github_output:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
            output.write(f'cuda={str(result["cuda"]).lower()}\n')


if __name__ == '__main__':
    main()
