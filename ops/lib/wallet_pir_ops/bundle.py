"""Checksummed, relocatable operations releases for the existing SSH fleet.

Only maintained code/resources are packaged; inventories, credentials, runtime
state, tests and retained evidence are never inputs. Verification precedes any
extraction. Binary releases keep their own existing artifact verification.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile

from .paths import ROOT

SCHEMA = 'wallet-pir-operations-v1'
PREFIXES = ('ops/lib/', 'ops/scripts/', 'ops/deploy/', 'ops/infra/',
            'enhance/ops/deploy/', 'enhance/ops/scripts/',
            'transparent/ops/lib/', 'transparent/ops/scripts/', 'transparent/ops/deploy/',
            'transparent/ops/scaler/', 'transparent/ops/systemd/', 'receiver/ops/digitalocean/')
EXACT = {'transparent/__init__.py', 'transparent/ops/__init__.py',
         'tools/ci/release.py', 'docs/operations.md'}
EXTENSIONS = {'.py', '.sh', '.toml', '.service', '.timer', '.in', '.caddy', '.tf', '.tftpl', '.yaml'}
MAX_BYTES = 32 * 1024 * 1024
REQUIRED = {'ops/scripts/wallet-pir-deploy.py', 'ops/scripts/wallet-pir-roles.py',
            'ops/scripts/wallet-pir-bundle.py', 'ops/deploy/services.toml',
            'ops/lib/wallet_pir_ops/__init__.py', 'ops/lib/wallet_pir_ops/deploy/host_helper.py',
            'ops/lib/wallet_pir_ops/release.py', 'transparent/ops/lib/deploy_cli.py',
            'transparent/ops/lib/roles.py', 'receiver/ops/digitalocean/service.toml'}


def allowed(name):
    path = PurePosixPath(name)
    return (not path.is_absolute() and str(path) == name and '..' not in path.parts
            and not any(p in ('tests', '__pycache__', '.terraform') for p in path.parts)
            and (name in EXACT or (name.startswith(PREFIXES) and
                 (path.suffix in EXTENSIONS or path.name == 'Caddyfile' or name.endswith('.example.json')))))


def contents(root):
    files, total = {}, 0
    for prefix in PREFIXES:
        directory = root / prefix
        if not directory.is_dir():
            raise ValueError('operations resource directory missing: ' + prefix)
        for path in sorted(directory.rglob('*')):
            name = path.relative_to(root).as_posix()
            if allowed(name) and path.is_file():
                if path.is_symlink():
                    raise ValueError('operations resources must be regular files: ' + name)
                total += path.stat().st_size
                if total > MAX_BYTES:
                    raise ValueError('operations resources exceed their size bound')
                files[name] = path.read_bytes()
    for name in EXACT:
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise ValueError('operations resource missing: ' + name)
        total += path.stat().st_size
        if total > MAX_BYTES:
            raise ValueError('operations resources exceed their size bound')
        files[name] = path.read_bytes()
    if not REQUIRED <= files.keys():
        raise ValueError('operations resources are incomplete')
    return files


def assemble(destination, revision, *, root=ROOT):
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('operations revision must be a full commit SHA')
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    if revision != head:
        raise ValueError('operations revision does not match the source checkout')
    dirty = bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=root))
    files = contents(root)
    manifest = {'schema': SCHEMA, 'source_revision': revision, 'source_dirty': dirty,
                'qualification': 'unqualified', 'files': {
                    name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())}}
    files['operations.json'] = (json.dumps(manifest, sort_keys=True, indent=2) + '\n').encode()
    if sum(map(len, files.values())) > MAX_BYTES:
        raise ValueError('operations release exceeds its size bound')
    with Path(destination).open('xb') as stream, tarfile.open(fileobj=stream, mode='w:gz') as archive:
        for name, data in sorted(files.items()):
            member = tarfile.TarInfo(name)
            member.size = len(data)
            member.mode = 0o755 if name.endswith(('.py', '.sh')) else 0o644
            archive.addfile(member, io.BytesIO(data))
    return manifest


def verify(archive_path, revision):
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('operations revision must be a full commit SHA')
    payload, total = {}, 0
    with tarfile.open(archive_path, mode='r:gz') as archive:
        for member in archive:
            if (not member.isfile() or member.name in payload or member.size < 0
                    or not (member.name == 'operations.json' or allowed(member.name))):
                raise ValueError('operations archive contains a duplicate, unsafe or unowned file')
            total += member.size
            if total > MAX_BYTES:
                raise ValueError('operations archive exceeds its size bound')
            payload[member.name] = archive.extractfile(member).read()
    if 'operations.json' not in payload:
        raise ValueError('operations manifest missing')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate operations manifest field')
            result[key] = value
        return result
    manifest = json.loads(payload.pop('operations.json'), object_pairs_hook=unique)
    if (not isinstance(manifest, dict) or set(manifest) !=
            {'schema', 'source_revision', 'source_dirty', 'qualification', 'files'}
            or manifest['schema'] != SCHEMA or manifest['source_revision'] != revision
            or type(manifest['source_dirty']) is not bool or manifest['qualification'] != 'unqualified'
            or not isinstance(manifest['files'], dict) or set(manifest['files']) != set(payload)
            or not REQUIRED <= payload.keys()):
        raise ValueError('operations manifest revision or inventory mismatch')
    for name, data in payload.items():
        if hashlib.sha256(data).hexdigest() != manifest['files'][name]:
            raise ValueError('operations checksum mismatch: ' + name)
    return manifest, payload


def extract(archive_path, destination, revision):
    manifest, payload = verify(archive_path, revision)
    destination = Path(destination)
    # Exclusive new root prevents pre-existing symlinks or unrelated files.
    destination.mkdir(mode=0o755, parents=False, exist_ok=False)
    for name, data in payload.items():
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        path.chmod(0o755 if name.endswith(('.py', '.sh')) else 0o644)
    (destination / 'operations.json').write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n')
    return manifest


def check_installed(*, mutation=False, revision=None, root=ROOT):
    """An unpacked release must keep every reviewed file and binary revision.

    A checkout has no operations manifest and continues to use its existing
    source-review procedures. Dirty packaged sources are inspection-only.
    """
    path = root / 'operations.json'
    if not path.exists():
        return
    manifest = json.loads(path.read_text())
    if (not isinstance(manifest, dict) or set(manifest) !=
            {'schema', 'source_revision', 'source_dirty', 'qualification', 'files'}
            or manifest['schema'] != SCHEMA or not isinstance(manifest['source_revision'], str)
            or not re.fullmatch('[0-9a-f]{40}', manifest['source_revision'])
            or type(manifest['source_dirty']) is not bool or manifest['qualification'] != 'unqualified'
            or not isinstance(manifest['files'], dict) or not REQUIRED <= manifest['files'].keys()):
        raise ValueError('installed operations manifest is incomplete')
    installed = contents(root)
    if set(installed) != set(manifest['files']):
        raise ValueError('installed operations inventory changed')
    for name, digest in manifest['files'].items():
        if not allowed(name) or hashlib.sha256(installed[name]).hexdigest() != digest:
            raise ValueError('installed operations resource changed: ' + name)
    if mutation and manifest.get('source_dirty') is not False:
        raise ValueError('dirty operations releases are inspection-only; use a clean full-CI release')
    if revision is not None and revision != manifest.get('source_revision'):
        raise ValueError('binary and operations releases must name the same source revision')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('assemble', 'verify', 'extract'))
    parser.add_argument('--sha', required=True)
    parser.add_argument('--archive', required=True, type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    if args.command == 'assemble':
        result = assemble(args.archive, args.sha)
    elif args.command == 'verify':
        result, _ = verify(args.archive, args.sha)
    elif args.output is None:
        parser.error('extract requires --output')
    else:
        result = extract(args.archive, args.output, args.sha)
    print(json.dumps({'schema': result['schema'], 'source_revision': result['source_revision'],
                      'source_dirty': result['source_dirty'], 'files': len(result['files'])}, sort_keys=True))
