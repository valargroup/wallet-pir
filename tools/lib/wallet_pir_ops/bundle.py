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
PREFIXES = ('tools/lib/', 'tools/scripts/', 'tools/deploy/', 'tools/infra/',
            'tools/enhance/deploy/', 'tools/enhance/scripts/', 'tools/enhance/lib/',
            'tools/transparent/lib/', 'tools/transparent/scripts/', 'tools/transparent/deploy/',
            'tools/transparent/scaler/', 'tools/transparent/systemd/', 'tools/receiver/digitalocean/',
            'tools/receiver/lib/', 'tools/receiver/deploy/', 'tools/status/deploy/', 'tools/status/lib/',
            'tools/txid/deploy/', 'tools/txid/lib/', 'tools/txid/scripts/')
EXACT = {'tools/transparent/__init__.py', 'tools/enhance/__init__.py', 'tools/receiver/__init__.py',
         'tools/status/__init__.py', 'tools/txid/__init__.py',
         'src/shared/pir-service-discovery/Cargo.toml',
         'tools/ci/release.py', 'docs/operations.md', 'docs/transparent/prepared-distribution.md'}
EXTENSIONS = {'.py', '.sh', '.toml', '.service', '.timer', '.in', '.caddy', '.tf', '.tftpl', '.yaml'}
MAX_BYTES = 32 * 1024 * 1024
REQUIRED = {'tools/scripts/wallet-pir-deploy.py', 'tools/scripts/wallet-pir-roles.py',
            'tools/scripts/wallet-pir-bundle.py', 'tools/deploy/services.toml',
            'tools/lib/wallet_pir_ops/__init__.py', 'tools/lib/wallet_pir_ops/deploy/host_helper.py',
            'tools/lib/wallet_pir_ops/release.py', 'tools/transparent/lib/deploy_cli.py',
            'tools/transparent/lib/roles.py', 'tools/receiver/digitalocean/service.toml',
            'tools/enhance/lib/roles.py', 'tools/receiver/lib/roles.py'}
# Resource directories added after retained releases were installed; those
# releases name their own inventory and remain valid without them.
LATER_PREFIXES = ('tools/status/deploy/', 'tools/status/lib/', 'tools/txid/deploy/', 'tools/txid/lib/', 'tools/txid/scripts/')


def mode(name):
    """Permission bits of a packaged file: scripts and modules executable, the rest read-only."""
    return 0o755 if name.endswith(('.py', '.sh')) else 0o644


def allowed(name):
    path = PurePosixPath(name)
    return (not path.is_absolute() and str(path) == name and '..' not in path.parts
            and not any(p in ('tests', '__pycache__', '.terraform') for p in path.parts)
            and (name in EXACT or (name.startswith(PREFIXES) and
                 (path.suffix in EXTENSIONS or path.name == 'Caddyfile' or name.endswith('.example.json')))))


def contents(root, *, required_exact=None):
    # Current assembly needs every current exact resource. A retained installed
    # release instead names its own exact inventory; still enumerate any extra
    # recognized files so additions cannot disappear from self-check.
    required_exact = EXACT if required_exact is None else required_exact
    files, total = {}, 0
    for prefix in PREFIXES:
        directory = root / prefix
        if not directory.is_dir():
            if required_exact is not EXACT and prefix in LATER_PREFIXES:
                continue
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
        if not path.exists() and not path.is_symlink() and name not in required_exact:
            continue
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
    return write_archive(destination, revision, contents(root), dirty)


def write_archive(destination, revision, files, dirty):
    """The archive of `files` labelled with `revision`; `assemble` supplies a checkout's files."""
    # `qualification` describes this integrity artifact only. Full-CI provenance
    # and product qualification are separate records that never live in it.
    manifest = {'schema': SCHEMA, 'source_revision': revision, 'source_dirty': dirty,
                'qualification': 'unqualified', 'files': {
                    name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())}}
    files = dict(files)
    files['operations.json'] = (json.dumps(manifest, sort_keys=True, indent=2) + '\n').encode()
    if sum(map(len, files.values())) > MAX_BYTES:
        raise ValueError('operations release exceeds its size bound')
    with Path(destination).open('xb') as stream, tarfile.open(fileobj=stream, mode='w:gz') as archive:
        for name, data in sorted(files.items()):
            member = tarfile.TarInfo(name)
            member.size = len(data)
            member.mode = mode(name)
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
    raw = payload.pop('operations.json')
    manifest = json.loads(raw, object_pairs_hook=unique)
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
    payload['operations.json'] = raw
    return manifest, payload


def planned_files(archive_path, revision):
    """`(manifest, {name: [sha256, mode]}, bytes)` of a verified archive, its manifest included."""
    manifest, payload = verify(archive_path, revision)
    files = {name: [hashlib.sha256(data).hexdigest(), mode(name)] for name, data in payload.items()}
    return manifest, files, sum(map(len, payload.values()))


def extract(archive_path, destination, revision):
    manifest, payload = verify(archive_path, revision)
    destination = Path(destination)
    # Exclusive new root prevents pre-existing symlinks or unrelated files.
    destination.mkdir(mode=0o755, parents=False, exist_ok=False)
    for name, data in payload.items():
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        path.chmod(mode(name))
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
    installed = contents(root, required_exact=set(manifest['files']))
    if set(installed) != set(manifest['files']):
        raise ValueError('installed operations inventory changed')
    for name, digest in manifest['files'].items():
        if not allowed(name) or hashlib.sha256(installed[name]).hexdigest() != digest:
            raise ValueError('installed operations resource changed: ' + name)
    if mutation and manifest.get('source_dirty') is not False:
        raise ValueError('dirty operations releases are inspection-only; use a clean full-CI release')
    if revision is not None and revision != manifest.get('source_revision'):
        raise ValueError('binary and operations releases must name the same source revision')


def self_check(revision, root=ROOT):
    """This unpacked release, where it is, holds exactly its manifest's files for `revision`."""
    if not (root / 'operations.json').is_file():
        raise ValueError('not an unpacked operations release: %s' % root)
    check_installed(root=root, revision=revision)
    return json.loads((root / 'operations.json').read_text())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('assemble', 'verify', 'extract', 'self-check'),
                        help='self-check verifies the unpacked release this script belongs to')
    parser.add_argument('--sha', required=True)
    parser.add_argument('--archive', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    if args.command == 'self-check':
        result = self_check(args.sha)
    elif args.archive is None:
        parser.error('%s requires --archive' % args.command)
    elif args.command == 'assemble':
        result = assemble(args.archive, args.sha)
    elif args.command == 'verify':
        result, _ = verify(args.archive, args.sha)
    elif args.output is None:
        parser.error('extract requires --output')
    else:
        result = extract(args.archive, args.output, args.sha)
    print(json.dumps({'schema': result['schema'], 'source_revision': result['source_revision'],
                      'source_dirty': result['source_dirty'], 'files': len(result['files'])}, sort_keys=True))
