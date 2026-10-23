"""Offline provider collection, invoked only by the locked maintenance operation.

The stopped worker has no live reader or candidate pins. Keep its saved active
inputs, explicitly retained rollback trees, and three unused publications. Plans
include file identities, so a changed tree or control record refuses deletion.
No symlink is traversed while counting or removing files.
"""
from collections import Counter
from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat

DIGEST = re.compile('[0-9a-f]{64}')
STAGE = re.compile('staging-[0-9]+-[0-9]+')
CONTROL = ('active.json', 'active.invalid.json', 'revocation-floor.json',
           'revocation-fallback.json', 'publisher-high-water.json', 'publisher-high-water-fallback.json')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def real_directory(path):
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise ValueError('collection requires a real directory: ' + str(path))


def identities(root, paths):
    """Names and lstat identities below selected paths, without following links."""
    result = []
    for path in paths:
        info = path.lstat()
        result.append([str(path.relative_to(root)), info.st_dev, info.st_ino, info.st_mode,
                       info.st_size, info.st_mtime_ns, info.st_nlink, info.st_blocks,
                       os.readlink(path) if stat.S_ISLNK(info.st_mode) else None])
        if stat.S_ISDIR(info.st_mode):
            result.extend(identities(root, sorted(path.iterdir())))
    return result


def plan(root, retained):
    """Compute an exact offline deletion manifest; the caller holds both owner locks."""
    root = Path(root)
    real_directory(root)
    root = root.resolve(strict=True)
    keep = set()
    controls = {}
    for name in CONTROL:
        path = root / name
        try:
            if not stat.S_ISREG(path.lstat().st_mode):
                raise ValueError('provider control record must be a regular file: ' + name)
            raw = path.read_bytes()
        except FileNotFoundError:
            continue
        controls[name] = hashlib.sha256(raw).hexdigest()
        value = json.loads(raw)
        if name == 'active.json':
            retained = [*retained, value['directory'], *([value['runtimes']] if value.get('runtimes') else []),
                        *([value['assignment']] if value.get('assignment') else [])]
    for path in retained:
        relative = Path(path).resolve(strict=True).relative_to(root)
        if not relative.parts:
            raise ValueError('retained input names the provider root')
        if not (DIGEST.fullmatch(relative.parts[0]) or STAGE.fullmatch(relative.parts[0])):
            raise ValueError('retained input must belong to a publication tree')
        real_directory(root / relative.parts[0])
        keep.add(relative.parts[0])
    trees = [path for path in root.iterdir() if (DIGEST.fullmatch(path.name) or STAGE.fullmatch(path.name))
             and stat.S_ISDIR(path.lstat().st_mode)]
    unused = sorted((p for p in trees if p.name not in keep), key=lambda p: (p.stat().st_mtime_ns, p.name), reverse=True)
    keep.update(p.name for p in unused[:3])
    removed = [p for p in trees if p.name not in keep]
    references = {p.name for tree in trees if tree.name in keep for p in tree.iterdir() if DIGEST.fullmatch(p.name)}
    sources, objects = root / '.sources', root / 'objects'
    for directory in (sources, objects):
        if directory.exists() or directory.is_symlink():
            real_directory(directory)
    if sources.exists():
        removed += [p for p in sources.iterdir() if DIGEST.fullmatch(p.name) and p.name not in references
                    and stat.S_ISDIR(p.lstat().st_mode)]
    rows = identities(root, sorted(removed))
    links = Counter((row[1], row[2]) for row in rows if stat.S_ISREG(row[3]))
    # Copy-fallback caches are reusable downloads and follow the live collector's
    # policy. A hard-linked map proves the normal content-addressed link mode.
    maps = [tree / 'shards.json' for tree in trees if tree.name in keep and (tree / 'shards.json').exists()]
    if any(not stat.S_ISREG(path.lstat().st_mode) for path in maps):
        raise ValueError('retained maps must be regular files')
    linked = not maps or all(p.lstat().st_nlink >= 2 for p in maps)
    if objects.exists() and linked:
        removed += [p for p in objects.iterdir() if DIGEST.fullmatch(p.name)
                    and stat.S_ISREG(p.lstat().st_mode)
                    and p.stat().st_nlink - links[(p.stat().st_dev, p.stat().st_ino)] == 1]
        removed_objects = {p.name for p in removed if p.parent == objects}
        for path in objects.iterdir():
            marker = re.fullmatch('([0-9a-f]{64})(?:\\.received\\.json|\\.checked-[0-9a-f]{64})', path.name)
            if (marker and stat.S_ISREG(path.lstat().st_mode)
                    and (marker[1] in removed_objects or not (objects / marker[1]).exists())):
                removed.append(path)
    rows = identities(root, sorted(removed))
    links = Counter((row[1], row[2]) for row in rows if stat.S_ISREG(row[3]))
    freed = {(row[1], row[2]): row[7] * 512 for row in rows
             if stat.S_ISREG(row[3]) and links[(row[1], row[2])] == row[6]}
    manifest = {'root': str(root), 'retained': sorted(keep), 'control_sha256': controls,
                'delete': sorted(str(p.relative_to(root)) for p in removed), 'identities': rows,
                'reclaimable_bytes': sum(freed.values())}
    return manifest


def run(root, retained, expected=None):
    """Plan, or apply only that exact manifest, while excluding worker and staging owners."""
    root = Path(root)
    real_directory(root)
    with ExitStack() as locks:
        for name in ('bootstrap.lock', '.sources.lock'):
            descriptor = os.open(root / name, os.O_RDWR | os.O_NOFOLLOW)
            locks.callback(os.close, descriptor)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest = plan(root, retained)
        manifest_sha256 = digest(manifest)
        if expected is not None:
            if manifest_sha256 != expected:
                raise ValueError('collection manifest changed; no data was deleted')
            # Trees, then sources, then objects: no linked object is removed
            # before the publication and catalog links that make it obsolete.
            for prefix in ('', '.sources/', 'objects/'):
                for name in manifest['delete']:
                    if (prefix == '' and '/' not in name) or (prefix and name.startswith(prefix)):
                        path = root / name
                        if path.is_symlink():
                            raise ValueError('collection target became a symlink')
                        if path.is_dir():
                            shutil.rmtree(path)
                        else:
                            path.unlink()
            for path in (root, root / '.sources', root / 'objects'):
                if path.is_dir():
                    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
        space = os.statvfs(root)
        return {'manifest': manifest, 'manifest_sha256': manifest_sha256,
                'applied': expected is not None, 'available_bytes': space.f_bavail * space.f_frsize}
