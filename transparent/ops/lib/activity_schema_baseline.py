"""Private, bounded v10 rollback snapshots used by trusted schema phases.

Snapshot bytes are independent copies, never hard links to mutable state. Large
journal/publication/cache trees remain in their original, separate namespaces;
identity/sentinel checks fence their retention. No service is started or origin
reopened here: canonical anchors and warm retrieval must pass the recovery phase
before routing is restored. Call only from the wrapper's trusted phase program.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat

MAX_BYTES = 1024 * 1024 * 1024
MAX_FILES = 65536
EXCLUDED = {'ssh', '__pycache__'}
HEX = re.compile(r'^[0-9a-f]{64}$')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def checksum(path):
    with Path(path).open('rb') as stream:
        hasher, size = hashlib.sha256(), 0
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            require(size <= MAX_BYTES, 'baseline checksum exceeds byte bound')
            hasher.update(chunk)
        return hasher.hexdigest()


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic(path, data, mode=0o600):
    temporary = path.with_name(path.name+'.next')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        with os.fdopen(fd, 'wb') as out:
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
        sync_dir(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def safe_path(value):
    require(isinstance(value, str) and value.startswith('/') and '\x00' not in value,
            'snapshot paths must be absolute')
    path = Path(value)
    require(path != Path('/') and '..' not in path.parts, 'unsafe snapshot path')
    return path


def validate_plan(plan, root):
    root = safe_path(str(root))
    physical_root = root.resolve()
    require(isinstance(plan, dict) and set(plan) == {'version', 'files', 'retained'}, 'invalid baseline plan')
    require(type(plan['version']) is int and plan['version'] == 1, 'unsupported baseline plan')
    require(isinstance(plan['files'], list) and 1 <= len(plan['files']) <= 256, 'invalid baseline files')
    require(isinstance(plan['retained'], list) and 1 <= len(plan['retained']) <= 256, 'invalid retained paths')
    paths = []
    for item in plan['files']:
        require(isinstance(item, dict) and set(item) == {'path', 'required'}, 'invalid baseline file entry')
        require(type(item['required']) is bool, 'required must be boolean')
        path = safe_path(item['path'])
        physical = path.parent.resolve()/path.name
        require(physical != physical_root and physical_root not in physical.parents and physical not in physical_root.parents,
                'backup and live state overlap')
        require(all(physical != p and physical not in p.parents and p not in physical.parents for p in paths),
                'overlapping baseline paths')
        paths.append(physical)
    for item in plan['retained']:
        require(isinstance(item, dict) and set(item) == {'path', 'sentinel', 'sha256'}, 'invalid retained path')
        path, sentinel = safe_path(item['path']), safe_path(item['sentinel'])
        require(isinstance(item['sha256'], str) and HEX.fullmatch(item['sha256']), 'invalid retained checksum')
        physical = path.resolve()
        require(physical != physical_root and physical_root not in physical.parents and physical not in physical_root.parents,
                'retention overlaps backup')
        require(path.resolve() in sentinel.resolve().parents, 'retention sentinel must be inside retained tree')
        # Publisher activation records live at the root of the retained
        # publication namespace. Snapshot these small mutable files separately
        # while retaining the large tables in place; never copy table trees.
        controls = {'active.json', 'withdrawn.json', 'activation.json'}
        require(all((p.parent == physical and p.name in controls and not p.is_symlink() and
                     (not p.exists() or p.is_file())) or
                    (physical != p and physical not in p.parents and p not in physical.parents) for p in paths),
                'large retained trees cannot be copied as mutable state')
    return plan


def describe(path, remaining=MAX_BYTES):
    info = path.lstat()
    mode = stat.S_IMODE(info.st_mode)
    ownership = {'uid': info.st_uid, 'gid': info.st_gid}
    if stat.S_ISLNK(info.st_mode):
        return {'kind': 'symlink', 'mode': mode, **ownership, 'target': os.readlink(path)}
    if stat.S_ISREG(info.st_mode):
        require(info.st_size <= remaining, 'baseline bytes exceed bound')
        return {'kind': 'file', 'mode': mode, **ownership, 'size': info.st_size, 'sha256': checksum(path)}
    if stat.S_ISDIR(info.st_mode):
        return {'kind': 'directory', 'mode': mode, **ownership}
    raise ValueError('special files cannot be rollback inputs')


def entries(path):
    """Enumerate a state tree without traversing links or copying locks/sockets."""
    result = {'.': describe(path)}
    remaining = MAX_BYTES - result['.'].get('size', 0)
    if result['.']['kind'] == 'directory':
        for current, directories, files in os.walk(path, followlinks=False):
            directories[:] = sorted(d for d in directories if d not in EXCLUDED)
            for name in directories + sorted(files):
                if name.endswith('.lock'):
                    continue
                entry = Path(current)/name
                value = describe(entry, remaining)
                remaining -= value.get('size', 0)
                result[str(entry.relative_to(path))] = value
                require(len(result) <= MAX_FILES, 'baseline file count exceeds bound')
    return result


def retained_identity(item):
    path, sentinel = Path(item['path']), Path(item['sentinel'])
    require(path.is_dir() and sentinel.is_file(), 'retained rollback data is missing')
    require(checksum(sentinel) == item['sha256'], 'retained rollback sentinel changed')
    info = path.stat()
    return {'device': info.st_dev, 'inode': info.st_ino, 'resolved': str(path.resolve())}


def verify(root):
    """Validate the entire snapshot before the first restoring side effect."""
    root = safe_path(str(root))
    require(root.is_dir() and not root.is_symlink(), 'baseline directory is missing or a symlink')
    record = json.loads((root/'complete.json').read_text())
    require(record.get('version') == 1, 'unsupported baseline receipt')
    require(root.stat().st_mode & 0o077 == 0 and root.stat().st_uid == os.geteuid(),
            'baseline requires a private owned directory')
    plan = validate_plan(record['plan'], root)
    require(record['plan_sha256'] == hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest(),
            'baseline plan changed')
    require(set(record['files']) == {item['path'] for item in plan['files']}, 'incomplete baseline coverage')
    expected = {'complete.json'}
    total, count = 0, 0
    for index, item in enumerate(plan['files']):
        saved = root/'files'/str(index)
        tree = record['files'][item['path']]
        if tree is None:
            require(not item['required'] and not saved.exists() and not saved.is_symlink(), 'required baseline missing')
            continue
        require(entries(saved) == tree, 'rollback bytes or modes changed')
        for relative, info in tree.items():
            expected.add(str(Path('files')/str(index)) if relative == '.' else str(Path('files')/str(index)/relative))
            total += info.get('size', 0)
            count += 1
    require(total <= MAX_BYTES and count <= MAX_FILES, 'baseline exceeds bounds')
    # Reject added payloads as well as changed/missing named files. Structural
    # directories are allowed; no unrecorded regular file or link is trusted.
    for current, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(current)/name
            if path.is_file() or path.is_symlink():
                require(str(path.relative_to(root)) in expected, 'unexpected baseline payload')
    require(len(record['retained']) == len(plan['retained']), 'incomplete retained identities')
    for item, identity in zip(plan['retained'], record['retained']):
        require(retained_identity(item) == identity, 'retained rollback namespace changed')
    return record


def capture(root, plan):
    root = Path(root)
    validate_plan(plan, root)
    if root.exists():
        record = verify(root)  # Partial captures are never silently reused.
        require(record['plan'] == plan, 'a different baseline already exists')
        return record
    root.mkdir(parents=True, mode=0o700)
    os.chmod(root, 0o700)
    (root/'files').mkdir(mode=0o700)
    sync_dir(root.parent)
    retained = [retained_identity(item) for item in plan['retained']]
    trees, total, count = {}, 0, 0
    for index, item in enumerate(plan['files']):
        source, saved = Path(item['path']), root/'files'/str(index)
        if not source.exists() and not source.is_symlink():
            require(not item['required'], 'required baseline path is missing')
            trees[item['path']] = None
            continue
        tree = entries(source)
        total += sum(info.get('size', 0) for info in tree.values())
        count += len(tree)
        require(total <= MAX_BYTES and count <= MAX_FILES, 'baseline exceeds bounds')
        disk = shutil.disk_usage(root)
        require(disk.free-total >= disk.total * .20, 'rollback snapshot would violate disk headroom')
        for relative, info in tree.items():
            src = source if relative == '.' else source/relative
            dst = saved if relative == '.' else saved/relative
            if info['kind'] == 'directory':
                dst.mkdir(mode=info['mode'])
                os.chown(dst, info['uid'], info['gid'])
                os.chmod(dst, info['mode'])
            elif info['kind'] == 'symlink':
                dst.symlink_to(info['target'])
                os.chown(dst, info['uid'], info['gid'], follow_symlinks=False)
            else:
                shutil.copyfile(src, dst, follow_symlinks=False)
                os.chown(dst, info['uid'], info['gid'])
                os.chmod(dst, info['mode'])
                with dst.open('rb') as stream:
                    os.fsync(stream.fileno())
        require(entries(source) == tree and entries(saved) == tree, 'live baseline changed during capture')
        # fsync every directory before publishing the completion receipt.
        for current, _, _ in os.walk(saved if saved.is_dir() and not saved.is_symlink() else saved.parent, topdown=False):
            sync_dir(current)
        trees[item['path']] = tree
    record = {'version': 1, 'plan': plan, 'plan_sha256': hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest(),
              'files': trees, 'retained': retained}
    atomic(root/'complete.json', json.dumps(record, sort_keys=True).encode())
    return verify(root)


def restore(root, include=None):
    """Restore only reviewed targets; retain displaced candidate state for audit.

    Routing files are deliberately restored separately by reopen-v10, after its
    canonical verifier. Caller supplies include to defer Caddy/load/scaler files.
    Repeating a restore compares matching bytes and is safe after a partial run.
    """
    root = Path(root)
    record = verify(root)
    allowed = {item['path'] for item in record['plan']['files']}
    require(include is None or set(include) <= allowed, 'restore targets were not captured')
    for index, item in enumerate(record['plan']['files']):
        if include is not None and item['path'] not in include:
            continue
        target, saved = Path(item['path']), root/'files'/str(index)
        tree = record['files'][item['path']]
        if target.exists() or target.is_symlink():
            if tree is not None and entries(target) == tree:
                continue
            # Displaced data stays on the live filesystem, since the private
            # backup volume and root filesystem can differ.
            displaced = target.with_name(target.name+'.schema-displaced-'+record['plan_sha256'][:12])
            require(not displaced.exists() and not displaced.is_symlink(), 'displaced state needs explicit reconciliation')
            os.rename(target, displaced)
            sync_dir(target.parent)
        if tree is None:
            continue
        temporary = target.with_name(target.name+'.schema-restore-next')
        require(not temporary.exists() and not temporary.is_symlink(), 'interrupted restore needs explicit reconciliation')
        if tree['.']['kind'] == 'directory':
            shutil.copytree(saved, temporary, symlinks=True)
        elif tree['.']['kind'] == 'symlink':
            temporary.symlink_to(tree['.']['target'])
        else:
            shutil.copy2(saved, temporary, follow_symlinks=False)
        for relative, info in tree.items():
            entry = temporary if relative == '.' else temporary/relative
            os.chown(entry, info['uid'], info['gid'], follow_symlinks=False)
            if info['kind'] != 'symlink':
                os.chmod(entry, info['mode'])
        require(entries(temporary) == tree, 'restored bytes differ from baseline')
        for current, _, files in os.walk(temporary, topdown=False):
            for name in files:
                path = Path(current)/name
                if not path.is_symlink():
                    with path.open('rb') as stream:
                        os.fsync(stream.fileno())
            sync_dir(current)
        if temporary.is_file() and not temporary.is_symlink():
            with temporary.open('rb') as stream:
                os.fsync(stream.fileno())
        os.replace(temporary, target)
        sync_dir(target.parent)
    return record
