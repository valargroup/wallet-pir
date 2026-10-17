#!/usr/bin/env python3
"""Preserve verified checkout mtimes alongside a hosted Cargo target.

This is an optimization within an existing cache trust partition, never an
alternative to Cargo's fingerprint checks. Only regular, tracked files inside
this checkout are eligible. No Cargo fingerprints or outputs are modified.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import time

SCHEMA = 2
METADATA = '.wallet-pir-sources.json'


def digest(value):
    return hashlib.sha256(value).hexdigest()


def tracked(root):
    return subprocess.check_output(['git', 'ls-files', '-z'], cwd=root).split(b'\0')


def owned_target(root, target):
    """Reject aliases and source overlap before creating or moving anything.

    Keep the lexical path: resolving an alias first loses the ownership check.
    Repeat after extraction, since the archive can introduce new aliases.
    """
    require('..' not in root.parts and '..' not in target.parts)
    root, target = Path(os.path.abspath(root)), Path(os.path.abspath(target))
    for path in (root, target):
        require(not any(p.is_symlink() for p in (path, *path.parents)))
        require(path.resolve() == path)
    require(not root.is_relative_to(target))
    require(not target.exists() or target.is_dir())
    require(not target.is_relative_to(root / '.git'))
    for name in tracked(root):
        if name:
            source = root / os.fsdecode(name)
            require(not source.is_relative_to(target) and not target.is_relative_to(source))
    return target


def linked(root, target):
    # A symlink can alias another tracked file whose restored mtime would hide
    # different bytes at the original input path. Do not reuse any outputs in
    # such a checkout, including links to external/untracked destinations.
    # Include ignored/untracked inputs: an include_str!/build-script input can
    # alias an unchanged tracked destination normalized below. Do not traverse
    # links, Git internals or the separately validated output directory.
    def failed(error):
        raise error
    for directory, dirs, names in os.walk(root, followlinks=False, onerror=failed):
        parent = Path(directory)
        for name in dirs + names:
            path = parent / name
            if path == root / '.git' or path == target:
                if name in dirs:
                    dirs.remove(name)
                continue
            if path.is_symlink():
                return True
    return False


def files(root):
    result = {}
    for name in tracked(root):
        if not name:
            continue
        relative = os.fsdecode(name)
        path = root / relative
        try:
            # Do not traverse a symlink, including a symlink in a parent.
            if not path.resolve().is_relative_to(root) or any(p.is_symlink() for p in (path, *path.parents)):
                continue
            info = path.stat()
            if stat.S_ISREG(info.st_mode):
                result[relative] = [digest(path.read_bytes()), info.st_mtime_ns, stat.S_IMODE(info.st_mode)]
        except FileNotFoundError:
            pass
    return result


def fingerprints(target):
    """Bind source metadata to this target state, including timestamp checks.

    Fingerprint bytes alone do not identify a rebuild of changed source bytes.
    Cargo's dep-info / invoked.timestamp mtimes change even in that case.
    """
    result = {}
    for directory in [*target.glob('*/.fingerprint'), *target.glob('*/*/.fingerprint')]:
        for path in sorted(directory.rglob('*')):
            if path.is_file():
                result[path.relative_to(target).as_posix()] = [digest(path.read_bytes()), path.stat().st_mtime_ns]
    return digest(json.dumps(result, sort_keys=True).encode())


def encoded(record):
    return json.dumps(record, sort_keys=True).encode()


def require(condition):
    if not condition:
        raise ValueError('incompatible workspace timestamp record')


def restore(root, target, identity, pending):
    """Validate the entire record before restoring any source timestamp.

    Missing/corrupt/stale metadata or changed bytes leave source files newer
    than restored fingerprints, even if the caller supplied old source mtimes.
    Capture the inputs BEFORE compilation; sealing them is a separate step.
    """
    target = owned_target(root, target)
    root = root.resolve()
    symlinks = linked(root, target)
    if symlinks and target.exists():
        # Preserve the restored archive for inspection, outside the saved path.
        # Never edit Cargo fingerprints or follow/touch a symlink destination.
        unused = Path(tempfile.mkdtemp(prefix=target.name + '-unused-', dir=target.parent))
        target.rename(unused / 'target')
    target.mkdir(parents=True, exist_ok=True)
    current = files(root)
    previous = {}
    status = 'miss'
    try:
        envelope = json.loads((target / METADATA).read_text())
        record = envelope['record']
        require(envelope['checksum'] == digest(encoded(record)))
        require(record['schema'] == SCHEMA and record['identity'] == identity and record['root'] == str(root))
        require(record['fingerprints'] == fingerprints(target))
        previous = record['files']
        require(isinstance(previous, dict))
        for name, entry in previous.items():
            require(isinstance(name, str) and isinstance(entry, list) and len(entry) == 3)
            require(isinstance(entry[0], str) and len(entry[0]) == 64)
            require(type(entry[1]) is int and 0 < entry[1] <= record['captured_ns'])
            require(type(entry[2]) is int and 0 <= entry[2] <= 0o7777)
        status = 'verified'
    except (OSError, ValueError, KeyError, TypeError):
        previous = {}
    if symlinks:
        previous = {}
        status = 'symlink-cold'
    # Future-dated cache timestamps may cost reuse, but cannot make changed
    # inputs look fresh. Do not mutate restored fingerprints to manufacture hits.
    newest = max((p.stat().st_mtime_ns for p in target.rglob('*') if p.is_file()), default=0)
    changed_time = max(time.time_ns(), newest + 1)
    reused = 0
    for name, entry in current.items():
        old = previous.get(name)
        matches = bool(old and old[0] == entry[0] and old[2] == entry[2])
        stamp = old[1] if matches else changed_time
        reused += matches
        path = root / name
        os.utime(path, ns=(path.stat().st_atime_ns, stamp))
        entry[1] = stamp
    record = {'schema': SCHEMA, 'identity': identity, 'root': str(root),
              'captured_ns': max(time.time_ns(), changed_time), 'files': current}
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_bytes(encoded(record))
    print('CI_WORKSPACE_CACHE ' + json.dumps({'metadata': status, 'restored_source_mtimes': reused,
                                             'new_source_mtimes': len(current) - reused}), flush=True)


def seal(root, target, identity, pending):
    """Co-archive only inputs unchanged since the pre-build capture.

    actions/cache saves this target only on job success. Changes made by build
    scripts or other commands after capture are deliberately ineligible.
    """
    record = json.loads(pending.read_text())
    require(record['identity'] == identity and record['root'] == str(root.resolve()))
    current = files(root.resolve())
    record['files'] = {name: entry for name, entry in record['files'].items() if current.get(name) == entry}
    record['fingerprints'] = fingerprints(target)
    target.mkdir(parents=True, exist_ok=True)
    (target / METADATA).write_text(json.dumps({'record': record, 'checksum': digest(encoded(record))}, sort_keys=True))
