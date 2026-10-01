"""Stdlib-only source-staging helper, prefixed with the shared hostlock source.

Only the deployment wrapper transmits/runs this helper. It has no standalone
entry point without that prefix. Root controls all paths and is trusted.
"""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sys
import tarfile
import tempfile
import time

STAGE_ROOT = Path('/srv/transparent-activity/ops')
LOCK_PATH = Path('/run/lock/wallet-pir-production.lock')
MEMINFO = Path('/proc/meminfo')
MAX_COMPRESSED = 64 << 20
MAX_EXPANDED = 512 << 20
MAX_ENTRIES = 16384


class SourceStageError(ValueError):
    pass


def require(ok, message):
    if not ok:
        raise SourceStageError(message)


def sha256(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def atomic_json(path, value):
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix='.receipt-')
    try:
        with os.fdopen(descriptor, 'w') as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def headroom(root):
    fields = {}
    with MEMINFO.open() as stream:
        for line in stream:
            key, _, value = line.partition(':')
            if key in ('MemTotal', 'MemAvailable'):
                fields[key] = int(value.split()[0])
    require(fields.get('MemTotal', 0) > 0 and fields.get('MemAvailable', 0)*5 >= fields['MemTotal'],
            'source staging memory below 20 percent available')
    parent = root
    while not parent.exists():
        parent = parent.parent
    disk = os.statvfs(parent)
    require(disk.f_bavail * disk.f_frsize >= MAX_COMPRESSED+MAX_EXPANDED+(1 << 30),
            'insufficient staging disk reserve')
    require(disk.f_bavail / disk.f_blocks >= .20, 'staging disk below 20 percent headroom')


def unpack(archive, target, source, lock):
    files, seen, expanded = {}, set(), 0
    with tarfile.open(archive, 'r:gz') as tar:
        require(tar.pax_headers.get('comment') == source, 'archive is not pinned to the requested git commit')
        for count, entry in enumerate(tar, 1):
            require(count <= MAX_ENTRIES, 'source archive has too many entries')
            path = PurePosixPath(entry.name)
            require(entry.name and not path.is_absolute() and '..' not in path.parts
                    and str(path) not in ('.', '') and '\\' not in entry.name
                    and not any(ord(c) < 32 for c in entry.name), 'unsafe source archive path')
            name = str(path)
            require(name not in seen, 'duplicate source archive path')
            seen.add(name)
            require(entry.isdir() or entry.isfile(), 'source archive links and special files are forbidden')
            expanded += entry.size
            require(entry.size >= 0 and expanded <= MAX_EXPANDED, 'source archive expansion exceeds bound')
            lock.verify()
            output = target/name
            if entry.isdir():
                output.mkdir(parents=True, exist_ok=False, mode=0o700)
                continue
            output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            # No links exist in the fresh, private extraction directory.
            with tar.extractfile(entry) as incoming, output.open('xb') as stream:
                shutil.copyfileobj(incoming, stream, 1 << 20)
                stream.flush()
                os.fchmod(stream.fileno(), 0o700 if entry.mode & 0o111 else 0o600)
                os.fsync(stream.fileno())
            files[name] = sha256(output)
    require('ops/scripts/wallet-pir-deploy.py' in files and
            'transparent/ops/lib/activity_schema_operation.py' in files, 'source archive lacks schema deployment wrapper')
    return files


def verify_receipt(receipt, target, source, checksum):
    require(receipt.get('version') == 1, 'unsupported source staging receipt version')
    require(receipt.get('source_sha') == source and receipt.get('archive_sha256') == checksum,
            'retained source identity differs')
    require(receipt.get('status') == 'staged', 'unfinished source staging; reconcile its private receipt first')
    expected = receipt['files']
    require(isinstance(expected, dict) and 1 <= len(expected) <= MAX_ENTRIES, 'invalid retained source file list')
    for name, checksum in expected.items():
        path = PurePosixPath(name)
        require(not path.is_absolute() and '..' not in path.parts and str(path) == name
                and isinstance(checksum, str) and len(checksum) == 64, 'invalid retained source file identity')
    paths = list(target.rglob('*'))
    require(not any(p.is_symlink() for p in paths), 'retained source contains a symlink')
    actual = {str(p.relative_to(target)) for p in paths if not p.is_dir()}
    require(actual == set(expected), 'retained source file set changed')
    for name, checksum in expected.items():
        path = target/name
        require(not path.is_symlink() and path.is_file() and sha256(path) == checksum,
                'retained source file checksum changed')


def stage(request, lock, incoming, root=STAGE_ROOT):
    source, checksum = request['source_sha'], request['sha256']
    target = root/'sources'/source
    receipt_path = root/'staging'/(source+'.json')
    lock.verify()
    if request['mode'] == 'status':
        if not receipt_path.exists():
            return {'status': 'absent', 'source_sha': source}
        receipt = json.loads(receipt_path.read_text())
        if receipt.get('status') == 'staged':
            verify_receipt(receipt, target, source, checksum)
        return {'status': receipt['status'], 'source_sha': receipt['source_sha'],
                'archive_sha256': receipt['archive_sha256'], 'path': str(target),
                'verified_files': len(receipt.get('files', {})) if receipt.get('status') == 'staged' else 0}
    headroom(root)
    if target.exists() or receipt_path.exists():
        require(target.is_dir() and not target.is_symlink() and receipt_path.is_file(), 'incomplete retained source staging')
        receipt = json.loads(receipt_path.read_text())
        verify_receipt(receipt, target, source, checksum)
        return {'status': 'staged', 'source_sha': source, 'archive_sha256': checksum, 'path': str(target)}
    if request['mode'] == 'preflight':
        return {'status': 'preflight-passed', 'source_sha': source, 'archive_sha256': checksum, 'path': str(target)}
    require(request['mode'] == 'stage', 'unsupported source stage mode')
    lock.verify()
    (root/'sources').mkdir(parents=True, exist_ok=True, mode=0o700)
    (root/'staging').mkdir(parents=True, exist_ok=True, mode=0o700)
    receipt = {'version': 1, 'status': 'receiving', 'source_sha': source,
               'archive_sha256': checksum, 'started_unix': time.time(), 'pid': os.getpid()}
    atomic_json(receipt_path, receipt)  # Intent before receiving or extracting.
    temporary = Path(tempfile.mkdtemp(dir=root, prefix='.source-'))
    try:
        archive, extraction = temporary/'source.tar.gz', temporary/'extract'
        with archive.open('xb') as stream:
            size, digest = 0, hashlib.sha256()
            for chunk in iter(lambda: incoming.read(1 << 20), b''):
                size += len(chunk)
                require(size <= MAX_COMPRESSED, 'compressed source archive exceeds bound')
                lock.verify()
                stream.write(chunk)
                digest.update(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        require(digest.hexdigest() == checksum, 'received source archive checksum differs')
        extraction.mkdir(mode=0o700)
        files = unpack(archive, extraction, source, lock)
        # Persist directory entries as well as file contents before publishing
        # the immutable source directory and its completed receipt.
        directories = [extraction, *(p for p in extraction.rglob('*') if p.is_dir())]
        for directory in sorted(directories, key=lambda p: len(p.parts), reverse=True):
            lock.verify()
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        lock.verify()
        headroom(root)
        require(not target.exists(), 'source target appeared during extraction')
        os.rename(extraction, target)
        fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        # Retain the exact reviewed export for coordinator-to-worker bootstrap;
        # no separately uploaded archive or reconstruction is needed.
        retained_archive=root/'staging'/(source+'.tar.gz')
        require(not retained_archive.exists(), 'retained source archive already exists')
        os.chmod(archive,0o400)
        os.rename(archive,retained_archive)
        fd=os.open(retained_archive.parent,os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)
        receipt.update(status='staged', files=files, compressed_bytes=size, completed_unix=time.time(),
                       retained_archive=str(retained_archive))
        atomic_json(receipt_path, receipt)
        return {'status': 'staged', 'source_sha': source, 'archive_sha256': checksum, 'path': str(target)}
    except BaseException as error:
        # If a crash happened after rename, retain that source and the unfinished
        # receipt; a retry cannot silently accept or overwrite it.
        receipt.update(status='failed', error_type=type(error).__name__)
        atomic_json(receipt_path, receipt)
        raise
    finally:
        shutil.rmtree(temporary)


def main():
    request = json.loads(sys.argv[1])
    try:
        require(set(request) == {'mode', 'source_sha', 'sha256', 'machine_id'}, 'invalid source stage request')
        require(request['mode'] in ('preflight', 'stage', 'status'), 'invalid source stage mode')
        import re
        for field, size in [('source_sha', 40), ('sha256', 64), ('machine_id', 32)]:
            require(isinstance(request[field], str) and re.fullmatch('[0-9a-f]{'+str(size)+'}', request[field]),
                    'invalid source stage identity')
        config = {'type': 'pinned_host', 'machine_id': request['machine_id']}
        # PinnedHostLock comes from the exact shared hostlock source prefix.
        lock = PinnedHostLock(config, path=LOCK_PATH)
        if request['mode'] == 'stage':
            with lock:
                local_schema_fence()  # Exact shared source is prefixed by the wrapper.
                result = stage(request, lock, sys.stdin.buffer)
        else:
            require(os.geteuid() == 0 and lock.MACHINE_ID.read_text().strip() == request['machine_id'],
                    'source staging is not on the pinned root coordinator')
            # Read-only preflight/status must not even create a lock file.
            result = stage(request, PinnedHostLock(None), sys.stdin.buffer)
        print(json.dumps({'ok': True, 'result': result}))
    except Exception as error:
        # No raw paths, request arguments, stderr or archive contents in errors.
        message = str(error) if isinstance(error, SourceStageError) else type(error).__name__
        print(json.dumps({'ok': False, 'error': message}))


if __name__ == '__main__':
    main()
