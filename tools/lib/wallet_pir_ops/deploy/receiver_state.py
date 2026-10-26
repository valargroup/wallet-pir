"""Stream a stopped Receiver directory between authenticated deployment connections.

The archive includes a content manifest, verified before the staged directory is
installed. The transfer receipt identifies the transaction for interrupted
rollback. This module is also sent verbatim to hosts; it uses the standard
library only and never transfers credentials.
"""
import fcntl
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import pwd
import shutil
import stat
import sys
import tarfile

MANIFEST = '.receiver-transfer.json'
OWNER = '.receiver-transfer-owner'


def inventory(root):
    files = {}
    for path in sorted(root.rglob('*')):
        if path.name in (MANIFEST, OWNER) and path.parent == root:
            continue
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise ValueError('Receiver state contains a link or special file')
        digest = hashlib.sha256()
        with path.open('rb') as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b''):
                digest.update(chunk)
        files[path.relative_to(root).as_posix()] = [path.stat().st_size, digest.hexdigest()]
    if not {'directory-v3.sqlite', 'distribution/authority.json'} <= files.keys():
        raise ValueError('Receiver state is missing its database or publication authority')
    return files


def pack(root, transaction):
    if root.is_symlink():
        raise ValueError('Receiver state root must not be a link')
    with (root / 'publisher.lock').open('rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        files = inventory(root)
        receipt = json.dumps({'transaction': transaction, 'files': files}, sort_keys=True).encode()
        with gzip.GzipFile(fileobj=sys.stdout.buffer, mode='wb', compresslevel=1) as compressed, \
                tarfile.open(fileobj=compressed, mode='w|') as archive:
            for path in sorted(root.iterdir()):
                if path.name not in (MANIFEST, OWNER):
                    archive.add(path, arcname=path.name)
            entry = tarfile.TarInfo(MANIFEST)
            entry.size, entry.mode = len(receipt), 0o600
            archive.addfile(entry, io.BytesIO(receipt))


def unpack(root, transaction, user):
    if root.name not in ('.receiver-adoption-' + transaction, '.receiver-return-' + transaction):
        raise ValueError('Receiver extraction must use its transaction staging directory')
    if root.exists():
        if root.is_symlink() or (root / OWNER).read_text() != transaction:
            raise ValueError('Receiver staging directory belongs to another owner')
        shutil.rmtree(root)
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    try:
        (root / OWNER).write_text(transaction)
        with tarfile.open(fileobj=sys.stdin.buffer, mode='r|gz') as archive:
            for entry in archive:
                name = PurePosixPath(entry.name)
                if name.is_absolute() or '..' in name.parts or not (
                        entry.isfile() or entry.isdir() or entry.islnk()):
                    raise ValueError('unsafe Receiver state archive member')
                archive.extract(entry, root, filter='data')
        # Consume the sender's remaining trailer before it can close its SSH
        # connection. Installation still waits for the complete manifest check.
        for _chunk in iter(lambda: sys.stdin.buffer.read(1 << 20), b''):
            pass
        receipt = json.loads((root / MANIFEST).read_text())
        if receipt['transaction'] != transaction or receipt['files'] != inventory(root):
            raise ValueError('Receiver state transfer differs from its manifest')
        account = pwd.getpwnam(user)
        for path in [*root.rglob('*'), root]:
            os.chown(path, account.pw_uid, account.pw_gid, follow_symlinks=False)
        # Flush both extracted data and directory entries before installation.
        for path in [*root.rglob('*'), root]:
            descriptor = os.open(path, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        print(json.dumps({'files': len(receipt['files']),
                          'bytes': sum(value[0] for value in receipt['files'].values())}))
    except BaseException:
        shutil.rmtree(root)
        raise


if __name__ == '__main__':
    operation, directory, identifier, *arguments = sys.argv[1:]
    if operation == 'pack':
        pack(Path(directory), identifier)
    elif operation == 'unpack':
        unpack(Path(directory), identifier, arguments[0])
    else:
        raise ValueError('unknown Receiver transfer operation')
