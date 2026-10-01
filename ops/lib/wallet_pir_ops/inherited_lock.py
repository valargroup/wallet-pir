"""Keep a wrapper-owned production lock alive in deployment descendants.

The descriptor is local to the coordinator, including when its child is SSH.
It is never sent as an environment value to a remote host. Absence permits the
existing standalone development helpers; a malformed inheritance claim fails
before spawning anything.
"""
import os
import re
import stat
import sys
from pathlib import Path

VARIABLE = 'WALLET_PIR_PRODUCTION_LOCK_FDS'


def descriptors(required=False, path=None):
    value = os.environ.get(VARIABLE, '')
    if not value:
        if required:
            raise ValueError('production phase requires an inherited wrapper lock')
        return ()
    if not re.fullmatch(r'[0-9]+(?:,[0-9]+)*', value):
        raise ValueError('invalid inherited production lock descriptors')
    values = tuple(map(int, value.split(',')))
    if len(values) > 4 or len(set(values)) != len(values) or min(values) < 3:
        raise ValueError('invalid inherited production lock descriptor set')
    for fd in values:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError('inherited production lock must be a private owned regular file')
    if path is not None:
        target = Path(path).lstat()
        if not stat.S_ISREG(target.st_mode):
            raise ValueError('production lock path is not a regular file')
        if not any((os.fstat(fd).st_dev, os.fstat(fd).st_ino) == (target.st_dev, target.st_ino) for fd in values):
            raise ValueError('inherited descriptor does not name the production lock')
    return values


def options():
    """Keyword arguments for both subprocess.run and create_subprocess_exec."""
    fds = descriptors()
    if not fds:
        return {}
    return {'pass_fds': fds, 'env': dict(os.environ, PYTHONDONTWRITEBYTECODE='1')}


def transport_command(argv):
    """SSH closes extra FDs; retain them in a surviving, timeout-safe keeper."""
    argv = list(map(str,argv))
    if descriptors() and argv and Path(argv[0]).name in ('ssh','scp','rsync'):
        return [sys.executable,'-B',str(Path(__file__).with_name('ssh_lock_keeper.py')),*argv]
    return argv
