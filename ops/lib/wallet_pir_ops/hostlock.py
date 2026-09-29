"""A root-only writer lock pinned to one machine.

DigitalOcean Spaces does not enforce Terraform's conditional S3 lock writes,
so a Spaces-backed state has no lock of its own. Every writer of such a state
instead runs as root on one designated host and holds a flock there.
"""
import fcntl
import os
from pathlib import Path
import re
import stat


class PinnedHostLock:
    """Serialize state writers on one pinned Linux host.

    All manual writers must use the same host and lock. This is an operational
    single-writer boundary, not a distributed lock or protection from root.
    Pass `fd` to child processes (`subprocess.run(pass_fds=(lock.fd,))`) so an
    interrupted controller cannot release the lock while its child, such as a
    Terraform apply, is still running.

    `config` is `{"type": "pinned_host", "machine_id": "<32 hex>"}`, or None
    for a backend that locks natively, in which case this is a no-op.
    Subclasses choose `PATH`; `path` overrides it per instance.
    """
    PATH = None
    MACHINE_ID = Path('/etc/machine-id')
    ROOT_UID = 0

    def __init__(self, config, path=None):
        self.config = config
        self.fd = None
        if path is not None:
            self.PATH = Path(path)

    def __enter__(self):
        if self.config is None:
            return self
        if (not isinstance(self.config, dict) or set(self.config) != {'type', 'machine_id'}
                or self.config['type'] != 'pinned_host'
                or not isinstance(self.config['machine_id'], str)
                or not re.fullmatch('[0-9a-f]{32}', self.config['machine_id'])
                or self.MACHINE_ID.read_text().strip() != self.config['machine_id']
                or os.geteuid() != self.ROOT_UID or self.PATH is None):
            raise ValueError('state writer must run as root on the pinned host')
        fd = os.open(self.PATH, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != self.ROOT_UID or info.st_mode & 0o077:
                raise ValueError('state lock requires a private root-owned regular file')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.fd = fd
            self.verify()
        except BaseException:
            self.fd = None
            os.close(fd)
            raise
        return self

    def verify(self):
        """Refuse if the lock is not held or its path now names another file."""
        if self.config is not None:
            if self.fd is None:
                raise ValueError('pinned-host state lock is not held')
            opened, current = os.fstat(self.fd), Path(self.PATH).lstat()
            if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
                raise ValueError('state lock file was replaced')

    def descriptors(self):
        """The descriptors a child must inherit to keep this lock alive."""
        if self.config is None:
            return ()
        self.verify()
        return (self.fd,)

    def __exit__(self, *_args):
        if self.fd is not None:
            # Do not LOCK_UN: a surviving Terraform child shares this lock.
            os.close(self.fd)
            self.fd = None
