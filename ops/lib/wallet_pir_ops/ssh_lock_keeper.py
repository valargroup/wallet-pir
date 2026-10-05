"""Retain wrapper lock FDs until SSH ends, including after launcher timeout.

OpenSSH closes extra descriptors during startup. A separate keeper therefore
holds them outside SSH. Its launcher can be killed without releasing the
keeper's descriptors; reconciliation must acquire the lock after SSH exits.
"""
import os
from pathlib import Path
import signal
import subprocess
import sys

if __package__ is None or __package__ == '':
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wallet_pir_ops import inherited_lock


def release_stdio():
    """Drop this process's pipe ends without closing the lock descriptors.

    Fork leaves both the waiter and the survivor holding the launcher's
    stdin, stdout and stderr. Remote writes and the host lock session read
    stdin until EOF, and a full stdout pipe blocks the transport. Only the
    transport process may keep those ends.
    """
    null = os.open(os.devnull, os.O_RDWR)
    try:
        for fd in (0, 1, 2):
            os.dup2(null, fd)
    finally:
        os.close(null)


def main(argv):
    inherited_lock.descriptors(required=True)
    if not argv or Path(argv[0]).name not in ('ssh','scp','rsync'):
        raise ValueError('lock keeper requires a transport command')
    pid = os.fork()
    if pid:
        release_stdio()
        _, status = os.waitpid(pid, 0)
        return os.waitstatus_to_exitcode(status)
    # This process survives timeout/termination of its launcher. SSH itself
    # gets ordinary signal behavior and no misleading descriptor environment.
    env = dict(os.environ)
    env.pop(inherited_lock.VARIABLE, None)
    signals = (signal.SIGINT,signal.SIGTERM,signal.SIGHUP)
    for sig in signals:
        signal.signal(sig, signal.SIG_IGN)
    def restore_child_signals():
        for sig in signals:
            signal.signal(sig, signal.SIG_DFL)
    try:
        child = subprocess.Popen(argv, env=env, close_fds=True, preexec_fn=restore_child_signals)
        release_stdio()
        status = child.wait()
    except BaseException:
        status = 75
    os._exit(status if 0 <= status <= 255 else 75)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
