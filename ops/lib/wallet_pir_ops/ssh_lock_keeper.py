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


def main(argv):
    inherited_lock.descriptors(required=True)
    if not argv or Path(argv[0]).name not in ('ssh','scp','rsync'):
        raise ValueError('lock keeper requires a transport command')
    pid = os.fork()
    if pid:
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
        status = child.wait()
    except BaseException:
        status = 75
    os._exit(status if 0 <= status <= 255 else 75)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
