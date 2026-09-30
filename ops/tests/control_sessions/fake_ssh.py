"""A stand-in for `ssh` in the control-session tests. Contacts nothing.

Every invocation appends its arguments to $FAKE_SSH_LOG. `-O check` answers
from the control socket. A master fails its first $FAKE_SSH_FAILURES starts
the way a refused connection does; later starts bind their `-L` listeners and
their control socket, then run until SIGTERM, as `ssh -N -M` does.
"""
import fcntl
import json
import os
from pathlib import Path
import signal
import socket
import sys
import threading

args = sys.argv[1:]
if os.environ.get('FAKE_SSH_LOG'):
    with open(os.environ['FAKE_SSH_LOG'], 'a') as log:
        log.write(json.dumps({'pid': os.getpid(), 'args': args}) + '\n')

control = next(a.split('=', 1)[1] for a in args if a.startswith('-oControlPath='))
destination = args[-1]

if '-O' in args:
    probe = socket.socket(socket.AF_UNIX)
    try:
        probe.connect(control)
    except OSError:
        print(f'Control socket connect({control}): No such file or directory', file=sys.stderr)
        sys.exit(255)
    print(f'Master running (pid={os.getpid()})', file=sys.stderr)
    sys.exit(0)

# Both sessions start together. The shared failure count has to move in one
# step, or two masters can read the same value and the suite sees an extra
# refused connection.
state = Path(os.environ['FAKE_SSH_STATE'])
counter = state / 'starts'
with (state / 'starts.lock').open('a') as handle:
    fcntl.flock(handle, fcntl.LOCK_EX)
    starts = int(counter.read_text()) + 1 if counter.exists() else 1
    counter.write_text(str(starts))
if starts <= int(os.environ.get('FAKE_SSH_FAILURES', '0')):
    print(f'ssh: connect to host {destination.partition("@")[2]} port 22: Connection refused', file=sys.stderr)
    sys.exit(255)

listeners = []


def serve(listener, reply):
    while True:
        try:
            connection, _ = listener.accept()
        except OSError:
            return
        with connection:
            try:
                connection.sendall(reply)
            except OSError:
                pass


try:
    for flag, spec in zip(args, args[1:]):
        if flag != '-L':
            continue
        if spec.startswith('/'):
            path, _, target = spec.partition(':')
            listener = socket.socket(socket.AF_UNIX)
            listener.bind(path)
        else:
            host, port, target = spec.split(':', 2)
            listener = socket.socket()
            listener.bind((host, int(port)))
        listener.listen(8)
        listeners.append(listener)
        threading.Thread(target=serve, args=(listener, target.encode() + b'\n'), daemon=True).start()
except OSError as error:
    print(f'bind: {error.strerror}', file=sys.stderr)
    print('Could not request local forwarding.', file=sys.stderr)
    sys.exit(255)

master = socket.socket(socket.AF_UNIX)
master.bind(control)
master.listen(8)
threading.Thread(target=serve, args=(master, b'mux\n'), daemon=True).start()


def stop(*_):
    os.unlink(control)
    os._exit(0)


signal.signal(signal.SIGTERM, stop)
while True:
    signal.pause()
