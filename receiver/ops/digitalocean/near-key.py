#!/usr/bin/env python3
"""Installs a NEAR Intents explorer partner key on the receiver Droplet.

The key becomes /etc/receiver-pir/near-<id>.env, the file the unit names through
the deploy inventory's NEAR_KEY (see README.md). Run it on the coordinator under
the production lock, with exactly one newline-terminated key line on stdin:

    printf '%s\\n' "$KEY" | flock -n /run/lock/wallet-pir-production.lock near-key.py install <droplet> <id>

Input without its final newline is refused as incomplete, so a producer that
dies partway cannot install a truncated key. Stdin must end within 30 seconds
and hold at most one line of 4096 characters, so a stalled or runaway producer
cannot hold the lock. The key never appears in an argument, in output or in a
file on the coordinator; it reaches the Droplet on SSH's stdin. Key files are
never edited or deleted: an id already installed with the same key is a no-op
and one with another key is refused. All of them stay, a few bytes each,
because rolling back through several deploys needs the key files their units
name.
"""
import os
import re
import select
import shlex
import subprocess
import sys
import time

ID = re.compile(r'[A-Za-z0-9._-]+')
KEY_LINE = re.compile(rb'[A-Za-z0-9._~+/=-]{1,4096}\n')
# The longest acceptable stdin: a 4096-character key and its newline.
MAX_STDIN = 4097
STDIN_SECONDS = 30
# The whole remote step, connection included.
REMOTE_SECONDS = 60
SSH = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
       '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3']
USAGE = 'usage: near-key.py install <droplet> <id>, with one key line on stdin'

# Runs on the Droplet as root, with the id as its argument and the key line on
# stdin, which it checks again. A temporary file is never named near-*.env, so
# one an interrupted run leaves is never read as a key.
REMOTE = r'''
import os, re, secrets, stat, sys

directory = "/etc/receiver-pir"
key_id = sys.argv[1]
data = sys.stdin.buffer.read(4098)
if not re.fullmatch(r"[A-Za-z0-9._-]+", key_id) or not re.fullmatch(rb"[A-Za-z0-9._~+/=-]{1,4096}\n", data):
    sys.exit("refused: malformed id, or stdin is not one complete key line")
content = b"NEAR_INTENTS_EXPLORER=" + data
final = os.path.join(directory, "near-" + key_id + ".env")


def existing():
    """The bytes at the final name, or None when nothing is there; refuses anything but a file."""
    try:
        fd = os.open(final, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    except OSError:
        settle(None)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        settle(None)
    with os.fdopen(fd, "rb") as handle:
        return handle.read()


def settle(current):
    """Exits for an existing final name holding `current`: 0 if it is this key, else 1."""
    if current == content:
        print(final + " is already installed with this key")
        sys.exit(0)
    sys.exit("refused: " + final + " exists with another key or is not a file; key files are never replaced")


os.makedirs(directory, mode=0o700, exist_ok=True)
info = os.lstat(directory)
if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
    sys.exit("refused: " + directory + " must be a directory owned by this user and writable by no one else")
current = existing()
if current is not None:
    settle(current)
temp = os.path.join(directory, ".near-" + key_id + "." + secrets.token_hex(8) + ".tmp")
fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
try:
    with os.fdopen(fd, "wb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.link(temp, final)
    except FileExistsError:
        settle(existing())
finally:
    try:
        os.unlink(temp)
    except FileNotFoundError:
        pass
for path in (directory, os.path.dirname(directory)):
    dfd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
print("installed " + final)
'''


class Refused(Exception):
    pass


def read_stdin(fd, seconds, limit):
    """Reads `fd` to its end within `seconds`, refusing it as soon as it passes `limit` bytes."""
    deadline = time.monotonic() + seconds
    data = b''
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise Refused('stdin did not end within %g seconds' % seconds)
        if not select.select([fd], [], [], remaining)[0]:
            continue
        chunk = os.read(fd, limit + 1 - len(data))
        if not chunk:
            return data
        data += chunk
        if len(data) > limit:
            raise Refused('stdin holds more than one key line of at most 4096 characters')


def main(argv=None, stdin=0):
    """Validates the arguments and stdin, then installs the key over SSH; returns the exit status."""
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 3 or argv[0] != 'install' or not argv[1] or argv[1].startswith('-'):
        print(USAGE, file=sys.stderr)
        return 2
    _, droplet, key_id = argv
    if not ID.fullmatch(key_id):
        print("near-key.py: the id must be letters, digits, '.', '_' or '-'", file=sys.stderr)
        return 2
    try:
        line = read_stdin(stdin, STDIN_SECONDS, MAX_STDIN)
        if not line.endswith(b'\n'):
            raise Refused('stdin ended without a newline, so the key may be incomplete')
        if not KEY_LINE.fullmatch(line):
            raise Refused('stdin must hold one line of letters, digits and ._~+/=- (not shown)')
    except Refused as refusal:
        print('near-key.py: %s; nothing was installed' % refusal, file=sys.stderr)
        return 2
    command = SSH + [droplet, 'python3 -c %s %s' % (shlex.quote(REMOTE), key_id)]
    try:
        return subprocess.run(command, input=line, timeout=REMOTE_SECONDS).returncode
    except subprocess.TimeoutExpired:
        print('near-key.py: timed out after %g seconds; run it again, which reports whether the key '
              'is installed' % REMOTE_SECONDS, file=sys.stderr)
        return 124


if __name__ == '__main__':
    sys.exit(main())
