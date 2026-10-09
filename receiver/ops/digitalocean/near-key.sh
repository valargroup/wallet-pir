#!/bin/sh
# Installs a NEAR Intents explorer partner key on the receiver Droplet as
# /etc/receiver-pir/near-<id>.env, the file the unit names through the deploy
# inventory's NEAR_KEY (see README.md). Run it on the coordinator under the
# production lock, with the key on stdin:
#
#   ... | flock -n /run/lock/wallet-pir-production.lock near-key.sh install <droplet> <id>
#
# The key never appears in an argument (printf is a shell builtin), in output or
# in a file on the coordinator. Key files are never edited or deleted: an id
# already installed with the same key is a no-op and one with another key is
# refused. All of them stay, a few bytes each, because rolling back through
# several deploys needs the key files their units name.
set -eu

usage() {
  echo 'usage: near-key.sh install <droplet> <id>, with the partner key on stdin' >&2
  exit 2
}

[ "$#" -eq 3 ] && [ "$1" = install ] || usage
droplet=$2
id=$3
case $droplet in
  '' | -*) usage ;;
esac
case $id in
  '' | *[!A-Za-z0-9._-]*)
    echo "near-key.sh: the id must be letters, digits, '.', '_' or '-'" >&2
    exit 2
    ;;
esac
# One line; command substitution drops a trailing newline.
key=$(cat)
case $key in
  '' | *[!A-Za-z0-9._~+/=-]*)
    echo 'near-key.sh: stdin must hold one key of letters, digits and ._~+/=- (not shown)' >&2
    exit 2
    ;;
esac
if [ "${#key}" -gt 4096 ]; then
  echo 'near-key.sh: the key is longer than 4096 characters' >&2
  exit 2
fi

# Runs on the Droplet as root, with the id as its argument and the key line on
# stdin. A temporary file is never named near-*.env, so one an interrupted run
# leaves is never read as a key. Single quotes would end the remote command.
program=$(cat <<'EOF'
import os, re, secrets, stat, sys

directory = "/etc/receiver-pir"
key_id = sys.argv[1]
data = sys.stdin.buffer.read()
if not re.fullmatch(r"[A-Za-z0-9._-]+", key_id) or not re.fullmatch(rb"[A-Za-z0-9._~+/=-]+\n", data):
    sys.exit("refused: malformed id or key")
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
EOF
)
case $program in
  *\'*) echo 'near-key.sh: the remote program must not contain single quotes' >&2; exit 70 ;;
esac

status=0
printf '%s\n' "$key" | timeout 60 ssh -o BatchMode=yes -o ConnectTimeout=10 \
  -o ServerAliveInterval=10 -o ServerAliveCountMax=3 "$droplet" \
  "python3 -c '$program' $id" || status=$?
if [ "$status" -eq 124 ]; then
  echo 'near-key.sh: timed out; run it again, which reports whether the key is installed' >&2
fi
exit "$status"
