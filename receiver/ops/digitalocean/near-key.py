#!/usr/bin/env python3
"""Installs a NEAR Intents explorer partner key on the receiver Droplet as /etc/receiver-pir/near-<id>.env.

Run it on the coordinator under the production lock, with the key in
NEAR_INTENTS_EXPLORER (as `infisical run` sets it) and the deploy inventory
named by WALLET_PIR_DEPLOY_INVENTORY, whose pinned SSH reaches the Droplet:

    flock -n /run/lock/wallet-pir-production.lock near-key.py <id>

The key travels only on SSH's stdin, never in an argument or in output. A key
file is written whole under a temporary name and published without replacing
an existing name, so an installed id never changes; a retry uses a new id.
It only stages the file: a locked deploy that names the id activates it.
"""
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'ops/lib'))
from wallet_pir_ops.deploy import descriptors  # noqa: E402
from wallet_pir_ops.deploy.remote import SSHExecutor  # noqa: E402

KEY = re.compile(r'[A-Za-z0-9._~+/=-]{1,4096}')
REMOTE_SECONDS = 60

# Runs on the Droplet as root with the id as its argument and the key file's
# content on stdin. The temporary name is never near-*.env, so one an
# interrupted run leaves is never read as a key.
REMOTE = r'''
import os, sys
directory = "/etc/receiver-pir"
final = os.path.join(directory, "near-%s.env" % sys.argv[1])
temp = os.path.join(directory, ".near-%s.%d.tmp" % (sys.argv[1], os.getpid()))
data = sys.stdin.buffer.read()
if not data.endswith(b"\n"):
    sys.exit("refused: the key file content arrived incomplete")
os.makedirs(directory, mode=0o700, exist_ok=True)
with open(os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "wb") as handle:
    handle.write(data)
    handle.flush()
    os.fsync(handle.fileno())
try:
    os.link(temp, final)
except FileExistsError:
    sys.exit("refused: %s exists; key files are never replaced, so install under a new id" % final)
finally:
    os.unlink(temp)
fd = os.open(directory, os.O_RDONLY)
os.fsync(fd)
os.close(fd)
print("installed " + final)
'''


def main(argv=None):
    """Validates the id and the key, then installs the key file over pinned SSH; returns the exit status."""
    argv = sys.argv[1:] if argv is None else argv
    key = os.environ.get('NEAR_INTENTS_EXPLORER', '')
    inventory_path = os.environ.get('WALLET_PIR_DEPLOY_INVENTORY')
    if len(argv) != 1 or not descriptors.TEMPLATE_VALUE.fullmatch(argv[0]) or not inventory_path:
        print('usage: near-key.py <id>, with WALLET_PIR_DEPLOY_INVENTORY set; the id is a template value '
              '(letters, digits, ".", "_" or "-")', file=sys.stderr)
        return 2
    if not KEY.fullmatch(key):
        print('near-key.py: NEAR_INTENTS_EXPLORER must be one key of letters, digits and ._~+/=- (not shown)',
              file=sys.stderr)
        return 2
    inventory = descriptors.load_inventory(inventory_path)
    service = descriptors.load_descriptors(ROOT / 'enhance/ops/deploy/deploy.toml')['receiver']
    (server,) = descriptors.targets(service, inventory)
    prefix = ['sudo', '-n', '--'] if inventory.hosts[server.host].get('sudo') else []
    # Stages a key file no unit names yet, under the lock; a locked deploy activates it (see the docstring).
    command = SSHExecutor(inventory).raw_transport(server.host) + [
        shlex.join([*prefix, 'python3', '-c', REMOTE, argv[0]])]
    try:
        return subprocess.run(command, input=b'NEAR_INTENTS_EXPLORER=%s\n' % key.encode(),
                              timeout=REMOTE_SECONDS).returncode
    except subprocess.TimeoutExpired:
        print('near-key.py: timed out after %d seconds; the key may be installed, so retry with a new id'
              % REMOTE_SECONDS, file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
