#!/usr/bin/env python3
"""Replaces the receiver Droplet's Caddyfile, verifies it, and restores the predecessor on failure.

The deploy tool writes only unit files, so this is a locked helper by design, as
Transparent's router Caddy changes are.

Run it on the coordinator under the production lock, with the deploy inventory
named by WALLET_PIR_DEPLOY_INVENTORY, whose pinned SSH reaches the Droplet:

    flock -n /run/lock/wallet-pir-production.lock caddy.py apply <candidate Caddyfile>

It validates the candidate beside the live file, keeps the live file as
/etc/caddy/Caddyfile.before-<UTC time>, renames the candidate over it, reloads
Caddy and verifies: the inventory's exact check from the running release,
without `--await-feed-reads` since nothing restarts, and `/v1/receiver/health`
and `/metrics` answering exactly 404 at the public origin. On failure it puts
the backup back, reloads and verifies again. Exit status: 0 applied and
verified, 1 not applied (see the message), 75 outcome unknown: an SSH step
timed out or lost its connection, and the remote step may still have run, or
restoring the predecessor failed, so Caddy may still run the candidate.
"""
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'ops/lib'))
from wallet_pir_ops.deploy import descriptors  # noqa: E402
from wallet_pir_ops.deploy.remote import SSHExecutor  # noqa: E402

LIVE = '/etc/caddy/Caddyfile'
DENIED = ('/v1/receiver/health', '/metrics')
STEP_SECONDS = 180
UNKNOWN = 75

# Runs on the Droplet as root with one JSON request as its argument (the
# candidate on stdin for `apply`) and prints one JSON reply.
REMOTE = r'''
import json, os, shutil, subprocess, sys
LIVE = "/etc/caddy/Caddyfile"
request = json.loads(sys.argv[1])
temp = os.path.join(os.path.dirname(LIVE), ".Caddyfile.%d.tmp" % os.getpid())


def run(argv, seconds):
    """Runs `argv` without stdin; returns its status (1 if it cannot run or times out) and output tail."""
    try:
        result = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=seconds)
    except (OSError, subprocess.TimeoutExpired) as error:
        return 1, str(error)
    return result.returncode, (result.stdout + result.stderr)[-2000:]


def reply(ok, output=""):
    """Prints the one reply line and exits."""
    print(json.dumps({"ok": ok, "output": output}))
    sys.exit(0)


if request["op"] == "probe":
    show = run(["systemctl", "show", "-p", "MainPID", request["unit"]], 30)[1]
    pid = show.strip().partition("=")[2]
    if not pid.isdigit() or pid == "0":
        reply(False, request["unit"] + " has no main process")
    release = os.path.dirname(os.path.realpath("/proc/%s/exe" % pid))
    code, output = run([word.replace("{release_dir}", release) for word in request["argv"]], 150)
    reply(code == 0, output)
if request["op"] == "apply":
    with open(temp, "wb") as handle:
        handle.write(sys.stdin.buffer.read())
    code, output = run(["caddy", "validate", "--adapter", "caddyfile", "--config", temp], 60)
    if code:
        os.unlink(temp)
        print(json.dumps({"invalid": True, "output": output}))
        sys.exit(0)
    os.link(LIVE, request["backup"])
else:
    shutil.copy2(request["backup"], temp)
os.rename(temp, LIVE)
code, output = run(["systemctl", "reload", "caddy"], 90)
reply(code == 0, "reload failed: " + output if code else "")
'''


class Unknown(Exception):
    """A remote step timed out, lost its connection or gave no reply, or a restoration failed."""


class Droplet:
    """The receiver host, its pinned transport and its verification, from the deploy inventory."""

    def __init__(self, inventory_path):
        inventory = descriptors.load_inventory(inventory_path)
        service = descriptors.load_descriptors(ROOT / 'enhance/ops/deploy/deploy.toml')['receiver']
        (server,) = descriptors.targets(service, inventory)
        check = descriptors.exact_check(service, inventory)
        if check is None or check['host'] != server.host:
            raise ValueError('services.receiver.exact_check must run on the receiver host %s' % server.host)
        argv = list(check['argv'])
        if '--await-feed-reads' in argv:
            del argv[argv.index('--await-feed-reads'):argv.index('--await-feed-reads') + 2]
        self.host, self.unit, self.probe = server.host, server.unit, argv
        self.origin = argv[argv.index('--origin') + 1].rstrip('/')
        prefix = ['sudo', '-n', '--'] if inventory.hosts[server.host].get('sudo') else []
        self.transport = SSHExecutor(inventory).raw_transport(server.host)
        self.command = lambda request: shlex.join([*prefix, 'python3', '-c', REMOTE, json.dumps(request)])

    def call(self, request, stdin=b''):
        """Runs one remote operation and returns its reply; raises Unknown without one."""
        try:
            result = subprocess.run(self.transport + [self.command(request)], input=stdin, capture_output=True,
                                    timeout=STEP_SECONDS)
            return json.loads(result.stdout.decode(errors='replace').strip().splitlines()[-1])
        except subprocess.TimeoutExpired:
            raise Unknown('%s timed out after %g seconds' % (request['op'], STEP_SECONDS)) from None
        except (IndexError, ValueError):
            raise Unknown('%s gave no reply (ssh exit %d): %s' % (request['op'], result.returncode,
                          result.stderr.decode(errors='replace').strip()[-500:])) from None

    def verify(self):
        """Failures of the exact probe on the Droplet and of the public 404s; empty when all pass."""
        try:
            reply = self.call({'op': 'probe', 'unit': self.unit, 'argv': self.probe})
            failures = [] if reply['ok'] else ['probe failed: ' + reply['output'][-1000:]]
        except Unknown as error:
            failures = ['probe: %s' % error]
        for path in DENIED:
            status = subprocess.run(['curl', '--silent', '--output', '/dev/null', '--write-out', '%{http_code}',
                                     '--proto', '=https', '--max-time', '15', self.origin + path],
                                    stdin=subprocess.DEVNULL, capture_output=True, text=True).stdout.strip()
            if status != '404':
                failures.append('%s%s answered %s, not 404' % (self.origin, path, status or 'nothing'))
        return failures


def apply(droplet, candidate):
    """Applies `candidate`, restoring the predecessor if it fails; returns the exit status."""
    backup = '%s.before-%s' % (LIVE, time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()))
    reply = droplet.call({'op': 'apply', 'backup': backup}, candidate)
    if reply.get('invalid'):
        print('caddy.py: candidate invalid; nothing replaced:\n' + reply['output'], file=sys.stderr)
        return 1
    failures = [reply['output']] if not reply['ok'] else droplet.verify()
    if not failures:
        print('caddy.py: applied and verified on %s; predecessor kept as %s' % (droplet.host, backup))
        return 0
    print('caddy.py: candidate failed: %s; restoring %s' % ('; '.join(failures), backup), file=sys.stderr)
    reply = droplet.call({'op': 'restore', 'backup': backup})
    failures = [reply['output']] if not reply['ok'] else droplet.verify()
    if failures:
        raise Unknown('restoring %s failed: %s' % (backup, '; '.join(failures)))
    print('caddy.py: restored and verified the predecessor; the candidate is not live', file=sys.stderr)
    return 1


def main(argv=None):
    """Parses the command line; returns the exit status."""
    argv = sys.argv[1:] if argv is None else argv
    inventory = os.environ.get('WALLET_PIR_DEPLOY_INVENTORY')
    if len(argv) != 2 or argv[0] != 'apply' or not inventory:
        print('usage: caddy.py apply <candidate Caddyfile>, with WALLET_PIR_DEPLOY_INVENTORY set, under the '
              'production lock', file=sys.stderr)
        return 1
    try:
        droplet = Droplet(inventory)
        candidate = Path(argv[1]).read_bytes()
    except (OSError, ValueError, KeyError) as error:
        print('caddy.py: nothing changed: %s' % error, file=sys.stderr)
        return 1
    try:
        return apply(droplet, candidate)
    except Unknown as error:
        print('caddy.py: outcome unknown on %s: %s; under the lock, inspect %s and the newest %s.before-* '
              '(journalctl -u caddy shows whether a reload applied) before anything else'
              % (droplet.host, error, LIVE, LIVE), file=sys.stderr)
        return UNKNOWN


if __name__ == '__main__':
    sys.exit(main())
