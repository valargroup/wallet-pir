#!/usr/bin/env python3
"""Replaces the receiver Droplet's Caddyfile, verifies it, and restores the predecessor on failure.

Run it on the coordinator under the production lock, with the deploy inventory
named by WALLET_PIR_DEPLOY_INVENTORY:

    flock -n /run/lock/wallet-pir-production.lock caddy.py apply <candidate Caddyfile>

It follows Transparent's router workflow (`fleet_activate_router` and
`rollback_fleet` in transparent/ops/scripts/deploy-transparent-shard.sh): keep
the predecessor, validate, install, reload, verify, and restore on failure.
Verification is the inventory's `exact_check` probe from the running release,
without `--await-feed-reads` since nothing restarts, run on the Droplet, plus
`/v1/receiver/health` and `/metrics` answering exactly 404 at the public origin,
checked from the coordinator as an outside client. The live configuration must
pass the same verification before anything changes. The predecessor is kept as
/etc/caddy/Caddyfile.before-<UTC time>, also after a restore. A live file that
already holds the candidate, as an apply interrupted before its reload leaves
it, is validated, reloaded and verified without a new backup or a restore.

Exit status: 0 applied and verified (or already on disk, then reloaded and
verified), 2 usage, 3 refused before any change, 4 candidate invalid (nothing
replaced), 5 candidate rejected and the predecessor restored and verified, 6
restoration failed, 7 the candidate already on disk failed its reload or
verification (nothing restored), 75 outcome unknown. SSH's deadline stops only
the local client: after a timeout or a lost connection the remote step may
still replace the file or reload Caddy, so 75 means check the Droplet before
anything else.
"""
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from urllib.parse import urlsplit

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'ops/lib'))
from wallet_pir_ops.deploy import descriptors  # noqa: E402
from wallet_pir_ops.deploy.remote import SSHExecutor  # noqa: E402

DESCRIPTORS = ROOT / 'enhance/ops/deploy/deploy.toml'
SERVICE = 'receiver'
LIVE = '/etc/caddy/Caddyfile'
BACKUP = LIVE + '.before-%s'
DENIED = ('/v1/receiver/health', '/metrics')
FEED_WAIT = '--await-feed-reads'
MAX_CANDIDATE = 1 << 20
# Per-step deadlines, each for its whole SSH call; restoring gets its own.
RESOLVE_SECONDS = 60
PROBE_SECONDS = 180
APPLY_SECONDS = 180
RESTORE_SECONDS = 180
CURL_SECONDS = 15
KEEPALIVE = ['-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3']
APPLIED, USAGE, REFUSED, INVALID, RESTORED, RESTORE_FAILED, RECONCILE_FAILED, UNKNOWN = 0, 2, 3, 4, 5, 6, 7, 75

# Runs on the Droplet as root with one JSON request as its argument (the
# candidate on stdin for `apply`) and prints one JSON reply. Temporary names are
# hidden siblings of the live file, so validation resolves relative imports as
# the live file would and Caddy never reads them.
REMOTE = r'''
import hashlib, json, os, secrets, shutil, subprocess, sys

LIVE = "/etc/caddy/Caddyfile"
request = json.loads(sys.argv[1])


def reply(status, **fields):
    """Prints the reply and exits; the runner reads only this line."""
    print(json.dumps(dict(fields, status=status)))
    sys.exit(0)


def run(argv, seconds):
    """Runs `argv` without stdin; returns its status (124 at the deadline) and the tail of its output.

    Never raises, so a step after the live file is replaced always replies.
    """
    try:
        result = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=seconds)
    except subprocess.TimeoutExpired:
        return 124, "timed out after %s seconds" % seconds
    except OSError as error:
        return 127, str(error)
    return result.returncode, (result.stdout + result.stderr)[-2000:]


def digest(path):
    """The sha256 of the file at `path`, or None if it cannot be read."""
    try:
        with open(path, "rb") as handle:
            return hashlib.sha256(handle.read()).hexdigest()
    except OSError:
        return None


def sibling(kind):
    """A new temporary name beside the live file."""
    return os.path.join(os.path.dirname(LIVE), ".Caddyfile.%s-%s" % (kind, secrets.token_hex(8)))


def reload(status, failed="reload_failed", **fields):
    """Reloads Caddy and replies `status`, or `failed`, with `fields`."""
    code, output = run(["systemctl", "reload", "caddy"], 90)
    reply(failed if code else status, output=output, **fields)


def latest_backup():
    """The name of the newest kept backup beside the live file (UTC names sort by time), or None."""
    directory, name = os.path.split(LIVE)
    backups = sorted(entry for entry in os.listdir(directory) if entry.startswith(name + ".before-"))
    return backups[-1] if backups else None


def resolve():
    """Replies the running server's digest once its release holds the binary."""
    unit = request["unit"]
    code, output = run(["systemctl", "show", "-p", "ActiveState", "-p", "MainPID", unit], 30)
    properties = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
    pid = properties.get("MainPID", "")
    if code or properties.get("ActiveState") != "active" or not pid.isdigit() or pid == "0":
        reply("refused", reason=unit + " is not active with a main process")
    running = digest("/proc/%s/exe" % pid)
    release = os.path.join(request["root"], "releases", str(running))
    binary = os.path.join(release, request["binary"])
    if running is None or digest(binary) != running:
        reply("refused", reason="%s does not hash to the running executable's %s" % (binary, running))
    if not os.path.isfile(LIVE):
        reply("refused", reason=LIVE + " is not a file")
    reply("ok", digest=running)


def apply():
    """Validates the candidate on stdin, keeps the live file as the backup, installs the candidate and reloads.

    A candidate the live file already holds (say after a lost reply between
    rename and reload) is validated and reloaded but not backed up again.
    """
    candidate = sys.stdin.buffer.read()
    with open(LIVE, "rb") as handle:
        live = handle.read()
    same = candidate == live
    temp = sibling("candidate")
    try:
        with os.fdopen(os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644), "wb") as handle:
            handle.write(candidate)
        code, output = run(["caddy", "validate", "--adapter", "caddyfile", "--config", temp], 60)
        if code:
            reply("invalid", output=output, same=same)
        if same:
            os.unlink(temp)
            reload("reconciled", "reconcile_failed", latest_backup=latest_backup())
        try:
            descriptor = os.open(request["backup"], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
        except FileExistsError:
            reply("refused", reason=request["backup"] + " already exists")
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(live)
        shutil.copystat(LIVE, request["backup"])
        os.rename(temp, LIVE)
    finally:
        if os.path.lexists(temp):
            os.unlink(temp)
    reload("applied")


def restore():
    """Puts a copy of the backup over the live file, keeping the backup, and reloads."""
    temp = sibling("restore")
    try:
        shutil.copy2(request["backup"], temp)
        os.rename(temp, LIVE)
    finally:
        if os.path.lexists(temp):
            os.unlink(temp)
    reload("restored")


try:
    if request["op"] == "probe":
        code, output = run(request["argv"], request["timeout"])
        reply("ran", returncode=code, output=output)
    {"resolve": resolve, "apply": apply, "restore": restore}[request["op"]]()
except Exception as error:
    reply("error", reason="%s: %s" % (type(error).__name__, error))
'''


class Refused(Exception):
    """Nothing was changed."""


class Unknown(Exception):
    """A remote step timed out, lost its connection or gave no reply."""


def say(message, stream=None):
    """Prints one line of the outcome, prefixed with the program's name."""
    print('caddy.py: ' + message, file=stream or sys.stdout, flush=True)


def without_feed_wait(argv):
    """`argv` without `--await-feed-reads` and its value: a Caddy change restarts nothing."""
    result, rest = [], list(argv)
    while rest:
        argument = rest.pop(0)
        if argument == FEED_WAIT:
            if not rest:
                raise Refused('%s in the exact check has no value' % FEED_WAIT)
            rest.pop(0)
        elif not argument.startswith(FEED_WAIT + '='):
            result.append(argument)
    return result


class Droplet:
    """The receiver host from the deploy inventory, its exact check and the public origin."""

    def __init__(self, inventory_path, candidate):
        inventory = descriptors.load_inventory(inventory_path)
        self.service = descriptors.load_descriptors(DESCRIPTORS)[SERVICE]
        servers = descriptors.targets(self.service, inventory)
        if len(servers) != 1:
            raise Refused('the inventory must list one receiver server, not %d' % len(servers))
        self.host = servers[0].host
        self.unit = servers[0].unit
        check = descriptors.exact_check(self.service, inventory)
        if check is None or check['host'] != self.host:
            raise Refused('services.receiver.exact_check must run on the receiver host %s' % self.host)
        self.check = without_feed_wait(check['argv'])
        origins = [b for a, b in zip(self.check, self.check[1:]) if a == '--origin']
        if len(origins) != 1 or urlsplit(origins[0]).scheme != 'https' or not urlsplit(origins[0]).hostname:
            raise Refused('the exact check must name one https --origin')
        self.origin = origins[0].rstrip('/')
        site = urlsplit(self.origin).hostname
        if not re.search(r'(?m)^%s\s*\{' % re.escape(site), candidate.decode(errors='replace')):
            raise Refused('the candidate has no site block for %s' % site)
        raw = SSHExecutor(inventory).raw_transport(self.host)
        self.destination = raw[-1]
        self.transport = raw[:1] + KEEPALIVE + raw[1:]
        self.prefix = ['sudo', '-n', '--'] if inventory.hosts[self.host].get('sudo') else []

    def call(self, request, seconds, stdin=b''):
        """Runs one remote operation and returns its reply; raises Unknown without one."""
        command = shlex.join([*self.prefix, 'python3', '-c', REMOTE, json.dumps(request)])
        try:
            result = subprocess.run(self.transport + [command], input=stdin, capture_output=True, timeout=seconds)
        except subprocess.TimeoutExpired:
            raise Unknown('%s timed out after %g seconds' % (request['op'], seconds)) from None
        lines = result.stdout.decode(errors='replace').strip().splitlines()
        try:
            return json.loads(lines[-1])
        except (IndexError, ValueError):
            raise Unknown('%s gave no reply (ssh exit %d): %s' % (
                request['op'], result.returncode, result.stderr.decode(errors='replace').strip()[-500:])) from None

    def restore_command(self, backup):
        """The manual restore, for output and the README."""
        temp = '/etc/caddy/.Caddyfile.restore'
        return 'ssh %s %s' % (self.destination, shlex.quote(
            'cp -p %s %s && mv %s %s && systemctl reload caddy' % (backup, temp, temp, LIVE)))


def public_status(url):
    """The status `url` answers with, without following redirects ('000' for no answer)."""
    try:
        result = subprocess.run(['curl', '--silent', '--output', '/dev/null', '--write-out', '%{http_code}',
                                 '--proto', '=https', '--max-time', str(CURL_SECONDS), url],
                                stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=CURL_SECONDS + 10)
    except subprocess.TimeoutExpired:
        return '000'
    return result.stdout.strip() or '000'


def verify(droplet, probe):
    """Failures of the probe on the Droplet and of the public denials; empty when all pass."""
    failures = []
    try:
        reply = droplet.call({'op': 'probe', 'argv': probe, 'timeout': PROBE_SECONDS}, PROBE_SECONDS + 30)
        if reply.get('status') != 'ran' or reply.get('returncode') != 0:
            failures.append('probe failed: %s' % (reply.get('output') or reply.get('reason') or reply)[-1000:])
    except Unknown as error:
        failures.append('probe: %s' % error)
    for path in DENIED:
        status = public_status(droplet.origin + path)
        if status != '404':
            failures.append('%s%s answered %s, not 404' % (droplet.origin, path, status))
    return failures


def restore(droplet, backup, probe):
    """Puts the backup back and verifies it; returns the exit status."""
    say('restoring %s on %s from %s' % (LIVE, droplet.host, backup), sys.stderr)
    manual = 'restore by hand under the lock: ' + droplet.restore_command(backup)
    try:
        reply = droplet.call({'op': 'restore', 'backup': backup}, RESTORE_SECONDS)
    except Unknown as error:
        say('restoration outcome unknown on %s: %s; the backup %s is kept; %s'
            % (droplet.host, error, backup, manual), sys.stderr)
        return UNKNOWN
    if reply.get('status') != 'restored':
        say('restoration failed on %s: %s; the backup %s is kept; %s'
            % (droplet.host, reply.get('output') or reply.get('reason'), backup, manual), sys.stderr)
        return RESTORE_FAILED
    failures = verify(droplet, probe)
    if failures:
        say('restored %s, but it fails verification: %s; the backup %s is kept'
            % (backup, '; '.join(failures), backup), sys.stderr)
        return RESTORE_FAILED
    say('restored and verified the predecessor on %s; the candidate is not live; backup %s kept'
        % (droplet.host, backup), sys.stderr)
    return RESTORED


def reconcile(droplet, reply, probe):
    """Verifies the reload of a live file that already held the candidate; returns the exit status.

    Nothing is restored on failure: no backup was made, and the latest one may
    not be what Caddy ran before.
    """
    failures = (['reload failed: %s' % reply.get('output')] if reply['status'] == 'reconcile_failed'
                else verify(droplet, probe))
    if not failures:
        say('%s already held the candidate; reloaded and verified on %s; no backup made' % (LIVE, droplet.host))
        return APPLIED
    latest = reply.get('latest_backup') and os.path.join(os.path.dirname(LIVE), reply['latest_backup'])
    manual = ('the latest backup is %s; if it is the configuration to return to, restore it by hand under the '
              'lock: %s' % (latest, droplet.restore_command(latest)) if latest else 'no backup exists to restore')
    say('%s already held the candidate, and reloading or verifying it failed: %s; nothing restored, no backup made; %s'
        % (LIVE, '; '.join(failures), manual), sys.stderr)
    return RECONCILE_FAILED


def apply(inventory_path, candidate_path):
    """Applies one candidate Caddyfile; returns the exit status."""
    try:
        with open(candidate_path, 'rb') as handle:
            candidate = handle.read(MAX_CANDIDATE + 1)
        if len(candidate) > MAX_CANDIDATE:
            raise Refused('the candidate exceeds %d bytes' % MAX_CANDIDATE)
        droplet = Droplet(inventory_path, candidate)
    except (Refused, OSError, ValueError, KeyError) as error:
        say('refused before any change: %s' % error, sys.stderr)
        return REFUSED
    say('host %s (%s)' % (droplet.host, droplet.destination))
    try:
        reply = droplet.call({'op': 'resolve', 'unit': droplet.unit, 'root': droplet.service.root,
                              'binary': droplet.service.binary}, RESOLVE_SECONDS)
    except Unknown as error:
        reply = {'reason': str(error)}
    if reply.get('status') != 'ok':
        say('refused before any change: no running release to verify with: %s' % reply.get('reason'), sys.stderr)
        return REFUSED
    release = droplet.service.release_dir(reply['digest'])
    probe = [argument.replace('{release_dir}', release) for argument in droplet.check]
    if any(re.search(r'\{[a-z_]+\}', argument) for argument in probe):
        say('refused before any change: the exact check has a placeholder besides {release_dir}', sys.stderr)
        return REFUSED
    say('running release %s' % release)
    failures = verify(droplet, probe)
    if failures:
        say('refused before any change: the live configuration already fails verification: %s'
            % '; '.join(failures), sys.stderr)
        return REFUSED
    backup = BACKUP % time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    say('baseline verified; a replaced file is kept as %s' % backup)
    try:
        reply = droplet.call({'op': 'apply', 'backup': backup}, APPLY_SECONDS, stdin=candidate)
    except Unknown as error:
        say('outcome unknown on %s: %s; the remote step may still replace %s and reload Caddy; check it '
            'under the lock, and if %s exists, restore by hand: %s'
            % (droplet.host, error, LIVE, backup, droplet.restore_command(backup)), sys.stderr)
        return UNKNOWN
    status = reply.get('status')
    if status in ('reconciled', 'reconcile_failed'):
        return reconcile(droplet, reply, probe)
    if status == 'invalid':
        say('candidate invalid; %s not replaced%s, Caddy not reloaded:\n%s'
            % (LIVE, ' (it already holds these bytes)' if reply.get('same') else '', reply.get('output')), sys.stderr)
        return INVALID
    if status != 'applied' and status != 'reload_failed':
        say('refused before any change: %s' % reply.get('reason'), sys.stderr)
        return REFUSED
    failures = (['reload failed: %s' % reply.get('output')] if status == 'reload_failed'
                else verify(droplet, probe))
    if not failures:
        say('applied and verified on %s; backup %s' % (droplet.host, backup))
        return APPLIED
    say('candidate failed after replacing %s: %s' % (LIVE, '; '.join(failures)), sys.stderr)
    return restore(droplet, backup, probe)


def main(argv=None):
    """Parses the command line; returns the exit status."""
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2 or argv[0] != 'apply':
        print('usage: caddy.py apply <candidate Caddyfile>, under the production lock', file=sys.stderr)
        return USAGE
    inventory = os.environ.get('WALLET_PIR_DEPLOY_INVENTORY')
    if not inventory:
        print('caddy.py: set WALLET_PIR_DEPLOY_INVENTORY to the deploy inventory', file=sys.stderr)
        return USAGE
    return apply(inventory, argv[1])


if __name__ == '__main__':
    sys.exit(main())
