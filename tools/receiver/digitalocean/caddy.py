#!/usr/bin/env python3
"""Replaces the receiver Droplet's Caddyfile, verifies it, and restores the predecessor on failure.

The deploy tool writes only unit files, so this is a locked helper by design, as
Transparent's router Caddy changes are.

Run it on the coordinator under the production lock, with the deploy inventory
named by WALLET_PIR_DEPLOY_INVENTORY, whose pinned SSH reaches the Droplet:

    flock -n /run/lock/wallet-pir-production.lock caddy.py apply <candidate Caddyfile>

It validates the candidate beside the live file, keeps the live file as
/etc/caddy/Caddyfile.before-<UTC time>-<random>, renames the candidate over it, reloads
Caddy and verifies: the inventory's exact check from the running release,
and `/v1/receiver/health`
and `/metrics` answering exactly 404 at the public origin. On failure it puts
the backup back, reloads and verifies again. Exit status: 0 applied and
verified, 1 not applied (see the message), 75 outcome unknown: an SSH step
timed out, lost its connection or failed to sync a write to disk, and the
remote step may still have run, or
restoring the predecessor failed, so Caddy may still run the candidate.

A release that changes the wallet routes changes the binary's probe with them,
so neither alone verifies. `cutover` changes both under one hold of the
production lock, which it takes itself (do not wrap it in flock):

    caddy.py cutover <candidate Caddyfile> deploy receiver --archive F --sha S ...
    caddy.py cutover <predecessor Caddyfile> rollback receiver [--transaction ID]

It applies the candidate, checks the operator routes stay off the public edge,
then runs that deploy tool command in this process under the same lock. A
deploy's exact check already probes the new binary through the new routes. The
inventory's probe then runs once more against whatever release is running. If
the command fails and the receiver still runs the binary it ran before, the
predecessor Caddyfile comes back and is verified with it. A command that fails
with another binary running, as when a deploy commits and then fails to refresh
its baseline, or that succeeds but whose probe then fails, is an unknown
outcome, since both changed; the candidate routes stay.
"""
import contextlib
import functools
import json
import secrets
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops.deploy import descriptors  # noqa: E402
from wallet_pir_ops.deploy.remote import LockHeld, SSHExecutor, production_lock  # noqa: E402

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


def sync(path):
    """Fsyncs the file or directory at `path`, so a write or rename in it survives a crash."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


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
if request["op"] == "release":
    show = run(["systemctl", "show", "-p", "MainPID", request["unit"]], 30)[1]
    pid = show.strip().partition("=")[2]
    if not pid.isdigit() or pid == "0":
        reply(False, request["unit"] + " has no main process")
    reply(True, os.path.realpath("/proc/%s/exe" % pid))
if request["op"] == "apply":
    with open(temp, "wb") as handle:
        handle.write(sys.stdin.buffer.read())
        handle.flush()
        os.fsync(handle.fileno())
    code, output = run(["caddy", "validate", "--adapter", "caddyfile", "--config", temp], 60)
    if code:
        os.unlink(temp)
        print(json.dumps({"invalid": True, "output": output}))
        sys.exit(0)
    os.link(LIVE, request["backup"])
    sync(os.path.dirname(LIVE))
else:
    shutil.copy2(request["backup"], temp)
    sync(temp)
os.rename(temp, LIVE)
# A failed sync raises, so the caller gets no reply and reports an unknown outcome.
sync(os.path.dirname(LIVE))
code, output = run(["systemctl", "reload", "caddy"], 90)
reply(code == 0, "reload failed: " + output if code else "")
'''


class Unknown(Exception):
    """A remote step timed out, lost its connection or gave no reply, or a restoration failed."""


class Droplet:
    """The receiver host, its pinned transport and its verification, from the deploy inventory."""

    def __init__(self, inventory_path):
        inventory = descriptors.load_inventory(inventory_path)
        service = descriptors.load_descriptors(ROOT / 'tools/deploy/services.toml')['receiver']
        (server,) = descriptors.targets(service, inventory)
        check = descriptors.exact_check(service, inventory)
        if check is None or check['host'] != server.host:
            raise ValueError('services.receiver.exact_check must run on the receiver host %s' % server.host)
        argv = list(check['argv'])
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

    def release(self):
        """The binary the receiver's main process runs, or None if it cannot be read."""
        try:
            reply = self.call({'op': 'release', 'unit': self.unit})
        except Unknown:
            return None
        return reply['output'] if reply['ok'] else None

    def verify(self):
        """Failures of the exact probe on the Droplet and of the public 404s; empty when all pass."""
        try:
            reply = self.call({'op': 'probe', 'unit': self.unit, 'argv': self.probe})
            failures = [] if reply['ok'] else ['probe failed: ' + reply['output'][-1000:]]
        except Unknown as error:
            failures = ['probe: %s' % error]
        return failures + self.exposed()

    def exposed(self):
        """Operator routes the public origin answers with anything but 404; empty when none."""
        failures = []
        for path in DENIED:
            status = subprocess.run(['curl', '--silent', '--output', '/dev/null', '--write-out', '%{http_code}',
                                     '--proto', '=https', '--max-time', '15', self.origin + path],
                                    stdin=subprocess.DEVNULL, capture_output=True, text=True).stdout.strip()
            if status != '404':
                failures.append('%s%s answered %s, not 404' % (self.origin, path, status or 'nothing'))
        return failures


def install(droplet, candidate):
    """Validates `candidate`, keeps the live file as a backup and reloads Caddy on it.

    Returns the backup's path and the reload's failures, or `(None, [])` when the
    candidate is invalid and nothing changed.
    """
    # The random part keeps two runs in one second from colliding on the same backup,
    # which would fail the link before the live file is replaced.
    backup = '%s.before-%s-%s' % (LIVE, time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()), secrets.token_hex(4))
    reply = droplet.call({'op': 'apply', 'backup': backup}, candidate)
    if reply.get('invalid'):
        print('caddy.py: candidate invalid; nothing replaced:\n' + reply['output'], file=sys.stderr)
        return None, []
    return backup, [] if reply['ok'] else [reply['output']]


def restore(droplet, backup, failures):
    """Puts `backup` back after the candidate's `failures` and verifies it; returns 1.

    Raises Unknown if the predecessor does not reload or verify.
    """
    print('caddy.py: candidate failed: %s; restoring %s' % ('; '.join(failures), backup), file=sys.stderr)
    reply = droplet.call({'op': 'restore', 'backup': backup})
    failures = [reply['output']] if not reply['ok'] else droplet.verify()
    if failures:
        raise Unknown('restoring %s failed: %s' % (backup, '; '.join(failures)))
    print('caddy.py: restored and verified the predecessor; the candidate is not live', file=sys.stderr)
    return 1


def apply(droplet, candidate):
    """Applies `candidate`, restoring the predecessor if it fails; returns the exit status."""
    backup, failures = install(droplet, candidate)
    if backup is None:
        return 1
    failures = failures or droplet.verify()
    if not failures:
        print('caddy.py: applied and verified on %s; predecessor kept as %s' % (droplet.host, backup))
        return 0
    return restore(droplet, backup, failures)


def run_deploy(argv, lock, inventory):
    """Runs the deploy tool command `argv` on `inventory` in this process under the held `lock`.

    Returns its status. `inventory` must be the one `lock` and the Caddyfile belong to.
    """
    from wallet_pir_ops.deploy import cli
    return cli.main(['--inventory', inventory, *argv], lock=lambda: contextlib.nullcontext(lock))


def deploy_command(argv, inventory):
    """Parses the deploy tool command `argv` as that tool will, without running it.

    Returns an error message, or None for a receiver deploy or rollback of `inventory`
    with the reviewed service descriptors that keeps its exact check. Help, malformed
    arguments and another inventory are errors, so none can follow an installed
    Caddyfile or deploy under another inventory's lock.
    """
    from wallet_pir_ops.deploy import cli
    try:
        with contextlib.redirect_stdout(sys.stderr):
            args = cli.parser().parse_args(argv)
    except SystemExit:
        return 'the deploy command does not parse'
    if args.command not in ('deploy', 'rollback') or getattr(args, 'service', None) != 'receiver':
        return 'a cutover runs only deploy receiver or rollback receiver'
    # The cutover's joint verification is the deploy's exact check and the probe.
    if getattr(args, 'skip_exact_check', False):
        return 'a cutover needs the exact check'
    try:
        same = os.path.samefile(args.inventory, inventory)
    except (OSError, TypeError):
        same = False
    if not same:
        return 'the deploy command names another inventory than WALLET_PIR_DEPLOY_INVENTORY'
    if Path(args.descriptors).resolve() != (ROOT / 'tools/deploy/services.toml').resolve():
        return 'the deploy command names other service descriptors'
    return None


def still_held(lock):
    """Verifies `lock` once something changed, raising Unknown without it.

    Nothing may be restored without the lock, so the operator reconciles instead.
    """
    try:
        lock.verify()
    except Exception as error:
        raise Unknown('lost the production lock with the candidate installed, so nothing was restored (%s); '
                      'reacquire the lock, check the running release with wallet-pir-deploy.py status receiver, '
                      'then rerun caddy.py cutover with the Caddyfile that release needs' % error) from error


def cutover(droplet, candidate, argv, lock, deploy):
    """Applies `candidate` and runs the receiver deploy command `argv` under `lock`; see the module doc.

    Returns the exit status. Raises Unknown when the outcome is unknown.
    """
    command = next(word for word in argv if word in ('deploy', 'rollback'))
    try:
        lock.verify()
    except Exception as error:
        print('caddy.py: nothing changed: %s' % error, file=sys.stderr)
        return 1
    # The predecessor's routes come back only beside the binary they were installed for.
    running = droplet.release()
    if running is None:
        print('caddy.py: nothing changed: the running receiver binary cannot be read', file=sys.stderr)
        return 1
    backup, failures = install(droplet, candidate)
    if backup is None:
        return 1
    failures = failures or droplet.exposed()
    if not failures:
        still_held(lock)
        try:
            code = deploy(argv, lock)
        except BaseException as error:
            raise Unknown('%s stopped with the candidate live: %r' % (command, error)) from error
        if code == 0:
            failures = droplet.verify()
            if failures:
                raise Unknown('%s succeeded with the candidate live, then the probe failed: %s'
                              % (command, '; '.join(failures)))
            print('caddy.py: cut over on %s with %s; predecessor kept as %s' % (droplet.host, command, backup))
            return 0
        failures = ['%s exited %d' % (command, code)]
        # A deploy can commit and still exit nonzero, as when the baseline refresh after
        # its commit fails, leaving the new binary running behind the candidate routes.
        now = droplet.release()
        if now != running:
            raise Unknown('%s exited %d and the receiver now runs %s, not %s, so the candidate routes stay; '
                          'check wallet-pir-deploy.py status receiver for the transaction outcome'
                          % (command, code, now or 'an unreadable binary', running))
    still_held(lock)
    return restore(droplet, backup, failures)


def main(argv=None):
    """Parses the command line; returns the exit status."""
    argv = sys.argv[1:] if argv is None else argv
    inventory = os.environ.get('WALLET_PIR_DEPLOY_INVENTORY')
    applying = len(argv) == 2 and argv[0] == 'apply'
    cutting = len(argv) >= 3 and argv[0] == 'cutover'
    if not (applying or cutting) or not inventory:
        print('usage: caddy.py apply <candidate Caddyfile> under the production lock, or caddy.py cutover '
              '<candidate Caddyfile> deploy|rollback receiver ARGS..., with WALLET_PIR_DEPLOY_INVENTORY set',
              file=sys.stderr)
        return 1
    refusal = cutting and deploy_command(argv[2:], inventory)
    if refusal:
        print('caddy.py: nothing changed: %s' % refusal, file=sys.stderr)
        return 1
    try:
        droplet = Droplet(inventory)
        candidate = Path(argv[1]).read_bytes()
    except (OSError, ValueError, KeyError) as error:
        print('caddy.py: nothing changed: %s' % error, file=sys.stderr)
        return 1
    try:
        if applying:
            return apply(droplet, candidate)
        loaded = descriptors.load_inventory(inventory)
        with contextlib.ExitStack() as stack:
            try:
                lock = stack.enter_context(production_lock(loaded, SSHExecutor(loaded)))
            except (LockHeld, OSError, ValueError) as error:
                print('caddy.py: nothing changed: %s' % error, file=sys.stderr)
                return 1
            deploy = functools.partial(run_deploy, inventory=inventory)
            return cutover(droplet, candidate, argv[2:], lock, deploy)
    except Unknown as error:
        print('caddy.py: outcome unknown on %s: %s; under the lock, inspect %s and the newest %s.before-* '
              '(journalctl -u caddy shows whether a reload applied) before anything else'
              % (droplet.host, error, LIVE, LIVE), file=sys.stderr)
        return UNKNOWN


if __name__ == '__main__':
    sys.exit(main())
