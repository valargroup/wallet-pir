"""The receiver's Caddyfile helper, `tools/receiver/digitalocean/caddy.py`.

A stub `ssh` runs the helper's remote program locally, with `/etc/caddy`,
`/opt/receiver-pir` and `/proc/` moved under a temporary root. Stub `systemctl`,
`caddy`, `curl` and `receiver-directory` model the Droplet: a successful reload
copies the live Caddyfile to `run/caddy.running`, the configuration the probe and
the public routes answer from, and `# stub:` lines in a Caddyfile make them fail.
"""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / 'tools/receiver/digitalocean/caddy.py'
EXAMPLE = ROOT / 'tools/enhance/deploy/deploy-inventory.example.json'
LIVE = (ROOT / 'tools/receiver/digitalocean/Caddyfile').read_text()

STUB_SSH = textwrap.dedent('''\
    #!{python}
    """Runs the remote command as ssh would, under HOST_ROOT; STUB_SSH_SLEEP_OP stalls that operation."""
    import os, subprocess, sys, time
    root = os.environ['HOST_ROOT']
    args = sys.argv[1:]
    while args[0] in ('-o', '-F', '-i'):
        args = args[2:]
    command = ' '.join(args[1:])
    if os.environ.get('STUB_SSH_SLEEP_OP') and '"op": "%s"' % os.environ['STUB_SSH_SLEEP_OP'] in command:
        time.sleep(30)
    for path in ('/etc/caddy', '/opt/receiver-pir', '/proc/'):
        command = command.replace(path, root + path)
    sys.exit(subprocess.run(['sh', '-c', command]).returncode)
''')
STUBS = {
    'systemctl': '''\
        """`show` names the receiver's main process; `reload caddy` makes the live file the running one.

        A live file with `# stub: reload fails` fails the reload, leaving the running one.
        """
        import os, shutil, sys
        root = os.environ['HOST_ROOT']
        with open(os.environ['STUB_SYSTEMCTL_LOG'], 'a') as log:
            log.write(' '.join(sys.argv[1:]) + '\\n')
        if sys.argv[1] == 'show':
            print('MainPID=4242')
        elif sys.argv[1:] == ['reload', 'caddy']:
            live = root + '/etc/caddy/Caddyfile'
            if '# stub: reload fails' in open(live).read():
                sys.exit('reload failed')
            shutil.copy(live, root + '/run/caddy.running')
        else:
            sys.exit('unexpected systemctl call')
    ''',
    'caddy': '''\
        """`validate` fails a file with `# stub: invalid`."""
        import sys
        assert sys.argv[1:5] == ['validate', '--adapter', 'caddyfile', '--config'], sys.argv
        if '# stub: invalid' in open(sys.argv[5]).read():
            sys.exit('Error: adapting config using caddyfile: invalid')
    ''',
    'curl': '''\
        """Answers 404 unless the running file has `# stub: expose <path>` for that path."""
        import os, sys
        from urllib.parse import urlsplit
        running = open(os.environ['HOST_ROOT'] + '/run/caddy.running').read()
        print('200' if '# stub: expose ' + urlsplit(sys.argv[-1]).path + '\\n' in running else '404', end='')
    ''',
}
# The running release's `receiver-directory`, whose `probe` misses while the
# running file has `# stub: probe fails`, or its `@wallet` matcher lacks one of the
# routes named in a `route` file beside the binary, the routes that release reads.
PROBE = '''\
    import json, os, sys
    with open(os.environ['STUB_PROBE_LOG'], 'a') as log:
        log.write(json.dumps(sys.argv) + '\\n')
    running = open(os.environ['HOST_ROOT'] + '/run/caddy.running').read()
    if '# stub: probe fails' in running:
        sys.exit('lookup missed the pinned payment')
    route = os.path.join(os.path.dirname(sys.argv[0]), 'route')
    if os.path.exists(route):
        served = [l.split()[2:] for l in running.splitlines() if l.split()[:2] == ['@wallet', 'path']]
        missing = [r for r in open(route).read().split() if r not in sum(served, [])]
        if missing:
            sys.exit('the edge does not serve ' + ' '.join(missing))
    print('{"passed":true}')
'''


def wallet_routes(caddyfile):
    """The paths the `@wallet` matcher of `caddyfile` sends to the receiver."""
    (routes,) = [line.split()[2:] for line in caddyfile.splitlines() if line.split()[:2] == ['@wallet', 'path']]
    return routes


# The production edge before this release, which served aggregate filters, the
# witness file and one publication's `init` instead of block filters, witnesses over
# PIR and the shard inventory, and the routes the release adds to it.
PREVIOUS_ROUTES = ['/v1/receiver/init', '/v1/receiver/query', '/v1/receiver/public/*',
                   '/v1/receiver/witness/*', '/v1/receiver/filters/*']
PREVIOUS = LIVE.replace(' '.join(wallet_routes(LIVE)), ' '.join(PREVIOUS_ROUTES))
ADDED = {'/v1/receiver/block-filters/*', '/v1/receiver/inventory', '/v1/receiver/inventory/*',
         '/v1/receiver/shard-filters/*'}

# Loaded as `sitecustomize` by the remote program, it logs each fsync, link, rename and
# Caddy reload to STUB_FS_LOG in order, and fails directory fsyncs under STUB_DIR_SYNC_FAILS.
FS_HOOK = '''\
    import os, stat, subprocess
    if os.environ.get('STUB_FS_LOG'):
        def log(event):
            with open(os.environ['STUB_FS_LOG'], 'a') as handle:
                handle.write(event + '\\n')

        def wrap(module, name, event):
            original = getattr(module, name)

            def logged(*args, **kwargs):
                log(event(*args))
                return original(*args, **kwargs)
            setattr(module, name, logged)

        def fsync(fd):
            directory = stat.S_ISDIR(os.fstat(fd).st_mode)
            if directory and os.environ.get('STUB_DIR_SYNC_FAILS'):
                raise OSError('directory sync failed')
            return 'fsync ' + ('directory' if directory else 'file')
        wrap(os, 'fsync', fsync)
        wrap(os, 'link', lambda *args: 'link')
        wrap(os, 'rename', lambda *args: 'rename')
        wrap(subprocess, 'run', lambda argv, *args: ' '.join(argv) if argv[0] == 'systemctl' else '')
'''


def script(path, body):
    """Writes an executable Python script at `path`."""
    path.write_text('#!%s\n%s' % (sys.executable, textwrap.dedent(body)))
    path.chmod(0o755)


class StubDroplet(unittest.TestCase):
    """The stub Droplet and the helpers that drive and inspect it."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.dir = Path(temp.name)
        self.root = self.dir / 'host'
        self.caddy_dir = self.root / 'etc/caddy'
        self.caddy_dir.mkdir(parents=True)
        (self.root / 'run').mkdir()
        self.live = self.caddy_dir / 'Caddyfile'
        self.live.write_text(LIVE)
        self.running = self.root / 'run/caddy.running'
        self.running.write_text(LIVE)
        self.release = self.root / 'opt/receiver-pir/releases' / ('ab' * 32)
        self.release.mkdir(parents=True)
        script(self.release / 'receiver-directory', PROBE)
        (self.root / 'proc/4242').mkdir(parents=True)
        (self.root / 'proc/4242/exe').symlink_to(self.release / 'receiver-directory')
        bin_dir = self.dir / 'bin'
        bin_dir.mkdir()
        (bin_dir / 'ssh').write_text(STUB_SSH.format(python=sys.executable))
        (bin_dir / 'ssh').chmod(0o755)
        for name, body in STUBS.items():
            script(bin_dir / name, body)
        self.inventory = {'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
                          'hosts': {'coordinator': {}, 'receiver-01': {}},
                          'services': {'receiver': json.loads(EXAMPLE.read_text())['services']['receiver']}}
        (self.dir / 'inventory.json').write_text(json.dumps(self.inventory))
        self.logs = {name: self.dir / (name + '.log') for name in ('systemctl', 'probe')}
        self.env = {'PATH': os.pathsep.join([str(bin_dir), str(Path(sys.executable).parent), os.environ['PATH']]),
                    'HOST_ROOT': str(self.root), 'STUB_SYSTEMCTL_LOG': str(self.logs['systemctl']),
                    'STUB_PROBE_LOG': str(self.logs['probe']),
                    'WALLET_PIR_DEPLOY_INVENTORY': str(self.dir / 'inventory.json')}

    def apply(self, candidate):
        """Runs `caddy.py apply` on `candidate` written to a file."""
        path = self.dir / 'Caddyfile.candidate'
        path.write_text(candidate)
        return subprocess.run([sys.executable, str(SCRIPT), 'apply', str(path)], env=self.env,
                              capture_output=True, text=True, timeout=120)

    def lines(self, name):
        path = self.logs[name]
        return path.read_text().splitlines() if path.exists() else []

    def reloads(self):
        return self.lines('systemctl').count('reload caddy')

    def backups(self):
        return sorted(self.caddy_dir.glob('Caddyfile.before-*'))

    def assert_restored(self, run):
        """The candidate failed, and the original is live and running again from a kept backup."""
        self.assertEqual(run.returncode, 1, run.stderr)
        self.assertIn('restored and verified the predecessor', run.stderr)
        self.assertEqual((self.live.read_text(), self.running.read_text()), (LIVE, LIVE))
        (backup,) = self.backups()
        self.assertEqual(backup.read_text(), LIVE)
        self.assertEqual(sorted(p.name for p in self.caddy_dir.iterdir()), ['Caddyfile', backup.name])


class ReceiverCaddy(StubDroplet):
    def test_a_verified_candidate_is_applied_and_the_predecessor_kept(self):
        candidate = LIVE + '# reviewed change\n'
        run = self.apply(candidate)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual((self.live.read_text(), self.running.read_text()), (candidate, candidate))
        (backup,) = self.backups()
        self.assertEqual(backup.read_text(), LIVE)
        self.assertIn(backup.name, run.stdout)
        # The exact check from the running release.
        (argv,) = [json.loads(line) for line in self.lines('probe')]
        check = self.inventory['services']['receiver']['exact_check']['argv']
        self.assertEqual(argv[1:], check[1:])

    def test_an_invalid_candidate_replaces_nothing(self):
        run = self.apply(LIVE + '# stub: invalid\n')
        self.assertEqual(run.returncode, 1, run.stderr)
        self.assertIn('candidate invalid', run.stderr)
        self.assertEqual((self.live.read_text(), self.reloads()), (LIVE, 0))
        self.assertEqual(sorted(p.name for p in self.caddy_dir.iterdir()), ['Caddyfile'])

    def test_wrong_routing_is_restored(self):
        run = self.apply(LIVE + '# stub: probe fails\n')
        self.assertIn('lookup missed the pinned payment', run.stderr)
        self.assert_restored(run)
        self.assertEqual((self.reloads(), len(self.lines('probe'))), (2, 2))

    def test_an_exposed_metrics_route_is_restored(self):
        run = self.apply(LIVE + '# stub: expose /metrics\n')
        self.assertIn('https://receiver-pir.valargroup.dev/metrics answered 200, not 404', run.stderr)
        self.assert_restored(run)

    def test_a_failed_restoration_is_an_unknown_outcome(self):
        # The predecessor cannot be reloaded, so Caddy may still run the candidate.
        original = LIVE + '# stub: reload fails\n'
        self.live.write_text(original)
        run = self.apply(LIVE + '# stub: probe fails\n')
        self.assertEqual(run.returncode, 75, run.stderr)
        self.assertIn('outcome unknown on receiver-01: restoring', run.stderr)
        (backup,) = self.backups()
        self.assertIn(backup.name, run.stderr)
        self.assertEqual((backup.read_text(), self.live.read_text()), (original, original))

    def test_each_replacement_is_durable_before_the_reload(self):
        hooks = self.dir / 'hooks'
        hooks.mkdir()
        (hooks / 'sitecustomize.py').write_text(textwrap.dedent(FS_HOOK))
        log = self.dir / 'fs.log'
        self.env.update(PYTHONPATH=str(hooks), STUB_FS_LOG=str(log))
        self.assert_restored(self.apply(LIVE + '# stub: probe fails\n'))
        events = [event for event in log.read_text().splitlines() if event and not event.startswith('systemctl show')]
        self.assertEqual(events, [
            # Apply: the candidate, then the backup link and the replacement in the directory.
            'fsync file', 'link', 'fsync directory', 'rename', 'fsync directory', 'systemctl reload caddy',
            # Restore: the copied backup, then its replacement.
            'fsync file', 'rename', 'fsync directory', 'systemctl reload caddy'])
        # A failed directory sync never reports success, and nothing reloads.
        log.unlink()
        self.env['STUB_DIR_SYNC_FAILS'] = '1'
        run = self.apply(LIVE + '# reviewed change\n')
        self.assertEqual(run.returncode, 75, run.stderr)
        self.assertNotIn('systemctl reload caddy', log.read_text())
        self.assertEqual(self.running.read_text(), LIVE)

    def test_two_runs_in_one_second_keep_both_backups(self):
        spec = importlib.util.spec_from_file_location('receiver_caddy', SCRIPT)
        helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(helper)
        path = self.dir / 'Caddyfile.candidate'
        statuses = []
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, self.env))
            stack.enter_context(patch.object(helper.time, 'strftime', lambda *args: '20261020T000000Z'))
            stack.enter_context(patch('sys.stdout', io.StringIO()))
            for change in ('# first\n', '# second\n'):
                path.write_text(LIVE + change)
                statuses.append(helper.main(['apply', str(path)]))
        self.assertEqual(statuses, [0, 0])
        self.assertEqual(sorted(b.read_text() for b in self.backups()), sorted([LIVE, LIVE + '# first\n']))
        self.assertEqual(self.live.read_text(), LIVE + '# second\n')

    def test_a_timeout_is_reported_as_an_unknown_outcome(self):
        spec = importlib.util.spec_from_file_location('receiver_caddy', SCRIPT)
        helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(helper)
        path = self.dir / 'Caddyfile.candidate'
        path.write_text(LIVE + '# reviewed change\n')
        output = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(helper, 'STEP_SECONDS', 0.5))
            stack.enter_context(patch.dict(os.environ, dict(self.env, STUB_SSH_SLEEP_OP='apply')))
            stack.enter_context(patch('sys.stderr', output))
            status = helper.main(['apply', str(path)])
        self.assertEqual(status, helper.UNKNOWN)
        self.assertIn('outcome unknown on receiver-01: apply timed out', output.getvalue())



class ReceiverCutover(StubDroplet):
    """The previous release, on the `PREVIOUS` edge, upgrades to this one."""

    def setUp(self):
        super().setUp()
        self.previous = self.root / 'opt/receiver-pir/releases' / ('cd' * 32)
        self.previous.mkdir(parents=True)
        self.previous, self.release = self.previous.resolve(), self.release.resolve()
        script(self.previous / 'receiver-directory', PROBE)
        (self.previous / 'route').write_text(' '.join(PREVIOUS_ROUTES))
        (self.release / 'route').write_text(' '.join(wallet_routes(LIVE)))
        self.live.write_text(PREVIOUS)
        self.running.write_text(PREVIOUS)
        self.run_release(self.previous)
        spec = importlib.util.spec_from_file_location('receiver_caddy', SCRIPT)
        self.helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.helper)
        self.locks = []

    def run_release(self, release):
        """Makes the receiver's main process run `release`."""
        (self.root / 'proc/4242/exe').unlink()
        (self.root / 'proc/4242/exe').symlink_to(release / 'receiver-directory')

    def deploy(self, fails=False, commits=False):
        """A stand-in for the deploy tool: `deploy` runs this release and its exact check,
        rolling back to the previous one when that fails or `fails`; `rollback` runs the
        previous release. With `commits`, it keeps the new release and still exits 1, as a
        deploy whose baseline refresh fails after its commit does."""
        def run(argv, lock, inventory):
            self.assertEqual(inventory, self.env['WALLET_PIR_DEPLOY_INVENTORY'])
            self.locks.append(lock)
            if argv[0] == 'rollback':
                self.run_release(self.previous)
                return 0
            self.run_release(self.release)
            check = subprocess.run([str(self.release / 'receiver-directory'), 'probe'], env=self.env,
                                   capture_output=True)
            if commits:
                return 1
            if fails or check.returncode:
                self.run_release(self.previous)
                return 1
            return 0
        return run

    def cutover(self, candidate, argv, deploy, lock=None):
        """Runs `caddy.py cutover` on `candidate` with deploy tool `argv` under `lock`, a
        stand-in that stays held by default."""
        path = self.dir / 'Caddyfile.candidate'
        path.write_text(candidate)
        self.locks = []
        lock = lock or type('Lock', (), {'verify': lambda self: None})()
        output = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, self.env))
            stack.enter_context(patch.object(self.helper, 'production_lock',
                                             lambda inventory, executor: contextlib.nullcontext(lock)))
            stack.enter_context(patch.object(self.helper, 'run_deploy', deploy))
            stack.enter_context(patch('sys.stderr', output))
            status = self.helper.main(['cutover', str(path), *argv])
        self.assertTrue(all(held is lock for held in self.locks))
        return status, output.getvalue()

    def running_release(self):
        """The release directory the receiver's main process runs."""
        return (self.root / 'proc/4242/exe').resolve().parent

    def test_neither_change_alone_upgrades(self):
        # The new routes break the running binary's probe, so they are restored.
        run = self.apply(LIVE)
        removed = [route for route in PREVIOUS_ROUTES if route not in wallet_routes(LIVE)]
        self.assertEqual(set(removed), {'/v1/receiver/init', '/v1/receiver/witness/*', '/v1/receiver/filters/*'})
        self.assertIn('the edge does not serve ' + ' '.join(removed), run.stderr)
        self.assertEqual(run.returncode, 1, run.stderr)
        self.assertEqual((self.live.read_text(), self.running.read_text()), (PREVIOUS, PREVIOUS))
        # The new binary's own check, as its deploy runs it, fails through the old routes.
        status, _ = self.cutover(PREVIOUS, ['deploy', 'receiver'], self.deploy())
        self.assertEqual((status, self.running_release()), (1, self.previous))
        self.assertEqual(self.live.read_text(), PREVIOUS)

    def test_a_cutover_changes_the_binary_and_routes_together(self):
        # The new release's probe reads every route this Caddyfile adds.
        self.assertEqual(set(wallet_routes(LIVE)) - set(PREVIOUS_ROUTES), ADDED)
        status, output = self.cutover(LIVE, ['deploy', 'receiver', '--sha', 'x'], self.deploy())
        self.assertEqual((status, len(self.locks)), (0, 1), output)
        self.assertEqual((self.live.read_text(), self.running.read_text()), (LIVE, LIVE))
        self.assertEqual(self.running_release(), self.release)
        (backup,) = self.backups()
        self.assertEqual(backup.read_text(), PREVIOUS)
        # The deploy's check, then the inventory's probe, both from the new release.
        probes = [json.loads(line)[0] for line in self.lines('probe')]
        self.assertEqual(probes, [str(self.release / 'receiver-directory')] * 2)
        # Rolling back restores both, verified with the previous binary, keeping both
        # backups even within the same second.
        status, output = self.cutover(backup.read_text(), ['rollback', 'receiver'], self.deploy())
        self.assertEqual(status, 0, output)
        self.assertEqual(len(self.backups()), 2)
        self.assertEqual((self.live.read_text(), self.running.read_text()), (PREVIOUS, PREVIOUS))
        self.assertEqual(self.running_release(), self.previous)

    def test_a_failed_deploy_restores_the_previous_routes(self):
        status, output = self.cutover(LIVE, ['deploy', 'receiver'], self.deploy(fails=True))
        self.assertEqual(status, 1, output)
        self.assertIn('deploy exited 1', output)
        self.assertIn('restored and verified the predecessor', output)
        self.assertEqual((self.live.read_text(), self.running.read_text()), (PREVIOUS, PREVIOUS))
        self.assertEqual(self.running_release(), self.previous)

    def test_a_deploy_that_commits_then_fails_keeps_the_candidate_routes(self):
        status, output = self.cutover(LIVE, ['deploy', 'receiver'], self.deploy(commits=True))
        self.assertEqual(status, self.helper.UNKNOWN, output)
        self.assertIn('so the candidate routes stay', output)
        self.assertEqual((self.live.read_text(), self.running.read_text()), (LIVE, LIVE))
        self.assertEqual(self.running_release(), self.release)

    def test_exposed_operator_routes_stop_the_cutover_before_the_deploy(self):
        deploy = self.deploy()
        status, output = self.cutover(LIVE + '# stub: expose /metrics\n', ['deploy', 'receiver'], deploy)
        self.assertEqual(status, 1, output)
        self.assertEqual((self.locks, self.running_release()), ([], self.previous))
        self.assertEqual(self.live.read_text(), PREVIOUS)

    def test_the_deploy_tool_runs_under_the_held_lock(self):
        from wallet_pir_ops.deploy import cli
        held, seen = object(), []
        def main(argv, lock):
            with lock() as inner:
                seen.append((argv, inner))
            return 3
        with patch.object(cli, 'main', main):
            self.assertEqual(self.helper.run_deploy(['deploy', 'receiver'], held, 'a.json'), 3)
        self.assertEqual(seen, [(['--inventory', 'a.json', 'deploy', 'receiver'], held)])

    def test_a_cutover_and_its_deploy_share_one_inventory(self):
        other = self.dir / 'other-inventory.json'
        other.write_text(json.dumps(self.inventory))
        same = self.env['WALLET_PIR_DEPLOY_INVENTORY']
        for argv, refusal in [
            (['--inventory', str(other), 'deploy', 'receiver'], 'names another inventory'),
            (['--descriptors', str(other), 'deploy', 'receiver'], 'names other service descriptors'),
        ]:
            with self.subTest(argv=argv):
                status, output = self.cutover(LIVE, argv, self.deploy())
                self.assertEqual(status, 1, output)
                self.assertIn('caddy.py: nothing changed: ', output)
                self.assertIn(refusal, output)
        self.assertEqual((self.locks, self.reloads(), self.backups()), ([], 0, []))
        self.assertEqual(self.live.read_text(), PREVIOUS)
        # The same inventory named again is the one the cutover holds.
        status, output = self.cutover(LIVE, ['--inventory', same, 'deploy', 'receiver'], self.deploy())
        self.assertEqual(status, 0, output)
        self.assertEqual((self.live.read_text(), self.running_release()), (LIVE, self.release))

    def test_a_cutover_parses_the_whole_command_before_any_change(self):
        for argv, refusal in [
            (['deploy', 'receiver', '--skip-exact-check'], 'needs the exact check'),
            (['deploy', 'status', '--sha', 'x'], 'only deploy receiver or rollback receiver'),
            (['plan', 'receiver', '--sha', 'x'], 'only deploy receiver or rollback receiver'),
            (['deploy', 'receiver', '--help'], 'does not parse'),
            (['deploy', 'receiver', '--archive'], 'does not parse'),
        ]:
            with self.subTest(argv=argv):
                status, output = self.cutover(LIVE, argv, self.deploy())
                self.assertEqual(status, 1, output)
                self.assertIn('caddy.py: nothing changed: ', output)
                self.assertIn(refusal, output)
        self.assertEqual((self.locks, self.reloads(), self.backups()), ([], 0, []))
        self.assertEqual(self.live.read_text(), PREVIOUS)

    def test_a_lost_lock_after_installing_is_unknown_and_restores_nothing(self):
        from wallet_pir_ops.deploy.remote import LockHeld

        class Lock:
            """A lock whose SSH session ends after `lasts` more checks pass."""
            def __init__(self, lasts):
                self.lasts = lasts

            def verify(self):
                self.lasts -= 1
                if self.lasts < 0:
                    raise LockHeld('lost /run/lock/wallet-pir-production.lock: its SSH session ended')

        # Lost once the candidate is installed: no deploy runs.
        status, output = self.cutover(LIVE, ['deploy', 'receiver', '--sha', 'x'], self.deploy(), Lock(1))
        self.assertEqual(status, self.helper.UNKNOWN, output)
        self.assertIn('lost the production lock with the candidate installed', output)
        self.assertIn('wallet-pir-deploy.py status receiver', output)
        self.assertEqual((self.locks, self.reloads(), self.live.read_text()), ([], 1, LIVE))
        # Lost during a deploy that then fails: the predecessor is not restored.
        self.live.write_text(PREVIOUS)
        self.running.write_text(PREVIOUS)
        for backup in self.backups():
            backup.unlink()
        status, output = self.cutover(LIVE, ['deploy', 'receiver', '--sha', 'x'], self.deploy(fails=True), Lock(2))
        self.assertEqual(status, self.helper.UNKNOWN, output)
        self.assertIn('nothing was restored', output)
        self.assertEqual((len(self.locks), self.reloads(), self.live.read_text()), (1, 2, LIVE))

if __name__ == '__main__':
    unittest.main()
