"""The receiver's Caddyfile helper, `receiver/ops/digitalocean/caddy.py`.

A stub `ssh` runs the helper's remote program locally, with `/etc/caddy`,
`/opt/receiver-pir` and `/proc/` moved under a temporary root. Stub `systemctl`,
`caddy`, `curl` and `receiver-probe` model the Droplet: a successful reload copies
the live Caddyfile to `run/caddy.running`, the configuration the probe and the
public routes answer from, and `# stub:` lines in a Caddyfile make them fail (see
`STUBS`). Deadlines are exercised in process, with the helper's limits shortened.
"""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'receiver/ops/digitalocean/caddy.py'
EXAMPLE = ROOT / 'enhance/ops/deploy/deploy-inventory.example.json'
LIVE = (ROOT / 'receiver/ops/digitalocean/Caddyfile').read_text()
PID = '4242'

STUB_SSH = textwrap.dedent('''\
    #!{python}
    """Logs argv, then runs the remote command as ssh would, under HOST_ROOT.

    STUB_SSH_SLEEP_OP sleeps 30 seconds before that operation; STUB_SSH_FAIL_OP
    drops the connection (exit 255) instead of running it.
    """
    import json, os, subprocess, sys, time
    root = os.environ['HOST_ROOT']
    with open(os.environ['STUB_LOG'], 'a') as log:
        log.write(json.dumps(sys.argv) + '\\n')
    args = sys.argv[1:]
    while args[0] in ('-o', '-F', '-i'):
        args = args[2:]
    command = ' '.join(args[1:])
    for op, action in ((os.environ.get('STUB_SSH_SLEEP_OP'), 'sleep'), (os.environ.get('STUB_SSH_FAIL_OP'), 'fail')):
        if op and '"op": "%s"' % op in command:
            if action == 'fail':
                sys.exit(255)
            time.sleep(30)
    for path in ('/etc/caddy', '/opt/receiver-pir', '/proc/'):
        command = command.replace(path, root + path)
    sys.exit(subprocess.run(['sh', '-c', command]).returncode)
''')
STUBS = {
    'systemctl': '''\
        """`show` reports the receiver unit; `reload caddy` makes the live file the running one.

        A live file with `# stub: reload fails` fails the reload, leaving the running one.
        """
        import os, shutil, sys
        root = os.environ['HOST_ROOT']
        with open(os.environ['STUB_SYSTEMCTL_LOG'], 'a') as log:
            log.write(' '.join(sys.argv[1:]) + '\\n')
        if sys.argv[1] == 'show':
            print('ActiveState=' + os.environ.get('STUB_ACTIVE', 'active'))
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
        import os, sys
        with open(os.environ['STUB_CADDY_LOG'], 'a') as log:
            log.write(' '.join(sys.argv[1:]) + '\\n')
        assert sys.argv[1:5] == ['validate', '--adapter', 'caddyfile', '--config'], sys.argv
        if '# stub: invalid' in open(sys.argv[5]).read():
            sys.exit('Error: adapting config using caddyfile: invalid')
    ''',
    'curl': '''\
        """Answers 404 unless the running file has `# stub: expose <path>` for that path."""
        import os, sys
        from urllib.parse import urlsplit
        url = sys.argv[-1]
        assert '-L' not in sys.argv and '--location' not in sys.argv, sys.argv
        running = open(os.environ['HOST_ROOT'] + '/run/caddy.running').read()
        print('200' if '# stub: expose ' + urlsplit(url).path + '\\n' in running else '404', end='')
    ''',
}
PROBE = '''\
    """`receiver-directory`: logs its argv; fails while the running file has `# stub: probe fails`."""
    import json, os, sys
    with open(os.environ['STUB_PROBE_LOG'], 'a') as log:
        log.write(json.dumps(sys.argv[1:]) + '\\n')
    if '# stub: probe fails' in open(os.environ['HOST_ROOT'] + '/run/caddy.running').read():
        sys.exit('lookup missed the pinned payment')
    print('{"passed":true}')
'''
BINARY = ('#!%s\n%s' % (sys.executable, textwrap.dedent(PROBE))).encode()
DIGEST = hashlib.sha256(BINARY).hexdigest()


def load_helper():
    """The helper as a module, for its deadlines in process."""
    spec = importlib.util.spec_from_file_location('receiver_caddy', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def script(path, body):
    """Writes an executable Python script at `path`."""
    path.write_text('#!%s\n%s' % (sys.executable, textwrap.dedent(body)))
    path.chmod(0o755)


class ReceiverCaddy(unittest.TestCase):
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
        (self.root / 'proc' / PID).mkdir(parents=True)
        (self.root / 'proc' / PID / 'exe').write_bytes(BINARY)
        self.release = self.root / 'opt/receiver-pir/releases' / DIGEST
        self.release.mkdir(parents=True)
        (self.release / 'receiver-directory').write_bytes(BINARY)
        (self.release / 'receiver-directory').chmod(0o755)
        bin_dir = self.dir / 'bin'
        bin_dir.mkdir()
        (bin_dir / 'ssh').write_text(STUB_SSH.format(python=sys.executable))
        (bin_dir / 'ssh').chmod(0o755)
        for name, body in STUBS.items():
            script(bin_dir / name, body)
        example = json.loads(EXAMPLE.read_text())
        self.inventory = {'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
                          'hosts': {'coordinator': {}, 'receiver-01': {}},
                          'services': {'receiver': example['services']['receiver']}}
        self.inventory_path = self.dir / 'inventory.json'
        self.write_inventory()
        self.logs = {name: self.dir / (name + '.log') for name in ('ssh', 'systemctl', 'caddy', 'probe')}
        self.env = {'PATH': os.pathsep.join([str(bin_dir), str(Path(sys.executable).parent), os.environ['PATH']]),
                    'HOST_ROOT': str(self.root), 'STUB_LOG': str(self.logs['ssh']),
                    'STUB_SYSTEMCTL_LOG': str(self.logs['systemctl']), 'STUB_CADDY_LOG': str(self.logs['caddy']),
                    'STUB_PROBE_LOG': str(self.logs['probe']),
                    'WALLET_PIR_DEPLOY_INVENTORY': str(self.inventory_path)}

    def write_inventory(self):
        self.inventory_path.write_text(json.dumps(self.inventory))

    def apply(self, candidate, env=None, args=None):
        """Runs `caddy.py apply` on `candidate` written to a file."""
        path = self.dir / 'Caddyfile.candidate'
        path.write_text(candidate)
        argv = args if args is not None else ['apply', str(path)]
        return subprocess.run([sys.executable, str(SCRIPT), *argv], env=dict(self.env, **(env or {})),
                              capture_output=True, text=True, timeout=120)

    def apply_in_process(self, candidate, env, **limits):
        """Runs the helper's `main` with `limits` patched; returns its status and output."""
        helper = load_helper()
        path = self.dir / 'Caddyfile.candidate'
        path.write_text(candidate)
        output = io.StringIO()
        with contextlib.ExitStack() as stack:
            for name, value in limits.items():
                stack.enter_context(patch.object(helper, name, value))
            stack.enter_context(patch.dict(os.environ, dict(self.env, **env)))
            stack.enter_context(patch('sys.stdout', output))
            stack.enter_context(patch('sys.stderr', output))
            status = helper.main(['apply', str(path)])
        return status, output.getvalue()

    def lines(self, name):
        path = self.logs[name]
        return path.read_text().splitlines() if path.exists() else []

    def reloads(self):
        return self.lines('systemctl').count('reload caddy')

    def backups(self):
        return sorted(self.caddy_dir.glob('Caddyfile.before-*'))

    def assert_untouched(self, run=None):
        """The live and running files are the original, with no backup, reload or leftover temporary file."""
        self.assertEqual(self.live.read_text(), LIVE)
        self.assertEqual(self.running.read_text(), LIVE)
        self.assertEqual(self.reloads(), 0)
        self.assertEqual(sorted(p.name for p in self.caddy_dir.iterdir()), ['Caddyfile'])

    def assert_restored(self, run):
        """The candidate was replaced by the original from a kept backup, and Caddy runs the original."""
        self.assertEqual(self.live.read_text(), LIVE)
        self.assertEqual(self.running.read_text(), LIVE)
        (backup,) = self.backups()
        self.assertEqual(backup.read_text(), LIVE)
        self.assertIn('/etc/caddy/' + backup.name, run.stdout + run.stderr)
        self.assertEqual(sorted(p.name for p in self.caddy_dir.iterdir()), ['Caddyfile', backup.name])

    def test_a_verified_candidate_is_applied_and_the_predecessor_kept(self):
        candidate = LIVE + '# reviewed change\n'
        run = self.apply(candidate)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn('applied and verified on receiver-01', run.stdout)
        self.assertEqual(self.live.read_text(), candidate)
        self.assertEqual(self.running.read_text(), candidate)
        (backup,) = self.backups()
        self.assertEqual(backup.read_text(), LIVE)
        self.assertIn('backup /etc/caddy/' + backup.name, run.stdout)
        self.assertEqual(self.reloads(), 1)
        self.assertEqual(len(self.lines('caddy')), 1)
        self.assertEqual(len(self.lines('probe')), 2)  # baseline and after the reload
        ssh = json.loads(self.lines('ssh')[0])
        self.assertEqual(ssh[1:5], ['-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3'])
        self.assertIn('receiver-01', ssh)

    def test_the_probe_runs_from_the_running_release_without_the_feed_wait(self):
        run = self.apply(LIVE + '# reviewed change\n')
        self.assertEqual(run.returncode, 0, run.stderr)
        argv = json.loads(self.lines('probe')[0])
        check = self.inventory['services']['receiver']['exact_check']['argv']
        position = check.index('--await-feed-reads')
        expected = check[1:position] + check[position + 2:]
        release = str(self.root) + '/opt/receiver-pir/releases/' + DIGEST
        self.assertEqual(argv, [a.replace('{release_dir}', release) for a in expected])
        self.assertIn('--witnesses', argv)
        self.assertNotIn('--await-feed-reads', argv)
        self.assertNotIn(check[position + 1], argv)

    def restore_attempted(self):
        return any('"op": "restore"' in json.loads(line)[-1] for line in self.lines('ssh'))

    def on_disk(self, candidate):
        """The live file holds `candidate` while Caddy runs the original, as an interrupted apply leaves it."""
        self.live.write_text(candidate)

    def test_a_candidate_already_on_disk_is_validated_reloaded_and_verified(self):
        candidate = LIVE + '# reviewed change\n'
        self.on_disk(candidate)
        run = self.apply(candidate)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn('already held the candidate; reloaded and verified on receiver-01; no backup made', run.stdout)
        self.assertEqual(self.live.read_text(), candidate)
        self.assertEqual(self.running.read_text(), candidate)
        self.assertEqual(self.reloads(), 1)
        (validated,) = self.lines('caddy')
        self.assertIn('/etc/caddy/.Caddyfile.candidate-', validated)
        self.assertEqual(len(self.lines('probe')), 2)  # baseline and after the reload
        self.assertEqual(sorted(p.name for p in self.caddy_dir.iterdir()), ['Caddyfile'])

    def test_a_failing_candidate_already_on_disk_is_not_reported_or_restored(self):
        cases = {
            'invalid': ('# stub: invalid\n', 4, 'not replaced (it already holds these bytes)'),
            'reload': ('# stub: reload fails\n', 7, 'reload failed'),
            'probe': ('# stub: probe fails\n', 7, 'lookup missed the pinned payment'),
            'route': ('# stub: expose /metrics\n', 7, '/metrics answered 200, not 404'),
        }
        for case, (marker, status, reason) in cases.items():
            for kept in ([], ['20260101T000000Z', '20260102T000000Z']):
                with self.subTest(case=case, kept=kept):
                    self.setUp()
                    for stamp in kept:
                        (self.caddy_dir / ('Caddyfile.before-' + stamp)).write_text(LIVE)
                    candidate = LIVE + marker
                    self.on_disk(candidate)
                    run = self.apply(candidate)
                    self.assertEqual(run.returncode, status, run.stderr)
                    self.assertIn(reason, run.stderr)
                    self.assertNotIn('verified on', run.stdout)
                    self.assertFalse(self.restore_attempted())
                    self.assertEqual(self.live.read_text(), candidate)
                    self.assertEqual([p.name for p in self.backups()], ['Caddyfile.before-' + s for s in kept])
                    self.assertEqual(self.reloads(), 0 if case == 'invalid' else 1)
                    self.assertEqual(self.running.read_text(), candidate if case in ('probe', 'route') else LIVE)
                    if case == 'invalid':
                        continue
                    self.assertIn('nothing restored, no backup made', run.stderr)
                    if kept:
                        self.assertIn("latest backup is /etc/caddy/Caddyfile.before-%s; " % kept[-1], run.stderr)
                        self.assertIn("ssh receiver-01 'cp -p /etc/caddy/Caddyfile.before-%s " % kept[-1], run.stderr)
                    else:
                        self.assertIn('no backup exists to restore', run.stderr)

    def test_an_invalid_candidate_replaces_nothing(self):
        run = self.apply(LIVE + '# stub: invalid\n')
        self.assertEqual(run.returncode, 4, run.stderr)
        self.assertIn('candidate invalid', run.stderr)
        self.assert_untouched()
        (validated,) = self.lines('caddy')
        self.assertIn('/etc/caddy/.Caddyfile.candidate-', validated)

    def test_a_candidate_that_breaks_the_probe_is_restored(self):
        run = self.apply(LIVE + '# stub: probe fails\n')
        self.assertEqual(run.returncode, 5, run.stderr)
        self.assertIn('lookup missed the pinned payment', run.stderr)
        self.assertIn('restored and verified', run.stderr)
        self.assert_restored(run)
        self.assertEqual(self.reloads(), 2)
        self.assertEqual(len(self.lines('probe')), 3)

    def test_a_candidate_exposing_an_operator_route_is_restored(self):
        for path in ('/metrics', '/v1/receiver/health'):
            with self.subTest(path=path):
                self.setUp()
                run = self.apply(LIVE + '# stub: expose %s\n' % path)
                self.assertEqual(run.returncode, 5, run.stderr)
                self.assertIn('https://receiver-pir.valargroup.dev%s answered 200, not 404' % path, run.stderr)
                self.assert_restored(run)

    def test_a_failed_reload_is_restored(self):
        run = self.apply(LIVE + '# stub: reload fails\n')
        self.assertEqual(run.returncode, 5, run.stderr)
        self.assertIn('reload failed', run.stderr)
        self.assert_restored(run)
        self.assertEqual(len(self.lines('probe')), 2)  # baseline and after restoring

    def test_a_failed_restoration_has_its_own_status_and_keeps_the_backup(self):
        # The predecessor cannot be reloaded, which the baseline does not notice.
        original = LIVE + '# stub: reload fails\n'
        self.live.write_text(original)
        self.running.write_text(original)
        run = self.apply(LIVE + '# stub: probe fails\n')
        self.assertEqual(run.returncode, 6, run.stderr)
        self.assertIn('restoration failed on receiver-01', run.stderr)
        (backup,) = self.backups()
        self.assertEqual(backup.read_text(), original)
        self.assertEqual(self.live.read_text(), original)
        self.assertIn("ssh receiver-01 'cp -p /etc/caddy/%s " % backup.name, run.stderr)

    def test_a_failing_baseline_refuses_before_any_change(self):
        for marker in ('# stub: probe fails\n', '# stub: expose /metrics\n'):
            with self.subTest(marker=marker):
                self.setUp()
                self.running.write_text(LIVE + marker)
                run = self.apply(LIVE + '# reviewed change\n')
                self.assertEqual(run.returncode, 3, run.stderr)
                self.assertIn('already fails verification', run.stderr)
                self.assertEqual(self.live.read_text(), LIVE)
                self.assertEqual(self.reloads(), 0)
                self.assertEqual(self.lines('caddy'), [])
                self.assertEqual(self.backups(), [])

    def test_the_running_release_must_be_complete(self):
        cases = {
            'inactive': lambda: None,
            'digest': lambda: (self.release / 'receiver-directory').write_bytes(b'another build\n'),
        }
        for case, mutate in cases.items():
            with self.subTest(case=case):
                self.setUp()
                mutate()
                run = self.apply(LIVE + '# reviewed change\n', env={'STUB_ACTIVE': 'inactive'} if case == 'inactive' else None)
                self.assertEqual(run.returncode, 3, run.stderr)
                self.assertIn('no running release to verify with', run.stderr)
                self.assertEqual(self.lines('probe'), [])
                self.assert_untouched()

    def test_inventory_and_candidate_problems_refuse_before_any_ssh(self):
        cases = [
            ('the receiver host', lambda check: check.update(host='coordinator'), LIVE),
            ('has no value', lambda check: check.update(argv=check['argv'][:-1]), LIVE),
            ('no site block', lambda check: None, LIVE.replace('receiver-pir.valargroup.dev {', 'other.example {')),
        ]
        for reason, mutate, candidate in cases:
            with self.subTest(reason=reason):
                self.setUp()
                mutate(self.inventory['services']['receiver']['exact_check'])
                self.write_inventory()
                run = self.apply(candidate)
                self.assertEqual(run.returncode, 3, run.stderr)
                self.assertIn(reason, run.stderr)
                self.assertEqual(self.lines('ssh'), [])
        for env, args in [({}, []), ({}, ['apply']), ({}, ['install', 'x']),
                          ({'WALLET_PIR_DEPLOY_INVENTORY': ''}, None)]:
            with self.subTest(args=args):
                self.assertEqual(self.apply(LIVE, env=env, args=args).returncode, 2)

    def test_a_stalled_apply_reports_an_unknown_outcome(self):
        started = time.monotonic()
        status, output = self.apply_in_process(LIVE + '# reviewed change\n', {'STUB_SSH_SLEEP_OP': 'apply'},
                                               APPLY_SECONDS=0.5)
        self.assertEqual(status, 75)
        self.assertLess(time.monotonic() - started, 20)
        self.assertIn('outcome unknown on receiver-01: apply timed out after 0.5 seconds', output)
        self.assertIn('/etc/caddy/Caddyfile.before-', output)
        self.assertEqual(self.live.read_text(), LIVE)

    def test_a_lost_connection_reports_an_unknown_outcome(self):
        run = self.apply(LIVE + '# reviewed change\n', env={'STUB_SSH_FAIL_OP': 'apply'})
        self.assertEqual(run.returncode, 75, run.stderr)
        self.assertIn('outcome unknown', run.stderr)
        self.assertIn('ssh exit 255', run.stderr)
        # A lost restoration is unknown too, with the candidate still live here.
        run = self.apply(LIVE + '# stub: probe fails\n', env={'STUB_SSH_FAIL_OP': 'restore'})
        self.assertEqual(run.returncode, 75, run.stderr)
        self.assertIn('restoration outcome unknown on receiver-01', run.stderr)
        (backup,) = self.backups()
        self.assertEqual(backup.read_text(), LIVE)

    def test_pinned_ssh_comes_from_the_inventory(self):
        known_hosts = self.dir / 'known_hosts'
        known_hosts.write_text('192.0.2.18 ssh-ed25519 AAAA\n')
        self.inventory['ssh'] = {'mode': 'pinned', 'user': 'root', 'key': str(self.dir / 'key'),
                                 'known_hosts': str(known_hosts),
                                 'known_hosts_sha256': hashlib.sha256(known_hosts.read_bytes()).hexdigest()}
        self.inventory['hosts'] = {'coordinator': {'address': '192.0.2.10'}, 'receiver-01': {'address': '192.0.2.18'}}
        self.write_inventory()
        droplet = load_helper().Droplet(self.inventory_path, LIVE.encode())
        self.assertEqual(droplet.transport[-1], 'root@192.0.2.18')
        self.assertEqual(droplet.transport[1:5], ['-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3'])
        self.assertIn('UserKnownHostsFile=' + str(known_hosts.resolve()), droplet.transport)


if __name__ == '__main__':
    unittest.main()
