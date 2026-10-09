"""The receiver's Caddyfile helper, `receiver/ops/digitalocean/caddy.py`.

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

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'receiver/ops/digitalocean/caddy.py'
EXAMPLE = ROOT / 'enhance/ops/deploy/deploy-inventory.example.json'
LIVE = (ROOT / 'receiver/ops/digitalocean/Caddyfile').read_text()

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
# running file has `# stub: probe fails`.
PROBE = '''\
    import json, os, sys
    with open(os.environ['STUB_PROBE_LOG'], 'a') as log:
        log.write(json.dumps(sys.argv[1:]) + '\\n')
    if '# stub: probe fails' in open(os.environ['HOST_ROOT'] + '/run/caddy.running').read():
        sys.exit('lookup missed the pinned payment')
    print('{"passed":true}')
'''


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

    def test_a_verified_candidate_is_applied_and_the_predecessor_kept(self):
        candidate = LIVE + '# reviewed change\n'
        run = self.apply(candidate)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual((self.live.read_text(), self.running.read_text()), (candidate, candidate))
        (backup,) = self.backups()
        self.assertEqual(backup.read_text(), LIVE)
        self.assertIn(backup.name, run.stdout)
        # The exact check from the running release, without the feed wait.
        (argv,) = [json.loads(line) for line in self.lines('probe')]
        check = self.inventory['services']['receiver']['exact_check']['argv']
        wait = check.index('--await-feed-reads')
        self.assertEqual(argv, check[1:wait] + check[wait + 2:])

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


if __name__ == '__main__':
    unittest.main()
