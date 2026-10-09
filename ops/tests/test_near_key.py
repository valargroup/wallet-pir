"""The receiver's NEAR key installer, `receiver/ops/digitalocean/near-key.sh`.

A stub `ssh` runs the installer's remote program locally, with
`/etc/receiver-pir` moved under a temporary root, and logs its argv, so the
tests see what reaches the Droplet and that the key travels only on stdin. A
stub `timeout` runs its command. For interruption, the stub can load a
`sitecustomize` into the remote Python that fails or kills it at `os.link`.
"""
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'receiver/ops/digitalocean/near-key.sh'
KEY = 'pk.Abc_123-xyz~+/=='
ID = '2026-10-09'

STUB_SSH = textwrap.dedent('''\
    #!{python}
    """Logs argv, then runs the remote command as ssh would, under HOST_ROOT."""
    import json, os, subprocess, sys
    root = os.environ['HOST_ROOT']
    with open(os.environ['STUB_LOG'], 'a') as log:
        log.write(json.dumps(['ssh'] + sys.argv[1:]) + '\\n')
    args = sys.argv[1:]
    while args[0] == '-o':
        args = args[2:]
    command = ' '.join(args[1:]).replace('/etc/receiver-pir', root + '/etc/receiver-pir')
    env = dict(os.environ)
    if env.get('NEAR_KEY_FAIL'):
        env['PYTHONPATH'] = os.environ['STUB_SITE']
    sys.exit(subprocess.run(['sh', '-c', command], env=env).returncode)
''')
STUB_TIMEOUT = textwrap.dedent('''\
    #!{python}
    """Logs argv, then runs the command without a limit."""
    import json, os, sys
    with open(os.environ['STUB_LOG'], 'a') as log:
        log.write(json.dumps(['timeout'] + sys.argv[1:]) + '\\n')
    os.execvp(sys.argv[2], sys.argv[2:])
''')
SITECUSTOMIZE = textwrap.dedent('''\
    """Fails (`link`) or kills (`kill`) the remote program where it would publish the key."""
    import errno, os, signal
    def link(source, destination):
        """Stands in for `os.link`."""
        if os.environ['NEAR_KEY_FAIL'] == 'kill':
            os.kill(os.getpid(), signal.SIGKILL)
        raise OSError(errno.EIO, 'injected failure')
    os.link = link
''')


class NearKeyInstaller(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.dir = Path(temp.name)
        self.root = self.dir / 'host'
        (self.root / 'etc').mkdir(parents=True)
        self.keys = self.root / 'etc/receiver-pir'
        self.log = self.dir / 'stub.log'
        bin_dir, site = self.dir / 'bin', self.dir / 'site'
        bin_dir.mkdir()
        site.mkdir()
        for name, text in (('ssh', STUB_SSH), ('timeout', STUB_TIMEOUT)):
            (bin_dir / name).write_text(text.format(python=sys.executable))
            (bin_dir / name).chmod(0o755)
        (site / 'sitecustomize.py').write_text(SITECUSTOMIZE)
        self.env = {'PATH': '%s%s%s' % (bin_dir, os.pathsep, os.environ.get('PATH', '')),
                    'HOST_ROOT': str(self.root), 'STUB_LOG': str(self.log), 'STUB_SITE': str(site)}

    def install(self, key=KEY, key_id=ID, fail=None, args=None):
        """Runs `near-key.sh install root@droplet <key_id>` with `key` on stdin."""
        env = dict(self.env, **({'NEAR_KEY_FAIL': fail} if fail else {}))
        argv = args if args is not None else ['install', 'root@droplet', key_id]
        return subprocess.run([str(SCRIPT), *argv], input=key, env=env, capture_output=True, text=True,
                              timeout=60)

    def calls(self):
        """Each stub call's argv, in order."""
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def installed(self, key_id=ID):
        return self.keys / ('near-%s.env' % key_id)

    def assert_key_never_shown(self, run, key=KEY):
        self.assertNotIn(key, run.stdout + run.stderr)
        self.assertFalse(any(key in word for call in self.calls() for word in call), self.calls())

    def test_install_writes_one_private_key_file_over_ssh(self):
        run = self.install()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(self.installed().read_bytes(), b'NEAR_INTENTS_EXPLORER=%s\n' % KEY.encode())
        self.assertEqual(stat.S_IMODE(self.installed().stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.keys.stat().st_mode), 0o700)
        self.assertEqual(sorted(p.name for p in self.keys.iterdir()), ['near-%s.env' % ID])
        self.assert_key_never_shown(run)
        timeout, ssh = self.calls()
        self.assertEqual(timeout[:3], ['timeout', '60', 'ssh'])
        self.assertEqual(ssh[:9], ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '-o',
                                   'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3'])
        self.assertEqual(ssh[9], 'root@droplet')
        # A trailing newline, as `echo` gives, is not part of the key.
        run = self.install(KEY + '\n', 'echoed')
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(self.installed('echoed').read_bytes(), self.installed().read_bytes())

    def test_an_installed_id_is_never_replaced(self):
        self.assertEqual(self.install().returncode, 0)
        before = self.installed().stat()
        run = self.install()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn('already installed', run.stdout)
        other = 'pk.another-key'
        run = self.install(other)
        self.assertEqual(run.returncode, 1)
        self.assertIn('never replaced', run.stderr)
        self.assert_key_never_shown(run, other)
        after = self.installed().stat()
        self.assertEqual((after.st_ino, after.st_mtime_ns), (before.st_ino, before.st_mtime_ns))
        self.assertEqual(self.installed().read_bytes(), b'NEAR_INTENTS_EXPLORER=%s\n' % KEY.encode())
        self.assertEqual(len(list(self.keys.iterdir())), 1)

    def test_a_directory_at_the_final_name_is_refused(self):
        self.installed().mkdir(parents=True)
        run = self.install()
        self.assertEqual(run.returncode, 1)
        self.assertTrue(self.installed().is_dir())
        self.assertEqual(list(self.installed().iterdir()), [])
        self.assertEqual(sorted(p.name for p in self.keys.iterdir()), ['near-%s.env' % ID])

    def test_a_key_directory_others_can_write_is_refused(self):
        self.keys.mkdir(mode=0o700)
        self.keys.chmod(0o775)
        run = self.install()
        self.assertEqual(run.returncode, 1)
        self.assertIn('writable by no one else', run.stderr)
        self.assertEqual(list(self.keys.iterdir()), [])

    def test_bad_ids_and_usage_reach_nothing(self):
        for key_id in ['', '../x', 'a/b', 'a b', 'a:b', 'x;y', "a'b"]:
            with self.subTest(key_id=key_id):
                self.assertEqual(self.install(key_id=key_id).returncode, 2)
        for args in [[], ['install', 'root@droplet'], ['remove', 'root@droplet', ID], ['install', '-oProxyCommand=x', ID]]:
            with self.subTest(args=args):
                self.assertEqual(self.install(args=args).returncode, 2)
        self.assertEqual(self.calls(), [])
        self.assertFalse(self.keys.exists())

    def test_invalid_keys_are_refused_without_being_shown(self):
        for key in ['', '\n', 'two\nlines', 'has space', 'quo"te', "quo'te", 'semi;colon', 'dollar$x', 'x' * 4097]:
            with self.subTest(key=key[:20]):
                run = self.install(key)
                self.assertEqual(run.returncode, 2)
                if key.strip():
                    self.assertNotIn(key, run.stdout + run.stderr)
        self.assertEqual(self.calls(), [])
        self.assertFalse(self.keys.exists())

    def test_an_interrupted_install_publishes_nothing(self):
        run = self.install(fail='link')
        self.assertEqual(run.returncode, 1)
        self.assertFalse(self.installed().exists())
        self.assertEqual(list(self.keys.iterdir()), [])
        self.assert_key_never_shown(run)
        # Killed after its temporary file is written: it may stay, never as a key file.
        run = self.install(fail='kill')
        self.assertNotEqual(run.returncode, 0)
        self.assertFalse(self.installed().exists())
        self.assertEqual(list(self.keys.glob('near-*.env')), [])
        left = list(self.keys.iterdir())
        run = self.install()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(self.installed().read_bytes(), b'NEAR_INTENTS_EXPLORER=%s\n' % KEY.encode())
        self.assertEqual(sorted(self.keys.iterdir()), sorted(left + [self.installed()]))


if __name__ == '__main__':
    unittest.main()
