"""The receiver's NEAR key installer, `receiver/ops/digitalocean/near-key.py`.

A stub `ssh` runs the installer's remote program locally, with
`/etc/receiver-pir` moved under a temporary root, and logs its argv, so the
tests see what reaches the Droplet and that the key travels only on stdin. With
`NEAR_KEY_HOOK` set, the stub loads a `sitecustomize` into the remote Python
that interrupts it or traces it (see `SITECUSTOMIZE`). Deadlines are exercised
in process, with the installer's limits shortened.
"""
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'receiver/ops/digitalocean/near-key.py'
KEY = 'pk.Abc_123-xyz~+/=='
LINE = KEY + '\n'
ID = '2026-10-09'

STUB_SSH = textwrap.dedent('''\
    #!{python}
    """Logs argv, then runs the remote command as ssh would, under HOST_ROOT."""
    import json, os, subprocess, sys, time
    root = os.environ['HOST_ROOT']
    with open(os.environ['STUB_LOG'], 'a') as log:
        log.write(json.dumps(sys.argv) + '\\n')
    time.sleep(float(os.environ.get('STUB_SSH_SLEEP', '0')))
    args = sys.argv[1:]
    while args[0] == '-o':
        args = args[2:]
    command = ' '.join(args[1:]).replace('/etc/receiver-pir', root + '/etc/receiver-pir')
    env = dict(os.environ)
    if env.get('NEAR_KEY_HOOK'):
        env['PYTHONPATH'] = os.environ['STUB_SITE']
    sys.exit(subprocess.run(['sh', '-c', command], env=env).returncode)
''')
SITECUSTOMIZE = textwrap.dedent('''\
    """Hooks the remote program as NEAR_KEY_HOOK says.

    `link` fails its publishing link, `kill` kills it there and `kill-after-link`
    just after it; `other-owner` makes regular files appear owned by another user;
    `trace-fsync` logs each fsync'd inode to FSYNC_LOG.
    """
    import errno, json, os, signal, stat
    hook = os.environ['NEAR_KEY_HOOK']
    real_link, real_fstat, real_fsync = os.link, os.fstat, os.fsync

    def link(source, destination):
        """Stands in for `os.link`."""
        if hook == 'kill':
            os.kill(os.getpid(), signal.SIGKILL)
        if hook == 'link':
            raise OSError(errno.EIO, 'injected failure')
        real_link(source, destination)
        if hook == 'kill-after-link':
            os.kill(os.getpid(), signal.SIGKILL)

    def fstat(fd):
        """Stands in for `os.fstat`."""
        info = real_fstat(fd)
        if hook == 'other-owner' and stat.S_ISREG(info.st_mode):
            return os.stat_result(info[:4] + (info.st_uid + 1,) + info[5:10])
        return info

    def fsync(fd):
        """Stands in for `os.fsync`."""
        with open(os.environ['FSYNC_LOG'], 'a') as log:
            log.write(json.dumps(real_fstat(fd).st_ino) + '\\n')
        real_fsync(fd)

    os.link, os.fstat = link, fstat
    if hook == 'trace-fsync':
        os.fsync = fsync
''')


def load_installer():
    """The installer as a module, for its deadlines in process."""
    spec = importlib.util.spec_from_file_location('near_key', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class NearKeyInstaller(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.dir = Path(temp.name)
        self.root = self.dir / 'host'
        (self.root / 'etc').mkdir(parents=True)
        self.keys = self.root / 'etc/receiver-pir'
        self.log = self.dir / 'stub.log'
        self.fsyncs = self.dir / 'fsync.log'
        bin_dir, site = self.dir / 'bin', self.dir / 'site'
        bin_dir.mkdir()
        site.mkdir()
        (bin_dir / 'ssh').write_text(STUB_SSH.format(python=sys.executable))
        (bin_dir / 'ssh').chmod(0o755)
        (site / 'sitecustomize.py').write_text(SITECUSTOMIZE)
        self.env = {'PATH': '%s%s%s' % (bin_dir, os.pathsep, os.environ.get('PATH', '')),
                    'HOST_ROOT': str(self.root), 'STUB_LOG': str(self.log), 'STUB_SITE': str(site),
                    'FSYNC_LOG': str(self.fsyncs)}

    def install(self, line=LINE, key_id=ID, hook=None, args=None):
        """Runs `near-key.py install root@droplet <key_id>` with `line` on stdin."""
        env = dict(self.env, **({'NEAR_KEY_HOOK': hook} if hook else {}))
        argv = args if args is not None else ['install', 'root@droplet', key_id]
        return subprocess.run([str(SCRIPT), *argv], input=line, env=env, capture_output=True, text=True,
                              timeout=60)

    def calls(self):
        """Each stub ssh call's argv, in order."""
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def installed(self, key_id=ID):
        return self.keys / ('near-%s.env' % key_id)

    def assert_key_never_shown(self, run, key=KEY):
        self.assertNotIn(key, run.stdout + run.stderr)
        self.assertFalse(any(key in word for call in self.calls() for word in call), self.calls())

    def assert_nothing_installed(self):
        self.assertEqual(self.calls(), [])
        self.assertFalse(self.keys.exists())

    def test_install_writes_one_private_key_file_over_ssh(self):
        run = self.install()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(self.installed().read_bytes(), b'NEAR_INTENTS_EXPLORER=%s' % LINE.encode())
        self.assertEqual(stat.S_IMODE(self.installed().stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.keys.stat().st_mode), 0o700)
        self.assertEqual(sorted(p.name for p in self.keys.iterdir()), ['near-%s.env' % ID])
        self.assert_key_never_shown(run)
        (ssh,) = self.calls()
        self.assertEqual(ssh[1:10], ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '-o',
                                     'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3', 'root@droplet'])

    def test_an_installed_id_is_never_replaced(self):
        self.assertEqual(self.install().returncode, 0)
        before = self.installed().stat()
        run = self.install()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn('already installed', run.stdout)
        other = 'pk.another-key'
        run = self.install(other + '\n')
        self.assertEqual(run.returncode, 1)
        self.assertIn('never replaced', run.stderr)
        self.assert_key_never_shown(run, other)
        after = self.installed().stat()
        self.assertEqual((after.st_ino, after.st_mtime_ns), (before.st_ino, before.st_mtime_ns))
        self.assertEqual(self.installed().read_bytes(), b'NEAR_INTENTS_EXPLORER=%s' % LINE.encode())
        self.assertEqual(len(list(self.keys.iterdir())), 1)

    def test_a_directory_at_the_final_name_is_refused(self):
        self.installed().mkdir(parents=True)
        run = self.install()
        self.assertEqual(run.returncode, 1)
        self.assertTrue(self.installed().is_dir())
        self.assertEqual(list(self.installed().iterdir()), [])
        self.assertEqual(sorted(p.name for p in self.keys.iterdir()), ['near-%s.env' % ID])

    def test_an_existing_key_file_must_be_private_with_one_name(self):
        """Matching content is not enough: an exposed, foreign or shared file is refused."""
        self.assertEqual(self.install().returncode, 0)
        content = self.installed().read_bytes()
        self.installed().chmod(0o644)
        run = self.install()
        self.assertEqual(run.returncode, 1)
        self.assertIn('not a private file', run.stderr)
        self.installed().chmod(0o600)
        run = self.install(hook='other-owner')
        self.assertEqual(run.returncode, 1)
        self.assertIn('not a private file', run.stderr)
        os.link(self.installed(), self.dir / 'elsewhere')
        run = self.install()
        self.assertEqual(run.returncode, 1)
        self.assertIn('not a private file', run.stderr)
        os.unlink(self.dir / 'elsewhere')
        self.assertEqual(self.install().returncode, 0)
        self.assertEqual(self.installed().read_bytes(), content)

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
        for args in [[], ['install', 'root@droplet'], ['remove', 'root@droplet', ID],
                     ['install', '-oProxyCommand=x', ID]]:
            with self.subTest(args=args):
                self.assertEqual(self.install(args=args).returncode, 2)
        self.assert_nothing_installed()

    def test_invalid_keys_are_refused_without_being_shown(self):
        for line in ['', '\n', 'two\nlines\n', 'has space\n', 'quo"te\n', "quo'te\n", 'semi;colon\n',
                     'dollar$x\n', 'x' * 4097 + '\n', LINE + '\n']:
            with self.subTest(line=line[:20]):
                run = self.install(line)
                self.assertEqual(run.returncode, 2)
                for word in line.split():
                    self.assertNotIn(word, run.stdout + run.stderr)
        self.assert_nothing_installed()

    def test_a_truncated_key_line_is_refused_as_incomplete(self):
        """A producer that dies partway leaves a valid-looking prefix without its newline."""
        for line in [KEY, KEY[:5]]:
            with self.subTest(line=line):
                run = self.install(line)
                self.assertEqual(run.returncode, 2)
                self.assertIn('without a newline', run.stderr)
                self.assert_key_never_shown(run)
        self.assert_nothing_installed()

    def test_a_stalled_producer_is_refused_at_the_deadline(self):
        installer = load_installer()
        read, write = os.pipe()
        self.addCleanup(os.close, write)
        self.addCleanup(os.close, read)
        os.write(write, KEY.encode())  # a key, then neither its newline nor the end
        stderr = io.StringIO()
        started = time.monotonic()
        with patch.object(installer, 'STDIN_SECONDS', 0.5), patch.dict(os.environ, self.env), \
                patch('sys.stderr', stderr):
            status = installer.main(['install', 'root@droplet', ID], stdin=read)
        self.assertEqual(status, 2)
        self.assertLess(time.monotonic() - started, 5)
        self.assertIn('did not end within 0.5 seconds', stderr.getvalue())
        self.assertNotIn(KEY, stderr.getvalue())
        self.assert_nothing_installed()

    def test_an_endless_stream_is_refused_without_reading_it_all(self):
        producer = subprocess.Popen(['yes', 'x' * 100], stdout=subprocess.PIPE)
        self.addCleanup(producer.wait)
        self.addCleanup(producer.kill)
        run = subprocess.run([str(SCRIPT), 'install', 'root@droplet', ID], stdin=producer.stdout,
                             env=self.env, capture_output=True, text=True, timeout=10)
        producer.stdout.close()
        self.assertEqual(run.returncode, 2)
        self.assertIn('more than one key line', run.stderr)
        self.assert_nothing_installed()

    def test_a_stalled_remote_step_times_out(self):
        installer = load_installer()
        read, write = os.pipe()
        os.write(write, LINE.encode())
        os.close(write)
        self.addCleanup(os.close, read)
        stderr = io.StringIO()
        env = dict(self.env, STUB_SSH_SLEEP='30')
        started = time.monotonic()
        with patch.object(installer, 'REMOTE_SECONDS', 0.5), patch.dict(os.environ, env), \
                patch('sys.stderr', stderr):
            status = installer.main(['install', 'root@droplet', ID], stdin=read)
        self.assertEqual(status, 124)
        self.assertLess(time.monotonic() - started, 10)
        self.assertIn('timed out', stderr.getvalue())
        self.assertFalse(self.keys.exists())

    def test_an_interrupted_install_publishes_nothing(self):
        run = self.install(hook='link')
        self.assertEqual(run.returncode, 1)
        self.assertFalse(self.installed().exists())
        self.assertEqual(list(self.keys.iterdir()), [])
        self.assert_key_never_shown(run)
        # Killed after its temporary file is written: it may stay, never as a key file.
        run = self.install(hook='kill')
        self.assertNotEqual(run.returncode, 0)
        self.assertFalse(self.installed().exists())
        self.assertEqual(list(self.keys.glob('near-*.env')), [])
        left = list(self.keys.iterdir())
        run = self.install()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(self.installed().read_bytes(), b'NEAR_INTENTS_EXPLORER=%s' % LINE.encode())
        self.assertEqual(sorted(self.keys.iterdir()), sorted(left + [self.installed()]))


if __name__ == '__main__':
    unittest.main()
