"""The receiver's NEAR key installer, `receiver/ops/digitalocean/near-key.py`.

A stub `ssh` logs its argv and runs the installer's remote program locally,
with `/etc/receiver-pir` moved under a temporary root, so the tests see the
transport, what reaches the Droplet and that the key travels only on stdin.
"""
import hashlib
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
SCRIPT = ROOT / 'receiver/ops/digitalocean/near-key.py'
EXAMPLE = ROOT / 'enhance/ops/deploy/deploy-inventory.example.json'
KEY = 'pk.Abc_123-xyz~+/=='
ID = '2026-10-09'

STUB_SSH = textwrap.dedent('''\
    #!{python}
    """Logs argv, then runs the remote command as ssh would, under HOST_ROOT."""
    import json, os, subprocess, sys
    with open(os.environ['STUB_LOG'], 'a') as log:
        log.write(json.dumps(sys.argv) + '\\n')
    args = sys.argv[1:]
    while args[0] in ('-o', '-F', '-i'):
        args = args[2:]
    command = ' '.join(args[1:]).replace('/etc/receiver-pir', os.environ['HOST_ROOT'] + '/etc/receiver-pir')
    sys.exit(subprocess.run(['sh', '-c', command]).returncode)
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
        bin_dir = self.dir / 'bin'
        bin_dir.mkdir()
        (bin_dir / 'ssh').write_text(STUB_SSH.format(python=sys.executable))
        (bin_dir / 'ssh').chmod(0o755)
        self.known_hosts = self.dir / 'known_hosts'
        self.known_hosts.write_text('192.0.2.18 ssh-ed25519 AAAA\n')
        inventory = {'lock': {'type': 'remote', 'host': 'coordinator'},
                     'ssh': {'mode': 'pinned', 'user': 'root', 'key': str(self.dir / 'deploy-key'),
                             'known_hosts': str(self.known_hosts),
                             'known_hosts_sha256': hashlib.sha256(self.known_hosts.read_bytes()).hexdigest()},
                     'hosts': {'coordinator': {'address': '192.0.2.10'}, 'receiver-01': {'address': '192.0.2.18'}},
                     'services': {'receiver': json.loads(EXAMPLE.read_text())['services']['receiver']}}
        (self.dir / 'inventory.json').write_text(json.dumps(inventory))
        self.env = {'PATH': '%s%s%s' % (bin_dir, os.pathsep, os.environ.get('PATH', '')),
                    'HOST_ROOT': str(self.root), 'STUB_LOG': str(self.log),
                    'WALLET_PIR_DEPLOY_INVENTORY': str(self.dir / 'inventory.json')}

    def install(self, key=KEY, key_id=ID):
        """Runs `near-key.py <key_id>` with `key` in NEAR_INTENTS_EXPLORER."""
        return subprocess.run([sys.executable, str(SCRIPT), key_id], env=dict(self.env, NEAR_INTENTS_EXPLORER=key),
                              capture_output=True, text=True, timeout=60)

    def calls(self):
        """Each stub ssh call's argv, in order."""
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def installed(self, key_id=ID):
        return self.keys / ('near-%s.env' % key_id)

    def assert_key_never_shown(self, run, key=KEY):
        self.assertNotIn(key, run.stdout + run.stderr)
        self.assertFalse(any(key in word for call in self.calls() for word in call), self.calls())

    def test_install_writes_one_private_key_file_over_pinned_ssh(self):
        run = self.install()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(self.installed().read_text(), 'NEAR_INTENTS_EXPLORER=%s\n' % KEY)
        self.assertEqual(stat.S_IMODE(self.installed().stat().st_mode), 0o600)
        self.assertEqual(sorted(p.name for p in self.keys.iterdir()), ['near-%s.env' % ID])
        self.assert_key_never_shown(run)
        (ssh,) = self.calls()
        self.assertIn('UserKnownHostsFile=' + str(self.known_hosts.resolve()), ssh)
        self.assertEqual(ssh[ssh.index('-i') + 1], str((self.dir / 'deploy-key').resolve()))
        self.assertIn('root@192.0.2.18', ssh)

    def test_an_installed_id_is_never_replaced(self):
        self.assertEqual(self.install().returncode, 0)
        before = self.installed().stat()
        other = 'pk.another-key'
        run = self.install(other)
        self.assertEqual(run.returncode, 1)
        self.assertIn('never replaced', run.stderr)
        self.assert_key_never_shown(run, other)
        after = self.installed().stat()
        self.assertEqual((after.st_ino, after.st_mtime_ns), (before.st_ino, before.st_mtime_ns))
        self.assertEqual(self.installed().read_text(), 'NEAR_INTENTS_EXPLORER=%s\n' % KEY)
        self.assertEqual(sorted(p.name for p in self.keys.iterdir()), ['near-%s.env' % ID])

    def test_bad_ids_and_keys_reach_nothing(self):
        for key_id in ['', '../x', 'a/b', 'a b', 'a:b', "a'b"]:
            with self.subTest(key_id=key_id):
                self.assertEqual(self.install(key_id=key_id).returncode, 2)
        for key in ['', 'has space', 'two\nlines', 'quo"te', 'semi;colon', 'x' * 4097]:
            with self.subTest(key=key[:20]):
                run = self.install(key)
                self.assertEqual(run.returncode, 2)
                for word in key.split():
                    self.assertNotIn(word, run.stdout + run.stderr)
        self.assertEqual(self.calls(), [])
        self.assertFalse(self.keys.exists())


if __name__ == '__main__':
    unittest.main()
