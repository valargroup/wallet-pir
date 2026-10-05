"""The shared operations primitives: durable files, the pinned-host lock, the
saved-plan runner, the read-only DigitalOcean client, pinned SSH and the
transparent worker unit rewriter."""
import hashlib
import http.server
import json
import os
from pathlib import Path
import shlex
import stat
import subprocess
import sys
import tempfile
import textwrap
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import urllib.error

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'ops/lib'))
from wallet_pir_ops import digitalocean, durable, hostlock, pinned_ssh, terraform, transparent_unit  # noqa: E402

MACHINE = 'a' * 32


def lock_class(directory):
    """The production lock, pointed at a temporary host where this user is 'root'."""
    class Lock(hostlock.PinnedHostLock):
        PATH = directory / 'writer.lock'
        MACHINE_ID = directory / 'machine-id'
        ROOT_UID = os.geteuid()
    Lock.MACHINE_ID.write_text(MACHINE + '\n')
    return Lock


CONFIG = {'type': 'pinned_host', 'machine_id': MACHINE}

# A Terraform stand-in: logs each call, writes plans, and reports whether it
# inherited a descriptor for the lock file and whether that lock is held.
FAKE_TERRAFORM = textwrap.dedent('''\
    #!{python}
    import fcntl, json, os, sys
    args = sys.argv[1:]
    lock = os.environ.get('FAKE_LOCK')
    record = {{'args': args}}
    if lock:
        target = os.stat(lock)
        inherited = False
        for fd in range(3, 256):
            try:
                info = os.fstat(fd)
            except OSError:
                continue
            inherited |= (info.st_dev, info.st_ino) == (target.st_dev, target.st_ino)
        probe = os.open(lock, os.O_RDONLY)
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            held = False
        except BlockingIOError:
            held = True
        finally:
            os.close(probe)
        record.update(inherited=inherited, held=held)
    with open(os.environ['FAKE_LOG'], 'a') as log:
        log.write(json.dumps(record) + '\\n')
    command = args[1]
    if os.environ.get('FAKE_FAIL') == command:
        sys.exit(1)
    if command == 'plan':
        out = next(a for a in args if a.startswith('-out='))[5:]
        with open(out, 'wb') as handle:
            handle.write(b'saved-plan')
    elif command in ('show', 'output', 'state'):
        print(json.dumps({{'command': command}}))
''')


class FakeTerraform:
    def __init__(self, directory):
        self.bin = directory / 'bin'
        self.bin.mkdir()
        self.log = directory / 'terraform.log'
        executable = self.bin / 'terraform'
        executable.write_text(FAKE_TERRAFORM.format(python=sys.executable))
        executable.chmod(0o755)
        self.env = {'PATH': str(self.bin) + os.pathsep + os.environ.get('PATH', ''),
                    'FAKE_LOG': str(self.log)}

    def calls(self):
        if not self.log.exists():
            return []
        return [json.loads(line) for line in self.log.read_text().splitlines()]


class DurableTests(unittest.TestCase):
    def test_digest_is_canonical_and_refuses_nan(self):
        self.assertEqual(durable.digest({'b': [1, 'x'], 'a': None}),
                         hashlib.sha256(b'{"a":null,"b":[1,"x"]}').hexdigest())
        self.assertEqual(durable.digest({'a': 1, 'b': 2}), durable.digest({'b': 2, 'a': 1}))
        with self.assertRaises(ValueError):
            durable.digest({'a': float('nan')})

    def test_atomic_json_is_private_indented_and_leaves_no_temporary(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'value.json'
            durable.atomic_json(path, {'z': 1, 'a': 'é'})
            self.assertEqual(path.read_bytes(), b'{\n  "a": "\\u00e9",\n  "z": 1\n}\n')
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            durable.atomic_json(path, {'shared': True}, mode=0o644)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o644)
            with self.assertRaises(ValueError):
                durable.atomic_json(path, {'bad': float('inf')}, prefix='.probe-')
            # The previous file survives a failed write and nothing is left behind.
            self.assertEqual(json.loads(path.read_text()), {'shared': True})
            self.assertEqual(sorted(p.name for p in Path(temp).iterdir()), ['value.json'])


class HostLockTests(unittest.TestCase):
    def test_no_config_is_a_no_op(self):
        with hostlock.PinnedHostLock(None) as lock:
            self.assertIsNone(lock.fd)
            self.assertEqual(lock.descriptors(), ())
            lock.verify()

    def test_contention_inheritance_and_replacement(self):
        with tempfile.TemporaryDirectory() as temp:
            Lock = lock_class(Path(temp))
            with Lock(CONFIG) as held:
                self.assertEqual(held.descriptors(), (held.fd,))
                with self.assertRaises(BlockingIOError):
                    with Lock(CONFIG):
                        pass
                # A child keeps the lock after its controller closes the descriptor.
                child = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'],
                                         stdin=subprocess.PIPE, pass_fds=held.descriptors())
            try:
                with self.assertRaises(BlockingIOError):
                    with Lock(CONFIG):
                        pass
            finally:
                child.communicate(timeout=10)
            with Lock(CONFIG) as held:
                Lock.PATH.unlink()
                Lock.PATH.touch(mode=0o600)
                with self.assertRaisesRegex(ValueError, '^state lock file was replaced$'):
                    held.descriptors()
            self.assertIsNone(held.fd)

    def test_instance_path_overrides_the_class_path(self):
        with tempfile.TemporaryDirectory() as temp:
            Lock = lock_class(Path(temp))
            other = Path(temp) / 'other.lock'
            with Lock(CONFIG, path=other) as held, Lock(CONFIG):
                self.assertEqual(os.fstat(held.fd).st_ino, other.stat().st_ino)

    def test_refusals_keep_their_messages(self):
        with tempfile.TemporaryDirectory() as temp:
            Lock = lock_class(Path(temp))
            for config in [dict(CONFIG, machine_id='b' * 32), dict(CONFIG, type='native'),
                           dict(CONFIG, extra=True), {'type': 'pinned_host', 'machine_id': 7}, ['type', 'machine_id']]:
                with self.subTest(config=config), self.assertRaisesRegex(ValueError, '^state writer must run as root on the pinned host$'):
                    with Lock(config):
                        pass
            with patch.object(Lock, 'ROOT_UID', os.geteuid() + 1):
                with self.assertRaisesRegex(ValueError, 'pinned host'):
                    with Lock(CONFIG):
                        pass
            Lock.PATH.touch()
            Lock.PATH.chmod(0o640)
            with self.assertRaisesRegex(ValueError, '^state lock requires a private root-owned regular file$'):
                with Lock(CONFIG):
                    pass
            Lock.PATH.unlink()
            Lock.PATH.symlink_to(Lock.MACHINE_ID)
            with self.assertRaises(OSError):
                with Lock(CONFIG):
                    pass
            unheld = Lock(CONFIG)
            with self.assertRaisesRegex(ValueError, '^pinned-host state lock is not held$'):
                unheld.verify()


class TerraformTests(unittest.TestCase):
    def test_clean_environment_strips_ambient_overrides(self):
        source = {'PATH': '/bin', 'TF_CLI_ARGS_apply': '-lock=false', 'TF_WORKSPACE': 'production',
                  'DIGITALOCEAN_API_URL': 'https://elsewhere.invalid', 'HOME': '/root'}
        env = terraform.clean_environment(source, TF_VAR_x='1')
        self.assertEqual(env, {'PATH': '/bin', 'HOME': '/root', 'TF_IN_AUTOMATION': '1', 'TF_INPUT': '0', 'TF_VAR_x': '1'})
        env = terraform.clean_environment(source, strip=('TF_', 'DIGITALOCEAN_', 'HOME'))
        self.assertNotIn('HOME', env)

    def test_saved_plan_digest_binds_apply(self):
        with tempfile.TemporaryDirectory() as temp:
            fake = FakeTerraform(Path(temp))
            root = Path(temp) / 'root'
            root.mkdir()
            runner = terraform.Terraform(root, env=fake.env)
            plan = Path(temp) / 'saved.tfplan'
            digest = runner.save_plan(plan, ['-var-file=inputs.json'])
            self.assertEqual(digest, hashlib.sha256(b'saved-plan').hexdigest())
            self.assertEqual(stat.S_IMODE(plan.stat().st_mode), 0o600)
            self.assertEqual(runner.show_json(plan), {'command': 'show'})
            self.assertEqual(runner.output_json(), {'command': 'output'})
            self.assertEqual(runner.state_pull(), {'command': 'state'})
            plan.write_bytes(b'changed after review')
            with self.assertRaisesRegex(ValueError, 'saved plan changed'):
                runner.apply(plan, digest)
            self.assertNotIn('apply', [c['args'][1] for c in fake.calls()])
            plan.write_bytes(b'saved-plan')
            runner.apply(plan, digest)
            calls = [c['args'] for c in fake.calls()]
            self.assertEqual(calls[0], [f'-chdir={root.resolve()}', 'plan', '-input=false', '-lock-timeout=60s',
                                        '-out=' + str(plan), '-var-file=inputs.json'])
            self.assertEqual(calls[-1], [f'-chdir={root.resolve()}', 'apply', '-input=false', '-lock-timeout=60s', str(plan)])

    def test_failure_is_fenced_and_timeouts_are_bounded(self):
        with tempfile.TemporaryDirectory() as temp:
            fake = FakeTerraform(Path(temp))
            runner = terraform.Terraform(temp, env=dict(fake.env, FAKE_FAIL='apply'))
            plan = Path(temp) / 'saved.tfplan'
            digest = runner.save_plan(plan)
            with self.assertRaisesRegex(RuntimeError, 'remains fenced'):
                runner.apply(plan, digest)
            result = SimpleNamespace(returncode=0, stdout=b'{}')
            with patch.object(terraform.subprocess, 'run', return_value=result) as run:
                runner.apply(plan, digest)
                self.assertEqual(run.call_args.kwargs['timeout'], 900)
                self.assertEqual(run.call_args.kwargs['pass_fds'], ())
                runner.show_json(plan)
                self.assertEqual(run.call_args.kwargs['timeout'], 120)

    def test_terraform_inherits_the_held_lock(self):
        with tempfile.TemporaryDirectory() as temp:
            Lock = lock_class(Path(temp))
            fake = FakeTerraform(Path(temp))
            env = dict(fake.env, FAKE_LOCK=str(Lock.PATH))
            with Lock(CONFIG) as held:
                runner = terraform.Terraform(temp, env=env, lock=held)
                runner.output_json()
                Lock.PATH.unlink()
                Lock.PATH.touch(mode=0o600)
                with self.assertRaisesRegex(ValueError, 'replaced'):
                    runner.output_json()
            call, = fake.calls()
            self.assertTrue(call['inherited'])
            self.assertTrue(call['held'])


class DigitalOceanTests(unittest.TestCase):
    def test_tag_filter_pagination_and_duplicates(self):
        client = digitalocean.DigitalOcean('fixture-token')
        pages = [{'droplets': [{'id': 1}], 'links': {'pages': {'next': 'https://api.digitalocean.com/v2/droplets?page=2&per_page=200&tag_name=t'}}},
                 {'droplets': [{'id': 2}]}]
        with patch.object(client, 'get', side_effect=pages) as get:
            self.assertEqual([d['id'] for d in client.droplets(tag='transparent-pir-worker')], [1, 2])
            self.assertEqual(get.call_args_list[0].args[0], '/v2/droplets?per_page=200&tag_name=transparent-pir-worker')
        with patch.object(client, 'get', return_value={'droplets': []}) as get:
            client.droplets()
            get.assert_called_once_with('/v2/droplets?per_page=200')
        for tag in ['a&b', 'x y', '', 'a/b', 7]:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                client.droplets(tag=tag)
        pages[0]['links']['pages']['next'] = 'https://elsewhere.invalid/v2/droplets?page=2'
        with patch.object(client, 'get', side_effect=pages), self.assertRaises(ValueError):
            client.droplets()
        with patch.object(client, 'get', side_effect=[{'droplets': [{'id': 1}], 'links': {'pages': {'next': 'https://api.digitalocean.com/v2/droplets?page=2'}}},
                                                      {'droplets': [{'id': 1}]}]):
            with self.assertRaises(ValueError):
                client.droplets()

    def test_single_droplet_and_absence(self):
        client = digitalocean.DigitalOcean('fixture-token')
        with patch.object(client, 'get', return_value={'droplet': {'id': 5}}) as get:
            self.assertEqual(client.droplet(5), {'id': 5})
            get.assert_called_once_with('/v2/droplets/5')
        gone = urllib.error.HTTPError('u', 404, 'Not Found', {}, None)
        with patch.object(client, 'get', side_effect=gone):
            self.assertIsNone(client.droplet('5'))
        failed = urllib.error.HTTPError('u', 500, 'Error', {}, None)
        with patch.object(client, 'get', side_effect=failed), self.assertRaises(urllib.error.HTTPError):
            client.droplet(5)
        for value in ['0', '5/../account', '-1', 'x']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                client.droplet(value)

    def test_redirects_are_not_followed_with_the_token(self):
        seen = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append((self.path, self.headers.get('Authorization')))
                self.send_response(302)
                self.send_header('Location', '/stolen')
                self.end_headers()

            def log_message(self, *_args):
                pass
        server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = digitalocean.DigitalOcean('fixture-token')
            client.API = f'http://127.0.0.1:{server.server_port}'
            with self.assertRaises(urllib.error.HTTPError) as caught:
                client.get('/v2/account')
            self.assertEqual(caught.exception.code, 302)
            caught.exception.close()
        finally:
            server.shutdown()
            server.server_close()
        self.assertEqual(seen, [('/v2/account', 'Bearer fixture-token')])


class PinnedSSHTests(unittest.TestCase):
    def test_pin_is_checked_before_every_connection(self):
        with tempfile.TemporaryDirectory() as temp:
            known = Path(temp) / 'known_hosts'
            known.write_text('10.0.0.5 ssh-ed25519 AAAA\n')
            pin = pinned_ssh.sha256(known.read_bytes())
            with self.assertRaisesRegex(ValueError, 'differs from the verified pin'):
                pinned_ssh.PinnedSSH('10.0.0.5', Path(temp) / 'key', known, 'b' * 64)
            with self.assertRaises(ValueError):
                pinned_ssh.PinnedSSH('10.0.0.5; rm -rf /', Path(temp) / 'key', known, pin)
            remote = pinned_ssh.PinnedSSH('10.0.0.5', Path(temp) / 'key', known, pin, user='deploy')
            arguments = ['printf', '%s', 'literal;$(must-not-run)']
            ok = SimpleNamespace(returncode=0, stdout=b'ok')
            with patch.object(pinned_ssh.subprocess, 'run', return_value=ok) as run:
                self.assertEqual(remote.command(arguments), b'ok')
                call = run.call_args.args[0]
                self.assertEqual(call[0], 'ssh')
                self.assertEqual(call[-2], 'deploy@10.0.0.5')
                self.assertEqual(shlex.split(call[-1]), arguments)
                for option in ('BatchMode=yes', 'StrictHostKeyChecking=yes', 'ForwardAgent=no',
                               'GlobalKnownHostsFile=/dev/null', 'UserKnownHostsFile=' + str(known.resolve())):
                    self.assertIn(option, call)
                self.assertEqual(call[1:3], ['-F', '/dev/null'])
                remote.copy([Path(temp) / 'a'], '/srv/x/')
                self.assertEqual(run.call_args.args[0][-1], 'deploy@10.0.0.5:/srv/x/')
            with patch.object(pinned_ssh.subprocess, 'run', return_value=SimpleNamespace(returncode=255, stdout=b'')):
                with self.assertRaises(RuntimeError):
                    remote.command(['true'])
            known.write_text('10.0.0.5 ssh-ed25519 BBBB\n')
            with patch.object(pinned_ssh.subprocess, 'run') as run:
                with self.assertRaisesRegex(ValueError, 'changed'):
                    remote.command(['true'])
                with self.assertRaisesRegex(ValueError, 'changed'):
                    remote.copy([], '/tmp/')
                run.assert_not_called()


class TransparentUnitTests(unittest.TestCase):
    UNIT = ('[Unit]\nDescription=w\n\n[Service]\nMemoryHigh=5905580032\n'
            'ExecStart=/usr/local/bin/transparent-shard-server --shard-dir /p/old --cache-bytes 5368709120 '
            '--worker-id=transparent-pir-recent-01 --runtime-cache-dir /srv/transparent-pir/runtime-cache '
            '--active-record /opt/transparent-publisher/active.json\nMemoryMax=7G\n')

    def test_flags_are_replaced_in_either_spelling_and_appended_only_on_request(self):
        unit, found = transparent_unit.rewrite_exec(self.UNIT, {'--worker-id': 'a3', '--cache-bytes': 7, '--query-slots': 2})
        self.assertEqual(found, {'--worker-id', '--cache-bytes'})
        self.assertIn('--worker-id=a3 ', unit)
        self.assertIn('--cache-bytes 7 ', unit)
        self.assertNotIn('--query-slots', unit)
        unit, _ = transparent_unit.rewrite_exec(self.UNIT, {'--query-slots': 2}, append=True)
        self.assertTrue(transparent_unit.exec_args(unit)[-2:] == ['--query-slots', '2'])
        with self.assertRaisesRegex(ValueError, 'ExecStart'):
            transparent_unit.rewrite_exec('[Service]\n', {'--worker-id': 'x'})

    def test_service_directives_are_set_once_and_directories_found(self):
        unit = transparent_unit.set_service(self.UNIT, {'MemoryMax': '56G', 'MemoryHigh': 51539607552})
        self.assertEqual(unit.count('MemoryMax='), 1)
        self.assertIn('[Service]\nMemoryMax=56G\nMemoryHigh=51539607552\n', unit)
        self.assertIn('MemoryHigh=51539607552', transparent_unit.set_memory_high(self.UNIT, 'archive-owner'))
        self.assertEqual(list(transparent_unit.unit_directories(self.UNIT)),
                         ['/srv/transparent-pir/runtime-cache', '/opt/transparent-publisher'])


if __name__ == '__main__':
    unittest.main()
