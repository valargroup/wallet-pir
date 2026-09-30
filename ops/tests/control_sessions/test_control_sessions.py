"""The control-session supervisor, its restricted account model, the Status
configuration that replaces the two tunnel units, and Transparent's use of the
shared helpers. A stub `ssh` (fake_ssh.py) stands in for OpenSSH; nothing here
contacts a host. `RealSshdTest` runs only with WALLET_PIR_SSHD_PROBE=1, against
a private sshd on 127.0.0.1."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'ops/lib'))
from wallet_pir_ops import control_sessions as cs, pinned_ssh  # noqa: E402

FAKE_SSH = Path(__file__).resolve().parent / 'fake_ssh.py'
EXAMPLE = ROOT / 'ops/deploy/control-sessions.status.example.json'
UNITS = {'status-control': ROOT / 'enhance/ops/deploy/status-control-tunnel.service.in',
         'status-query': ROOT / 'enhance/ops/deploy/status-query-tunnel.service.in'}
CLOUD_INIT = ROOT / 'ops/infra/digitalocean/production/status/cloud-init.yaml.tftpl'
PUBLIC_KEY = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOVJfyTmLRUrVY7xJ3LvsjGEEBPy/b0TkM5A2HpVRZl9 status-control'


def status_example(host='10.124.0.7', pin='a' * 64):
    text = EXAMPLE.read_text().replace('@STATUS_HOST@', host).replace('@STATUS_KNOWN_HOSTS_SHA256@', pin)
    return json.loads(text)


def free_port():
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]


def wait_for(predicate, timeout=10, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError('condition not reached')


def accept_forever(listener, reply):
    """Answer every connection with `reply` until `listener` is closed."""
    while True:
        try:
            connection, _ = listener.accept()
            with connection:
                connection.sendall(reply)
        except OSError:
            if listener.fileno() == -1:
                return


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


class HelperTest(unittest.TestCase):
    def test_socket_name_is_transparents_digest(self):
        identity = ['owner-1', '10.0.0.5', '/opt/transparent-publisher/credentials/known_hosts',
                    '/opt/transparent-publisher/credentials/deploy-ssh']
        self.assertEqual(cs.socket_name('c-', identity), 'c-bcd07ff0c0cd8aff86983ef7fe6b835b')

    def test_forward_and_master_arguments(self):
        self.assertEqual(cs.forward_args(), [])
        self.assertEqual(cs.forward_args(local=[('127.0.0.1:1', '127.0.0.1:2')], reverse=[('127.0.0.1:3', '127.0.0.1:4')],
                                         unix=[('/l.sock', '/r.sock')]),
                         ['-oStreamLocalBindMask=0177', '-oExitOnForwardFailure=yes', '-L', '127.0.0.1:1:127.0.0.1:2',
                          '-R', '127.0.0.1:3:127.0.0.1:4', '-L', '/l.sock:/r.sock'])
        self.assertEqual(cs.master_args(['ssh'], '/c', 'u@h', 2, 3, ['-X']),
                         ['ssh', '-oControlMaster=yes', '-oControlPersist=no', '-oControlPath=/c',
                          '-oServerAliveInterval=2', '-oServerAliveCountMax=3', '-N', '-X', 'u@h'])
        self.assertEqual(cs.client_args(['ssh'], Path('/c')),
                         ['ssh', '-oControlMaster=no', '-oProxyCommand=false', '-oControlPath=/c'])

    def test_backoff_doubles_to_its_bound_and_resets_after_a_stable_run(self):
        backoff = cs.Backoff(1, 8, 30)
        self.assertEqual([backoff.after_exit(0) for _ in range(6)], [1, 2, 4, 8, 8, 8])
        self.assertEqual(backoff.after_exit(30), 1)
        self.assertEqual(backoff.after_exit(29), 2)

    def test_stale_socket_is_cleared_but_other_paths_are_untouched(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            path = Path(directory) / 's'
            dead = socket.socket(socket.AF_UNIX)
            dead.bind(str(path))
            dead.close()
            cs.clear_stale_socket(path, 'control path')
            self.assertFalse(path.exists())
            path.write_text('preserve')
            with self.assertRaisesRegex(cs.Blocked, 'not a socket'):
                cs.clear_stale_socket(path, 'control path')
            self.assertEqual(path.read_text(), 'preserve')
            path.unlink()
            (Path(directory) / 'target').write_text('x')
            path.symlink_to(Path(directory) / 'target')
            with self.assertRaisesRegex(cs.Blocked, 'not a socket'):
                cs.clear_stale_socket(path, 'control path')
            self.assertTrue(path.is_symlink())
            path.unlink()
            live = socket.socket(socket.AF_UNIX)
            live.bind(str(path))
            live.listen(1)
            try:
                with self.assertRaisesRegex(cs.Blocked, 'live process'):
                    cs.clear_stale_socket(path, 'control path')
                self.assertTrue(path.is_socket())
            finally:
                live.close()


class ConfigTest(unittest.TestCase):
    def test_status_example_reproduces_both_tunnel_units(self):
        config = cs.Config.parse(status_example())
        sessions = {s.name: s for s in config.sessions}
        self.assertEqual(set(sessions), set(UNITS))
        for name, unit in UNITS.items():
            with self.subTest(unit=unit.name):
                line = next(l for l in unit.read_text().splitlines() if l.startswith('ExecStart='))
                argv = shlex.split(line.removeprefix('ExecStart=').replace('@STATUS_HOST@', '10.124.0.7'))
                self.assertEqual(argv[0], '/usr/bin/ssh')
                options = dict(argv[i + 1].split('=', 1) for i, a in enumerate(argv) if a == '-o')
                session = sessions[name]
                self.assertEqual(argv[-1], session.destination)
                self.assertEqual(argv[argv.index('-i') + 1], session.identity_file)
                self.assertEqual(options['UserKnownHostsFile'], session.known_hosts)
                self.assertEqual(options['StrictHostKeyChecking'], 'yes')
                self.assertEqual(options['IdentitiesOnly'], 'yes')
                self.assertEqual(options['ExitOnForwardFailure'], 'yes')
                self.assertEqual(int(options['ServerAliveInterval']), session.server_alive_interval)
                self.assertEqual(int(options['ServerAliveCountMax']), session.server_alive_count_max)
                self.assertEqual('-C' in argv, session.compression)
                self.assertIn('-N', argv)
                def split(spec):
                    parts = spec.split(':')
                    return ':'.join(parts[:2]), ':'.join(parts[2:])
                self.assertEqual([split(argv[i + 1]) for i, a in enumerate(argv) if a == '-L'], list(session.local_forwards))
                self.assertEqual([split(argv[i + 1]) for i, a in enumerate(argv) if a == '-R'], list(session.reverse_forwards))
                self.assertEqual(session.unix_forwards, ())
                # The supervisor's own master carries the same forwards and options.
                master = session.master_args(config.runtime_dir)
                for flag in ('-L', '-R'):
                    self.assertEqual([master[i + 1] for i, a in enumerate(master) if a == flag],
                                     [argv[i + 1] for i, a in enumerate(argv) if a == flag])
                for option in ('StrictHostKeyChecking=yes', 'IdentitiesOnly=yes',
                               'UserKnownHostsFile=' + session.known_hosts):
                    self.assertIn(option, master)
                self.assertIn('-oExitOnForwardFailure=yes', master)
                self.assertIn(f'-oServerAliveCountMax={options["ServerAliveCountMax"]}', master)

    def test_status_master_arguments(self):
        config = cs.Config.parse(status_example())
        common = ['ssh', '-F', '/dev/null', '-i', '/etc/status-pir-control/id_ed25519', '-o', 'BatchMode=yes',
                  '-o', 'IdentitiesOnly=yes', '-o', 'ForwardAgent=no', '-o', 'ConnectTimeout=10',
                  '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=/etc/status-pir-control/known_hosts',
                  '-o', 'GlobalKnownHostsFile=/dev/null']
        control, query = config.sessions
        self.assertEqual(control.master_args(config.runtime_dir), common + [
            '-oCompression=yes', '-oControlMaster=yes', '-oControlPersist=no',
            '-oControlPath=/run/wallet-pir-control-sessions/c-fe1630be37e16c83c38ef7217c48b8d0',
            '-oServerAliveInterval=2', '-oServerAliveCountMax=4', '-N', '-oExitOnForwardFailure=yes',
            '-L', '127.0.0.1:8481:127.0.0.1:8481', '-L', '127.0.0.1:8482:127.0.0.1:8482',
            '-R', '127.0.0.1:8495:127.0.0.1:8480', 'status-control@10.124.0.7'])
        self.assertEqual(query.master_args(config.runtime_dir), common + [
            '-oControlMaster=yes', '-oControlPersist=no',
            '-oControlPath=/run/wallet-pir-control-sessions/c-4521f885e4dea936d78a21a8e310f634',
            '-oServerAliveInterval=2', '-oServerAliveCountMax=4', '-N', '-oExitOnForwardFailure=yes',
            '-L', '127.0.0.1:8492:127.0.0.1:8484', 'status-control@10.124.0.7'])
        self.assertEqual(control.client_args(config.runtime_dir)[-3:],
                         ['-oControlMaster=no', '-oProxyCommand=false',
                          '-oControlPath=' + str(control.control_path(config.runtime_dir))])

    def test_invalid_configurations_are_refused(self):
        def session(**changes):
            value = status_example()
            value['sessions'][1].update(changes)
            return value
        cases = {
            'placeholder': json.loads(EXAMPLE.read_text()),
            'schema': {**status_example(), 'schema': 'other'},
            'unknown field': session(proxy_jump='x'),
            'option injection': session(destination='-oProxyCommand=x@host'),
            'public listen': session(local_forwards=[['0.0.0.0:8492', '127.0.0.1:8484']]),
            'hostname target': session(local_forwards=[['127.0.0.1:8492', 'localhost:8484']]),
            'port range': session(local_forwards=[['127.0.0.1:70000', '127.0.0.1:8484']]),
            'duplicate listen': session(local_forwards=[['127.0.0.1:8481', '127.0.0.1:8484']]),
            'unix colon': session(unix_forwards=[['/run/x:y', '/run/control.sock']]),
            'relative key': session(identity_file='id_ed25519'),
            'bad pin': session(known_hosts_sha256='ABC'),
            'no sessions': {**status_example(), 'sessions': []},
        }
        for name, value in cases.items():
            with self.subTest(name), self.assertRaises(ValueError):
                cs.Config.parse(value)


class AccountTest(unittest.TestCase):
    def test_generator_reproduces_the_status_cloud_init_key_line(self):
        installed = next(l.strip() for l in CLOUD_INIT.read_text().splitlines() if l.strip().startswith('restrict,'))
        installed = installed.replace('${control_public_key}', PUBLIC_KEY)
        config = cs.Config.parse(status_example())
        self.assertEqual(cs.authorized_keys_line(config.sessions, PUBLIC_KEY), installed)
        self.assertEqual(cs.authorized_keys_line(config.sessions, PUBLIC_KEY + '\n'), installed)

    def test_match_block_repeats_limits_and_disables_unix_socket_forwarding(self):
        config = cs.Config.parse(status_example())
        block = cs.sshd_match_block('status-control', config.sessions, '/etc/ssh/status-control/authorized_keys')
        lines = [l.strip() for l in block.splitlines()]
        for expected in ['Match User status-control', 'AuthorizedKeysFile /etc/ssh/status-control/authorized_keys',
                         'AllowTcpForwarding yes', 'AllowStreamLocalForwarding no',
                         'PermitOpen 127.0.0.1:8481 127.0.0.1:8482 127.0.0.1:8484', 'PermitListen 127.0.0.1:8495',
                         'AllowAgentForwarding no', 'X11Forwarding no', 'PermitTTY no', 'ForceCommand /bin/false',
                         'ClientAliveInterval 2', 'ClientAliveCountMax 3']:
            self.assertIn(expected, lines)
        local_only = cs.sshd_match_block('status-control', config.sessions[1:], '/k')
        self.assertIn('    AllowTcpForwarding local', local_only.splitlines())
        self.assertIn('    PermitListen none', local_only.splitlines())

    def test_unrestrictable_or_malformed_grants_are_refused(self):
        unix = cs.Session.parse(dict(name='t', destination='pir-control@10.0.0.5', identity_file='/k', known_hosts='/h',
                                     unix_forwards=[['/run/w/s.sock', '/run/transparent-pir/control.sock']]))
        reverse_only = cs.Session.parse(dict(name='r', destination='pir-control@10.0.0.5', identity_file='/k',
                                             known_hosts='/h', reverse_forwards=[['127.0.0.1:8495', '127.0.0.1:8480']]))
        config = cs.Config.parse(status_example())
        with self.assertRaisesRegex(ValueError, 'loopback TCP'):
            cs.authorized_keys_line([unix], PUBLIC_KEY)
        with self.assertRaisesRegex(ValueError, 'loopback TCP'):
            cs.sshd_match_block('pir-control', [unix], '/k')
        with self.assertRaisesRegex(ValueError, 'none'):
            cs.authorized_keys_line([reverse_only], PUBLIC_KEY)
        self.assertIn('    PermitOpen none', cs.sshd_match_block('pir-control', [reverse_only], '/k').splitlines())
        for key in [PUBLIC_KEY + '\nssh-ed25519 AAAA other', 'command="x" ' + PUBLIC_KEY, 'ssh-dss AAAA', '']:
            with self.subTest(key=key), self.assertRaises(ValueError):
                cs.authorized_keys_line(config.sessions, key)
        with self.assertRaises(ValueError):
            cs.sshd_match_block('root user', config.sessions, '/k')


class SupervisorTest(unittest.TestCase):
    """The supervisor against the stub `ssh` on PATH."""

    def setUp(self):
        self.directory = Path(tempfile.mkdtemp(dir='/tmp', prefix='cs-'))
        self.addCleanup(shutil.rmtree, self.directory, True)
        bin_dir = self.directory / 'bin'
        bin_dir.mkdir()
        (bin_dir / 'ssh').write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(FAKE_SSH))} "$@"\n')
        (bin_dir / 'ssh').chmod(0o755)
        self.log = self.directory / 'ssh.log'
        environment = dict(PATH=f'{bin_dir}:{os.environ["PATH"]}', FAKE_SSH_LOG=str(self.log),
                           FAKE_SSH_STATE=str(self.directory), FAKE_SSH_FAILURES='0')
        saved = {k: os.environ.get(k) for k in environment}
        os.environ.update(environment)
        self.addCleanup(lambda: [os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
                                 for k, v in saved.items()])
        self.known_hosts = self.directory / 'known_hosts'
        self.known_hosts.write_text('10.124.0.7 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHostKeyFixture\n')
        self.ports = [free_port(), free_port(), free_port()]

    def config(self, **changes):
        run = self.directory / 'run'
        session = lambda name, forwards, **extra: dict(
            name=name, destination='status-control@10.124.0.7', identity_file='/etc/status-pir-control/id_ed25519',
            known_hosts=str(self.known_hosts), known_hosts_sha256=pinned_ssh.sha256(self.known_hosts.read_bytes()),
            server_alive_interval=2, server_alive_count_max=4, local_forwards=forwards, **extra)
        value = dict(schema=cs.SCHEMA, runtime_dir=str(run), status_file=str(run / 'status.json'),
                     restart_initial_seconds=0.05, restart_max_seconds=0.2, stable_seconds=30,
                     sessions=[session('status-control', [[f'127.0.0.1:{self.ports[0]}', '127.0.0.1:8481'],
                                                          [f'127.0.0.1:{self.ports[1]}', '127.0.0.1:8482']],
                                       reverse_forwards=[['127.0.0.1:8495', '127.0.0.1:8480']], compression=True),
                               session('status-query', [[f'127.0.0.1:{self.ports[2]}', '127.0.0.1:8484']])])
        value.update(changes)
        return cs.Config.parse(value)

    def start(self, config):
        supervisor = cs.Supervisor(config, log=io.StringIO(), poll_interval=0.02)
        thread = threading.Thread(target=supervisor.run, daemon=True)
        thread.start()

        def stop():
            supervisor.stop.set()
            thread.join(10)
            self.assertFalse(thread.is_alive())
        self.addCleanup(stop)
        return supervisor, stop

    def status(self, config):
        try:
            return json.loads(config.status_file.read_text())['sessions']
        except (OSError, ValueError):
            return {}

    def invocations(self):
        return [json.loads(l) for l in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_concurrent_fake_starts_count_every_refused_connection(self):
        count = 32
        os.environ['FAKE_SSH_FAILURES'] = str(count)
        processes = [subprocess.Popen([sys.executable, str(FAKE_SSH),
                                      f'-oControlPath={self.directory}/unused', 'fixture@127.0.0.1'],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                     for _ in range(count)]
        self.addCleanup(lambda: [process.kill() for process in processes if process.poll() is None])
        self.assertEqual([process.wait(timeout=10) for process in processes], [255] * count)
        self.assertEqual(int((self.directory / 'starts').read_text()), count)

    def test_injected_clock_exercises_supervisor_backoff_without_sleep(self):
        elapsed, waits = [0.0], []
        lifetimes = iter([0, 0, 0, 0, 30, 29])
        supervisor = cs.Supervisor(self.config(), log=io.StringIO(),
                                   clock=lambda: elapsed[0], monotonic=lambda: elapsed[0])
        class Stop:
            def is_set(self):
                return len(waits) == 6
            def wait(self, delay):
                waits.append(delay)
                elapsed[0] += delay
                return self.is_set()
        supervisor.stop = Stop()
        def attempt(session):
            elapsed[0] += next(lifetimes)
            return 'backoff', 255, 'fixture'
        from unittest.mock import patch
        with patch.object(supervisor, '_attempt', side_effect=attempt), patch.object(supervisor, '_update'):
            supervisor._supervise(supervisor.config.sessions[0])
        self.assertEqual(waits, [0.05, 0.1, 0.2, 0.2, 0.05, 0.1])

    def test_restart_backoff_status_file_and_check(self):
        os.environ['FAKE_SSH_FAILURES'] = '3'
        config = self.config()
        ok, _ = cs.check(config)
        self.assertFalse(ok)
        supervisor, stop = self.start(config)
        sessions = wait_for(lambda: (s := self.status(config)) and all(
            v['state'] == 'connected' for v in s.values()) and s)
        masters = [i for i in self.invocations() if '-O' not in i['args']]
        # Three refused connections (shared by both sessions), then one master each.
        self.assertEqual(len(masters), 5)
        self.assertEqual(sum(v['restarts'] for v in sessions.values()), 3)
        by_args = {tuple(s.master_args(config.runtime_dir)[1:]): s.name for s in config.sessions}
        self.assertEqual({tuple(i['args']) for i in masters}, set(by_args))
        for name, state in sessions.items():
            self.assertTrue(alive(state['pid']))
            self.assertIsNotNone(state['connected_since'])
            self.assertIsNone(state['next_start_in_seconds'])
            if state['restarts']:
                self.assertEqual(state['last_exit_code'], 255)
                self.assertIn('Connection refused', state['last_error'])
        self.assertEqual(config.status_file.stat().st_mode & 0o777, 0o640)
        ok, report = cs.check(config)
        self.assertTrue(ok, report)
        self.assertEqual([f['kind'] for f in report[0]['forwards']], ['local', 'local', 'reverse'])
        self.assertTrue(all(entry['known_hosts_pinned'] for entry in report))
        # A killed master is restarted; its supervisor keeps running.
        pid = sessions['status-query']['pid']
        os.kill(pid, 15)
        wait_for(lambda: (s := self.status(config)) and s['status-query']['state'] == 'connected'
                 and s['status-query']['pid'] != pid)
        pids = [v['pid'] for v in self.status(config).values()]
        stop()
        self.assertEqual({v['state'] for v in self.status(config).values()}, {'stopped'})
        for pid in pids:
            wait_for(lambda: not alive(pid))
        self.assertFalse(cs.check(config)[0])

    def test_a_live_process_on_a_session_path_is_never_touched(self):
        config = self.config()
        config.runtime_dir.mkdir()
        holder = socket.socket(socket.AF_UNIX)
        path = config.sessions[1].control_path(config.runtime_dir)
        holder.bind(str(path))
        holder.listen(1)
        self.addCleanup(holder.close)
        # A live holder accepts: macOS refuses connections to a full backlog.
        threading.Thread(target=accept_forever, args=(holder, b''), daemon=True).start()
        self.start(config)
        state = wait_for(lambda: (s := self.status(config)) and s['status-query']['state'] == 'blocked'
                         and s['status-control']['state'] == 'connected' and s['status-query'])
        self.assertIn('did not start', state['last_error'])
        self.assertEqual([i for i in self.invocations() if '-oControlPath=' + str(path) in i['args']], [])
        self.assertTrue(cs.socket_listening(path))

    def test_known_hosts_change_blocks_the_session(self):
        config = self.config()
        self.known_hosts.write_text('10.124.0.7 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIReplacedHostKey\n')
        self.start(config)
        wait_for(lambda: (s := self.status(config)) and all(
            v['state'] == 'blocked' and 'pinned digest' in v['last_error'] for v in s.values()))
        self.assertEqual(self.invocations(), [])

    def test_one_supervisor_per_runtime_directory(self):
        config = self.config()
        self.start(config)
        wait_for(lambda: (s := self.status(config)) and all(v['state'] == 'connected' for v in s.values()))
        with self.assertRaisesRegex(RuntimeError, 'another control-session supervisor'):
            cs.Supervisor(config, log=io.StringIO()).run()

    def test_command_line_check_and_account_output(self):
        path = self.directory / 'control-sessions.json'
        path.write_text(json.dumps(status_example()))
        key = self.directory / 'key.pub'
        key.write_text(PUBLIC_KEY + '\n')
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(cs.main(['--config', str(path), 'authorized-keys', '--user', 'status-control',
                                      '--public-key', str(key)]), 0)
            self.assertEqual(cs.main(['--config', str(path), 'sshd-match', '--user', 'status-control',
                                      '--authorized-keys-file', '/etc/ssh/status-control/authorized_keys']), 0)
            self.assertEqual(cs.main(['--config', str(path), 'validate']), 0)
        self.assertIn('permitlisten="127.0.0.1:8495" ' + PUBLIC_KEY, output.getvalue())
        self.assertIn('AllowStreamLocalForwarding no', output.getvalue())
        config = self.config()
        path.write_text(json.dumps(dict(schema=cs.SCHEMA, runtime_dir=str(config.runtime_dir),
                                        sessions=[dict(name='q', destination='status-control@10.124.0.7',
                                                       identity_file='/k', known_hosts=str(self.known_hosts),
                                                       local_forwards=[[f'127.0.0.1:{self.ports[2]}',
                                                                        '127.0.0.1:8484']])])))
        with contextlib.redirect_stdout(io.StringIO()) as report:
            self.assertEqual(cs.main(['--config', str(path), 'check']), 1)
        self.assertFalse(json.loads(report.getvalue())['ok'])
        script = subprocess.run([sys.executable, str(ROOT / 'ops/scripts/wallet-pir-control-sessions.py'),
                                 '--config', str(path), 'validate'], capture_output=True, text=True)
        self.assertEqual(script.returncode, 0, script.stderr)
        self.assertIn('status-control@10.124.0.7', script.stdout)


class TransparentTest(unittest.TestCase):
    """transparent-live-fleet.py and the shared helpers build the same masters.

    The live reconciler stays a standalone script on the coordinator, so it
    keeps its own copy of these few lines; this pins the two together.
    """

    KNOWN_HOSTS = '/opt/transparent-publisher/credentials/known_hosts'
    KEY = '/opt/transparent-publisher/credentials/deploy-ssh'
    DIRECT = ['ssh', '-oBatchMode=yes', '-oConnectTimeout=3', '-oStrictHostKeyChecking=yes',
              '-o', 'UserKnownHostsFile=' + KNOWN_HOSTS, '-i', KEY]
    CONTROL = '/opt/transparent-publisher/state/ssh/c-bcd07ff0c0cd8aff86983ef7fe6b835b'
    FORWARD = '/opt/transparent-publisher/state/ssh/s-6965c9a33f9987916b7a9289cb82ab35'
    # Captured from transparent-live-fleet.py.
    MASTER = DIRECT + ['-oControlMaster=yes', '-oControlPersist=no', '-oControlPath=' + CONTROL,
                       '-oServerAliveInterval=2', '-oServerAliveCountMax=3', '-N', 'root@10.0.0.5']
    MASTER_FORWARDING = MASTER[:-1] + ['-oStreamLocalBindMask=0177', '-oExitOnForwardFailure=yes', '-L',
                                       FORWARD + ':/run/transparent-pir/control.sock', 'root@10.0.0.5']
    CLIENT = DIRECT + ['-oControlMaster=no', '-oProxyCommand=false', '-oControlPath=' + CONTROL]

    def load(self, path, name):
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_live_fleet_arguments_are_unchanged(self):
        import asyncio
        from unittest.mock import patch
        live = self.load(ROOT / 'transparent/ops/scripts/transparent-live-fleet.py', 'live_fleet_cs')
        worker = dict(id='owner-1', role='archive-owner', ssh_host='10.0.0.5', upstream='10.0.0.5:8093')
        with tempfile.TemporaryDirectory() as directory:
            roster = Path(directory) / 'roster.json'
            roster.write_text(json.dumps([worker]))
            for forwarding, expected in ((False, self.MASTER), (True, self.MASTER_FORWARDING)):
                fleet = live.Fleet(dict(roster=str(roster), state_dir=directory, known_hosts=self.KNOWN_HOSTS,
                                        ssh_key=self.KEY, control_sessions=True, status_socket_forwarding=forwarding))
                fleet.control_dir = Path('/opt/transparent-publisher/state/ssh')
                captured = []

                async def spawn(*args, **kwargs):
                    captured.append(list(args))
                    raise RuntimeError('captured')

                async def session():
                    with patch.object(live.asyncio, 'create_subprocess_exec', spawn):
                        with self.assertRaisesRegex(RuntimeError, 'captured'):
                            await fleet.control_session(worker)
                asyncio.run(session())
                self.assertEqual(captured, [expected])
                self.assertEqual(fleet.control_session_args(worker), self.CLIENT)
                self.assertEqual(str(fleet.status_forward_path(worker)), self.FORWARD)

    def test_shared_helpers_build_the_same_arguments(self):
        identity = ['owner-1', '10.0.0.5', self.KNOWN_HOSTS, self.KEY]
        control = '/opt/transparent-publisher/state/ssh/' + cs.socket_name('c-', identity)
        self.assertEqual(control, self.CONTROL)
        forward = '/opt/transparent-publisher/state/ssh/' + cs.socket_name(
            's-', [control, '/run/transparent-pir/control.sock'])
        self.assertEqual(forward, self.FORWARD)
        self.assertEqual(cs.master_args(self.DIRECT, control, 'root@10.0.0.5', 2, 3), self.MASTER)
        self.assertEqual(
            cs.master_args(self.DIRECT, control, 'root@10.0.0.5', 2, 3,
                           cs.forward_args(unix=[(forward, '/run/transparent-pir/control.sock')])),
            self.MASTER_FORWARDING)
        self.assertEqual(cs.client_args(self.DIRECT, control), self.CLIENT)


@unittest.skipUnless(os.environ.get('WALLET_PIR_SSHD_PROBE') == '1' and Path('/usr/sbin/sshd').exists(),
                     'set WALLET_PIR_SSHD_PROBE=1 to run a private sshd on 127.0.0.1')
class RealSshdTest(unittest.TestCase):
    """The generated account against a real sshd, and the supervisor with real
    ssh. sshd runs as the current user on a loopback port; the Match block
    applies to that user."""

    def setUp(self):
        self.directory = Path(tempfile.mkdtemp(dir='/tmp', prefix='cs-sshd-'))
        self.addCleanup(shutil.rmtree, self.directory, True)
        d = self.directory
        for name in ('host', 'user'):
            subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(d / name)], check=True)
        self.port = free_port()
        self.user = os.environ.get('USER') or os.getlogin()
        host_key = ' '.join((d / 'host.pub').read_text().split()[:2])
        (d / 'known_hosts').write_text(f'[127.0.0.1]:{self.port} {host_key}\n')
        self.targets = {name: self.serve_tcp(name) for name in ('allowed', 'other', 'reverse-target')}
        self.unix_target = d / 't.sock'
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(self.unix_target))
        listener.listen(4)
        threading.Thread(target=accept_forever, args=(listener, b'unix\n'), daemon=True).start()
        self.local, self.reverse = free_port(), free_port()
        self.session = dict(name='probe', destination=f'{self.user}@127.0.0.1', port=self.port,
                            identity_file=str(d / 'user'), known_hosts=str(d / 'known_hosts'),
                            known_hosts_sha256=pinned_ssh.sha256((d / 'known_hosts').read_bytes()),
                            local_forwards=[[f'127.0.0.1:{self.local}', f'127.0.0.1:{self.targets["allowed"]}']],
                            reverse_forwards=[[f'127.0.0.1:{self.reverse}',
                                               f'127.0.0.1:{self.targets["reverse-target"]}']])
        self.config = cs.Config.parse(dict(schema=cs.SCHEMA, runtime_dir=str(d / 'run'), sessions=[self.session],
                                           restart_initial_seconds=0.2, restart_max_seconds=1))
        sessions = list(self.config.sessions)
        (d / 'authorized_keys').write_text(
            cs.authorized_keys_line(sessions, (d / 'user.pub').read_text()) + '\n')
        (d / 'sshd_config').write_text(
            f'Port {self.port}\nListenAddress 127.0.0.1\nHostKey {d}/host\nPidFile {d}/sshd.pid\n'
            'UsePAM no\nStrictModes no\nPasswordAuthentication no\nKbdInteractiveAuthentication no\n'
            + cs.sshd_match_block(self.user, sessions, str(d / 'authorized_keys')))
        subprocess.run(['/usr/sbin/sshd', '-t', '-f', str(d / 'sshd_config')], check=True)
        self.sshd = subprocess.Popen(['/usr/sbin/sshd', '-D', '-e', '-f', str(d / 'sshd_config')],
                                     stderr=subprocess.DEVNULL)
        self.addCleanup(self.sshd.wait)
        self.addCleanup(self.sshd.terminate)
        wait_for(lambda: self.connectable(self.port))

    def serve_tcp(self, name):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(4)
        self.addCleanup(listener.close)
        threading.Thread(target=accept_forever, args=(listener, name.encode() + b'\n'), daemon=True).start()
        return listener.getsockname()[1]

    @staticmethod
    def connectable(port):
        try:
            socket.create_connection(('127.0.0.1', port), 1).close()
            return True
        except OSError:
            return False

    @staticmethod
    def read(address):
        family = socket.AF_UNIX if isinstance(address, str) else socket.AF_INET
        with socket.socket(family) as connection:
            connection.settimeout(3)
            connection.connect(address)
            return connection.recv(64).strip().decode()

    def test_supervised_master_forwards_only_what_the_account_permits(self):
        supervisor = cs.Supervisor(self.config, log=io.StringIO(), poll_interval=0.05)
        thread = threading.Thread(target=supervisor.run, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 10)
        self.addCleanup(supervisor.stop.set)
        wait_for(lambda: supervisor.sessions['probe']['state'] == 'connected', 15)
        wait_for(lambda: self.connectable(self.reverse), 5)
        self.assertEqual(self.read(('127.0.0.1', self.local)), 'allowed')
        self.assertEqual(self.read(('127.0.0.1', self.reverse)), 'reverse-target')
        self.assertTrue(cs.check(self.config)[0])
        # Anything else, over a client using the same key: refused by sshd.
        session = cs.Session.parse(self.session)
        refused_tcp, refused_listen = free_port(), free_port()
        unix_local = str(self.directory / 'u.sock')
        unix_remote = str(self.directory / 'r.sock')
        probe = subprocess.Popen(
            session.base_args() + ['-N', '-oExitOnForwardFailure=no',
                                   '-L', f'127.0.0.1:{refused_tcp}:127.0.0.1:{self.targets["other"]}',
                                   '-L', f'{unix_local}:{self.unix_target}',
                                   '-R', f'{unix_remote}:127.0.0.1:{self.targets["allowed"]}',
                                   '-R', f'127.0.0.1:{refused_listen}:127.0.0.1:{self.targets["allowed"]}',
                                   session.destination], stderr=subprocess.DEVNULL)
        self.addCleanup(probe.wait)
        self.addCleanup(probe.terminate)
        wait_for(lambda: self.connectable(refused_tcp) and Path(unix_local).exists(), 10)
        time.sleep(0.5)
        for address in (('127.0.0.1', refused_tcp), unix_local):
            with self.subTest(address=address):
                self.assertEqual(self.read(address), '')
        self.assertFalse(Path(unix_remote).exists())
        self.assertFalse(self.connectable(refused_listen))


if __name__ == '__main__':
    unittest.main()
