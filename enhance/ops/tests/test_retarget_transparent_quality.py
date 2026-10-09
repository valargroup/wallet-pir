import contextlib
import datetime
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('retarget', ROOT / 'scripts/retarget-transparent-quality.py')
retarget = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(retarget)

NOW = 1_791_500_000.0
ROSTER = [{'id': 'transparent-pir-recent-01', 'role': 'recent-replica', 'upstream': '10.0.0.10:8093',
           'ssh_host': '10.0.0.10'},
          {'id': 'transparent-pir-archive-03', 'role': 'archive-owner', 'upstream': '10.0.0.7:8093',
           'ssh_host': '10.0.0.7', 'archive_range': [0, 81]}]


class Runs:
    def __init__(self, stdout='', returncode=0):
        self.calls, self.stdout, self.returncode = [], stdout, returncode

    def __call__(self, command, **kwargs):
        self.calls.append(command)
        return subprocess.CompletedProcess(command, self.returncode, self.stdout, '')


def run_main(argv, runs):
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        code = retarget.main(argv, now=lambda: NOW, run=runs)
    return code, out.getvalue()


class ApmTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        d = self.root = Path(self.directory.name)
        self.apm = d / 'transparent.json'
        self.apm.write_text(json.dumps({'publisher_url': 'http://127.0.0.1:8094/metrics',
                                        'roster': '/opt/transparent-publisher/roster.json',
                                        'synthetic_status': '/opt/transparent-5qps-20260929/status.json',
                                        'host_snapshot': '/var/lib/pir-apm/hosts.json'}))
        self.apm.chmod(0o640)
        self.hosts = d / 'hosts.json'
        self.hosts.write_text(json.dumps({'roster': {'path': '/opt/transparent-publisher/roster.json',
                                                     'unit': 'transparent-shard-server.service'},
                                          'targets': []}))
        self.roster = d / 'roster.json'
        self.roster.write_text(json.dumps(ROSTER))
        self.status = d / 'status.json'
        self.write_status(NOW - 10)

    def tearDown(self):
        self.directory.cleanup()

    def write_status(self, at):
        utc = datetime.datetime.fromtimestamp(at, datetime.timezone.utc).isoformat()
        self.status.write_text(json.dumps({'utc': utc, 'mode': 'running'}))

    def argv(self, *extra):
        return ['apm', '--apm-config', str(self.apm), '--hosts-config', str(self.hosts),
                '--roster', str(self.roster), '--load-status', str(self.status), *extra]

    def test_plan_changes_nothing(self):
        before = self.apm.read_bytes(), self.hosts.read_bytes()
        runs = Runs()
        code, out = run_main(self.argv(), runs)
        self.assertEqual(code, 0)
        self.assertIn('"would_restart": "pir-apm"', out)
        self.assertEqual((self.apm.read_bytes(), self.hosts.read_bytes()), before)
        self.assertEqual(runs.calls, [])

    def test_apply_moves_only_the_two_inputs_and_restarts_apm(self):
        original = json.loads(self.apm.read_text())
        runs = Runs()
        backups = self.root / 'backup'
        code, _ = run_main(self.argv('--apply', '--backup-dir', str(backups)), runs)
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(self.apm.read_text()),
                         {**original, 'roster': str(self.roster), 'synthetic_status': str(self.status)})
        self.assertEqual(self.apm.stat().st_mode & 0o777, 0o640)
        self.assertEqual(json.loads(self.hosts.read_text())['roster'],
                         {'path': str(self.roster), 'unit': 'transparent-shard-server.service'})
        self.assertEqual(runs.calls, [['systemctl', 'restart', 'pir-apm']])
        restored = {json.loads(p.read_text()).get('synthetic_status') for p in backups.iterdir()}
        self.assertIn('/opt/transparent-5qps-20260929/status.json', restored)
        # A second apply would overwrite the original backup; it refuses instead.
        self.apm.write_text(json.dumps(original))
        with self.assertRaises(ValueError):
            run_main(self.argv('--apply', '--backup-dir', str(backups)), Runs())

    def test_stale_status_or_unreadable_roster_refuses(self):
        self.write_status(NOW - 3600)
        with self.assertRaises(ValueError):
            run_main(self.argv(), Runs())
        self.write_status(NOW - 10)
        self.roster.write_text(json.dumps([{'id': 'x', 'role': 'recent', 'upstream': 'h:1', 'ssh_host': 'h'}]))
        with self.assertRaises(ValueError):
            run_main(self.argv(), Runs())


class MonitorTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        d = self.root = Path(self.directory.name)
        self.canary = d / 'quality-canary'
        self.canary.write_bytes(b'new canary')
        self.fixture = d / 'fixture.json'
        self.fixture.write_bytes(b'{"schema":"transparent-shard-v11"}')
        self.pins = d / 'pins.json'
        self.pins.write_text(json.dumps({'fixture_sha256': hashlib.sha256(self.fixture.read_bytes()).hexdigest(),
                                         'anchor_height': 3488499, 'anchor_hash': 'a' * 64}))
        self.probes = d / 'probes.json'
        self.status_probe = {'service': 'status', 'command': ['/opt/status-probe'], 'timeout_seconds': 30}
        self.probes.write_text(json.dumps([self.status_probe, {
            'service': 'transparent', 'timeout_seconds': 30,
            'command': ['/opt/pir-monitor/old/quality-canary', '--url', 'https://transparent-pir.valargroup.dev',
                        '--fixture', '/opt/pir-monitor/old/fixture.json', '--fixture-sha256', '1' * 64,
                        '--rpc-url', 'http://10.0.0.1:8232', '--cookie', '/etc/pir-monitor/rpc-cookie',
                        '--anchor-height', '3494910', '--anchor-hash', '2' * 64]}]))

    def tearDown(self):
        self.directory.cleanup()

    def argv(self, *extra):
        return ['monitor', '--probes-config', str(self.probes), '--canary', str(self.canary),
                '--canary-sha256', hashlib.sha256(b'new canary').hexdigest(), '--fixture', str(self.fixture),
                '--pins', str(self.pins), *extra]

    def test_apply_after_the_new_command_passes(self):
        runs = Runs('{"passed":true,"category":"","duration_seconds":1.2}\n')
        code, _ = run_main(self.argv('--apply', '--backup-dir', str(self.root / 'backup')), runs)
        self.assertEqual(code, 0)
        configs = json.loads(self.probes.read_text())
        self.assertEqual(configs[0], self.status_probe)
        command = configs[1]['command']
        self.assertEqual(command, [str(self.canary), '--url', 'https://transparent-pir.valargroup.dev',
                                   '--fixture', str(self.fixture),
                                   '--fixture-sha256', hashlib.sha256(self.fixture.read_bytes()).hexdigest(),
                                   '--rpc-url', 'http://10.0.0.1:8232', '--cookie', '/etc/pir-monitor/rpc-cookie',
                                   '--anchor-height', '3488499', '--anchor-hash', 'a' * 64])
        self.assertEqual(runs.calls, [command, ['systemctl', 'restart', 'pir-monitor']])

    def test_failing_probe_changes_nothing(self):
        before = self.probes.read_bytes()
        runs = Runs('{"passed":false,"category":"oracle_invalid","duration_seconds":0.1}\n', returncode=1)
        code, _ = run_main(self.argv('--apply', '--backup-dir', str(self.root / 'backup')), runs)
        self.assertEqual(code, 1)
        self.assertEqual(self.probes.read_bytes(), before)
        self.assertEqual(len(runs.calls), 1)

    def test_pinned_artifacts_must_match(self):
        self.canary.write_bytes(b'other canary')
        with self.assertRaises(ValueError):
            run_main(self.argv(), Runs('{"passed":true}\n'))


if __name__ == '__main__':
    unittest.main()
