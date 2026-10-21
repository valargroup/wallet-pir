"""The locked continuous-load script repair through the deploy wrapper; only hosts are fake."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / 'tools/lib'), str(ROOT / 'tools/tests/operations/deploy')]
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors  # noqa: E402
from tools.transparent.lib import load_repair as m  # noqa: E402

OLD_SCRIPT = '/srv/transparent-activity/ops/sources/c9262e84/transparent/ops/scripts/transparent-quality-load.py'
UNIT_TEXT = ('[Service]\nType=simple\nExecStart=/usr/bin/python3 -B %s --root %s --api v2 '
             '--expected-worker-sha256-file %s/pins.json\nNice=10\nCPUQuota=200%%\nRestart=no\n'
             % (OLD_SCRIPT, m.LOAD_ROOT, m.LOAD_ROOT))
WORKERS = {'transparent-pir-v12-archive': 'transparent-v12-archive.service',
           'transparent-pir-v12-recent': 'transparent-v12-recent.service'}
SERVER = b'pir-query-server v12'
NEW = hashlib.sha256(m.SOURCE.read_bytes()).hexdigest()


class LoadRepair(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = Path(self.dir.name)
        self.state, self.request = root / 'state', root / 'request.json'
        self.fake = FakeFleet()
        for unit in WORKERS.values():
            self.fake.install('v12', unit, '[Service]\nExecStart=/usr/local/bin/pir-query-server\n',
                              files={'/usr/local/bin/pir-query-server': SERVER})
        self.files = {'/usr/bin/python3': b'python', OLD_SCRIPT: b'old checker',
                      m.LOAD_ROOT + '/pins.json': json.dumps({i: sha256(SERVER) for i in WORKERS}),
                      m.LOAD_ROOT + '/rate-query': b'rate-query', m.LOAD_ROOT + '/fixture.json': b'{}',
                      m.LOAD_ROOT + '/permit': b'deny\n', m.LOAD_ROOT + '/incidents-20261021T00.jsonl.gz': b'history'}
        self.fake.install('coordinator', m.UNIT, UNIT_TEXT, files=self.files)
        self.fake.systemctl('coordinator', 'stop', m.UNIT)  # the crashed checker
        self.fake.log.clear()
        self.mode = 'running'

        def observer(host, op, detail):
            # The started checker writes its status after two healthy ticks.
            if op == 'systemctl' and detail[:2] == ('restart', m.UNIT):
                self.fake.put(host, m.LOAD_ROOT + '/status.json', json.dumps({'mode': self.mode}))
        self.fake.observer = observer
        self.inventory = root / 'inventory.json'
        self.inventory.write_text(json.dumps({'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
                                              'hosts': {'v12': {}, 'coordinator': {}}, 'services': {}}))
        self.write_request()

    def tearDown(self):
        self.dir.cleanup()

    def write_request(self, change=lambda r: None):
        request = {'schema': m.SCHEMA, 'host': 'coordinator',
                   'previous': {'unit_sha256': sha256(UNIT_TEXT.encode()), 'script_path': OLD_SCRIPT,
                                'script_sha256': sha256(b'old checker')},
                   'script': {'git_sha': '1' * 40, 'sha256': NEW},
                   'load': {'pins_sha256': sha256(self.files[m.LOAD_ROOT + '/pins.json'].encode()),
                            'rate_query_sha256': sha256(b'rate-query'), 'fixture_sha256': sha256(b'{}')},
                   'workers': {i: {'host': 'v12', 'unit': u} for i, u in WORKERS.items()},
                   'exact_check': {'host': 'coordinator', 'argv': ['/usr/local/bin/transparent-exact-probe'], 'timeout': 600}}
        change(request)
        self.request.write_bytes(json.dumps(request).encode())

    def run_cli(self, action, *extra):
        lines, now = [], [0.0]

        def sleep(seconds):
            now[0] += seconds
        code = cli.main(['--inventory', str(self.inventory), '--state-dir', str(self.state),
                         'transparent-load-repair-' + action, '--request', str(self.request),
                         '--request-sha256', hashlib.sha256(self.request.read_bytes()).hexdigest(), *extra],
                        executor=self.fake, out=lines.append, sleep=sleep, clock=lambda: now[0],
                        lock=lambda: self.fake.hold_lock('coordinator', descriptors.LOCK_PATH))
        return code, lines

    def plan_sha(self):
        code, lines = self.run_cli('plan')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.log, [], 'plan is read-only')
        return next(line.split()[-1] for line in lines if line.startswith('plan_sha256: '))

    def preserved(self):
        for path in (OLD_SCRIPT, m.LOAD_ROOT + '/pins.json', m.LOAD_ROOT + '/permit',
                     m.LOAD_ROOT + '/incidents-20261021T00.jsonl.gz'):
            self.assertEqual(self.fake.sha256('coordinator', path), sha256(
                self.files[path] if isinstance(self.files[path], bytes) else self.files[path].encode()), path)

    def test_repair_installs_reviewed_script_and_starts_only_the_load(self):
        code, lines = self.run_cli('preflight')
        self.assertEqual(code, 0, lines)
        self.assertIn('independent encrypted-answer check passed', lines)
        sha, self.fake.runs = self.plan_sha(), []
        code, lines = self.run_cli('deploy', '--expect-plan-sha256', sha)
        self.assertEqual(code, 0, lines)
        unit = self.fake.probe_unit('coordinator', m.UNIT)
        self.assertEqual(unit['active_state'], 'active')
        self.assertEqual(unit['fragment_text'], UNIT_TEXT.replace(OLD_SCRIPT, m.script_path(NEW)))
        self.assertEqual(self.fake.sha256('coordinator', m.script_path(NEW)), NEW)
        self.preserved()
        restarts = [d for h, op, d in self.fake.log if op == 'systemctl' and d[0] != 'daemon-reload']
        self.assertEqual(restarts, [('restart', m.UNIT)])
        self.assertEqual(self.fake.runs, [('coordinator', ['/usr/local/bin/transparent-exact-probe'])])
        self.assertEqual(self.run_cli('status')[0], 0)
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 0, lines)
        unit = self.fake.probe_unit('coordinator', m.UNIT)
        self.assertEqual((unit['active_state'], unit['fragment_text']), ('inactive', UNIT_TEXT))
        self.preserved()

    def test_refuses_latch_changed_binary_and_wrong_plan_without_changes(self):
        sha = self.plan_sha()
        self.fake.put('coordinator', m.LOAD_ROOT + '/latched.json', '{}')
        code, lines = self.run_cli('deploy', '--expect-plan-sha256', sha)
        self.assertTrue(any('critical latch' in line for line in lines), lines)
        self.fake.host('coordinator').files.pop(m.LOAD_ROOT + '/latched.json')
        self.fake.put('v12', '/usr/local/bin/pir-query-server', b'other')
        self.fake.systemctl('v12', 'restart', WORKERS['transparent-pir-v12-recent'])
        code, lines = self.run_cli('deploy', '--expect-plan-sha256', sha)
        self.assertTrue(any('the load pins' in line for line in lines), lines)
        self.fake.put('v12', '/usr/local/bin/pir-query-server', SERVER)
        self.fake.systemctl('v12', 'restart', WORKERS['transparent-pir-v12-recent'])
        code, lines = self.run_cli('deploy', '--expect-plan-sha256', '0' * 64)
        self.assertTrue(any('--expect-plan-sha256' in line for line in lines), lines)
        self.assertEqual([e for e in self.fake.log if e[0] == 'coordinator'], [])
        self.assertEqual(self.fake.probe_unit('coordinator', m.UNIT)['fragment_text'], UNIT_TEXT)

    def test_failed_exact_check_or_unhealthy_load_leaves_previous_unit_stopped(self):
        sha = self.plan_sha()
        self.fake.exact_result = (1, 'decoded row differs')
        code, lines = self.run_cli('deploy', '--expect-plan-sha256', sha)
        self.assertNotEqual(code, 0)
        self.assertEqual(self.fake.probe_unit('coordinator', m.UNIT)['fragment_text'], UNIT_TEXT)
        self.fake.exact_result = (0, 'ok')
        self.mode = 'paused'
        code, lines = self.run_cli('deploy', '--expect-plan-sha256', sha)
        self.assertNotEqual(code, 0)
        unit = self.fake.probe_unit('coordinator', m.UNIT)
        self.assertEqual((unit['active_state'], unit['fragment_text']), ('inactive', UNIT_TEXT))
        self.preserved()


if __name__ == '__main__':
    unittest.main()
