"""Worker binary upgrade of the v12 history fleet through the deploy wrapper; only hosts are fake."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / 'ops/lib'), str(ROOT / 'ops/tests/deploy')]
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors  # noqa: E402
from transparent.ops.lib import binary_upgrade  # noqa: E402

OLD, NEW = b'transparent-shard-server v12', b'transparent-shard-server next'
SERVER = '/usr/local/bin/transparent-shard-server'
WORKERS = {'transparent-v12-archive.service': '10.142.0.9:8093', 'transparent-v12-recent.service': '10.142.0.9:8193'}
CONTROLLER = 'transparent-v12-publish-controller.service'
CONFIG_PATH, FLEET, SCRIPT = ('/opt/transparent-publisher/v12/controller.json', '/opt/transparent-publisher/v12/fleet.json',
                              '/srv/transparent-activity/ops/sources/v12-ff27/transparent/ops/scripts/transparent-live-fleet.py')
CONFIG = {'data_dir': '/srv/transparent-activity/full-v3/journal', 'follow_journal': False, 'fleet_command': SCRIPT,
          'fleet_config': FLEET, 'publication_root': '/srv/transparent-activity/full-v12/publications',
          'source_sha': 'fb90519b'}
GIT = '1' * 40


def identity(unit):
    return {'publication_sha256': 'ab' * 32, 'assignment': unit.split('-')[2].split('.')[0]}


class BinaryUpgrade(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = Path(self.dir.name)
        self.state, self.binary = root / 'state', root / 'next'
        self.binary.write_bytes(NEW)
        self.fake = FakeFleet()
        self.shift = {}  # unit -> identity a restarted NEW worker reports instead
        for unit, listen in WORKERS.items():
            self.fake.install('v12', unit, '[Service]\nExecStart=%s serve --listen %s --cache-bytes 64\nMemoryMax=8G\n'
                              % (SERVER, listen), files={SERVER: OLD})
            for path in ('/v1/health', '/v1/ready'):
                self.fake.endpoints[('v12', 'http://%s%s' % (listen, path))] = unit
            self.fake.health_shapes[('v12', unit)] = lambda ready, exe, unit=unit: {
                'ready': ready, **(self.shift.get(unit) if exe == sha256(NEW) and unit in self.shift else identity(unit))}
        self.fake.install('coordinator', CONTROLLER, '[Service]\nExecStart=/usr/local/bin/controller --config %s\n' % CONFIG_PATH,
                          files={CONFIG_PATH: json.dumps(CONFIG), FLEET: '{"assignments": []}', SCRIPT: '#!python',
                                 '/usr/local/bin/controller': b'controller'})
        self.inventory = root / 'inventory.json'
        self.inventory.write_text(json.dumps({
            'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
            'hosts': {'v12': {}, 'coordinator': {}},
            'services': {'transparent-history': {'roles': {
                'worker': [{'host': 'v12', 'unit': u, 'vars': {'listen': l}} for u, l in WORKERS.items()],
                'controller': [{'host': 'coordinator', 'unit': CONTROLLER, 'vars': {'listen': '10.142.0.3:8096'},
                                'responsibilities': ['journal-writer']}]}}}}))
        self.request = root / 'request.json'
        self.write_request()

    def tearDown(self):
        self.dir.cleanup()

    def write_request(self, change=lambda r: None):
        state = self.fake.probe_unit('coordinator', CONTROLLER)
        request = {
            'schema': binary_upgrade.SCHEMA, 'service': 'transparent-history',
            'release': {'git_sha': GIT, 'binary_sha256': sha256(NEW)},
            'publication': {'source_sha': 'fb90519b'},
            'controller': {'host': 'coordinator', 'unit_sha256': binary_upgrade.unit_digest(state),
                           'files': {p: self.fake.sha256('coordinator', p) for p in (CONFIG_PATH, FLEET, SCRIPT)}},
            'workers': {'worker@v12:' + u: {'ready_identity': identity(u), 'limits': {'MemoryMax': '8G'},
                                            'args': ['--cache-bytes', '64']} for u in WORKERS},
        }
        change(request)
        self.request.write_bytes(json.dumps(request).encode())
        return hashlib.sha256(self.request.read_bytes()).hexdigest()

    def run_cli(self, action, *extra, digest=None):
        lines, now = [], [0.0]
        def sleep(seconds):
            now[0] += seconds
        code = cli.main(['--inventory', str(self.inventory), '--state-dir', str(self.state), 'transparent-binary-' + action,
                         '--request', str(self.request), '--request-sha256',
                         digest or hashlib.sha256(self.request.read_bytes()).hexdigest(), *extra],
                        executor=self.fake, out=lines.append, sleep=sleep, clock=lambda: now[0],
                        lock=lambda: self.fake.hold_lock('coordinator', descriptors.LOCK_PATH))
        return code, lines

    def running(self):
        return {u: self.fake.probe_unit('v12', u)['exe_sha256'] for u in WORKERS}

    def test_upgrade_preserves_publication_and_rolls_back(self):
        self.assertEqual(self.run_cli('baseline')[0], 0)
        self.fake.log.clear()
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.log, [], 'preflight changes nothing')
        self.assertIn('release: git %s binary %s; publication: {"source_sha": "fb90519b"}' % (GIT, sha256(NEW)), lines)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(NEW)})
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['main_pid'] > 0, True)
        self.assertFalse([m for m in self.fake.mutations('coordinator') if m[1] == 'systemctl'], 'controller untouched')
        self.assertEqual(self.fake.sha256('v12', '/opt/transparent-publisher/releases/%s/transparent-shard-server' % sha256(NEW)),
                         sha256(NEW))
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})

    def test_wrong_source_publication_or_topology_refuses_before_any_change(self):
        cases = {
            'source': lambda r: r['publication'].update(source_sha='ff27d649'),
            'assignment': lambda r: r['workers']['worker@v12:transparent-v12-recent.service']['ready_identity'].update(
                assignment='archive'),
            'fleet bytes': lambda r: r['controller']['files'].update({FLEET: '0' * 64}),
            'budget': lambda r: r['workers']['worker@v12:transparent-v12-archive.service']['limits'].update(MemoryMax='9G'),
        }
        self.run_cli('baseline')
        for name, change in cases.items():
            with self.subTest(name):
                self.write_request(change)
                self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
                self.assertEqual(self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')[0], 1)
                self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.write_request()
        self.fake.put('coordinator', CONFIG_PATH, json.dumps({**CONFIG, 'follow_journal': True}))
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 1)
        self.assertTrue(any('follow_journal' in line for line in lines), lines)
        self.write_request(lambda r: r.update(service='transparent-display'))
        self.assertEqual(self.run_cli('preflight')[0], 1)
        self.assertEqual(self.run_cli('preflight', digest='0' * 64)[0], 1)
        self.assertEqual(self.fake.log, [])

    def test_identity_change_after_restart_rolls_back_every_touched_worker(self):
        self.run_cli('baseline')
        self.shift['transparent-v12-recent.service'] = {'publication_sha256': 'cd' * 32, 'assignment': 'recent'}
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1)
        self.assertTrue(any('publication identity changed' in line for line in lines), lines)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        code, lines = self.run_cli('status')
        self.assertEqual(code, 0, lines)
        self.assertTrue(any('rolled-back' in line for line in lines), lines)

    def test_staged_release_bytes_are_immutable(self):
        release = '/opt/transparent-publisher/releases/%s/transparent-shard-server' % sha256(NEW)
        self.fake.put('v12', release, b'other bytes')
        self.run_cli('baseline')
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1)
        self.assertTrue(any('immutable release' in line for line in lines), lines)
        self.assertEqual(self.fake.read('v12', release), 'other bytes')
        self.assertEqual(set(self.running().values()), {sha256(OLD)})


if __name__ == '__main__':
    unittest.main()
