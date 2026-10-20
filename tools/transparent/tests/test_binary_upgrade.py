"""Worker and controller binary upgrades of the v12 history and 4k8 display fleets through the deploy wrapper.

Only hosts are fake. Ready, status, assignment and load bodies have the shapes
the deployed services produce; the controller's map advances during a rollout.
"""
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
from tools.transparent.lib import binary_upgrade  # noqa: E402

OLD, NEW = b'transparent-shard-server v12', b'transparent-shard-server next'
CTRL_OLD, CTRL_NEW = b'controller v12', b'controller next'
SERVER = '/usr/local/bin/transparent-query-server'
CONTROLLER_BIN = '/usr/local/bin/transparent-publication-controller'
WORKERS = {'transparent-v12-archive.service': '10.142.0.9:8093', 'transparent-v12-recent.service': '10.142.0.9:8193'}
IDS = {'transparent-v12-archive.service': 'transparent-v12-archive-01', 'transparent-v12-recent.service': 'transparent-v12-recent-01'}
CONTROLLER = 'transparent-v12-publish-controller.service'
LOAD_UNIT, LOAD_ROOT = 'transparent-v2-5qps-continuous.service', '/srv/transparent-activity/canonical-load/v12'
CONFIG_PATH, FLEET, SCRIPT = ('/opt/transparent-publisher/v12/controller.json', '/opt/transparent-publisher/v12/fleet.json',
                              '/srv/transparent-activity/ops/sources/v12-ff27/ops/transparent/scripts/transparent-live-fleet.py')
STATE_DIR = '/srv/transparent-activity/full-v12/fleet'
CONFIG = {'data_dir': '/srv/transparent-activity/full-v3/journal', 'follow_journal': False, 'fleet_command': SCRIPT,
          'fleet_config': FLEET, 'publication_root': '/srv/transparent-activity/full-v12/publications',
          'source_sha': 'fb90519b'}
GIT = '1' * 40
MAP, NEXT_MAP = 'ab' * 32, 'ac' * 32


def assignment(map_sha256):
    """A native assignment in declaration order, so its canonical bytes are `json.dumps` compact."""
    return {'schema': 'transparent-assignment-v1',
            'set': {'shard_schema': 'transparent-shard-v12', 'map_sha256': map_sha256, 'network': 'main',
                    'genesis_hash': '00' * 32, 'shards': 3, 'start_height': 419200,
                    'covered_through': 3100000 + (map_sha256 == NEXT_MAP), 'recent_from_shard': 2},
            'generated_by': {'tool': 'shard-assign', 'source_sha': 'fb90519b', 'generated_at': '2026-10-16T00:00:00Z'},
            'workers': [{'id': IDS['transparent-v12-archive.service'], 'role': 'archive-owner', 'replica_group': None,
                         'upstream': WORKERS['transparent-v12-archive.service'], 'cache_bytes': 64, 'shards': [0, 1],
                         'estimated_resident_bytes': 10},
                        {'id': IDS['transparent-v12-recent.service'], 'role': 'recent-replica', 'replica_group': 'recent-a',
                         'upstream': WORKERS['transparent-v12-recent.service'], 'cache_bytes': 64, 'shards': [2],
                         'estimated_resident_bytes': 5}],
            'unassigned': []}


def canonical(value):
    return hashlib.sha256(json.dumps(value, separators=(',', ':')).encode()).hexdigest()


def ready(unit, map_sha256):
    """A real v12 `/v1/ready` body for the worker under the assignment of `map_sha256`."""
    plan = assignment(map_sha256)
    entry = next(w for w in plan['workers'] if w['id'] == IDS[unit])
    return {'ready': True, 'map_sha256': map_sha256, 'assignment_sha256': canonical(plan),
            'worker_assignment_sha256': canonical(['transparent-worker-assignment-v1', plan['schema'], plan['set'], entry]),
            'worker_id': entry['id'], 'role': entry['role'], 'replica_group': entry['replica_group'],
            'assigned_shards': len(entry['shards']), 'warm_runtimes': 4, 'target_runtimes': 4, 'prewarm_failed': 0}


class Harness(unittest.TestCase):
    service = None
    descriptors = ()

    def tearDown(self):
        self.dir.cleanup()

    def run_cli(self, action, *extra, digest=None):
        lines, now = [], [0.0]

        def sleep(seconds):
            now[0] += seconds
        code = cli.main([*self.descriptors, '--inventory', str(self.inventory), '--state-dir', str(self.state), 'transparent-binary-' + action,
                         '--request', str(self.request), '--request-sha256',
                         digest or hashlib.sha256(self.request.read_bytes()).hexdigest(), *extra],
                        executor=self.fake, out=lines.append, sleep=sleep, clock=lambda: now[0],
                        lock=lambda: self.fake.hold_lock('coordinator', descriptors.LOCK_PATH))
        return code, lines

    def write_request(self, change=lambda r: None, component='workers'):
        request = self.base_request(component)
        change(request)
        self.request.write_bytes(json.dumps(request).encode())
        return hashlib.sha256(self.request.read_bytes()).hexdigest()


class HistoryUpgrade(Harness):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = Path(self.dir.name)
        self.state, self.binary, self.controller_binary = root / 'state', root / 'next', root / 'controller-next'
        self.binary.write_bytes(NEW)
        self.controller_binary.write_bytes(CTRL_NEW)
        self.fake = FakeFleet()
        self.shift = {}  # unit -> fields a restarted NEW worker reports instead
        self.map = [MAP]  # the controller's current map; advances during the rollout
        self.controller_status = {'phase': 'serving'}
        for unit, listen in WORKERS.items():
            self.fake.install('v12', unit, '[Service]\nExecStart=%s serve --listen %s --cache-bytes 64\nMemoryMax=8G\n'
                              % (SERVER, listen), files={SERVER: OLD})
            for path in ('/v1/health', '/v1/ready'):
                self.fake.endpoints[('v12', 'http://%s%s' % (listen, path))] = unit
            self.fake.health_shapes[('v12', unit)] = lambda ready_, exe, unit=unit: {
                **ready(unit, self.map[0]), 'ready': ready_, **(self.shift.get(unit, {}) if exe == sha256(NEW) else {})}
        files = {CONFIG_PATH: json.dumps(CONFIG), FLEET: json.dumps({'state_dir': STATE_DIR, 'roster': '/x'}),
                 SCRIPT: '#!python', CONTROLLER_BIN: CTRL_OLD}
        for digest in (MAP, NEXT_MAP):
            # Formatting differs from the canonical bytes, as the planner writes it.
            files['%s/%s.assignment.json' % (STATE_DIR, digest)] = json.dumps(assignment(digest), indent=2, sort_keys=True)
        self.fake.install('coordinator', CONTROLLER, '[Service]\nExecStart=%s --config %s\nMemoryMax=4G\n'
                          % (CONTROLLER_BIN, CONFIG_PATH), files=files)
        self.fake.endpoints[('coordinator', 'http://10.142.0.3:8096/v1/status')] = CONTROLLER
        self.fake.health_shapes[('coordinator', CONTROLLER)] = lambda ready_, exe: {
            **self.controller_status, 'map_sha256': self.map[0],
            **(self.controller_shift if exe == sha256(CTRL_NEW) else {})}
        self.controller_shift = {}
        self.fake.install('coordinator', LOAD_UNIT, '[Service]\nExecStart=/usr/bin/python3 transparent-quality-load.py\n',
                          files={'/usr/bin/python3': b'python', LOAD_ROOT + '/pins.json':
                                 json.dumps({i: sha256(OLD) for i in IDS.values()})})
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

    def base_request(self, component):
        state = self.fake.probe_unit('coordinator', CONTROLLER)
        return {
            'schema': binary_upgrade.SCHEMA, 'service': 'transparent-history', 'component': component,
            'release': {'git_sha': GIT, 'binary_sha256': sha256(NEW if component == 'workers' else CTRL_NEW)},
            'publication': {'source_sha': 'fb90519b'},
            'controller': {'host': 'coordinator', 'unit_sha256': binary_upgrade.unit_digest(state),
                           'files': {p: self.fake.sha256('coordinator', p) for p in (CONFIG_PATH, FLEET, SCRIPT)}},
            'workers': {'worker@v12:' + u: {'ready_identity': {'worker_id': IDS[u], 'role': ready(u, MAP)['role']},
                                            'limits': {'MemoryMax': '8G'}, 'args': ['--cache-bytes', '64']}
                        for u in WORKERS},
            'load': {'host': 'coordinator', 'pins_sha256': self.fake.sha256('coordinator', LOAD_ROOT + '/pins.json'), 'resume_after_upgrade': True},
        }

    def running(self):
        return {u: self.fake.probe_unit('v12', u)['exe_sha256'] for u in WORKERS}

    def pins(self):
        return json.loads(self.fake.read('coordinator', LOAD_ROOT + '/pins.json'))

    def load_active(self):
        return self.fake.probe_unit('coordinator', LOAD_UNIT)['active_state'] == 'active'

    def test_archive_restore_uses_the_product_warmup_budget(self):
        self.run_cli('baseline')
        http_get = self.fake.http_get
        polls = [0]
        def cold_restore(host, url, timeout=5):
            current = self.fake.probe_unit('v12', 'transparent-v12-archive.service')['exe_sha256']
            if host == 'v12' and ':8093/' in url and url.endswith('/v1/ready') and current == sha256(NEW) and polls[0] < 110:
                polls[0] += 1
                return 503, json.dumps({'ready': False, 'reason': 'prewarming'})
            return http_get(host, url, timeout)
        self.fake.http_get = cold_restore
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(NEW)})

    def test_preflight_reports_running_alert_mode_and_delivery_not_the_observer(self):
        self.run_cli('baseline')
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 0, lines)
        text = '\n'.join(lines)
        self.assertIn('alert_mode configured on disk (read only at APM start): unknown', text)
        self.assertIn('transparent mode of the running APM: unknown (incident database unreadable)', text)
        self.assertIn('notifications: APM delivery: unknown', text)
        self.assertIn('recorded only; Slack is not notified', text)
        # Configured active on disk, but the running APM still evaluates shadow.
        self.fake.put('coordinator', binary_upgrade.APM_CONFIG, json.dumps({'services': [
            {'service': 'transparent', 'kind': 'history', 'alert_mode': 'active'}]}))
        self.fake.put('coordinator', '/usr/bin/python3', b'python')
        summary = {'progress_ok': True, 'delivery': {'configured': True, 'pending': 0, 'last_success': 5, 'failure': False}}
        replies = {binary_upgrade.APM_STATE: {'mode': 'active', 'mode:quality-transparent': 'shadow'},
                   'http://127.0.0.1:3002/monitor-status': summary}
        self.fake.product_check = lambda host, argv: (
            (0, json.dumps({'ok': True, 'result': replies[argv[-1]]})) if argv[-1] in replies else (1, 'unexpected'))
        code, lines = self.run_cli('status')
        text = '\n'.join(lines)
        self.assertIn('configured on disk (read only at APM start): active', text)
        self.assertIn('transparent mode of the running APM: shadow', text)
        self.assertIn('configured active differs from running shadow', text)
        self.assertIn('webhook configured=True pending=0 last_success=5', text)
        self.assertIn('recorded only; Slack is not notified', text)
        self.assertNotIn('(Slack)', text)

    def test_successful_retry_resumes_the_requested_observer(self):
        self.run_cli('baseline')
        self.fake.systemctl('coordinator', 'stop', LOAD_UNIT)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertTrue(self.load_active(), 'the request restores the observer after a successful retry')

    def test_worker_upgrade_pauses_and_repins_load_then_rolls_back(self):
        self.assertEqual(self.run_cli('baseline')[0], 0)
        self.fake.log.clear()
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.log, [], 'preflight changes nothing')
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(NEW)})
        self.assertFalse([m for m in self.fake.mutations('coordinator') if m[1] == 'systemctl' and CONTROLLER in m[2]],
                         'controller untouched')
        self.assertEqual(self.pins(), {i: sha256(NEW) for i in IDS.values()})
        self.assertTrue(self.load_active())
        stops = [m for m in self.fake.mutations('coordinator') if m[1] == 'systemctl' and m[2] == ('stop', LOAD_UNIT)]
        self.assertEqual(len(stops), 1)
        self.assertEqual(self.fake.sha256('v12', '/opt/transparent-publisher/releases/%s/transparent-query-server'
                                          % sha256(NEW)), sha256(NEW))
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.assertEqual(self.pins(), {i: sha256(OLD) for i in IDS.values()})
        self.assertFalse(self.load_active(), 'rollback leaves load stopped for the exact gate')

    def test_wrong_source_ownership_budget_or_topology_refuses_before_any_change(self):
        cases = {
            'source': lambda r: r['publication'].update(source_sha='ff27d649'),
            'worker id': lambda r: r['workers']['worker@v12:transparent-v12-recent.service']['ready_identity'].update(
                worker_id=IDS['transparent-v12-archive.service']),
            'role': lambda r: r['workers']['worker@v12:transparent-v12-recent.service']['ready_identity'].update(
                role='archive-owner'),
            'invented field': lambda r: r['workers']['worker@v12:transparent-v12-recent.service']['ready_identity'].update(
                publication_sha256='ab' * 32),
            'frozen digest': lambda r: r['workers']['worker@v12:transparent-v12-recent.service']['ready_identity'].update(
                worker_assignment_sha256='b1' * 32),
            'fleet bytes': lambda r: r['controller']['files'].update({FLEET: '0' * 64}),
            'budget': lambda r: r['workers']['worker@v12:transparent-v12-archive.service']['limits'].update(MemoryMax='9G'),
            'load pins': lambda r: r['load'].update(pins_sha256='0' * 64),
            'no load': lambda r: r.update(load=None),
            'component': lambda r: r.update(component='router'),
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
        self.fake.put('coordinator', CONFIG_PATH, json.dumps(CONFIG))
        self.write_request(lambda r: r.update(service='transparent-display'))
        self.assertEqual(self.run_cli('preflight')[0], 1)
        self.assertEqual(self.run_cli('preflight', digest='0' * 64)[0], 1)
        self.assertEqual([m for m in self.fake.log if m[1] != 'mkdir'], [])
        self.assertTrue(self.load_active())

    def test_wrong_assignment_scope_refuses(self):
        self.run_cli('baseline')
        # A worker reporting the other worker's scope, or an assignment the controller does not hold.
        for name, fields in {'scope': {'worker_assignment_sha256': ready('transparent-v12-archive.service', MAP)[
                                 'worker_assignment_sha256']},
                             'shards': {'assigned_shards': 2}, 'group': {'replica_group': 'recent-b'}}.items():
            with self.subTest(name):
                shape = self.fake.health_shapes[('v12', 'transparent-v12-recent.service')]
                self.fake.health_shapes[('v12', 'transparent-v12-recent.service')] = (
                    lambda r, exe, fields=fields: {**ready('transparent-v12-recent.service', self.map[0]), **fields})
                code, lines = self.run_cli('preflight', '--binary', str(self.binary))
                self.assertEqual(code, 1)
                self.assertTrue(any('controller assignment says' in line for line in lines), lines)
                self.fake.health_shapes[('v12', 'transparent-v12-recent.service')] = shape
        self.fake.remove('coordinator', '%s/%s.assignment.json' % (STATE_DIR, MAP))
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 1)
        self.assertTrue(any('no valid controller assignment' in line for line in lines), lines)

    def test_controller_map_advancing_mid_rollout_is_the_authority(self):
        self.run_cli('baseline')
        shape = self.fake.health_shapes[('v12', 'transparent-v12-archive.service')]

        def advancing(ready_, exe):
            # The controller publishes a new map while the first worker restarts.
            if exe == sha256(NEW):
                self.map[0] = NEXT_MAP
            return shape(ready_, exe)
        self.fake.health_shapes[('v12', 'transparent-v12-archive.service')] = advancing
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        # A worker that keeps serving a stale map after the controller moved on refuses.
        self.shift['transparent-v12-recent.service'] = ready('transparent-v12-recent.service', MAP)
        code, lines = self.run_cli('status')
        self.assertEqual(code, 1)
        self.assertTrue(any('controller current is' in line for line in lines), lines)

    def test_unready_or_malformed_responses_refuse(self):
        self.run_cli('baseline')
        archive = 'transparent-v12-archive.service'
        for name, change in {'prewarm': {'prewarm_failed': 1}, 'cold': {'warm_runtimes': 1}}.items():
            with self.subTest(name):
                self.fake.health_shapes[('v12', archive)] = (
                    lambda r, exe, change=change: {**ready(archive, self.map[0]), **change})
                self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.fake.health_shapes[('v12', archive)] = lambda r, exe: ready(archive, self.map[0])
        for status in ({'phase': 'serving'}, {'phase': 'preparing', 'map_sha256': MAP}, {'map_sha256': 'not-a-digest'}):
            with self.subTest(status):
                self.fake.health_shapes[('coordinator', CONTROLLER)] = lambda r, exe, status=status: status
                self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})

    def test_identity_change_after_restart_rolls_back_and_leaves_load_stopped(self):
        self.run_cli('baseline')
        self.shift['transparent-v12-recent.service'] = {'role': 'archive-owner'}
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1)
        self.assertTrue(any('publication identity changed' in line for line in lines), lines)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.assertEqual(self.pins(), {i: sha256(OLD) for i in IDS.values()})
        self.assertFalse(self.load_active())
        self.assertTrue(any('left stopped for investigation' in line for line in lines), lines)
        code, lines = self.run_cli('status')
        self.assertEqual(code, 0, lines)
        self.assertTrue(any('rolled-back' in line for line in lines), lines)

    def test_existing_latch_refuses_and_is_preserved(self):
        self.run_cli('baseline')
        self.fake.put('coordinator', LOAD_ROOT + '/latched.json', '{"reason": "incorrect decoded response"}')
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1)
        self.assertTrue(any('critical latch' in line for line in lines), lines)
        self.assertEqual(self.fake.read('coordinator', LOAD_ROOT + '/latched.json'), '{"reason": "incorrect decoded response"}')
        self.assertTrue(self.load_active())
        self.assertEqual(set(self.running().values()), {sha256(OLD)})

    def test_controller_upgrade_is_its_own_transaction(self):
        self.run_cli('baseline')
        self.write_request(component='controller')
        code, lines = self.run_cli('deploy', '--binary', str(self.controller_binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        staged = '/opt/transparent-publisher/controller/releases/%s/transparent-publication-controller' % sha256(CTRL_NEW)
        self.assertEqual(self.fake.sha256('coordinator', staged), sha256(CTRL_NEW))
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_NEW))
        self.assertEqual(set(self.running().values()), {sha256(OLD)}, 'workers untouched')
        self.assertTrue(self.load_active())
        self.assertFalse([m for m in self.fake.mutations('v12') if m[1] == 'systemctl'])
        # The normalized unit pin still holds: only the binary changed.
        self.assertEqual(self.run_cli('status')[0], 0)
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))

    def adapter_request(self, **settings):
        adapter = self.binary.parent / 'fleet.py'
        adapter.write_text('#!/usr/bin/python3\nprint("adapter")\n')
        spec = {'sha256': sha256(adapter.read_bytes()), 'previous_configuration': json.dumps(CONFIG)}
        spec['configuration'] = json.dumps({**CONFIG, 'fleet_command': binary_upgrade.adapter_path(spec), **settings})
        self.write_request(lambda request: request.update(staging_adapter=spec), component='controller')
        return adapter, spec

    def test_controller_updates_adapter_and_rollback_restores_configuration(self):
        self.assertEqual(self.run_cli('baseline')[0], 0)
        adapter, spec = self.adapter_request()
        code, lines = self.run_cli('deploy', '--binary', str(self.controller_binary), '--staging-adapter', str(adapter),
                                   '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.read('coordinator', CONFIG_PATH), spec['configuration'])
        self.assertEqual(self.fake.sha256('coordinator', binary_upgrade.adapter_path(spec)), spec['sha256'])
        self.assertEqual(self.run_cli('status')[0], 0)
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.read('coordinator', CONFIG_PATH), json.dumps(CONFIG))
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))
        self.assertEqual(self.fake.read('coordinator', SCRIPT), '#!python')

    def test_adapter_cannot_change_publication_settings(self):
        self.run_cli('baseline')
        adapter, _ = self.adapter_request(source_sha='changed')
        code, lines = self.run_cli('deploy', '--binary', str(self.controller_binary), '--staging-adapter', str(adapter),
                                   '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('may change only controller fleet_command' in line for line in lines), lines)
        self.assertEqual(self.fake.read('coordinator', CONFIG_PATH), json.dumps(CONFIG))
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))

    def test_failed_controller_adapter_upgrade_restores_previous_configuration(self):
        self.run_cli('baseline')
        adapter, _ = self.adapter_request()
        self.controller_shift = {'phase': 'halted'}
        code, lines = self.run_cli('deploy', '--binary', str(self.controller_binary), '--staging-adapter', str(adapter),
                                   '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertEqual(self.fake.read('coordinator', CONFIG_PATH), json.dumps(CONFIG))
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))

    def test_adapter_rejects_wrong_source_bytes_and_existing_corruption(self):
        self.run_cli('baseline')
        adapter, spec = self.adapter_request()
        adapter.write_text('wrong bytes')
        code, lines = self.run_cli('preflight', '--binary', str(self.controller_binary), '--staging-adapter', str(adapter))
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('checksum-bound' in line for line in lines), lines)
        self.fake.mkdir('coordinator', str(Path(binary_upgrade.adapter_path(spec)).parent), 0o755)
        self.fake.write('coordinator', binary_upgrade.adapter_path(spec), b'wrong bytes', 0o755)
        code, lines = self.run_cli('preflight', '--binary', str(self.controller_binary), '--staging-adapter', str(adapter))
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('immutable staging adapter path' in line for line in lines), lines)

    def test_observer_restart_failure_rolls_back_verified_workers(self):
        self.run_cli('baseline')
        systemctl = self.fake.systemctl
        def fail_observer_restart(host, *args):
            if args == ('restart', LOAD_UNIT):
                raise RuntimeError('observer restart failed')
            return systemctl(host, *args)
        self.fake.systemctl = fail_observer_restart
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.assertEqual(self.pins(), {i: sha256(OLD) for i in IDS.values()})
        self.assertFalse(self.load_active())

        self.fake.systemctl = systemctl
        self.write_request()
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(NEW)})
        self.assertTrue(self.load_active(), 'a recovered observer permits an ordinary retry')

    def test_explicit_rollback_recovers_an_unhealthy_controller(self):
        self.run_cli('baseline')
        self.write_request(component='controller')
        self.assertEqual(self.run_cli('deploy', '--binary', str(self.controller_binary), '--skip-exact-check')[0], 0)
        self.controller_shift = {'phase': 'halted'}
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))

    def test_explicit_rollback_preserves_a_latch_while_restoring_workers(self):
        self.run_cli('baseline')
        self.assertEqual(self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')[0], 0)
        self.fake.put('coordinator', LOAD_ROOT + '/latched.json', '{"reason":"wrong answer"}')
        self.run_cli('rollback')
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.assertEqual(self.fake.read('coordinator', LOAD_ROOT + '/latched.json'), '{"reason":"wrong answer"}')
        self.assertFalse(self.load_active())

    def test_incomplete_ready_body_refuses_before_mutation(self):
        self.run_cli('baseline')
        archive = 'transparent-v12-archive.service'
        for change in [{'ready': False}, {'warm_runtimes': None, 'target_runtimes': None}, {'prewarm_failed': None}]:
            with self.subTest(change=change):
                self.fake.health_shapes[('v12', archive)] = lambda r, exe, change=change: {**ready(archive, MAP), **change}
                self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})

    def test_controller_that_restarts_unhealthy_rolls_back(self):
        self.run_cli('baseline')
        self.controller_shift = {'phase': 'halted'}
        self.write_request(component='controller')
        code, lines = self.run_cli('deploy', '--binary', str(self.controller_binary), '--skip-exact-check')
        self.assertEqual(code, 1)
        self.assertTrue(any("controller phase is 'halted'" in line for line in lines), lines)
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))

    def test_worker_binary_is_never_selected_as_controller(self):
        self.fake.put('coordinator', SERVER, OLD)
        self.fake.edit('coordinator', '/etc/systemd/system/' + CONTROLLER,
                       '[Service]\nExecStart=%s --config %s\nMemoryMax=4G\n' % (SERVER, CONFIG_PATH))
        self.write_request(component='controller')
        code, lines = self.run_cli('preflight', '--binary', str(self.controller_binary))
        self.assertEqual(code, 1)
        self.assertTrue(any('worker executable' in line for line in lines), lines)

    def test_controller_unit_drift_beyond_binary_refuses(self):
        self.run_cli('baseline')
        self.write_request(component='controller')
        self.fake.edit('coordinator', '/etc/systemd/system/%s.d/budget.conf' % CONTROLLER, '[Service]\nMemoryMax=6G\n')
        code, lines = self.run_cli('deploy', '--binary', str(self.controller_binary), '--skip-exact-check')
        self.assertEqual(code, 1)
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))

    def test_staged_release_bytes_are_immutable(self):
        release = '/opt/transparent-publisher/releases/%s/transparent-query-server' % sha256(NEW)
        self.fake.put('v12', release, b'other bytes')
        self.run_cli('baseline')
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1)
        self.assertTrue(any('immutable release' in line for line in lines), lines)
        self.assertEqual(self.fake.read('v12', release), 'other bytes')
        self.assertEqual(set(self.running().values()), {sha256(OLD)})


DISPLAY_WORKER, DISPLAY_CONTROLLER = 'transparent-txid-display-worker.service', 'transparent-txid-display-k8-controller.service'
DISPLAY_ROOT = '/srv/txid-display-genesis/v4/root'
DISPLAY_SERVER, DISPLAY_CONTROLLER_BIN = '/usr/local/bin/transparent-txid-server', '/usr/local/bin/txid-display-controller'
# The deployed run-mode command: geometry comes from root/display-root.json, not a flag.
DISPLAY_RUN = ('%s run --root %s --journal /srv/txid-display-genesis/journal-v3 '
               '--workers /opt/transparent-txid-display-k8/workers.json --mode replay-then-live '
               '--status-listen 127.0.0.1:8098 --rpc-url http://127.0.0.1:8232 --rpc-cookie /root/.cache/zakura/.cookie '
               '--blocks-per-step 25 --step-interval-ms 2000' % (DISPLAY_CONTROLLER_BIN, DISPLAY_ROOT))


class DisplayUpgrade(Harness):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = Path(self.dir.name)
        self.state, self.binary, self.controller_binary = root / 'state', root / 'next', root / 'controller-next'
        self.binary.write_bytes(NEW)
        self.controller_binary.write_bytes(CTRL_NEW)
        self.fake = FakeFleet()
        self.map = [MAP]
        self.worker_shift = {}
        self.fake.install('gpu', DISPLAY_WORKER, '[Service]\nExecStart=%s serve --listen 10.142.0.20:8094\nMemoryMax=40G\n'
                          % DISPLAY_SERVER, files={DISPLAY_SERVER: OLD})
        for path in ('/v1/health', '/v1/ready'):
            self.fake.endpoints[('gpu', 'http://10.142.0.20:8094' + path)] = DISPLAY_WORKER
        self.fake.health_shapes[('gpu', DISPLAY_WORKER)] = lambda ready_, exe: {
            'ready': ready_, 'warm': True, 'role': 'combined', 'map_sha256': self.map[0], 'covered_through': 3100000,
            **(self.worker_shift if exe == sha256(NEW) else {})}
        self.install_controller(DISPLAY_RUN)
        self.fake.endpoints[('coordinator', 'http://127.0.0.1:8098/v1/status')] = DISPLAY_CONTROLLER
        self.fake.health_shapes[('coordinator', DISPLAY_CONTROLLER)] = lambda ready_, exe: {
            'phase': 'running', 'halted': None, 'stalled': False, 'published': {'map_sha256': self.map[0]}}
        self.inventory = root / 'inventory.json'
        self.inventory.write_text(json.dumps({
            'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
            'hosts': {'gpu': {}, 'coordinator': {}},
            'services': {'transparent-display': {'roles': {
                'worker': [{'host': 'gpu', 'unit': DISPLAY_WORKER, 'vars': {'listen': '10.142.0.20:8094'}}],
                'controller': [{'host': 'coordinator', 'unit': DISPLAY_CONTROLLER,
                                'vars': {'listen': '127.0.0.1:8098'}}]}}}}))
        self.request = root / 'request.json'
        self.write_request()

    def install_controller(self, command):
        self.fake.install('coordinator', DISPLAY_CONTROLLER, '[Service]\nExecStart=%s\n' % command,
                          files={DISPLAY_CONTROLLER_BIN: CTRL_OLD,
                                 DISPLAY_ROOT + '/display-root.json': '{"geometry": "txid-4k8"}'})

    def base_request(self, component):
        state = self.fake.probe_unit('coordinator', DISPLAY_CONTROLLER)
        return {
            'schema': binary_upgrade.SCHEMA, 'service': 'transparent-display', 'component': component,
            'release': {'git_sha': GIT, 'binary_sha256': sha256(NEW if component == 'workers' else CTRL_NEW)},
            'publication': {'root': DISPLAY_ROOT},
            'controller': {'host': 'coordinator', 'unit_sha256': binary_upgrade.unit_digest(state),
                           'files': {DISPLAY_ROOT + '/display-root.json':
                                     self.fake.sha256('coordinator', DISPLAY_ROOT + '/display-root.json')}},
            'workers': {'worker@gpu': {'ready_identity': {'role': 'combined'}, 'limits': {'MemoryMax': '40G'},
                                       'args': ['--listen', '10.142.0.20:8094']}},
            'load': None,
        }

    def test_a_pinned_layout_with_another_geometry_refuses(self):
        self.run_cli('baseline')
        self.fake.put('coordinator', DISPLAY_ROOT + '/display-root.json', '{"geometry":"txid-2k8"}')
        self.write_request()
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('txid-4k8' in line for line in lines), lines)

    def test_worker_readiness_is_read_from_the_ready_endpoint(self):
        self.run_cli('baseline')
        http_get = self.fake.http_get
        def distinct_health(host, url, timeout=5):
            if url.endswith('/v1/health'):
                return 200, json.dumps({'role': 'combined', 'map_sha256': self.map[0]})
            return http_get(host, url, timeout)
        self.fake.http_get = distinct_health
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.run_cli('rollback')[0], 0)

    def test_real_run_mode_controller_without_geometry_passes(self):
        """Before: the check demanded `--geometry txid-4k8`, which run mode does not take, so this refused."""
        self.assertEqual(self.run_cli('baseline')[0], 0)
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 0, lines)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(NEW))
        self.assertEqual(self.run_cli('rollback')[0], 0)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))

    def test_display_refusals(self):
        self.run_cli('baseline')
        cases = {
            'root': lambda r: r['publication'].update(root='/srv/txid-display-genesis/v3/root'),
            'root file': lambda r: r['controller']['files'].update({DISPLAY_ROOT + '/display-root.json': '0' * 64}),
            'role': lambda r: r['workers']['worker@gpu']['ready_identity'].update(role='archive-owner'),
            'load': lambda r: r.update(load={'host': 'coordinator', 'pins_sha256': '0' * 64}),
        }
        for name, change in cases.items():
            with self.subTest(name):
                self.write_request(change)
                self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.write_request()
        for name, status in {'halted': {'halted': 'journal gap'}, 'stalled': {'stalled': True},
                             'phase': {'phase': 'replaying'}, 'no map': {'published': None}}.items():
            with self.subTest(name):
                self.fake.health_shapes[('coordinator', DISPLAY_CONTROLLER)] = lambda r, exe, status=status: {
                    'phase': 'running', 'halted': None, 'stalled': False, 'published': {'map_sha256': self.map[0]}, **status}
                self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.fake.health_shapes[('coordinator', DISPLAY_CONTROLLER)] = lambda r, exe: {
            'phase': 'running', 'halted': None, 'stalled': False, 'published': {'map_sha256': NEXT_MAP}}
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 1)
        self.assertTrue(any('controller current is' in line for line in lines), lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))

    def test_controller_passing_geometry_refuses(self):
        self.install_controller(DISPLAY_RUN + ' --geometry txid-4k8')
        self.write_request()
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 1)
        self.assertTrue(any('--geometry' in line for line in lines), lines)

    def test_worker_that_restarts_cold_rolls_back(self):
        self.run_cli('baseline')
        self.worker_shift = {'warm': False}
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))

    def test_controller_waits_for_starting_to_become_published(self):
        self.run_cli('baseline')
        self.write_request(component='controller')
        shape = self.fake.health_shapes[('coordinator', DISPLAY_CONTROLLER)]
        probes = [0]
        def starting(ready_, exe):
            if exe == sha256(CTRL_NEW) and probes[0] < 4:
                probes[0] += 1
                return {'phase': 'starting'}
            return shape(ready_, exe)
        self.fake.health_shapes[('coordinator', DISPLAY_CONTROLLER)] = starting
        code, lines = self.run_cli('deploy', '--binary', str(self.controller_binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.probe_unit('coordinator', DISPLAY_CONTROLLER)['exe_sha256'], sha256(CTRL_NEW))

    def test_worker_waits_for_controller_to_resume_publication(self):
        self.assertEqual(self.run_cli('baseline')[0], 0)
        shape = self.fake.health_shapes[('coordinator', DISPLAY_CONTROLLER)]
        recovering = [4]

        def controller_status(ready_, exe):
            document = shape(ready_, exe)
            worker = self.fake.probe_unit('gpu', DISPLAY_WORKER)
            if worker['exe_sha256'] == sha256(NEW) and recovering[0]:
                recovering[0] -= 1
                return {**document, 'stalled': True}
            return document

        self.fake.health_shapes[('coordinator', DISPLAY_CONTROLLER)] = controller_status
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(recovering[0], 0)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(NEW))

    def test_worker_that_leaves_controller_stalled_rolls_back(self):
        self.assertEqual(self.run_cli('baseline')[0], 0)
        shape = self.fake.health_shapes[('coordinator', DISPLAY_CONTROLLER)]

        def controller_status(ready_, exe):
            worker = self.fake.probe_unit('gpu', DISPLAY_WORKER)
            return {**shape(ready_, exe), 'stalled': worker['exe_sha256'] == sha256(NEW)}

        self.fake.health_shapes[('coordinator', DISPLAY_CONTROLLER)] = controller_status
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('controller did not resume publication' in line for line in lines), lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))

    def test_display_controller_upgrade_keeps_run_arguments(self):
        self.run_cli('baseline')
        self.write_request(component='controller')
        code, lines = self.run_cli('deploy', '--binary', str(self.controller_binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        state = self.fake.probe_unit('coordinator', DISPLAY_CONTROLLER)
        self.assertEqual(state['exe_sha256'], sha256(CTRL_NEW))
        staged = '/opt/transparent-txid-display/controller/releases/%s/txid-display-controller' % sha256(CTRL_NEW)
        self.assertEqual(self.fake.host('coordinator').units[DISPLAY_CONTROLLER]['exec_start'],
                         DISPLAY_RUN.replace(DISPLAY_CONTROLLER_BIN, staged))
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))
        self.assertEqual(self.run_cli('rollback')[0], 0)
        self.assertEqual(self.fake.probe_unit('coordinator', DISPLAY_CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))


PUBLISHER, PUBLISHER_CONFIG = binary_upgrade.PUBLISHER_UNIT, binary_upgrade.PUBLISHER_CONFIG
PUBLISHER_BIN = '/opt/pir-publisher/releases/%s/pir-publisher' % ('cd' * 32)
HISTORY_LISTEN, DISPLAY_LISTEN = '10.142.0.3:8096', '127.0.0.1:8098'


class UnifiedOwner:
    """The live topology after `publisher-cutover-*`: pir-publisher.service owns both products' controller duties.

    The standalone controllers are stopped and disabled, the inventory names no
    product controller role, and the workers of both products keep their units.
    """
    descriptors = ('--descriptors', str(ROOT / 'tools/deploy/publisher/transparent-services.toml'))

    def adopt(self, standalone, status_url):
        shape = self.fake.health_shapes[('coordinator', standalone)]
        self.fake.systemctl('coordinator', 'stop', standalone)
        self.fake.systemctl('coordinator', 'disable', standalone)
        self.configuration = {
            'management_listen': '127.0.0.1:18080',
            'transparent_history': {'enabled': True, 'settings': {
                **CONFIG, 'listen': HISTORY_LISTEN, 'ship_runtimes': False, 'distribute_prepared': False}},
            'transaction_details': {'enabled': True, 'settings': {
                'root': DISPLAY_ROOT, 'journal': '/srv/txid-display-genesis/journal-v3', 'status_listen': DISPLAY_LISTEN,
                'ship_runtimes': False, 'trust_sealed': False}}}
        self.fake.install('coordinator', PUBLISHER, '[Service]\nExecStart=%s --config %s\nTimeoutStopSec=infinity\n'
                          % (PUBLISHER_BIN, PUBLISHER_CONFIG),
                          files={PUBLISHER_BIN: b'pir-publisher', PUBLISHER_CONFIG: json.dumps(self.configuration)})
        self.fake.endpoints[('coordinator', status_url)] = PUBLISHER
        self.fake.health_shapes[('coordinator', PUBLISHER)] = shape
        self.fake.endpoints[('coordinator', 'http://127.0.0.1:18080/ready')] = PUBLISHER
        self.write_inventory()
        self.write_request()

    def write_inventory(self, change=lambda document: None):
        history = [{'host': 'v12', 'unit': u, 'vars': {'listen': l}} for u, l in WORKERS.items()]
        display = [{'host': 'gpu', 'unit': DISPLAY_WORKER, 'vars': {'listen': '10.142.0.20:8094'}}]
        document = {
            'lock': {'type': 'remote', 'host': 'coordinator'}, 'ssh': {'mode': 'config'},
            'hosts': {'v12': {}, 'gpu': {}, 'coordinator': {}},
            'services': {'transparent-history': {'roles': {'worker': history}},
                         'transparent-display': {'roles': {'worker': display}},
                         'publisher': {'roles': {'server': [{'host': 'coordinator',
                                                             'vars': {'management_listen': '127.0.0.1:18080'}}]},
                                       'managed_products': ['transparent_history', 'transaction_details'],
                                       'exact_check': {'host': 'coordinator', 'argv': ['/bin/check-both']}}}}
        change(document)
        self.inventory.write_text(json.dumps(document))

    def base_request(self, component):
        request = self.standalone_request(component)
        files = {path: digest for path, digest in request['controller']['files'].items()
                 if path != CONFIG_PATH}
        request['controller'] = {'host': 'coordinator',
                                 'unit_sha256': binary_upgrade.unit_digest(self.fake.probe_unit('coordinator', PUBLISHER)),
                                 'files': {**files, PUBLISHER_CONFIG: self.fake.sha256('coordinator', PUBLISHER_CONFIG)}}
        return request

    def publisher_untouched(self):
        self.assertFalse([m for m in self.fake.mutations('coordinator') if m[1] == 'systemctl' and PUBLISHER in m[2]],
                         'a worker transaction never restarts the unified publisher')
        self.assertEqual(self.fake.probe_unit('coordinator', PUBLISHER)['active_state'], 'active')

    def refuses(self, *, change_request=None, message=None):
        if change_request:
            self.write_request(change_request)
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 1, lines)
        if message:
            self.assertTrue(any(message in line for line in lines), lines)
        self.assertEqual(self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')[0], 1)

    def controller_component_directs_to_publisher_lifecycle(self):
        self.write_request(component='controller')
        for action, extra in (('preflight', ()), ('deploy', ('--skip-exact-check',))):
            code, lines = self.run_cli(action, '--binary', str(self.controller_binary), *extra)
            self.assertEqual(code, 1, lines)
            self.assertTrue(any('publisher-cutover-*' in line for line in lines), lines)
        self.assertEqual(self.fake.probe_unit('coordinator', PUBLISHER)['exe_sha256'], sha256(b'pir-publisher'))
        self.publisher_untouched()


class UnifiedHistoryUpgrade(UnifiedOwner, Harness):
    # The standalone fleet's setup and helpers, without its standalone-only tests.
    standalone_request = HistoryUpgrade.base_request
    running, pins, load_active = HistoryUpgrade.running, HistoryUpgrade.pins, HistoryUpgrade.load_active

    def setUp(self):
        HistoryUpgrade.setUp(self)
        self.adopt(CONTROLLER, 'http://%s/v1/status' % HISTORY_LISTEN)
        self.fake.log.clear()

    def test_unified_worker_upgrade_keeps_publisher_load_and_rollback(self):
        self.assertEqual(self.run_cli('baseline')[0], 0)
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 0, lines)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(NEW)})
        self.assertEqual(self.pins(), {i: sha256(NEW) for i in IDS.values()})
        self.assertTrue(self.load_active())
        self.publisher_untouched()
        self.assertEqual(self.run_cli('rollback')[0], 0)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.assertEqual(self.pins(), {i: sha256(OLD) for i in IDS.values()})
        self.publisher_untouched()

    def test_unified_controller_upgrade_belongs_to_the_publisher_lifecycle(self):
        self.run_cli('baseline')
        self.controller_component_directs_to_publisher_lifecycle()

    def test_unified_ambiguous_owner_or_drift_refuses_before_any_change(self):
        self.run_cli('baseline')
        published = self.fake.read('coordinator', PUBLISHER_CONFIG)
        drift = {
            'follow_journal': lambda c: c['transparent_history']['settings'].update(follow_journal=True),
            'source': lambda c: c['transparent_history']['settings'].update(source_sha='ff27d649'),
            'disabled': lambda c: c['transparent_history'].update(enabled=False),
        }
        for name, change in drift.items():
            with self.subTest(name):
                configuration = json.loads(published)
                change(configuration)
                self.fake.put('coordinator', PUBLISHER_CONFIG, json.dumps(configuration))
                self.write_request()  # Pinning the drifted bytes does not make them acceptable.
                self.refuses()
                self.fake.put('coordinator', PUBLISHER_CONFIG, published)
        self.write_request()
        with self.subTest('unpinned configuration'):
            self.fake.put('coordinator', PUBLISHER_CONFIG, published + ' ')
            self.refuses(message='differs from its pinned digest')
            self.fake.put('coordinator', PUBLISHER_CONFIG, published)
        with self.subTest('standalone controller running beside the publisher'):
            self.fake.systemctl('coordinator', 'restart', CONTROLLER)
            self.refuses(message='beside ' + PUBLISHER)
            self.fake.systemctl('coordinator', 'stop', CONTROLLER)
        with self.subTest('both owners in the inventory'):
            self.write_inventory(lambda d: d['services']['transparent-history']['roles'].update(controller=[
                {'host': 'coordinator', 'unit': CONTROLLER, 'vars': {'listen': HISTORY_LISTEN}}]))
            self.refuses()
        with self.subTest('owner on another host'):
            self.write_inventory(lambda d: d['services']['publisher']['roles']['server'][0].update(host='v12'))
            self.refuses()
        self.write_inventory()
        with self.subTest('registry without the publisher'):
            self.descriptors = ('--descriptors', str(ROOT / 'tools/transparent/deploy/services.toml'))
            self.refuses()
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.publisher_untouched()


class UnifiedDisplayUpgrade(UnifiedOwner, Harness):
    standalone_request = DisplayUpgrade.base_request
    install_controller = DisplayUpgrade.install_controller

    def setUp(self):
        DisplayUpgrade.setUp(self)
        self.adopt(DISPLAY_CONTROLLER, 'http://%s/v1/status' % DISPLAY_LISTEN)
        self.fake.log.clear()

    def test_unified_worker_upgrade_keeps_publisher_and_rolls_back(self):
        self.assertEqual(self.run_cli('baseline')[0], 0)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(NEW))
        self.publisher_untouched()
        self.assertEqual(self.run_cli('rollback')[0], 0)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))
        self.publisher_untouched()

    def test_unified_controller_upgrade_belongs_to_the_publisher_lifecycle(self):
        self.run_cli('baseline')
        self.controller_component_directs_to_publisher_lifecycle()

    def test_unified_display_root_and_layout_refuse(self):
        self.run_cli('baseline')
        self.refuses(change_request=lambda r: r['publication'].update(root='/srv/other/root'),
                     message='publisher display root')
        self.write_request()
        self.fake.put('coordinator', DISPLAY_ROOT + '/display-root.json', '{"geometry":"txid-2k8"}')
        self.write_request()
        self.refuses(message='txid-4k8')
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))
        self.publisher_untouched()


if __name__ == '__main__':
    unittest.main()
