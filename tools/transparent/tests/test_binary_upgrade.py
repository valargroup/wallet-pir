"""Binary upgrades of the v12 history and 4k8 display workers through the deploy wrapper.

New transactions run under the unified publisher; a retired standalone
controller keeps only status and rollback of a recorded transaction. Only hosts
are fake. Ready, status and load bodies have the shapes the deployed services
produce; the controller's map advances during a rollout.
"""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / 'tools/lib'), str(ROOT / 'tools/tests/operations/deploy')]
from fake_fleet import FakeFleet, sha256  # noqa: E402
from wallet_pir_ops.deploy import cli, descriptors  # noqa: E402
from tools.transparent.lib import binary_upgrade  # noqa: E402

OLD, NEW = b'pir-query-server v12', b'pir-query-server next'
CTRL_OLD, CTRL_NEW = b'controller v12', b'controller next'
SERVER = '/usr/local/bin/pir-query-server'
CONTROLLER_BIN = '/usr/local/bin/transparent-publication-controller'


def history_config(unit):
    """Each worker unit's generated pir-query-server configuration."""
    return '/etc/transparent-pir/%s.json' % unit.removesuffix('.service')


WORKERS = {'transparent-v12-archive.service': '10.142.0.9:8093', 'transparent-v12-recent.service': '10.142.0.9:8193'}
IDS = {'transparent-v12-archive.service': 'transparent-v12-archive-01', 'transparent-v12-recent.service': 'transparent-v12-recent-01'}
CONTROLLER = 'transparent-v12-publish-controller.service'
LOAD_UNIT, LOAD_ROOT = 'transparent-v2-5qps-continuous.service', '/srv/transparent-activity/canonical-load/v12'
CONFIG_PATH = '/opt/transparent-publisher/v12/controller.json'
CONFIG = {'data_dir': '/srv/transparent-activity/full-v3/journal', 'follow_journal': False,
          'publication_root': '/srv/transparent-activity/full-v12/publications', 'source_sha': 'fb90519b'}
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

    def record(self, binary=None):
        """A transaction a standalone controller recorded before its retirement."""
        with patch.object(binary_upgrade.Upgrade, 'require_current', lambda upgrade, action: None):
            self.assertEqual(self.run_cli('baseline')[0], 0)
            _, lines = self.run_cli('deploy', '--binary', str(binary or self.binary), '--skip-exact-check')
        self.assertTrue(any(line.startswith('committed ') for line in lines), lines)

    def forward_is_retired(self, binaries):
        """Every new transaction refuses before any host change; status still answers."""
        for component, binary in binaries.items():
            self.write_request(component=component)
            for action, extra in (('plan', ()), ('preflight', ('--binary', str(binary))), ('baseline', ()),
                                  ('deploy', ('--binary', str(binary), '--skip-exact-check'))):
                with self.subTest(component=component, action=action):
                    code, lines = self.run_cli(action, *extra)
                    self.assertEqual(code, 1, lines)
                    self.assertTrue(any('is retired' in line for line in lines), lines)
        self.assertEqual([m for m in self.fake.log if m[1] not in ('mkdir',)], [])


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
            self.fake.install('v12', unit, '[Service]\nExecStart=%s --config %s\nMemoryMax=8G\n'
                              % (SERVER, history_config(unit)), files={SERVER: OLD})
            for path in ('/v1/health', '/v1/ready'):
                self.fake.endpoints[('v12', 'http://%s%s' % (listen, path))] = unit
            self.fake.health_shapes[('v12', unit)] = lambda ready_, exe, unit=unit: {
                **ready(unit, self.map[0]), 'ready': ready_, **(self.shift.get(unit, {}) if exe == sha256(NEW) else {})}
        files = {CONFIG_PATH: json.dumps(CONFIG), CONTROLLER_BIN: CTRL_OLD}
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
                           'files': {CONFIG_PATH: self.fake.sha256('coordinator', CONFIG_PATH)}},
            'workers': {'worker@v12:' + u: {'ready_identity': {'worker_id': IDS[u], 'role': ready(u, MAP)['role']},
                                            'limits': {'MemoryMax': '8G'}, 'args': ['--config', history_config(u)]}
                        for u in WORKERS},
            'load': {'host': 'coordinator', 'pins_sha256': self.fake.sha256('coordinator', LOAD_ROOT + '/pins.json'), 'resume_after_upgrade': True},
        }

    def running(self):
        return {u: self.fake.probe_unit('v12', u)['exe_sha256'] for u in WORKERS}

    def pins(self):
        return json.loads(self.fake.read('coordinator', LOAD_ROOT + '/pins.json'))

    def load_active(self):
        return self.fake.probe_unit('coordinator', LOAD_UNIT)['active_state'] == 'active'

    def test_new_standalone_transactions_are_retired(self):
        self.forward_is_retired({'workers': self.binary, 'controller': self.controller_binary})

    def test_rollback_of_a_recorded_worker_transaction_repins_and_stops_load(self):
        self.record()
        self.assertEqual(set(self.running().values()), {sha256(NEW)})
        # The recorded transaction left the load pinned to what the workers run.
        self.fake.put('coordinator', LOAD_ROOT + '/pins.json', json.dumps({i: sha256(NEW) for i in IDS.values()}))
        self.assertEqual(self.run_cli('status')[0], 0)
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.assertEqual(self.pins(), {i: sha256(OLD) for i in IDS.values()})
        self.assertFalse(self.load_active(), 'rollback leaves load stopped for the exact gate')

    def test_explicit_rollback_preserves_a_latch_while_restoring_workers(self):
        self.record()
        self.fake.put('coordinator', LOAD_ROOT + '/latched.json', '{"reason":"wrong answer"}')
        self.run_cli('rollback')
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.assertEqual(self.fake.read('coordinator', LOAD_ROOT + '/latched.json'), '{"reason":"wrong answer"}')
        self.assertFalse(self.load_active())

    def test_recorded_controller_transaction_rolls_back(self):
        self.write_request(component='controller')
        self.record(self.controller_binary)
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_NEW))
        self.assertEqual(self.run_cli('status')[0], 0)
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))
        self.assertEqual(set(self.running().values()), {sha256(OLD)}, 'workers untouched')
        self.assertTrue(self.load_active())

    def test_explicit_rollback_recovers_an_unhealthy_controller(self):
        self.write_request(component='controller')
        self.record(self.controller_binary)
        self.controller_shift = {'phase': 'halted'}
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.probe_unit('coordinator', CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))

    def test_worker_binary_is_never_selected_as_controller(self):
        self.fake.put('coordinator', SERVER, OLD)
        self.fake.edit('coordinator', '/etc/systemd/system/' + CONTROLLER,
                       '[Service]\nExecStart=%s --config %s\nMemoryMax=4G\n' % (SERVER, CONFIG_PATH))
        self.write_request(component='controller')
        code, lines = self.run_cli('rollback')
        self.assertEqual(code, 1)
        self.assertTrue(any('worker executable' in line for line in lines), lines)


DISPLAY_WORKER, DISPLAY_CONTROLLER = 'transparent-txid-display-worker.service', 'transparent-txid-display-k8-controller.service'
DISPLAY_ROOT = '/srv/txid-display-genesis/v4/root'
DISPLAY_SERVER, DISPLAY_CONTROLLER_BIN = '/usr/local/bin/pir-query-server', '/usr/local/bin/txid-display-controller'
DISPLAY_CONFIG = '/etc/transparent-pir/txid-display-worker.json'
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
        self.fake.install('gpu', DISPLAY_WORKER, '[Service]\nExecStart=%s --config %s\nMemoryMax=40G\n'
                          % (DISPLAY_SERVER, DISPLAY_CONFIG), files={DISPLAY_SERVER: OLD})
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
                                       'args': ['--config', DISPLAY_CONFIG]}},
            'load': None,
        }

    def test_new_standalone_transactions_are_retired(self):
        self.forward_is_retired({'workers': self.binary, 'controller': self.controller_binary})

    def test_recorded_worker_and_controller_transactions_roll_back(self):
        self.record()
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(NEW))
        self.assertEqual(self.run_cli('rollback')[0], 0)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))
        self.write_request(component='controller')
        self.record(self.controller_binary)
        self.assertEqual(self.fake.probe_unit('coordinator', DISPLAY_CONTROLLER)['exe_sha256'], sha256(CTRL_NEW))
        self.assertEqual(self.run_cli('rollback')[0], 0)
        self.assertEqual(self.fake.probe_unit('coordinator', DISPLAY_CONTROLLER)['exe_sha256'], sha256(CTRL_OLD))
        self.assertEqual(self.fake.host('coordinator').units[DISPLAY_CONTROLLER]['exec_start'], DISPLAY_RUN)


class DisplayWorkerChecks:
    """Display worker transactions under the current controller authority."""

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
                self.fake.health_shapes[('coordinator', self.status_owner)] = lambda r, exe, status=status: {
                    'phase': 'running', 'halted': None, 'stalled': False, 'published': {'map_sha256': self.map[0]}, **status}
                self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.fake.health_shapes[('coordinator', self.status_owner)] = lambda r, exe: {
            'phase': 'running', 'halted': None, 'stalled': False, 'published': {'map_sha256': NEXT_MAP}}
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 1)
        self.assertTrue(any('controller current is' in line for line in lines), lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))

    def test_worker_that_restarts_cold_rolls_back(self):
        self.run_cli('baseline')
        self.worker_shift = {'warm': False}
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))

    def test_worker_waits_for_controller_to_resume_publication(self):
        self.assertEqual(self.run_cli('baseline')[0], 0)
        shape = self.fake.health_shapes[('coordinator', self.status_owner)]
        recovering = [4]

        def controller_status(ready_, exe):
            document = shape(ready_, exe)
            worker = self.fake.probe_unit('gpu', DISPLAY_WORKER)
            if worker['exe_sha256'] == sha256(NEW) and recovering[0]:
                recovering[0] -= 1
                return {**document, 'stalled': True}
            return document

        self.fake.health_shapes[('coordinator', self.status_owner)] = controller_status
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(recovering[0], 0)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(NEW))

    def test_worker_that_leaves_controller_stalled_rolls_back(self):
        self.assertEqual(self.run_cli('baseline')[0], 0)
        shape = self.fake.health_shapes[('coordinator', self.status_owner)]

        def controller_status(ready_, exe):
            worker = self.fake.probe_unit('gpu', DISPLAY_WORKER)
            return {**shape(ready_, exe), 'stalled': worker['exe_sha256'] == sha256(NEW)}

        self.fake.health_shapes[('coordinator', self.status_owner)] = controller_status
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('controller did not resume publication' in line for line in lines), lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))



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
            'transparent_history': {'enabled': True, 'settings': {**CONFIG, 'listen': HISTORY_LISTEN}},
            'transaction_details': {'enabled': True, 'settings': {
                'root': DISPLAY_ROOT, 'journal': '/srv/txid-display-genesis/journal-v3', 'status_listen': DISPLAY_LISTEN,
                'trust_sealed': False}}}
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


PUBLISHER_ORIGIN = 'https://enhance-pir.example'


def provider_shard_dir(unit):
    return '/srv/transparent-activity/provider/%s' % unit.removesuffix('.service')


def provider_assignment(map_sha256):
    """The assignment a recent provider's own bootstrap stages: one `provider` entry."""
    plan = assignment(map_sha256)
    entry = dict(plan['workers'][1], id='provider')
    return {**plan, 'workers': [entry]}


class ProviderHistoryUpgrade(UnifiedOwner, Harness):
    """Autonomous HTTPS providers under the unified publisher: archive full holdings, recent self-assigned."""
    standalone_request = HistoryUpgrade.base_request
    running = HistoryUpgrade.running
    FULL = {'transparent-v12-archive.service': True, 'transparent-v12-recent.service': False}

    def select_recent(self, unready=False):
        key = 'worker@v12:transparent-v12-recent.service'
        state = self.fake.probe_unit('v12', 'transparent-v12-recent.service')
        self.write_request(lambda r: r.update(selected_workers=[key], **({
            'unready_predecessors': {key: {'binary_sha256': state['exe_sha256'],
                                          'invocation_id': state['invocation_id']}}} if unready else {})))

    def test_recent_only_upgrade_and_rollback_leave_archive_invocation_untouched(self):
        self.select_recent()
        archive = self.fake.probe_unit('v12', 'transparent-v12-archive.service')
        self.assertEqual(self.run_cli('baseline')[0], 0)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.running()['transparent-v12-recent.service'], sha256(NEW))
        self.assertEqual(self.running()['transparent-v12-archive.service'], sha256(OLD))
        self.assertEqual(self.run_cli('rollback')[0], 0)
        self.assertEqual(self.fake.probe_unit('v12', 'transparent-v12-archive.service'), archive)
        self.publisher_untouched()

    def test_recent_only_upgrade_keeps_serving_archive_background_warmup_out_of_the_release_gate(self):
        self.select_recent()
        archive = self.fake.probe_unit('v12', 'transparent-v12-archive.service')
        self.fake.health_shapes[('v12', 'transparent-v12-archive.service')] = lambda ready_, exe: {
            **self.provider_ready('transparent-v12-archive.service', self.map[0]), 'warm_runtimes': 1}
        self.assertEqual(self.run_cli('baseline')[0], 0)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.probe_unit('v12', 'transparent-v12-archive.service'), archive)

    def test_recent_only_upgrade_still_refuses_failed_archive_preparation(self):
        self.select_recent()
        self.fake.health_shapes[('v12', 'transparent-v12-archive.service')] = lambda ready_, exe: {
            **self.provider_ready('transparent-v12-archive.service', self.map[0]), 'prewarm_failed': 1}
        code, lines = self.run_cli('plan')
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('prewarm_failed' in line for line in lines), lines)

    def test_exact_invalidated_predecessor_can_be_replaced_but_archive_must_be_healthy(self):
        self.select_recent(unready=True)
        old_http = self.fake.http_get
        self.fake.http_get = lambda host, url, timeout=5: (
            (503, json.dumps({'error': 'publication invalidated by reorg'}))
            if url == 'http://10.142.0.9:8193/v2/shards/init' else old_http(host, url, timeout))
        self.assertEqual(self.run_cli('baseline')[0], 0)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.running()['transparent-v12-archive.service'], sha256(OLD))

    def test_unready_permission_refuses_a_restarted_or_differently_failed_predecessor(self):
        self.select_recent(unready=True)
        self.assertEqual(self.run_cli('baseline')[0], 0)
        self.fake.systemctl('v12', 'restart', 'transparent-v12-recent.service')
        old_http = self.fake.http_get
        self.fake.http_get = lambda host, url, timeout=5: (
            (503, json.dumps({'error': 'another failure'}))
            if url == 'http://10.142.0.9:8193/v2/shards/init' else old_http(host, url, timeout))
        before = len(self.fake.log)
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 1, lines)
        self.assertEqual(len(self.fake.log), before)
        self.assertTrue(any('pinned running invocation' in line for line in lines), lines)

    def test_pinned_restarted_provider_can_recover_before_its_listener_opens(self):
        self.select_recent(unready=True)
        self.place('v12', provider_shard_dir('transparent-v12-recent.service') + '/active.invalid.json', ['ab' * 32])
        old_http = self.fake.http_get
        self.fake.http_get = lambda host, url, timeout=5: (
            (0, 'connection refused') if url == 'http://10.142.0.9:8193/v2/shards/init' else old_http(host, url, timeout))
        self.assertEqual(self.run_cli('baseline')[0], 0)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.running()['transparent-v12-archive.service'], sha256(OLD))

    def test_a_closed_listener_without_saved_revocations_is_not_a_recovery_predecessor(self):
        self.select_recent(unready=True)
        old_http = self.fake.http_get
        self.fake.http_get = lambda host, url, timeout=5: (
            (0, 'connection refused') if url == 'http://10.142.0.9:8193/v2/shards/init' else old_http(host, url, timeout))
        for revoked in [None, [], ['not-a-digest']]:
            self.place('v12', provider_shard_dir('transparent-v12-recent.service') + '/active.invalid.json', revoked)
            code, lines = self.run_cli('plan')
            self.assertEqual(code, 1, lines)
            self.assertTrue(any('not refusing reorganized history' in line for line in lines), lines)

    def test_worker_selection_refuses_unknown_duplicate_and_empty_targets(self):
        for selected in [[], ['unknown'], ['worker@v12:transparent-v12-recent.service'] * 2]:
            self.write_request(lambda r: r.update(selected_workers=selected))
            code, lines = self.run_cli('plan')
            self.assertEqual(code, 1, lines)
            self.assertTrue(any('selected_workers' in line for line in lines), lines)

    def test_reviewed_publisher_storage_directory_can_be_pinned_without_changing_it(self):
        config = json.loads(self.fake.read('coordinator', binary_upgrade.PUBLISHER_CONFIG))
        root = '/srv/transparent-activity/full-v12/publications.prepared-storage'
        config['transparent_history']['settings']['publication_root'] = root
        self.place('coordinator', binary_upgrade.PUBLISHER_CONFIG, config)
        self.write_request()
        code, lines = self.run_cli('plan')
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('publication_root' in line for line in lines), lines)
        self.write_request(lambda r: r['publication'].update(publication_root=root))
        self.assertEqual(self.run_cli('baseline')[0], 0)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(json.loads(self.fake.read('coordinator', binary_upgrade.PUBLISHER_CONFIG)), config)
        self.publisher_untouched()

    def test_relative_or_mismatched_publisher_directory_refuses(self):
        for root in ['relative', '/srv/../another', '/different']:
            self.write_request(lambda r: r['publication'].update(publication_root=root))
            code, lines = self.run_cli('plan')
            self.assertEqual(code, 1, lines)

    def failed_archive_rollback(self):
        self.run_cli('baseline')
        unit = 'transparent-v12-archive.service'
        def fail_once(host, operation, detail):
            if operation == 'systemctl' and detail == ('restart', unit):
                self.fake.unhealthy.update({('v12', sha256(OLD)), ('v12', sha256(NEW))})
        recent = 'transparent-v12-recent.service'
        self.fake.health_shapes[('v12', recent)] = lambda ready_, exe: {
            **self.provider_ready(recent, self.map[0]), 'ready': True if exe == sha256(OLD) else ready_}
        self.fake.observer = fail_once
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        from wallet_pir_ops.deploy.transaction import Journal
        journal = Journal.load(self.state, 'transparent-history')
        self.assertEqual(journal.status, 'rollback-failed', lines)
        self.fake.observer = None
        self.fake.unhealthy.remove(('v12', sha256(NEW)))
        self.write_request(lambda r: r.update(repair={'transaction': journal.id, 'predecessors': {
            'worker@v12:' + unit: sha256(OLD)}}))
        return journal

    def test_failed_rollback_can_be_replaced_without_claiming_it_was_ready(self):
        old = self.failed_archive_rollback()
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(NEW)})
        from wallet_pir_ops.deploy.transaction import Journal
        self.assertEqual(Journal.load(self.state, 'transparent-history', old.id).status, 'rollback-failed')
        new = Journal.load(self.state, 'transparent-history')
        self.assertTrue(any(e.get('transaction') == old.id for e in new.data['events']))

    def test_repair_refuses_a_restarted_predecessor(self):
        self.failed_archive_rollback()
        self.fake.systemctl('v12', 'restart', 'transparent-v12-archive.service')
        before = len(self.fake.log)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertEqual(len(self.fake.log), before)
        self.assertTrue(any('exact acknowledged restored processes' in line for line in lines), lines)

    def test_repair_refuses_a_different_transaction(self):
        self.failed_archive_rollback()
        request = json.loads(self.request.read_text())
        request['repair']['transaction'] = 'transparent-history-another'
        self.request.write_text(json.dumps(request))
        before = len(self.fake.log)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertEqual(len(self.fake.log), before)

    def test_repair_still_requires_the_replacement_to_be_ready(self):
        self.failed_archive_rollback()
        self.fake.unhealthy.add(('v12', sha256(NEW)))
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertEqual(self.running()['transparent-v12-archive.service'], sha256(OLD))

    def lagging_archive(self, stays_old=False):
        self.map[0] = NEXT_MAP
        unit = 'transparent-v12-archive.service'
        def health(ready_, exe):
            digest = MAP if exe == sha256(OLD) or stays_old else NEXT_MAP
            self.install_active(unit, digest)
            return {**self.provider_ready(unit, digest), 'ready': ready_}
        self.fake.health_shapes[('v12', unit)] = health
        self.install_active('transparent-v12-recent.service', NEXT_MAP)

    def pin_previous_archive(self, digest=MAP):
        self.write_request(lambda r: r.update(previous_maps={
            'worker@v12:transparent-v12-archive.service': digest}))

    def test_reviewed_older_provider_can_upgrade_but_replacement_must_be_current(self):
        self.lagging_archive()
        self.run_cli('baseline')
        self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.pin_previous_archive()
        self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 0)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(NEW)})
        self.publisher_untouched()

    def test_previous_publication_permission_does_not_apply_to_replacement(self):
        self.lagging_archive(stays_old=True)
        self.run_cli('baseline')
        self.pin_previous_archive()
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('controller current is' in line for line in lines), lines)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})

    def test_replacement_can_catch_up_within_the_existing_readiness_deadline(self):
        self.lagging_archive()
        unit = 'transparent-v12-archive.service'
        attempts = [0]
        def health(ready_, exe):
            attempts[0] += exe == sha256(NEW)
            digest = NEXT_MAP if attempts[0] >= 6 else MAP
            self.install_active(unit, digest)
            return {**self.provider_ready(unit, digest), 'ready': ready_}
        self.fake.health_shapes[('v12', unit)] = health
        self.run_cli('baseline')
        self.pin_previous_archive()
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertGreaterEqual(attempts[0], 6)
        self.assertEqual(set(self.running().values()), {sha256(NEW)})

    def test_unreviewed_older_publication_still_refuses_before_change(self):
        self.lagging_archive()
        self.run_cli('baseline')
        self.pin_previous_archive('f' * 64)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.publisher_untouched()

    def setUp(self):
        HistoryUpgrade.setUp(self)
        self.adopt(CONTROLLER, 'http://%s/v1/status' % HISTORY_LISTEN)
        for unit, full in self.FULL.items():
            root = provider_shard_dir(unit)
            self.place('v12', history_config(unit), self.provider_document(full, root))
            self.install_active(unit, MAP)
            self.fake.health_shapes[('v12', unit)] = lambda ready_, exe, unit=unit: {
                **self.provider_ready(unit, self.map[0]), 'ready': ready_,
                **(self.shift.get(unit, {}) if exe == sha256(NEW) else {})}
        self.discovery = (200, json.dumps({'service': 'pir-publisher', 'version': '1.0.0'}))
        http_get = self.fake.http_get
        self.fake.http_get = lambda host, url, timeout=5: (
            self.discovery if url == PUBLISHER_ORIGIN + binary_upgrade.DISCOVERY_PATH else http_get(host, url, timeout))
        self.write_request()
        self.fake.log.clear()

    @staticmethod
    def provider_document(full, root):
        """The installed `pir-query-server --config FILE` shape: settings under enabled transparent_history."""
        return {'management_listen': '127.0.0.1:18393', 'transparent_history': {'enabled': True, 'settings': {
            'publisher': PUBLISHER_ORIGIN, 'publisher_network': 'main', 'publisher_genesis': '00' * 32,
            'listen': '10.142.0.9:8093', 'shard_dir': root, 'full_holdings': full}}}

    def place(self, host, path, value):
        """Fixture state, not a deployment write."""
        self.fake.host(host).files[path] = json.dumps(value).encode()

    def install_active(self, unit, map_sha256):
        root = provider_shard_dir(unit)
        directory = '%s/stage-%s' % (root, map_sha256[:8])
        active = {'directory': directory, 'assignment': None, 'map_sha256': map_sha256, 'runtimes': directory}
        if not self.FULL[unit]:
            active['assignment'] = directory + '/assignment.json'
            self.place('v12', active['assignment'], provider_assignment(map_sha256))
        self.place('v12', root + '/active.json', active)

    def provider_ready(self, unit, map_sha256):
        counts = {'ready': True, 'map_sha256': map_sha256, 'warm_runtimes': 4, 'target_runtimes': 4, 'prewarm_failed': 0}
        if self.FULL[unit]:
            return counts
        plan = provider_assignment(map_sha256)
        entry = plan['workers'][0]
        return {**counts, 'assignment_sha256': canonical(plan),
                'worker_assignment_sha256': canonical(['transparent-worker-assignment-v1', plan['schema'], plan['set'], entry]),
                'worker_id': 'provider', 'role': 'recent-replica', 'replica_group': 'recent-a', 'assigned_shards': 1}

    def base_request(self, component):
        request = UnifiedOwner.base_request(self, component)
        request['load'] = None
        for unit in WORKERS:
            pinned = request['workers']['worker@v12:' + unit]
            pinned['ready_identity'] = binary_upgrade.PROVIDER_IDENTITY[self.FULL[unit]]
            pinned['config_sha256'] = self.fake.sha256('v12', history_config(unit))
            pinned['unit_sha256'] = binary_upgrade.unit_digest(self.fake.probe_unit('v12', unit))
        return request

    def test_full_and_recent_provider_upgrade_and_rollback_keep_publisher(self):
        self.assertEqual(self.run_cli('baseline')[0], 0)
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 0, lines)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(NEW)})
        self.publisher_untouched()
        self.assertEqual(self.run_cli('rollback')[0], 0, lines)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.publisher_untouched()

    def test_provider_mismatches_refuse_before_any_change(self):
        self.run_cli('baseline')
        recent, archive = 'worker@v12:transparent-v12-recent.service', 'worker@v12:transparent-v12-archive.service'

        def swap_identities(r):
            r['workers'][recent]['ready_identity'], r['workers'][archive]['ready_identity'] = (
                r['workers'][archive]['ready_identity'], r['workers'][recent]['ready_identity'])
        cases = {
            'wrong mode': (swap_identities, 'configuration makes it'),
            'config changed': (lambda r: r['workers'][recent].update(config_sha256='0' * 64), 'differs from its pinned digest'),
            'unit changed': (lambda r: r['workers'][archive].update(unit_sha256='0' * 64), 'unit changed from the pinned bytes'),
            'fleet pins under the publisher': (lambda r: [p.pop(k) for p in r['workers'].values()
                                                          for k in ('config_sha256', 'unit_sha256')], None),
        }
        for name, (change, message) in cases.items():
            with self.subTest(name):
                self.refuses(change_request=change, message=message)
                self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.write_request()

    def test_provider_configuration_without_enabled_history_refuses(self):
        self.run_cli('baseline')
        recent = 'transparent-v12-recent.service'
        root = provider_shard_dir(recent)
        disabled = self.provider_document(False, root)
        disabled['transparent_history']['enabled'] = False
        cases = {'disabled': disabled, 'missing': {'management_listen': '127.0.0.1:18393'},
                 'top-level settings': disabled['transparent_history']['settings']}
        for name, document in cases.items():
            with self.subTest(name):
                self.place('v12', history_config(recent), document)
                self.write_request()  # pins the changed bytes, so only the shape refuses
                self.refuses(message='transparent_history')
                self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.place('v12', history_config(recent), self.provider_document(False, root))
        self.write_request()

    def test_publisher_without_discovery_refuses_before_any_change(self):
        self.run_cli('baseline')
        for name, reply in {'404': (404, 'not found'),
                            'other major': (200, json.dumps({'service': 'pir-publisher', 'version': '2.0.0'}))}.items():
            with self.subTest(name):
                self.discovery = reply
                self.refuses(message='upgrade the publisher first')
                self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.publisher_untouched()

    def test_provider_ownership_or_readiness_mismatch_refuses(self):
        self.run_cli('baseline')
        recent = 'transparent-v12-recent.service'
        cases = {
            'active map': (lambda: self.install_active(recent, NEXT_MAP), 'active record names map'),
            'foreign assignment': (lambda: self.place('v12', '%s/stage-%s/assignment.json' % (
                provider_shard_dir(recent), MAP[:8]), provider_assignment(NEXT_MAP)),
                'assignment names another map'),
            'not warm': (lambda: self.shift.update({recent: {'warm_runtimes': 3}}), None),
        }
        for name, (break_, message) in cases.items():
            with self.subTest(name):
                break_()
                if name == 'not warm':
                    self.fake.health_shapes[('v12', recent)] = lambda ready_, exe: {
                        **self.provider_ready(recent, self.map[0]), 'ready': ready_, 'warm_runtimes': 3}
                    message = 'warm_runtimes 3 of target 4'
                self.refuses(message=message)
                self.install_active(recent, MAP)

    def test_ready_worker_finishes_background_warming_before_acceptance(self):
        self.run_cli('baseline')
        original = self.fake.http_get
        polls = [0]
        def warming(host, url, timeout=5):
            status, body = original(host, url, timeout)
            if host == 'v12' and ':8093/' in url and url.endswith('/v1/ready'):
                current = self.fake.probe_unit(host, 'transparent-v12-archive.service')['exe_sha256']
                if current == sha256(NEW) and polls[0] < 3:
                    polls[0] += 1
                    document = json.loads(body)
                    document.update(ready=True, warm_runtimes=1)
                    return status, json.dumps(document)
            return status, body
        self.fake.http_get = warming
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(polls[0], 3)
        self.assertEqual(set(self.running().values()), {sha256(NEW)})

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

    def test_unready_or_malformed_responses_refuse(self):
        self.run_cli('baseline')
        archive = 'transparent-v12-archive.service'
        for name, change in {'prewarm': {'prewarm_failed': 1}, 'cold': {'warm_runtimes': 1}}.items():
            with self.subTest(name):
                self.fake.health_shapes[('v12', archive)] = (
                    lambda r, exe, change=change: {**self.provider_ready(archive, self.map[0]), **change})
                self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.fake.health_shapes[('v12', archive)] = lambda r, exe: self.provider_ready(archive, self.map[0])
        for status in ({'phase': 'serving'}, {'phase': 'preparing', 'map_sha256': MAP}, {'map_sha256': 'not-a-digest'}):
            with self.subTest(status):
                self.fake.health_shapes[('coordinator', PUBLISHER)] = lambda r, exe, status=status: status
                self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})

    def test_incomplete_ready_body_refuses_before_mutation(self):
        self.run_cli('baseline')
        archive = 'transparent-v12-archive.service'
        for change in [{'ready': False}, {'warm_runtimes': None, 'target_runtimes': None}, {'prewarm_failed': None}]:
            with self.subTest(change=change):
                self.fake.health_shapes[('v12', archive)] = lambda r, exe, change=change: {**self.provider_ready(archive, MAP), **change}
                self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.assertEqual(set(self.running().values()), {sha256(OLD)})

    def test_a_worker_on_retired_flags_needs_its_configured_unit_first(self):
        unit = 'transparent-v12-archive.service'
        self.fake.edit('v12', '/etc/systemd/system/' + unit,
                       '[Service]\nExecStart=/usr/local/bin/transparent-query-server --listen %s --cache-bytes 64\n'
                       'MemoryMax=8G\n' % WORKERS[unit])
        self.write_request()
        code, lines = self.run_cli('preflight', '--binary', str(self.binary))
        self.assertEqual(code, 1)
        self.assertTrue(any('runs a retired executable with command-line settings' in line for line in lines), lines)

    def test_staged_release_bytes_are_immutable(self):
        release = '/opt/transparent-publisher/releases/%s/pir-query-server' % sha256(NEW)
        self.fake.put('v12', release, b'other bytes')
        self.run_cli('baseline')
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1)
        self.assertTrue(any('immutable release' in line for line in lines), lines)
        self.assertEqual(self.fake.read('v12', release), 'other bytes')
        self.assertEqual(set(self.running().values()), {sha256(OLD)})


    def test_wrong_source_budget_component_or_service_refuses_before_any_change(self):
        cases = {
            'source': lambda r: r['publication'].update(source_sha='ff27d649'),
            'budget': lambda r: r['workers']['worker@v12:transparent-v12-archive.service']['limits'].update(MemoryMax='9G'),
            'component': lambda r: r.update(component='router'),
            'load pins': lambda r: r.update(load={'host': 'coordinator', 'pins_sha256': '0' * 64,
                                                  'resume_after_upgrade': True}),
        }
        self.run_cli('baseline')
        for name, change in cases.items():
            with self.subTest(name):
                self.refuses(change_request=change)
                self.assertEqual(set(self.running().values()), {sha256(OLD)})
        self.write_request(lambda r: r.update(service='transparent-display'))
        self.assertEqual(self.run_cli('preflight')[0], 1)
        self.write_request()
        self.assertEqual(self.run_cli('preflight', digest='0' * 64)[0], 1)
        self.publisher_untouched()

    def test_fleet_controlled_pins_refuse_under_the_unified_publisher(self):
        self.run_cli('baseline')
        fleet = UnifiedOwner.base_request(self, 'workers')
        self.refuses(change_request=lambda r: r.update(workers=fleet['workers'], load=fleet['load']),
                     message='publishes History over HTTPS only')

    def test_ambiguous_owner_or_publisher_drift_refuses_before_any_change(self):
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



class UnifiedDisplayUpgrade(DisplayWorkerChecks, UnifiedOwner, Harness):
    status_owner = PUBLISHER
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


    def lagging_display(self, stays_old=False):
        self.map[0] = NEXT_MAP
        def health(ready_, exe):
            digest = MAP if exe == sha256(OLD) or stays_old else NEXT_MAP
            return {'ready': ready_, 'warm': True, 'role': 'combined',
                    'map_sha256': digest, 'covered_through': 3100000}
        self.fake.health_shapes[('gpu', DISPLAY_WORKER)] = health

    def pin_previous_display(self, digest=MAP):
        self.write_request(lambda request: request.update(previous_maps={'worker@gpu': digest}))

    def test_reviewed_older_display_can_upgrade_to_the_current_publication(self):
        self.lagging_display()
        self.assertEqual(self.run_cli('baseline')[0], 0)
        self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 1)
        self.pin_previous_display()
        self.assertEqual(self.run_cli('preflight', '--binary', str(self.binary))[0], 0)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 0, lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(NEW))
        self.publisher_untouched()

    def test_a_display_replacement_cannot_use_the_predecessors_older_map(self):
        self.lagging_display(stays_old=True)
        self.assertEqual(self.run_cli('baseline')[0], 0)
        self.pin_previous_display()
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertTrue(any('controller current is' in line for line in lines), lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))
        self.publisher_untouched()

    def test_an_unreviewed_older_display_map_refuses_before_change(self):
        self.lagging_display()
        self.assertEqual(self.run_cli('baseline')[0], 0)
        self.pin_previous_display('f' * 64)
        code, lines = self.run_cli('deploy', '--binary', str(self.binary), '--skip-exact-check')
        self.assertEqual(code, 1, lines)
        self.assertEqual(self.fake.probe_unit('gpu', DISPLAY_WORKER)['exe_sha256'], sha256(OLD))
        self.publisher_untouched()


if __name__ == '__main__':
    unittest.main()
