#!/usr/bin/env python3
"""The txid display deploy family against an in-memory fleet.

Only hosts are fake: requests, rendering, journals, the release bundle and the
CLI are the repository's own. The fake models files, systemd units, the
adapter on the coordinator, the history router's re-render, Caddy and the
Terraform wrapper, and refuses to run the wrapper while the production lock
is held.
"""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'ops/lib'))
from wallet_pir_ops.deploy import cli, descriptors  # noqa: E402
from wallet_pir_ops.deploy.remote import Executor, LockHeld, RemoteError  # noqa: E402
from wallet_pir_ops import durable  # noqa: E402


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


P = load('txid_display_poc_test', ROOT / 'transparent/ops/lib/txid_display_poc.py')
LIVE = load('live_fleet_txid_test', ROOT / 'transparent/ops/scripts/transparent-live-fleet.py')
RELEASE = load('release_txid_test', ROOT / 'tools/ci/release.py')

SHA = 'a' * 40
NOW = 1_800_000_000.0
OLD_ADAPTER = b'#!/usr/bin/env python3\n# the deployed history adapter before the hook\n'
GLOB = '/etc/caddy/txid-display/*.caddy'
IMPORT = '\timport ' + GLOB
HOSTS = {'coordinator': 'coordinator', 'archive': 'archive-03', 'recent': 'recent-01', 'router': 'router-01'}
VPC = {'archive': '10.142.0.5', 'recent': '10.142.0.6'}
LOAD = '/opt/transparent-5qps-20260929/status.json'
WRAPPER = '/opt/enhance-pir/ops/wallet-pir-terraform.sh'
TF_ROOT = '/opt/enhance-pir/infra/production'
MAP = 'c' * 64
CANDIDATE = '/srv/zakura/txid-display-poc/root/candidate-100-%s-1' % ('d' * 64)
ROUTER = ('{\n\tservers {\n\t\tmetrics\n\t}\n}\n' + ''.join(
    site + ' {\n\trequest_body {\n\t\tmax_size 1MB\n\t}\n\t@recent path_regexp ^/v1/shards/(1)/\n'
    '\t@metadata path /v1/shards /v1/shards/init\n\thandle {\n\t\trespond 404\n\t}\n}\n'
    for site in ('pir.example', 'http://10.142.0.9:8080')))


def sha256(data):
    return hashlib.sha256(data.encode() if isinstance(data, str) else data).hexdigest()


def binary_bytes(name):
    return ('#!/bin/sh\n# %s built for the test\n' % name).encode()


def request():
    return {
        'schema': P.SCHEMA, 'source_sha': SHA,
        'release': {'archive_sha256': 'b' * 64, 'ci_run': 7,
                    'binaries': {name: sha256(binary_bytes(name)) for name in P.BINARIES},
                    'files': {name: sha256((ROOT / path).read_bytes()) for name, path in P.RELEASE_FILES.items()}},
        'hosts': {'coordinator': {'inventory': 'coordinator'},
                  'archive': {'inventory': 'archive-03', 'vpc_ip': VPC['archive']},
                  'recent': {'inventory': 'recent-01', 'vpc_ip': VPC['recent']},
                  'router': {'inventory': 'router-01'}},
        'ports': {'worker': 8095, 'controller_status': 8099},
        'units': {
            'archive': {'cache_bytes': 4 << 30, 'memory_max': '6G', 'cpu_quota': '200%', 'cpu_weight': 20, 'nice': 10,
                        'oom_score_adjust': 1000, 'build_threads': 2, 'build_slots': 1, 'query_slots': 2,
                        'retain_revisions': 2, 'disk_cache_bytes': 8 << 30, 'ready_timeout_seconds': 1800},
            'recent': {'cache_bytes': 512 << 20, 'memory_max': '1.5G', 'cpu_quota': '100%', 'cpu_weight': 20,
                       'nice': 10, 'oom_score_adjust': 1000, 'build_threads': 1, 'build_slots': 1, 'query_slots': 2,
                       'retain_revisions': 1, 'disk_cache_bytes': None, 'ready_timeout_seconds': 600},
            'controller': {'memory_max': '4G', 'cpu_quota': '200%', 'cpu_weight': 20, 'nice': 10},
            'ingest': {'memory_max': '12G', 'cpu_quota': '400%', 'cpu_weight': 20, 'io_weight': 20, 'nice': 10,
                       'workers': 4},
            'bootstrap': {'memory_max': '8G', 'cpu_quota': '400%', 'cpu_weight': 20, 'nice': 10}},
        'journal': {'data_dir': '/srv/zakura/txid-display-poc/journal', 'start_height': 3289805,
                    'node_cache_dir': '/root/.cache/zakura'},
        'publication': {'root': '/srv/zakura/txid-display-poc/root'},
        'controller': {'mode': 'replay-then-live', 'blocks_per_step': 25, 'step_interval_ms': 2000,
                       'geometry': 'txid-2k', 'n_archive': 1, 'n_recent': 1, 'archive_target': 20000,
                       'recent_floor': 10000, 'reorg_margin': 100, 'max_archive_shards': 24, 'archives': 15,
                       'replay_seals': 4, 'rpc_url': 'http://127.0.0.1:8232',
                       'rpc_cookie': '/root/.cache/zakura/.cookie'},
        'router': {'import_glob': GLOB, 'live_fleet_sha256': sha256(OLD_ADAPTER)},
        'terraform': {'wrapper': WRAPPER, 'root': TF_ROOT},
        'history': {'load_status': LOAD},
        'measure': {'output': '/srv/zakura/txid-display-poc/measure', 'public_url': 'https://pir.example'},
        'baseline': {'coordinator': {'mem_available_bytes': 48 << 30}, 'archive': {'mem_available_bytes': 12 << 30},
                     'recent': {'mem_available_bytes': 12 << 30}},
    }


class Lock:
    def __init__(self, fake):
        self.fake = fake

    def verify(self):
        if not self.fake.locked:
            raise LockHeld('lost')


class Fake(Executor):
    """Hosts with files, units and the services the deploy talks to."""

    def __init__(self):
        self.files, self.modes, self.dirs = {}, {}, set()
        self.units = {}
        self.log, self.runs = [], []
        self.locked = False
        self.listen = {}
        self.disk = {}
        self.wait_results = {}
        self.caddy_rejects = None
        self.reload_fails = False
        self.summaries = []
        self.applied = []
        self.active, self.prepared = {}, {}
        self.history = {'freshness_seconds': 3, 'ready_replicas': 2}
        self.worker_binary = sha256(binary_bytes('transparent-txid-server'))
        self.reconciler_renders = True

    # ------------------------------------------------------------ helpers

    def put(self, host, path, data, mode=0o644):
        data = data.encode() if isinstance(data, str) else data
        self.files[(host, path)] = data
        self.modes[(host, path)] = mode
        parent = os.path.dirname(path)
        while parent != '/':
            self.dirs.add((host, parent))
            parent = os.path.dirname(parent)

    def text(self, host, path):
        data = self.files.get((host, path))
        return None if data is None else data.decode()

    def unit(self, host, unit):
        return self.units.setdefault((host, unit), {'load': 'not-found', 'active': 'inactive', 'sub': 'dead',
                                                    'result': 'success', 'restarts': 0, 'fragment': '',
                                                    'enabled': False})

    def mutations(self, host=None):
        return [entry for entry in self.log if host is None or entry[0] == host]

    def commands(self, host=None):
        return [argv for h, argv in self.runs if host is None or h == host]

    def healthy(self, now=NOW):
        coordinator = 'coordinator'
        self.put(coordinator, P.MEMBERSHIP, json.dumps({'routed_recent': 2, 'updated_unix': now}))
        self.put(coordinator, LOAD, json.dumps({'mode': 'running', 'utc': '2027-01-15T08:00:00+00:00',
                                                'trailing_60s': {'exact': 300, 'errors': 0}}))
        for host in HOSTS.values():
            self.put(host, '/proc/meminfo', 'MemTotal:       16777216 kB\nMemAvailable:   12582912 kB\n')
            self.disk[host] = (1000, 500)
        # The m-8vcpu-64gb coordinator.
        self.put(coordinator, '/proc/meminfo', 'MemTotal:       67108864 kB\nMemAvailable:   50331648 kB\n')
        self.put(coordinator, '/root/.cache/zakura/state/v29/mainnet/CURRENT', 'MANIFEST')

    # ------------------------------------------------------------ executor

    def _mutate(self, host, op, detail):
        if not self.locked:
            raise AssertionError('%s %s on %s without the production lock' % (op, detail, host))
        self.log.append((host, op, detail))

    def check_identity(self, host):
        pass

    def sha256(self, host, path):
        data = self.files.get((host, path))
        return None if data is None else sha256(data)

    def read(self, host, path):
        return self.text(host, path)

    def write(self, host, path, data, mode):
        self._mutate(host, 'write', path)
        if (host, os.path.dirname(path)) not in self.dirs:
            raise FileNotFoundError(os.path.dirname(path))
        self.files[(host, path)] = bytes(data)
        self.modes[(host, path)] = mode

    def upload(self, host, local_path, path, mode):
        self._mutate(host, 'upload', path)
        self.files[(host, path)] = Path(local_path).read_bytes()
        self.modes[(host, path)] = mode

    def mkdir(self, host, path, mode):
        self._mutate(host, 'mkdir', path)
        while path != '/':
            self.dirs.add((host, path))
            path = os.path.dirname(path)

    def rename(self, host, source, destination):
        self._mutate(host, 'rename', (source, destination))
        if (host, destination) in self.dirs:
            raise FileExistsError(destination)
        for (h, path) in list(self.files):
            if h == host and path.startswith(source + '/'):
                self.files[(h, destination + path[len(source):])] = self.files.pop((h, path))
        self.dirs = {(h, destination + p[len(source):]) if h == host and (p == source or p.startswith(source + '/'))
                     else (h, p) for h, p in self.dirs}

    def remove(self, host, path):
        self._mutate(host, 'remove', path)
        self.files.pop((host, path), None)

    def free_bytes(self, host, path):
        return 1 << 40

    def systemctl(self, host, *args):
        self._mutate(host, 'systemctl', args)
        verb = args[0]
        if verb == 'daemon-reload':
            return
        unit = args[1]
        state = self.unit(host, unit)
        if verb == 'enable':
            state['enabled'] = True
        elif verb in ('restart', 'start'):
            if unit != 'caddy' and (host, '/etc/systemd/system/' + unit) not in self.files and not unit.startswith(
                    'transparent-replica') and not unit.startswith('transparent-control'):
                raise RemoteError('%s: unit %s not found' % (host, unit))
            state.update(load='loaded', active='active', sub='running',
                         fragment='/etc/systemd/system/' + unit)
            if unit == P.CONTROLLER_UNIT:
                self.listen.setdefault(host, []).append('127.0.0.1:8099')
            if unit == 'transparent-replica-reconciler.service' and self.reconciler_renders:
                self.render_history()
        elif verb == 'reload':
            if self.reload_fails:
                raise RemoteError('caddy reload failed')
        else:
            raise AssertionError('unexpected systemctl %s' % (args,))

    def render_history(self):
        """The history adapter's next render: the import before metadata in each site."""
        fleet = json.loads(self.text('coordinator', P.HISTORY_FLEET))
        lines = [line for line in ROUTER.split('\n')]
        imports = fleet.get('route_imports') or []
        rendered = []
        for line in lines:
            if line.startswith('\t@metadata'):
                rendered += ['\timport ' + glob for glob in imports]
            rendered.append(line)
        self.files[('router-01', P.CADDYFILE)] = '\n'.join(rendered).encode()

    def http_get(self, host, url, timeout=5):
        if url == P.HISTORY_STATUS:
            return 200, json.dumps(self.history)
        for key, address in VPC.items():
            if url == 'http://%s:8095/v1/ready' % address:
                active = self.unit(HOSTS[key], P.WORKER_UNIT)['active'] == 'active'
                if not active or key not in self.active:
                    return 503, '{}'
                return 200, json.dumps({'ready': True, 'role': P.ROLES[key], 'map_sha256': self.active[key],
                                        'warm': True})
        if url == 'https://pir.example/v1/txid/shards':
            routed = self.text('router-01', '/etc/caddy/txid-display/routes.caddy')
            return (200, json.dumps({'shards': []})) if routed and 'withdrawn' not in routed else (404, '')
        if url == 'https://pir.example/v1/shards':
            return 200, json.dumps({'shards': []})
        return 0, 'connection refused'

    def adapter(self, host, request):
        worker = request['worker']
        name = HOSTS[worker]
        if request['operation'] == 'ship':
            assert request['source'] == CANDIDATE and request['kind'] == 'candidate'
            directory = '%s/%s' % (P.PUBLICATIONS, request['name'])
            self.dirs.add((name, directory))
            return {'ok': True, 'directory': directory, 'seconds': 0.5}
        command = request['command']
        if self.unit(name, P.WORKER_UNIT)['active'] != 'active':
            return {'ok': False, 'error': 'control socket refused the connection'}
        operation = command['operation']
        if operation == 'status':
            active = self.active.get(worker)
            reply = {'ok': True, 'role': P.ROLES[worker], 'warm': active is not None, 'staged': [],
                     'binary_sha256': self.worker_binary,
                     'active': {'directory': '%s/%s' % (P.PUBLICATIONS, active), 'map_sha256': active}
                     if active else None}
        elif operation == 'prepare':
            assert command['expected'] == (self.active.get(worker) or '')
            self.prepared[worker] = command['publication']['map_sha256']
            reply = {'ok': True, 'warm': True, 'built': 2, 'reused': 0, 'seconds': 1.0}
        elif operation == 'activate':
            assert self.prepared.get(worker) == command['map_sha256']
            self.active[worker] = command['map_sha256']
            reply = {'ok': True}
        else:
            raise AssertionError(operation)
        return {'ok': reply['ok'], 'reply': reply, 'seconds': 0.1}

    def run(self, host, argv, timeout):
        self.runs.append((host, list(argv)))
        command = argv[0]
        if command == 'systemctl' and argv[1] == 'show':
            state = self.unit(host, argv[2])
            return 0, ('LoadState=%(load)s\nActiveState=%(active)s\nSubState=%(sub)s\nResult=%(result)s\n'
                       'NRestarts=%(restarts)d\nFragmentPath=%(fragment)s\n' % state)
        if command == 'systemctl':
            self._mutate(host, 'systemctl', tuple(argv[1:]))
            state = self.unit(host, argv[2])
            if argv[1] == 'stop':
                if state['fragment'].startswith('/run/'):
                    state.update(load='not-found', active='inactive', sub='dead')
                else:
                    state.update(active='inactive', sub='dead')
            elif argv[1] == 'disable':
                state['enabled'] = False
            return 0, ''
        if command == 'systemd-run':
            self._mutate(host, 'systemd-run', argv)
            unit = argv[1].removeprefix('--unit=')
            if '--wait' in argv:
                return self.wait_results.get(unit, (0, 'done'))
            self.unit(host, unit).update(load='loaded', active='active', sub='running',
                                         fragment='/run/systemd/transient/' + unit)
            return 0, ''
        if command == 'test':
            path = argv[2]
            exists = (host, path) in self.dirs or (argv[1] == '-e' and (host, path) in self.files)
            return (0 if exists else 1), ''
        if command == 'stat':
            mode = self.modes.get((host, argv[3]))
            return (0, '%o\n' % mode) if mode is not None else (1, 'No such file')
        if command == 'sh' and 'df' in argv[2]:
            size, available = self.disk[host]
            return 0, '     1B-blocks      Avail\n%d %d\n' % (size, available)
        if command == 'ss':
            return 0, ''.join('LISTEN 0 4096 %s 0.0.0.0:*\n' % address for address in self.listen.get(host, []))
        if command == 'find':
            files = [path for h, path in self.files if h == host and re.fullmatch(
                re.escape(argv[1]) + r'/candidate-[^/]+/txid-shards\.json', path)]
            return 0, '\n'.join(files)
        if command == 'caddy':
            config = self.text(host, argv[3])
            if self.caddy_rejects and self.caddy_rejects(argv[3], config):
                return 1, 'Error: adapting config'
            return 0, 'Valid configuration'
        if command == 'python3' and argv[1] == '-c':
            assert not self.locked, 'Terraform summary under the production lock'
            return 0, json.dumps(self.summaries.pop(0) if len(self.summaries) > 1 else self.summaries[0])
        if command == WRAPPER:
            # The wrapper takes the production lock with `flock -n` itself.
            if self.locked:
                return 1, 'flock: failed to get lock'
            if argv[1] == 'plan':
                path = argv[3].removeprefix('-out=')
                self.files[(host, path)] = b'saved plan with secrets'
                return 0, 'Plan: 0 to add, 1 to change, 0 to destroy.'
            self.applied.append(argv[2])
            return 0, 'Apply complete!'
        if argv[-1] == '--help':
            return 0, 'Usage: ...'
        if command == '/usr/bin/python3' and argv[1].endswith('txid-display-fleet.py'):
            assert argv[2] == P.FLEET_JSON
            if len(argv) == 4:
                return 0, 'usage'
            reply = self.adapter(host, json.loads(self.files[(host, argv[3])]))
            self.files[(host, argv[4])] = json.dumps(reply).encode()
            return (0 if reply['ok'] else 1), ''
        if command == 'journalctl':
            return 0, 'ingest progress height=3290000\n'
        raise AssertionError('unexpected command on %s: %s' % (host, argv))

    @contextlib.contextmanager
    def hold_lock(self, host, path):
        assert host == 'coordinator' and path == descriptors.LOCK_PATH
        if self.locked:
            raise LockHeld('%s is held on %s' % (path, host))
        self.locked = True
        try:
            yield Lock(self)
        finally:
            self.locked = False


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds


def inventory():
    return descriptors.Inventory(hosts={name: {} for name in HOSTS.values()}, ssh={'mode': 'config'},
                                 lock={'type': 'remote', 'host': 'coordinator'}, services={})


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = Path(self.tmp.name) / 'state'
        self.fake = Fake()
        self.fake.healthy()
        self.clock = Clock()
        self.output = []
        self.request = request()

    def poc(self, value=None, **options):
        value = value or self.request
        return P.Poc(inventory(), value, durable.digest(value), self.state, executor=self.fake,
                     out=self.output.append, sleep=self.clock.sleep, clock=self.clock,
                     now=lambda: NOW + self.clock.t, **options)

    def journal(self, identifier):
        return json.loads((self.state / (identifier + '.json')).read_text())


class RequestTests(Base):
    def test_reviewed_request_validates_and_mutations_refuse(self):
        P.validate(self.request, inventory())

        def mutate(path, value):
            changed = json.loads(json.dumps(self.request))
            target = changed
            for key in path[:-1]:
                target = target[key]
            if value is KeyError:
                del target[path[-1]]
            else:
                target[path[-1]] = value
            return changed
        refused = [
            (('extra',), 1), (('schema',), 'other'), (('source_sha',), 'abc'),
            (('journal', 'data_dir'), '/srv/transparent-pir/display'),
            (('publication', 'root'), '/srv/zakura/txid-display-poc/journal/root'),
            (('units', 'recent', 'cpu_weight'), 100), (('units', 'archive', 'nice'), 0),
            (('units', 'recent', 'oom_score_adjust'), 0), (('units', 'recent', 'cache_bytes'), 2 << 30),
            (('controller', 'recent_floor'), 9999), (('controller', 'archive_target'), 5000),
            (('controller', 'reorg_margin'), 99), (('controller', 'n_archive'), 65), (('controller', 'n_recent'), 0),
            (('controller', 'max_archive_shards'), 0), (('controller', 'archives'), 0),
            (('controller', 'n_buckets'), 1), (('controller', 'replay_seals'), KeyError),
            (('controller', 'mode'), 'fast'), (('controller', 'rpc_url'), 'http://10.0.0.1:8232'),
            (('ports', 'worker'), 8096), (('ports', 'controller_status'), 8094),
            (('hosts', 'recent', 'inventory'), 'archive-03'), (('hosts', 'archive', 'vpc_ip'), '8.8.8.8'),
            (('router', 'import_glob'), '/etc/caddy/Caddyfile'), (('router', 'import_glob'), '/tmp/x/*.caddy'),
            (('router', 'import_glob'), '/etc/caddy/../*.caddy'), (('router', 'import_glob'), '/etc/caddy/./*.caddy'),
            (('router', 'import_glob'), '/etc/caddy/.hidden/*.caddy'),
            (('release', 'binaries', 'txid-control'), KeyError), (('measure', 'public_url'), 'http://pir.example'),
            (('baseline', 'archive'), {}),
        ]
        for path, value in refused:
            with self.subTest(path=path, value=value), self.assertRaises(P.PocError):
                P.validate(mutate(path, value), inventory())
        remote_elsewhere = descriptors.Inventory(hosts={n: {} for n in HOSTS.values()}, ssh={'mode': 'config'},
                                                 lock={'type': 'remote', 'host': 'router-01'}, services={})
        with self.assertRaisesRegex(P.PocError, 'lock host'):
            P.validate(self.request, remote_elsewhere)

    def test_an_accepted_import_glob_is_one_the_history_adapter_renders(self):
        accepted = []
        for glob in (GLOB, '/etc/caddy/display.d/*.caddy', '/etc/caddy/a..b/*.caddy', '/etc/caddy/../*.caddy',
                     '/etc/caddy/./*.caddy', '/etc/caddy/..x/*.caddy', '/etc/caddy/x/../*.caddy'):
            changed = request()
            changed['router']['import_glob'] = glob
            try:
                P.validate(changed, inventory())
            except P.PocError:
                continue
            # A glob the adapter refused would fail every history router render.
            self.assertEqual(LIVE.route_import_lines({'route_imports': [glob]}), ['\timport ' + glob])
            accepted.append(glob)
        self.assertEqual(accepted, [GLOB, '/etc/caddy/display.d/*.caddy'])

    def test_request_file_must_match_reviewed_digest_and_have_unique_keys(self):
        path = Path(self.tmp.name) / 'request.json'
        path.write_text(json.dumps(self.request))
        self.assertEqual(P.read_request(path, durable.digest(self.request)), self.request)
        with self.assertRaisesRegex(P.PocError, 'reviewed'):
            P.read_request(path, 'f' * 64)
        path.write_text('{"schema": 1, "schema": 2}')
        with self.assertRaisesRegex(P.PocError, 'duplicate'):
            P.read_request(path, 'f' * 64)


class PlanTests(Base):
    def test_plan_is_deterministic_and_reads_no_host(self):
        class Refuses(Executor):
            def __getattribute__(self, name):
                if name in ('run', 'read', 'sha256', 'http_get', 'write'):
                    raise AssertionError('plan touched a host')
                return object.__getattribute__(self, name)
        first = P.Poc(inventory(), self.request, durable.digest(self.request), self.state, executor=Refuses())
        second = P.Poc(inventory(), json.loads(json.dumps(self.request)), durable.digest(self.request), self.state,
                       executor=Refuses())
        self.assertEqual(first.plan_sha256(), second.plan_sha256())
        changed = request()
        changed['units']['recent']['memory_max'] = '1G'
        self.assertNotEqual(first.plan_sha256(), self.poc(changed).plan_sha256())

    def test_rendered_units_carry_the_reviewed_limits(self):
        files = self.poc().render()
        archive, recent, controller = files['unit:archive'], files['unit:recent'], files['unit:controller']
        release = '/opt/transparent-txid-display/releases/' + SHA
        self.assertIn('ExecStart=%s/transparent-txid-server --listen 10.142.0.5:8095 --role archive-owner '
                      '--cache-bytes %d' % (release, 4 << 30), archive)
        for line in ('MemoryMax=6G', 'CPUQuota=200%', 'CPUWeight=20', 'IOWeight=20', 'Nice=10',
                     'OOMScoreAdjust=1000', 'Environment=TRANSPARENT_BUILD_THREADS=2', 'MemorySwapMax=0',
                     'ProtectSystem=strict'):
            self.assertIn('\n' + line + '\n', archive)
        self.assertIn('--runtime-cache-dir /srv/transparent-txid-display/runtime-cache', archive)
        self.assertIn('--control-socket /run/transparent-txid-display/control.sock', archive)
        # `collect` deletes on disk only under a collect root: the adapter's
        # publications directory, where every cycle's candidate lands.
        publications = json.loads(files['fleet.json'])['workers']
        for key, unit in (('archive', archive), ('recent', recent)):
            argv = [line for line in unit.splitlines() if line.startswith('ExecStart=')][0].split()
            self.assertEqual(argv[argv.index('--collect-root') + 1], publications[key]['publications'])
            self.assertEqual(argv[argv.index('--active-record') + 1], P.ACTIVE_RECORD)
        self.assertNotIn('runtime-cache', recent)
        for line in ('MemoryMax=1.5G', 'CPUQuota=100%', 'Environment=TRANSPARENT_BUILD_THREADS=1'):
            self.assertIn('\n' + line + '\n', recent)
        self.assertIn('--retain-revisions 1', recent)
        self.assertIn('--role recent-replica', recent)
        for line in ('MemoryMax=4G', 'CPUQuota=200%', 'CPUWeight=20', 'Nice=10', 'ProtectSystem=strict'):
            self.assertIn('\n' + line + '\n', controller)
        self.assertIn('--workers /opt/transparent-txid-display/workers.json --mode replay-then-live '
                      '--status-listen 127.0.0.1:8099', controller)
        self.assertIn('--blocks-per-step 25 --step-interval-ms 2000', controller)
        # `run` takes no seal flags: the bootstrapped root carries the rule.
        start = [line for line in controller.splitlines() if line.startswith('ExecStart=')][0]
        self.assertTrue(start.endswith('--rpc-cookie /root/.cache/zakura/.cookie --blocks-per-step 25 '
                                       '--step-interval-ms 2000'), start)
        for flag in ('--geometry', '--n-', '--archive-target', '--recent-floor', '--t-archive', '--k-recent'):
            self.assertNotIn(flag, start)
        # The node's RPC cookie under /root stays readable.
        self.assertFalse([line for line in controller.splitlines() if line.startswith('ProtectHome')])
        for unit in (archive, recent, controller):
            for path in ('/srv/transparent-pir', '/opt/transparent-publisher/state', 'control.sock\n'):
                self.assertNotIn('ReadWritePaths=' + path, unit)

    def test_rendered_configs_follow_the_transport_contract(self):
        files = self.poc().render()
        workers = json.loads(files['workers.json'])['workers']
        self.assertEqual([(w['name'], w['role']) for w in workers],
                         [('archive', 'archive-owner'), ('recent', 'recent-replica')])
        self.assertEqual(workers[0]['transport'], {
            'command': '/opt/transparent-txid-display/releases/%s/txid-display-fleet.py' % SHA,
            'config': '/opt/transparent-txid-display/fleet.json'})
        fleet = json.loads(files['fleet.json'])
        self.assertEqual((fleet['ssh_key'], fleet['known_hosts']), (
            '/opt/transparent-publisher/credentials/deploy-ssh', '/opt/transparent-publisher/credentials/known_hosts'))
        self.assertEqual(fleet['workers']['recent']['ssh_host'], '10.142.0.6')
        adapter = load('fleet_config_check', ROOT / 'transparent/ops/scripts/txid-display-fleet.py')
        with tempfile.TemporaryDirectory() as tmp:
            config = json.loads(files['fleet.json'])
            config['control_dir'] = tmp + '/ssh'
            (Path(tmp) / 'fleet.json').write_text(json.dumps(config))
            self.assertEqual(adapter.load_config(Path(tmp) / 'fleet.json')['workers'], fleet['workers'])
        snippet = files['routes.caddy']
        self.assertEqual(snippet.count(LIVE.ROUTER_HEALTH), 2)
        archive = snippet.index('handle /v1/txid/archive/* {')
        recent = snippet.index('handle /v1/txid/* {')
        self.assertLess(archive, recent)
        self.assertIn('reverse_proxy 10.142.0.5:8095 {', snippet[archive:recent])
        self.assertIn('reverse_proxy 10.142.0.6:8095 {', snippet[recent:])
        self.assertIn('respond "transparent txid display withdrawn" 503', files['withdrawn.caddy'])
        self.assertEqual(files['tfvars'].splitlines()[-1], 'transparent_txid_display_port_enabled = true')

    def test_transient_units_carry_their_limits(self):
        transient = self.poc().plan()['transient']
        ingest = transient['ingest']
        for item in ('--unit=transparent-txid-display-ingest.service', '--property=CPUQuota=400%',
                     '--property=CPUWeight=20', '--property=IOWeight=20', '--property=MemoryMax=12G',
                     '--property=Nice=10', '--property=RemainAfterExit=yes'):
            self.assertIn(item, ingest)
        tail = ingest[ingest.index('/opt/transparent-txid-display/releases/%s/transparent-event-ingest' % SHA):]
        self.assertEqual(tail[1:], ['--state-dir', '/root/.cache/zakura', '--workers', '4', '--txid-display',
                                    '--start-height', '3289805', '--data-dir', '/srv/zakura/txid-display-poc/journal'])
        smoke = transient['ingest-smoke']
        self.assertIn('--wait', smoke)
        self.assertEqual(smoke[-4:], ['--stop-height', '3289814', '--data-dir',
                                      '/srv/zakura/txid-display-poc/journal.smoke'])
        self.assertIn('--property=MemoryMax=8G', transient['bootstrap'])
        # txid-display-controller's flags: DisplaySealParams plus the map window.
        seal = ['--geometry', 'txid-2k', '--n-archive', '1', '--n-recent', '1', '--archive-target', '20000',
                '--recent-floor', '10000', '--reorg-margin', '100', '--max-archive-shards', '24']
        controller = '/opt/transparent-txid-display/releases/%s/txid-display-controller' % SHA
        plan_start = transient['plan-start']
        self.assertEqual(plan_start[plan_start.index(controller):], [
            controller, 'plan-start', '--journal', '/srv/zakura/txid-display-poc/journal', '--archives', '15',
            '--replay-seals', '4', *seal])
        bootstrap = transient['bootstrap']
        self.assertEqual(bootstrap[bootstrap.index(controller):], [
            controller, 'bootstrap', '--root', '/srv/zakura/txid-display-poc/root', '--journal',
            '/srv/zakura/txid-display-poc/journal', '--start', '{start}', '--through', '{through}', *seal])
        verify = transient['verify']
        self.assertEqual(verify[verify.index(controller):], [
            controller, 'verify', '--root', '/srv/zakura/txid-display-poc/root', '--journal',
            '/srv/zakura/txid-display-poc/journal'])
        self.assertIn('getbestblockhash', (ROOT / P.RELEASE_FILES['txid-display-observe.py']).read_text())
        self.assertIn('--interval-ms', transient['observer'])
        self.assertIn('https://pir.example/v1/txid/shards', transient['mapwatch'])

    def test_release_bundle_kind_matches_the_deploy(self):
        self.assertEqual(RELEASE.BINARIES[P.KIND], list(P.BINARIES))
        self.assertEqual(sorted(Path(path).name for path in RELEASE.FILES[P.KIND]), sorted(P.RELEASE_FILES))
        self.assertEqual(sorted(RELEASE.FILES[P.KIND]), sorted(P.RELEASE_FILES.values()))


class PreflightTests(Base):
    def assertRefused(self, pattern, phase='workers'):
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'preflight refused'):
            poc.deploy(phase, poc.plan_sha256(), map_sha256=MAP)
        self.assertTrue(any(re.search(pattern, line) for line in self.output), self.output)
        changed = [entry for entry in self.fake.mutations()
                   if not (isinstance(entry[2], str) and entry[2].startswith(P.TRANSACTIONS))]
        self.assertEqual(changed, [])
        transaction = self.journal(json.loads((self.state / 'latest.json').read_text())['id'])
        self.assertEqual((transaction['status'], transaction['changes']), ('failed', []))

    def test_history_health_refusals(self):
        cases = [
            (lambda f: f.put('coordinator', P.MEMBERSHIP, json.dumps({'routed_recent': 1, 'updated_unix': NOW})),
             'routes 1 recent'),
            (lambda f: f.put('coordinator', P.MEMBERSHIP, json.dumps({'routed_recent': 2, 'updated_unix': NOW - 600})),
             'stale'),
            (lambda f: f.history.update(freshness_seconds=45), 'freshness 45'),
            (lambda f: f.history.update(ready_replicas=1), 'ready recent'),
            (lambda f: f.put('coordinator', LOAD, json.dumps({'mode': 'paused', 'utc': '2027-01-15T08:00:00+00:00',
                                                              'trailing_60s': {'exact': 300, 'errors': 0}})),
             'load is paused'),
            (lambda f: f.put('coordinator', LOAD, json.dumps({'mode': 'running', 'utc': '2027-01-15T08:00:00+00:00',
                                                              'trailing_60s': {'exact': 299, 'errors': 1}})),
             'not exact'),
            (lambda f: f.put('coordinator', os.path.dirname(LOAD) + '/latched.json', '{}'), 'latched'),
            (lambda f: f.put('coordinator', P.ACTUATOR_OPERATION, '{}'), 'actuator'),
        ]
        for mutate, pattern in cases:
            with self.subTest(pattern=pattern):
                self.fake = Fake()
                self.fake.healthy()
                mutate(self.fake)
                self.output = []
                self.assertRefused(pattern)

    def test_resource_and_port_refusals(self):
        cases = [
            (lambda f: f.put('recent-01', '/proc/meminfo', 'MemTotal: 8388608 kB\nMemAvailable: 2097152 kB\n'),
             'recent: current MemAvailable', 'workers'),
            (lambda f: f.disk.update({'archive-03': (1000, 100)}), '10.0% free', 'workers'),
            (lambda f: f.listen.update({'archive-03': ['10.142.0.5:8095']}), 'port 8095 is already in use', 'workers'),
            (lambda f: f.listen.update({'coordinator': ['127.0.0.1:8099']}), 'port 8099', 'controller'),
        ]
        for mutate, pattern, phase in cases:
            with self.subTest(pattern=pattern):
                self.fake = Fake()
                self.fake.healthy()
                mutate(self.fake)
                self.output = []
                self.assertRefused(pattern, phase)
        baseline = request()
        baseline['baseline']['archive']['mem_available_bytes'] = 4 << 30
        poc = self.poc(baseline)
        self.assertTrue(any('W0 baseline' in p for p in poc.action_problems('workers')))

    def test_ingest_needs_the_node_state_directory(self):
        del self.fake.files[('coordinator', '/root/.cache/zakura/state/v29/mainnet/CURRENT')]
        self.fake.dirs = {d for d in self.fake.dirs if 'zakura/state' not in d[1]}
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'preflight refused'):
            poc.act('ingest-start', poc.plan_sha256())
        self.assertIn('problem: coordinator: node state directory /root/.cache/zakura/state/v29/mainnet is missing',
                      self.output)

    def test_unreviewed_plan_lock_and_schema_fence_refuse_before_any_change(self):
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'plan changed since review'):
            poc.deploy('workers', 'f' * 64, map_sha256=MAP)
        self.fake.locked = True
        with self.assertRaises(LockHeld):
            poc.deploy('workers', poc.plan_sha256(), map_sha256=MAP)
        self.fake.locked = False
        self.fake.put('coordinator', '/srv/transparent-activity/ops/schema/latest-transparent-schema.json',
                      json.dumps({'id': 'transparent-schema-x'}))
        self.fake.put('coordinator', '/srv/transparent-activity/ops/schema/transparent-schema-x.json',
                      json.dumps({'journal_version': 1, 'id': 'transparent-schema-x', 'status': 'running'}))
        with self.assertRaisesRegex(ValueError, 'unfinished schema transaction'):
            poc.deploy('workers', poc.plan_sha256(), map_sha256=MAP)
        self.assertEqual(self.fake.mutations(), [])
        self.assertEqual(list(self.state.glob('*.json')), [])

    def test_preflight_command_reports_every_problem_and_takes_the_lock(self):
        self.fake.history['freshness_seconds'] = 99
        problems = self.poc().preflight()
        self.assertTrue(any('freshness 99' in p for p in problems))
        self.assertFalse(self.fake.locked)
        self.fake.locked = True
        self.fake.history['freshness_seconds'] = 1
        self.assertTrue(any('production lock' in p for p in self.poc().preflight()))


class StageTests(Base):
    def bundle(self, attempt=1):
        target = Path(self.tmp.name) / 'target'
        (target / 'release').mkdir(parents=True, exist_ok=True)
        for name in P.BINARIES:
            (target / 'release' / name).write_bytes(binary_bytes(name))
        bundles = Path(self.tmp.name) / ('bundles-%d' % attempt)
        RELEASE.assemble(SHA, target, bundles, P.KIND)
        archive = bundles / (P.KIND + '.tar.gz')
        self.request['release']['archive_sha256'] = sha256(archive.read_bytes())
        return archive

    def test_stage_uploads_verifies_and_self_checks_each_host_once(self):
        archive = self.bundle()
        poc = self.poc()
        poc.deploy('stage', poc.plan_sha256(), archive=str(archive))
        release = '/opt/transparent-txid-display/releases/' + SHA
        for key, names in P.HOST_RELEASE.items():
            host = HOSTS[key]
            for name in names:
                self.assertEqual(self.fake.sha256(host, release + '/' + name),
                                 self.request['release']['binaries'].get(name) or self.request['release']['files'][name])
                self.assertIn([release + '/' + name, '--help'] if not name.endswith('.py')
                              else ['/usr/bin/python3', release + '/' + name, '--help'], self.fake.commands(host))
        self.assertIsNone(self.fake.sha256('archive-03', release + '/txid-display-controller'))
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'committed')
        uploads = len([m for m in self.fake.mutations() if m[1] == 'upload'])
        poc = self.poc()
        poc.deploy('stage', poc.plan_sha256(), archive=str(archive))
        self.assertEqual(len([m for m in self.fake.mutations() if m[1] == 'upload']), uploads)

    def test_stage_refuses_other_bytes(self):
        archive = self.bundle()
        self.request['release']['binaries']['txid-control'] = 'e' * 64
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'txid-control differs'):
            poc.deploy('stage', poc.plan_sha256(), archive=str(archive))
        self.assertFalse([m for m in self.fake.mutations() if m[1] == 'upload'])
        self.request = request()
        archive = self.bundle(2)
        self.request['release']['archive_sha256'] = sha256(archive.read_bytes())
        self.fake.put('recent-01', '/opt/transparent-txid-display/releases/%s/txid-control' % SHA, b'other')
        self.fake.put('recent-01', '/opt/transparent-txid-display/releases/%s/transparent-txid-server' % SHA, b'x')
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'holds other bytes'):
            poc.deploy('stage', poc.plan_sha256(), archive=str(archive))


class HookTests(Base):
    def setUp(self):
        super().setUp()
        self.fake.put('coordinator', P.LIVE_FLEET, OLD_ADAPTER, 0o755)
        self.fleet = json.dumps({'roster': '/opt/transparent-publisher/roster.json', 'public_host': 'pir.example'})
        self.fake.put('coordinator', P.HISTORY_FLEET, self.fleet, 0o600)
        self.fake.put('router-01', P.CADDYFILE, ROUTER)

    def test_hook_validates_first_installs_restarts_and_waits_for_the_render(self):
        poc = self.poc()
        poc.deploy('router-hook', poc.plan_sha256())
        validate = [argv for argv in self.fake.commands('router-01') if argv[0] == 'caddy']
        self.assertEqual(len(validate), 1)
        self.assertEqual(self.fake.text('coordinator', P.LIVE_FLEET),
                         (ROOT / 'transparent/ops/scripts/transparent-live-fleet.py').read_text())
        self.assertEqual(self.fake.modes[('coordinator', P.LIVE_FLEET)], 0o755)
        self.assertEqual(json.loads(self.fake.text('coordinator', P.HISTORY_FLEET))['route_imports'], [GLOB])
        restarts = [m[2] for m in self.fake.mutations('coordinator') if m[1] == 'systemctl']
        self.assertEqual(restarts, [('restart', unit) for unit in P.HISTORY_DAEMONS])
        self.assertEqual(self.fake.text('router-01', P.CADDYFILE).count(IMPORT), 2)
        self.assertIn(('router-01', '/etc/caddy/txid-display'), self.fake.dirs)
        record = self.journal(poc.transaction.id)
        self.assertEqual(record['status'], 'committed')
        previous = {entry['path']: entry['previous'] for entry in record['changes']}
        self.assertEqual(previous, {P.LIVE_FLEET: OLD_ADAPTER.decode(), P.HISTORY_FLEET: self.fleet})
        self.assertFalse(record['events'][-2]['other_changes'])
        self.assertTrue(any(m[2].startswith(P.TRANSACTIONS + '/' + poc.transaction.id + '/files/')
                            for m in self.fake.mutations('coordinator') if m[1] == 'write'))

        rollback = self.poc()
        rollback.rollback(poc.transaction.id)
        self.assertEqual(self.fake.files[('coordinator', P.LIVE_FLEET)], OLD_ADAPTER)
        self.assertEqual(self.fake.modes[('coordinator', P.LIVE_FLEET)], 0o755)
        self.assertEqual(self.fake.text('coordinator', P.HISTORY_FLEET), self.fleet)
        self.assertEqual(self.fake.text('router-01', P.CADDYFILE), ROUTER)
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'rolled-back')

    def test_rollback_never_puts_older_bytes_over_another_writer(self):
        poc = self.poc()
        poc.deploy('router-hook', poc.plan_sha256())
        # A history publisher redeploy rewrites fleet.json after the hook.
        redeployed = json.dumps({**json.loads(self.fake.text('coordinator', P.HISTORY_FLEET)),
                                 'managed_recent_workers': ['recent-02']})
        self.fake.put('coordinator', P.HISTORY_FLEET, redeployed, 0o600)
        before = len(self.fake.mutations())
        with self.assertRaisesRegex(P.PocError, 'rollback refused before any change: %s on coordinator changed'
                                    % P.HISTORY_FLEET):
            self.poc().rollback(poc.transaction.id)
        self.assertEqual(self.fake.mutations()[before:], [])
        self.assertEqual(self.fake.text('coordinator', P.HISTORY_FLEET), redeployed)
        self.assertEqual(self.fake.text('coordinator', P.LIVE_FLEET),
                         (ROOT / 'transparent/ops/scripts/transparent-live-fleet.py').read_text())
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'committed')
        # Put back the bytes the hook wrote, and the same rollback proceeds.
        self.fake.put('coordinator', P.HISTORY_FLEET, json.dumps({**json.loads(self.fleet), 'route_imports': [GLOB]}),
                      0o600)
        self.poc().rollback(poc.transaction.id)
        self.assertEqual(self.fake.text('coordinator', P.HISTORY_FLEET), self.fleet)
        self.assertEqual(self.fake.files[('coordinator', P.LIVE_FLEET)], OLD_ADAPTER)

    def test_hook_refuses_an_unreviewed_adapter_or_a_rejected_composition(self):
        self.fake.put('coordinator', P.LIVE_FLEET, b'something else', 0o755)
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'neither the reviewed one'):
            poc.deploy('router-hook', poc.plan_sha256())
        self.fake.put('coordinator', P.LIVE_FLEET, OLD_ADAPTER, 0o755)
        self.fake.caddy_rejects = lambda path, config: IMPORT in config
        with self.assertRaisesRegex(P.PocError, 'caddy rejected'):
            poc.deploy('router-hook', poc.plan_sha256())
        self.assertEqual(self.fake.files[('coordinator', P.LIVE_FLEET)], OLD_ADAPTER)
        self.assertEqual(self.fake.text('coordinator', P.HISTORY_FLEET), self.fleet)
        self.assertFalse([m for m in self.fake.mutations() if m[1] == 'systemctl'])

    def test_hook_waits_for_the_history_render_and_fails_without_it(self):
        self.fake.reconciler_renders = False
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'import hook did not happen'):
            poc.deploy('router-hook', poc.plan_sha256())
        self.assertGreaterEqual(self.clock.t, 900)


class DeployedFleet(Base):
    """A coordinator with a bootstrap candidate and the router hook in place."""

    def setUp(self):
        super().setUp()
        self.fake.put('coordinator', CANDIDATE + '/txid-shards.json', b'map')
        self.map = sha256(b'map')
        self.fake.put('router-01', P.CADDYFILE, ROUTER.replace('\t@metadata', IMPORT + '\n\t@metadata'))
        self.fake.dirs.add(('router-01', '/etc/caddy/txid-display'))

    def deploy(self, phase, **options):
        poc = self.poc()
        poc.deploy(phase, poc.plan_sha256(), **options)
        return poc


class WorkerTests(DeployedFleet):
    def test_workers_ship_verify_install_prepare_activate_and_wait_warm(self):
        poc = self.deploy('workers', map_sha256=self.map)
        files = poc.render()
        for key in P.WORKERS:
            host = HOSTS[key]
            self.assertEqual(self.fake.text(host, '/etc/systemd/system/' + P.WORKER_UNIT), files['unit:' + key])
            self.assertEqual(self.fake.unit(host, P.WORKER_UNIT)['active'], 'active')
            self.assertTrue(self.fake.unit(host, P.WORKER_UNIT)['enabled'])
            self.assertEqual(self.fake.active[key], self.map)
            verify = [argv for argv in self.fake.commands(host) if argv[0] == 'systemd-run']
            self.assertEqual(len(verify), 1)
            self.assertEqual(verify[0][-7:], ['--role', P.ROLES[key], '--publication-dir',
                                              '%s/%s' % (P.PUBLICATIONS, self.map), '--cache-bytes',
                                              str(self.request['units'][key]['cache_bytes']), '--verify-only'])
            self.assertIn('--property=MemoryMax=' + self.request['units'][key]['memory_max'], verify[0])
            order = [m[2] for m in self.fake.mutations(host) if m[1] == 'systemctl']
            self.assertEqual(order, [('daemon-reload',), ('enable', P.WORKER_UNIT), ('restart', P.WORKER_UNIT)])
        self.assertEqual(self.fake.text('coordinator', P.FLEET_JSON), files['fleet.json'])
        # Archive owner first, completely, before the recent replica.
        hosts = [m[0] for m in self.fake.mutations() if m[1] == 'systemctl']
        self.assertEqual(hosts, ['archive-03'] * 3 + ['recent-01'] * 3)
        self.assertEqual([path for h, path in self.fake.files if path.startswith(P.REQUESTS)], [])
        events = [event['message'] for event in self.journal(poc.transaction.id)['events']]
        self.assertIn('archive: serving %s warm' % self.map, events)
        self.assertLess(events.index('archive: serving %s warm' % self.map),
                        events.index('recent: shipped candidate %s' % self.map))

        poc.rollback(poc.transaction.id)
        for key in P.WORKERS:
            host = HOSTS[key]
            self.assertIsNone(self.fake.text(host, '/etc/systemd/system/' + P.WORKER_UNIT))
            self.assertEqual(self.fake.unit(host, P.WORKER_UNIT)['active'], 'inactive')
            self.assertFalse(self.fake.unit(host, P.WORKER_UNIT)['enabled'])
        self.assertIsNone(self.fake.text('coordinator', P.FLEET_JSON))
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'rolled-back')

    def test_workers_need_the_reviewed_candidate_and_release_binary(self):
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'no candidate'):
            poc.deploy('workers', poc.plan_sha256(), map_sha256='e' * 64)
        self.fake.worker_binary = 'f' * 64
        with self.assertRaisesRegex(P.PocError, 'not the release'):
            poc.deploy('workers', poc.plan_sha256(), map_sha256=self.map)
        record = self.journal(poc.transaction.id)
        self.assertEqual(record['status'], 'failed')
        poc.rollback(poc.transaction.id)
        self.assertIsNone(self.fake.text('archive-03', '/etc/systemd/system/' + P.WORKER_UNIT))


class RouteTests(DeployedFleet):
    def test_route_validates_a_scratch_composition_then_reloads(self):
        poc = self.deploy('route')
        snippet = poc.render()['routes.caddy']
        self.assertEqual(self.fake.text('router-01', '/etc/caddy/txid-display/routes.caddy'), snippet)
        configs = [argv[3] for argv in self.fake.commands('router-01') if argv[0] == 'caddy']
        scratch = '%s/%s' % (P.VALIDATE, poc.transaction.id)
        self.assertEqual(configs, [scratch + '/Caddyfile', P.CADDYFILE])
        self.assertNotIn(('router-01', scratch + '/Caddyfile'), self.fake.files)
        self.assertIn(('router-01', 'systemctl', ('reload', 'caddy')), self.fake.mutations('router-01'))
        poc.rollback(poc.transaction.id)
        self.assertIsNone(self.fake.text('router-01', '/etc/caddy/txid-display/routes.caddy'))
        self.assertEqual([m[2] for m in self.fake.mutations('router-01') if m[1] == 'systemctl'],
                         [('reload', 'caddy')] * 2)

    def test_a_rejected_live_router_never_keeps_the_snippet(self):
        self.fake.caddy_rejects = lambda path, config: path == P.CADDYFILE
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'caddy rejected the live router'):
            poc.deploy('route', poc.plan_sha256())
        self.assertIsNone(self.fake.text('router-01', '/etc/caddy/txid-display/routes.caddy'))

    def test_route_needs_the_hook(self):
        self.fake.put('router-01', P.CADDYFILE, ROUTER)
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'deploy router-hook first'):
            poc.deploy('route', poc.plan_sha256())


class ControllerTests(DeployedFleet):
    def setUp(self):
        super().setUp()
        for key in P.WORKERS:
            self.fake.put(HOSTS[key], '/etc/systemd/system/' + P.WORKER_UNIT, 'unit')
            self.fake.unit(HOSTS[key], P.WORKER_UNIT).update(load='loaded', active='active', sub='running')

    def test_controller_installs_configs_and_unit_and_checks_stability(self):
        poc = self.deploy('controller')
        files = poc.render()
        self.assertEqual(self.fake.text('coordinator', P.WORKERS_JSON), files['workers.json'])
        self.assertEqual(self.fake.text('coordinator', '/etc/systemd/system/' + P.CONTROLLER_UNIT),
                         files['unit:controller'])
        self.assertEqual(self.fake.unit('coordinator', P.CONTROLLER_UNIT)['active'], 'active')
        poc.rollback(poc.transaction.id)
        self.assertIsNone(self.fake.text('coordinator', P.WORKERS_JSON))
        self.assertEqual(self.fake.unit('coordinator', P.CONTROLLER_UNIT)['active'], 'inactive')

    def test_controller_refuses_while_another_journal_writer_runs(self):
        self.fake.unit('coordinator', P.INGEST_UNIT).update(load='loaded', active='active', sub='running')
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'only journal writer'):
            poc.deploy('controller', poc.plan_sha256())
        self.assertIsNone(self.fake.text('coordinator', P.WORKERS_JSON))


class FirewallTests(Base):
    def setUp(self):
        super().setUp()
        for name in P.TF_SOURCES:
            self.fake.put('coordinator', '%s/%s' % (TF_ROOT, name),
                          (ROOT / 'ops/infra/digitalocean/production' / name).read_bytes())

    def test_two_step_plan_then_apply_the_reviewed_plan_unlocked(self):
        poc = self.poc()
        self.fake.summaries = [poc.expected_firewall(True)]
        poc.deploy('firewall', poc.plan_sha256())
        tfvars = '%s/%s' % (TF_ROOT, P.TFVARS)
        self.assertEqual(self.fake.text('coordinator', tfvars), poc.render()['tfvars'])
        record = self.journal(poc.transaction.id)
        self.assertEqual(record['status'], 'planned')
        digest = record['terraform']['plan_sha256']
        self.assertEqual(digest, sha256(b'saved plan with secrets'))
        self.assertIn('review the saved plan, then re-run with --terraform-plan-sha256 ' + digest, self.output)
        self.assertEqual(self.fake.applied, [])
        apply = self.poc()
        with self.assertRaisesRegex(P.PocError, 'must equal the reviewed saved plan'):
            apply.deploy('firewall', apply.plan_sha256(), terraform_plan_sha256='f' * 64)
        apply.deploy('firewall', apply.plan_sha256(), terraform_plan_sha256=digest)
        self.assertEqual(self.fake.applied, [record['terraform']['plan_file']])
        self.assertNotIn(('coordinator', record['terraform']['plan_file']), self.fake.files)
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'committed')

        # Rolling back an applied rule plans and applies its removal the same way.
        self.fake.summaries = [poc.expected_firewall(False)]
        rollback = self.poc()
        rollback.rollback(poc.transaction.id)
        self.assertIsNone(self.fake.text('coordinator', tfvars))
        removal = self.journal(rollback.transaction.id)
        self.assertEqual((removal['action'], removal['status']), ('firewall-rollback', 'planned'))
        rollback = self.poc()
        rollback.rollback(poc.transaction.id, removal['terraform']['plan_sha256'])
        self.assertEqual(len(self.fake.applied), 2)
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'rolled-back')

    def test_any_other_change_is_refused_and_the_variable_file_restored(self):
        poc = self.poc()
        self.fake.summaries = [{'changes': [{'address': P.FIREWALL, 'actions': ['update']},
                                            {'address': 'digitalocean_droplet.coordinator', 'actions': ['delete', 'create']}],
                                'added': poc.expected_firewall(True)['added'], 'removed': []}]
        with self.assertRaisesRegex(P.PocError, 'not exactly one in-place worker firewall change'):
            poc.deploy('firewall', poc.plan_sha256())
        self.assertIsNone(self.fake.text('coordinator', '%s/%s' % (TF_ROOT, P.TFVARS)))
        self.assertEqual([path for h, path in self.fake.files if path.endswith('.tfplan')], [])
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'rolled-back')
        self.assertEqual(self.fake.applied, [])

    def test_a_rule_already_in_place_needs_no_apply(self):
        poc = self.poc()
        self.fake.summaries = [{'changes': [], 'added': [], 'removed': []}]
        poc.deploy('firewall', poc.plan_sha256())
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'committed')
        self.assertEqual([path for h, path in self.fake.files if path.endswith('.tfplan')], [])
        self.assertEqual(self.fake.applied, [])

    def test_the_coordinator_root_must_carry_these_sources(self):
        self.fake.put('coordinator', TF_ROOT + '/transparent.tf', b'older root')
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'differs from this checkout'):
            poc.deploy('firewall', poc.plan_sha256())
        self.assertEqual(self.fake.applied, [])

    def test_the_variable_rule_is_off_by_default(self):
        text = (ROOT / 'ops/infra/digitalocean/production/transparent.tf').read_text()
        rule = text[text.index('dynamic "inbound_rule" {\n    for_each = var.transparent_txid_display_port_enabled'):]
        rule = rule[:rule.index('\n  }\n')]
        self.assertIn('? [%d] : []' % P.WORKER_PORT, rule)
        self.assertIn('source_tags = [digitalocean_tag.coordinator.name, digitalocean_tag.transparent_router.name]',
                      rule)
        variables = (ROOT / 'ops/infra/digitalocean/production/variables.tf').read_text()
        block = variables[variables.index('variable "%s"' % P.TF_VARIABLE):]
        self.assertIn('default     = false', block[:block.index('\n}\n')])
        for tag, name in (('coordinator', 'wallet-pir-coordinator'), ('transparent_router', 'transparent-pir-router')):
            source = (ROOT / 'ops/infra/digitalocean/production' / ('shared.tf' if tag == 'coordinator'
                                                                     else 'transparent.tf')).read_text()
            self.assertIn('resource "digitalocean_tag" "%s" {\n  name = "%s"' % (tag, name), source)
            self.assertIn(name, P.FIREWALL_TAGS)


class TransientTests(Base):
    def test_ingest_start_smoke_status_and_stop(self):
        poc = self.poc()
        poc.act('ingest-start', poc.plan_sha256(), smoke=True)
        smoke = [argv for argv in self.fake.commands('coordinator') if argv[0] == 'systemd-run']
        self.assertEqual(smoke, [poc.plan()['transient']['ingest-smoke']])
        self.fake.wait_results[P.SMOKE_UNIT] = (1, 'state directory format mismatch')
        with self.assertRaisesRegex(P.PocError, 'smoke ingest failed'):
            self.poc().act('ingest-start', poc.plan_sha256(), smoke=True)
        poc = self.poc()
        poc.act('ingest-start', poc.plan_sha256())
        self.assertEqual(self.fake.unit('coordinator', P.INGEST_UNIT)['active'], 'active')
        with self.assertRaisesRegex(P.PocError, 'stop it first'):
            self.poc().act('ingest-start', poc.plan_sha256())
        status = self.poc().ingest_status()
        self.assertEqual(status[P.INGEST_UNIT]['active'], 'active')
        self.assertEqual(status[P.INGEST_UNIT]['log'], ['ingest progress height=3290000'])
        stop = self.poc()
        stop.act('ingest-stop')
        self.assertEqual(self.fake.unit('coordinator', P.INGEST_UNIT)['load'], 'not-found')
        self.assertEqual(self.journal(stop.transaction.id)['undo'], [])

    def test_bootstrap_uses_plan_start_unless_given_a_range(self):
        self.fake.wait_results['transparent-txid-display-plan-start.service'] = (
            0, 'counting\n{"start": 3300000, "through": 3500000, "journal_start": 3289805, '
               '"archives_at_through": 15, "replay_seals": 4, "expected_drops": 0}\n')
        poc = self.poc()
        poc.act('bootstrap', poc.plan_sha256())
        argv = [a for a in self.fake.commands('coordinator') if a[0] == 'systemd-run'][-1]
        start = argv.index('--start')
        self.assertEqual(argv[start:start + 4], ['--start', '3300000', '--through', '3500000'])
        self.assertEqual(self.journal(poc.transaction.id)['bootstrap'], {'start': 3300000, 'through': 3500000})
        self.fake.units.clear()
        with self.assertRaisesRegex(P.PocError, 'bootstrap start height'):
            self.poc().act('bootstrap', poc.plan_sha256(), start=3289805, through=3500000)
        with self.assertRaisesRegex(P.PocError, 'or neither'):
            self.poc().act('bootstrap', poc.plan_sha256(), start=3300000)

    def test_measure_start_and_stop(self):
        poc = self.poc()
        poc.act('measure-start', poc.plan_sha256())
        run = self.journal(poc.transaction.id)['measure']['run']
        observer = [a for a in self.fake.commands('coordinator') if a[0] == 'systemd-run'][0]
        self.assertIn('/srv/zakura/txid-display-poc/measure/%s/observer.jsonl' % run, observer)
        self.assertIn('--property=MemoryMax=256M', observer)
        for unit in (P.OBSERVER_UNIT, P.MAPWATCH_UNIT):
            self.assertEqual(self.fake.unit('coordinator', unit)['active'], 'active')
        self.poc().act('measure-stop')
        for unit in (P.OBSERVER_UNIT, P.MAPWATCH_UNIT):
            self.assertEqual(self.fake.unit('coordinator', unit)['active'], 'inactive')

    def test_verify_reports_the_controller_result(self):
        poc = self.poc()
        poc.act('verify', poc.plan_sha256())
        self.fake.wait_results['transparent-txid-display-verify.service'] = (1, 'sealed digest 7 differs')
        with self.assertRaisesRegex(P.PocError, 'sealed digest 7 differs'):
            self.poc().act('verify', poc.plan_sha256())


class StopAndRetireTests(DeployedFleet):
    def setUp(self):
        super().setUp()
        poc = self.poc()
        self.routes = poc.render()['routes.caddy']
        self.fake.put('router-01', '/etc/caddy/txid-display/routes.caddy', self.routes)
        for key, unit in (('archive', P.WORKER_UNIT), ('recent', P.WORKER_UNIT), ('coordinator', P.CONTROLLER_UNIT)):
            self.fake.put(HOSTS[key], '/etc/systemd/system/' + unit, poc.render()['unit:' + ('controller' if key ==
                                                                                            'coordinator' else key)])
            self.fake.unit(HOSTS[key], unit).update(load='loaded', active='active', sub='running', enabled=True,
                                                     fragment='/etc/systemd/system/' + unit)

    def test_stop_withdraws_then_stops_the_controller_and_rolls_back(self):
        poc = self.poc()
        poc.act('stop')
        self.assertEqual(self.fake.text('router-01', '/etc/caddy/txid-display/routes.caddy'), P.WITHDRAWN)
        self.assertEqual(self.fake.unit('coordinator', P.CONTROLLER_UNIT)['active'], 'inactive')
        router = [m for m in self.fake.mutations() if m[0] == 'router-01' and m[1] == 'systemctl']
        controller = [i for i, m in enumerate(self.fake.mutations()) if m[2] == ('stop', P.CONTROLLER_UNIT)]
        self.assertLess(self.fake.mutations().index(router[0]), controller[0])
        record = self.journal(poc.transaction.id)
        self.assertLess(record['stop_seconds'], P.STOP_TARGET_SECONDS)
        poc.rollback(poc.transaction.id)
        self.assertEqual(self.fake.text('router-01', '/etc/caddy/txid-display/routes.caddy'), self.routes)
        self.assertEqual(self.fake.unit('coordinator', P.CONTROLLER_UNIT)['active'], 'active')

    def test_stop_still_stops_the_controller_when_caddy_refuses(self):
        self.fake.reload_fails = True
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'controller stopped, but the route was not withdrawn'):
            poc.act('stop')
        self.assertEqual(self.fake.text('router-01', '/etc/caddy/txid-display/routes.caddy'), self.routes)
        self.assertEqual(self.fake.unit('coordinator', P.CONTROLLER_UNIT)['active'], 'inactive')

    def test_retire_removes_units_route_and_hook_keeps_data_and_closes_the_firewall(self):
        hook = self.poc()
        self.fake.put('coordinator', P.LIVE_FLEET, OLD_ADAPTER, 0o755)
        self.fake.put('coordinator', P.HISTORY_FLEET, json.dumps({'public_host': 'pir.example'}), 0o600)
        self.fake.put('router-01', P.CADDYFILE, ROUTER)
        hook.deploy('router-hook', hook.plan_sha256())
        self.fake.put('coordinator', '%s/%s' % (TF_ROOT, P.TFVARS), hook.render()['tfvars'], 0o600)
        self.fake.unit('coordinator', P.MAPWATCH_UNIT).update(load='loaded', active='active', sub='running',
                                                              fragment='/run/systemd/transient/' + P.MAPWATCH_UNIT)
        poc = self.poc()
        self.fake.summaries = [poc.expected_firewall(False)]
        poc.retire(poc.plan_sha256(), None)
        for key, unit in (('archive', P.WORKER_UNIT), ('recent', P.WORKER_UNIT), ('coordinator', P.CONTROLLER_UNIT)):
            self.assertIsNone(self.fake.text(HOSTS[key], '/etc/systemd/system/' + unit))
            self.assertFalse(self.fake.unit(HOSTS[key], unit)['enabled'])
            mutations = [m[2] for m in self.fake.mutations(HOSTS[key])]
            self.assertLess(mutations.index(('disable', unit)), mutations.index('/etc/systemd/system/' + unit))
        self.assertEqual(self.fake.unit('coordinator', P.MAPWATCH_UNIT)['load'], 'not-found')
        self.assertIsNone(self.fake.text('router-01', '/etc/caddy/txid-display/routes.caddy'))
        self.assertEqual(self.fake.files[('coordinator', P.LIVE_FLEET)], OLD_ADAPTER)
        self.assertNotIn('route_imports', json.loads(self.fake.text('coordinator', P.HISTORY_FLEET)))
        self.assertIsNone(self.fake.text('coordinator', '%s/%s' % (TF_ROOT, P.TFVARS)))
        record = self.journal(poc.transaction.id)
        self.assertEqual(record['status'], 'planned')
        self.assertIn('  coordinator:/srv/zakura/txid-display-poc/journal', self.output)
        self.assertIn('  archive-03:/srv/transparent-txid-display', self.output)
        self.assertFalse([m for m in self.fake.mutations() if m[1] == 'remove' and m[2].startswith('/srv/')])
        apply = self.poc()
        apply.retire(apply.plan_sha256(), record['terraform']['plan_sha256'])
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'committed')

        # Retirement itself rolls back from its journal: units, route and hook return.
        restore = self.poc()
        restore.rollback(poc.transaction.id)
        self.assertEqual(self.fake.text('router-01', '/etc/caddy/txid-display/routes.caddy'), self.routes)
        for key, unit in (('archive', P.WORKER_UNIT), ('coordinator', P.CONTROLLER_UNIT)):
            self.assertEqual(self.fake.unit(HOSTS[key], unit)['active'], 'active')
            self.assertTrue(self.fake.unit(HOSTS[key], unit)['enabled'])


class CommandLineTests(Base):
    def run_cli(self, *argv):
        inventory_path = Path(self.tmp.name) / 'inventory.json'
        inventory_path.write_text(json.dumps({
            'hosts': {name: {} for name in HOSTS.values()}, 'ssh': {'mode': 'config'},
            'lock': {'type': 'remote', 'host': 'coordinator'}}))
        request_path = Path(self.tmp.name) / 'request.json'
        request_path.write_text(json.dumps(self.request))
        lines = []
        code = cli.main(['--inventory', str(inventory_path), '--state-dir', str(self.state), *argv,
                         '--request', str(request_path), '--request-sha256', durable.digest(self.request)],
                        executor=self.fake, out=lines.append, sleep=self.clock.sleep, clock=self.clock,
                        now=lambda: NOW)
        return code, lines

    def test_plan_prints_its_digest_and_deploy_prints_status_and_rollback(self):
        code, lines = self.run_cli('txid-display-plan')
        self.assertEqual(code, 0)
        digest = lines[-1].removeprefix('plan sha256: ')
        self.assertEqual(digest, durable.digest(json.loads(lines[0])))
        self.fake.put('router-01', P.CADDYFILE, ROUTER.replace('\t@metadata', IMPORT + '\n\t@metadata'))
        code, lines = self.run_cli('txid-display-deploy', '--phase', 'route', '--expect-plan-sha256', digest)
        self.assertEqual(code, 0, lines)
        rollback = [line for line in lines if line.startswith('rollback: ')]
        self.assertEqual(len(rollback), 1)
        self.assertIn('txid-display-rollback', rollback[0])
        self.assertIn('--transaction txid-display-deploy-route-', rollback[0])
        status = json.loads(next(line for line in lines if line.startswith('{') and '"files"' in line))
        self.assertEqual(status['files']['routes.caddy'], 'planned')
        code, lines = self.run_cli('txid-display-deploy', '--phase', 'route', '--expect-plan-sha256', 'f' * 64)
        self.assertEqual(code, 1)
        self.assertTrue(lines[-1].startswith('error: plan changed since review'))

    def test_preflight_exit_status(self):
        code, lines = self.run_cli('txid-display-preflight')
        self.assertEqual((code, lines[-1]), (0, 'preflight passed'))
        self.fake.history['freshness_seconds'] = 31
        code, lines = self.run_cli('txid-display-preflight')
        self.assertEqual((code, lines[-1]), (1, 'preflight refused'))

    def test_every_command_is_closed_and_requires_the_request(self):
        parser = cli.parser()
        for name in cli.TXID_DISPLAY:
            with self.subTest(name=name), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser.parse_args(['txid-display-' + name])
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(['txid-display-deploy', '--request', 'r', '--request-sha256', 'h',
                               '--expect-plan-sha256', 'h', '--phase', 'everything'])
        self.assertEqual(cli.TXID_DISPLAY_PHASES, P.PHASES)


if __name__ == '__main__':
    unittest.main()
