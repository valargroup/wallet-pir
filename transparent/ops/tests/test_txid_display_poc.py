#!/usr/bin/env python3
"""The txid display deploy family against an in-memory fleet.

Only hosts are fake: requests, rendering, journals, the release bundle and the
CLI are the repository's own. The fake models files, systemd units, the
adapter on the coordinator and the site host's Caddy.
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
HOSTS = {'coordinator': 'coordinator', 'archive': 'archive-03'}
# A host that ran an earlier recent-replica worker, for hosts.former_recent.
FORMER_HOST = 'recent-01'
VPC = {'archive': '10.142.0.5'}
LOAD = '/opt/transparent-5qps-20260929/status.json'
ADAPTER = '/srv/transparent-activity/ops/sources/%s/transparent/ops/scripts/transparent-live-fleet.py' % ('e' * 40)
FLEET = '/opt/transparent-publisher/v11/fleet.json'
MEMBERSHIP = '/opt/transparent-publisher/v11/state/membership.json'
HISTORY_EXEC = '{ path=/usr/bin/python3 ; argv[]=/usr/bin/python3 -B %s %s --reconcile }' % (ADAPTER, FLEET)
MAP = 'c' * 64
CANDIDATE = '/srv/zakura/txid-display-poc/root/candidate-100-%s-1' % ('d' * 64)
SITE = {'name': 'k8', 'hostname': 'txid.example'}
N = P.names(SITE)
RELEASE_DIR = '/opt/transparent-txid-display-k8/releases/' + SHA
# The site host's Caddyfile before the first route.
DEFAULT_CADDY = ':80 {\n\troot * /usr/share/caddy\n\tfile_server\n}\n'


def sha256(data):
    return hashlib.sha256(data.encode() if isinstance(data, str) else data).hexdigest()


def binary_bytes(name):
    return ('#!/bin/sh\n# %s built for the test\n' % name).encode()


def request():
    """A site: its own hostname, coordinator names and journal, one combined worker."""
    return {
        'schema': P.SCHEMA, 'source_sha': SHA,
        'release': {'archive_sha256': 'b' * 64, 'ci_run': 7,
                    'binaries': {name: sha256(binary_bytes(name)) for name in P.BINARIES},
                    'files': {name: sha256((ROOT / path).read_bytes()) for name, path in P.RELEASE_FILES.items()}},
        'hosts': {'coordinator': {'inventory': 'coordinator'},
                  'archive': {'inventory': 'archive-03', 'vpc_ip': VPC['archive']}},
        'ports': {'worker': 8095, 'controller_status': 8098},
        'units': {
            'archive': {'cache_bytes': 4 << 30, 'memory_max': '6G', 'cpu_quota': '200%', 'cpu_weight': 20, 'nice': 10,
                        'oom_score_adjust': 1000, 'build_threads': 2, 'build_slots': 1, 'query_slots': 2,
                        'retain_revisions': 2, 'disk_cache_bytes': 8 << 30, 'ready_timeout_seconds': 1800},
            'controller': {'memory_max': '4G', 'cpu_quota': '200%', 'cpu_weight': 20, 'nice': 10},
            'ingest': {'memory_max': '12G', 'cpu_quota': '400%', 'cpu_weight': 20, 'io_weight': 20, 'nice': 10,
                       'workers': 4},
            'bootstrap': {'memory_max': '8G', 'cpu_quota': '400%', 'cpu_weight': 20, 'nice': 10}},
        'journal': {'data_dir': '/srv/zakura/txid-display-poc/journal', 'start_height': 3289805,
                    'node_cache_dir': '/root/.cache/zakura'},
        'publication': {'root': '/srv/zakura/txid-display-poc/root'},
        'controller': {'mode': 'replay-then-live', 'blocks_per_step': 25, 'step_interval_ms': 2000,
                       'geometry': 'txid-2k8', 'n_archive': 1, 'n_recent': 1, 'archive_target': 90000,
                       'recent_window': 90000, 'reorg_margin': 100, 'max_archive_shards': 24, 'archives': 15,
                       'replay_seals': 4, 'rpc_url': 'http://127.0.0.1:8232',
                       'rpc_cookie': '/root/.cache/zakura/.cookie'},
        'site': dict(SITE),
        'history': {'load_status': LOAD, 'adapter': ADAPTER, 'fleet': FLEET, 'membership': MEMBERSHIP},
        'measure': {'output': '/srv/zakura/txid-display-poc/measure', 'public_url': 'https://txid.example'},
        'baseline': {'coordinator': {'mem_available_bytes': 48 << 30}, 'archive': {'mem_available_bytes': 12 << 30}},
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
        self.active, self.prepared = {}, {}
        # Prepare calls that give up before the worker finishes (txid-control's 600 s wait).
        self.slow_prepares = 0
        self.preparing = {}
        self.calls = []
        self.history = {'freshness_seconds': 3, 'ready_replicas': 2}
        self.worker_binary = sha256(binary_bytes('transparent-txid-server'))
        # The role each worker reports.
        self.roles = dict(P.ROLES)
        # Whether a site's Caddy holds its certificate yet.
        self.certificate = True

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
        self.put(coordinator, MEMBERSHIP, json.dumps({'routed_recent': 2, 'updated_unix': now}))
        self.put(coordinator, LOAD, json.dumps({'mode': 'running', 'utc': '2027-01-15T08:00:00+00:00',
                                                'trailing_60s': {'exact': 300, 'errors': 0}}))
        for host in (*HOSTS.values(), FORMER_HOST):
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
            worker = next((key for key, name in HOSTS.items() if name == host and key in P.WORKERS), None)
            if unit == P.WORKER_UNIT and worker is not None:
                # A worker starts from its durable record, or serves nothing.
                record = self.text(host, P.ACTIVE_RECORD)
                if record is None:
                    self.active.pop(worker, None)
                else:
                    self.active[worker] = json.loads(record)['map_sha256']
            if unit == N.CONTROLLER_UNIT:
                self.listen.setdefault(host, []).append('127.0.0.1:8098')
        elif verb == 'reload':
            if self.reload_fails:
                raise RemoteError('caddy reload failed')
        else:
            raise AssertionError('unexpected systemctl %s' % (args,))

    def http_get(self, host, url, timeout=5):
        if url == P.HISTORY_STATUS:
            return 200, json.dumps(self.history)
        for key, address in VPC.items():
            if url == 'http://%s:8095/v1/ready' % address:
                active = self.unit(HOSTS[key], P.WORKER_UNIT)['active'] == 'active'
                if not active or key not in self.active:
                    return 503, '{}'
                return 200, json.dumps({'ready': True, 'role': self.roles[key], 'map_sha256': self.active[key],
                                        'warm': True})
        if url == 'https://txid.example/v2/txid/map':
            site = self.text('archive-03', P.CADDYFILE) or ''
            served = 'txid.example {' in site and 'handle /v2/txid/archive/*' in site
            return (200, json.dumps({'chunks': []})) if served and self.certificate else (0, 'TLS handshake failed')
        return 0, 'connection refused'

    def adapter(self, host, request):
        worker = request['worker']
        name = HOSTS[worker]
        self.calls.append((worker, request['command']['operation'] if 'command' in request else request['operation']))
        if request['operation'] == 'ship':
            assert request['source'] == CANDIDATE and request['kind'] == 'candidate'
            assert request['deadline_seconds'] == P.SHIP_SECONDS
            directory = '%s/%s' % (P.PUBLICATIONS, request['name'])
            self.dirs.add((name, directory))
            return {'ok': True, 'directory': directory, 'seconds': 0.5}
        assert request.get('deadline_seconds', 85) <= 600, 'beyond txid-control\'s own wait'
        command = request['command']
        if self.unit(name, P.WORKER_UNIT)['active'] != 'active':
            return {'ok': False, 'error': 'control socket refused the connection'}
        operation = command['operation']
        if operation == 'status':
            active = self.active.get(worker)
            reply = {'ok': True, 'role': self.roles[worker], 'warm': active is not None, 'staged': [],
                     'binary_sha256': self.worker_binary,
                     'active': {'directory': '%s/%s' % (P.PUBLICATIONS, active), 'map_sha256': active}
                     if active else None,
                     'preparing': {'map_sha256': self.preparing[worker]} if worker in self.preparing else None}
        elif operation == 'prepare':
            assert command['expected'] == (self.active.get(worker) or '')
            if self.slow_prepares:
                self.slow_prepares -= 1
                self.preparing[worker] = command['publication']['map_sha256']
                return {'ok': False, 'error': 'FleetError: worker sent no control reply'}
            self.preparing.pop(worker, None)
            self.prepared[worker] = command['publication']['map_sha256']
            reply = {'ok': True, 'warm': True, 'built': 2, 'reused': 0, 'seconds': 1.0}
        elif operation == 'activate':
            assert self.prepared.get(worker) == command['map_sha256']
            self.serve(worker, command['map_sha256'])
            reply = {'ok': True}
        else:
            raise AssertionError(operation)
        return {'ok': reply['ok'], 'reply': reply, 'seconds': 0.1}

    def serve(self, worker, map_sha256):
        """A worker's activation, with the durable record it restarts from."""
        self.active[worker] = map_sha256
        self.put(HOSTS[worker], P.ACTIVE_RECORD, json.dumps({
            'directory': '%s/%s' % (P.PUBLICATIONS, map_sha256), 'map_sha256': map_sha256}), 0o600)

    def run(self, host, argv, timeout):
        self.runs.append((host, list(argv)))
        command = argv[0]
        if command.endswith('/txid-control'):
            # `txid-control SOCKET < command.json`: argv[1] is the socket and
            # stdin is empty here, so even `--help` fails to parse a command.
            if len(argv) == 1:
                return 1, 'Error: "usage: txid-control SOCKET < command.json"\n'
            return 1, 'Error: Error("EOF while parsing a value", line: 1, column: 0)\n'
        if command == 'systemctl' and argv[1] == 'show' and 'ExecStart' in argv:
            default = HISTORY_EXEC if argv[2] in P.HISTORY_DAEMONS else ''
            return 0, self.unit(host, argv[2]).get('exec', default) + '\n'
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
        if argv[-1] == '--help':
            return 0, 'Usage: ...'
        if command == '/usr/bin/python3' and argv[1].endswith('txid-display-fleet.py'):
            assert argv[2] == N.FLEET_JSON
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
    return descriptors.Inventory(hosts={name: {} for name in (*HOSTS.values(), FORMER_HOST)}, ssh={'mode': 'config'},
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
            (('units', 'archive', 'cpu_weight'), 100), (('units', 'archive', 'nice'), 0),
            (('units', 'archive', 'oom_score_adjust'), 0), (('units', 'archive', 'cache_bytes'), 8 << 30),
            (('controller', 'recent_window'), 9999), (('controller', 'archive_target'), 5000),
            (('controller', 'reorg_margin'), 99), (('controller', 'n_archive'), 65), (('controller', 'n_recent'), 0),
            (('controller', 'max_archive_shards'), 0), (('controller', 'archives'), 0),
            (('controller', 'max_archive_shards'), 1025), (('controller', 'archives'), 1025),
            (('journal', 'start_height'), -1),
            (('controller', 'n_buckets'), 1), (('controller', 'replay_seals'), KeyError),
            (('controller', 'mode'), 'fast'), (('controller', 'rpc_url'), 'http://10.0.0.1:8232'),
            (('ports', 'worker'), 8096), (('ports', 'controller_status'), 8094),
            (('hosts', 'archive', 'inventory'), 'coordinator'), (('hosts', 'archive', 'vpc_ip'), '8.8.8.8'),
            (('release', 'binaries', 'txid-control'), KeyError), (('measure', 'public_url'), 'http://txid.example'),
            (('baseline', 'archive'), {}),
            (('history', 'adapter'), '/srv/transparent-activity/ops/sources/x/transparent-fleet-scaler.py'),
            (('history', 'fleet'), '/opt/transparent-publisher/v11/../fleet.json'), (('history', 'membership'), KeyError),
        ]
        for path, value in refused:
            with self.subTest(path=path, value=value), self.assertRaises(P.PocError):
                P.validate(mutate(path, value), inventory())
        # Genesis coverage: a journal from height 0 and a 1,000-archive window.
        for path, value in [(('journal', 'start_height'), 0), (('controller', 'max_archive_shards'), 1000),
                            (('controller', 'archives'), 426)]:
            with self.subTest(path=path, value=value):
                P.validate(mutate(path, value), inventory())
        remote_elsewhere = descriptors.Inventory(hosts={n: {} for n in HOSTS.values()}, ssh={'mode': 'config'},
                                                 lock={'type': 'remote', 'host': 'archive-03'}, services={})
        with self.assertRaisesRegex(P.PocError, 'lock host'):
            P.validate(self.request, remote_elsewhere)

    def test_a_request_must_be_a_site_with_one_combined_worker(self):
        def changed(edit):
            value = request()
            edit(value)
            return value
        retired_router = {'import_glob': '/etc/caddy/txid-display/*.caddy', 'live_fleet_sha256': 'f' * 64}
        refused = [
            # The retired /v1 history router lineage: no site, a router and Terraform.
            (lambda v: (v.pop('site'), v.update(router=retired_router)), 'request must name a site'),
            (lambda v: v.pop('site'), 'request must name a site'),
            (lambda v: v.update(router=retired_router), 'request must have exactly'),
            (lambda v: v.update(terraform={'wrapper': '/x', 'root': '/y'}), 'request must have exactly'),
            (lambda v: v['hosts'].update(router={'inventory': 'coordinator'}), 'hosts must have exactly'),
            (lambda v: v['hosts'].update(recent={'inventory': FORMER_HOST, 'vpc_ip': '10.142.0.6'}),
             'combined layout'),
            (lambda v: v['units'].update(recent=dict(v['units']['archive'])), 'combined layout'),
            (lambda v: v['baseline'].update(recent={'mem_available_bytes': 1}), 'combined layout'),
            (lambda v: v['site'].update(name='K8'), 'site.name'),
            (lambda v: v['site'].update(name=''), 'site.name'),
            (lambda v: v['site'].update(hostname='https://txid.example'), 'site.hostname'),
            (lambda v: v['site'].update(extra=1), 'site must have exactly'),
            (lambda v: v['measure'].update(public_url='https://pir.example'), 'site.s own'),
            (lambda v: v['hosts'].update({P.FORMER_RECENT: {'inventory': 'archive-03'}}), 'must be distinct hosts'),
        ]
        for edit, pattern in refused:
            with self.subTest(pattern=pattern), self.assertRaisesRegex(P.PocError, pattern):
                P.validate(changed(edit), inventory())
        with self.assertRaisesRegex(P.PocError, 'coordinator and archive must be distinct hosts'):
            P.validate(changed(lambda v: v['hosts']['archive'].update(inventory='coordinator')), inventory())

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
        changed['units']['archive']['memory_max'] = '5G'
        self.assertNotEqual(first.plan_sha256(), self.poc(changed).plan_sha256())

    def test_plan_names_its_own_coordinator_paths_units_and_site(self):
        poc = self.poc()
        files, plan = poc.render(), poc.plan()
        self.assertEqual(plan['phases'], ['stage', 'workers', 'route', 'controller'])
        self.assertEqual(sorted(plan), ['files', 'hosts', 'phases', 'release', 'release_dir', 'request_sha256',
                                        'retires', 'schema', 'site', 'source_sha', 'templates', 'transient'])
        self.assertEqual(sorted(files), ['fleet.json', 'routes.caddy', 'unit:archive', 'unit:controller',
                                         'withdrawn.caddy', 'workers.json'])
        self.assertEqual(plan['hosts'], {'coordinator': 'coordinator', 'archive': 'archive-03'})
        self.assertEqual(sorted(plan['release']), ['archive', 'coordinator'])
        self.assertEqual(plan['retires'], {})
        self.assertEqual(plan['site'], {**SITE, 'host': 'archive-03', 'opt': '/opt/transparent-txid-display-k8',
                                        'units': sorted({N.CONTROLLER_UNIT, *N.TRANSIENT})})
        self.assertEqual(plan['files']['routes.caddy']['host'], 'archive')
        self.assertEqual(plan['files']['routes.caddy']['path'], P.CADDYFILE)
        self.assertEqual(plan['files']['unit:controller']['path'],
                         '/etc/systemd/system/transparent-txid-display-k8-controller.service')
        self.assertEqual(plan['files']['fleet.json']['path'], '/opt/transparent-txid-display-k8/fleet.json')
        self.assertEqual(plan['release_dir'], RELEASE_DIR)
        controller = files['unit:controller']
        self.assertIn('ReadWritePaths=/srv/zakura/txid-display-poc/root /srv/zakura/txid-display-poc/journal '
                      '/opt/transparent-txid-display-k8/state\n', controller)
        self.assertNotIn('/opt/transparent-txid-display/', controller)
        self.assertEqual(json.loads(files['fleet.json'])['control_dir'], '/opt/transparent-txid-display-k8/state/ssh')
        transient = plan['transient']
        self.assertEqual({argv[1] for argv in transient.values()},
                         {'--unit=' + unit for unit in (*N.TRANSIENT, N.PLAN_START_UNIT, N.VERIFY_UNIT)})

    def test_rendered_units_carry_the_reviewed_limits(self):
        files = self.poc().render()
        archive, controller = files['unit:archive'], files['unit:controller']
        self.assertIn('ExecStart=%s/transparent-txid-server --listen 10.142.0.5:8095 --role combined '
                      '--cache-bytes %d' % (RELEASE_DIR, 4 << 30), archive)
        for line in ('MemoryMax=6G', 'CPUQuota=200%', 'CPUWeight=20', 'IOWeight=20', 'Nice=10',
                     'OOMScoreAdjust=1000', 'Environment=TRANSPARENT_BUILD_THREADS=2', 'MemorySwapMax=0',
                     'ProtectSystem=strict'):
            self.assertIn('\n' + line + '\n', archive)
        self.assertIn('--runtime-cache-dir /srv/transparent-txid-display/runtime-cache', archive)
        self.assertIn('--control-socket /run/transparent-txid-display/control.sock', archive)
        self.assertIn('--retain-revisions 2', archive)
        # `collect` deletes on disk only under its collect roots: the adapter's
        # publications directory, where every cycle's candidate lands, and its
        # staged directory, where sealed revisions wait to be named.
        publications = json.loads(files['fleet.json'])['workers']
        argv = [line for line in archive.splitlines() if line.startswith('ExecStart=')][0].split()
        roots = [argv[i + 1] for i, a in enumerate(argv) if a == '--collect-root']
        self.assertEqual(roots, [publications['archive']['publications'], publications['archive']['staged']])
        self.assertEqual(argv[argv.index('--active-record') + 1], P.ACTIVE_RECORD)
        for line in ('MemoryMax=4G', 'CPUQuota=200%', 'CPUWeight=20', 'Nice=10', 'ProtectSystem=strict'):
            self.assertIn('\n' + line + '\n', controller)
        self.assertIn('--workers /opt/transparent-txid-display-k8/workers.json --mode replay-then-live '
                      '--status-listen 127.0.0.1:8098', controller)
        self.assertIn('--blocks-per-step 25 --step-interval-ms 2000', controller)
        # `run` takes no seal flags: the bootstrapped root carries the rule.
        start = [line for line in controller.splitlines() if line.startswith('ExecStart=')][0]
        self.assertTrue(start.endswith('--rpc-cookie /root/.cache/zakura/.cookie --blocks-per-step 25 '
                                       '--step-interval-ms 2000'), start)
        for flag in ('--geometry', '--n-', '--archive-target', '--recent-window', '--t-archive', '--k-recent'):
            self.assertNotIn(flag, start)
        # The node's RPC cookie under /root stays readable.
        self.assertFalse([line for line in controller.splitlines() if line.startswith('ProtectHome')])
        for unit in (archive, controller):
            for path in ('/srv/transparent-pir', '/opt/transparent-publisher/state', 'control.sock\n'):
                self.assertNotIn('ReadWritePaths=' + path, unit)

    def test_rendered_configs_follow_the_transport_contract(self):
        files = self.poc().render()
        workers = json.loads(files['workers.json'])['workers']
        self.assertEqual([(w['name'], w['role']) for w in workers], [('archive', 'combined')])
        self.assertEqual(workers[0]['transport'], {
            'command': RELEASE_DIR + '/txid-display-fleet.py', 'config': '/opt/transparent-txid-display-k8/fleet.json'})
        fleet = json.loads(files['fleet.json'])
        self.assertEqual((fleet['ssh_key'], fleet['known_hosts']), (
            '/opt/transparent-publisher/credentials/deploy-ssh', '/opt/transparent-publisher/credentials/known_hosts'))
        self.assertEqual(sorted(fleet['workers']), ['archive'])
        self.assertEqual(fleet['workers']['archive']['ssh_host'], '10.142.0.5')
        adapter = load('fleet_config_check', ROOT / 'transparent/ops/scripts/txid-display-fleet.py')
        with tempfile.TemporaryDirectory() as tmp:
            config = json.loads(files['fleet.json'])
            config['control_dir'] = tmp + '/ssh'
            (Path(tmp) / 'fleet.json').write_text(json.dumps(config))
            self.assertEqual(adapter.load_config(Path(tmp) / 'fleet.json')['workers'], fleet['workers'])
        site = files['routes.caddy']
        self.assertTrue(site.split('\n')[2] == 'txid.example {', site)
        self.assertIn('\thandle {\n\t\trespond 404\n\t}\n}\n', site)
        self.assertEqual(site.count(LIVE.ROUTER_HEALTH), 2)
        # Every /v2/txid/ path reaches the one combined worker; nothing names /v1/txid.
        archive = site.index('\thandle /v2/txid/archive/* {\n')
        rest = site.index('\thandle /v2/txid/* {\n')
        self.assertLess(archive, rest)
        self.assertIn('reverse_proxy 10.142.0.5:8095 {', site[archive:rest])
        self.assertIn('reverse_proxy 10.142.0.5:8095 {', site[rest:])
        self.assertNotIn('/v1/txid', site + files['withdrawn.caddy'])
        self.assertIn('respond "transparent txid display withdrawn" 503', files['withdrawn.caddy'])
        self.assertIn('txid.example {', files['withdrawn.caddy'])

    def test_transient_units_carry_their_limits(self):
        transient = self.poc().plan()['transient']
        ingest = transient['ingest']
        for item in ('--unit=transparent-txid-display-k8-ingest.service', '--property=CPUQuota=400%',
                     '--property=CPUWeight=20', '--property=IOWeight=20', '--property=MemoryMax=12G',
                     '--property=Nice=10', '--property=RemainAfterExit=yes'):
            self.assertIn(item, ingest)
        tail = ingest[ingest.index(RELEASE_DIR + '/transparent-event-ingest'):]
        self.assertEqual(tail[1:], ['--state-dir', '/root/.cache/zakura', '--workers', '4', '--txid-display',
                                    '--start-height', '3289805', '--data-dir', '/srv/zakura/txid-display-poc/journal'])
        smoke = transient['ingest-smoke']
        self.assertIn('--wait', smoke)
        self.assertEqual(smoke[-4:], ['--stop-height', '3289814', '--data-dir',
                                      '/srv/zakura/txid-display-poc/journal.smoke'])
        self.assertIn('--property=MemoryMax=8G', transient['bootstrap'])
        # txid-display-controller's flags: DisplaySealParams plus the map window.
        seal = ['--geometry', 'txid-2k8', '--n-archive', '1', '--n-recent', '1', '--archive-target', '90000',
                '--recent-window', '90000', '--reorg-margin', '100', '--max-archive-shards', '24']
        controller = RELEASE_DIR + '/txid-display-controller'
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
        self.assertIn('https://txid.example/v2/txid/shards', transient['mapwatch'])

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
                   if not (isinstance(entry[2], str) and entry[2].startswith(N.TRANSACTIONS))]
        self.assertEqual(changed, [])
        transaction = self.journal(json.loads((self.state / 'latest.json').read_text())['id'])
        self.assertEqual((transaction['status'], transaction['changes']), ('failed', []))

    def test_history_health_refusals(self):
        cases = [
            (lambda f: f.put('coordinator', MEMBERSHIP, json.dumps({'routed_recent': 1, 'updated_unix': NOW})),
             'routes 1 recent'),
            (lambda f: f.put('coordinator', MEMBERSHIP, json.dumps({'routed_recent': 2, 'updated_unix': NOW - 600})),
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
            # A schema cutover moved history to another adapter and fleet.
            (lambda f: f.unit('coordinator', P.HISTORY_DAEMONS[0]).update(
                exec='{ argv[]=/usr/bin/python3 -B /opt/transparent-publisher/transparent-live-fleet.py '
                     '/opt/transparent-publisher/fleet.json }'), 'reconciler.service does not run'),
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
            (lambda f: f.put('archive-03', '/proc/meminfo', 'MemTotal: 8388608 kB\nMemAvailable: 2097152 kB\n'),
             'archive: current MemAvailable', 'workers'),
            (lambda f: f.disk.update({'archive-03': (1000, 100)}), '10.0% free', 'workers'),
            (lambda f: f.listen.update({'archive-03': ['10.142.0.5:8095']}), 'port 8095 is already in use', 'workers'),
            (lambda f: f.listen.update({'coordinator': ['127.0.0.1:8098']}), 'port 8098', 'controller'),
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
        self.assertEqual(sorted(P.HOST_RELEASE), sorted(HOSTS))
        for key, names in P.HOST_RELEASE.items():
            host = HOSTS[key]
            for name in names:
                self.assertEqual(self.fake.sha256(host, RELEASE_DIR + '/' + name),
                                 self.request['release']['binaries'].get(name) or self.request['release']['files'][name])
                if name == 'txid-control':
                    # Its contract has no --help; with no socket it prints its usage and exits 1.
                    self.assertIn([RELEASE_DIR + '/' + name], self.fake.commands(host))
                    self.assertNotIn([RELEASE_DIR + '/' + name, '--help'], self.fake.commands(host))
                    continue
                self.assertIn([RELEASE_DIR + '/' + name, '--help'] if not name.endswith('.py')
                              else ['/usr/bin/python3', RELEASE_DIR + '/' + name, '--help'], self.fake.commands(host))
        self.assertIsNone(self.fake.sha256('archive-03', RELEASE_DIR + '/txid-display-controller'))
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'committed')
        uploads = len([m for m in self.fake.mutations() if m[1] == 'upload'])
        poc = self.poc()
        poc.deploy('stage', poc.plan_sha256(), archive=str(archive))
        self.assertEqual(len([m for m in self.fake.mutations() if m[1] == 'upload']), uploads)

    def test_a_self_check_that_fails_refuses_the_stage(self):
        archive = self.bundle()
        run = self.fake.run

        def broken(host, argv, timeout):
            if argv == [RELEASE_DIR + '/txid-control']:
                return 127, 'exec format error'
            return run(host, argv, timeout)
        self.fake.run = broken
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'staged txid-control failed its self-check'):
            poc.deploy('stage', poc.plan_sha256(), archive=str(archive))

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
        self.fake.put('archive-03', RELEASE_DIR + '/txid-control', b'other')
        self.fake.put('archive-03', RELEASE_DIR + '/transparent-txid-server', b'x')
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'holds other bytes'):
            poc.deploy('stage', poc.plan_sha256(), archive=str(archive))


class DeployedFleet(Base):
    """A coordinator with a bootstrap candidate, and the site host's default Caddyfile."""

    def setUp(self):
        super().setUp()
        self.fake.put('coordinator', CANDIDATE + '/txid-shards.json', b'map')
        self.map = sha256(b'map')
        self.fake.put('archive-03', P.CADDYFILE, DEFAULT_CADDY)

    def deploy(self, phase, **options):
        poc = self.poc()
        poc.deploy(phase, poc.plan_sha256(), **options)
        return poc


class WorkerTests(DeployedFleet):
    def test_workers_ship_verify_install_prepare_activate_and_wait_warm(self):
        poc = self.deploy('workers', map_sha256=self.map)
        files = poc.render()
        host = 'archive-03'
        self.assertEqual(self.fake.text(host, '/etc/systemd/system/' + P.WORKER_UNIT), files['unit:archive'])
        self.assertEqual(self.fake.unit(host, P.WORKER_UNIT)['active'], 'active')
        self.assertTrue(self.fake.unit(host, P.WORKER_UNIT)['enabled'])
        self.assertEqual(self.fake.active, {'archive': self.map})
        verify = [argv for argv in self.fake.commands(host) if argv[0] == 'systemd-run']
        self.assertEqual(len(verify), 1)
        self.assertEqual(verify[0][-7:], ['--role', 'combined', '--publication-dir',
                                          '%s/%s' % (P.PUBLICATIONS, self.map), '--cache-bytes',
                                          str(self.request['units']['archive']['cache_bytes']), '--verify-only'])
        self.assertIn('--property=MemoryMax=' + self.request['units']['archive']['memory_max'], verify[0])
        order = [m[2] for m in self.fake.mutations(host) if m[1] == 'systemctl']
        self.assertEqual(order, [('daemon-reload',), ('enable', P.WORKER_UNIT), ('restart', P.WORKER_UNIT)])
        self.assertEqual(self.fake.text('coordinator', N.FLEET_JSON), files['fleet.json'])
        self.assertEqual([m[0] for m in self.fake.mutations() if m[1] == 'systemctl'], [host] * 3)
        self.assertEqual({worker for worker, _ in self.fake.calls}, {'archive'})
        self.assertEqual([path for h, path in self.fake.files if path.startswith(N.REQUESTS)], [])
        events = [event['message'] for event in self.journal(poc.transaction.id)['events']]
        self.assertIn('archive: serving %s warm' % self.map, events)
        status = poc.status()
        self.assertEqual(list(status['workers']), ['archive'])
        self.assertEqual(status['workers']['archive']['ready']['role'], 'combined')
        self.assertNotIn('router_hook', status)

        poc.rollback(poc.transaction.id)
        self.assertIsNone(self.fake.text(host, '/etc/systemd/system/' + P.WORKER_UNIT))
        self.assertEqual(self.fake.unit(host, P.WORKER_UNIT)['active'], 'inactive')
        self.assertFalse(self.fake.unit(host, P.WORKER_UNIT)['enabled'])
        self.assertIsNone(self.fake.text('coordinator', N.FLEET_JSON))
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'rolled-back')

    def test_workers_refuse_while_the_controller_runs(self):
        self.fake.unit('coordinator', N.CONTROLLER_UNIT).update(load='loaded', active='active', sub='running')
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'controller drives the workers; stop it first'):
            poc.deploy('workers', poc.plan_sha256(), map_sha256=self.map)
        self.assertEqual([m for m in self.fake.mutations() if not m[2].startswith(N.TRANSACTIONS)], [])
        self.assertEqual(self.fake.active, {})

    def test_a_rerun_keeps_the_workers_own_map_and_restarts_only_a_changed_unit(self):
        self.deploy('workers', map_sha256=self.map)
        # The controller then advanced the worker, and was stopped.
        newer = 'e' * 64
        self.fake.serve('archive', newer)
        self.fake.log.clear()
        self.fake.calls.clear()
        self.fake.runs.clear()
        poc = self.deploy('workers', map_sha256=self.map)
        events = [event['message'] for event in self.journal(poc.transaction.id)['events']]
        self.assertIn('archive: unit unchanged; the worker keeps running', events)
        self.assertEqual([m[2] for m in self.fake.mutations('archive-03') if m[1] == 'systemctl'], [])
        changed = request()
        changed['units']['archive']['query_slots'] = 3
        self.request = changed
        self.fake.log.clear()
        self.fake.calls.clear()
        self.fake.runs.clear()
        poc = self.deploy('workers', map_sha256=self.map)
        # No ship, verify, prepare or activate: the bootstrap map never replaces a newer one.
        self.assertEqual(self.fake.active, {'archive': newer})
        self.assertEqual(self.fake.calls, [('archive', 'status')])
        self.assertEqual([argv for argv in self.fake.commands() if argv[0] == 'systemd-run'], [])
        self.assertEqual([m[2] for m in self.fake.mutations('archive-03') if m[1] == 'systemctl'],
                         [('daemon-reload',), ('enable', P.WORKER_UNIT), ('restart', P.WORKER_UNIT)])
        events = [event['message'] for event in self.journal(poc.transaction.id)['events']]
        self.assertIn('archive: keeps serving %s, not the requested %s' % (newer, self.map), events)
        self.assertIn('archive: serving %s warm' % newer, events)

    def test_a_fresh_lineage_replaces_the_workers_active_map_and_rolls_back(self):
        self.deploy('workers', map_sha256=self.map)
        old = 'e' * 64
        self.fake.serve('archive', old)
        record = self.fake.text('archive-03', P.ACTIVE_RECORD)
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'not the new one'):
            poc.deploy('workers', poc.plan_sha256(), map_sha256=self.map, replace_active=self.map)
        with self.assertRaisesRegex(P.PocError, 'serves %s, not the replaced' % old):
            poc.deploy('workers', poc.plan_sha256(), map_sha256=self.map, replace_active='f' * 64)
        self.assertEqual(self.fake.active, {'archive': old})
        poc = self.deploy('workers', map_sha256=self.map, replace_active=old)
        # The worker is stopped, loses its record, then ships, verifies,
        # prepares and activates the new lineage as an empty worker would.
        self.assertEqual(self.fake.active, {'archive': self.map})
        events = [event['message'] for event in self.journal(poc.transaction.id)['events']]
        self.assertIn('archive: removed the active record of %s' % old, events)
        self.assertIn('archive: shipped candidate %s' % self.map, events)
        self.assertIn('stopped %s on archive' % P.WORKER_UNIT, events)
        poc.rollback(poc.transaction.id)
        self.assertEqual(self.fake.text('archive-03', P.ACTIVE_RECORD), record)
        self.assertEqual(self.fake.unit('archive-03', P.WORKER_UNIT)['active'], 'active')
        with self.assertRaisesRegex(P.PocError, 'belongs to the workers phase'):
            poc.deploy('route', poc.plan_sha256(), replace_active=old)

    def test_a_prepare_longer_than_one_control_call_is_polled_to_completion(self):
        self.fake.slow_prepares = 2
        poc = self.deploy('workers', map_sha256=self.map)
        self.assertEqual(self.fake.active, {'archive': self.map})
        events = [event['message'] for event in self.journal(poc.transaction.id)['events']]
        self.assertEqual(events.count('archive: still preparing %s' % self.map[:12]), 2)

    def test_a_refused_prepare_is_not_repeated(self):
        self.fake.slow_prepares = 1
        status = self.fake.adapter

        def forgetting(host, request):
            reply = status(host, request)
            self.fake.preparing.clear()
            return reply
        self.fake.adapter = forgetting
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'fleet control on archive failed: FleetError: worker sent no'):
            poc.deploy('workers', poc.plan_sha256(), map_sha256=self.map)
        self.assertEqual(self.fake.active, {})

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

    def test_a_former_recent_host_has_its_worker_unit_retired(self):
        unit = '/etc/systemd/system/' + P.WORKER_UNIT
        previous = 'an earlier recent-replica unit'
        self.fake.put(FORMER_HOST, unit, previous)
        self.fake.unit(FORMER_HOST, P.WORKER_UNIT).update(load='loaded', active='active', sub='running',
                                                          enabled=True, fragment=unit)
        self.request['hosts'][P.FORMER_RECENT] = {'inventory': FORMER_HOST}
        poc = self.poc()
        self.assertEqual(poc.plan()['retires'], {P.FORMER_RECENT: FORMER_HOST})
        self.assertNotEqual(poc.plan_sha256(), self.poc(request()).plan_sha256())
        poc.deploy('workers', poc.plan_sha256(), map_sha256=self.map)
        self.assertIn('--role combined ', self.fake.text('archive-03', unit))
        self.assertEqual(self.fake.unit('archive-03', P.WORKER_UNIT)['active'], 'active')
        self.assertIsNone(self.fake.text(FORMER_HOST, unit))
        self.assertNotEqual(self.fake.unit(FORMER_HOST, P.WORKER_UNIT)['active'], 'active')
        self.assertIn(['systemctl', 'disable', P.WORKER_UNIT], self.fake.commands(FORMER_HOST))

        poc.rollback(poc.transaction.id)
        self.assertEqual(self.fake.text(FORMER_HOST, unit), previous)
        self.assertEqual(self.fake.unit(FORMER_HOST, P.WORKER_UNIT)['active'], 'active')


class RouteTests(DeployedFleet):
    def test_route_validates_writes_the_site_and_waits_for_its_certificate(self):
        self.fake.certificate = False
        # The certificate arrives while the route waits for the public map.
        sleep = self.clock.sleep

        def issuing(seconds):
            sleep(seconds)
            if self.clock.t > 30:
                self.fake.certificate = True
        self.clock.sleep = issuing
        poc = self.deploy('route')
        self.assertEqual(self.fake.text('archive-03', P.CADDYFILE), poc.render()['routes.caddy'])
        self.assertIn(('archive-03', 'systemctl', ('reload', 'caddy')), self.fake.mutations('archive-03'))
        configs = [argv[3] for argv in self.fake.commands('archive-03') if argv[0] == 'caddy']
        scratch = '%s/%s' % (N.VALIDATE, poc.transaction.id)
        self.assertEqual(configs, [scratch + '/Caddyfile', P.CADDYFILE])
        self.assertNotIn(('archive-03', scratch + '/Caddyfile'), self.fake.files)
        self.assertEqual([c for c in self.fake.commands('coordinator') if c[0] == 'caddy'], [])
        poc.rollback(poc.transaction.id)
        self.assertEqual(self.fake.text('archive-03', P.CADDYFILE), DEFAULT_CADDY)
        self.assertEqual([m[2] for m in self.fake.mutations('archive-03') if m[1] == 'systemctl'],
                         [('reload', 'caddy')] * 2)

    def test_a_rejected_site_never_keeps_its_caddyfile(self):
        self.fake.caddy_rejects = lambda path, config: path == P.CADDYFILE
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'caddy rejected the live site'):
            poc.deploy('route', poc.plan_sha256())
        self.assertEqual(self.fake.text('archive-03', P.CADDYFILE), DEFAULT_CADDY)
        self.fake.caddy_rejects = lambda path, config: path != P.CADDYFILE
        with self.assertRaisesRegex(P.PocError, 'caddy rejected the composed site'):
            poc.deploy('route', poc.plan_sha256())
        self.assertEqual(self.fake.text('archive-03', P.CADDYFILE), DEFAULT_CADDY)

    def test_route_waits_for_the_public_map_and_fails_without_it(self):
        self.fake.certificate = False
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'display map at https://txid.example/v2/txid/map did not happen'):
            poc.deploy('route', poc.plan_sha256())
        self.assertGreaterEqual(self.clock.t, P.SITE_SECONDS)
        poc.rollback(poc.transaction.id)
        self.assertEqual(self.fake.text('archive-03', P.CADDYFILE), DEFAULT_CADDY)

    def test_retired_router_phases_are_unknown(self):
        poc = self.poc()
        for phase in ('firewall', 'router-hook'):
            with self.subTest(phase=phase), self.assertRaisesRegex(P.PocError, 'unknown phase'):
                poc.deploy(phase, poc.plan_sha256())


class ControllerTests(DeployedFleet):
    def setUp(self):
        super().setUp()
        self.fake.put('archive-03', '/etc/systemd/system/' + P.WORKER_UNIT, 'unit')
        self.fake.unit('archive-03', P.WORKER_UNIT).update(load='loaded', active='active', sub='running')

    def test_controller_installs_configs_and_unit_and_checks_stability(self):
        poc = self.deploy('controller')
        files = poc.render()
        self.assertEqual(self.fake.text('coordinator', N.WORKERS_JSON), files['workers.json'])
        self.assertEqual([(w['name'], w['role']) for w in json.loads(files['workers.json'])['workers']],
                         [('archive', 'combined')])
        self.assertEqual(self.fake.text('coordinator', '/etc/systemd/system/' + N.CONTROLLER_UNIT),
                         files['unit:controller'])
        self.assertEqual(self.fake.unit('coordinator', N.CONTROLLER_UNIT)['active'], 'active')
        poc.rollback(poc.transaction.id)
        self.assertIsNone(self.fake.text('coordinator', N.WORKERS_JSON))
        self.assertEqual(self.fake.unit('coordinator', N.CONTROLLER_UNIT)['active'], 'inactive')

    def test_controller_refuses_while_another_journal_writer_runs(self):
        self.fake.unit('coordinator', N.INGEST_UNIT).update(load='loaded', active='active', sub='running')
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'only journal writer'):
            poc.deploy('controller', poc.plan_sha256())
        self.assertIsNone(self.fake.text('coordinator', N.WORKERS_JSON))


class TransientTests(Base):
    def test_ingest_start_smoke_status_and_stop(self):
        poc = self.poc()
        poc.act('ingest-start', poc.plan_sha256(), smoke=True)
        smoke = [argv for argv in self.fake.commands('coordinator') if argv[0] == 'systemd-run']
        self.assertEqual(smoke, [poc.plan()['transient']['ingest-smoke']])
        self.fake.wait_results[N.SMOKE_UNIT] = (1, 'state directory format mismatch')
        with self.assertRaisesRegex(P.PocError, 'smoke ingest failed'):
            self.poc().act('ingest-start', poc.plan_sha256(), smoke=True)
        poc = self.poc()
        poc.act('ingest-start', poc.plan_sha256())
        self.assertEqual(self.fake.unit('coordinator', N.INGEST_UNIT)['active'], 'active')
        with self.assertRaisesRegex(P.PocError, 'stop it first'):
            self.poc().act('ingest-start', poc.plan_sha256())
        status = self.poc().ingest_status()
        self.assertEqual(status[N.INGEST_UNIT]['active'], 'active')
        self.assertEqual(status[N.INGEST_UNIT]['log'], ['ingest progress height=3290000'])
        stop = self.poc()
        stop.act('ingest-stop')
        self.assertEqual(self.fake.unit('coordinator', N.INGEST_UNIT)['load'], 'not-found')
        self.assertEqual(self.journal(stop.transaction.id)['undo'], [])

    def test_bootstrap_uses_plan_start_unless_given_a_range(self):
        self.fake.wait_results[N.PLAN_START_UNIT] = (
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
        for unit in (N.OBSERVER_UNIT, N.MAPWATCH_UNIT):
            self.assertEqual(self.fake.unit('coordinator', unit)['active'], 'active')
        self.poc().act('measure-stop')
        for unit in (N.OBSERVER_UNIT, N.MAPWATCH_UNIT):
            self.assertEqual(self.fake.unit('coordinator', unit)['active'], 'inactive')

    def test_verify_reports_the_controller_result(self):
        poc = self.poc()
        poc.act('verify', poc.plan_sha256())
        self.fake.wait_results[N.VERIFY_UNIT] = (1, 'sealed digest 7 differs')
        with self.assertRaisesRegex(P.PocError, 'sealed digest 7 differs'):
            self.poc().act('verify', poc.plan_sha256())


class StopAndRetireTests(DeployedFleet):
    def setUp(self):
        super().setUp()
        poc = self.poc()
        self.routes = poc.render()['routes.caddy']
        self.fake.put('archive-03', P.CADDYFILE, self.routes)
        for host, unit, name in (('archive-03', P.WORKER_UNIT, 'unit:archive'),
                                 ('coordinator', N.CONTROLLER_UNIT, 'unit:controller')):
            self.fake.put(host, '/etc/systemd/system/' + unit, poc.render()[name])
            self.fake.unit(host, unit).update(load='loaded', active='active', sub='running', enabled=True,
                                              fragment='/etc/systemd/system/' + unit)

    def test_stop_withdraws_then_stops_the_controller_and_rolls_back(self):
        poc = self.poc()
        poc.act('stop')
        self.assertEqual(self.fake.text('archive-03', P.CADDYFILE), poc.render()['withdrawn.caddy'])
        self.assertEqual(self.fake.unit('coordinator', N.CONTROLLER_UNIT)['active'], 'inactive')
        reload = [m for m in self.fake.mutations() if m[0] == 'archive-03' and m[1] == 'systemctl']
        controller = [i for i, m in enumerate(self.fake.mutations()) if m[2] == ('stop', N.CONTROLLER_UNIT)]
        self.assertLess(self.fake.mutations().index(reload[0]), controller[0])
        record = self.journal(poc.transaction.id)
        self.assertLess(record['stop_seconds'], P.STOP_TARGET_SECONDS)
        poc.rollback(poc.transaction.id)
        self.assertEqual(self.fake.text('archive-03', P.CADDYFILE), self.routes)
        self.assertEqual(self.fake.unit('coordinator', N.CONTROLLER_UNIT)['active'], 'active')

    def test_stop_still_stops_the_controller_when_caddy_refuses(self):
        self.fake.reload_fails = True
        poc = self.poc()
        with self.assertRaisesRegex(P.PocError, 'controller stopped, but the route was not withdrawn'):
            poc.act('stop')
        self.assertEqual(self.fake.text('archive-03', P.CADDYFILE), self.routes)
        self.assertEqual(self.fake.unit('coordinator', N.CONTROLLER_UNIT)['active'], 'inactive')

    def test_retire_removes_units_withdraws_the_site_and_keeps_data(self):
        self.fake.unit('coordinator', N.MAPWATCH_UNIT).update(load='loaded', active='active', sub='running',
                                                              fragment='/run/systemd/transient/' + N.MAPWATCH_UNIT)
        poc = self.poc()
        poc.retire(poc.plan_sha256())
        for host, unit in (('archive-03', P.WORKER_UNIT), ('coordinator', N.CONTROLLER_UNIT)):
            self.assertIsNone(self.fake.text(host, '/etc/systemd/system/' + unit))
            self.assertFalse(self.fake.unit(host, unit)['enabled'])
            mutations = [m[2] for m in self.fake.mutations(host)]
            self.assertLess(mutations.index(('disable', unit)), mutations.index('/etc/systemd/system/' + unit))
        self.assertEqual(self.fake.unit('coordinator', N.MAPWATCH_UNIT)['load'], 'not-found')
        # The host keeps a Caddyfile, so Caddy keeps running: the withdrawn site.
        self.assertEqual(self.fake.text('archive-03', P.CADDYFILE), poc.render()['withdrawn.caddy'])
        self.assertEqual(self.journal(poc.transaction.id)['status'], 'committed')
        self.assertIn('  coordinator:/srv/zakura/txid-display-poc/journal', self.output)
        self.assertIn('  archive-03:/srv/transparent-txid-display', self.output)
        self.assertFalse([m for m in self.fake.mutations() if m[1] == 'remove' and m[2].startswith('/srv/')])

        # Retirement itself rolls back from its journal: units and route return.
        restore = self.poc()
        restore.rollback(poc.transaction.id)
        self.assertEqual(self.fake.text('archive-03', P.CADDYFILE), self.routes)
        for host, unit in (('archive-03', P.WORKER_UNIT), ('coordinator', N.CONTROLLER_UNIT)):
            self.assertEqual(self.fake.unit(host, unit)['active'], 'active')
            self.assertTrue(self.fake.unit(host, unit)['enabled'])


class CommandLineTests(DeployedFleet):
    def run_cli(self, *argv):
        inventory_path = Path(self.tmp.name) / 'inventory.json'
        inventory_path.write_text(json.dumps({
            'hosts': {name: {} for name in (*HOSTS.values(), FORMER_HOST)}, 'ssh': {'mode': 'config'},
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

    def test_a_request_without_a_site_is_refused(self):
        del self.request['site']
        code, lines = self.run_cli('txid-display-plan')
        self.assertEqual(code, 1)
        self.assertIn('request must name a site', lines[-1])

    def test_preflight_exit_status(self):
        code, lines = self.run_cli('txid-display-preflight')
        self.assertEqual((code, lines[-1]), (0, 'preflight passed'))
        self.fake.history['freshness_seconds'] = 31
        code, lines = self.run_cli('txid-display-preflight')
        self.assertEqual((code, lines[-1]), (1, 'preflight refused'))

    def test_every_command_is_closed_and_requires_the_request(self):
        parser = cli.parser()
        for name in cli.transparent_cli().TXID_DISPLAY:
            with self.subTest(name=name), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser.parse_args(['txid-display-' + name])
        for phase in ('everything', 'firewall', 'router-hook'):
            with self.subTest(phase=phase), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser.parse_args(['txid-display-deploy', '--request', 'r', '--request-sha256', 'h',
                                   '--expect-plan-sha256', 'h', '--phase', phase])
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(['txid-display-retire', '--request', 'r', '--request-sha256', 'h',
                               '--expect-plan-sha256', 'h', '--terraform-plan-sha256', 'h'])
        self.assertEqual(cli.transparent_cli().TXID_DISPLAY_PHASES, P.PHASES)


if __name__ == '__main__':
    unittest.main()
