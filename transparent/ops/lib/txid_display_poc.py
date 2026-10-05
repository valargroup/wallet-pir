"""The `wallet-pir-deploy.py txid-display-*` command family.

Runs the tiered txid display proof of concept side by side with history on
production: a controller and a fresh display journal on the coordinator, an
archive-owner worker, a recent-replica worker and a `/v1/txid/` route on the
history router. History units and data are never changed; the only
history-adjacent change is the router import hook (`router-hook`).

Every command takes the reviewed request and its sha256. `plan` is read-only
and deterministic: rendered units, configs and snippets come from the request
and this checkout, never from live hosts, so its digest is the review token
that mutating commands require. Mutations hold the production lock, run the
schema fence before every effect, and journal each file's prior bytes and mode
before replacing it, under `<state-dir>/txid-display/` with a copy on the
host. `rollback` restores exactly those bytes and undoes the recorded unit and
route effects. `retire` removes everything but data, whose paths it prints.
Every host effect goes through the deploy `Executor`, so tests substitute an
in-memory fleet.
"""
import contextlib
import datetime
import hashlib
import importlib.util
import ipaddress
import json
from pathlib import Path
import posixpath
import re
import secrets
import shlex
import sys
import tempfile
import time

from wallet_pir_ops import durable, schema_fence
from wallet_pir_ops.deploy import units
from wallet_pir_ops.deploy.remote import SSHExecutor, production_lock

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = 'txid-display-poc-request-v1'
PLAN_SCHEMA = 'txid-display-poc-plan-v1'
JOURNAL_KIND = 'txid-display-poc-transaction-v1'
FLEET_SCHEMA = 'txid-display-fleet-v1'
KIND = 'transparent-txid-display'
BINARIES = ('transparent-txid-server', 'txid-control', 'txid-inventory', 'txid-display-controller',
            'transparent-event-ingest')
SCRIPTS = ('txid-display-fleet.py', 'txid-display-observe.py')
TEMPLATES = ('transparent-txid-display-worker.service.in', 'transparent-txid-display-controller.service.in',
             'txid-display-routes.caddy.in')
# Release files by basename, as tools/ci/release.py ships them.
RELEASE_FILES = {name: 'transparent/ops/deploy/' + name for name in TEMPLATES}
RELEASE_FILES.update({name: 'transparent/ops/scripts/' + name for name in SCRIPTS})
# What each host runs. Workers never receive the controller or the backfill.
HOST_RELEASE = {
    'coordinator': ('txid-display-controller', 'transparent-event-ingest', 'txid-inventory', 'txid-control', *SCRIPTS),
    'archive': ('transparent-txid-server', 'txid-control'),
    'recent': ('transparent-txid-server', 'txid-control'),
}
WORKERS = ('archive', 'recent')
ROLES = {'archive': 'archive-owner', 'recent': 'recent-replica'}
HOST_KEYS = ('coordinator', 'archive', 'recent', 'router')
PHASES = ('stage', 'firewall', 'router-hook', 'workers', 'route', 'controller')
ACTIONS = ('ingest-start', 'ingest-stop', 'bootstrap', 'verify', 'measure-start', 'measure-stop', 'stop')
MODES = ('live', 'replay', 'replay-then-live')
GEOMETRIES = ('txid-2k', 'txid-4k')
# DisplaySealParams plus the map window, as txid-display-controller's seal flags.
SEAL_KEYS = ('n_archive', 'n_recent', 'archive_target', 'recent_floor', 'reorg_margin', 'max_archive_shards')
MAX_BUCKETS = 64

OPT = '/opt/transparent-txid-display'
RELEASES = OPT + '/releases'
TRANSACTIONS = OPT + '/transactions'
STATE = OPT + '/state'
REQUESTS = STATE + '/requests'
TERRAFORM_PLANS = OPT + '/terraform'
VALIDATE = OPT + '/validate'
WORKERS_JSON = OPT + '/workers.json'
FLEET_JSON = OPT + '/fleet.json'
WORKER_DATA = '/srv/transparent-txid-display'
PUBLICATIONS = WORKER_DATA + '/publications'
STAGED = WORKER_DATA + '/staged'
RUNTIME_CACHE = WORKER_DATA + '/runtime-cache'
WORKER_STATE = OPT + '/worker'
ACTIVE_RECORD = WORKER_STATE + '/active.json'
CONTROL_SOCKET = '/run/transparent-txid-display/control.sock'
SYSTEM = '/etc/systemd/system'
WORKER_UNIT = 'transparent-txid-display-worker.service'
CONTROLLER_UNIT = 'transparent-txid-display-controller.service'
INGEST_UNIT = 'transparent-txid-display-ingest.service'
SMOKE_UNIT = 'transparent-txid-display-ingest-smoke.service'
BOOTSTRAP_UNIT = 'transparent-txid-display-bootstrap.service'
OBSERVER_UNIT = 'transparent-txid-display-observer.service'
MAPWATCH_UNIT = 'transparent-txid-display-mapwatch.service'
TRANSIENT = (INGEST_UNIT, SMOKE_UNIT, BOOTSTRAP_UNIT, OBSERVER_UNIT, MAPWATCH_UNIT)
JOURNAL_WRITERS = (INGEST_UNIT, SMOKE_UNIT, BOOTSTRAP_UNIT)
CADDYFILE = '/etc/caddy/Caddyfile'
PUBLISHER = '/opt/transparent-publisher'
LIVE_FLEET = PUBLISHER + '/transparent-live-fleet.py'
HISTORY_FLEET = PUBLISHER + '/fleet.json'
HISTORY_DAEMONS = ('transparent-replica-reconciler.service', 'transparent-control-sessions.service')
# The existing read-only history deploy credentials, reused by the adapter.
DEPLOY_KEY = PUBLISHER + '/credentials/deploy-ssh'
KNOWN_HOSTS = PUBLISHER + '/credentials/known_hosts'
MEMBERSHIP = PUBLISHER + '/state/membership.json'
ACTUATOR_OPERATION = PUBLISHER + '/scaler/journal/operation.json'
HISTORY_STATUS = 'http://127.0.0.1:8094/v1/status'
NODE_STATE = 'state/v29/mainnet'
TFVARS = 'txid-display.auto.tfvars'
TF_VARIABLE = 'transparent_txid_display_port_enabled'
TF_SOURCES = ('transparent.tf', 'variables.tf')
FIREWALL = 'digitalocean_firewall.transparent_worker'
FIREWALL_TAGS = ('transparent-pir-router', 'wallet-pir-coordinator')
WITHDRAWN = ('# Written by `wallet-pir-deploy.py txid-display-stop`: display withdrawn.\n'
             '\thandle /v1/txid/* {\n\t\theader Retry-After 1\n'
             '\t\trespond "transparent txid display withdrawn" 503\n\t}\n')
# The schema guards' and stop triggers' floors; never request inputs.
FREE_FLOOR = 0.20
FRESHNESS_SECONDS = 30
MIN_ROUTED_RECENT = 2
MEMBERSHIP_SECONDS = 60
LOAD_STATUS_SECONDS = 120
STOP_TARGET_SECONDS = 60
SELF_CHECK_SECONDS = 60
# A cold bootstrap candidate holds every archive; per-cycle ships take seconds.
SHIP_SECONDS = 1800
# Under txid-control's own 600 s wait for a control reply.
PREPARE_CALL_SECONDS = 540
MEASURE_LIMITS = {'MemoryMax': '256M', 'CPUQuota': '25%', 'CPUWeight': '20', 'IOWeight': '20', 'Nice': '10'}
# Ports history, Enhance, the node and the prototype already use.
RESERVED_PORTS = (8080, 8081, 8083, 8090, 8092, 8093, 8094, 8192, 8232)
# ops/infra/digitalocean/production/transparent.tf opens exactly this port.
WORKER_PORT = 8095
HISTORY_ROOTS = ('/srv/transparent-pir', '/srv/transparent-activity', '/opt/transparent-publisher',
                 '/run/transparent-pir', '/opt/enhance-pir')

HEX40 = re.compile('[0-9a-f]{40}')
HEX64 = re.compile('[0-9a-f]{64}')
PLAIN_PATH = re.compile(r'/[A-Za-z0-9._/-]+')
MEMORY = re.compile(r'([0-9]+(?:\.[0-9]+)?)([KMGT])')
QUOTA = re.compile(r'[1-9][0-9]{0,3}%')
# No `.`, `..` or hidden directory. validate() also refuses any `..`, as the
# history adapter's route_import_lines does: a glob it refuses fails its render.
GLOB = re.compile(r'(/etc/caddy/[A-Za-z0-9_-][A-Za-z0-9._-]*)/\*\.caddy')
PUBLIC_URL = re.compile(r'https://[A-Za-z0-9.-]+')
RPC_URL = re.compile(r'http://127\.0\.0\.1:[0-9]{2,5}')
TRANSACTION = re.compile(r'txid-display-[a-z-]+-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{6}')


class PocError(ValueError):
    pass


def require(ok, message):
    if not ok:
        raise PocError(message)


def sha256(data):
    return hashlib.sha256(data.encode() if isinstance(data, str) else data).hexdigest()


def file_sha256(path):
    return sha256(Path(path).read_bytes())


def memory_bytes(text):
    """systemd's base-1024 size suffixes."""
    match = MEMORY.fullmatch(text) if isinstance(text, str) else None
    require(match, 'invalid memory size %r' % (text,))
    return int(float(match[1]) * 1024 ** ('KMGT'.index(match[2]) + 1))


# ------------------------------------------------------------------ request

def exact(value, keys, what):
    require(isinstance(value, dict) and set(value) == set(keys), '%s must have exactly %s' % (what, sorted(keys)))
    return value


def integer(value, low, high, what):
    require(type(value) is int and low <= value <= high, '%s must be an integer in [%d, %d]' % (what, low, high))
    return value


def plain_path(value, what, history=False):
    require(isinstance(value, str) and PLAIN_PATH.fullmatch(value) and posixpath.normpath(value) == value,
            '%s must be a plain normalized absolute path' % what)
    require(history or not any(inside(value, root) for root in HISTORY_ROOTS),
            '%s must stay outside history and Enhance paths' % what)
    return value


def inside(path, root):
    return path == root or path.startswith(root.rstrip('/') + '/')


def validate(request, inventory):
    """The reviewed request, or PocError. Closed key sets throughout."""
    exact(request, ('schema', 'source_sha', 'release', 'hosts', 'ports', 'units', 'journal', 'publication',
                    'controller', 'router', 'terraform', 'history', 'measure', 'baseline'), 'request')
    require(request['schema'] == SCHEMA, 'request schema must be %s' % SCHEMA)
    require(isinstance(request['source_sha'], str) and HEX40.fullmatch(request['source_sha']),
            'source_sha must be a full lowercase commit')
    release = exact(request['release'], ('archive_sha256', 'ci_run', 'binaries', 'files'), 'release')
    require(isinstance(release['archive_sha256'], str) and HEX64.fullmatch(release['archive_sha256']),
            'release.archive_sha256 must be the CI artifact archive digest')
    require(release['ci_run'] is None or type(release['ci_run']) is int and release['ci_run'] > 0,
            'release.ci_run must be a run id or null')
    for field, names in (('binaries', BINARIES), ('files', tuple(RELEASE_FILES))):
        exact(release[field], names, 'release.' + field)
        require(all(isinstance(v, str) and HEX64.fullmatch(v) for v in release[field].values()),
                'release.%s values must be sha256 digests' % field)

    hosts = exact(request['hosts'], HOST_KEYS, 'hosts')
    names = []
    for key in HOST_KEYS:
        exact(hosts[key], ('inventory', 'vpc_ip') if key in WORKERS else ('inventory',), 'hosts.' + key)
        require(hosts[key]['inventory'] in inventory.hosts, 'hosts.%s names no inventory host' % key)
        names.append(hosts[key]['inventory'])
        if key in WORKERS:
            try:
                address = ipaddress.IPv4Address(hosts[key]['vpc_ip'])
            except (ipaddress.AddressValueError, TypeError):
                raise PocError('hosts.%s.vpc_ip must be an IPv4 address' % key) from None
            require(address.is_private and not address.is_loopback, 'hosts.%s.vpc_ip must be a VPC address' % key)
    require(len(set(names)) == len(names), 'coordinator, archive, recent and router must be distinct hosts')
    if inventory.lock.get('type') == 'remote':
        require(inventory.lock['host'] == hosts['coordinator']['inventory'],
                'the production lock host must be the coordinator')

    ports = exact(request['ports'], ('worker', 'controller_status'), 'ports')
    for key, value in ports.items():
        integer(value, 1024, 65535, 'ports.' + key)
        require(value not in RESERVED_PORTS, 'ports.%s collides with an existing service' % key)
    require(ports['worker'] == WORKER_PORT, 'ports.worker must be %d, the port the firewall rule opens' % WORKER_PORT)
    require(ports['worker'] != ports['controller_status'], 'ports must differ')

    limits = exact(request['units'], ('archive', 'recent', 'controller', 'ingest', 'bootstrap'), 'units')
    for key in WORKERS:
        spec = exact(limits[key], ('cache_bytes', 'memory_max', 'cpu_quota', 'cpu_weight', 'nice',
                                   'oom_score_adjust', 'build_threads', 'build_slots', 'query_slots',
                                   'retain_revisions', 'disk_cache_bytes', 'ready_timeout_seconds'), 'units.' + key)
        integer(spec['cache_bytes'], 64 << 20, 64 << 30, 'units.%s.cache_bytes' % key)
        require(spec['cache_bytes'] < memory_bytes(spec['memory_max']), 'units.%s cache must fit MemoryMax' % key)
        # The OOM killer's first choice on a host shared with history.
        integer(spec['oom_score_adjust'], 500, 1000, 'units.%s.oom_score_adjust' % key)
        integer(spec['build_threads'], 1, 16, 'units.%s.build_threads' % key)
        integer(spec['build_slots'], 1, 8, 'units.%s.build_slots' % key)
        integer(spec['query_slots'], 1, 16, 'units.%s.query_slots' % key)
        integer(spec['retain_revisions'], 1, 16, 'units.%s.retain_revisions' % key)
        integer(spec['ready_timeout_seconds'], 60, 7200, 'units.%s.ready_timeout_seconds' % key)
        require(spec['disk_cache_bytes'] is None or type(spec['disk_cache_bytes']) is int
                and 1 << 30 <= spec['disk_cache_bytes'] <= 256 << 30, 'units.%s.disk_cache_bytes' % key)
    exact(limits['controller'], ('memory_max', 'cpu_quota', 'cpu_weight', 'nice'), 'units.controller')
    exact(limits['ingest'], ('memory_max', 'cpu_quota', 'cpu_weight', 'io_weight', 'nice', 'workers'), 'units.ingest')
    integer(limits['ingest']['io_weight'], 1, 50, 'units.ingest.io_weight')
    integer(limits['ingest']['workers'], 1, 16, 'units.ingest.workers')
    exact(limits['bootstrap'], ('memory_max', 'cpu_quota', 'cpu_weight', 'nice'), 'units.bootstrap')
    for key, spec in limits.items():
        memory_bytes(spec['memory_max'])
        require(isinstance(spec['cpu_quota'], str) and QUOTA.fullmatch(spec['cpu_quota']),
                'units.%s.cpu_quota must look like 200%%' % key)
        # Display always loses: below the default weight of 100, and niced.
        integer(spec['cpu_weight'], 1, 50, 'units.%s.cpu_weight' % key)
        integer(spec['nice'], 5, 19, 'units.%s.nice' % key)

    journal = exact(request['journal'], ('data_dir', 'start_height', 'node_cache_dir'), 'journal')
    plain_path(journal['data_dir'], 'journal.data_dir')
    integer(journal['start_height'], 1, 1 << 40, 'journal.start_height')
    plain_path(journal['node_cache_dir'], 'journal.node_cache_dir')
    publication = exact(request['publication'], ('root',), 'publication')
    plain_path(publication['root'], 'publication.root')

    controller = exact(request['controller'], ('mode', 'blocks_per_step', 'step_interval_ms', 'geometry', *SEAL_KEYS,
                                               'archives', 'replay_seals', 'rpc_url', 'rpc_cookie'), 'controller')
    require(controller['mode'] in MODES, 'controller.mode must be one of %s' % (MODES,))
    integer(controller['blocks_per_step'], 1, 1000, 'controller.blocks_per_step')
    integer(controller['step_interval_ms'], 100, 600000, 'controller.step_interval_ms')
    require(controller['geometry'] in GEOMETRIES, 'controller.geometry must be one of %s' % (GEOMETRIES,))
    for key in ('n_archive', 'n_recent'):
        integer(controller[key], 1, MAX_BUCKETS, 'controller.' + key)
    # The anonymity floor of the brief; a request cannot lower it.
    integer(controller['archive_target'], 10000, 1 << 32, 'controller.archive_target')
    integer(controller['recent_floor'], 10000, 1 << 32, 'controller.recent_floor')
    # Sealed shards never change, so they must stay below any reorg the node accepts.
    integer(controller['reorg_margin'], 100, 10000, 'controller.reorg_margin')
    integer(controller['max_archive_shards'], 1, 64, 'controller.max_archive_shards')
    # plan-start's targets: archives bootstrap makes, and seals replay adds after them.
    integer(controller['archives'], 1, 64, 'controller.archives')
    integer(controller['replay_seals'], 0, 64, 'controller.replay_seals')
    require(isinstance(controller['rpc_url'], str) and RPC_URL.fullmatch(controller['rpc_url']),
            'controller.rpc_url must be the loopback node RPC')
    plain_path(controller['rpc_cookie'], 'controller.rpc_cookie')

    router = exact(request['router'], ('import_glob', 'live_fleet_sha256'), 'router')
    require(isinstance(router['import_glob'], str) and GLOB.fullmatch(router['import_glob'])
            and '..' not in router['import_glob'], 'router.import_glob must be /etc/caddy/<dir>/*.caddy')
    require(isinstance(router['live_fleet_sha256'], str) and HEX64.fullmatch(router['live_fleet_sha256']),
            'router.live_fleet_sha256 must be the reviewed deployed history adapter digest')
    terraform = exact(request['terraform'], ('wrapper', 'root'), 'terraform')
    plain_path(terraform['wrapper'], 'terraform.wrapper', history=True)
    plain_path(terraform['root'], 'terraform.root', history=True)
    history = exact(request['history'], ('load_status',), 'history')
    plain_path(history['load_status'], 'history.load_status', history=True)
    measure = exact(request['measure'], ('output', 'public_url'), 'measure')
    plain_path(measure['output'], 'measure.output')
    require(isinstance(measure['public_url'], str) and PUBLIC_URL.fullmatch(measure['public_url']),
            'measure.public_url must be https://<host>')
    data = [journal['data_dir'], journal['data_dir'] + '.smoke', publication['root'], measure['output']]
    require(not any(inside(a, b) for i, a in enumerate(data) for j, b in enumerate(data) if i != j),
            'journal, publication and measure directories must not nest')
    baseline = exact(request['baseline'], ('coordinator', 'archive', 'recent'), 'baseline')
    for key, value in baseline.items():
        exact(value, ('mem_available_bytes',), 'baseline.' + key)
        integer(value['mem_available_bytes'], 1, 1 << 44, 'baseline.%s.mem_available_bytes' % key)
    require(len(durable.canonical(request)) <= 65536, 'request exceeds bound')
    return request


def read_request(path, expected):
    require(isinstance(expected, str) and HEX64.fullmatch(expected), '--request-sha256 must be 64 hex digits')
    raw = Path(path).read_bytes()
    require(len(raw) <= 65536, 'request exceeds bound')

    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate request field %r' % key)
            result[key] = value
        return result
    request = json.loads(raw, object_pairs_hook=unique)
    require(durable.digest(request) == expected, 'request differs from the reviewed --request-sha256')
    return request


# ------------------------------------------------------------- transaction

class Transaction:
    """One journaled action: every file's prior bytes and every recorded undo step.

    Written durably before each effect it describes, so an interrupted action
    rolls back from this file alone. Undo steps run around the file restores:
    `pre` steps in reverse (stop a unit), `post` steps in order (daemon-reload,
    reload Caddy, restart a daemon, start a unit again).
    """

    def __init__(self, poc, data, path):
        self.poc, self.data, self.path = poc, data, Path(path)

    @classmethod
    def create(cls, poc, action):
        poc.journal_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        identifier = 'txid-display-%s-%s-%s' % (action, time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()),
                                                secrets.token_hex(3))
        transaction = cls(poc, {
            'kind': JOURNAL_KIND, 'id': identifier, 'action': action, 'request_sha256': poc.request_sha256,
            'plan_sha256': poc.plan_sha256(), 'status': 'running', 'created_unix': time.time(),
            'changes': [], 'undo': [], 'events': [],
        }, poc.journal_dir / (identifier + '.json'))
        transaction.save()
        durable.atomic_json(poc.journal_dir / 'latest.json', {'id': identifier}, mode=0o600)
        return transaction

    @classmethod
    def load(cls, poc, identifier=None):
        if identifier is None:
            pointer = poc.journal_dir / 'latest.json'
            require(pointer.exists(), 'no txid display transaction in %s' % poc.journal_dir)
            identifier = json.loads(pointer.read_text())['id']
        require(isinstance(identifier, str) and TRANSACTION.fullmatch(identifier), 'invalid transaction id')
        path = poc.journal_dir / (identifier + '.json')
        require(path.exists(), 'no transaction %s in %s' % (identifier, poc.journal_dir))
        data = json.loads(path.read_text())
        require(data.get('kind') == JOURNAL_KIND and data.get('id') == identifier, 'transaction record differs')
        require(data['request_sha256'] == poc.request_sha256, 'transaction %s belongs to another request' % identifier)
        return cls(poc, data, path)

    @property
    def id(self):
        return self.data['id']

    @property
    def status(self):
        return self.data['status']

    def save(self):
        durable.atomic_json(self.path, self.data, mode=0o600)

    def event(self, message, **fields):
        self.data['events'].append({'unix': round(time.time(), 3), 'message': message, **fields})
        self.save()
        self.poc.out('%s: %s' % (self.id, message))

    def finish(self, status, **fields):
        self.data['status'] = status
        self.data.update(fields)
        self.event('status ' + status)

    def undo(self, when, host, op, **arguments):
        """Record how to undo an effect, before the effect."""
        require(when in ('pre', 'post'), 'undo group is pre or post')
        step = {'when': when, 'host': host, 'op': op, **arguments}
        if step not in self.data['undo']:
            self.data['undo'].append(step)
            self.save()

    def record(self, host, path, previous, previous_mode, new, mode):
        entry = {'host': host, 'path': path, 'mode': mode, 'previous': previous, 'previous_mode': previous_mode,
                 'previous_sha256': None if previous is None else sha256(previous),
                 'new_sha256': None if new is None else sha256(new), 'applied': False}
        self.data['changes'].append(entry)
        self.save()
        return entry

    def change(self, host, path, data, mode):
        """Replace `path` on `host` (a request host key), journaling its prior bytes first.

        An existing file keeps its mode. Returns False when the bytes already match.
        """
        poc = self.poc
        data = data.encode() if isinstance(data, str) else bytes(data)
        poc.guard()
        name = poc.host(host)
        previous = poc.ex.read(name, path)
        if previous is not None and previous.encode() == data:
            return False
        previous_mode = poc.mode_of(host, path) if previous is not None else None
        entry = self.record(host, path, previous, previous_mode, data, previous_mode or mode)
        self.backup(host, path, previous)
        poc.ex.mkdir(name, posixpath.dirname(path), 0o755)
        poc.ex.write(name, path, data, entry['mode'])
        entry['applied'] = True
        self.event('wrote %s on %s' % (path, host), sha256=entry['new_sha256'])
        return True

    def remove(self, host, path):
        """Remove a file, journaling its bytes so a rollback puts it back."""
        poc = self.poc
        poc.guard()
        previous = poc.ex.read(poc.host(host), path)
        if previous is None:
            return False
        entry = self.record(host, path, previous, poc.mode_of(host, path), None, None)
        self.backup(host, path, previous)
        poc.ex.remove(poc.host(host), path)
        entry['applied'] = True
        self.event('removed %s on %s' % (path, host))
        return True

    def backup(self, host, path, previous):
        """The host's own copy of the prior bytes, beside the runner's journal."""
        if previous is None:
            return
        name = self.poc.host(host)
        directory = '%s/%s/files' % (TRANSACTIONS, self.id)
        self.poc.ex.mkdir(name, directory, 0o700)
        self.poc.ex.write(name, '%s/%d-%s' % (directory, len(self.data['changes']), posixpath.basename(path)),
                          previous.encode(), 0o600)


# --------------------------------------------------------------------- poc

class Poc:
    def __init__(self, inventory, request, request_sha256, state_dir, executor=None, lock=None, out=print,
                 sleep=time.sleep, clock=time.monotonic, now=time.time, root=ROOT, poll_seconds=2.0):
        self.inventory = inventory
        self.request = validate(request, inventory)
        self.request_sha256 = request_sha256
        self.journal_dir = Path(state_dir)
        self.ex = executor or SSHExecutor(inventory)
        self.lock_factory = lock or (lambda: production_lock(inventory, self.ex))
        self.lock = None
        self.out = out
        self.sleep = sleep
        self.clock = clock
        self.now = now
        self.root = Path(root)
        self.poll_seconds = poll_seconds
        self.transaction = None
        self._plan = None

    # ------------------------------------------------------------ identity

    def host(self, key):
        return self.request['hosts'][key]['inventory']

    @property
    def release_dir(self):
        return '%s/%s' % (RELEASES, self.request['source_sha'])

    def binary(self, name):
        return '%s/%s' % (self.release_dir, name)

    @property
    def import_line(self):
        return '\timport ' + self.request['router']['import_glob']

    @property
    def routes_dir(self):
        return GLOB.fullmatch(self.request['router']['import_glob'])[1]

    @property
    def routes_path(self):
        return self.routes_dir + '/routes.caddy'

    @property
    def tfvars_path(self):
        return '%s/%s' % (self.request['terraform']['root'], TFVARS)

    def upstream(self, key):
        return '%s:%d' % (self.request['hosts'][key]['vpc_ip'], self.request['ports']['worker'])

    def ready_url(self, key):
        return 'http://%s/v1/ready' % self.upstream(key)

    # ----------------------------------------------------------- rendering

    def template(self, name):
        return (self.root / RELEASE_FILES[name]).read_text()

    def render(self):
        """Every file the deploy writes, from the request and this checkout only."""
        r = self.request
        files = {}
        for key in WORKERS:
            spec = r['units'][key]
            extra = ''
            if spec['disk_cache_bytes']:
                extra = ' --runtime-cache-dir %s --runtime-cache-max-bytes %d' % (RUNTIME_CACHE, spec['disk_cache_bytes'])
            files['unit:' + key] = units.render(self.template('transparent-txid-display-worker.service.in'), {
                'RELEASE': self.release_dir, 'LISTEN': self.upstream(key), 'ROLE': ROLES[key],
                'COLLECT_ROOT': PUBLICATIONS,
                'CACHE_BYTES': str(spec['cache_bytes']), 'BUILD_SLOTS': str(spec['build_slots']),
                'QUERY_SLOTS': str(spec['query_slots']), 'RETAIN_REVISIONS': str(spec['retain_revisions']),
                'EXTRA_ARGS': extra, 'BUILD_THREADS': str(spec['build_threads']), 'MEMORY_MAX': spec['memory_max'],
                'CPU_QUOTA': spec['cpu_quota'], 'CPU_WEIGHT': str(spec['cpu_weight']), 'NICE': str(spec['nice']),
                'OOM_SCORE_ADJUST': str(spec['oom_score_adjust'])})
        controller, limits = r['controller'], r['units']['controller']
        # `run` reads the seal rule from the bootstrapped root; it takes no seal flags.
        extra = ''
        if controller['mode'] != 'live':
            extra = ' --blocks-per-step %d --step-interval-ms %d' % (controller['blocks_per_step'],
                                                                     controller['step_interval_ms'])
        files['unit:controller'] = units.render(self.template('transparent-txid-display-controller.service.in'), {
            'RELEASE': self.release_dir, 'ROOT': r['publication']['root'], 'JOURNAL': r['journal']['data_dir'],
            'MODE': controller['mode'], 'STATUS_PORT': str(r['ports']['controller_status']),
            'RPC_URL': controller['rpc_url'], 'RPC_COOKIE': controller['rpc_cookie'], 'EXTRA_ARGS': extra,
            'MEMORY_MAX': limits['memory_max'], 'CPU_QUOTA': limits['cpu_quota'],
            'CPU_WEIGHT': str(limits['cpu_weight']), 'NICE': str(limits['nice'])})
        for name in ('unit:archive', 'unit:recent', 'unit:controller'):
            starts = [value for _, key, value in units.entries(files[name]) if key == 'ExecStart']
            require(len(starts) == 1, '%s must have exactly one ExecStart' % name)
        transport = {'command': self.binary('txid-display-fleet.py'), 'config': FLEET_JSON}
        # Archive owners first: the controller stages and prepares them before
        # the recent replica, which serves the map.
        files['workers.json'] = json.dumps({'workers': [
            {'name': key, 'role': ROLES[key], 'transport': transport} for key in WORKERS]}, indent=2) + '\n'
        files['fleet.json'] = json.dumps({
            'schema': FLEET_SCHEMA, 'ssh_key': DEPLOY_KEY, 'known_hosts': KNOWN_HOSTS,
            'control_dir': STATE + '/ssh',
            'workers': {key: {'ssh_host': r['hosts'][key]['vpc_ip'], 'user': 'root', 'publications': PUBLICATIONS,
                              'staged': STAGED, 'control_socket': CONTROL_SOCKET,
                              'control_binary': self.binary('txid-control')} for key in WORKERS},
        }, indent=2, sort_keys=True) + '\n'
        files['routes.caddy'] = units.render(self.template('txid-display-routes.caddy.in'), {
            'ARCHIVE_UPSTREAM': self.upstream('archive'), 'RECENT_UPSTREAM': self.upstream('recent')})
        files['withdrawn.caddy'] = WITHDRAWN
        files['tfvars'] = '# txid display proof of concept; removed by txid-display-retire.\n%s = true\n' % TF_VARIABLE
        return files

    def targets(self):
        """`name -> (host key, path, mode)` of every rendered file."""
        return {
            'unit:archive': ('archive', '%s/%s' % (SYSTEM, WORKER_UNIT), 0o644),
            'unit:recent': ('recent', '%s/%s' % (SYSTEM, WORKER_UNIT), 0o644),
            'unit:controller': ('coordinator', '%s/%s' % (SYSTEM, CONTROLLER_UNIT), 0o644),
            'workers.json': ('coordinator', WORKERS_JSON, 0o600),
            'fleet.json': ('coordinator', FLEET_JSON, 0o600),
            'routes.caddy': ('router', self.routes_path, 0o644),
            'withdrawn.caddy': ('router', self.routes_path, 0o644),
            'tfvars': ('coordinator', self.tfvars_path, 0o600),
        }

    def unit_limits(self, key):
        spec = self.request['units'][key]
        return {'MemoryMax': spec['memory_max'], 'CPUQuota': spec['cpu_quota'], 'CPUWeight': str(spec['cpu_weight']),
                'IOWeight': str(spec.get('io_weight', 20)), 'Nice': str(spec['nice'])}

    @staticmethod
    def systemd_run(unit, limits, argv, wait=False, environment=()):
        """A transient unit under display's resource limits.

        Asynchronous units remain after a clean exit so `status` reports their
        result; `ingest-stop`, `measure-stop` or `retire` unload them.
        """
        command = ['systemd-run', '--unit=' + unit, '--quiet', '--property=MemorySwapMax=0']
        command += ['--wait', '--pipe', '--collect'] if wait else ['--property=RemainAfterExit=yes']
        command += ['--property=%s=%s' % (key, value) for key, value in limits.items()]
        command += ['--setenv=' + item for item in environment]
        return command + list(map(str, argv))

    def transient(self):
        """Argv of every transient unit; `{start}`, `{through}` and `{run}` are filled at start."""
        r = self.request
        journal, root = r['journal'], r['publication']['root']
        ingest = [self.binary('transparent-event-ingest'), '--state-dir', journal['node_cache_dir'],
                  '--workers', str(r['units']['ingest']['workers']), '--txid-display',
                  '--start-height', str(journal['start_height'])]
        controller = self.binary('txid-display-controller')
        seal = ['--geometry', r['controller']['geometry']]
        for key in SEAL_KEYS:
            seal += ['--' + key.replace('_', '-'), str(r['controller'][key])]
        measure = r['measure']['output'] + '/{run}'
        observe = ['/usr/bin/python3', self.binary('txid-display-observe.py')]
        return {
            'ingest': self.systemd_run(INGEST_UNIT, self.unit_limits('ingest'),
                                       ingest + ['--data-dir', journal['data_dir']]),
            # Ten blocks into a separate directory, so the real journal starts clean.
            'ingest-smoke': self.systemd_run(SMOKE_UNIT, self.unit_limits('ingest'), ingest + [
                '--stop-height', str(journal['start_height'] + 9), '--data-dir', journal['data_dir'] + '.smoke'],
                wait=True),
            'plan-start': self.systemd_run('transparent-txid-display-plan-start.service',
                                           self.unit_limits('bootstrap'), [
                controller, 'plan-start', '--journal', journal['data_dir'],
                '--archives', str(r['controller']['archives']),
                '--replay-seals', str(r['controller']['replay_seals']), *seal], wait=True),
            'bootstrap': self.systemd_run(BOOTSTRAP_UNIT, self.unit_limits('bootstrap'), [
                controller, 'bootstrap', '--root', root, '--journal', journal['data_dir'],
                '--start', '{start}', '--through', '{through}', *seal]),
            'verify': self.systemd_run('transparent-txid-display-verify.service', self.unit_limits('controller'),
                                       [controller, 'verify', '--root', root, '--journal', journal['data_dir']],
                                       wait=True),
            'observer': self.systemd_run(OBSERVER_UNIT, MEASURE_LIMITS, observe + [
                'observer', '--rpc-url', r['controller']['rpc_url'], '--cookie', r['controller']['rpc_cookie'],
                '--interval-ms', '250', '--out', measure + '/observer.jsonl']),
            'mapwatch': self.systemd_run(MAPWATCH_UNIT, MEASURE_LIMITS, observe + [
                'mapwatch', '--url', r['measure']['public_url'] + '/v1/txid/shards', '--interval-ms', '1000',
                '--out-dir', measure + '/mapwatch']),
        }

    def verify_only(self, key, directory):
        spec = self.request['units'][key]
        return self.systemd_run('transparent-txid-display-verify-only.service', self.unit_limits(key), [
            self.binary('transparent-txid-server'), '--role', ROLES[key], '--publication-dir', directory,
            '--cache-bytes', str(spec['cache_bytes']), '--verify-only'],
            wait=True, environment=('TRANSPARENT_BUILD_THREADS=%d' % spec['build_threads'],))

    def expected_firewall(self, enabled):
        rule = {'protocol': 'tcp', 'port_range': str(self.request['ports']['worker']),
                'source_tags': sorted(FIREWALL_TAGS), 'source_addresses': 0}
        return {'changes': [{'address': FIREWALL, 'actions': ['update']}],
                'added': [rule] if enabled else [], 'removed': [] if enabled else [rule]}

    def plan(self):
        if self._plan is None:
            files = self.render()
            targets = self.targets()
            self._plan = {
                'schema': PLAN_SCHEMA, 'request_sha256': self.request_sha256,
                'source_sha': self.request['source_sha'], 'release_dir': self.release_dir,
                'hosts': {key: self.host(key) for key in HOST_KEYS},
                'phases': list(PHASES),
                'files': {name: {'host': targets[name][0], 'path': targets[name][1],
                                 'mode': '%04o' % targets[name][2], 'sha256': sha256(text)}
                          for name, text in sorted(files.items())},
                'release': {key: list(names) for key, names in HOST_RELEASE.items()},
                'templates': {name: file_sha256(self.root / path) for name, path in sorted(RELEASE_FILES.items())},
                'router_hook': {
                    'host': 'coordinator', 'script': LIVE_FLEET,
                    'previous_sha256': self.request['router']['live_fleet_sha256'],
                    'new_sha256': file_sha256(self.root / 'transparent/ops/scripts/transparent-live-fleet.py'),
                    'fleet_config': HISTORY_FLEET, 'route_imports': [self.request['router']['import_glob']],
                    'restart': list(HISTORY_DAEMONS), 'router_dir': self.routes_dir},
                'terraform': {
                    'root': self.request['terraform']['root'], 'wrapper': self.request['terraform']['wrapper'],
                    'tfvars': self.tfvars_path,
                    'sources': {name: file_sha256(self.root / 'ops/infra/digitalocean/production' / name)
                                for name in TF_SOURCES},
                    'expected': self.expected_firewall(True)},
                'transient': self.transient(),
            }
        return self._plan

    def plan_sha256(self):
        return durable.digest(self.plan())

    def check_plan(self, expected):
        require(isinstance(expected, str) and HEX64.fullmatch(expected), '--expect-plan-sha256 is required')
        actual = self.plan_sha256()
        require(actual == expected, 'plan changed since review: %s, expected %s' % (actual, expected))

    # --------------------------------------------------------- host helpers

    def run(self, key, argv, timeout=120, check=True):
        argv = [str(part) for part in argv]
        code, output = self.ex.run(self.host(key), argv, timeout)
        if check and code:
            raise PocError('%s: %s exited %d: %s' % (key, shlex.join(argv[:3]), code, output[-1500:]))
        return code, output

    def mode_of(self, key, path):
        code, output = self.run(key, ['stat', '-c', '%a', path], 30, check=False)
        return int(output.strip(), 8) if code == 0 and re.fullmatch(r'[0-7]{3,4}', output.strip()) else None

    def unit_state(self, key, unit):
        _, output = self.run(key, ['systemctl', 'show', unit, '--no-pager', '-p',
                                   'LoadState,ActiveState,SubState,Result,NRestarts,FragmentPath'], 30, check=False)
        state = dict(line.split('=', 1) for line in output.splitlines() if '=' in line)
        return {'load': state.get('LoadState', ''), 'active': state.get('ActiveState', ''),
                'sub': state.get('SubState', ''), 'result': state.get('Result', ''),
                'restarts': int(state.get('NRestarts') or 0), 'fragment': state.get('FragmentPath', '')}

    def running(self, key, unit):
        return self.unit_state(key, unit)['active'] in ('active', 'activating', 'reloading', 'deactivating')

    def exists(self, key, path, kind='-e'):
        return self.run(key, ['test', kind, path], 30, check=False)[0] == 0

    def disk(self, key, path):
        """`(size, available)` bytes of the filesystem holding `path` or its nearest existing ancestor."""
        script = 'p="$1"; while [ ! -e "$p" ]; do p=$(dirname "$p"); done; df -B1 --output=size,avail "$p"'
        _, output = self.run(key, ['sh', '-c', script, 'sh', path], 30)
        size, available = output.strip().splitlines()[-1].split()
        return int(size), int(available)

    def meminfo(self, key):
        text = self.ex.read(self.host(key), '/proc/meminfo') or ''
        values = {line.split(':')[0]: int(line.split()[1]) * 1024 for line in text.splitlines() if line.endswith('kB')}
        require('MemTotal' in values and 'MemAvailable' in values, '%s: /proc/meminfo is unreadable' % key)
        return values['MemTotal'], values['MemAvailable']

    def listening(self, key, port):
        _, output = self.run(key, ['ss', '-ltnH'], 30)
        return [fields[3] for fields in map(str.split, output.splitlines())
                if len(fields) > 3 and fields[3].rsplit(':', 1)[-1] == str(port)]

    def read_json(self, key, path):
        text = self.ex.read(self.host(key), path)
        return None if text is None else json.loads(text)

    def wait(self, description, probe, timeout):
        """Poll `probe` until it returns a truthy value; PocError after `timeout` seconds."""
        deadline = self.clock() + timeout
        last = None
        while True:
            try:
                value = probe()
                if value:
                    return value
            except (PocError, ValueError, KeyError, TypeError) as error:
                last = error
            if self.clock() >= deadline:
                raise PocError('%s did not happen within %ds%s' % (description, timeout,
                                                                    ' (%s)' % last if last else ''))
            self.sleep(self.poll_seconds)

    # ---------------------------------------------------- lock and fences

    @contextlib.contextmanager
    def locked(self, action):
        """Hold the production lock for one journaled action; the fence runs first.

        A transaction still `running` on exit is committed, or failed on error;
        one that moved to another status (planned, rolled-back) keeps it.
        """
        with self.lock_factory() as lock:
            self.lock = lock
            try:
                self.guard()
                if action is None:
                    yield None
                    return
                transaction = self.transaction = Transaction.create(self, action)
                try:
                    yield transaction
                except BaseException as error:
                    if transaction.status == 'running':
                        transaction.finish('failed', error='%s: %s' % (type(error).__name__, error))
                    raise
                if transaction.status == 'running':
                    transaction.finish('committed')
            finally:
                self.lock = None

    def guard(self):
        """Before every effect: the lock is still ours and no schema owner is unfinished."""
        require(self.lock is not None, 'production lock is not held')
        self.lock.verify()
        if self.inventory.lock.get('type') == 'pinned_host':
            schema_fence.local_schema_fence()
        else:
            schema_fence.schema_mutation_fence(lambda path: self.ex.read(self.inventory.lock['host'], path))

    # ----------------------------------------------------------- preflight

    def history_problems(self):
        """History must be healthy before display adds anything beside it."""
        problems = []
        coordinator = self.host('coordinator')
        membership = self.read_json('coordinator', MEMBERSHIP)
        if membership is None:
            problems.append('history membership.json is missing')
        else:
            if (membership.get('routed_recent') or 0) < MIN_ROUTED_RECENT:
                problems.append('history routes %s recent replicas; at least %d are required'
                                % (membership.get('routed_recent'), MIN_ROUTED_RECENT))
            if self.now() - float(membership.get('updated_unix') or 0) > MEMBERSHIP_SECONDS:
                problems.append('history membership.json is stale')
        status, body = self.ex.http_get(coordinator, HISTORY_STATUS, 5)
        try:
            history = json.loads(body) if status == 200 else None
        except ValueError:
            history = None
        if not isinstance(history, dict):
            problems.append('history publication status is unavailable (HTTP %s)' % status)
        else:
            freshness = history.get('freshness_seconds')
            if not isinstance(freshness, (int, float)) or freshness > FRESHNESS_SECONDS:
                problems.append('history freshness %s s exceeds %d s' % (freshness, FRESHNESS_SECONDS))
            if (history.get('ready_replicas') or 0) < MIN_ROUTED_RECENT:
                problems.append('history reports %s ready recent replicas' % history.get('ready_replicas'))
        path = self.request['history']['load_status']
        load = self.read_json('coordinator', path)
        if load is None:
            problems.append('5 QPS load status %s is missing' % path)
        else:
            trailing = load.get('trailing_60s') or {}
            if load.get('mode') != 'running':
                problems.append('5 QPS load is %s, not running' % load.get('mode'))
            if trailing.get('errors') != 0 or not trailing.get('exact'):
                problems.append('5 QPS load was not exact over the trailing minute')
            if self.ex.read(coordinator, posixpath.join(posixpath.dirname(path), 'latched.json')) is not None:
                problems.append('5 QPS load has a latched incident')
            if self.now() - iso_unix(load.get('utc')) > LOAD_STATUS_SECONDS:
                problems.append('5 QPS load status is stale')
        if self.ex.read(coordinator, ACTUATOR_OPERATION) is not None:
            problems.append('a fleet actuator operation is open')
        return problems

    def reservations(self, action):
        """Display MemoryMax per host that `action` is about to start."""
        r = self.request['units']
        if action == 'workers':
            return {key: 0 if self.running(key, WORKER_UNIT) else memory_bytes(r[key]['memory_max'])
                    for key in WORKERS}
        starts = {'controller': (CONTROLLER_UNIT, 'controller'), 'ingest-start': (INGEST_UNIT, 'ingest'),
                  'ingest-smoke': (SMOKE_UNIT, 'ingest'), 'bootstrap': (BOOTSTRAP_UNIT, 'bootstrap'),
                  'verify': (None, 'controller')}
        if action in starts:
            unit, limits = starts[action]
            running = unit is not None and self.running('coordinator', unit)
            return {'coordinator': 0 if running else memory_bytes(r[limits]['memory_max'])}
        if action == 'measure-start':
            return {'coordinator': 2 * memory_bytes(MEASURE_LIMITS['MemoryMax'])}
        return {}

    def memory_problems(self, reserve):
        """Both W0's and the current MemAvailable must keep 20% of the host after `reserve`."""
        problems = []
        for key, reservation in reserve.items():
            total, available = self.meminfo(key)
            baseline = self.request['baseline'][key]['mem_available_bytes']
            for label, value in (('W0 baseline', baseline), ('current', available)):
                if value - reservation < FREE_FLOOR * total:
                    problems.append('%s: %s MemAvailable %d minus display MemoryMax %d is under %d%% of %d'
                                    % (key, label, value, reservation, FREE_FLOOR * 100, total))
        return problems

    def disk_problems(self, paths):
        problems = []
        for key, path in paths:
            size, available = self.disk(key, path)
            if available < FREE_FLOOR * size:
                problems.append('%s: %s has %.1f%% free, under %d%%'
                                % (key, path, 100 * available / size, FREE_FLOOR * 100))
        return problems

    def port_problems(self, key, port, unit):
        if self.running(key, unit):
            return []
        found = self.listening(key, port)
        return ['%s: port %d is already in use (%s)' % (key, port, ', '.join(found))] if found else []

    def action_problems(self, action):
        """Memory, disk, port and input checks of one forward action."""
        r = self.request
        problems = self.memory_problems(self.reservations(action))
        paths = {'stage': [(key, OPT) for key in HOST_RELEASE],
                 'workers': [(key, WORKER_DATA) for key in WORKERS],
                 'route': [('router', '/etc/caddy')], 'router-hook': [('router', '/etc/caddy')],
                 'measure-start': [('coordinator', r['measure']['output'])]}
        default = [('coordinator', p) for p in (r['journal']['data_dir'], r['publication']['root'], OPT)]
        problems += self.disk_problems(paths.get(action, default))
        if action == 'workers':
            for key in WORKERS:
                problems += self.port_problems(key, r['ports']['worker'], WORKER_UNIT)
        if action == 'controller':
            problems += self.port_problems('coordinator', r['ports']['controller_status'], CONTROLLER_UNIT)
        if action in ('ingest-start', 'ingest-smoke'):
            state = posixpath.join(r['journal']['node_cache_dir'], NODE_STATE)
            if not self.exists('coordinator', state, '-d'):
                problems.append('coordinator: node state directory %s is missing' % state)
        return problems

    def require_preflight(self, action):
        problems = self.history_problems() + self.action_problems(action)
        for problem in problems:
            self.out('problem: ' + problem)
        require(not problems, 'preflight refused %s before any change' % action)

    def preflight(self):
        """Read-only, apart from taking and releasing the production lock."""
        problems = self.history_problems()
        for action in ('stage', 'workers', 'controller', 'ingest-start'):
            problems += ['%s: %s' % (action, p) for p in self.action_problems(action)]
        try:
            with self.locked(None):
                pass
        except Exception as error:
            problems.append('production lock or schema fence: %s' % error)
        return problems

    # -------------------------------------------------------------- stage

    def stage(self, transaction, archive):
        """Upload and verify the exact CI release to every host; inert."""
        r = self.request['release']
        require(archive, '--archive (the CI transparent-txid-display bundle) is required for stage')
        require(file_sha256(archive) == r['archive_sha256'], 'release archive digest differs from the request')
        with tempfile.TemporaryDirectory(prefix='txid-display-release-') as scratch:
            bundle = Path(scratch) / 'bundle'
            # Verifies the revision, the checksum inventory and every file's digest.
            load_release().extract(Path(archive), bundle, self.request['source_sha'], KIND)
            for name, digest in {**r['binaries'], **r['files']}.items():
                require(file_sha256(bundle / name) == digest, 'release %s differs from the request' % name)
            for name, path in RELEASE_FILES.items():
                require(file_sha256(bundle / name) == file_sha256(self.root / path),
                        'release %s differs from this checkout; deploy from the release revision' % name)
            for key, names in HOST_RELEASE.items():
                self.stage_host(transaction, key, bundle, names)

    def stage_host(self, transaction, key, bundle, names):
        host = self.host(key)
        digests = {**self.request['release']['binaries'], **self.request['release']['files']}
        present = {name: self.ex.sha256(host, self.binary(name)) for name in names}
        require(all(present[name] in (None, digests[name]) for name in names),
                '%s: immutable release %s holds other bytes' % (key, self.release_dir))
        if all(digest is None for digest in present.values()):
            self.guard()
            partial = '%s/.partial-%s' % (RELEASES, transaction.id)
            transaction.event('staging release on %s' % key)
            self.ex.mkdir(host, partial, 0o755)
            for name in names:
                self.ex.upload(host, bundle / name, '%s/%s' % (partial, name), 0o755)
                require(self.ex.sha256(host, '%s/%s' % (partial, name)) == digests[name],
                        '%s: uploaded %s does not match' % (key, name))
            self.ex.rename(host, partial, self.release_dir)
        else:
            require(all(present.values()), '%s: release %s is partially staged; inspect it' % (key, self.release_dir))
        for name in names:
            argv, status, marker = self.self_check(name)
            code, output = self.run(key, argv, SELF_CHECK_SECONDS, check=False)
            require(code == status and marker in output, '%s: staged %s failed its self-check (exit %d): %s'
                    % (key, name, code, output[-500:]))
        transaction.event('%s: release staged and every executable runs' % key)

    def self_check(self, name):
        """A non-mutating invocation of a staged executable: `(argv, exit status, output marker)`."""
        path = self.binary(name)
        if name == 'txid-control':
            # `txid-control SOCKET < command.json` has no --help: argv[1] is the
            # socket. Without one it prints its usage and exits 1, touching nothing.
            return [path], 1, 'usage: txid-control SOCKET'
        if name.endswith('.py'):
            return ['/usr/bin/python3', path, '--help'], 0, ''
        return [path, '--help'], 0, ''

    # --------------------------------------------------------- terraform

    def terraform_summary(self, plan_file):
        """Resource changes and worker-firewall rule delta of a saved plan, computed on the coordinator.

        `terraform show -json` carries variable values, secrets included, so
        only this summary leaves the host.
        """
        _, output = self.run('coordinator', ['python3', '-c', SUMMARIZE, self.request['terraform']['wrapper'],
                                             plan_file, FIREWALL], 600)
        return json.loads(output.strip().splitlines()[-1])

    def terraform_plan(self, transaction, enabled):
        """Save and check a plan with no lock held: the wrapper takes the production lock itself.

        A plan with no change at all means the rule is already as wanted.
        Anything but exactly one in-place worker firewall update is refused.
        """
        require(self.lock is None, 'the Terraform wrapper takes the production lock itself')
        plan_file = '%s/%s.tfplan' % (TERRAFORM_PLANS, transaction.id)
        record = transaction.data['terraform'] = {'plan_file': plan_file, 'plan_sha256': None, 'summary': None,
                                                  'enabled': enabled}
        transaction.save()
        try:
            self.run('coordinator', [self.request['terraform']['wrapper'], 'plan', '-input=false',
                                     '-out=' + plan_file], 900)
            summary = self.terraform_summary(plan_file)
            digest = self.ex.sha256(self.host('coordinator'), plan_file)
        except BaseException:
            self.discard_plan(transaction)
            raise
        record.update(plan_sha256=digest, summary=summary)
        transaction.save()
        if summary == {'changes': [], 'added': [], 'removed': []}:
            self.discard_plan(transaction)
            transaction.finish('committed', firewall='already ' + ('open' if enabled else 'closed'))
            return None
        if summary != self.expected_firewall(enabled):
            self.discard_plan(transaction)
            raise PocError('saved plan is not exactly one in-place worker firewall change: %s'
                           % json.dumps(summary, sort_keys=True))
        transaction.finish('planned')
        self.out(json.dumps({'terraform_plan': summary, 'plan_file': plan_file, 'plan_sha256': digest},
                            sort_keys=True))
        self.out('review the saved plan, then re-run with --terraform-plan-sha256 %s' % digest)
        return digest

    def terraform_apply(self, transaction, expected):
        """Apply exactly the reviewed saved plan, then delete it (it holds variable values)."""
        record = transaction.data.get('terraform')
        require(record and transaction.status == 'planned', 'transaction %s has no saved plan to apply'
                % transaction.id)
        require(expected == record['plan_sha256'],
                '--terraform-plan-sha256 must equal the reviewed saved plan %s' % record['plan_sha256'])
        coordinator = self.host('coordinator')
        require(self.ex.sha256(coordinator, record['plan_file']) == expected, 'saved plan file changed since review')
        require(self.terraform_summary(record['plan_file']) == self.expected_firewall(record['enabled']),
                'saved plan summary changed since review')
        require(self.lock is None, 'the Terraform wrapper takes the production lock itself')
        self.run('coordinator', [self.request['terraform']['wrapper'], 'apply', record['plan_file']], 1800)
        self.discard_plan(transaction)
        transaction.finish('committed', applied_plan_sha256=expected)

    def discard_plan(self, transaction):
        """Saved plans hold variable values; keep none that will not be applied."""
        with self.locked(None):
            self.ex.remove(self.host('coordinator'), transaction.data['terraform']['plan_file'])

    def transactions(self, action, status, of=None):
        """This request's transactions of `action` in `status`, oldest first."""
        found = []
        for path in self.journal_dir.glob('txid-display-*.json'):
            data = json.loads(path.read_text())
            if (data.get('kind') == JOURNAL_KIND and data.get('request_sha256') == self.request_sha256
                    and data.get('action') == action and data.get('status') == status
                    and (of is None or data.get('of') == of)):
                found.append((data['created_unix'], data['id']))
        return [Transaction.load(self, identifier) for _, identifier in sorted(found)]

    def planned(self, action, of=None):
        found = self.transactions(action, 'planned', of)
        require(found, 'no planned %s transaction to apply' % action)
        return found[-1]

    def apply_planned(self, action, expected, of=None):
        """Second invocation of a Terraform step: fence and preflight, then apply unlocked."""
        transaction = self.transaction = self.planned(action, of)
        with self.locked(None):
            if action == 'deploy-firewall':
                self.require_preflight('firewall')
        self.terraform_apply(transaction, expected)
        return transaction

    def firewall(self, expected, terraform_plan_sha256):
        """Two invocations: save and summarize a plan, then apply that exact plan."""
        self.check_plan(expected)
        if terraform_plan_sha256:
            return self.apply_planned('deploy-firewall', terraform_plan_sha256)
        with self.locked('deploy-firewall') as transaction:
            self.require_preflight('firewall')
            coordinator = self.host('coordinator')
            for name, digest in self.plan()['terraform']['sources'].items():
                path = posixpath.join(self.request['terraform']['root'], name)
                require(self.ex.sha256(coordinator, path) == digest,
                        'coordinator Terraform root %s differs from this checkout' % path)
            transaction.change('coordinator', self.tfvars_path, self.render()['tfvars'], 0o600)
            self.guard()
            self.ex.mkdir(coordinator, TERRAFORM_PLANS, 0o700)
            transaction.finish('planning')
        try:
            self.terraform_plan(transaction, True)
        except BaseException:
            # A refused or failed plan opens nothing; put the variable file back.
            with self.locked(None):
                self.restore(transaction)
            raise

    def firewall_rollback(self, transaction, terraform_plan_sha256):
        """Close the rule again: remove the variable file, then plan and apply its removal."""
        if terraform_plan_sha256:
            self.apply_planned('firewall-rollback', terraform_plan_sha256, of=transaction.id)
            transaction.finish('rolled-back', rolled_back_by=self.transaction.id)
            return
        with self.locked('firewall-rollback') as removal:
            removal.data['of'] = transaction.id
            removal.remove('coordinator', self.tfvars_path)
            removal.finish('planning')
        try:
            if self.terraform_plan(removal, False) is None:
                transaction.finish('rolled-back', rolled_back_by=removal.id)
        except BaseException:
            with self.locked(None):
                self.restore(removal)
            raise

    # ------------------------------------------------------- router hook

    def simulate_import(self, caddyfile):
        """The live router with the hook's import where the history adapter renders it."""
        result, inserted = [], 0
        for line in caddyfile.split('\n'):
            if line.startswith('\t@metadata path '):
                result.append(self.import_line)
                inserted += 1
            result.append(line)
        require(inserted, 'the live router has no metadata route; history is not serving normally')
        return '\n'.join(result)

    def caddy_validate(self, transaction, files):
        """`caddy validate` a scratch composition on the router, then remove the scratch files."""
        router = self.host('router')
        scratch = '%s/%s' % (VALIDATE, transaction.id)
        self.guard()
        self.ex.mkdir(router, scratch, 0o700)
        try:
            for name, text in files.items():
                self.ex.write(router, '%s/%s' % (scratch, name), text.encode(), 0o600)
            code, output = self.run('router', ['caddy', 'validate', '--config', scratch + '/Caddyfile',
                                               '--adapter', 'caddyfile'], 120, check=False)
        finally:
            for name in files:
                self.ex.remove(router, '%s/%s' % (scratch, name))
        require(code == 0, 'caddy rejected the composed router: %s' % output[-1500:])

    def hook_rendered(self):
        """The live router once every site carries the import line."""
        text = self.ex.read(self.host('router'), CADDYFILE) or ''
        lines = text.split('\n')
        sites = sum(line.startswith('\t@metadata path ') for line in lines)
        return text if sites and lines.count(self.import_line) == sites else None

    def router_hook(self, transaction):
        plan = self.plan()['router_hook']
        coordinator, router = self.host('coordinator'), self.host('router')
        glob = self.request['router']['import_glob']
        current = self.ex.sha256(coordinator, LIVE_FLEET)
        require(current in (plan['previous_sha256'], plan['new_sha256']),
                'deployed history adapter %s is neither the reviewed one nor this checkout' % current)
        fleet = self.read_json('coordinator', HISTORY_FLEET)
        require(isinstance(fleet, dict), 'history fleet.json is missing')
        require(fleet.get('route_imports') in (None, [], [glob]), 'history fleet.json imports other routes')
        before = self.ex.read(router, CADDYFILE)
        require(before is not None, 'router Caddyfile is missing')
        if self.import_line not in before.split('\n'):
            # A bad hook would stall every history activation at its own
            # `caddy validate`; prove the composition before touching history.
            self.caddy_validate(transaction, {'Caddyfile': self.simulate_import(before)})
        self.guard()
        self.ex.mkdir(router, self.routes_dir, 0o755)
        # Loaded code persists in these daemons; activations exec the script fresh.
        transaction.undo('post', 'coordinator', 'restart', units=list(HISTORY_DAEMONS))
        script = (self.root / 'transparent/ops/scripts/transparent-live-fleet.py').read_bytes()
        changed = transaction.change('coordinator', LIVE_FLEET, script, 0o755)
        # The history adapter's own atomic_json serialization.
        changed |= transaction.change('coordinator', HISTORY_FLEET, json.dumps({**fleet, 'route_imports': [glob]}),
                                      0o600)
        if changed:
            self.apply('coordinator', {'op': 'restart', 'units': list(HISTORY_DAEMONS)})
        rendered = self.wait('the history router render with the import hook', self.hook_rendered, 900)
        strip = lambda text: [line for line in text.split('\n') if line != self.import_line]  # noqa: E731
        transaction.event('history router imports %s' % glob, other_changes=strip(rendered) != strip(before))

    # ----------------------------------------------------------- workers

    def fleet(self, transaction, request, timeout):
        """One call of the production adapter on the coordinator; request and reply travel as files."""
        coordinator = self.host('coordinator')
        self.guard()
        transaction.data['fleet_calls'] = transaction.data.get('fleet_calls', 0) + 1
        transaction.save()
        name = '%s-%d' % (transaction.id, transaction.data['fleet_calls'])
        request_path, reply_path = '%s/%s.json' % (REQUESTS, name), '%s/%s.reply.json' % (REQUESTS, name)
        self.ex.mkdir(coordinator, REQUESTS, 0o700)
        self.ex.write(coordinator, request_path, json.dumps(request).encode(), 0o600)
        try:
            code, output = self.ex.run(coordinator, ['/usr/bin/python3', self.binary('txid-display-fleet.py'),
                                                     FLEET_JSON, request_path, reply_path], timeout + 30)
            raw = self.ex.read(coordinator, reply_path)
        finally:
            self.ex.remove(coordinator, request_path)
            self.ex.remove(coordinator, reply_path)
        reply = json.loads(raw) if raw else {}
        require(code == 0 and reply.get('ok') is True, 'fleet %s on %s failed: %s' % (
            request['operation'], request['worker'], reply.get('error') or output[-1500:]))
        return reply

    def control(self, transaction, worker, command, timeout=85):
        return self.fleet(transaction, {'operation': 'control', 'worker': worker, 'command': command,
                                        'deadline_seconds': timeout}, timeout)['reply']

    def candidate(self, map_sha256):
        """The coordinator candidate directory whose txid-shards.json has this digest."""
        root = self.request['publication']['root']
        _, output = self.run('coordinator', ['find', root, '-mindepth', '2', '-maxdepth', '2', '-type', 'f',
                                             '-path', root + '/candidate-*/txid-shards.json'], 60)
        for path in sorted(output.split()):
            if self.ex.sha256(self.host('coordinator'), path) == map_sha256:
                return posixpath.dirname(path)
        raise PocError('no candidate under %s has map %s; run txid-display-bootstrap first' % (root, map_sha256))

    def workers(self, transaction, map_sha256):
        require(isinstance(map_sha256, str) and HEX64.fullmatch(map_sha256),
                '--publication-map-sha256 (the bootstrap candidate map) is required for workers')
        # Once running, the controller owns every worker's map and `expected`;
        # a restart or prepare here would race it or regress the served map.
        require(not self.running('coordinator', CONTROLLER_UNIT),
                'the controller drives the workers; stop it first (txid-display-stop)')
        source = self.candidate(map_sha256)
        files = self.render()
        self.guard()
        self.ex.mkdir(self.host('coordinator'), STATE + '/ssh', 0o700)
        transaction.change('coordinator', FLEET_JSON, files['fleet.json'], 0o600)
        for key in WORKERS:
            self.worker(transaction, key, files['unit:' + key], source, map_sha256)

    def worker(self, transaction, key, unit_text, source, map_sha256):
        """Install and start one worker; only a worker with no active map takes `map_sha256`.

        A worker that already serves a map (from an earlier deploy or the
        controller) restarts onto its own active record and keeps it, since the
        bootstrap candidate may be older; the controller advances it later.
        The unit restarts only when its bytes (and so the release) changed.
        """
        host = self.host(key)
        binary = self.request['release']['binaries']['transparent-txid-server']
        timeout = self.request['units'][key]['ready_timeout_seconds']
        self.guard()
        for path in (PUBLICATIONS, STAGED, RUNTIME_CACHE, WORKER_STATE):
            self.ex.mkdir(host, path, 0o700)
        record = self.read_json(key, ACTIVE_RECORD)
        serving = record.get('map_sha256') if isinstance(record, dict) else None
        directory = '%s/%s' % (PUBLICATIONS, map_sha256)
        if serving is None:
            shipped = self.fleet(transaction, {'operation': 'ship', 'worker': key, 'kind': 'candidate',
                                               'source': source, 'name': map_sha256, 'link_dest': None,
                                               'deadline_seconds': SHIP_SECONDS}, SHIP_SECONDS)
            require(shipped.get('directory') == directory, 'adapter shipped to %s' % shipped.get('directory'))
            transaction.event('%s: shipped candidate %s' % (key, map_sha256), seconds=shipped.get('seconds'))
            self.guard()
            self.run(key, self.verify_only(key, directory), timeout)
            transaction.event('%s: --verify-only accepted the candidate' % key)
        unit = '%s/%s' % (SYSTEM, WORKER_UNIT)
        running = self.running(key, WORKER_UNIT)
        if running and self.ex.read(host, unit) == unit_text:
            transaction.event('%s: unit unchanged; the worker keeps running' % key)
        else:
            if running:
                transaction.undo('post', key, 'start-unit', unit=WORKER_UNIT)
            transaction.undo('pre', key, 'stop-unit', unit=WORKER_UNIT)
            transaction.undo('post', key, 'systemctl', args=['daemon-reload'])
            transaction.change(key, unit, unit_text, 0o644)
            self.guard()
            self.ex.systemctl(host, 'daemon-reload')
            self.ex.systemctl(host, 'enable', WORKER_UNIT)
            self.ex.systemctl(host, 'restart', WORKER_UNIT)
        status = self.wait('%s control socket' % key, lambda: self.control(transaction, key, {'operation': 'status'}),
                           300)
        require(status.get('role') == ROLES[key], '%s reports role %s' % (key, status.get('role')))
        require(status.get('binary_sha256') == binary, '%s runs %s, not the release' % (key, status.get('binary_sha256')))
        active = (status.get('active') or {}).get('map_sha256')
        require(active == serving, '%s serves %s, but its active record names %s' % (key, active, serving))
        if active is None:
            self.prepare(transaction, key, directory, map_sha256, timeout)
            self.control(transaction, key, {'operation': 'activate', 'expected': '', 'map_sha256': map_sha256})
            active = map_sha256
        elif active != map_sha256:
            transaction.event('%s: keeps serving %s, not the requested %s' % (key, active, map_sha256))
        self.wait('%s ready and warm on %s' % (key, active[:12]), lambda: self.worker_ready(key, active), timeout)
        transaction.event('%s: serving %s warm' % (key, active))

    def prepare(self, transaction, key, directory, map_sha256, timeout):
        """Prepare `map_sha256` on an empty worker within `timeout` seconds.

        txid-control waits at most 600 s for a reply, and a cold archive build
        can take longer. The worker keeps building after a call gives up, and
        a repeated prepare waits for that build and then answers from its warm
        candidate, so a failed call is repeated while status still shows this
        map preparing or prepared. Any other failure is final.
        """
        command = {'operation': 'prepare', 'expected': '',
                   'publication': {'directory': directory, 'map_sha256': map_sha256}}
        deadline = self.clock() + timeout
        while True:
            left = deadline - self.clock()
            try:
                return self.control(transaction, key, command, max(1, int(min(PREPARE_CALL_SECONDS, left))))
            except PocError:
                if self.clock() >= deadline:
                    raise
                status = self.control(transaction, key, {'operation': 'status'})
                building = {(status.get(field) or {}).get('map_sha256') for field in ('preparing', 'candidate')}
                if map_sha256 not in building:
                    raise
                transaction.event('%s: still preparing %s' % (key, map_sha256[:12]))

    def worker_ready(self, key, map_sha256):
        status, body = self.ex.http_get(self.host(key), self.ready_url(key), 5)
        if status != 200:
            return False
        ready = json.loads(body)
        return ready.get('role') == ROLES[key] and ready.get('map_sha256') == map_sha256 and ready.get('warm') is True

    # ------------------------------------------------------------- route

    def reload_caddy(self):
        self.guard()
        code, output = self.run('router', ['caddy', 'validate', '--config', CADDYFILE, '--adapter', 'caddyfile'],
                                120, check=False)
        require(code == 0, 'caddy rejected the live router: %s' % output[-1500:])
        self.ex.systemctl(self.host('router'), 'reload', 'caddy')

    def write_route(self, transaction, snippet):
        """Replace the display snippet and reload; never leave an unaccepted one in the glob."""
        transaction.undo('post', 'router', 'reload-caddy')
        if not transaction.change('router', self.routes_path, snippet, 0o644):
            return
        try:
            self.reload_caddy()
        except BaseException:
            self.restore(transaction)
            raise

    def route(self, transaction):
        router = self.host('router')
        live = self.ex.read(router, CADDYFILE) or ''
        require(self.import_line in live.split('\n'), 'the live router lacks %r; deploy router-hook first'
                % self.import_line.strip())
        snippet = self.render()['routes.caddy']
        scratch = '%s/%s' % (VALIDATE, transaction.id)
        composed = '\n'.join('\timport %s/routes.caddy' % scratch if line == self.import_line else line
                             for line in live.split('\n'))
        self.caddy_validate(transaction, {'Caddyfile': composed, 'routes.caddy': snippet})
        self.write_route(transaction, snippet)
        public = self.request['measure']['public_url']
        status, body = self.ex.http_get(router, public + '/v1/txid/shards', 10)
        require(status == 200 and isinstance(json.loads(body).get('shards'), list),
                'the display map is not served through the router (HTTP %s)' % status)
        status, _ = self.ex.http_get(router, public + '/v1/shards', 10)
        require(status == 200, 'history metadata is not served after the reload (HTTP %s)' % status)
        transaction.event('router serves /v1/txid/ and history metadata')

    # -------------------------------------------------------- controller

    def controller(self, transaction):
        for unit in JOURNAL_WRITERS:
            require(self.unit_state('coordinator', unit)['sub'] not in ('running', 'start'),
                    '%s still runs; the controller must be the only journal writer' % unit)
        for key in WORKERS:
            require(self.running(key, WORKER_UNIT), '%s worker is not running; deploy workers first' % key)
        files = self.render()
        coordinator = self.host('coordinator')
        self.guard()
        self.ex.mkdir(coordinator, STATE + '/ssh', 0o700)
        transaction.change('coordinator', FLEET_JSON, files['fleet.json'], 0o600)
        transaction.change('coordinator', WORKERS_JSON, files['workers.json'], 0o600)
        if self.running('coordinator', CONTROLLER_UNIT):
            transaction.undo('post', 'coordinator', 'start-unit', unit=CONTROLLER_UNIT)
        transaction.undo('pre', 'coordinator', 'stop-unit', unit=CONTROLLER_UNIT)
        transaction.undo('post', 'coordinator', 'systemctl', args=['daemon-reload'])
        transaction.change('coordinator', '%s/%s' % (SYSTEM, CONTROLLER_UNIT), files['unit:controller'], 0o644)
        self.guard()
        self.ex.systemctl(coordinator, 'daemon-reload')
        self.ex.systemctl(coordinator, 'enable', CONTROLLER_UNIT)
        self.ex.systemctl(coordinator, 'restart', CONTROLLER_UNIT)
        port = self.request['ports']['controller_status']
        self.wait('the controller status listener', lambda: '127.0.0.1:%d' % port in self.listening('coordinator', port),
                  120)
        # A crash loop shows as restarts within the first seconds.
        self.sleep(10)
        state = self.unit_state('coordinator', CONTROLLER_UNIT)
        require(state['active'] == 'active' and state['restarts'] == 0, 'the controller is not stable: %s' % state)
        transaction.event('controller running')

    # ------------------------------------------------- transient actions

    def start_transient(self, transaction, argv, unit):
        state = self.unit_state('coordinator', unit)
        require(state['load'] != 'loaded' or state['active'] in ('inactive', 'failed'),
                '%s is %s/%s; stop it first' % (unit, state['active'], state['sub']))
        self.guard()
        if state['active'] == 'failed':
            self.run('coordinator', ['systemctl', 'reset-failed', unit], 30, check=False)
        transaction.undo('pre', 'coordinator', 'stop-unit', unit=unit)
        self.run('coordinator', argv, 120)
        transaction.event('started %s' % unit, argv=argv)

    def ingest_start(self, transaction, smoke):
        r = self.request
        require(not self.running('coordinator', CONTROLLER_UNIT), 'the controller owns the journal; stop it first')
        require(not self.running('coordinator', INGEST_UNIT) or not smoke, 'the backfill is running')
        self.guard()
        self.ex.mkdir(self.host('coordinator'), posixpath.dirname(r['journal']['data_dir']), 0o755)
        transient = self.plan()['transient']
        if not smoke:
            self.start_transient(transaction, transient['ingest'], INGEST_UNIT)
            return
        self.guard()
        code, output = self.run('coordinator', transient['ingest-smoke'], 1800, check=False)
        transaction.event('smoke ingest exited %d' % code, output=output[-2000:])
        require(code == 0, 'smoke ingest failed (exit %d): %s' % (code, output[-1500:]))

    def ingest_status(self):
        result = {}
        for unit in (INGEST_UNIT, SMOKE_UNIT):
            _, log = self.run('coordinator', ['journalctl', '-u', unit, '-n', '20', '--no-pager', '-o', 'cat'],
                              30, check=False)
            result[unit] = {**self.unit_state('coordinator', unit), 'log': log.splitlines()[-20:]}
        return result

    def stop_units(self, transaction, key, names):
        """Stop units that are loaded; a rollback starts again only installed units that were active."""
        for unit in names:
            state = self.unit_state(key, unit)
            if state['load'] != 'loaded':
                continue
            if state['active'] in ('active', 'activating', 'reloading') and state['fragment'].startswith(SYSTEM + '/'):
                transaction.undo('post', key, 'start-unit', unit=unit)
            self.guard()
            self.run(key, ['systemctl', 'stop', unit], 120)
            transaction.event('stopped %s on %s' % (unit, key))

    def bootstrap(self, transaction, start, through):
        r = self.request
        require(self.unit_state('coordinator', INGEST_UNIT)['sub'] not in ('running', 'start'),
                'the backfill still runs; bootstrap reads the finished journal')
        require(not self.running('coordinator', CONTROLLER_UNIT), 'the controller runs; bootstrap starts it once')
        require((start is None) == (through is None), 'give both --start-height and --through-height, or neither')
        self.guard()
        self.ex.mkdir(self.host('coordinator'), r['publication']['root'], 0o755)
        transient = self.plan()['transient']
        if start is None:
            _, output = self.run('coordinator', transient['plan-start'], 3600)
            lines = [line for line in output.splitlines() if line.startswith('{')]
            require(lines, 'plan-start printed no JSON range')
            chosen = json.loads(lines[-1])
            start, through = chosen.get('start'), chosen.get('through')
            transaction.event('plan-start chose the bootstrap range', plan_start=chosen)
        # The journal's parent check reads block S0 - 1, so S0 starts above it.
        integer(start, r['journal']['start_height'] + 1, 1 << 40, 'bootstrap start height')
        integer(through, start, 1 << 40, 'bootstrap through height')
        argv = [part.replace('{start}', str(start)).replace('{through}', str(through))
                for part in transient['bootstrap']]
        transaction.data['bootstrap'] = {'start': start, 'through': through}
        self.start_transient(transaction, argv, BOOTSTRAP_UNIT)

    def verify(self, transaction):
        self.guard()
        code, output = self.run('coordinator', self.plan()['transient']['verify'], 7200, check=False)
        transaction.event('controller verify exited %d' % code, output=output[-2000:])
        require(code == 0, 'controller verify failed (exit %d): %s' % (code, output[-1500:]))

    def measure_start(self, transaction):
        run = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(self.now()))
        directory = '%s/%s' % (self.request['measure']['output'], run)
        self.guard()
        self.ex.mkdir(self.host('coordinator'), directory + '/mapwatch', 0o755)
        transaction.data['measure'] = {'run': run, 'directory': directory}
        transient = self.plan()['transient']
        for name, unit in (('observer', OBSERVER_UNIT), ('mapwatch', MAPWATCH_UNIT)):
            self.start_transient(transaction, [part.replace('{run}', run) for part in transient[name]], unit)

    # --------------------------------------------- stop, rollback, retire

    def stop(self, transaction):
        """Withdraw `/v1/txid/` with a 503 snippet, then stop the controller; under a minute.

        A route that cannot be withdrawn puts the previous snippet back, so the
        glob never holds bytes Caddy refused, and the controller still stops.
        """
        started = self.clock()
        failure = None
        if self.exists('router', self.routes_dir, '-d'):
            transaction.undo('post', 'router', 'reload-caddy')
            try:
                if transaction.change('router', self.routes_path, WITHDRAWN, 0o644):
                    self.reload_caddy()
            except Exception as error:
                failure = error
                entry = transaction.data['changes'][-1] if transaction.data['changes'] else None
                if entry and entry['path'] == self.routes_path:
                    self.revert(entry)
                    transaction.event('withdrawal refused; previous snippet restored')
        self.stop_units(transaction, 'coordinator', [CONTROLLER_UNIT])
        seconds = round(self.clock() - started, 3)
        transaction.data['stop_seconds'] = seconds
        transaction.save()
        if seconds > STOP_TARGET_SECONDS:
            self.out('warning: stop took %.1f s, beyond the %d s target' % (seconds, STOP_TARGET_SECONDS))
        if failure is not None:
            raise PocError('the controller stopped, but the route was not withdrawn: %s' % failure)

    def apply(self, key, step):
        """One recorded undo (or forward) step on a host."""
        host = self.host(key)
        self.guard()
        op = step['op']
        if op == 'systemctl':
            self.ex.systemctl(host, *step['args'])
        elif op == 'restart':
            for unit in step['units']:
                self.ex.systemctl(host, 'restart', unit)
        elif op == 'stop-unit':
            # Tolerates a unit that is already gone, so a rollback can repeat.
            for args in (['stop', step['unit']], ['disable', step['unit']]):
                self.run(key, ['systemctl', *args], 120, check=False)
        elif op == 'start-unit':
            if self.exists(key, '%s/%s' % (SYSTEM, step['unit'])):
                self.ex.systemctl(host, 'enable', step['unit'])
                self.ex.systemctl(host, 'start', step['unit'])
        elif op == 'reload-caddy':
            self.reload_caddy()
        else:
            raise PocError('unknown undo step %r' % op)

    def revert(self, entry):
        """Put one journaled file back to its prior bytes, or remove it if it was new."""
        host = self.host(entry['host'])
        self.guard()
        if entry['previous'] is None:
            self.ex.remove(host, entry['path'])
        else:
            self.ex.mkdir(host, posixpath.dirname(entry['path']), 0o755)
            self.ex.write(host, entry['path'], entry['previous'].encode(),
                          entry['previous_mode'] or entry['mode'] or 0o600)

    def drifted(self, transaction):
        """Journaled files that hold neither the bytes the transaction wrote nor its prior bytes.

        Another writer owns those bytes now (a history publisher redeploy of
        fleet.json, or a later display transaction), and putting the prior
        bytes back would silently undo its change. Entries are checked newest
        first against what the restore would leave, so a path changed twice
        in one transaction is judged step by step.
        """
        current, problems = {}, []
        for entry in reversed(transaction.data['changes']):
            key = (entry['host'], entry['path'])
            if key not in current:
                current[key] = self.ex.sha256(self.host(entry['host']), entry['path'])
            if current[key] not in (entry['new_sha256'], entry['previous_sha256']):
                problems.append('%s on %s changed since %s wrote it' % (entry['path'], entry['host'], transaction.id))
            current[key] = entry['previous_sha256']
        return problems

    def restore(self, transaction):
        """Undo a transaction's effects from its journal alone; safe to repeat.

        Refused before any effect if a journaled file has another writer's
        bytes. Units start again only after every file and daemon-reload is back.
        """
        data = transaction.data
        drifted = self.drifted(transaction)
        require(not drifted, 'rollback refused before any change: %s; roll back later transactions first, '
                'or use txid-display-retire, which removes the hook by content' % '; '.join(drifted))
        transaction.finish('rolling-back')
        for step in reversed([s for s in data['undo'] if s['when'] == 'pre']):
            self.apply(step['host'], step)
        for entry in reversed(data['changes']):
            self.revert(entry)
            transaction.event('restored %s on %s' % (entry['path'], entry['host']))
        post = [s for s in data['undo'] if s['when'] == 'post']
        for step in sorted(post, key=lambda s: s['op'] == 'start-unit'):
            self.apply(step['host'], step)
        transaction.finish('rolled-back')

    def rollback(self, identifier, terraform_plan_sha256=None):
        transaction = self.transaction = Transaction.load(self, identifier)
        require(transaction.status not in ('rolled-back', 'superseded'),
                'transaction %s is already %s' % (transaction.id, transaction.status))
        firewall = transaction.data.get('terraform')
        if firewall and firewall['enabled'] and transaction.status != 'planned':
            # The rule may exist: removing the variable file alone would not close it.
            return self.firewall_rollback(transaction, terraform_plan_sha256)
        with self.locked(None):
            self.restore(transaction)
            if firewall:
                self.ex.remove(self.host('coordinator'), firewall['plan_file'])
        if firewall and not firewall['enabled'] and firewall.get('summary'):
            self.out('note: the worker firewall stays as Terraform left it; deploy --phase firewall reopens it')

    def retire(self, expected, terraform_plan_sha256):
        """Remove every unit, the route and the hook, then close the firewall. Data stays."""
        self.check_plan(expected)
        if terraform_plan_sha256:
            self.apply_planned('retire', terraform_plan_sha256)
            self.print_data()
            return
        with self.locked('retire') as transaction:
            persistent = (('coordinator', CONTROLLER_UNIT), ('archive', WORKER_UNIT), ('recent', WORKER_UNIT))
            self.stop_units(transaction, 'coordinator', [CONTROLLER_UNIT, *TRANSIENT])
            for key in WORKERS:
                self.stop_units(transaction, key, [WORKER_UNIT])
            for key, unit in persistent:
                path = '%s/%s' % (SYSTEM, unit)
                if self.ex.read(self.host(key), path) is None:
                    continue
                transaction.undo('post', key, 'systemctl', args=['daemon-reload'])
                self.guard()
                # Disable while the unit file still names its install links.
                self.run(key, ['systemctl', 'disable', unit], 120, check=False)
                transaction.remove(key, path)
                self.ex.systemctl(self.host(key), 'daemon-reload')
            if self.ex.read(self.host('router'), self.routes_path) is not None:
                transaction.undo('post', 'router', 'reload-caddy')
                transaction.remove('router', self.routes_path)
                self.reload_caddy()
            self.unhook(transaction)
            transaction.remove('coordinator', self.tfvars_path)
            transaction.finish('planning')
        # Always plan: a plan without changes proves the rule is already closed.
        try:
            self.terraform_plan(transaction, False)
        except BaseException as error:
            transaction.finish('failed', error='%s: %s' % (type(error).__name__, error))
            raise
        self.print_data()

    def unhook(self, transaction):
        """Revert the router hook: the exact prior adapter while it is still ours, and the fleet key."""
        coordinator = self.host('coordinator')
        changed = False
        hooks = self.transactions('deploy-router-hook', 'committed')
        script = next((entry for hook in reversed(hooks) for entry in hook.data['changes']
                       if entry['path'] == LIVE_FLEET and entry['previous'] is not None), None)
        transaction.undo('post', 'coordinator', 'restart', units=list(HISTORY_DAEMONS))
        if script and self.ex.sha256(coordinator, LIVE_FLEET) == self.plan()['router_hook']['new_sha256']:
            changed |= transaction.change('coordinator', LIVE_FLEET, script['previous'], 0o755)
        fleet = self.read_json('coordinator', HISTORY_FLEET)
        if isinstance(fleet, dict) and 'route_imports' in fleet:
            fleet.pop('route_imports')
            changed |= transaction.change('coordinator', HISTORY_FLEET, json.dumps(fleet), 0o600)
        if changed:
            self.apply('coordinator', {'op': 'restart', 'units': list(HISTORY_DAEMONS)})

    def print_data(self):
        r = self.request
        self.out('retained data; deleting it needs separate approval:')
        paths = [('coordinator', r['journal']['data_dir']), ('coordinator', r['journal']['data_dir'] + '.smoke'),
                 ('coordinator', r['publication']['root']), ('coordinator', r['measure']['output'])]
        paths += [(key, OPT) for key in HOST_KEYS] + [(key, WORKER_DATA) for key in WORKERS]
        for key, path in paths:
            self.out('  %s:%s' % (self.host(key), path))

    # ------------------------------------------------------------ status

    def status(self):
        files = self.render()
        deployed = {}
        for name, (key, path, _) in self.targets().items():
            if name == 'withdrawn.caddy':
                continue
            digest = self.ex.sha256(self.host(key), path)
            deployed[name] = ('absent' if digest is None else 'planned' if digest == sha256(files[name])
                              else 'withdrawn' if digest == sha256(WITHDRAWN) else 'differs')
        result = {'request_sha256': self.request_sha256, 'plan_sha256': self.plan_sha256(), 'files': deployed,
                  'units': {}, 'workers': {}}
        for key, unit in [('coordinator', u) for u in (CONTROLLER_UNIT, *TRANSIENT)] + [(k, WORKER_UNIT) for k in WORKERS]:
            state = self.unit_state(key, unit)
            result['units']['%s@%s' % (unit, key)] = {k: state[k] for k in ('load', 'active', 'sub', 'result',
                                                                            'restarts')}
        for key in WORKERS:
            status, body = self.ex.http_get(self.host(key), self.ready_url(key), 5)
            try:
                ready = json.loads(body) if status == 200 else None
            except ValueError:
                ready = None
            result['workers'][key] = {'http': status, 'ready': ready}
        fleet = self.read_json('coordinator', HISTORY_FLEET) or {}
        result['router_hook'] = {'route_imports': fleet.get('route_imports'),
                                 'adapter_sha256': self.ex.sha256(self.host('coordinator'), LIVE_FLEET)}
        recent = []
        for path in sorted(self.journal_dir.glob('txid-display-*.json'), key=lambda p: p.stat().st_mtime)[-10:]:
            data = json.loads(path.read_text())
            if data.get('request_sha256') == self.request_sha256:
                recent.append({k: data.get(k) for k in ('id', 'action', 'status')})
        result['transactions'] = recent
        return result

    # ----------------------------------------------------------- entries

    def deploy(self, phase, expected, archive=None, map_sha256=None, terraform_plan_sha256=None):
        require(phase in PHASES, 'unknown phase %r' % (phase,))
        if phase == 'firewall':
            return self.firewall(expected, terraform_plan_sha256)
        require(terraform_plan_sha256 is None, '--terraform-plan-sha256 belongs to the firewall phase')
        self.check_plan(expected)
        with self.locked('deploy-' + phase) as transaction:
            self.require_preflight(phase)
            if phase == 'stage':
                self.stage(transaction, archive)
            elif phase == 'router-hook':
                self.router_hook(transaction)
            elif phase == 'workers':
                self.workers(transaction, map_sha256)
            elif phase == 'route':
                self.route(transaction)
            else:
                self.controller(transaction)

    def act(self, action, expected=None, smoke=False, start=None, through=None):
        """Every other mutating command, journaled under the lock.

        Forward actions need the reviewed plan and a passing preflight; the
        withdrawing ones (`*-stop`, `stop`) need neither, so they always run.
        """
        require(action in ACTIONS, 'unknown action %r' % (action,))
        forward = action in ('ingest-start', 'bootstrap', 'verify', 'measure-start')
        if forward:
            self.check_plan(expected)
        name = 'ingest-smoke' if action == 'ingest-start' and smoke else action
        with self.locked(name) as transaction:
            if forward:
                self.require_preflight(name)
            if action == 'ingest-start':
                self.ingest_start(transaction, smoke)
            elif action == 'ingest-stop':
                self.stop_units(transaction, 'coordinator', [INGEST_UNIT, SMOKE_UNIT])
            elif action == 'bootstrap':
                self.bootstrap(transaction, start, through)
            elif action == 'verify':
                self.verify(transaction)
            elif action == 'measure-start':
                self.measure_start(transaction)
            elif action == 'measure-stop':
                self.stop_units(transaction, 'coordinator', [OBSERVER_UNIT, MAPWATCH_UNIT])
            else:
                self.stop(transaction)


def iso_unix(text):
    try:
        return datetime.datetime.fromisoformat(text).timestamp()
    except (TypeError, ValueError):
        return 0


def load_release():
    spec = importlib.util.spec_from_file_location('txid_display_release', ROOT / 'tools/ci/release.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Runs on the coordinator; only this summary of a saved plan leaves the host.
SUMMARIZE = r'''
import json, subprocess, sys
wrapper, plan, firewall = sys.argv[1:4]
shown = json.loads(subprocess.run([wrapper, "show", "-json", plan], check=True, capture_output=True).stdout)
def rules(value):
    return {json.dumps({"protocol": r.get("protocol"), "port_range": r.get("port_range"),
                        "source_tags": sorted(r.get("source_tags") or []),
                        "source_addresses": len(r.get("source_addresses") or [])}, sort_keys=True)
            for r in (value or {}).get("inbound_rule") or []}
changes, added, removed = [], [], []
for change in shown.get("resource_changes", []):
    actions = change["change"]["actions"]
    if actions in (["no-op"], ["read"]):
        continue
    changes.append({"address": change["address"], "actions": actions})
    if change["address"] == firewall:
        before, after = rules(change["change"].get("before")), rules(change["change"].get("after"))
        added = [json.loads(r) for r in sorted(after - before)]
        removed = [json.loads(r) for r in sorted(before - after)]
print(json.dumps({"changes": changes, "added": added, "removed": removed}, sort_keys=True))
'''


def rollback_command(args, transaction):
    return shlex.join([sys.executable, str(ROOT / 'ops/scripts/wallet-pir-deploy.py'),
                       *(['--inventory', args.inventory] if args.inventory else []), '--state-dir', args.state_dir,
                       'txid-display-rollback', '--request', args.request, '--request-sha256', args.request_sha256,
                       '--transaction', transaction])


def main(args, inventory, out=print, executor=None, lock=None, **options):
    """Dispatch one `txid-display-*` command; returns the process exit code."""
    request = read_request(args.request, args.request_sha256)
    poc = Poc(inventory, request, args.request_sha256, Path(args.state_dir) / 'txid-display', executor=executor,
              lock=lock, out=out, **options)
    command = args.command.removeprefix('txid-display-')
    if command == 'plan':
        out(json.dumps(poc.plan(), sort_keys=True, indent=2))
        if getattr(args, 'render_dir', None):
            directory = Path(args.render_dir)
            directory.mkdir(parents=True, exist_ok=True)
            for name, text in poc.render().items():
                (directory / name.replace(':', '-')).write_text(text)
        out('plan sha256: ' + poc.plan_sha256())
        return 0
    if command == 'preflight':
        problems = poc.preflight()
        for problem in problems:
            out('problem: ' + problem)
        out('preflight ' + ('refused' if problems else 'passed'))
        return 1 if problems else 0
    if command == 'status':
        out(json.dumps(poc.status(), sort_keys=True, indent=2))
        return 0
    if command == 'ingest-status':
        out(json.dumps(poc.ingest_status(), sort_keys=True, indent=2))
        return 0
    try:
        if command == 'deploy':
            poc.deploy(args.phase, args.expect_plan_sha256, archive=args.archive,
                       map_sha256=args.publication_map_sha256, terraform_plan_sha256=args.terraform_plan_sha256)
        elif command == 'rollback':
            poc.rollback(args.transaction, args.terraform_plan_sha256)
        elif command == 'retire':
            poc.retire(args.expect_plan_sha256, args.terraform_plan_sha256)
        else:
            poc.act(command, getattr(args, 'expect_plan_sha256', None), smoke=getattr(args, 'smoke', False),
                    start=getattr(args, 'start_height', None), through=getattr(args, 'through_height', None))
    finally:
        if poc.transaction is not None:
            try:
                out(json.dumps(poc.status(), sort_keys=True, indent=2))
            except Exception as error:  # the action's own outcome matters more than its report
                out('status unavailable: %s' % error)
            if command != 'rollback':
                out('rollback: ' + rollback_command(args, poc.transaction.id))
    return 0
