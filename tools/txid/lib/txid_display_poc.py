"""The `wallet-pir-deploy.py txid-display-*` command family.

Runs one txid display site side by side with history on production: a
controller and a fresh display journal on the coordinator, under coordinator
paths and unit names of the site's own, and one worker with role `combined` on
the site's archive host, holding both the sealed archives and the recent shard.
Caddy on that host serves the site's own hostname, and every canonical
`/transparent/v1/txid/` request and every request on its legacy `/v2/txid/`
alias goes to the worker. History units and data are never changed.

A request must name its `site`. The `/v1/txid/` lineage served through the
history router, with its split archive/recent layout, router import hook and
worker firewall phase, was retired with the wallet API `/v1` on 2026-10-13.

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
import types

from wallet_pir_ops import durable, schema_fence
from wallet_pir_ops.deploy import units
from wallet_pir_ops.deploy.remote import SSHExecutor, production_lock

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = 'txid-display-poc-request-v1'
PLAN_SCHEMA = 'txid-display-poc-plan-v1'
JOURNAL_KIND = 'txid-display-poc-transaction-v1'
FLEET_SCHEMA = 'txid-display-fleet-v1'
KIND = 'transparent-txid-display'
BINARIES = ('pir-query-server', 'txid-inventory', 'pir-publisher', 'txid-quality-canary')
SCRIPTS = ('txid-display-fleet.py', 'txid-display-observe.py')
TEMPLATES = ('transparent-txid-display-worker.service.in', 'transparent-txid-display-controller.service.in',
             'txid-display-routes.caddy.in')
# Release files by basename, as tools/ci/release.py ships them.
RELEASE_FILES = {name: 'tools/txid/deploy/' + name for name in TEMPLATES}
RELEASE_FILES.update({name: 'tools/txid/scripts/' + name for name in SCRIPTS})
# What each host runs. Workers never receive the controller or the backfill.
HOST_RELEASE = {
    'coordinator': ('pir-publisher', 'txid-inventory', *SCRIPTS),
    'archive': ('pir-query-server',),
}
# One combined worker on the archive host, which also serves the site.
WORKERS = ('archive',)
ROLES = {'archive': 'combined'}
HOST_KEYS = ('coordinator', 'archive')
# A request may name a host that ran an earlier recent-replica worker; the
# workers phase stops and removes that worker's unit there.
FORMER_RECENT = 'former_recent'
PHASES = ('stage', 'workers', 'route', 'controller')
ACTIONS = ('ingest-start', 'ingest-stop', 'bootstrap', 'verify', 'measure-start', 'measure-stop', 'stop')
MODES = ('live', 'replay', 'replay-then-live')
GEOMETRIES = ('txid-2k8', 'txid-4k8')
# DisplaySealParams plus the map window, as txid-display-controller's seal flags.
SEAL_KEYS = ('n_archive', 'n_recent', 'archive_target', 'recent_window', 'reorg_margin', 'max_archive_shards')
MAX_BUCKETS = 64
# Sealed archives a map lists: genesis coverage is about 189 archives at
# 90,000 entries, growing about 30 a year.
MAX_ARCHIVE_SHARDS = 1024

# Worker-host paths; coordinator paths are each site's own (names()).
OPT = '/opt/transparent-txid-display'
WORKER_DATA = '/srv/transparent-txid-display'
PUBLICATIONS = WORKER_DATA + '/publications'
STAGED = WORKER_DATA + '/staged'
RUNTIME_CACHE = WORKER_DATA + '/runtime-cache'
WORKER_STATE = OPT + '/worker'
ACTIVE_RECORD = WORKER_STATE + '/active.json'
CONTROL_SOCKET = '/run/transparent-txid-display/control.sock'
SYSTEM = '/etc/systemd/system'
WORKER_UNIT = 'transparent-txid-display-worker.service'
# The worker runs the release's pir-query-server over one generated, root-owned
# `transaction_details` configuration, read-only to the unit, with its own
# loopback management listener. The candidate check runs `--verify-only` over a
# separate file holding only the settings that check always used.
WORKER_CONFIG = '/etc/transparent-pir/txid-display-worker.json'
WORKER_CHECK_CONFIG = '/etc/transparent-pir/txid-display-worker.check.json'
WORKER_MANAGEMENT = '127.0.0.1:18195'
CADDYFILE = '/etc/caddy/Caddyfile'
PUBLISHER = '/opt/transparent-publisher'
HISTORY_DAEMONS = ('transparent-replica-reconciler.service', 'transparent-control-sessions.service')
# The existing read-only history deploy credentials, reused by the adapter.
DEPLOY_KEY = PUBLISHER + '/credentials/deploy-ssh'
KNOWN_HOSTS = PUBLISHER + '/credentials/known_hosts'
ACTUATOR_OPERATION = PUBLISHER + '/scaler/journal/operation.json'
HISTORY_STATUS = 'http://127.0.0.1:8094/v1/status'
NODE_STATE = 'state/v29/mainnet'
# The public txid prefix: a site serves its release's own /v2 paths.
SITE_API = '/v2/txid'
# Every prefix a site forwards to its worker: the canonical Transparent route
# first, then the legacy alias existing clients use. The worker serves both.
SITE_PREFIXES = ('/transparent/v1/txid', SITE_API)
WITHDRAWN = ('# Written by `wallet-pir-deploy.py txid-display-stop`: display withdrawn.\n'
             '\thandle %s/* {\n\t\theader Retry-After 1\n'
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
# A new site's first route also waits for Caddy's ACME certificate.
SITE_SECONDS = 300
# Under txid-control's own 600 s wait for a control reply.
PREPARE_CALL_SECONDS = 540
MEASURE_LIMITS = {'MemoryMax': '256M', 'CPUQuota': '25%', 'CPUWeight': '20', 'IOWeight': '20', 'Nice': '10'}
# Ports history, Enhance, the node and the prototype already use.
RESERVED_PORTS = (8080, 8081, 8083, 8090, 8092, 8093, 8094, 8192, 8232)
# tools/infra/digitalocean/transparent/main.tf opens exactly this port.
WORKER_PORT = 8095
HISTORY_ROOTS = ('/srv/transparent-pir', '/srv/transparent-activity', '/opt/transparent-publisher',
                 '/run/transparent-pir', '/opt/enhance-pir')
# A site is one display lineage: its own public hostname, served by Caddy on
# its combined worker's host, and its own coordinator paths and unit names, so
# no two sites touch each other's.
SITE_NAME = re.compile('[a-z0-9]{1,12}')
HOSTNAME = re.compile(r'(?=.{1,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}')

HEX40 = re.compile('[0-9a-f]{40}')
HEX64 = re.compile('[0-9a-f]{64}')
PLAIN_PATH = re.compile(r'/[A-Za-z0-9._/-]+')
MEMORY = re.compile(r'([0-9]+(?:\.[0-9]+)?)([KMGT])')
QUOTA = re.compile(r'[1-9][0-9]{0,3}%')
PUBLIC_URL = re.compile(r'https://[A-Za-z0-9.-]+')
RPC_URL = re.compile(r'http://127\.0\.0\.1:[0-9]{2,5}')
TRANSACTION = re.compile(r'txid-display-[a-z-]+-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{6}')


class PocError(ValueError):
    pass


def names(site):
    """One site's coordinator paths and unit names.

    Worker-host paths stay the module's: a site's worker runs on a host of its own.
    """
    suffix = '-' + site['name']
    opt = OPT + suffix
    state = opt + '/state'

    def unit(role):
        return 'transparent-txid-display%s-%s.service' % (suffix, role)
    found = types.SimpleNamespace(
        OPT=opt, RELEASES=opt + '/releases', TRANSACTIONS=opt + '/transactions', STATE=state,
        REQUESTS=state + '/requests', VALIDATE=opt + '/validate',
        WORKERS_JSON=opt + '/workers.json', FLEET_JSON=opt + '/fleet.json', CONTROLLER_UNIT=unit('controller'),
        INGEST_UNIT=unit('ingest'), SMOKE_UNIT=unit('ingest-smoke'), BOOTSTRAP_UNIT=unit('bootstrap'),
        OBSERVER_UNIT=unit('observer'), MAPWATCH_UNIT=unit('mapwatch'), PLAN_START_UNIT=unit('plan-start'),
        VERIFY_UNIT=unit('verify'))
    found.TRANSIENT = (found.INGEST_UNIT, found.SMOKE_UNIT, found.BOOTSTRAP_UNIT, found.OBSERVER_UNIT,
                       found.MAPWATCH_UNIT)
    found.JOURNAL_WRITERS = (found.INGEST_UNIT, found.SMOKE_UNIT, found.BOOTSTRAP_UNIT)
    return found


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
    require(isinstance(request, dict) and 'site' in request,
            'request must name a site: the /v1 history router lineage (router, terraform) is retired')
    exact(request, ('schema', 'source_sha', 'release', 'hosts', 'ports', 'units', 'journal', 'publication',
                    'controller', 'site', 'history', 'measure', 'baseline'), 'request')
    require(not any(isinstance(request[field], dict) and 'recent' in request[field]
                    for field in ('hosts', 'units', 'baseline')),
            'a site request runs the combined layout: hosts, units and baseline name no recent worker')
    workers, host_keys = WORKERS, HOST_KEYS
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

    former = isinstance(request['hosts'], dict) and FORMER_RECENT in request['hosts']
    hosts = exact(request['hosts'], host_keys + ((FORMER_RECENT,) if former else ()), 'hosts')
    if former:
        exact(hosts[FORMER_RECENT], ('inventory',), 'hosts.' + FORMER_RECENT)
        require(hosts[FORMER_RECENT]['inventory'] in inventory.hosts, 'hosts.%s names no inventory host' % FORMER_RECENT)
    names = [hosts[FORMER_RECENT]['inventory']] if former else []
    for key in host_keys:
        exact(hosts[key], ('inventory', 'vpc_ip') if key in WORKERS else ('inventory',), 'hosts.' + key)
        require(hosts[key]['inventory'] in inventory.hosts, 'hosts.%s names no inventory host' % key)
        names.append(hosts[key]['inventory'])
        if key in WORKERS:
            try:
                address = ipaddress.IPv4Address(hosts[key]['vpc_ip'])
            except (ipaddress.AddressValueError, TypeError):
                raise PocError('hosts.%s.vpc_ip must be an IPv4 address' % key) from None
            require(address.is_private and not address.is_loopback, 'hosts.%s.vpc_ip must be a VPC address' % key)
    keys = host_keys + ((FORMER_RECENT,) if former else ())
    require(len(set(names)) == len(names), '%s and %s must be distinct hosts' % (', '.join(keys[:-1]), keys[-1]))
    if inventory.lock.get('type') == 'remote':
        require(inventory.lock['host'] == hosts['coordinator']['inventory'],
                'the production lock host must be the coordinator')

    ports = exact(request['ports'], ('worker', 'controller_status'), 'ports')
    for key, value in ports.items():
        integer(value, 1024, 65535, 'ports.' + key)
        require(value not in RESERVED_PORTS, 'ports.%s collides with an existing service' % key)
    require(ports['worker'] == WORKER_PORT, 'ports.worker must be %d, the port the firewall rule opens' % WORKER_PORT)
    require(ports['worker'] != ports['controller_status'], 'ports must differ')

    limits = exact(request['units'], (*workers, 'controller', 'ingest', 'bootstrap'), 'units')
    for key in workers:
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
    # A genesis journal starts at 0; the display then starts at 1.
    integer(journal['start_height'], 0, 1 << 40, 'journal.start_height')
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
    integer(controller['recent_window'], 10000, 1 << 32, 'controller.recent_window')
    # Sealed shards never change, so they must stay below any reorg the node accepts.
    integer(controller['reorg_margin'], 100, 10000, 'controller.reorg_margin')
    integer(controller['max_archive_shards'], 1, MAX_ARCHIVE_SHARDS, 'controller.max_archive_shards')
    # plan-start's targets: archives bootstrap makes, and seals replay adds after them.
    integer(controller['archives'], 1, MAX_ARCHIVE_SHARDS, 'controller.archives')
    integer(controller['replay_seals'], 0, 64, 'controller.replay_seals')
    require(isinstance(controller['rpc_url'], str) and RPC_URL.fullmatch(controller['rpc_url']),
            'controller.rpc_url must be the loopback node RPC')
    plain_path(controller['rpc_cookie'], 'controller.rpc_cookie')

    exact(request['site'], ('name', 'hostname'), 'site')
    require(isinstance(request['site']['name'], str) and SITE_NAME.fullmatch(request['site']['name']),
            'site.name must be 1 to 12 lowercase letters or digits')
    require(isinstance(request['site']['hostname'], str) and HOSTNAME.fullmatch(request['site']['hostname']),
            'site.hostname must be a lowercase DNS name')
    # Schema cutovers move history's adapter, fleet and state, so the request names them.
    history = exact(request['history'], ('load_status', 'adapter', 'fleet', 'membership'), 'history')
    for key, value in history.items():
        plain_path(value, 'history.' + key, history=True)
    require(posixpath.basename(history['adapter']) == 'transparent-live-fleet.py',
            'history.adapter must be the deployed transparent-live-fleet.py')
    measure = exact(request['measure'], ('output', 'public_url'), 'measure')
    plain_path(measure['output'], 'measure.output')
    require(isinstance(measure['public_url'], str) and PUBLIC_URL.fullmatch(measure['public_url']),
            'measure.public_url must be https://<host>')
    require(measure['public_url'] == 'https://' + request['site']['hostname'],
            'measure.public_url must be the site\'s own https://<site.hostname>')
    data = [journal['data_dir'], journal['data_dir'] + '.smoke', publication['root'], measure['output']]
    require(not any(inside(a, b) for i, a in enumerate(data) for j, b in enumerate(data) if i != j),
            'journal, publication and measure directories must not nest')
    baseline = exact(request['baseline'], ('coordinator', *workers), 'baseline')
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
        directory = '%s/%s/files' % (self.poc.n.TRANSACTIONS, self.id)
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
        self.worker_keys, self.roles = WORKERS, ROLES
        self.host_keys = HOST_KEYS
        # The site serves its own hostname from Caddy on its worker host.
        self.site = self.request['site']
        self.n = names(self.site)
        self.route_host = 'archive'
        self.former_recent = FORMER_RECENT in self.request['hosts']
        self.host_release = HOST_RELEASE
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
        return '%s/%s' % (self.n.RELEASES, self.request['source_sha'])

    @property
    def publisher_config_path(self):
        return str(Path(self.n.WORKERS_JSON).with_name('publisher.json'))

    def binary(self, name):
        return '%s/%s' % (self.release_dir, name)

    @property
    def live_fleet(self):
        return self.request['history']['adapter']

    @property
    def history_fleet(self):
        return self.request['history']['fleet']

    @property
    def api(self):
        return SITE_API

    @property
    def routes_path(self):
        return CADDYFILE

    def site_file(self, body):
        """The whole Caddyfile of a site's worker host around one display snippet."""
        return ('# Rendered by `wallet-pir-deploy.py txid-display-deploy --phase route`: the %s txid display\n'
                '# site. Anything outside %s is not served here.\n%s {\n%s\thandle {\n\t\trespond 404\n\t}\n}\n'
                % (self.site['name'], ' and '.join(p + '/' for p in SITE_PREFIXES), self.site['hostname'], body))

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
        for key in self.worker_keys:
            spec = r['units'][key]
            files['unit:' + key] = units.render(self.template('transparent-txid-display-worker.service.in'), {
                'RELEASE': self.release_dir, 'ROLE': self.roles[key],
                'BUILD_THREADS': str(spec['build_threads']), 'MEMORY_MAX': spec['memory_max'],
                'CPU_QUOTA': spec['cpu_quota'], 'CPU_WEIGHT': str(spec['cpu_weight']), 'NICE': str(spec['nice']),
                'OOM_SCORE_ADJUST': str(spec['oom_score_adjust'])})
            files['config:' + key] = self.worker_config(key)
        controller, limits = r['controller'], r['units']['controller']
        # `run` reads the seal rule from the bootstrapped root; it takes no seal flags.
        extra = ''
        if controller['mode'] != 'live':
            extra = ' --blocks-per-step %d --step-interval-ms %d' % (controller['blocks_per_step'],
                                                                     controller['step_interval_ms'])
        files['unit:controller'] = units.render(self.template('transparent-txid-display-controller.service.in'), {
            'RELEASE': self.release_dir, 'ROOT': r['publication']['root'], 'JOURNAL': r['journal']['data_dir'],
            'MODE': controller['mode'], 'STATUS_PORT': str(r['ports']['controller_status']),
            'WORKERS_JSON': self.n.WORKERS_JSON, 'STATE': self.n.STATE,
            'RPC_URL': controller['rpc_url'], 'RPC_COOKIE': controller['rpc_cookie'], 'EXTRA_ARGS': extra,
            'PUBLISHER_CONFIG': self.publisher_config_path,
            'MEMORY_MAX': limits['memory_max'], 'CPU_QUOTA': limits['cpu_quota'],
            'CPU_WEIGHT': str(limits['cpu_weight']), 'NICE': str(limits['nice'])})
        settings = {
            'root': r['publication']['root'], 'journal': r['journal']['data_dir'],
            'workers': self.n.WORKERS_JSON, 'mode': controller['mode'],
            'rpc_url': controller['rpc_url'], 'rpc_cookie': controller['rpc_cookie'],
            'poll_ms': controller.get('poll_ms', 500),
            'status_listen': '127.0.0.1:%s' % r['ports']['controller_status'],
            'blocks_per_step': controller.get('blocks_per_step', 25),
            'step_interval_ms': controller.get('step_interval_ms', 2000),
            'max_recent_records': controller.get('max_recent_records'),
        }
        files['publisher.json'] = json.dumps({'management_listen': '127.0.0.1:18198',
            'transaction_details': {'enabled': True, 'settings': settings}}, indent=2) + '\n'
        for name in ['unit:' + key for key in self.worker_keys] + ['unit:controller']:
            starts = [value for _, key, value in units.entries(files[name]) if key == 'ExecStart']
            require(len(starts) == 1, '%s must have exactly one ExecStart' % name)
        transport = {'command': self.binary('txid-display-fleet.py'), 'config': self.n.FLEET_JSON}
        # One combined worker serves the archives and the map.
        files['workers.json'] = json.dumps({'workers': [
            {'name': key, 'role': self.roles[key], 'transport': transport} for key in self.worker_keys]},
            indent=2) + '\n'
        files['fleet.json'] = json.dumps({
            'schema': FLEET_SCHEMA, 'ssh_key': DEPLOY_KEY, 'known_hosts': KNOWN_HOSTS,
            'control_dir': self.n.STATE + '/ssh',
            'workers': {key: {'ssh_host': r['hosts'][key]['vpc_ip'], 'user': 'root', 'publications': PUBLICATIONS,
                              'staged': STAGED, 'control_socket': CONTROL_SOCKET,
                              'control_binary': self.binary('txid-control')} for key in self.worker_keys},
        }, indent=2, sort_keys=True) + '\n'
        routes = self.template('txid-display-routes.caddy.in')
        files['routes.caddy'] = self.site_file(''.join(units.render(routes, {
            'ARCHIVE_UPSTREAM': self.upstream('archive'), 'RECENT_UPSTREAM': self.upstream('archive'),
            'TXID': prefix}) for prefix in SITE_PREFIXES))
        files['withdrawn.caddy'] = self.site_file(''.join(WITHDRAWN % prefix for prefix in SITE_PREFIXES))
        return files

    def targets(self):
        """`name -> (host key, path, mode)` of every rendered file."""
        return {
            **{'unit:' + key: (key, '%s/%s' % (SYSTEM, WORKER_UNIT), 0o644) for key in self.worker_keys},
            **{'config:' + key: (key, WORKER_CONFIG, 0o644) for key in self.worker_keys},
            'unit:controller': ('coordinator', '%s/%s' % (SYSTEM, self.n.CONTROLLER_UNIT), 0o644),
            'workers.json': ('coordinator', self.n.WORKERS_JSON, 0o600),
            'publisher.json': ('coordinator', self.publisher_config_path, 0o600),
            'fleet.json': ('coordinator', self.n.FLEET_JSON, 0o600),
            'routes.caddy': (self.route_host, self.routes_path, 0o644),
            'withdrawn.caddy': (self.route_host, self.routes_path, 0o644),
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
        ingest = [self.binary('pir-publisher'), 'ingest-events', '--state-dir', journal['node_cache_dir'],
                  '--workers', str(r['units']['ingest']['workers']), '--txid-display',
                  '--start-height', str(journal['start_height'])]
        controller = self.binary('pir-publisher')
        seal = ['--geometry', r['controller']['geometry']]
        for key in SEAL_KEYS:
            seal += ['--' + key.replace('_', '-'), str(r['controller'][key])]
        measure = r['measure']['output'] + '/{run}'
        observe = ['/usr/bin/python3', self.binary('txid-display-observe.py')]
        return {
            'ingest': self.systemd_run(self.n.INGEST_UNIT, self.unit_limits('ingest'),
                                       ingest + ['--data-dir', journal['data_dir']]),
            # Ten blocks into a separate directory, so the real journal starts clean.
            'ingest-smoke': self.systemd_run(self.n.SMOKE_UNIT, self.unit_limits('ingest'), ingest + [
                '--stop-height', str(journal['start_height'] + 9), '--data-dir', journal['data_dir'] + '.smoke'],
                wait=True),
            'plan-start': self.systemd_run(self.n.PLAN_START_UNIT,
                                           self.unit_limits('bootstrap'), [
                controller, 'detail', 'plan-start', '--journal', journal['data_dir'],
                '--archives', str(r['controller']['archives']),
                '--replay-seals', str(r['controller']['replay_seals']), *seal], wait=True),
            'bootstrap': self.systemd_run(self.n.BOOTSTRAP_UNIT, self.unit_limits('bootstrap'), [
                controller, 'detail', 'bootstrap', '--root', root, '--journal', journal['data_dir'],
                '--start', '{start}', '--through', '{through}', *seal]),
            'verify': self.systemd_run(self.n.VERIFY_UNIT, self.unit_limits('controller'),
                                       [controller, 'detail', 'verify', '--root', root, '--journal', journal['data_dir']],
                                       wait=True),
            'observer': self.systemd_run(self.n.OBSERVER_UNIT, MEASURE_LIMITS, observe + [
                'observer', '--rpc-url', r['controller']['rpc_url'], '--cookie', r['controller']['rpc_cookie'],
                '--interval-ms', '250', '--out', measure + '/observer.jsonl']),
            'mapwatch': self.systemd_run(self.n.MAPWATCH_UNIT, MEASURE_LIMITS, observe + [
                'mapwatch', '--url', r['measure']['public_url'] + self.api + '/shards', '--interval-ms', '1000',
                '--out-dir', measure + '/mapwatch']),
        }

    @staticmethod
    def detail_config(settings):
        """A pir-query-server configuration enabling only `transaction_details`; never `verify_only`."""
        return json.dumps({'management_listen': WORKER_MANAGEMENT,
                           'transaction_details': {'enabled': True, 'settings': settings}}, indent=2) + '\n'

    def worker_config(self, key):
        """The serving worker's settings: the retired transparent-txid-server's flags, in their order."""
        spec = self.request['units'][key]
        settings = {'listen': self.upstream(key), 'role': self.roles[key], 'cache_bytes': spec['cache_bytes'],
                    'build_slots': spec['build_slots'], 'query_slots': spec['query_slots'],
                    'retain_revisions': spec['retain_revisions'], 'control_socket': CONTROL_SOCKET,
                    'active_record': ACTIVE_RECORD, 'collect_root': [PUBLICATIONS, STAGED]}
        if spec['disk_cache_bytes']:
            settings.update(runtime_cache_dir=RUNTIME_CACHE, runtime_cache_max_bytes=spec['disk_cache_bytes'])
        return self.detail_config(settings)

    def verify_only_config(self, key, directory):
        """The candidate check's settings: role, publication and cache budget only, as before."""
        spec = self.request['units'][key]
        return self.detail_config({'role': self.roles[key], 'publication_dir': directory,
                                   'cache_bytes': spec['cache_bytes']})

    def verify_only(self, key, directory):
        """The check's argv; it reads `verify_only_config`, written to WORKER_CHECK_CONFIG first."""
        spec = self.request['units'][key]
        return self.systemd_run('transparent-txid-display-verify-only.service', self.unit_limits(key), [
            self.binary('pir-query-server'), '--config', WORKER_CHECK_CONFIG, '--verify-only'],
            wait=True, environment=('TRANSPARENT_BUILD_THREADS=%d' % spec['build_threads'],))

    def plan(self):
        if self._plan is None:
            files = self.render()
            targets = self.targets()
            self._plan = {
                'schema': PLAN_SCHEMA, 'request_sha256': self.request_sha256,
                'source_sha': self.request['source_sha'], 'release_dir': self.release_dir,
                'hosts': {key: self.host(key) for key in self.host_keys},
                'retires': {FORMER_RECENT: self.host(FORMER_RECENT)} if self.former_recent else {},
                'phases': list(PHASES),
                'files': {name: {'host': targets[name][0], 'path': targets[name][1],
                                 'mode': '%04o' % targets[name][2], 'sha256': sha256(text)}
                          for name, text in sorted(files.items())},
                'release': {key: list(names) for key, names in self.host_release.items()},
                'templates': {name: file_sha256(self.root / path) for name, path in sorted(RELEASE_FILES.items())},
                'transient': self.transient(),
                'site': {**self.site, 'host': self.host(self.route_host), 'units': sorted(
                    {self.n.CONTROLLER_UNIT, *self.n.TRANSIENT}), 'opt': self.n.OPT},
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

    def exec_start(self, key, unit):
        _, output = self.run(key, ['systemctl', 'show', unit, '--no-pager', '-p', 'ExecStart', '--value'], 30,
                             check=False)
        return output

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
        for unit in HISTORY_DAEMONS:
            command = self.exec_start('coordinator', unit)
            if self.live_fleet not in command or self.history_fleet not in command:
                problems.append('%s does not run %s with %s' % (unit, self.live_fleet, self.history_fleet))
        membership = self.read_json('coordinator', self.request['history']['membership'])
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
                    for key in self.worker_keys}
        starts = {'controller': (self.n.CONTROLLER_UNIT, 'controller'), 'ingest-start': (self.n.INGEST_UNIT, 'ingest'),
                  'ingest-smoke': (self.n.SMOKE_UNIT, 'ingest'), 'bootstrap': (self.n.BOOTSTRAP_UNIT, 'bootstrap'),
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
        paths = {'stage': [(key, self.n.OPT) for key in self.host_release],
                 'workers': [(key, WORKER_DATA) for key in self.worker_keys],
                 'route': [(self.route_host, '/etc/caddy')],
                 'measure-start': [('coordinator', r['measure']['output'])]}
        default = [('coordinator', p) for p in (r['journal']['data_dir'], r['publication']['root'], self.n.OPT)]
        problems += self.disk_problems(paths.get(action, default))
        if action == 'workers':
            for key in self.worker_keys:
                problems += self.port_problems(key, r['ports']['worker'], WORKER_UNIT)
        if action == 'controller':
            problems += self.port_problems('coordinator', r['ports']['controller_status'], self.n.CONTROLLER_UNIT)
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
            for key, names in self.host_release.items():
                self.stage_host(transaction, key, bundle, names)

    def stage_host(self, transaction, key, bundle, names):
        host = self.host(key)
        digests = {**self.request['release']['binaries'], **self.request['release']['files']}
        present = {name: self.ex.sha256(host, self.binary(name)) for name in names}
        require(all(present[name] in (None, digests[name]) for name in names),
                '%s: immutable release %s holds other bytes' % (key, self.release_dir))
        if all(digest is None for digest in present.values()):
            self.guard()
            partial = '%s/.partial-%s' % (self.n.RELEASES, transaction.id)
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
        if name.endswith('.py'):
            return ['/usr/bin/python3', path, '--help'], 0, ''
        return [path, '--help'], 0, ''

    # ----------------------------------------------------------- route

    def caddy_validate(self, transaction, files):
        """`caddy validate` a scratch composition on the route host, then remove the scratch files."""
        host = self.host(self.route_host)
        scratch = '%s/%s' % (self.n.VALIDATE, transaction.id)
        self.guard()
        self.ex.mkdir(host, scratch, 0o700)
        try:
            for name, text in files.items():
                self.ex.write(host, '%s/%s' % (scratch, name), text.encode(), 0o600)
            code, output = self.run(self.route_host, ['caddy', 'validate', '--config', scratch + '/Caddyfile',
                                                      '--adapter', 'caddyfile'], 120, check=False)
        finally:
            for name in files:
                self.ex.remove(host, '%s/%s' % (scratch, name))
        require(code == 0, 'caddy rejected the composed site: %s' % output[-1500:])

    # ----------------------------------------------------------- workers

    def fleet(self, transaction, request, timeout):
        """One call of the production adapter on the coordinator; request and reply travel as files."""
        coordinator = self.host('coordinator')
        self.guard()
        transaction.data['fleet_calls'] = transaction.data.get('fleet_calls', 0) + 1
        transaction.save()
        name = '%s-%d' % (transaction.id, transaction.data['fleet_calls'])
        request_path, reply_path = '%s/%s.json' % (self.n.REQUESTS, name), '%s/%s.reply.json' % (self.n.REQUESTS, name)
        self.ex.mkdir(coordinator, self.n.REQUESTS, 0o700)
        self.ex.write(coordinator, request_path, json.dumps(request).encode(), 0o600)
        try:
            code, output = self.ex.run(coordinator, ['/usr/bin/python3', self.binary('txid-display-fleet.py'),
                                                     self.n.FLEET_JSON, request_path, reply_path], timeout + 30)
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

    def workers(self, transaction, map_sha256, replace_active=None):
        require(isinstance(map_sha256, str) and HEX64.fullmatch(map_sha256),
                '--publication-map-sha256 (the bootstrap candidate map) is required for workers')
        require(replace_active is None or (HEX64.fullmatch(replace_active) and replace_active != map_sha256),
                '--replace-active names the map a fresh lineage replaces, not the new one')
        # Once running, the controller owns every worker's map and `expected`;
        # a restart or prepare here would race it or regress the served map.
        require(not self.running('coordinator', self.n.CONTROLLER_UNIT),
                'the controller drives the workers; stop it first (txid-display-stop)')
        source = self.candidate(map_sha256)
        files = self.render()
        self.guard()
        self.ex.mkdir(self.host('coordinator'), self.n.STATE + '/ssh', 0o700)
        transaction.change('coordinator', self.n.FLEET_JSON, files['fleet.json'], 0o600)
        for key in self.worker_keys:
            self.worker(transaction, key, files['unit:' + key], source, map_sha256, replace_active,
                        files['config:' + key])
        if self.former_recent:
            self.retire_former_recent(transaction)

    def retire_former_recent(self, transaction):
        """Stop, disable and remove an earlier recent-replica unit; its data stays.

        Runs after the combined worker serves warm. A rollback puts the unit
        file back and starts it again if it was running.
        """
        key, path = FORMER_RECENT, '%s/%s' % (SYSTEM, WORKER_UNIT)
        self.stop_units(transaction, key, [WORKER_UNIT])
        if self.ex.read(self.host(key), path) is None:
            transaction.event('%s: no worker unit to remove' % key)
            return
        transaction.undo('post', key, 'systemctl', args=['daemon-reload'])
        self.guard()
        # Disable while the unit file still names its install links.
        self.run(key, ['systemctl', 'disable', WORKER_UNIT], 120, check=False)
        transaction.remove(key, path)
        self.ex.systemctl(self.host(key), 'daemon-reload')
        transaction.event('%s: removed the recent-replica unit; %s and %s stay' % (key, WORKER_DATA, self.n.OPT))

    def worker(self, transaction, key, unit_text, source, map_sha256, replace_active=None, config_text=None):
        """Install and start one worker; only a worker with no active map takes `map_sha256`.

        A worker that already serves a map (from an earlier deploy or the
        controller) restarts onto its own active record and keeps it, since the
        bootstrap candidate may be older; the controller advances it later.
        The unit restarts only when its bytes (and so the release) or its
        configuration's bytes changed; both are journaled.

        `replace_active` is the map of a lineage this deploy replaces, as a
        re-layout's fresh lineage does: a worker serving it is stopped and its
        active record removed (journaled, so a rollback puts it back), and it
        then takes `map_sha256` as an empty worker would.
        """
        host = self.host(key)
        binary = self.request['release']['binaries']['pir-query-server']
        config_text = self.worker_config(key) if config_text is None else config_text
        timeout = self.request['units'][key]['ready_timeout_seconds']
        self.guard()
        for path in (PUBLICATIONS, STAGED, RUNTIME_CACHE, WORKER_STATE):
            self.ex.mkdir(host, path, 0o700)
        record = self.read_json(key, ACTIVE_RECORD)
        serving = record.get('map_sha256') if isinstance(record, dict) else None
        if replace_active is not None and serving is not None:
            require(serving == replace_active,
                    '%s serves %s, not the replaced %s' % (key, serving, replace_active))
            self.stop_units(transaction, key, [WORKER_UNIT])
            transaction.remove(key, ACTIVE_RECORD)
            # The worker writes its own record again on activation; a rollback
            # stops it first and puts the replaced record back over that.
            transaction.data['changes'][-1]['worker_rewrites'] = True
            transaction.save()
            transaction.event('%s: removed the active record of %s' % (key, replace_active))
            serving = None
        directory = '%s/%s' % (PUBLICATIONS, map_sha256)
        if serving is None:
            shipped = self.fleet(transaction, {'operation': 'ship', 'worker': key, 'kind': 'candidate',
                                               'source': source, 'name': map_sha256, 'link_dest': None,
                                               'deadline_seconds': SHIP_SECONDS}, SHIP_SECONDS)
            require(shipped.get('directory') == directory, 'adapter shipped to %s' % shipped.get('directory'))
            transaction.event('%s: shipped candidate %s' % (key, map_sha256), seconds=shipped.get('seconds'))
            self.guard()
            self.ex.mkdir(host, posixpath.dirname(WORKER_CHECK_CONFIG), 0o755)
            self.ex.write(host, WORKER_CHECK_CONFIG, self.verify_only_config(key, directory).encode(), 0o644)
            try:
                self.run(key, self.verify_only(key, directory), timeout)
            finally:
                self.ex.remove(host, WORKER_CHECK_CONFIG)
            transaction.event('%s: --verify-only accepted the candidate' % key)
        unit = '%s/%s' % (SYSTEM, WORKER_UNIT)
        running = self.running(key, WORKER_UNIT)
        if running and self.ex.read(host, unit) == unit_text and self.ex.read(host, WORKER_CONFIG) == config_text:
            transaction.event('%s: unit unchanged; the worker keeps running' % key)
        else:
            if running:
                transaction.undo('post', key, 'start-unit', unit=WORKER_UNIT)
            transaction.undo('pre', key, 'stop-unit', unit=WORKER_UNIT)
            transaction.undo('post', key, 'systemctl', args=['daemon-reload'])
            transaction.change(key, WORKER_CONFIG, config_text, 0o644)
            transaction.change(key, unit, unit_text, 0o644)
            self.guard()
            self.ex.systemctl(host, 'daemon-reload')
            self.ex.systemctl(host, 'enable', WORKER_UNIT)
            self.ex.systemctl(host, 'restart', WORKER_UNIT)
        status = self.wait('%s control socket' % key, lambda: self.control(transaction, key, {'operation': 'status'}),
                           300)
        require(status.get('role') == self.roles[key], '%s reports role %s' % (key, status.get('role')))
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
        return ready.get('role') == self.roles[key] and ready.get('map_sha256') == map_sha256 and ready.get('warm') is True

    def reload_caddy(self):
        self.guard()
        code, output = self.run(self.route_host, ['caddy', 'validate', '--config', CADDYFILE, '--adapter',
                                                  'caddyfile'], 120, check=False)
        require(code == 0, 'caddy rejected the live site: %s' % output[-1500:])
        self.ex.systemctl(self.host(self.route_host), 'reload', 'caddy')

    def write_route(self, transaction, snippet):
        """Replace the site Caddyfile and reload; never leave bytes Caddy refused."""
        transaction.undo('post', self.route_host, 'reload-caddy')
        if not transaction.change(self.route_host, self.routes_path, snippet, 0o644):
            return
        try:
            self.reload_caddy()
        except BaseException:
            self.restore(transaction)
            raise

    def route(self, transaction):
        """Replace the site host's whole Caddyfile, then wait for the public map over TLS.

        The first route on a new host waits for Caddy's certificate as well.
        """
        site = self.render()['routes.caddy']
        self.caddy_validate(transaction, {'Caddyfile': site})
        self.write_route(transaction, site)
        url = self.request['measure']['public_url'] + self.api + '/map'

        def served():
            status, body = self.ex.http_get(self.host('coordinator'), url, 10)
            return status == 200 and isinstance(json.loads(body).get('chunks'), list)
        self.wait('the display map at %s' % url, served, SITE_SECONDS)
        transaction.event('%s serves %s/' % (self.site['hostname'], self.api))

    # -------------------------------------------------------- controller

    def controller(self, transaction):
        for unit in self.n.JOURNAL_WRITERS:
            require(self.unit_state('coordinator', unit)['sub'] not in ('running', 'start'),
                    '%s still runs; the controller must be the only journal writer' % unit)
        for key in self.worker_keys:
            require(self.running(key, WORKER_UNIT), '%s worker is not running; deploy workers first' % key)
        files = self.render()
        coordinator = self.host('coordinator')
        self.guard()
        self.ex.mkdir(coordinator, self.n.STATE + '/ssh', 0o700)
        transaction.change('coordinator', self.n.FLEET_JSON, files['fleet.json'], 0o600)
        transaction.change('coordinator', self.n.WORKERS_JSON, files['workers.json'], 0o600)
        transaction.change('coordinator', self.publisher_config_path, files['publisher.json'], 0o600)
        if self.running('coordinator', self.n.CONTROLLER_UNIT):
            transaction.undo('post', 'coordinator', 'start-unit', unit=self.n.CONTROLLER_UNIT)
        transaction.undo('pre', 'coordinator', 'stop-unit', unit=self.n.CONTROLLER_UNIT)
        transaction.undo('post', 'coordinator', 'systemctl', args=['daemon-reload'])
        transaction.change('coordinator', '%s/%s' % (SYSTEM, self.n.CONTROLLER_UNIT), files['unit:controller'], 0o644)
        self.guard()
        self.ex.systemctl(coordinator, 'daemon-reload')
        self.ex.systemctl(coordinator, 'enable', self.n.CONTROLLER_UNIT)
        self.ex.systemctl(coordinator, 'restart', self.n.CONTROLLER_UNIT)
        port = self.request['ports']['controller_status']
        self.wait('the controller status listener', lambda: '127.0.0.1:%d' % port in self.listening('coordinator', port),
                  120)
        # A crash loop shows as restarts within the first seconds.
        self.sleep(10)
        state = self.unit_state('coordinator', self.n.CONTROLLER_UNIT)
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
        require(not self.running('coordinator', self.n.CONTROLLER_UNIT), 'the controller owns the journal; stop it first')
        require(not self.running('coordinator', self.n.INGEST_UNIT) or not smoke, 'the backfill is running')
        self.guard()
        self.ex.mkdir(self.host('coordinator'), posixpath.dirname(r['journal']['data_dir']), 0o755)
        transient = self.plan()['transient']
        if not smoke:
            self.start_transient(transaction, transient['ingest'], self.n.INGEST_UNIT)
            return
        self.guard()
        code, output = self.run('coordinator', transient['ingest-smoke'], 1800, check=False)
        transaction.event('smoke ingest exited %d' % code, output=output[-2000:])
        require(code == 0, 'smoke ingest failed (exit %d): %s' % (code, output[-1500:]))

    def ingest_status(self):
        result = {}
        for unit in (self.n.INGEST_UNIT, self.n.SMOKE_UNIT):
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
        require(self.unit_state('coordinator', self.n.INGEST_UNIT)['sub'] not in ('running', 'start'),
                'the backfill still runs; bootstrap reads the finished journal')
        require(not self.running('coordinator', self.n.CONTROLLER_UNIT), 'the controller runs; bootstrap starts it once')
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
        self.start_transient(transaction, argv, self.n.BOOTSTRAP_UNIT)

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
        for name, unit in (('observer', self.n.OBSERVER_UNIT), ('mapwatch', self.n.MAPWATCH_UNIT)):
            self.start_transient(transaction, [part.replace('{run}', run) for part in transient[name]], unit)

    # --------------------------------------------- stop, rollback, retire

    def stop(self, transaction):
        """Withdraw `/v2/txid/` with a 503 site, then stop the controller; under a minute.

        A route that cannot be withdrawn puts the previous Caddyfile back, so
        the host never holds bytes Caddy refused, and the controller still stops.
        """
        started = self.clock()
        failure = None
        transaction.undo('post', self.route_host, 'reload-caddy')
        try:
            if transaction.change(self.route_host, self.routes_path, self.render()['withdrawn.caddy'], 0o644):
                self.reload_caddy()
        except Exception as error:
            failure = error
            entry = transaction.data['changes'][-1] if transaction.data['changes'] else None
            if entry and entry['path'] == self.routes_path:
                self.revert(entry)
                transaction.event('withdrawal refused; previous snippet restored')
        self.stop_units(transaction, 'coordinator', [self.n.CONTROLLER_UNIT])
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

        Another writer owns those bytes now (a later display transaction, or
        a hand edit), and putting the prior
        bytes back would silently undo its change. Entries are checked newest
        first against what the restore would leave, so a path changed twice
        in one transaction is judged step by step.
        """
        current, problems = {}, []
        for entry in reversed(transaction.data['changes']):
            key = (entry['host'], entry['path'])
            if key not in current:
                current[key] = self.ex.sha256(self.host(entry['host']), entry['path'])
            if entry.get('worker_rewrites'):
                current[key] = entry['previous_sha256']
                continue
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
                'or use txid-display-retire' % '; '.join(drifted))
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

    def rollback(self, identifier):
        transaction = self.transaction = Transaction.load(self, identifier)
        require(transaction.status not in ('rolled-back', 'superseded'),
                'transaction %s is already %s' % (transaction.id, transaction.status))
        with self.locked(None):
            self.restore(transaction)

    def retire(self, expected):
        """Remove every unit and withdraw the site's Caddyfile. Data stays.

        The site's host and firewall are Terraform's.
        """
        self.check_plan(expected)
        with self.locked('retire') as transaction:
            persistent = (('coordinator', self.n.CONTROLLER_UNIT), *((key, WORKER_UNIT) for key in self.worker_keys))
            self.stop_units(transaction, 'coordinator', [self.n.CONTROLLER_UNIT, *self.n.TRANSIENT])
            for key in self.worker_keys:
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
            for key in self.worker_keys:
                # The worker's generated configuration goes with its unit.
                transaction.remove(key, WORKER_CONFIG)
            # The host keeps a Caddyfile, so Caddy keeps running: the withdrawn site.
            transaction.undo('post', self.route_host, 'reload-caddy')
            if transaction.change(self.route_host, self.routes_path, self.render()['withdrawn.caddy'], 0o644):
                self.reload_caddy()
            transaction.finish('committed')
        self.print_data()

    def print_data(self):
        r = self.request
        self.out('retained data; deleting it needs separate approval:')
        paths = [('coordinator', r['journal']['data_dir']), ('coordinator', r['journal']['data_dir'] + '.smoke'),
                 ('coordinator', r['publication']['root']), ('coordinator', r['measure']['output'])]
        paths += [(key, self.n.OPT) for key in self.host_keys] + [(key, WORKER_DATA) for key in self.worker_keys]
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
                              else 'withdrawn' if digest == sha256(files['withdrawn.caddy']) else 'differs')
        result = {'request_sha256': self.request_sha256, 'plan_sha256': self.plan_sha256(), 'files': deployed,
                  'units': {}, 'workers': {}}
        for key, unit in [('coordinator', u) for u in (self.n.CONTROLLER_UNIT, *self.n.TRANSIENT)] + [(k, WORKER_UNIT) for k in self.worker_keys]:
            state = self.unit_state(key, unit)
            result['units']['%s@%s' % (unit, key)] = {k: state[k] for k in ('load', 'active', 'sub', 'result',
                                                                            'restarts')}
        for key in self.worker_keys:
            status, body = self.ex.http_get(self.host(key), self.ready_url(key), 5)
            try:
                ready = json.loads(body) if status == 200 else None
            except ValueError:
                ready = None
            result['workers'][key] = {'http': status, 'ready': ready}
        recent = []
        for path in sorted(self.journal_dir.glob('txid-display-*.json'), key=lambda p: p.stat().st_mtime)[-10:]:
            data = json.loads(path.read_text())
            if data.get('request_sha256') == self.request_sha256:
                recent.append({k: data.get(k) for k in ('id', 'action', 'status')})
        result['transactions'] = recent
        return result

    # ----------------------------------------------------------- entries

    def deploy(self, phase, expected, archive=None, map_sha256=None, replace_active=None):
        require(phase in PHASES, 'unknown phase %r' % (phase,))
        require(replace_active is None or phase == 'workers', '--replace-active belongs to the workers phase')
        self.check_plan(expected)
        with self.locked('deploy-' + phase) as transaction:
            self.require_preflight(phase)
            if phase == 'stage':
                self.stage(transaction, archive)
            elif phase == 'workers':
                self.workers(transaction, map_sha256, replace_active)
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
                self.stop_units(transaction, 'coordinator', [self.n.INGEST_UNIT, self.n.SMOKE_UNIT])
            elif action == 'bootstrap':
                self.bootstrap(transaction, start, through)
            elif action == 'verify':
                self.verify(transaction)
            elif action == 'measure-start':
                self.measure_start(transaction)
            elif action == 'measure-stop':
                self.stop_units(transaction, 'coordinator', [self.n.OBSERVER_UNIT, self.n.MAPWATCH_UNIT])
            else:
                self.stop(transaction)


def iso_unix(text):
    try:
        return datetime.datetime.fromisoformat(text).timestamp()
    except (TypeError, ValueError):
        return 0


def load_release():
    from wallet_pir_ops import release
    return release


def rollback_command(args, transaction):
    return shlex.join([sys.executable, str(ROOT / 'tools/scripts/wallet-pir-deploy.py'),
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
                       map_sha256=args.publication_map_sha256, replace_active=args.replace_active)
        elif command == 'rollback':
            poc.rollback(args.transaction)
        elif command == 'retire':
            poc.retire(args.expect_plan_sha256)
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
