"""Deployed changed-native candidate qualification through the production wrapper.

Four closed activities measure the canonical deployment after a committed
version-2 product transaction for candidate `c3c66b9b`: `staged-load`,
`freshness`, `capacity` and `fault`. `schema-qualify-run` launches one
detached owner under the coordinator production lock and the shared
input-staging ownership fence, so no other wrapper mutation and no second
qualification can overlap it. Rates, durations, thresholds, units, origins,
artifacts and every timing bound are fixed here; a request only selects among
them and supplies the reviewed capacity sample. Raw receipts and every raw
observation are written once and sealed read-only.

Every wait, network call and child shares one monotonic owner deadline, and no
bounded suboperation starts unless its whole bound still fits. Before any effect
the owner reconciles every pinned host: its production lock, the shared fences,
the latest owner of every namespace with PID and start time, and every transient
owner unit with its cgroup. A unit effect (local or remote) records the exact
pre-fault service and each phase durably before acting; anything other than a
verified result stays fenced until reconcile restores and proves that service.

Only the retained candidate clients run: `examples/rate-query` (canonical
encrypted queries), `transparent-loadtest` (wallet syncs) and the existing
recovery proof (reopened SQLite). `transparent-measure` serves its own local
shard set and cannot measure a deployment. Gaps the native interfaces cannot
measure are listed in MISSING and keep a result unqualified; fixtures in the
test suite never qualify anything.
"""
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.request

from wallet_pir_ops import durable, inherited_lock, schema_fence, transparent_map
from wallet_pir_ops.deploy.remote import ProductionLock, SSHExecutor

HERE = Path(__file__).parent


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


C = module('qualification_candidate', HERE/'activity_candidate.py')
H = module('qualification_host', HERE/'activity_schema_host.py')
S = module('qualification_source_receipt', HERE/'activity_source_stage_host.py')
R = module('qualification_recovery', HERE/'activity_recovery_proof.py')
_PRODUCT = []


def product():
    """The reviewed product validator, loaded only where a journal is read."""
    if not _PRODUCT:
        _PRODUCT.append(module('qualification_product', HERE/'activity_schema_product.py'))
    return _PRODUCT[0]


ROOT = Path('/srv/transparent-activity/qualification')
OWNERS = schema_fence.INPUT_STAGING
SOURCES = Path('/srv/transparent-activity/ops/sources')
STAGING = Path('/srv/transparent-activity/ops/staging')
SCHEMA = schema_fence.SCHEMA_STATE
REMOTE = Path('/srv/transparent-activity/ops/qualification-actions')
ROLLBACK_ROOT = Path('/opt/transparent-publisher/schema-rollback')
SHARD_ORIGIN = 'https://transparent-pir.valargroup.dev'
FILTER_ORIGIN = 'https://enhance-pir.valargroup.dev'
NODE = 'http://127.0.0.1:8232'
COOKIE = Path('/root/.cache/zakura/.cookie')
SCHEMA_V11 = 'transparent-shard-v11'
HEX = re.compile('[0-9a-f]{64}')
TXN = re.compile('transparent-schema-[A-Za-z0-9-]+')
NAME = re.compile('[a-zA-Z0-9-]{1,64}')
# Transient owners of every wrapper-launched activity, burst and job.
OWNER_UNIT = re.compile(r'transparent-(?:activity|full-burst)-[A-Za-z0-9@._-]{1,160}\.service')
MAX_REQUEST = 8 << 20
MAX_REPLY = 1 << 20
HEADROOM = .2

KINDS = ('staged-load', 'freshness', 'capacity', 'fault')
# Clients split the requested rate; rate-query fixes the 40/40/10/10 recent/
# archive directory/page mix (80/20 recent/archive) and fresh keys per query.
LOAD_STAGES = ({'qps':5, 'seconds':120, 'processes':5}, {'qps':20, 'seconds':600, 'processes':5})
LOAD_GATES = {'logical_failures':0, 'minimum_rate_fraction':.95, 'p50_below_seconds':.7,
              'p99_below_seconds':2.0, 'transport_failure_below_fraction':.01, 'recent_fraction':(.75, .85)}
FRESHNESS = {'seconds':21600, 'blocks':300, 'publication_seconds':30, 'recent_seconds':60,
             'bound_seconds':43200, 'permit_seconds':300, 'gap_seconds':10}
LEVELS = (8, 20, 40)
REQUIRED_TRIALS, MAX_TRIALS = 3, 6
# The supplied mixed-20 composition, doubled for 40. Eight slots cannot hold
# all nine classes; catch-up-7d is omitted there (1d/30d bracket it) so the
# heavy class runs at every level.
COMPOSITION = {
    8: {'unused':1, 'small-active':1, 'catch-up-1d':1, 'catch-up-30d':1, 'restore-6m':1,
        'restore-old':1, 'multi-script':1, 'reused-tail':1},
    20: {'unused':2, 'small-active':4, 'catch-up-1d':2, 'catch-up-7d':2, 'catch-up-30d':2,
         'restore-6m':3, 'restore-old':2, 'multi-script':2, 'reused-tail':1},
    40: {'unused':4, 'small-active':8, 'catch-up-1d':4, 'catch-up-7d':4, 'catch-up-30d':4,
         'restore-6m':6, 'restore-old':4, 'multi-script':4, 'reused-tail':2},
}
# deployment.md "macOS recovery beta acceptance targets"; multi-script is the
# cutoff forty-script wallet and restore-old the old-birthday restore.
P95_TARGETS = {'small-active':5, 'catch-up-1d':5, 'catch-up-7d':5, 'catch-up-30d':5,
               'restore-6m':10, 'restore-old':60, 'multi-script':60, 'unused':15}
HEAVY = 'reused-tail'
CAPACITY = {'duration_seconds':3600, 'recovery_deadline_seconds':600, 'preparation_deadline_seconds':3600,
            'request_timeout_seconds':60, 'measured_http_attempts':3, 'preparation_concurrency':2,
            'minimum_observations':100, 'failed_or_incomplete_fraction':.05, 'http_503_fraction':.10,
            'supported_fraction':.5, 'cgroup_memory_fraction':.8}
FAULTS = ('client-reopen', 'publication-interruption', 'recent-worker-loss', 'archive-restart',
          'router-restart', 'rollback-redeploy')
FAULT_TARGET = {'recent-worker-loss':'recent-replica', 'archive-restart':'archive-owner'}
FAULT_ACTION = {'publication-interruption':('coordinator', 'stop-start'), 'recent-worker-loss':('worker', 'stop-start'),
                'archive-restart':('worker', 'restart'), 'router-restart':('router', 'restart')}
REMOTE_UNITS = {'worker':H.WORKER, 'router':'caddy.service'}
REMOTE_DISK = {'worker':H.CACHE, 'router':Path('/')}
REMOTE_OPERATIONS = {'worker':('probe', 'identity', 'control-status', 'await-preparation', 'restart', 'stop-start'),
                     'router':('probe', 'identity', 'restart')}
EFFECTS = ('restart', 'stop-start')
COORDINATOR_UNITS = (*H.AUTHORITY, H.FILTER, H.LOAD)
PUBLISHER = H.AUTHORITY[0]
RECOVERY_SECONDS = 900
LOSS_HOLD_SECONDS = 60
PREPARATION_WAIT_SECONDS = 300
# Reviewed timing bounds. Each is the most a suboperation may take; it starts
# only when the owner deadline still holds the whole bound.
UNIT_SECONDS = {'stop':90, 'start':90}
REMOTE_BOUND = {'probe':30, 'identity':90, 'control-status':15, 'await-preparation':PREPARATION_WAIT_SECONDS+15,
                'restart':UNIT_SECONDS['stop']+UNIT_SECONDS['start'],
                'stop-start':UNIT_SECONDS['stop']+LOSS_HOLD_SECONDS+UNIT_SECONDS['start'], 'reconcile':240}
# A failed effect gets this much more to restore its owned unit before replying.
RESTORE_SECONDS = UNIT_SECONDS['start']+60
SSH_MARGIN = 30
QUERY_PROBE_SECONDS = 90
WALLET_PROBE_SECONDS = 180  # the recovery proof's 120 s native bound plus store inspection
ATTEMPT_SECONDS = QUERY_PROBE_SECONDS+WALLET_PROBE_SECONDS+60
RESTORE_LIMIT = 3
RECONCILE_SECONDS = 240
CHILD_STOP_SECONDS = 30
# Reserved before RuntimeMaxSec so a clean stop can end children and seal results;
# TimeoutStopSec covers the same work after an external stop.
OWNER_MARGIN = 150
STOP_TIMEOUT = 90
REMOTE_HEALTH_INTERVAL = 60
REMOTE_HEALTH_STALE = 150
LOOP_SECONDS = 2
# Reviewed schema recipe budgets; a successful rollback must use them unchanged.
ROLLBACK_BUDGET = {'withdraw-origins':60, 'restore-v10':140, 'verify-rollback':300, 'reopen-v10':100, 'verify-service':140}
FORWARD_TIMEOUT = 1800
RUNTIME = {'staged-load':2400, 'freshness':FRESHNESS['bound_seconds']+1800, 'capacity':10800, 'fault':3600}
EFFECT_SECONDS = {'client-reopen':300,
                  'publication-interruption':REMOTE_BOUND['await-preparation']+REMOTE_BOUND['control-status']+
                                             UNIT_SECONDS['stop']+UNIT_SECONDS['start']+RESTORE_SECONDS+3*SSH_MARGIN,
                  'recent-worker-loss':REMOTE_BOUND['stop-start']+RESTORE_SECONDS+SSH_MARGIN,
                  'archive-restart':REMOTE_BOUND['restart']+RESTORE_SECONDS+SSH_MARGIN,
                  'router-restart':REMOTE_BOUND['restart']+RESTORE_SECONDS+SSH_MARGIN, 'rollback-redeploy':0}
POST_SECONDS = 300
MEMORY = {'capacity':('10G', '12G')}
MISSING = {
    'all': ['quality alerts: their shadow state is APM configuration with no coordinator interface; '
            'only the stopped quality supervisor is verified'],
    'capacity': ['heavy continuation: transparent-loadtest retains incomplete heavy SQLite stores but no native '
                 'interface resumes a retained store; continuation is unmeasured and capacity stays unqualified'],
    'client-reopen': ['interrupted-store continuation: the interrupted store is integrity-checked and an exact '
                      'fresh recovery is required; no native interface resumes the interrupted store'],
    'archive-restart': ['cache corruption: injecting a corrupt runtime/disk cache needs destructive file mutation '
                        'with no reviewed native interface; only an archive-owner restart and cache reload run'],
    'rollback-redeploy': ['rolled-back service probe: the v10 state is proven only by the recipe\'s own '
                          'verify-rollback/verify-service phases; this owner probes the redeployed candidate, timed '
                          'from its own start, and retains the redeploy-commit-to-proof wall time'],
}


class Budget(ValueError):
    """A bounded suboperation does not fit in the remaining deadline."""


class Unknown(ValueError):
    """Transport lost: the remote outcome is unknown and stays fenced."""


class Interrupted(Exception):
    pass


def require(ok, message):
    if not ok:
        raise ValueError(message)


def digest(value):
    return durable.digest(value)


def checksum(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate qualification field')
        result[key] = value
    return result


class Deadline:
    """One monotonic bound shared by every wait, call and child of an operation."""
    def __init__(self, seconds, *, start=None, end=None):
        self.started = time.monotonic() if start is None else start
        self.end = self.started+seconds if end is None else min(end, self.started+seconds)

    def remaining(self):
        return self.end-time.monotonic()

    def elapsed(self):
        return time.monotonic()-self.started

    def need(self, seconds, what):
        if self.remaining() < seconds:
            raise Budget('%s needs %d s but only %.0f s remain' % (what, seconds, max(0, self.remaining())))

    def timeout(self, cap):
        left = self.remaining()
        if left <= 0:
            raise Budget('deadline exhausted')
        return min(cap, left)

    def child(self, seconds):
        return Deadline(seconds, end=self.end)

    def sleep(self, seconds):
        time.sleep(max(0, min(seconds, self.remaining())))
        if self.remaining() <= 0:
            raise Budget('deadline exhausted while waiting')


class BoundedCommands(H.Commands):
    """The reviewed systemctl/control calls, each capped by the shared deadline."""
    def __init__(self, deadline=None, cap=15):
        self.deadline, self.cap = deadline, cap

    def run(self, argv, *, data=None, timeout=30):
        limit = min(timeout, self.cap)
        if self.deadline is not None:
            limit = self.deadline.timeout(limit)
        return super().run(argv, data=data, timeout=limit)


def commands(deadline=None):
    return BoundedCommands(deadline)


def read_request(stream, expected):
    raw = stream.read(MAX_REQUEST+1)
    require(len(raw) <= MAX_REQUEST, 'qualification request exceeds bound')
    request = validate(json.loads(raw, object_pairs_hook=unique))
    require(digest(request) == expected, 'qualification request checksum differs')
    return request


def validate_sample(sample, level):
    require(isinstance(sample, dict) and {'genesis_hash', 'start_height', 'cutoff_height', 'anchor_height',
            'anchor_hash', 'clients'} <= set(sample), 'invalid capacity sample')
    require(all(isinstance(sample[k], str) and HEX.fullmatch(sample[k]) for k in ('genesis_hash', 'anchor_hash')) and
            all(type(sample[k]) is int and sample[k] >= 0 for k in ('start_height', 'cutoff_height', 'anchor_height')),
            'invalid capacity sample identity')
    clients = sample['clients']
    require(isinstance(clients, list) and 1 <= len(clients) <= 4096, 'invalid capacity sample clients')
    for client in clients:
        require(isinstance(client, dict) and {'class', 'scripts', 'required_from', 'expected_digest', 'journal_events'} <= set(client) and
                client['class'] in COMPOSITION[40] and isinstance(client['scripts'], list) and 1 <= len(client['scripts']) <= 64 and
                all(isinstance(s, str) and re.fullmatch('(?:[0-9a-f]{2}){1,520}', s) for s in client['scripts']) and
                type(client['required_from']) is int and type(client['journal_events']) is int and client['journal_events'] >= 0 and
                isinstance(client['expected_digest'], str) and HEX.fullmatch(client['expected_digest']), 'invalid capacity sample client')
    require(set(COMPOSITION[level]) <= {c['class'] for c in clients}, 'capacity sample lacks a composed profile')
    return sample


def validate(request):
    base = {'version', 'kind', 'source_sha', 'attempt', 'transaction', 'recipe_sha256'}
    require(isinstance(request, dict) and base <= set(request) and request.get('kind') in KINDS, 'invalid qualification request')
    kind, fault = request['kind'], request.get('fault')
    extra = {'staged-load':set(), 'freshness':set(), 'capacity':{'level', 'trial', 'sample'},
             'fault':{'fault'} | ({'target'} if fault in FAULT_TARGET else set()) |
                     ({'rolled_back_transaction'} if fault == 'rollback-redeploy' else set())}[kind]
    require(set(request) == base | extra, 'qualification request fields are closed')
    require(type(request['version']) is int and request['version'] == 1 and
            isinstance(request['source_sha'], str) and re.fullmatch('[0-9a-f]{40}', request['source_sha']) and
            type(request['attempt']) is int and 1 <= request['attempt'] <= 100 and
            isinstance(request['transaction'], str) and TXN.fullmatch(request['transaction']) and
            isinstance(request['recipe_sha256'], str) and HEX.fullmatch(request['recipe_sha256']), 'invalid qualification identity')
    if kind == 'capacity':
        require(type(request['level']) is int and request['level'] in LEVELS and
                type(request['trial']) is int and 1 <= request['trial'] <= MAX_TRIALS, 'invalid capacity trial')
        validate_sample(request['sample'], request['level'])
    if kind == 'fault':
        require(fault in FAULTS, 'unsupported lifecycle fault')
        require('target' not in request or isinstance(request['target'], str) and NAME.fullmatch(request['target']), 'invalid fault target')
        require('rolled_back_transaction' not in request or isinstance(request['rolled_back_transaction'], str) and
                TXN.fullmatch(request['rolled_back_transaction']) and request['rolled_back_transaction'] != request['transaction'],
                'rollback/redeploy needs the distinct rolled-back transaction')
    return request


def summary(request):
    """Request identity without the sample body."""
    result = {k:v for k, v in request.items() if k != 'sample'}
    if 'sample' in request:
        result['sample_sha256'] = digest(request['sample'])
    return result


def unit_name(sha):
    return 'transparent-activity-qualification-'+sha[:16]


def properties(kind):
    high, peak = MEMORY.get(kind, ('4G', '6G'))
    return ('CPUQuota=200%', 'MemoryHigh='+high, 'MemoryMax='+peak, 'MemorySwapMax=0', 'Nice=5', 'IOWeight=50',
            'Restart=no', 'KillMode=control-group', 'TimeoutStopSec=%d' % STOP_TIMEOUT, 'RemainAfterExit=yes',
            'RuntimeMaxSec=%d' % RUNTIME[kind])


def bounds():
    return {'owner_runtime_seconds':RUNTIME, 'owner_margin_seconds':OWNER_MARGIN, 'remote_seconds':REMOTE_BOUND,
            'restore_seconds':RESTORE_SECONDS,
            'ssh_margin_seconds':SSH_MARGIN, 'unit_seconds':UNIT_SECONDS, 'attempt_seconds':ATTEMPT_SECONDS,
            'effect_seconds':EFFECT_SECONDS, 'post_seconds':POST_SECONDS, 'restore_limit':RESTORE_LIMIT,
            'reconcile_seconds':RECONCILE_SECONDS, 'remote_health_interval_seconds':REMOTE_HEALTH_INTERVAL,
            'remote_health_stale_seconds':REMOTE_HEALTH_STALE, 'loop_seconds':LOOP_SECONDS}


def activity(request):
    kind = request['kind']
    if kind == 'staged-load':
        return {'stages':list(LOAD_STAGES), 'gates':LOAD_GATES, 'freshness_permit':FRESHNESS,
                'client':'examples/rate-query', 'workers_per_client':8, 'origin':SHARD_ORIGIN}
    if kind == 'freshness':
        return {'freshness':FRESHNESS, 'origins':[SHARD_ORIGIN, FILTER_ORIGIN], 'replicas':'every recent-replica upstream',
                'latency':'conservative: last node observation without the block to first observation serving it'}
    if kind == 'capacity':
        return {'level':request['level'], 'trial':request['trial'], 'composition':COMPOSITION[request['level']],
                'p95_targets':P95_TARGETS, 'heavy':HEAVY, 'capacity':CAPACITY, 'client':'transparent-loadtest',
                'mode':'sustained', 'seed':request['trial'], 'origins':[SHARD_ORIGIN, FILTER_ORIGIN]}
    fault = request['fault']
    return {'fault':fault, 'target':request.get('target'), 'action':list(FAULT_ACTION[fault]) if fault in FAULT_ACTION else None,
            'loss_hold_seconds':LOSS_HOLD_SECONDS if FAULT_ACTION.get(fault, (None, None))[1] == 'stop-start' else None,
            'preparation_wait_seconds':PREPARATION_WAIT_SECONDS if fault == 'publication-interruption' else None,
            'recovery_seconds':RECOVERY_SECONDS, 'rollback_budget':ROLLBACK_BUDGET,
            'rolled_back_transaction':request.get('rolled_back_transaction'),
            'probes':['canonical encrypted query (rate-query 5 QPS, 30 s)', 'reopened-wallet recovery proof',
                      'worker binary/assignment identity, worker map equal to the canonical public map, sealed prefix kept',
                      'retained rollback baselines unchanged on every host']}


def plan(request):
    validate(request)
    sha = digest(request)
    kind = request['kind']
    missing = MISSING['all'] + MISSING.get(kind, []) + MISSING.get(request.get('fault'), [])
    return {'version':2, 'kind':'deployed-qualification', 'request_sha256':sha, 'request':summary(request),
            'candidate_sha':C.SOURCE_SHA, 'candidate_identity':C.identity(), 'historical_release_sha':C.HISTORICAL_SHA,
            'activity':activity(request), 'unit':unit_name(sha), 'properties':list(properties(kind)),
            'bounds':bounds(), 'headroom':HEADROOM, 'missing_assurance':missing,
            'effects':'private qualification receipts, owned client processes'+
                      (' and one fixed, restorable unit transition' if request.get('fault') in FAULT_ACTION else '')}


# --- process identity --------------------------------------------------------

PROC = Path('/proc')


def boot_id():
    return (PROC/'sys/kernel/random/boot_id').read_text().strip()


def stat_fields(pid):
    raw = (PROC/str(pid)/'stat').read_text()
    return raw[raw.rindex(')')+2:].split()


def process_identity(pid):
    fields = stat_fields(pid)
    return {'pid':pid, 'start_ticks':int(fields[19]), 'session':int(fields[3]), 'boot_id':boot_id()}


def alive(identity):
    try:
        fields = stat_fields(identity['pid'])
        return identity.get('boot_id') == boot_id() and int(fields[19]) == identity['start_ticks'] and fields[0] != 'Z'
    except (OSError, ValueError, IndexError, KeyError, TypeError):
        return False


def started_unix(pid):
    """Wall-clock start of a live process, from boot time and start ticks."""
    boot = next(int(line.split()[1]) for line in (PROC/'stat').read_text().splitlines() if line.startswith('btime '))
    return boot + int(stat_fields(pid)[19])/os.sysconf('SC_CLK_TCK')


def executable_sha256(pid):
    return checksum(PROC/str(pid)/'exe')


def session_members(sessions):
    """Live processes in any recorded owned session on this boot."""
    sessions, found = set(sessions), []
    if not sessions:
        return found
    for entry in PROC.iterdir():
        if entry.name.isdigit():
            try:
                fields = stat_fields(entry.name)
                if int(fields[3]) in sessions and fields[0] != 'Z':
                    found.append(int(entry.name))
            except (OSError, ValueError, IndexError):
                continue
    return found


def write_once(path, value):
    """Immutable private receipt: create exclusively, fsync, seal read-only."""
    data = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False)+'\n').encode()
    write_bytes_once(path, data)


def write_bytes_once(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, data)
        os.fsync(fd)
        os.fchmod(fd, 0o400)
    finally:
        os.close(fd)


def seal(directory):
    for path in sorted(Path(directory).rglob('*')):
        if path.is_file() and not path.is_symlink():
            os.chmod(path, 0o400)


def lines(path):
    result = []
    if not Path(path).exists():
        return result
    with Path(path).open() as stream:
        for line in stream:
            line = line.strip()
            if line:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    value = None
                result.append(value if isinstance(value, dict) else {'event':'unparseable'})
    return result


def percentile(values, fraction):
    """Nearest rank; None for no observations."""
    values = sorted(values)
    return values[max(0, math.ceil(fraction*len(values))-1)] if values else None


# --- evaluators over retained raw receipts -----------------------------------

def evaluate_rate(processes, qps, seconds, fixture_sha256, gates=LOAD_GATES):
    """Gate one staged rate from every client's raw rate-query lines."""
    failures, queries, errors, succeeded, failed = [], [], [], set(), set()
    for index, events in enumerate(processes):
        ready = [e for e in events if e.get('event') == 'ready']
        if len(ready) != 1 or ready[0].get('fixture_sha256') != fixture_sha256 or \
                not isinstance(ready[0].get('targets'), list) or len(ready[0]['targets']) != 4 or \
                not all(isinstance(t, int) and t > 0 for t in ready[0]['targets']):
            failures.append('client %d did not bind the reviewed four-group fixture' % index)
        if any(e.get('event') == 'unparseable' for e in events):
            failures.append('client %d emitted unparseable output' % index)
        for e in events:
            key = (index, e.get('logical_id', e.get('sequence')))
            if e.get('event') == 'query':
                queries.append(e)
                if e.get('exact') is True:
                    succeeded.add(key)
            elif e.get('event') == 'error':
                errors.append(e)
                failed.add(key)
    inexact = sum(e.get('exact') is not True for e in queries)
    logical_failures = len(failed - succeeded) + inexact
    attempts = len(queries) + len(errors)
    transport = len(errors)/attempts if attempts else 1.0
    rate = len(succeeded)/seconds
    timings = [e['http_seconds'] for e in queries if isinstance(e.get('http_seconds'), (int, float))]
    p50, p99 = percentile(timings, .5), percentile(timings, .99)
    recent = sum(str(e.get('geometry', '')).startswith('recent') for e in queries)
    recent_fraction = recent/len(queries) if queries else None
    if logical_failures > gates['logical_failures']:
        failures.append('logical query failures')
    if rate < gates['minimum_rate_fraction']*qps:
        failures.append('completed rate below 95 percent of requested')
    if p50 is None or p50 >= gates['p50_below_seconds'] or p99 >= gates['p99_below_seconds']:
        failures.append('latency gate failed')
    if transport >= gates['transport_failure_below_fraction']:
        failures.append('transport attempt failures at or above 1 percent')
    low, high = gates['recent_fraction']
    if recent_fraction is None or not low <= recent_fraction <= high:
        failures.append('recent/archive mix outside 80/20')
    return {'status':'failed' if failures else 'passed', 'failures':failures, 'qps':qps, 'seconds':seconds,
            'clients':len(processes), 'exact_logical':len(succeeded), 'logical_failures':logical_failures,
            'attempts':attempts, 'failed_attempts':len(errors), 'transport_failure_fraction':transport,
            'completed_qps':rate, 'p50_seconds':p50, 'p99_seconds':p99, 'recent_fraction':recent_fraction,
            'missed_slots':sum(e.get('event') == 'missed_slot' for p in processes for e in p)}


class FreshnessWindow:
    """Qualifying continuous-freshness interval from raw tip/serving observations.

    Every time is a monotonic observation bound, never a guess of block birth.
    A new canonical height is born no earlier than the start of the last node
    observation that did not yet report it, and is visible no later than the end
    of the first observation that serves it; their difference is an upper bound
    on its latency. It must be within 30 s at the public origin and 60 s at every
    recent replica. A violation, reorganization or an observation gap longer than
    the fixed bound closes the interval as failed, retains it, and starts a new
    interval with no inherited credit, so unobserved time never counts as fresh.
    """
    def __init__(self, replicas, now, freshness=None):
        self.replicas, self.freshness = tuple(sorted(replicas)), freshness or FRESHNESS
        self.failed, self.tip, self.last_node = [], None, None
        self.start(now)

    def start(self, now):
        self.started, self.last_complete, self.births = now, now, {}
        self.visible = {layer:{} for layer in ('public', *self.replicas)}

    def close(self, now, reason):
        self.failed.append({'started':self.started, 'ended':now, 'reason':reason, 'blocks':self.blocks()})
        self.start(now)

    def blocks(self):
        return min(len(v) for v in self.visible.values())

    def node(self, height, before, after):
        if self.tip is not None and height < self.tip:
            self.close(after, 'chain reorganized')
        if self.tip is not None and self.last_node is not None:
            for value in range(self.tip+1, height+1):
                self.births.setdefault(value, self.last_node)
        self.tip, self.last_node = height, before

    def reorganized(self, now):
        self.close(now, 'chain reorganized')

    def serving(self, layer, end_height, before, after):
        for height, born in self.births.items():
            if height <= end_height and height not in self.visible[layer]:
                self.visible[layer][height] = after-born

    def complete(self, now):
        """One complete observation; a longer gap since the last one fails the interval."""
        gap = now-self.last_complete
        self.last_complete = now
        if gap > self.freshness['gap_seconds']:
            return 'observation gap of %.1f s exceeds %d s' % (gap, self.freshness['gap_seconds'])
        return None

    def violation(self, now):
        for layer, values in self.visible.items():
            budget = self.freshness['publication_seconds' if layer == 'public' else 'recent_seconds']
            late = [h for h, latency in values.items() if latency > budget]
            overdue = [h for h, born in self.births.items() if h not in values and now-born > budget]
            if late or overdue:
                return '%s freshness exceeded %ss at block %d' % (layer, budget, min(late+overdue))
        return None

    def qualifies(self, now):
        return now-self.started >= self.freshness['seconds'] and self.blocks() >= self.freshness['blocks']

    def permits(self):
        """At least one new canonical block observed fresh at every layer."""
        return self.blocks() >= 1

    def record(self, now):
        return {'started':self.started, 'seconds':now-self.started, 'blocks':self.blocks(),
                'maximum_seconds':{k:max(v.values(), default=None) for k, v in self.visible.items()},
                'latency':'upper bound from observation times', 'failed_intervals':list(self.failed)}


def metric_values(text, name):
    values = []
    for line in text.splitlines():
        if line.startswith(name) and (len(line) == len(name) or line[len(name)] in ' {'):
            try:
                values.append(float(line.rsplit(' ', 1)[1]))
            except (IndexError, ValueError):
                continue
    return values


def evaluate_metrics(entries, targets, fraction=CAPACITY['cgroup_memory_fraction']):
    """Worker restart/cgroup/queue/revision evidence from loadtest scrapes."""
    workers, failures = {}, []
    for target in targets:
        texts = [e['text'] for e in entries if e.get('target') == target and isinstance(e.get('text'), str)]
        gaps = sum(1 for e in entries if e.get('target') == target and not isinstance(e.get('text'), str))
        starts = {v for t in texts for v in metric_values(t, 'transparent_shard_process_start_time_seconds')}
        ratios = []
        for t in texts:
            current = metric_values(t, 'transparent_shard_cgroup_memory_current_bytes')
            peak = metric_values(t, 'transparent_shard_cgroup_memory_max_bytes')
            if current and peak and peak[0] > 0:
                ratios.append(current[0]/peak[0])
        def maximum(name):
            return max((v for t in texts for v in metric_values(t, name)), default=None)
        workers[target] = {'scrapes':len(texts), 'gaps':gaps, 'process_starts':len(starts),
                           'maximum_cgroup_memory_fraction':max(ratios, default=None),
                           'maximum_queue_depth':maximum('transparent_shard_query_queue_depth'),
                           'maximum_revisions_held':maximum('transparent_shard_revisions_held'),
                           'maximum_body_bytes_in_flight':maximum('transparent_shard_body_bytes_in_flight'),
                           'queue_rejections':maximum('transparent_shard_queue_rejections_total'),
                           'overloads':maximum('transparent_shard_overloads_total')}
        if not texts:
            failures.append('no metrics scrape for '+target)
        if len(starts) > 1:
            failures.append('worker restarted during trial: '+target)
        if not ratios:
            failures.append('no cgroup memory evidence for '+target)
        elif max(ratios) > fraction:
            failures.append('worker cgroup memory above 80 percent of limit: '+target)
    return workers, failures


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def wallet_events(events):
    """Scheduled, started and outcome events keyed by wallet; malformed input fails."""
    scheduled, started, outcomes, failures = {}, {}, {}, []
    for e in events:
        kind, identifier = e.get('type'), e.get('id')
        if kind not in ('scheduled', 'started', 'outcome') or type(identifier) is not int or not number(e.get('at')) or \
                kind == 'scheduled' and not isinstance(e.get('profile'), str) or \
                kind == 'outcome' and not isinstance(e.get('outcome'), str):
            failures.append('malformed wallet event')
            continue
        table = {'scheduled':scheduled, 'started':started, 'outcome':outcomes}[kind]
        if identifier in table:
            failures.append('duplicate %s wallet event' % kind)
            continue
        table[identifier] = e
    if set(started) - set(scheduled) or set(outcomes) - set(scheduled):
        failures.append('wallet event without a scheduled wallet')
    for identifier, e in started.items():
        if identifier in scheduled and e['at'] < scheduled[identifier]['at']:
            failures.append('wallet started before it was scheduled')
    for identifier, e in outcomes.items():
        begun = started.get(identifier, scheduled.get(identifier))
        if begun is not None and e['at'] < begun['at']:
            failures.append('wallet outcome precedes its start')
    return scheduled, started, outcomes, failures


def evaluate_capacity(directory, level, ended_unix, exit_code, stopped, targets):
    """One sustained trial from loadtest NDJSON; scheduled wallets never count.

    Only an owned loadtest that exited 0 by itself, unstopped, after a full
    window can pass, however sufficient its partial observations are.
    """
    directory = Path(directory)
    scheduled, started, outcomes, failures = wallet_events(lines(directory/'wallets.ndjson'))
    unterminated = sorted(set(scheduled) - set(outcomes))
    if unterminated:
        failures.append('scheduled wallets without a terminal outcome')
    t0 = min((e['at'] for e in scheduled.values()), default=None)
    window_end = t0 + CAPACITY['duration_seconds'] if t0 is not None else None
    profiles = {name:{'terminal':0, 'exact':0, 'outcomes':{}, 'seconds':[]} for name in COMPOSITION[level]}
    exact_in_window = 0
    for identifier, outcome in outcomes.items():
        if identifier not in scheduled:
            continue
        profile = scheduled[identifier]['profile']
        if profile not in profiles:
            failures.append('wallet outside the composed profiles')
            continue
        entry = profiles[profile]
        kind = outcome['outcome']
        entry['terminal'] += 1
        entry['outcomes'][kind] = entry['outcomes'].get(kind, 0)+1
        if kind == 'exact' and outcome.get('events_exact') is True and identifier in started and \
                outcome['at'] >= started[identifier]['at']:
            entry['exact'] += 1
            entry['seconds'].append(outcome['at']-started[identifier]['at'])
            exact_in_window += outcome['at'] <= window_end
    terminal = sum(p['terminal'] for p in profiles.values())
    exact = sum(p['exact'] for p in profiles.values())
    failed_fraction = (terminal-exact)/terminal if terminal else 1.0
    requests = lines(directory/'requests.ndjson')
    if any(r.get('type') != 'request' or type(r.get('status')) is not int for r in requests):
        failures.append('malformed request event')
    attempts = sum(1 for r in requests if r.get('type') == 'request')
    refused = sum(1 for r in requests if r.get('type') == 'request' and r.get('status') == 503)
    rate_503 = refused/attempts if attempts else 1.0
    upload = sum(r.get('bytes_up') or 0 for r in requests if r.get('type') == 'request')
    download = sum(r.get('bytes_down') or 0 for r in requests if r.get('type') == 'request')
    metrics = lines(directory/'metrics.ndjson')
    workers, metric_failures = evaluate_metrics(metrics, sorted(targets))
    failures += metric_failures
    if exit_code != 0:
        failures.append('loadtest did not exit 0 (exit %s)' % exit_code)
    if stopped:
        failures.append('stopped: '+str(stopped)[:300])
    complete = exit_code == 0 and not stopped and t0 is not None and ended_unix-t0 >= CAPACITY['duration_seconds']
    if not complete:
        failures.append('trial did not complete its 60-minute sustained window')
    if failed_fraction > CAPACITY['failed_or_incomplete_fraction']:
        failures.append('failed or incomplete syncs above 5 percent')
    if rate_503 > CAPACITY['http_503_fraction']:
        failures.append('HTTP 503 attempts above 10 percent')
    if not targets:
        failures.append('no worker metrics targets')
    for p in profiles.values():
        p['p95_seconds'] = percentile(p['seconds'], .95)
    return {'status':'failed' if failures else 'passed', 'failures':sorted(set(failures)), 'level':level,
            'complete':complete, 'loadtest_exit_code':exit_code, 'scheduled':len(scheduled), 'terminal':terminal,
            'exact':exact, 'unterminated':len(unterminated), 'failed_or_incomplete_fraction':failed_fraction,
            'request_attempts':attempts, 'http_503':refused, 'http_503_fraction':rate_503,
            'payload_bytes':{'up':upload, 'down':download}, 'window_started_unix':t0,
            'exact_in_window':exact_in_window,
            'sustained_exact_per_second':exact_in_window/CAPACITY['duration_seconds'] if complete else None,
            'profiles':profiles, 'workers':workers}


def capacity_decision(trials):
    """Level decisions and the supported-capacity recommendation.

    `trials` are reconciled trial results for one transaction. Ordinary p95s
    pool exact observations across a level's passing complete trials and need
    at least 100 per profile; fewer means extend, never accept. Heavy attempts
    count toward failure fractions but carry no latency target.
    """
    levels = {}
    for level in LEVELS:
        mine = sorted((t for t in trials if t['level'] == level), key=lambda t: t['trial'])
        passing = [t for t in mine if t['status'] == 'passed']
        profiles = {}
        for name, target in P95_TARGETS.items():
            if name not in COMPOSITION[level]:
                continue
            seconds = [s for t in passing for s in t['profiles'][name]['seconds']]
            p95 = percentile(seconds, .95)
            profiles[name] = {'observations':len(seconds), 'p95_seconds':p95, 'target_seconds':target,
                              'status':'insufficient' if len(seconds) < CAPACITY['minimum_observations'] else
                                       'passed' if p95 < target else 'failed'}
        observed = [t['profiles'].get(HEAVY, {}) for t in mine]
        heavy = {'terminal':sum(h.get('terminal', 0) for h in observed), 'exact':sum(h.get('exact', 0) for h in observed),
                 'timed_out':sum(h.get('outcomes', {}).get('timed_out', 0) for h in observed)}
        failed = [t['trial'] for t in mine if t['status'] != 'passed']
        throughput = [t['sustained_exact_per_second'] for t in passing]
        if failed:
            status = 'failed'
        elif len(passing) < REQUIRED_TRIALS:
            status = 'incomplete'
        elif any(p['status'] == 'failed' for p in profiles.values()):
            status = 'failed'
        elif any(p['status'] == 'insufficient' for p in profiles.values()):
            status = 'extend' if len(mine) < MAX_TRIALS else 'failed'
        else:
            status = 'passed'
        levels[level] = {'status':status, 'trials':[t['trial'] for t in mine], 'failed_trials':failed,
                         'profiles':profiles, 'heavy':heavy,
                         'sustainable_exact_per_second':min(throughput) if throughput and status == 'passed' else None}
    passed = [l for l in LEVELS if levels[l]['status'] == 'passed']
    best = max((levels[l]['sustainable_exact_per_second'] for l in passed), default=None)
    return {'levels':levels,
            'supported_exact_per_second':best*CAPACITY['supported_fraction'] if best is not None else None,
            'qualified':False, 'missing_assurance':MISSING['all']+MISSING['capacity']}


def escalation(trials, level, trial):
    """Refuse a trial that skips a required level or an unjustified extension."""
    decision = capacity_decision(trials)['levels']
    index = LEVELS.index(level)
    require(index == 0 or decision[LEVELS[index-1]]['status'] == 'passed', 'capacity escalation requires the previous level to pass')
    numbers = sorted(t['trial'] for t in trials if t['level'] == level)
    require(numbers == list(range(1, trial)), 'capacity trials must be sequential and complete')
    require(decision[level]['status'] not in ('failed', 'passed'), 'level decision is final; retain it')
    require(trial <= REQUIRED_TRIALS or decision[level]['status'] == 'extend', 'extension only for insufficient observations')


def verify_rollback_redeploy(original, redeploy, latest, recipe_sha256):
    """A real reviewed rollback followed by redeployment of the same recipe."""
    def passed(events, group, names):
        chosen = [e for e in events if e.get('group') == group]
        require([e.get('name') for e in chosen] == list(names) and
                all(e.get('status') == 'passed' and e.get('exit_code') == 0 for e in chosen), group+' phases did not all pass')
        return chosen
    for record in (original, redeploy):
        require(record.get('journal_version') == 1 and record.get('recipe_sha256') == recipe_sha256 and
                digest(record.get('recipe')) == recipe_sha256, 'rollback/redeploy did not use the reviewed recipe')
        recipe = record['recipe']
        require([c['timeout'] for c in recipe['steps']] == [FORWARD_TIMEOUT]*len(recipe['steps']) and
                {c['name']:c['timeout'] for c in recipe['rollback']} == ROLLBACK_BUDGET and
                [c['name'] for c in recipe['rollback']] == list(ROLLBACK_BUDGET), 'reviewed phase budgets changed')
    forward = [c['name'] for c in original['recipe']['steps']]
    require(original.get('status') == 'rolled-back' and 'v10_reconciliation' not in original and
            not original.get('recovery_programs'), 'original transaction was not rolled back by the reviewed recipe')
    passed(original['events'], 'steps', forward)
    rollback = passed(original['events'], 'rollback', ROLLBACK_BUDGET)
    require(len(original['events']) == len(forward)+len(rollback), 'unexpected original journal events')
    for event in rollback:
        require(event['seconds'] <= ROLLBACK_BUDGET[event['name']], 'rollback phase exceeded its budget')
    rollback_started = rollback[0]['started']
    rollback_ended = rollback[-1]['started']+rollback[-1]['seconds']
    require(rollback_ended-rollback_started <= sum(ROLLBACK_BUDGET.values()), 'rollback exceeded its 740-second budget')
    require(redeploy.get('status') == 'committed' and redeploy.get('created', 0) >= rollback_ended and
            latest == redeploy.get('id'), 'redeploy is not the latest committed transaction after rollback')
    steps = passed(redeploy['events'], 'steps', forward)
    require(len(redeploy['events']) == len(forward), 'redeploy recorded rollback events')
    return {'rolled_back_transaction':original['id'], 'redeploy_transaction':redeploy['id'],
            'rollback_started_unix':rollback_started, 'rollback_seconds':rollback_ended-rollback_started,
            'phases':{e['name']:e['seconds'] for e in rollback}, 'redeploy_created_unix':redeploy['created'],
            'redeploy_committed_unix':steps[-1]['started']+steps[-1]['seconds']}


def retained_request(sha):
    """The exact request an owner launched with, for status and reconcile."""
    require(HEX.fullmatch(sha or ''), 'invalid qualification request checksum')
    path = ROOT/sha/'request.json'
    require(not path.is_symlink(), 'retained request cannot be a symlink')
    with path.open('rb') as stream:
        return read_request(stream, sha)


def reconciled_trials(transaction):
    """Reconciled capacity trials of one transaction, failures included."""
    results = []
    if not OWNERS.exists():
        return results
    for path in sorted(OWNERS.glob('*.json')):
        if path.name == 'latest.json' or path.is_symlink():
            continue
        record = json.loads(path.read_bytes())
        request = record.get('request') if isinstance(record, dict) else None
        if (record.get('kind') == 'deployed-qualification' and record.get('status') == 'reconciled' and
                isinstance(request, dict) and request.get('kind') == 'capacity' and request.get('transaction') == transaction):
            result = json.loads((ROOT/record['request_sha256']/'result.json').read_bytes())
            # Interrupted or stopped trials count as failed attempts; they are never dropped.
            value = result.get('capacity') or {'level':request['level'], 'profiles':{}}
            results.append({**value, 'level':request['level'], 'trial':request['trial'],
                            'status':'passed' if result.get('status') == 'passed' else 'failed'})
    return results


def summary_report(transaction):
    require(isinstance(transaction, str) and TXN.fullmatch(transaction), 'invalid transaction identifier')
    trials = reconciled_trials(transaction)
    return {'transaction':transaction, 'trials':[{k:t.get(k) for k in ('level', 'trial', 'status', 'sustained_exact_per_second')}
            for t in trials], **capacity_decision(trials)}


# --- schema journal and product binding --------------------------------------

def load_record(identifier):
    require(TXN.fullmatch(identifier), 'invalid transaction identifier')
    path = SCHEMA/(identifier+'.json')
    require(not path.is_symlink() and path.stat().st_size <= 4 << 20, 'invalid schema journal')
    return json.loads(path.read_bytes())


def latest_transaction():
    pointer = SCHEMA/schema_fence.SCHEMA_POINTER
    require(pointer.is_file() and not pointer.is_symlink(), 'no schema transaction recorded')
    return json.loads(pointer.read_bytes())['id']


def spec_input(recipe):
    argv = recipe['preflight'][0]['argv']
    require(argv.count('--spec') == 1 and argv.count('--spec-sha256') == 1 and 'schema-product-preflight' in argv,
            'transaction recipe is not a reviewed product recipe')
    return {'path':argv[argv.index('--spec')+1], 'sha256':argv[argv.index('--spec-sha256')+1]}


def committed(record, recipe_sha256):
    require(record.get('journal_version') == 1 and record.get('status') == 'committed' and
            record.get('recipe_sha256') == recipe_sha256 and digest(record.get('recipe')) == recipe_sha256,
            'qualification requires the exact committed product transaction')
    forward = [c['name'] for c in record['recipe']['steps']]
    events = record.get('events', [])
    require([e.get('name') for e in events] == forward and all(e.get('status') == 'passed' for e in events),
            'committed transaction journal is incomplete')
    return record


def candidate_spec(record):
    P = product()
    reference = spec_input(record['recipe'])
    spec = P.validate(P.H.load(P.checked(reference)))
    require(spec['version'] == 2 and spec['candidate_sha'] == C.SOURCE_SHA, 'deployment is not the changed-native candidate')
    require(P.ROLLBACK_TIMEOUTS == ROLLBACK_BUDGET, 'reviewed rollback budgets changed')
    return reference, spec


def artifact(name):
    path = C.path(name)
    C.no_links(path)
    require(path.is_file() and checksum(path) == C.ARTIFACTS[name], 'candidate artifact identity changed: '+name)
    return path


# --- live observation ---------------------------------------------------------

def fetch(url, timeout):
    """Bounded raw GET: parsed body, raw bytes and the served map identity header."""
    request = urllib.request.Request(url, headers={'Cache-Control':'no-cache'})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read(MAX_REPLY+1)
        header = response.headers.get('x-shard-map-sha256')
    require(len(raw) <= MAX_REPLY, 'observation exceeds bound')
    return json.loads(raw), raw, header


def fetch_json(url, timeout=5):
    return fetch(url, timeout)[0]


def node_raw(method, params, timeout=3):
    cookie = COOKIE.read_text().strip()
    body = json.dumps({'jsonrpc':'2.0', 'id':method, 'method':method, 'params':params}).encode()
    request = urllib.request.Request(NODE, body, {'Content-Type':'application/json',
                                                  'Authorization':'Basic '+base64.b64encode(cookie.encode()).decode()})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read(65537)
    require(len(raw) <= 65536, 'node RPC response exceeds bound')
    result = json.loads(raw)
    require(result.get('error') is None, 'node RPC failed')
    return result['result'], raw


def node(method, params, timeout=3):
    return node_raw(method, params, timeout)[0]


def tail(url, timeout=5):
    shards = fetch_json(url, timeout)['shards']
    require(isinstance(shards, list) and shards, 'empty serving map')
    return shards[-1]


def local_resources(commands_=None):
    commands_ = commands_ or commands()
    memory = {line.split(':', 1)[0]:int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()
              if line.startswith(('MemTotal:', 'MemAvailable:'))}
    disks = {}
    for path in (Path('/srv/transparent-activity'), Path('/srv/zakura'), Path('/')):
        if path.exists():
            value = os.statvfs(path)
            disks[str(path)] = value.f_bavail/value.f_blocks
    units = {}
    for unit in COORDINATOR_UNITS:
        state = commands_.state(unit)
        units[unit] = {k:state.get(k) for k in ('ActiveState', 'MainPID', 'NRestarts')}
        group = state.get('ControlGroup', '')
        events = Path('/sys/fs/cgroup')/group.lstrip('/')/'memory.events'
        if group and events.is_file():
            counts = dict(line.split() for line in events.read_text().splitlines())
            units[unit].update(oom=int(counts.get('oom', 0)), oom_kill=int(counts.get('oom_kill', 0)))
    return {'unix':time.time(), 'memory_available':memory['MemAvailable']/memory['MemTotal'], 'disk_available':disks, 'units':units}


def floors(sample):
    """Headroom floor only; restart/OOM comparison is relative to a baseline."""
    require(sample['memory_available'] >= HEADROOM and all(v >= HEADROOM for v in sample['disk_available'].values()),
            'memory or disk headroom below 20 percent')


def unit_changes(before, after, allowed=()):
    """Unexpected restarts or OOM between two resource samples."""
    changes = []
    for unit, old in before['units'].items():
        new = after['units'].get(unit, {})
        if unit in allowed:
            continue
        for key in ('MainPID', 'NRestarts', 'oom', 'oom_kill'):
            if old.get(key) != new.get(key):
                changes.append('%s %s changed' % (unit, key))
    return changes


def quality_stopped(commands_=None):
    commands_ = commands_ or commands()
    state = commands_.state(H.QUALITY)
    require(state.get('MainPID') == '0' and state.get('ActiveState') in ('inactive', 'failed') and
            commands_.empty_cgroup(state), 'quality supervisor must remain stopped')


def rollback_identity(transaction):
    """The retained original rollback baseline of one transaction, verified read-only."""
    root = ROLLBACK_ROOT/transaction
    record = H.B.verify(root)
    return {'root':str(root), 'complete_sha256':checksum(root/'complete.json'), 'plan_sha256':record['plan_sha256']}


# --- all-host owner reconciliation ----------------------------------------------

def owner_namespaces():
    """Latest owner record of every mutation namespace on this host."""
    found = {}
    pointer = schema_fence.HOST_ACTIONS/'latest.json'
    if pointer.exists():
        value = json.loads(pointer.read_bytes())
        found['host-actions'] = schema_fence.HOST_ACTIONS/value['transaction']/(value['request_id']+'.json')
    for name, root in (('input-staging', schema_fence.INPUT_STAGING), ('qualification-actions', REMOTE)):
        pointer = root/'latest.json'
        if pointer.exists():
            found[name] = root/(json.loads(pointer.read_bytes())['request_sha256']+'.json')
    return found


def owner_processes(record):
    """Recorded owner and child identities, plus bare PIDs with their start time."""
    identities, bare = [], []
    for key in ('owner', 'launcher'):
        if isinstance(record.get(key), dict):
            identities.append(record[key])
    for child in record.get('children', []):
        if isinstance(child, dict) and isinstance(child.get('identity'), dict):
            identities.append(child['identity'])
    if type(record.get('pid')) is int and number(record.get('started_unix')):
        bare.append((record['pid'], record['started_unix']))
    return identities, bare


def owner_findings(commands_, *, own_unit=None, own_request=None, skip_input=None):
    """Raw quiescence evidence of every owner namespace and transient owner unit.

    The shared fence proves each latest owner reached a terminal status. That is
    not process quiescence, so every recorded identity must also be dead, a bare
    recorded PID must not name a process that started before its record, and
    every transient owner unit must have no main PID and an empty cgroup. Only
    this qualification's own record and unit are exempt, and only on the owner.
    """
    schema_fence.local_schema_fence(skip_input=skip_input)
    findings = {'namespaces':{}, 'units':{}, 'live':[]}
    for name, path in owner_namespaces().items():
        require(not path.is_symlink() and path.stat().st_size <= 4 << 20, 'invalid owner record: '+name)
        record = json.loads(path.read_bytes())
        entry = {'path':str(path), 'status':record.get('status'), 'phase':record.get('phase'), 'live':[]}
        if not (own_request is not None and record.get('request_sha256') == own_request):
            identities, bare = owner_processes(record)
            for identity in identities:
                if alive(identity):
                    entry['live'].append(identity)
            for pid, start in bare:
                try:
                    if stat_fields(pid)[0] != 'Z' and started_unix(pid) <= start+2:
                        entry['live'].append({'pid':pid, 'started_unix':start})
                except (OSError, ValueError, IndexError):
                    pass
        findings['namespaces'][name] = entry
        findings['live'] += [(name, item) for item in entry['live']]
    listing = commands_.run(['systemctl', 'list-units', '--all', '--plain', '--no-legend', '--no-pager', '--full',
                             'transparent-activity-*', 'transparent-full-burst-*'], timeout=10).decode()
    for line in listing.splitlines():
        match = OWNER_UNIT.search(line)
        if not match or match.group(0) == (own_unit or '')+'.service':
            continue
        unit = match.group(0)
        state = commands_.state(unit)
        idle = (state.get('MainPID') in ('0', '', None) and state.get('ActiveState') not in ('activating', 'deactivating', 'reloading')
                and commands_.empty_cgroup(state))
        findings['units'][unit] = {k:state.get(k) for k in ('ActiveState', 'SubState', 'MainPID', 'Result', 'ControlGroup')}
        if not idle:
            findings['live'].append((unit, findings['units'][unit]))
    require(not findings['live'], 'owner processes or units are not quiescent: %s' % [n for n, _ in findings['live']][:8])
    return findings


# --- one restorable unit effect --------------------------------------------------

def unit_identity(commands_, unit):
    """Unit state and, when running, its exact main process and executable."""
    state = commands_.state(unit)
    value = {k:state.get(k) for k in ('ActiveState', 'SubState', 'MainPID', 'NRestarts', 'FragmentPath', 'DropInPaths', 'ControlGroup')}
    pid = int(state.get('MainPID') or 0)
    if pid:
        value['process'] = process_identity(pid)
        value['executable_sha256'] = executable_sha256(pid)
    return value


def same_service(original, current):
    """The running unit is the owned pre-fault service definition and executable."""
    require(current.get('ActiveState') == 'active' and current.get('process') and
            all(current.get(k) == original.get(k) for k in ('FragmentPath', 'DropInPaths', 'ControlGroup', 'executable_sha256')),
            'running service differs from the owned pre-fault service; foreign state remains fenced')


class UnitEffect:
    """Stop, start or restart one fixed unit, phase by phase, and restore it.

    The exact pre-fault service is recorded before any effect and every phase
    is saved before the call that enters it, so an interrupted owner leaves a
    durable account. Restoration starts only this owned unit, only after an owned
    stop, at most RESTORE_LIMIT times; an unexpected or foreign state stays fenced.
    """
    def __init__(self, unit, record, save, commands_, ready=None):
        self.unit, self.record, self.save, self.commands, self.ready = unit, record, save, commands_, ready

    def capture(self):
        original = unit_identity(self.commands, self.unit)
        require(original['ActiveState'] == 'active' and original.get('process'), 'pre-fault unit is not the active service')
        self.record.update(unit=self.unit, original=original, phase='intent', restores=[])
        return original

    def phase(self, name):
        self.record['phase'] = name
        self.record.setdefault('phases', []).append({'phase':name, 'unix':time.time()})
        self.save()

    def systemctl(self, action, deadline):
        self.commands.run(['systemctl', '--no-block', action, self.unit], timeout=deadline.timeout(15))

    def settle(self, deadline, stopped):
        while True:
            state = self.commands.state(self.unit)
            if stopped:
                if state.get('ActiveState') in ('inactive', 'failed') and state.get('MainPID') in ('0', '') and \
                        self.commands.empty_cgroup(state):
                    return state
            elif state.get('ActiveState') == 'active' and state.get('MainPID') not in ('0', '', None):
                return state
            elif state.get('ActiveState') == 'failed':
                raise ValueError('unit failed while starting: '+self.unit)
            deadline.sleep(.5)

    def stop(self, deadline):
        self.phase('stopping')
        self.systemctl('stop', deadline)
        self.settle(deadline.child(UNIT_SECONDS['stop']), True)
        self.phase('stopped')

    def start(self, deadline):
        self.phase('starting')
        self.systemctl('start', deadline)
        self.settle(deadline.child(UNIT_SECONDS['start']), False)
        return self.verify(deadline, changed=True)

    def restart(self, deadline):
        self.phase('restarting')
        self.systemctl('restart', deadline)
        bound = deadline.child(UNIT_SECONDS['stop']+UNIT_SECONDS['start'])
        while True:
            state = self.commands.state(self.unit)
            pid = int(state.get('MainPID') or 0)
            if state.get('ActiveState') == 'active' and pid and process_identity(pid) != self.record['original']['process']:
                break
            require(state.get('ActiveState') != 'failed', 'unit failed while restarting: '+self.unit)
            bound.sleep(.5)
        return self.verify(deadline, changed=True)

    def settled(self, deadline):
        """The unit's state once no transition or queued job remains."""
        while True:
            current = unit_identity(self.commands, self.unit)
            jobs = self.commands.run(['systemctl', 'list-jobs', '--no-legend', '--plain', '--no-pager', self.unit],
                                     timeout=deadline.timeout(10)).decode()
            if current['ActiveState'] not in ('activating', 'deactivating', 'reloading') and self.unit not in jobs:
                return current
            deadline.sleep(.5)

    def verify(self, deadline, changed, ready=False):
        current = unit_identity(self.commands, self.unit)
        same_service(self.record['original'], current)
        require(not changed or current['process'] != self.record['original']['process'], 'unit did not restart')
        if ready and self.ready:
            current['ready'] = self.ready(deadline, current)
        self.record['after'] = current
        self.phase('verified')
        return current

    def restore(self, deadline):
        """Prove the owned service is back, starting it only after an owned stop."""
        attempts = self.record.setdefault('restores', [])
        require(len(attempts) < RESTORE_LIMIT, 'restoration attempts exhausted; the unit stays fenced for review')
        entry = {'started_unix':time.time(), 'phase':self.record.get('phase')}
        attempts.append(entry)
        self.save()
        current = self.settled(deadline)
        phase = self.record.get('phase')
        unchanged = current.get('process') == self.record['original']['process']
        if current['ActiveState'] == 'active':
            # An owned stop/restart that never took effect leaves the original process.
            require(not unchanged or phase in ('intent', 'stopping', 'restarting'),
                    'unit state contradicts its recorded phase; foreign state remains fenced')
            require(unchanged or phase != 'intent', 'unit changed without an owned effect; foreign state remains fenced')
        else:
            require(current['ActiveState'] in ('inactive', 'failed'), 'unknown unit state remains fenced')
            require(self.record.get('phase') not in ('intent', None), 'unit stopped without an owned effect; foreign state remains fenced')
            entry['start_intent_unix'] = time.time()
            self.save()  # owned start intent before the effect
            self.systemctl('start', deadline)
            self.settle(deadline.child(UNIT_SECONDS['start']), False)
            unchanged = False
        current = self.verify(deadline, changed=not unchanged, ready=True)
        entry.update(result='restored', ended_unix=time.time())
        self.record.update(status='restored', restored_unix=time.time())
        self.save()
        return current


# --- remote fixed actor --------------------------------------------------------

def remote_validate(request):
    require(isinstance(request, dict) and set(request) == {'version', 'source_sha', 'qualification_sha256', 'host',
            'role', 'machine_id', 'coordinator_machine_id', 'transaction', 'operation'}, 'invalid remote qualification request')
    require(request['version'] == 1 and type(request['version']) is int and
            isinstance(request['source_sha'], str) and re.fullmatch('[0-9a-f]{40}', request['source_sha']) and
            isinstance(request['qualification_sha256'], str) and HEX.fullmatch(request['qualification_sha256']) and
            isinstance(request['host'], str) and NAME.fullmatch(request['host']) and
            isinstance(request['transaction'], str) and TXN.fullmatch(request['transaction']) and
            all(isinstance(request[k], str) and re.fullmatch('[0-9a-f]{32}', request[k]) for k in ('machine_id', 'coordinator_machine_id')) and
            request['machine_id'] != request['coordinator_machine_id'], 'invalid remote qualification identity')
    require(request['role'] in REMOTE_OPERATIONS and request['operation'] in REMOTE_OPERATIONS[request['role']],
            'unsupported remote qualification operation')
    return request


def remote_command(request, action, sudo=False):
    return (['sudo', '-n', '--'] if sudo else []) + [
        '/usr/bin/python3', '-B', str(SOURCES/request['source_sha']/'ops/scripts/wallet-pir-deploy.py'),
        'schema-qualify-remote', '--action', action, '--request-sha256', digest(request)]


def control_summary(status):
    active = status.get('active') if isinstance(status.get('active'), dict) else {}
    return {'active_map_sha256':active.get('map_sha256'), 'preparing':status.get('preparing'),
            'candidate':status.get('candidate'), 'warm':status.get('warm'), 'invalidated':status.get('invalidated')}


class RemoteActor:
    """The remote half: read-only owner/identity probes or one restorable unit effect.

    An effect runs to its own monotonic bound regardless of the SSH channel:
    hangup and broken pipes are ignored, and termination enters bounded
    restoration. Reconcile restores the owned service or records that no
    request ever acted, so a late delivery can never act afterwards.
    """
    def __init__(self, request, commands_=None, lock_factory=None):
        self.request = remote_validate(request)
        self.sha = digest(request)
        self.unit = REMOTE_UNITS[request['role']]
        self.path = REMOTE/(self.sha+'.json')
        self.commands = commands_ or commands()
        self.lock_factory = lock_factory or (lambda: ProductionLock({'type':'pinned_host', 'machine_id':self.request['machine_id']}))

    def identity(self):
        require(os.geteuid() == 0 and Path('/etc/machine-id').read_text().strip() == self.request['machine_id'],
                'remote qualification requires the pinned root host')
        root = SOURCES/self.request['source_sha']
        require(Path(__file__).resolve().parents[3] == root, 'remote qualification requires immutable reviewed source')
        receipt = json.loads((STAGING/(self.request['source_sha']+'.json')).read_bytes())
        S.verify_receipt(receipt, root, self.request['source_sha'], receipt['archive_sha256'])

    def resources(self):
        return {'unit':self.unit, **self.commands.service_resources(self.unit, REMOTE_DISK[self.request['role']])}

    def terminal(self):
        pointer = REMOTE/'latest.json'
        if pointer.exists():
            latest = json.loads(pointer.read_bytes())['request_sha256']
            require(json.loads((REMOTE/(latest+'.json')).read_bytes()).get('status') in ('passed', 'restored', 'refused'),
                    'unfinished or unrestored remote qualification action; reconcile before another')

    def status(self):
        if not self.path.exists():
            return {'status':'absent', 'request_sha256':self.sha}
        require(not self.path.is_symlink(), 'remote action record cannot be a symlink')
        record = json.loads(self.path.read_bytes())
        require(record.get('request_sha256') == self.sha, 'remote action identity changed')
        return record

    def save(self, record):
        durable.atomic_json(self.path, record, mode=0o600)

    def ready(self, deadline, current):
        """Worker readiness on its fixed local endpoint, from the restored executable."""
        if self.request['role'] != 'worker':
            return None
        while True:
            try:
                ready = self.commands.cache_observation()['ready']
                if ready.get('ready') is True:
                    require(ready.get('binary_sha256') == current['executable_sha256'], 'restored worker executable differs')
                    return ready
            except (OSError, ValueError, urllib.error.URLError):
                pass
            deadline.sleep(2)

    def run(self, action):
        self.identity()
        if action == 'status':
            return self.status()
        if action == 'reconcile':
            return self.reconcile()
        if action == 'probe':
            require(self.request['operation'] not in EFFECTS, 'read-only operation required')
            return self.read_only()
        require(action == 'act' and self.request['operation'] in EFFECTS, 'unsupported remote action')
        return self.act()

    def read_only(self):
        operation = self.request['operation']
        reply = {'status':'passed', 'request_sha256':self.sha, 'host':self.request['host'], 'operation':operation}
        if operation in ('probe', 'identity'):
            deadline = Deadline(REMOTE_BOUND[operation])
            self.commands.deadline = deadline
            # Taking this host's lock proves no lock-holding owner or descendant lives here.
            try:
                with self.lock_factory() as lock:
                    lock.verify()
                    reply['findings'] = owner_findings(self.commands)
                    self.terminal()
                    reply['resources'] = self.resources()
                    if operation == 'identity':
                        reply['rollback'] = rollback_identity(self.request['transaction'])
            except BlockingIOError:
                raise ValueError('host production lock is held by another owner') from None
            return reply
        deadline = Deadline(REMOTE_BOUND[operation])
        self.commands.deadline = deadline
        if operation == 'control-status':
            reply['control'] = control_summary(self.commands.control())
            reply['unix'] = time.time()
            return reply
        while True:  # await-preparation
            status = control_summary(self.commands.control())
            if isinstance(status['preparing'], dict):
                return dict(reply, observed='preparing', control=status, unix=time.time())
            if deadline.remaining() < 1:
                return dict(reply, status='not-observed', control=status, unix=time.time())
            deadline.sleep(.2)

    def act(self):
        operation = self.request['operation']
        deadline = Deadline(REMOTE_BOUND[operation])
        self.commands.deadline = deadline
        try:
            lock = self.lock_factory().__enter__()
        except BlockingIOError:
            raise ValueError('host production lock is held by another owner') from None
        previous = {s:signal.getsignal(s) for s in (signal.SIGHUP, signal.SIGPIPE, signal.SIGTERM)}
        try:
            lock.verify()
            owner_findings(self.commands)
            self.terminal()
            require(not self.path.exists(), 'remote action already owned; inspect status, never replay')
            REMOTE.mkdir(parents=True, exist_ok=True, mode=0o700)
            record = {'status':'running', 'request':self.request, 'request_sha256':self.sha,
                      'owner':process_identity(os.getpid()), 'before':self.resources(), 'started_unix':time.time(),
                      'bound_seconds':REMOTE_BOUND[operation]}
            effect = UnitEffect(self.unit, record, lambda: self.save(record), self.commands, self.ready)
            effect.capture()
            self.save(record)  # durable intent with the exact pre-fault service
            durable.atomic_json(REMOTE/'latest.json', {'request_sha256':self.sha}, mode=0o600)
            # The SSH channel is not the owner: finish or restore within the bound.
            signal.signal(signal.SIGHUP, signal.SIG_IGN)
            signal.signal(signal.SIGPIPE, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, interrupted)
            try:
                if operation == 'restart':
                    effect.restart(deadline)
                else:
                    effect.stop(deadline)
                    lock.verify()
                    hold = time.monotonic()+LOSS_HOLD_SECONDS
                    while time.monotonic() < hold:
                        deadline.sleep(min(1, hold-time.monotonic()))
                        lock.verify()
                    effect.start(deadline)
                record.update(status='passed', after_resources=self.resources(), ended_unix=time.time())
            except Exception as error:
                record.update(status='failed', error_type=type(error).__name__, error=str(error)[:500], ended_unix=time.time())
                self.save(record)
                try:
                    effect.restore(Deadline(RESTORE_SECONDS))
                except Exception as again:
                    record.update(status='failed', restore_error=str(again)[:500])
            finally:
                self.save(record)
            return record
        finally:
            for number_, handler in previous.items():
                signal.signal(number_, handler)
            lock.__exit__(None, None, None)

    def reconcile(self):
        deadline = Deadline(RECONCILE_SECONDS)
        self.commands.deadline = deadline
        try:
            lock = self.lock_factory().__enter__()
        except BlockingIOError:
            raise ValueError('remote action owner still holds the host lock; reconcile again after it ends') from None
        try:
            lock.verify()
            record = self.status()
            if record['status'] == 'absent':
                # No intent was ever recorded; refuse this identity for good, so a
                # delayed delivery cannot act after reconciliation.
                REMOTE.mkdir(parents=True, exist_ok=True, mode=0o700)
                record = {'status':'refused', 'request':self.request, 'request_sha256':self.sha, 'phase':None,
                          'refused_unix':time.time(), 'reason':'reconciled before any intent; never acts'}
                self.save(record)
                return record
            if record['status'] in ('passed', 'restored', 'refused'):
                return record
            require(not alive(record['owner']), 'remote action owner is still alive')
            effect = UnitEffect(self.unit, record, lambda: self.save(record), self.commands, self.ready)
            try:
                effect.restore(deadline)
            finally:
                self.save(record)
            return record
        finally:
            lock.__exit__(None, None, None)


def remote_run(request, action):
    return RemoteActor(request).run(action)


class RemoteMonitor:
    """Remote owner/resource probes off the observation path, so SSH never stalls it."""
    def __init__(self, owner):
        self.owner, self.stopping, self.mutex = owner, threading.Event(), threading.Lock()
        self.results, self.error, self.last = [], None, time.monotonic()
        self.thread = threading.Thread(target=self.loop, daemon=True)

    def loop(self):
        while not self.stopping.is_set():
            try:
                value = self.owner.reconcile_hosts('probe')
                with self.mutex:
                    self.results.append((time.time(), value))
                    self.last = time.monotonic()
            except BaseException as error:  # noqa: B902 - retained and raised by the owner loop
                with self.mutex:
                    self.error = '%s: %s' % (type(error).__name__, str(error)[:300])
                return
            self.stopping.wait(REMOTE_HEALTH_INTERVAL)

    def take(self):
        with self.mutex:
            results, self.results = self.results, []
            return results, self.error, self.last

    def __enter__(self):
        self.owner.monitor = self
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.owner.monitor = None
        self.stopping.set()
        self.thread.join(REMOTE_BOUND['probe']+SSH_MARGIN+5)


# --- coordinator owner ----------------------------------------------------------

class Qualification:
    def __init__(self, inventory, request, request_sha256):
        self.inventory = inventory
        self.request = validate(request)
        self.sha = digest(request)
        require(self.sha == request_sha256, 'qualification request checksum differs')
        self.plan_value = plan(request)
        self.plan_sha = digest(self.plan_value)
        self.owner_path = OWNERS/(self.sha+'.json')
        self.directory = ROOT/self.sha
        self.unit = unit_name(self.sha)
        self.deadline = Deadline(RECONCILE_SECONDS+600)
        self.monitor = None

    # Identity and preflight -----------------------------------------------------
    def coordinator(self):
        require(self.inventory.lock.get('type') == 'pinned_host' and os.geteuid() == 0 and
                ProductionLock.MACHINE_ID.read_text().strip() == self.inventory.lock['machine_id'],
                'qualification must run on the pinned root coordinator')
        source = SOURCES/self.request['source_sha']
        require(Path(__file__).resolve() == source/'transparent/ops/lib/activity_deployed_qualification.py',
                'qualification requires the immutable staged operation source')
        receipt = json.loads((STAGING/(self.request['source_sha']+'.json')).read_bytes())
        S.verify_receipt(receipt, source, self.request['source_sha'], receipt['archive_sha256'])

    def deployment(self):
        """Exact committed candidate transaction and its reviewed product inputs."""
        latest = latest_transaction()
        if self.request.get('fault') == 'rollback-redeploy':
            original = load_record(self.request['rolled_back_transaction'])
            redeploy = load_record(self.request['transaction'])
            self.rollback_evidence = verify_rollback_redeploy(original, redeploy, latest, self.request['recipe_sha256'])
        require(latest == self.request['transaction'], 'qualification must bind the latest schema transaction')
        record = committed(load_record(self.request['transaction']), self.request['recipe_sha256'])
        reference, spec = candidate_spec(record)
        P = product()
        inventory = P.checked(spec['inventory'])
        require(self.inventory.lock == P.descriptors.load_inventory(inventory).lock,
                'wrapper inventory differs from the product coordinator inventory')
        for value in (spec['load']['fixture'], spec['load']['policy'], spec['assignment']):
            P.checked(value)
        policy = json.loads(Path(spec['load']['policy']['path']).read_bytes())
        require(policy.get('mode') == 'observe', 'scaler policy must remain observe-only')
        recovery = spec['routing']['recovery']['v11']
        require(recovery['binary'] == str(C.path('transparent-loadtest')) and
                recovery['binary_sha256'] == C.ARTIFACTS['transparent-loadtest'] and
                checksum(recovery['sample']) == recovery['sample_sha256'], 'candidate recovery reader or sample changed')
        assignment = json.loads(Path(spec['assignment']['path']).read_bytes())
        roles = {w['id']:w for w in assignment['workers']}
        hosts = []
        for entry in spec['hosts']:
            plan_ = entry['plan']
            worker = plan_['worker']
            hosts.append({'host':entry['host'], 'role':plan_['role'], 'machine_id':plan_['machine_id'],
                          'worker':None if worker is None else {'id':worker['id'], 'role':roles[worker['id']]['role'],
                          'upstream':roles[worker['id']]['upstream'], 'binary_sha256':worker['binary_sha256'],
                          'map_sha256':worker['map_sha256']}})
        require(sum(h['role'] == 'coordinator' for h in hosts) == 1 and sum(h['role'] == 'router' for h in hosts) >= 1 and
                sum(h['worker'] is not None and h['worker']['role'] == 'recent-replica' for h in hosts) >= 2 and
                sum(h['worker'] is not None and h['worker']['role'] == 'archive-owner' for h in hosts) >= 1,
                'deployment lacks its coordinator, router, recent replicas or an archive owner')
        self.spec, self.reference, self.hosts = spec, reference, hosts
        self.ssh = P.descriptors.load_inventory(inventory)
        return {'transaction':record['id'], 'recipe_sha256':record['recipe_sha256'], 'spec':reference, 'inventory':spec['inventory'],
                'product_source_sha':spec['source_sha'], 'candidate_sha':spec['candidate_sha'],
                'publication_sha256':spec['publication_sha256'], 'fixture':spec['load']['fixture'],
                'recovery_sample':{'path':recovery['sample'], 'sha256':recovery['sample_sha256']}, 'hosts':hosts}

    def retained_hosts(self):
        """Hosts and SSH inventory from this owner's immutable preflight receipt."""
        observed = json.loads((self.directory/'preflight.json').read_bytes())
        self.hosts = observed['deployment']['hosts']
        self.ssh = product().descriptors.load_inventory(product().checked(observed['deployment']['inventory']))

    def remote(self, host, operation, action):
        """One closed remote call; a lost or unstructured reply leaves the outcome unknown."""
        entry = next(h for h in self.hosts if h['host'] == host)
        request = remote_validate({'version':1, 'source_sha':self.request['source_sha'], 'qualification_sha256':self.sha,
                                   'host':host, 'role':entry['role'], 'machine_id':entry['machine_id'],
                                   'coordinator_machine_id':self.inventory.lock['machine_id'],
                                   'transaction':self.request['transaction'], 'operation':operation})
        target = self.ssh.hosts[host]
        require(target.get('machine_id') == entry['machine_id'], 'remote machine differs from the deployed host plan')
        sudo = target.get('user', self.ssh.ssh.get('user', 'root')) != 'root'
        require(not sudo or target.get('sudo'), 'remote qualification needs a root identity')
        bound = REMOTE_BOUND['reconcile' if action == 'reconcile' else operation]+SSH_MARGIN+(RESTORE_SECONDS if action == 'act' else 0)
        timeout = self.deadline.timeout(bound) if action != 'reconcile' else bound
        # No persistent SSH masters: their lifetime would retain the inherited lock.
        ssh = SSHExecutor(self.ssh).transport(host)
        ssh = [*ssh[:-1], '-oControlMaster=no', '-oControlPath=none', ssh[-1]]
        try:
            result = subprocess.run([*ssh, shlex.join(remote_command(request, action, sudo))], input=durable.canonical(request),
                                    capture_output=True, timeout=timeout, **inherited_lock.options())
        except subprocess.TimeoutExpired:
            raise Unknown('remote qualification %s on %s timed out; outcome unknown' % (action, host)) from None
        try:
            reply = json.loads(result.stdout[:MAX_REPLY])
            require(isinstance(reply, dict) and reply.get('request_sha256') == digest(request), 'remote reply identity differs')
        except ValueError:
            raise Unknown('remote qualification %s on %s returned no structured reply (exit %d)' %
                          (action, host, result.returncode)) from None
        require(result.returncode == 0, 'remote qualification %s failed on %s' % (action, host))
        return request, reply

    def remote_hosts(self):
        return [h for h in self.hosts if h['role'] != 'coordinator']

    def reconcile_hosts(self, operation='probe'):
        """Fresh owner reconciliation of every remote pinned host, in parallel, none exempt."""
        hosts = self.remote_hosts()
        with ThreadPoolExecutor(max_workers=max(1, len(hosts))) as pool:
            futures = {h['host']:pool.submit(self.remote, h['host'], operation, 'probe') for h in hosts}
        samples = {}
        for host, future in futures.items():
            _, reply = future.result()
            resources = reply['resources']
            require(resources['memory_available']*5 >= resources['memory_total'] and
                    resources['disk_available']*5 >= resources['disk_total'], 'remote headroom below 20 percent: '+host)
            samples[host] = reply
        return samples

    def local_owners(self, *, running):
        """The coordinator's own owner reconciliation; only this owner is exempt."""
        return owner_findings(commands(self.deadline), own_unit=self.unit if running else None,
                              own_request=self.sha if running else None, skip_input=self.sha if running else None)

    def all_hosts(self, operation, *, running):
        result = {'coordinator':{'findings':self.local_owners(running=running)}, **self.reconcile_hosts(operation)}
        if operation == 'identity':
            result['coordinator']['rollback'] = rollback_identity(self.request['transaction'])
        return result

    def readiness(self):
        identities = {}
        for host in self.hosts:
            worker = host['worker']
            if worker is None:
                continue
            ready = fetch_json('http://'+worker['upstream']+'/v1/ready', timeout=self.deadline.timeout(5))
            identities[worker['id']] = {k:ready.get(k) for k in ('ready', 'binary_sha256', 'map_sha256', 'incarnation',
                                        'started_unix', 'worker_id', 'role', 'assignment_sha256', 'worker_assignment_sha256')}
            require(ready.get('ready') is True and ready.get('binary_sha256') == worker['binary_sha256'] == C.ARTIFACTS['transparent-shard-server'] and
                    ready.get('worker_id') == worker['id'] and ready.get('role') == worker['role'],
                    'worker is not ready as its assigned candidate role: '+worker['id'])
        return identities

    def public_map(self):
        """The canonical public map: its protocol digest, sealed prefix and canonical tail."""
        mapping, raw, _ = fetch(SHARD_ORIGIN+'/v1/shards', self.deadline.timeout(5))
        last = mapping['shards'][-1]
        require(node('getblockhash', [last['end_height']], self.deadline.timeout(3)) == last['terminal_block_hash'],
                'public map tail is not canonical')
        return {'sha256':transparent_map.served_sha256(mapping), 'raw_sha256':hashlib.sha256(raw).hexdigest(),
                'sealed':{str(s['shard_id']):s['manifest_digest'] for s in mapping['shards'] if s['sealed']},
                'tail':{k:last[k] for k in ('shard_id', 'end_height', 'terminal_block_hash', 'manifest_digest')}}

    def kind_preflight(self):
        request = self.request
        if request['kind'] == 'capacity':
            escalation(reconciled_trials(request['transaction']), request['level'], request['trial'])
        if request.get('fault') in FAULT_TARGET:
            entry = next((h for h in self.hosts if h['host'] == request['target']), None)
            require(entry is not None and entry['worker'] is not None and entry['worker']['role'] == FAULT_TARGET[request['fault']],
                    'fault target is not a deployed '+FAULT_TARGET[request['fault']])

    def preflight(self, *, running=False):
        self.coordinator()
        deployment = self.deployment()
        for name in ('examples/rate-query', 'transparent-loadtest'):
            artifact(name)
        quality_stopped(commands(self.deadline))
        local = local_resources(commands(self.deadline))
        floors(local)
        hosts = self.all_hosts('identity', running=running)
        ready = self.readiness()
        public = self.public_map()
        self.kind_preflight()
        if not running:
            require(self.status()['status'] == 'absent', 'qualification already owned; inspect/reconcile, never replay')
        return {'deployment':deployment, 'local':local, 'remote':{h:v for h, v in hosts.items() if h != 'coordinator'},
                'coordinator':hosts['coordinator'], 'ready':ready, 'public':public}

    # Owner lifecycle ------------------------------------------------------------
    def status(self):
        require(not self.owner_path.is_symlink(), 'qualification owner cannot be a symlink')
        if not self.owner_path.exists():
            return {'status':'absent', 'request_sha256':self.sha}
        record = json.loads(self.owner_path.read_bytes())
        require(record.get('request_sha256') == self.sha and record.get('kind') == 'deployed-qualification' and
                record.get('plan_sha256') == self.plan_sha, 'qualification owner identity differs')
        return record

    def save(self, record):
        durable.atomic_json(self.owner_path, record, mode=0o600)

    def launch(self, expected):
        require(expected == self.plan_sha, 'qualification plan checksum differs')
        with ProductionLock(self.inventory.lock) as lock:
            lock.verify()
            schema_fence.local_schema_fence()
            observed = self.preflight()
            OWNERS.mkdir(parents=True, exist_ok=True, mode=0o700)
            ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.directory.mkdir(mode=0o700)
            write_once(self.directory/'request.json', self.request)
            write_once(self.directory/'plan.json', self.plan_value)
            write_once(self.directory/'preflight.json', observed)
            record = {'kind':'deployed-qualification', 'request':summary(self.request), 'request_sha256':self.sha,
                      'plan_sha256':self.plan_sha, 'status':'launching', 'unit':self.unit,
                      'launcher':process_identity(os.getpid()), 'started_unix':time.time(), 'children':[]}
            self.save(record)
            durable.atomic_json(OWNERS/'latest.json', {'request_sha256':self.sha}, mode=0o600)
            command = ['/usr/bin/systemd-run', '--quiet', '--unit='+self.unit,
                       *('--property='+p for p in properties(self.request['kind'])),
                       '--setenv=PYTHONDONTWRITEBYTECODE=1', '/usr/bin/python3', '-B',
                       str(SOURCES/self.request['source_sha']/'ops/scripts/wallet-pir-deploy.py'),
                       '--inventory', self.spec['inventory']['path'], 'schema-qualify-exec', '--request-sha256', self.sha]
            lock.verify()
            # systemd does not inherit the lock; the owner reacquires it before effects.
            subprocess.run(command, check=True, capture_output=True, timeout=30)
        return self.report()

    def execute(self):
        os.umask(0o077)
        # The owner bound ends before RuntimeMaxSec so a clean stop can seal results.
        self.deadline = Deadline(RUNTIME[self.request['kind']]-OWNER_MARGIN)
        record = self.status()
        require(record['status'] == 'launching', 'qualification launch identity changed or owner already ran')
        handoff = Deadline(15)
        lock = ProductionLock(self.inventory.lock)
        while True:
            try:
                lock.__enter__()
                break
            except BlockingIOError:
                require(handoff.remaining() > 0, 'qualification lock handoff timed out; reconcile launch intent')
                time.sleep(.1)
        self.lock = lock
        os.environ[inherited_lock.VARIABLE] = ','.join(map(str, lock.descriptors()))
        os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
        previous = signal.signal(signal.SIGTERM, interrupted)
        result = None
        try:
            schema_fence.local_schema_fence(skip_input=self.sha)
            record.update(status='running', owner=process_identity(os.getpid()), deadline_seconds=self.deadline.remaining())
            self.save(record)
            self.record = record
            baseline = self.preflight(running=True)
            write_once(self.directory/'baseline.json', baseline)
            self.baseline = baseline
            runner = {'staged-load':self.staged_load, 'freshness':self.freshness_window,
                      'capacity':self.capacity, 'fault':self.fault}[self.request['kind']]
            result = runner()
        except BaseException as error:
            result = {'status':'failed' if isinstance(error, Exception) and not isinstance(error, Interrupted) else 'interrupted',
                      'error_type':type(error).__name__, 'error':str(error)[:500]}
            raise
        finally:
            self.stop_children()
            effect = getattr(self, 'record', {}).get('effect', {})
            pending = [a['host'] for a in getattr(self, 'record', {}).get('remote_actions', [])
                       if a.get('status') not in ('passed', 'restored', 'refused')]
            if effect.get('unit') and effect.get('status') not in ('passed', 'restored') or pending:
                result = dict(result or {}, status='failed' if (result or {}).get('status') == 'passed' else (result or {}).get('status'),
                              fenced='owned unit effect requires reconcile restoration')
            result = dict(result or {}, plan_sha256=self.plan_sha, request_sha256=self.sha, ended_unix=time.time(),
                          missing_assurance=self.plan_value['missing_assurance'], qualified=False if result is None else
                          result.get('status') == 'passed' and not self.plan_value['missing_assurance'])
            write_once(self.directory/'result.json', result)
            seal(self.directory)
            record = self.status()
            record.update(getattr(self, 'record', {}), status='finished', finished_unix=time.time())
            self.save(record)
            signal.signal(signal.SIGTERM, previous)
            lock.__exit__(None, None, None)

    def report(self):
        record = self.status()
        if record['status'] == 'absent':
            return record
        state = commands().state(self.unit) if record['status'] != 'reconciled' else None
        result_path = self.directory/'result.json'
        result = json.loads(result_path.read_bytes()) if result_path.exists() else None
        owner = record.get('owner')
        return {'request_sha256':self.sha, 'plan_sha256':self.plan_sha, 'status':record['status'],
                'outcome':record.get('outcome'), 'unit':self.unit, 'unit_state':state,
                'owner_alive':alive(owner) if owner else None, 'effect':record.get('effect'),
                'remote_actions':record.get('remote_actions', []),
                'children':[{**c, 'alive':alive(c['identity'])} if c.get('identity') else c for c in record.get('children', [])],
                'result':None if result is None else {k:result.get(k) for k in ('status', 'failures', 'qualified', 'missing_assurance', 'error', 'fenced')}}

    def reconcile(self):
        """Prove the owner, children and every owned effect have ended or been restored."""
        self.deadline = Deadline(RECONCILE_SECONDS*3)
        with ProductionLock(self.inventory.lock) as lock:
            lock.verify()
            schema_fence.local_schema_fence(skip_input=self.sha)
            require(json.loads((OWNERS/'latest.json').read_bytes()) == {'request_sha256':self.sha}, 'reconcile the latest qualification first')
            record = self.status()
            require(record['status'] in ('launching', 'running', 'finished'), 'qualification does not require reconciliation')
            commands_ = commands(self.deadline)
            state = commands_.state(self.unit)
            # The owner's cgroup holds every child, including one started before its
            # identity was saved; holding the lock proves no lock-inheriting child lives.
            require(state.get('MainPID') in ('0', None, '') and commands_.empty_cgroup(state), 'qualification owner unit still runs')
            for identity in [record.get('owner'), *(c.get('identity') for c in record.get('children', []))]:
                require(not identity or not alive(identity), 'recorded qualification process is still alive')
            members = session_members(c['identity']['session'] for c in record.get('children', []) if c.get('identity'))
            require(not members, 'owned client descendants are still running')
            findings = {'unidentified_children':[c['name'] for c in record.get('children', []) if not c.get('identity')],
                        'owner_cgroup_empty':True}
            effect = record.get('effect', {})
            if effect.get('unit') and effect.get('status') not in ('passed', 'restored'):
                restored = UnitEffect(effect['unit'], effect, lambda: self.save(record), commands_)
                restored.restore(Deadline(RECONCILE_SECONDS))
                findings['local_effect'] = effect['status']
            for action in record.get('remote_actions', []):
                if action.get('status') not in ('passed', 'restored', 'refused'):
                    self.retained_hosts()
                    _, remote = self.remote(action['host'], action['operation'], 'reconcile')
                    require(remote.get('status') in ('passed', 'restored', 'refused'),
                            'remote qualification action is not restored on '+action['host'])
                    action.update(status=remote['status'], reconciled=remote.get('status'))
                    self.save(record)
            result_path = self.directory/'result.json'
            if not result_path.exists():
                write_once(result_path, {'status':'interrupted', 'request_sha256':self.sha, 'plan_sha256':self.plan_sha,
                                         'reconciled_unix':time.time(), 'qualified':False,
                                         'missing_assurance':self.plan_value['missing_assurance']})
            if self.directory.exists():
                seal(self.directory)
            result = json.loads(result_path.read_bytes())
            record.update(status='reconciled', outcome=result['status'], reconciled_unix=time.time(), reconciliation=findings,
                          unit_state={k:state.get(k) for k in ('ActiveState', 'Result', 'ExecMainStatus', 'NRestarts')},
                          result={k:result.get(k) for k in ('status', 'qualified')})
            self.save(record)
            return self.report()

    def run(self, action, expected=None):
        if action == 'plan':
            return self.plan_value
        if action == 'status':
            return self.report()
        if action == 'reconcile':
            return self.reconcile()
        if action == 'preflight':
            # Remote probes briefly take each host lock, so preflight holds the global one.
            with ProductionLock(self.inventory.lock) as lock:
                lock.verify()
                schema_fence.local_schema_fence()
                observed = self.preflight()
            return {'plan_sha256':self.plan_sha, 'status':'ready', 'deployment':observed['deployment']['transaction'],
                    'hosts':sorted(observed['remote']), 'workers':sorted(observed['ready'])}
        require(action == 'run', 'unsupported qualification action')
        return self.launch(expected)

    # Owned children -------------------------------------------------------------
    def spawn(self, name, argv, stdout, stderr):
        """Durable intent, then a new owned session that inherits the lock."""
        self.lock.verify()
        entry = {'name':name, 'status':'starting', 'intent_unix':time.time()}
        self.record['children'].append(entry)
        self.save(self.record)
        process = subprocess.Popen([str(a) for a in argv], stdout=stdout, stderr=stderr, start_new_session=True,
                                   pass_fds=self.lock.descriptors(), env=dict(os.environ))
        entry.update(status='running', identity=process_identity(process.pid))
        self.save(self.record)
        self.processes = getattr(self, 'processes', [])+[(entry, process)]
        return process

    def stop_children(self):
        for entry, process in getattr(self, 'processes', []):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=CHILD_STOP_SECONDS*2/3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=CHILD_STOP_SECONDS/3)
            entry.update(status='exited', exit_code=process.returncode)
        if getattr(self, 'processes', None):
            self.save(self.record)

    def quiescent_children(self):
        """No owned child or session member survives before an effect."""
        for entry in self.record.get('children', []):
            require(entry.get('status') == 'exited' and not alive(entry.get('identity') or {}),
                    'owned child is still running: '+entry['name'])
        require(not session_members(c['identity']['session'] for c in self.record.get('children', []) if c.get('identity')),
                'owned client descendants are still running')

    # Health -----------------------------------------------------------------------
    def health(self, stream, allowed=()):
        """One local sample plus any finished remote samples; violations raise."""
        self.lock.verify()
        commands_ = commands(self.deadline)
        sample = local_resources(commands_)
        stream.write(json.dumps({'local':sample})+'\n')
        stream.flush()
        floors(sample)
        quality_stopped(commands_)
        changes = unit_changes(self.baseline['local'], sample, allowed)
        require(not changes, 'unexpected restart or OOM: '+', '.join(changes))
        if self.monitor is not None:
            results, error, last = self.monitor.take()
            for unix, remote in results:
                stream.write(json.dumps({'remote':remote, 'unix':unix})+'\n')
                stream.flush()
                self.compare_remote(remote, allowed)
            require(error is None, 'remote owner reconciliation failed: '+str(error))
            require(time.monotonic()-last <= REMOTE_HEALTH_STALE, 'remote health observation is stale')

    def compare_remote(self, remote, allowed=()):
        for host, value in remote.items():
            if host in allowed or host == 'coordinator':
                continue
            old, new = self.baseline['remote'][host]['resources'], value['resources']
            require(all(new[k] == old[k] for k in ('oom', 'oom_kill', 'restarts', 'pid')), 'unexpected remote restart or OOM: '+host)

    def remote_health(self, stream, allowed=()):
        """A synchronous all-host owner reconciliation, used around effects."""
        remote = self.all_hosts('probe', running=True)
        stream.write(json.dumps({'remote':remote, 'unix':time.time()})+'\n')
        stream.flush()
        self.compare_remote(remote, allowed)
        return remote

    def retain(self, raw):
        """Every distinct raw response, once, by its digest."""
        sha = hashlib.sha256(raw).hexdigest()
        path = self.directory/'raw/responses'/sha
        if not path.exists():
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            write_bytes_once(path, raw)
        return sha

    def observe_freshness(self, window, stream):
        """One complete raw-retained observation of node, both origins and every replica."""
        calls = []
        def timed(name, function, *args):
            before, unix = time.monotonic(), time.time()
            value, raw = function(*args)
            after = time.monotonic()
            calls.append({'call':name, 'before':before, 'after':after, 'before_unix':unix, 'after_unix':time.time(),
                          'response_sha256':self.retain(raw)})
            return value, before, after
        def rpc(method, *params):
            return node_raw(method, list(params), self.deadline.timeout(3))
        def shards(url):
            value, raw, _ = fetch(url, self.deadline.timeout(4))
            require(isinstance(value.get('shards'), list) and value['shards'], 'empty serving map')
            return value['shards'][-1], raw
        height, before, after = timed('getblockcount', rpc, 'getblockcount')
        if window.tip is not None and height >= window.tip and getattr(self, 'tip_hash', None) is not None and \
                timed('getblockhash-tip', rpc, 'getblockhash', window.tip)[0] != self.tip_hash:
            window.reorganized(after)
        window.node(height, before, after)
        self.tip_hash = timed('getblockhash', rpc, 'getblockhash', height)[0]
        public, public_before, public_after = timed('public', shards, SHARD_ORIGIN+'/v1/shards')
        filters = timed('filters', shards, FILTER_ORIGIN+'/v1/filters/shards')[0]
        require(public == filters, 'public origins disagree')
        require(timed('getblockhash-public', rpc, 'getblockhash', public['end_height'])[0] == public['terminal_block_hash'],
                'public endpoint is not canonical')
        window.serving('public', public['end_height'], public_before, public_after)
        replicas = {}
        for host in self.hosts:
            worker = host['worker']
            if worker is not None and worker['role'] == 'recent-replica':
                value, replica_before, replica_after = timed(worker['id'], shards, 'http://'+worker['upstream']+'/v1/shards')
                require(timed('getblockhash-'+worker['id'], rpc, 'getblockhash', value['end_height'])[0] == value['terminal_block_hash'],
                        'replica is not canonical: '+worker['id'])
                window.serving(worker['id'], value['end_height'], replica_before, replica_after)
                replicas[worker['id']] = value['end_height']
        ended = time.monotonic()
        gap = window.complete(ended)
        stream.write(json.dumps({'unix':time.time(), 'node':height, 'public':public['end_height'], 'replicas':replicas,
                                 'blocks':window.blocks(), 'calls':calls})+'\n')
        stream.flush()
        return gap or window.violation(ended)

    def recent_ids(self):
        return [h['worker']['id'] for h in self.hosts if h['worker'] is not None and h['worker']['role'] == 'recent-replica']

    # Activities ---------------------------------------------------------------------
    def staged_load(self):
        raw = self.directory/'raw'
        raw.mkdir(mode=0o700)
        fixture = self.spec['load']['fixture']
        binary = artifact('examples/rate-query')
        results = []
        with (raw/'health.jsonl').open('x') as health, (raw/'freshness.jsonl').open('x') as fresh, RemoteMonitor(self):
            window = FreshnessWindow(self.recent_ids(), time.monotonic())
            permit = self.deadline.child(FRESHNESS['permit_seconds'])
            while not window.permits():
                violation = self.observe_freshness(window, fresh)
                require(violation is None, 'freshness does not permit staged load: '+str(violation))
                require(permit.remaining() > 0, 'no fresh canonical block observed; staged load not permitted')
                self.health(health)
                permit.sleep(1)
            for index, stage in enumerate(LOAD_STAGES):
                directory = raw/('stage-%d-%dqps' % (index, stage['qps']))
                directory.mkdir(mode=0o700)
                self.deadline.need(stage['seconds']+120, 'staged load at %d QPS' % stage['qps'])
                bound = self.deadline.child(stage['seconds']+90)
                permit_path = directory/'permit'
                permit_path.write_text('allow\n')
                files, processes = [], []
                failure = None
                try:
                    for client in range(stage['processes']):
                        out = (directory/('queries-%d.jsonl' % client)).open('xb')
                        err = (directory/('stderr-%d.log' % client)).open('xb')
                        files += [out, err]
                        processes.append(self.spawn('rate-%d-%d' % (index, client), [binary, '--url', SHARD_ORIGIN,
                            '--fixture', fixture['path'], '--qps', stage['qps']//stage['processes'], '--workers', 8,
                            '--seconds', stage['seconds'], '--permit', permit_path], out, err))
                    while any(p.poll() is None for p in processes):
                        try:
                            require(bound.remaining() > 0, 'rate clients exceeded their bound')
                            self.health(health)
                            violation = self.observe_freshness(window, fresh)
                            require(violation is None, str(violation))
                        except Exception as error:
                            failure = str(error)[:300]
                            break
                        permit_path.write_text('allow\n')
                        time.sleep(LOOP_SECONDS)
                finally:
                    permit_path.write_text('deny\n')
                    self.stop_children()
                    for file in files:
                        file.close()
                evaluation = evaluate_rate([lines(directory/('queries-%d.jsonl' % c)) for c in range(stage['processes'])],
                                           stage['qps'], stage['seconds'], fixture['sha256'])
                if failure or any(p.returncode != 0 for p in processes):
                    evaluation['status'] = 'failed'
                    evaluation['failures'].append(failure or 'client process failed')
                results.append(evaluation)
                if evaluation['status'] != 'passed':
                    break  # Refuse escalation; retain the failed stage.
        failures = [f for r in results for f in r['failures']] + ([] if len(results) == len(LOAD_STAGES) else ['escalation refused'])
        return {'status':'failed' if failures else 'passed', 'failures':failures, 'stages':results,
                'freshness':window.record(time.monotonic())}

    def freshness_window(self):
        raw = self.directory/'raw'
        raw.mkdir(mode=0o700)
        bound = self.deadline.child(FRESHNESS['bound_seconds'])
        with (raw/'health.jsonl').open('x') as health, (raw/'freshness.jsonl').open('x') as fresh, \
                (raw/'intervals.jsonl').open('x') as intervals, RemoteMonitor(self):
            window = FreshnessWindow(self.recent_ids(), time.monotonic())
            while True:
                self.health(health)
                try:
                    violation = self.observe_freshness(window, fresh)
                except (OSError, ValueError, KeyError, TypeError, IndexError, urllib.error.URLError) as error:
                    if isinstance(error, Budget):
                        raise
                    violation = 'observation failed: %s: %s' % (type(error).__name__, str(error)[:200])
                failed = len(window.failed)
                if violation:
                    window.close(time.monotonic(), violation)
                for interval in window.failed[failed:]:
                    intervals.write(json.dumps(interval)+'\n')
                    intervals.flush()
                if window.qualifies(time.monotonic()):
                    return {'status':'passed', 'failures':[], 'freshness':window.record(time.monotonic())}
                if bound.remaining() <= 1:
                    return {'status':'failed', 'failures':['no qualifying interval within the fixed bound'],
                            'freshness':window.record(time.monotonic())}
                bound.sleep(1)

    def capacity(self):
        request = self.request
        raw = self.directory/'raw'
        raw.mkdir(mode=0o700)
        cache = ROOT/'preparation-cache'
        cache.mkdir(mode=0o700, exist_ok=True)
        write_once(raw/'sample.json', request['sample'])
        metrics = {h['worker']['id']:'http://'+h['worker']['upstream']+'/metrics' for h in self.hosts if h['worker']}
        scenario = {'schema':'transparent-scenario-v1', 'name':'qualification level %d trial %d' % (request['level'], request['trial']),
                    'mode':'sustained', 'sample':'sample.json', 'shard_url':SHARD_ORIGIN, 'filter_url':FILTER_ORIGIN,
                    'profiles':COMPOSITION[request['level']], 'seed':request['trial'], 'allow_advancing_publication':True,
                    'preparation_cache':'reuse', 'preparation_cache_dir':str(cache),
                    'preparation_concurrency':CAPACITY['preparation_concurrency'],
                    'measured_http_attempts':CAPACITY['measured_http_attempts'],
                    'duration_seconds':CAPACITY['duration_seconds'], 'recovery_deadline_seconds':CAPACITY['recovery_deadline_seconds'],
                    'preparation_deadline_seconds':CAPACITY['preparation_deadline_seconds'],
                    'request_timeout_seconds':CAPACITY['request_timeout_seconds'], 'store':'sqlite',
                    'metrics_targets':metrics, 'notes':'Deployed candidate qualification; coordinator load host; '
                    'canonical continuous 5-QPS product load remains as background traffic.'}
        write_once(raw/'scenario.json', scenario)
        binary = artifact('transparent-loadtest')
        longest = CAPACITY['preparation_deadline_seconds']+CAPACITY['duration_seconds']+CAPACITY['recovery_deadline_seconds']+300
        self.deadline.need(longest+CHILD_STOP_SECONDS, 'a capacity trial')
        bound = self.deadline.child(longest)
        failure = None
        with (raw/'health.jsonl').open('x') as health, (raw/'freshness.jsonl').open('x') as fresh, \
                (raw/'loadtest.log').open('xb') as log, RemoteMonitor(self):
            window = FreshnessWindow(self.recent_ids(), time.monotonic())
            process = self.spawn('loadtest', [binary, '--scenario', raw/'scenario.json', '--out-dir', raw/'loadtest'], log, log)
            while process.poll() is None:
                try:
                    require(bound.remaining() > 0, 'loadtest exceeded its fixed bound')
                    self.health(health)
                    violation = self.observe_freshness(window, fresh)
                    require(violation is None, str(violation))
                except Exception as error:
                    failure = str(error)[:300]
                    break
                time.sleep(LOOP_SECONDS)
            ended = time.time()
            self.stop_children()
        evaluation = evaluate_capacity(raw/'loadtest', request['level'], ended, process.returncode, failure, metrics)
        evaluation['freshness'] = window.record(time.monotonic())
        return {'status':evaluation['status'], 'failures':evaluation['failures'], 'capacity':evaluation}

    def probe(self, directory):
        """Exact canonical encrypted queries, then a reopened-wallet recovery, each bounded."""
        self.deadline.need(QUERY_PROBE_SECONDS+WALLET_PROBE_SECONDS, 'a canonical query and wallet probe')
        directory.mkdir(mode=0o700)
        fixture = self.spec['load']['fixture']
        permit = directory/'permit'
        permit.write_text('allow\n')
        bound = self.deadline.child(QUERY_PROBE_SECONDS)
        try:
            with (directory/'queries.jsonl').open('xb') as out, (directory/'stderr.log').open('xb') as err:
                process = self.spawn('probe-query', [artifact('examples/rate-query'), '--url', SHARD_ORIGIN, '--fixture',
                                     fixture['path'], '--qps', 5, '--workers', 8, '--seconds', 30, '--permit', permit], out, err)
                while process.poll() is None:
                    require(bound.remaining() > 0, 'canonical query probe exceeded its bound')
                    permit.write_text('allow\n')
                    bound.sleep(1)
        finally:
            permit.write_text('deny\n')
            self.stop_children()
        query = evaluate_rate([lines(directory/'queries.jsonl')], 5, 30, fixture['sha256'],
                              dict(LOAD_GATES, p50_below_seconds=math.inf, p99_below_seconds=math.inf, recent_fraction=(0, 1)))
        require(process.returncode == 0 and query['status'] == 'passed', 'canonical encrypted query probe failed')
        self.deadline.need(WALLET_PROBE_SECONDS, 'a reopened-wallet proof')
        recovery = self.spec['routing']['recovery']['v11']
        wallet = R.run(artifact('transparent-loadtest'), recovery['binary_sha256'], recovery['sample'], recovery['sample_sha256'],
                       SCHEMA_V11, SHARD_ORIGIN, FILTER_ORIGIN, directory/'wallet', self.request['source_sha'])
        return {'query':query, 'wallet':{k:wallet.get(k) for k in ('status', 'native_report_sha256', 'sample_sha256')},
                'stores':len(wallet.get('observations', []))}

    def identity_check(self, ready, public, faulted=()):
        """Recovered workers serve the intended candidate assignment and the canonical public map.

        Publication advances during a fault, so the map is not compared to the
        stale pre-fault tail: every worker must serve exactly the current public
        map, whose tail is canonical, and every sealed pre-fault shard must remain
        unchanged. Binary and assignment identity must equal the baseline; only a
        faulted worker may have a new incarnation.
        """
        base_public = self.baseline['public']
        for shard, manifest in base_public['sealed'].items():
            require(public['sealed'].get(shard) == manifest, 'sealed publication changed: shard '+shard)
        for worker, identity in ready.items():
            old = self.baseline['ready'][worker]
            require(all(identity[k] == old[k] for k in ('binary_sha256', 'worker_id', 'role', 'assignment_sha256',
                                                        'worker_assignment_sha256')),
                    'original baseline identity not preserved: '+worker)
            require(identity['map_sha256'] == public['sha256'], 'worker does not serve the canonical public map: '+worker)
            if worker not in faulted:
                require(identity['incarnation'] == old['incarnation'], 'unexpected worker restart: '+worker)
            else:
                require(identity['incarnation'] != old['incarnation'], 'faulted worker did not restart: '+worker)

    def recover(self, raw, started, allowed, faulted=()):
        """Exact recovery within 900 s of the effect, every attempt inside that one deadline."""
        deadline = Deadline(RECOVERY_SECONDS, start=started)
        attempts, outer = 0, self.deadline
        try:
            with (raw/'health.jsonl').open('a') as health:
                while True:
                    attempts += 1
                    deadline.need(ATTEMPT_SECONDS, 'a recovery attempt')
                    self.deadline = deadline if deadline.end < outer.end else outer
                    try:
                        self.health(health, allowed)
                        result = self.probe(raw/('recovery-%02d' % attempts))
                        ready = self.readiness()
                        public = self.public_map()
                        self.identity_check(ready, public, faulted)
                        if 'node_height' in self.record.get('effect', {}):
                            require(public['tail']['end_height'] >= self.record['effect']['node_height'],
                                    'publication has not caught up to the pre-fault tip')
                    except Budget:
                        raise
                    except Exception as error:
                        with (raw/('recovery-%02d.error.json' % attempts)).open('x') as stream:
                            json.dump({'error_type':type(error).__name__, 'error':str(error)[:500], 'unix':time.time(),
                                       'elapsed_seconds':deadline.elapsed()}, stream)
                        deadline.need(15+ATTEMPT_SECONDS, 'another recovery attempt')
                        deadline.sleep(15)
                        continue
                    break
        except Budget as error:
            raise ValueError('recovery not established within 900 seconds: '+str(error)) from None
        finally:
            self.deadline = outer
        elapsed = deadline.elapsed()
        require(elapsed <= RECOVERY_SECONDS, 'recovery completed after 900 seconds (%.1f s)' % elapsed)
        return {'recovered_seconds':elapsed, 'attempts':attempts, 'probe':result, 'ready':ready, 'public':public}

    def owners_before_effect(self, raw):
        """Pinned all-host owner reconciliation under the lock, retained raw."""
        self.lock.verify()
        self.quiescent_children()
        hosts = self.all_hosts('probe', running=True)
        write_once(raw/'owners-before-effect.json', hosts)
        return hosts

    def fault(self):
        request = self.request
        raw = self.directory/'raw'
        raw.mkdir(mode=0o700)
        fault = request['fault']
        rollback = {'coordinator':self.baseline['coordinator']['rollback'],
                    **{h:v['rollback'] for h, v in self.baseline['remote'].items()}}
        with (raw/'health.jsonl').open('x') as health:
            self.health(health)
            self.remote_health(health)
        before = self.probe(raw/'before')
        self.deadline.need(EFFECT_SECONDS[fault]+RECOVERY_SECONDS+POST_SECONDS, 'the fault, its recovery and post checks')
        self.owners_before_effect(raw)
        effect = {'fault':fault, 'status':'starting', 'started_unix':time.time()}
        self.record['effect'] = effect
        self.save(self.record)  # durable intent before the effect
        started = time.monotonic()
        allowed, faulted = (), ()
        if fault == 'client-reopen':
            effect.update(self.client_reopen(raw))
            effect['status'] = 'passed'
        elif fault == 'publication-interruption':
            allowed = (PUBLISHER,)
            effect.update(self.publication_interruption())
        elif fault == 'rollback-redeploy':
            # Root performed both through schema-rollback/schema-deploy; this owner
            # only binds their journals and never synthesizes either outcome.
            effect.update(self.rollback_evidence, source='retained schema journals', status='passed')
            require(effect['rollback_seconds'] <= RECOVERY_SECONDS, 'rollback restore exceeded 900 seconds')
        else:
            role, operation = FAULT_ACTION[fault]
            host = request.get('target') or next(h['host'] for h in self.hosts if h['role'] == role)
            entry = next(h for h in self.hosts if h['host'] == host)
            allowed = (host,)
            faulted = (entry['worker']['id'],) if entry['worker'] else ()
            action = {'host':host, 'operation':operation, 'status':'starting', 'intent_unix':time.time()}
            self.record.setdefault('remote_actions', []).append(action)
            self.save(self.record)
            try:
                _, reply = self.remote(host, operation, 'act')
                action.update(status=reply['status'], phase=reply.get('phase'), error=reply.get('error'))
            except Unknown as error:
                action.update(status='unknown', error=str(error)[:300])
                raise
            except Exception as error:
                action.update(status='failed', error=str(error)[:300])
                raise
            finally:
                self.save(self.record)
            require(action['status'] == 'passed', 'remote fault action did not pass on %s: %s' % (host, action['status']))
            effect.update(remote=action, status='passed')
        self.save(self.record)
        recovered = self.recover(raw, started, allowed, faulted)
        if fault == 'rollback-redeploy':
            # Restore time is the reviewed rollback itself; redeploy then needs exact recovery.
            recovered['restore_seconds'] = self.rollback_evidence['rollback_seconds']
            recovered['redeploy_commit_to_proof_seconds'] = time.time()-self.rollback_evidence['redeploy_committed_unix']
        # Faulted units have new processes; quiescence is re-based after recovery.
        local = local_resources(commands(self.deadline))
        with (raw/'health.jsonl').open('a') as health:
            remote = self.remote_health(health, allowed)
            self.baseline = dict(self.baseline, local=local, remote={h:v for h, v in remote.items() if h != 'coordinator'})
            self.health(health)
        self.quiescent_children()
        after = self.all_hosts('identity', running=True)
        write_once(raw/'owners-after-recovery.json', after)
        require(set(after) == set(rollback) and all(after[h]['rollback'] == rollback[h] for h in rollback),
                'retained rollback baseline changed: '+', '.join(sorted(h for h in rollback if after.get(h, {}).get('rollback') != rollback[h])))
        return {'status':'passed', 'failures':[], 'before':before, 'effect':effect, 'recovery':recovered}

    def publication_interruption(self):
        """Stop the publisher only while a recent replica reports an in-progress preparation.

        The native worker control status reports `preparing` with the candidate map
        digest. The stop counts as inside that preparation only if the same worker,
        read after the stop completed, still has not activated that map.
        """
        recent = next(h['host'] for h in self.hosts if h['worker'] is not None and h['worker']['role'] == 'recent-replica')
        effect = self.record['effect']
        _, observed = self.remote(recent, 'await-preparation', 'probe')
        effect['preparation'] = observed
        self.save(self.record)
        require(observed.get('observed') == 'preparing', 'no in-progress preparation within %d s' % PREPARATION_WAIT_SECONDS)
        effect['node_height'] = node('getblockcount', [], self.deadline.timeout(3))
        unit = UnitEffect(PUBLISHER, effect, lambda: self.save(self.record), commands(self.deadline))
        unit.capture()
        self.save(self.record)  # durable pre-fault publisher identity
        self.lock.verify()
        try:
            unit.stop(self.deadline)
            try:
                _, after = self.remote(recent, 'control-status', 'probe')
                effect['after_stop'] = after
            finally:
                unit.start(self.deadline)
        except Exception as error:
            effect.update(error=str(error)[:300])
            try:
                unit.restore(Deadline(RESTORE_SECONDS))
            except Exception as again:
                effect.update(restore_error=str(again)[:300])
            self.save(self.record)
            raise
        preparing = observed['control']['preparing']['map_sha256']
        effect['inside_preparation'] = after['control']['active_map_sha256'] != preparing
        effect['status'] = 'passed'
        self.save(self.record)
        require(effect['inside_preparation'], 'publisher stop was not proven inside an in-progress preparation')
        return {}

    def client_reopen(self, raw):
        """Terminate this owner's own wallet client mid-sync, then reopen its store."""
        recovery = self.spec['routing']['recovery']['v11']
        directory = raw/'interrupted-client'
        directory.mkdir(mode=0o700)
        stores = directory/'stores'
        bound = self.deadline.child(EFFECT_SECONDS['client-reopen'])
        with (directory/'native.log').open('xb') as log:
            process = self.spawn('interrupted-client', [artifact('transparent-loadtest'), '--shard-url', SHARD_ORIGIN,
                '--filter-url', FILTER_ORIGIN, '--sample', recovery['sample'], '--steps', 1, '--step-duration', '300s',
                '--http-attempts', 1, '--timeout', '60s', '--store-dir', stores, '--retain-stores',
                '--json-out', directory/'native.json', '--run-id', 'interrupted-client', '--source-sha', self.request['source_sha']], log, log)
            try:
                waited = bound.child(120)
                while not any(stores.rglob('*.sqlite')) and process.poll() is None:
                    require(waited.remaining() > 0, 'owned client did not open a wallet store')
                    waited.sleep(.5)
                terminated = process.poll() is None
            finally:
                self.stop_children()
        require(terminated, 'owned client exited before it could be terminated')
        require(not session_members([process.pid]), 'terminated client left owned descendants')
        checked = []
        for path in sorted(stores.rglob('*.sqlite')):
            with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
                checked.append({'database':str(path), 'integrity':db.execute('PRAGMA integrity_check').fetchone()[0]})
        require(checked and all(c['integrity'] == 'ok' for c in checked), 'interrupted wallet store failed to reopen')
        return {'terminated_pid':process.pid, 'reopened':checked}


def interrupted(_signal, _frame):
    raise Interrupted('qualification owner interrupted')
