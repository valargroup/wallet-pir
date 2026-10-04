"""Deployed changed-native candidate qualification through the production wrapper.

Four closed activities measure the canonical deployment after a committed
version-2 product transaction for candidate `c3c66b9b`: `staged-load`,
`freshness`, `capacity` and `fault`. `schema-qualify-run` launches one
detached owner under the coordinator production lock and the shared
input-staging ownership fence, so no other wrapper mutation and no second
qualification can overlap it. Rates, durations, thresholds, units, origins and
artifacts are fixed here; a request only selects among them and supplies the
reviewed capacity sample. Raw receipts are written once and sealed read-only.

Only the retained candidate clients run: `examples/rate-query` (canonical
encrypted queries), `transparent-loadtest` (wallet syncs) and the existing
recovery proof (reopened SQLite). `transparent-measure` serves its own local
shard set and cannot measure a deployment. Gaps the native interfaces cannot
measure are listed in MISSING and keep a result unqualified; fixtures in the
test suite never qualify anything.
"""
import base64
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
import time
import urllib.error
import urllib.request

from wallet_pir_ops import durable, inherited_lock, schema_fence
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
SHARD_ORIGIN = 'https://transparent-pir.valargroup.dev'
FILTER_ORIGIN = 'https://enhance-pir.valargroup.dev'
NODE = 'http://127.0.0.1:8232'
COOKIE = Path('/root/.cache/zakura/.cookie')
SCHEMA_V11 = 'transparent-shard-v11'
HEX = re.compile('[0-9a-f]{64}')
TXN = re.compile('transparent-schema-[A-Za-z0-9-]+')
NAME = re.compile('[a-zA-Z0-9-]{1,64}')
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
             'bound_seconds':43200, 'permit_seconds':300}
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
FAULT_ACTION = {'publication-interruption':('coordinator', 'restart'), 'recent-worker-loss':('worker', 'stop-start'),
                'archive-restart':('worker', 'restart'), 'router-restart':('router', 'restart')}
REMOTE_UNITS = {'worker':H.WORKER, 'router':'caddy.service'}
REMOTE_DISK = {'worker':H.CACHE, 'router':Path('/')}
COORDINATOR_UNITS = (*H.AUTHORITY, H.FILTER, H.LOAD)
PUBLISHER = H.AUTHORITY[0]
RECOVERY_SECONDS = 900
LOSS_HOLD_SECONDS = 60
# Reviewed schema recipe budgets; a successful rollback must use them unchanged.
ROLLBACK_BUDGET = {'withdraw-origins':60, 'restore-v10':140, 'verify-rollback':300, 'reopen-v10':100, 'verify-service':140}
FORWARD_TIMEOUT = 1800
RUNTIME = {'staged-load':1800, 'freshness':FRESHNESS['bound_seconds']+900, 'capacity':10800, 'fault':3600}
MEMORY = {'capacity':('10G', '12G')}
MISSING = {
    'all': ['quality alerts: their shadow state is APM configuration with no coordinator interface; '
            'only the stopped quality supervisor is verified'],
    'capacity': ['heavy continuation: transparent-loadtest retains incomplete heavy SQLite stores but no native '
                 'interface resumes a retained store; continuation is unmeasured and capacity stays unqualified'],
    'client-reopen': ['interrupted-store continuation: the interrupted store is integrity-checked and an exact '
                      'fresh recovery is required; no native interface resumes the interrupted store'],
    'publication-interruption': ['mid-preparation timing: no native status interface exposes an in-progress '
                                 'preparation, so the restart is not proven to land inside one'],
    'archive-restart': ['cache corruption: injecting a corrupt runtime/disk cache needs destructive file mutation '
                        'with no reviewed native interface; only an owner restart and cache reload are exercised'],
}


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
            'Restart=no', 'KillMode=control-group', 'TimeoutStopSec=30', 'RemainAfterExit=yes',
            'RuntimeMaxSec=%d' % RUNTIME[kind])


def activity(request):
    kind = request['kind']
    if kind == 'staged-load':
        return {'stages':list(LOAD_STAGES), 'gates':LOAD_GATES, 'freshness_permit':FRESHNESS,
                'client':'examples/rate-query', 'workers_per_client':8, 'origin':SHARD_ORIGIN}
    if kind == 'freshness':
        return {'freshness':FRESHNESS, 'origins':[SHARD_ORIGIN, FILTER_ORIGIN], 'replicas':'every recent-replica upstream'}
    if kind == 'capacity':
        return {'level':request['level'], 'trial':request['trial'], 'composition':COMPOSITION[request['level']],
                'p95_targets':P95_TARGETS, 'heavy':HEAVY, 'capacity':CAPACITY, 'client':'transparent-loadtest',
                'mode':'sustained', 'seed':request['trial'], 'origins':[SHARD_ORIGIN, FILTER_ORIGIN]}
    fault = request['fault']
    return {'fault':fault, 'target':request.get('target'), 'action':FAULT_ACTION.get(fault),
            'loss_hold_seconds':LOSS_HOLD_SECONDS if fault == 'recent-worker-loss' else None,
            'recovery_seconds':RECOVERY_SECONDS, 'rollback_budget':ROLLBACK_BUDGET,
            'rolled_back_transaction':request.get('rolled_back_transaction'),
            'probes':['canonical encrypted query (rate-query 5 QPS, 30 s)', 'reopened-wallet recovery proof']}


def plan(request):
    validate(request)
    sha = digest(request)
    kind = request['kind']
    missing = MISSING['all'] + MISSING.get(kind, []) + MISSING.get(request.get('fault'), [])
    return {'version':1, 'kind':'deployed-qualification', 'request_sha256':sha, 'request':summary(request),
            'candidate_sha':C.SOURCE_SHA, 'candidate_identity':C.identity(), 'historical_release_sha':C.HISTORICAL_SHA,
            'activity':activity(request), 'unit':unit_name(sha), 'properties':list(properties(kind)),
            'headroom':HEADROOM, 'missing_assurance':missing,
            'effects':'private qualification receipts, owned client processes'+
                      (' and one fixed unit transition' if request.get('fault') in FAULT_ACTION else '')}


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
        return (identity.get('boot_id') == boot_id() and
                int(stat_fields(identity['pid'])[19]) == identity['start_ticks'])
    except (OSError, ValueError, IndexError):
        return False


def session_members(sessions):
    """Live processes in any recorded owned session on this boot."""
    sessions, found = set(sessions), []
    if not sessions:
        return found
    for entry in PROC.iterdir():
        if entry.name.isdigit():
            try:
                if int(stat_fields(entry.name)[3]) in sessions:
                    found.append(int(entry.name))
            except (OSError, ValueError, IndexError):
                continue
    return found


def write_once(path, value):
    """Immutable private receipt: create exclusively, fsync, seal read-only."""
    data = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False)+'\n').encode()
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
                    result.append(json.loads(line))
                except json.JSONDecodeError:
                    result.append({'event':'unparseable'})
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

    A new canonical height is timestamped when the node first reports it; it must
    become visible at the public origin within 30 s and at every recent replica
    within 60 s. A violation or reorganization closes the current interval as
    failed, retains it, and starts a new interval with no inherited credit.
    """
    def __init__(self, replicas, now, freshness=FRESHNESS):
        self.replicas, self.freshness = tuple(sorted(replicas)), freshness
        self.failed, self.tip = [], None
        self.start(now)

    def start(self, now):
        self.started, self.seen = now, {}
        self.visible = {layer:{} for layer in ('public', *self.replicas)}

    def close(self, now, reason):
        self.failed.append({'started':self.started, 'ended':now, 'reason':reason, 'blocks':self.blocks()})
        self.start(now)

    def blocks(self):
        return min(len(v) for v in self.visible.values())

    def node(self, height, now):
        if self.tip is not None and height < self.tip:
            self.close(now, 'chain reorganized')
        if self.tip is not None:
            for value in range(max(self.tip, max(self.seen, default=self.tip))+1, height+1):
                self.seen.setdefault(value, now)
        self.tip = height

    def reorganized(self, now):
        self.close(now, 'chain reorganized')

    def serving(self, layer, end_height, now):
        for height, observed in self.seen.items():
            if height <= end_height and height not in self.visible[layer]:
                self.visible[layer][height] = now-observed

    def violation(self, now):
        for layer, values in self.visible.items():
            budget = self.freshness['publication_seconds' if layer == 'public' else 'recent_seconds']
            late = [h for h, latency in values.items() if latency > budget]
            overdue = [h for h, seen in self.seen.items() if h not in values and now-seen > budget]
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
                'failed_intervals':list(self.failed)}


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


def evaluate_capacity(directory, level, ended_unix, exited_normally, targets):
    """One sustained trial from loadtest NDJSON; scheduled wallets never count."""
    directory = Path(directory)
    events = lines(directory/'wallets.ndjson')
    scheduled = {e['id']:e for e in events if e.get('type') == 'scheduled'}
    started = {e['id']:e for e in events if e.get('type') == 'started'}
    outcomes = {}
    duplicate = False
    for e in events:
        if e.get('type') == 'outcome':
            duplicate |= e.get('id') in outcomes
            outcomes[e.get('id')] = e
    failures = []
    if duplicate or set(outcomes) - set(scheduled):
        failures.append('duplicate or unscheduled wallet outcome')
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
        profile = scheduled[identifier].get('profile')
        if profile not in profiles:
            failures.append('wallet outside the composed profiles')
            continue
        entry = profiles[profile]
        kind = outcome.get('outcome')
        entry['terminal'] += 1
        entry['outcomes'][kind] = entry['outcomes'].get(kind, 0)+1
        if kind == 'exact' and outcome.get('events_exact') is True and identifier in started:
            entry['exact'] += 1
            entry['seconds'].append(outcome['at']-started[identifier]['at'])
            exact_in_window += outcome['at'] <= window_end
    terminal = sum(p['terminal'] for p in profiles.values())
    exact = sum(p['exact'] for p in profiles.values())
    failed_fraction = (terminal-exact)/terminal if terminal else 1.0
    requests = lines(directory/'requests.ndjson')
    attempts = sum(1 for r in requests if r.get('type') == 'request')
    refused = sum(1 for r in requests if r.get('type') == 'request' and r.get('status') == 503)
    rate_503 = refused/attempts if attempts else 1.0
    upload = sum(r.get('bytes_up') or 0 for r in requests if r.get('type') == 'request')
    download = sum(r.get('bytes_down') or 0 for r in requests if r.get('type') == 'request')
    metrics = lines(directory/'metrics.ndjson')
    workers, metric_failures = evaluate_metrics(metrics, sorted(targets))
    failures += metric_failures
    complete = exited_normally and t0 is not None and ended_unix-t0 >= CAPACITY['duration_seconds']
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
            'complete':complete, 'scheduled':len(scheduled), 'terminal':terminal, 'exact':exact,
            'unterminated':len(unterminated), 'failed_or_incomplete_fraction':failed_fraction,
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
    passed(redeploy['events'], 'steps', forward)
    require(len(redeploy['events']) == len(forward), 'redeploy recorded rollback events')
    return {'rolled_back_transaction':original['id'], 'redeploy_transaction':redeploy['id'],
            'rollback_started_unix':rollback_started, 'rollback_seconds':rollback_ended-rollback_started,
            'phases':{e['name']:e['seconds'] for e in rollback}, 'redeploy_created_unix':redeploy['created']}


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

def fetch_json(url, timeout=8):
    request = urllib.request.Request(url, headers={'Cache-Control':'no-cache'})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read(MAX_REPLY+1)
    require(len(raw) <= MAX_REPLY, 'observation exceeds bound')
    return json.loads(raw)


def node(method, params):
    cookie = COOKIE.read_text().strip()
    body = json.dumps({'jsonrpc':'2.0', 'id':method, 'method':method, 'params':params}).encode()
    request = urllib.request.Request(NODE, body, {'Content-Type':'application/json',
                                                  'Authorization':'Basic '+base64.b64encode(cookie.encode()).decode()})
    with urllib.request.urlopen(request, timeout=8) as response:
        raw = response.read(65537)
    require(len(raw) <= 65536, 'node RPC response exceeds bound')
    result = json.loads(raw)
    require(result.get('error') is None, 'node RPC failed')
    return result['result']


def tail(url):
    shards = fetch_json(url)['shards']
    require(isinstance(shards, list) and shards, 'empty serving map')
    return shards[-1]


def local_resources():
    memory = {line.split(':', 1)[0]:int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()
              if line.startswith(('MemTotal:', 'MemAvailable:'))}
    disks = {}
    for path in (Path('/srv/transparent-activity'), Path('/srv/zakura'), Path('/')):
        if path.exists():
            value = os.statvfs(path)
            disks[str(path)] = value.f_bavail/value.f_blocks
    commands = H.Commands()
    units = {}
    for unit in COORDINATOR_UNITS:
        state = commands.state(unit)
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


def quality_stopped():
    commands = H.Commands()
    state = commands.state(H.QUALITY)
    require(state.get('MainPID') == '0' and state.get('ActiveState') in ('inactive', 'failed') and
            commands.empty_cgroup(state), 'quality supervisor must remain stopped')


# --- remote fixed actor --------------------------------------------------------

def remote_validate(request):
    require(isinstance(request, dict) and set(request) == {'version', 'source_sha', 'qualification_sha256', 'host',
            'role', 'machine_id', 'coordinator_machine_id', 'operation'}, 'invalid remote qualification request')
    require(request['version'] == 1 and type(request['version']) is int and
            isinstance(request['source_sha'], str) and re.fullmatch('[0-9a-f]{40}', request['source_sha']) and
            isinstance(request['qualification_sha256'], str) and HEX.fullmatch(request['qualification_sha256']) and
            isinstance(request['host'], str) and NAME.fullmatch(request['host']) and
            all(isinstance(request[k], str) and re.fullmatch('[0-9a-f]{32}', request[k]) for k in ('machine_id', 'coordinator_machine_id')) and
            request['machine_id'] != request['coordinator_machine_id'], 'invalid remote qualification identity')
    require(request['role'] in REMOTE_UNITS and request['operation'] in ('probe', 'restart', 'stop-start') and
            (request['operation'] != 'stop-start' or request['role'] == 'worker'), 'unsupported remote qualification operation')
    return request


def remote_command(request, action):
    return ['/usr/bin/python3', '-B', str(SOURCES/request['source_sha']/'ops/scripts/wallet-pir-deploy.py'),
            'schema-qualify-remote', '--action', action, '--request-sha256', digest(request)]


class RemoteActor:
    """The remote half: a read-only probe or one fixed unit transition."""
    def __init__(self, request, commands=None):
        self.request = remote_validate(request)
        self.sha = digest(request)
        self.unit = REMOTE_UNITS[request['role']]
        self.path = REMOTE/(self.sha+'.json')
        self.commands = commands or H.Commands()

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
            require(json.loads((REMOTE/(latest+'.json')).read_bytes()).get('status') in ('passed', 'failed', 'reconciled'),
                    'unfinished remote qualification action; reconcile before another')

    def status(self):
        return json.loads(self.path.read_bytes()) if self.path.exists() else {'status':'absent', 'request_sha256':self.sha}

    def run(self, action):
        self.identity()
        if action == 'probe':
            require(self.request['operation'] == 'probe', 'probe request required')
            schema_fence.local_schema_fence()
            self.terminal()
            return {'status':'passed', 'host':self.request['host'], 'resources':self.resources()}
        if action == 'status':
            return self.status()
        if action == 'reconcile':
            return self.reconcile()
        require(action == 'act' and self.request['operation'] != 'probe', 'unsupported remote action')
        with ProductionLock({'type':'pinned_host', 'machine_id':self.request['machine_id']}) as lock:
            lock.verify()
            schema_fence.local_schema_fence()
            self.terminal()
            require(not self.path.exists(), 'remote action already owned; inspect status, never replay')
            REMOTE.mkdir(parents=True, exist_ok=True, mode=0o700)
            before = self.resources()
            record = {'status':'running', 'request':self.request, 'request_sha256':self.sha,
                      'owner':process_identity(os.getpid()), 'before':before, 'started_unix':time.time()}
            durable.atomic_json(self.path, record, mode=0o600)  # durable intent before the effect
            durable.atomic_json(REMOTE/'latest.json', {'request_sha256':self.sha}, mode=0o600)
            try:
                if self.request['operation'] == 'restart':
                    self.commands.unit('restart', self.unit)
                else:
                    self.commands.unit('stop', self.unit)
                    lock.verify()
                    time.sleep(LOSS_HOLD_SECONDS)
                    self.commands.unit('start', self.unit)
                deadline = time.monotonic()+60
                while self.commands.state(self.unit).get('ActiveState') != 'active':
                    require(time.monotonic() < deadline, 'unit did not return to active within 60 seconds')
                    time.sleep(1)
                record.update(status='passed', after=self.resources(), ended_unix=time.time())
            except BaseException as error:
                record.update(status='failed', error_type=type(error).__name__, ended_unix=time.time())
                raise
            finally:
                durable.atomic_json(self.path, record, mode=0o600)
            return record

    def reconcile(self):
        with ProductionLock({'type':'pinned_host', 'machine_id':self.request['machine_id']}) as lock:
            lock.verify()
            record = self.status()
            if record['status'] in ('absent', 'passed', 'failed', 'reconciled'):
                return record
            require(not alive(record['owner']), 'remote action owner is still alive')
            state = self.commands.state(self.unit)
            require(state.get('ActiveState') not in ('activating', 'deactivating'), 'remote unit is still transitioning')
            record.update(status='reconciled', reconciled_unix=time.time(), state=state)
            durable.atomic_json(self.path, record, mode=0o600)
            return record


def remote_run(request, action):
    return RemoteActor(request).run(action)


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
        require(sum(h['worker'] is not None and h['worker']['role'] == 'recent-replica' for h in hosts) >= 2 and
                sum(h['worker'] is not None and h['worker']['role'] == 'archive-owner' for h in hosts) >= 1,
                'deployment lacks recent replicas or an archive owner')
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

    def remote(self, host, operation, action, timeout):
        entry = next(h for h in self.hosts if h['host'] == host)
        request = remote_validate({'version':1, 'source_sha':self.request['source_sha'], 'qualification_sha256':self.sha,
                                   'host':host, 'role':entry['role'], 'machine_id':entry['machine_id'],
                                   'coordinator_machine_id':self.inventory.lock['machine_id'], 'operation':operation})
        argv = SSHExecutor(self.ssh).transport(host)+[shlex.join(remote_command(request, action))]
        result = subprocess.run(argv, input=durable.canonical(request), capture_output=True, timeout=timeout,
                                **inherited_lock.options())
        require(result.returncode == 0 and len(result.stdout) <= MAX_REPLY, 'remote qualification %s failed on %s' % (action, host))
        return request, json.loads(result.stdout)

    def remote_hosts(self):
        return [h for h in self.hosts if h['role'] != 'coordinator']

    def probe_all(self):
        """Fresh all-host owner reconciliation and resource baseline."""
        samples = {}
        for host in self.remote_hosts():
            _, reply = self.remote(host['host'], 'probe', 'probe', 60)
            resources = reply['resources']
            require(resources['memory_available']*5 >= resources['memory_total'] and
                    resources['disk_available']*5 >= resources['disk_total'], 'remote headroom below 20 percent: '+host['host'])
            samples[host['host']] = resources
        return samples

    def readiness(self):
        identities = {}
        for host in self.hosts:
            worker = host['worker']
            if worker is None:
                continue
            ready = fetch_json('http://'+worker['upstream']+'/v1/ready', timeout=5)
            identities[worker['id']] = {k:ready.get(k) for k in ('ready', 'binary_sha256', 'map_sha256', 'incarnation')}
            require(ready.get('ready') is True and ready.get('binary_sha256') == worker['binary_sha256'] == C.ARTIFACTS['transparent-shard-server'],
                    'worker is not ready on the candidate executable: '+worker['id'])
        return identities

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
        quality_stopped()
        local = local_resources()
        floors(local)
        remote = self.probe_all()
        ready = self.readiness()
        self.kind_preflight()
        if not running:
            require(self.status()['status'] == 'absent', 'qualification already owned; inspect/reconcile, never replay')
        return {'deployment':deployment, 'local':local, 'remote':remote, 'ready':ready}

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
        record = self.status()
        require(record['status'] == 'launching', 'qualification launch identity changed or owner already ran')
        deadline = time.monotonic()+15
        lock = ProductionLock(self.inventory.lock)
        while True:
            try:
                lock.__enter__()
                break
            except BlockingIOError:
                require(time.monotonic() < deadline, 'qualification lock handoff timed out; reconcile launch intent')
                time.sleep(.1)
        self.lock = lock
        os.environ[inherited_lock.VARIABLE] = ','.join(map(str, lock.descriptors()))
        os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
        previous = signal.signal(signal.SIGTERM, interrupted)
        result = None
        try:
            schema_fence.local_schema_fence(skip_input=self.sha)
            record.update(status='running', owner=process_identity(os.getpid()))
            self.save(record)
            self.record = record
            baseline = self.preflight(running=True)
            write_once(self.directory/'baseline.json', baseline)
            self.baseline = baseline
            runner = {'staged-load':self.staged_load, 'freshness':self.freshness_window,
                      'capacity':self.capacity, 'fault':self.fault}[self.request['kind']]
            result = runner()
        except BaseException as error:
            result = {'status':'failed' if isinstance(error, Exception) else 'interrupted',
                      'error_type':type(error).__name__, 'error':str(error)[:500]}
            raise
        finally:
            self.stop_children()
            result = dict(result or {}, plan_sha256=self.plan_sha, request_sha256=self.sha, ended_unix=time.time(),
                          missing_assurance=self.plan_value['missing_assurance'], qualified=False if result is None else
                          result.get('status') == 'passed' and not self.plan_value['missing_assurance'])
            write_once(self.directory/'result.json', result)
            seal(self.directory)
            record = self.status()
            record.update(status='finished', finished_unix=time.time())
            self.save(record)
            signal.signal(signal.SIGTERM, previous)
            lock.__exit__(None, None, None)

    def report(self):
        record = self.status()
        if record['status'] == 'absent':
            return record
        state = H.Commands().state(self.unit) if record['status'] != 'reconciled' else None
        result_path = self.directory/'result.json'
        result = json.loads(result_path.read_bytes()) if result_path.exists() else None
        owner = record.get('owner')
        return {'request_sha256':self.sha, 'plan_sha256':self.plan_sha, 'status':record['status'],
                'outcome':record.get('outcome'), 'unit':self.unit, 'unit_state':state,
                'owner_alive':alive(owner) if owner else None,
                'children':[{**c, 'alive':alive(c['identity'])} if c.get('identity') else c for c in record.get('children', [])],
                'result':None if result is None else {k:result.get(k) for k in ('status', 'failures', 'qualified', 'missing_assurance', 'error')}}

    def reconcile(self):
        with ProductionLock(self.inventory.lock) as lock:
            lock.verify()
            schema_fence.local_schema_fence(skip_input=self.sha)
            require(json.loads((OWNERS/'latest.json').read_bytes()) == {'request_sha256':self.sha}, 'reconcile the latest qualification first')
            record = self.status()
            require(record['status'] in ('launching', 'running', 'finished'), 'qualification does not require reconciliation')
            commands = H.Commands()
            state = commands.state(self.unit)
            require(state.get('MainPID') in ('0', None, '') and commands.empty_cgroup(state), 'qualification owner unit still runs')
            for identity in [record.get('owner'), *(c.get('identity') for c in record.get('children', []))]:
                require(not identity or not alive(identity), 'recorded qualification process is still alive')
            members = session_members(c['identity']['session'] for c in record.get('children', []) if c.get('identity'))
            require(not members, 'owned client descendants are still running')
            for action in record.get('remote_actions', []):
                if action.get('status') != 'passed':
                    self.retained_hosts()
                    _, remote = self.remote(action['host'], action['operation'], 'reconcile', 60)
                    require(remote.get('status') in ('passed', 'failed', 'reconciled', 'absent'),
                            'remote qualification action remains unfinished on '+action['host'])
                    action['reconciled'] = remote.get('status')
            result_path = self.directory/'result.json'
            if not result_path.exists():
                write_once(result_path, {'status':'interrupted', 'request_sha256':self.sha, 'plan_sha256':self.plan_sha,
                                         'reconciled_unix':time.time(), 'qualified':False,
                                         'missing_assurance':self.plan_value['missing_assurance']})
            if self.directory.exists():
                seal(self.directory)
            result = json.loads(result_path.read_bytes())
            record.update(status='reconciled', outcome=result['status'], reconciled_unix=time.time(),
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
        entry = {'name':name, 'status':'starting'}
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
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
            entry.update(status='exited', exit_code=process.returncode)
        if getattr(self, 'processes', None):
            self.save(self.record)

    # Health -----------------------------------------------------------------------
    def health(self, stream, allowed=()):
        """One local sample; violations raise and stop escalation."""
        self.lock.verify()
        sample = local_resources()
        stream.write(json.dumps({'local':sample})+'\n')
        stream.flush()
        floors(sample)
        quality_stopped()
        changes = unit_changes(self.baseline['local'], sample, allowed)
        require(not changes, 'unexpected restart or OOM: '+', '.join(changes))
        now = time.monotonic()
        if now >= getattr(self, 'next_remote', 0):
            remote = self.probe_all()
            stream.write(json.dumps({'remote':remote, 'unix':time.time()})+'\n')
            stream.flush()
            for host, value in remote.items():
                old = self.baseline['remote'][host]
                if host in allowed:
                    continue
                require(value['oom'] == old['oom'] and value['oom_kill'] == old['oom_kill'] and
                        value['restarts'] == old['restarts'] and value['pid'] == old['pid'],
                        'unexpected remote restart or OOM: '+host)
            self.next_remote = now+60

    def observe_freshness(self, window, stream):
        now = time.monotonic()
        height = node('getblockcount', [])
        if window.tip is not None and height >= window.tip and getattr(self, 'tip_hash', None) is not None and \
                node('getblockhash', [window.tip]) != self.tip_hash:
            window.reorganized(now)
        window.node(height, now)
        self.tip_hash = node('getblockhash', [height])
        public = tail(SHARD_ORIGIN+'/v1/shards')
        filters = tail(FILTER_ORIGIN+'/v1/filters/shards')
        require(public == filters or tail(FILTER_ORIGIN+'/v1/filters/shards') == tail(SHARD_ORIGIN+'/v1/shards'),
                'public origins disagree across a stable re-read')
        require(node('getblockhash', [public['end_height']]) == public['terminal_block_hash'], 'public endpoint is not canonical')
        window.serving('public', public['end_height'], time.monotonic())
        replicas = {}
        for host in self.hosts:
            worker = host['worker']
            if worker is not None and worker['role'] == 'recent-replica':
                value = tail('http://'+worker['upstream']+'/v1/shards')
                require(node('getblockhash', [value['end_height']]) == value['terminal_block_hash'], 'replica is not canonical: '+worker['id'])
                window.serving(worker['id'], value['end_height'], time.monotonic())
                replicas[worker['id']] = value['end_height']
        stream.write(json.dumps({'unix':time.time(), 'node':height, 'public':public['end_height'], 'replicas':replicas,
                                 'blocks':window.blocks()})+'\n')
        stream.flush()
        return window.violation(time.monotonic())

    def recent_ids(self):
        return [h['worker']['id'] for h in self.hosts if h['worker'] is not None and h['worker']['role'] == 'recent-replica']

    # Activities ---------------------------------------------------------------------
    def staged_load(self):
        raw = self.directory/'raw'
        raw.mkdir(mode=0o700)
        fixture = self.spec['load']['fixture']
        binary = artifact('examples/rate-query')
        results = []
        with (raw/'health.jsonl').open('x') as health, (raw/'freshness.jsonl').open('x') as fresh:
            window = FreshnessWindow(self.recent_ids(), time.monotonic())
            deadline = time.monotonic()+FRESHNESS['permit_seconds']
            while not window.permits():
                violation = self.observe_freshness(window, fresh)
                require(violation is None, 'freshness does not permit staged load: '+str(violation))
                require(time.monotonic() < deadline, 'no fresh canonical block observed; staged load not permitted')
                self.health(health)
                time.sleep(1)
            for index, stage in enumerate(LOAD_STAGES):
                directory = raw/('stage-%d-%dqps' % (index, stage['qps']))
                directory.mkdir(mode=0o700)
                permit = directory/'permit'
                permit.write_text('allow\n')
                files, processes = [], []
                failure = None
                try:
                    for client in range(stage['processes']):
                        out = (directory/('queries-%d.jsonl' % client)).open('xb')
                        err = (directory/('stderr-%d.log' % client)).open('xb')
                        files += [out, err]
                        processes.append(self.spawn('rate-%d-%d' % (index, client), [binary, '--url', SHARD_ORIGIN,
                            '--fixture', fixture['path'], '--qps', stage['qps']//stage['processes'], '--workers', 8,
                            '--seconds', stage['seconds'], '--permit', permit], out, err))
                    while any(p.poll() is None for p in processes):
                        try:
                            self.health(health)
                            violation = self.observe_freshness(window, fresh)
                            require(violation is None, str(violation))
                        except Exception as error:
                            failure = str(error)[:300]
                            break
                        permit.write_text('allow\n')
                        time.sleep(5)
                finally:
                    permit.write_text('deny\n')
                    self.stop_children()
                    for file in files:
                        file.close()
                evaluation = evaluate_rate([lines(directory/('queries-%d.jsonl' % c)) for c in range(stage['processes'])],
                                           stage['qps'], stage['seconds'], fixture['sha256'])
                if failure or any(p.returncode for p in processes):
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
        started = time.monotonic()
        with (raw/'health.jsonl').open('x') as health, (raw/'freshness.jsonl').open('x') as fresh, \
                (raw/'intervals.jsonl').open('x') as intervals:
            window = FreshnessWindow(self.recent_ids(), started)
            while True:
                self.health(health)
                try:
                    violation = self.observe_freshness(window, fresh)
                except (OSError, ValueError, KeyError, urllib.error.URLError) as error:
                    violation = 'observation failed: '+type(error).__name__
                failed = len(window.failed)
                if violation:
                    window.close(time.monotonic(), violation)
                for interval in window.failed[failed:]:
                    intervals.write(json.dumps(interval)+'\n')
                    intervals.flush()
                if window.qualifies(time.monotonic()):
                    return {'status':'passed', 'failures':[], 'freshness':window.record(time.monotonic())}
                if time.monotonic()-started > FRESHNESS['bound_seconds']:
                    return {'status':'failed', 'failures':['no qualifying interval within the fixed bound'],
                            'freshness':window.record(time.monotonic())}
                time.sleep(1)

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
        failure = None
        with (raw/'health.jsonl').open('x') as health, (raw/'freshness.jsonl').open('x') as fresh, \
                (raw/'loadtest.log').open('xb') as log:
            window = FreshnessWindow(self.recent_ids(), time.monotonic())
            process = self.spawn('loadtest', [binary, '--scenario', raw/'scenario.json', '--out-dir', raw/'loadtest'], log, log)
            while process.poll() is None:
                try:
                    self.health(health)
                    violation = self.observe_freshness(window, fresh)
                    require(violation is None, str(violation))
                except Exception as error:
                    failure = str(error)[:300]
                    break
                time.sleep(5)
            ended = time.time()
            self.stop_children()
        evaluation = evaluate_capacity(raw/'loadtest', request['level'], ended, failure is None, metrics)
        if failure:
            evaluation['failures'].append('stopped: '+failure)
            evaluation['status'] = 'failed'
        evaluation['loadtest_exit_code'] = process.returncode
        evaluation['freshness'] = window.record(time.monotonic())
        return {'status':evaluation['status'], 'failures':evaluation['failures'], 'capacity':evaluation}

    def probe(self, directory):
        """Exact canonical encrypted queries, then a reopened-wallet recovery."""
        directory.mkdir(mode=0o700)
        fixture = self.spec['load']['fixture']
        permit = directory/'permit'
        permit.write_text('allow\n')
        with (directory/'queries.jsonl').open('xb') as out, (directory/'stderr.log').open('xb') as err:
            process = self.spawn('probe-query', [artifact('examples/rate-query'), '--url', SHARD_ORIGIN, '--fixture',
                                 fixture['path'], '--qps', 5, '--workers', 8, '--seconds', 30, '--permit', permit], out, err)
            deadline = time.monotonic()+180
            while process.poll() is None:
                require(time.monotonic() < deadline, 'canonical query probe exceeded its bound')
                permit.write_text('allow\n')
                time.sleep(5)
        permit.write_text('deny\n')
        query = evaluate_rate([lines(directory/'queries.jsonl')], 5, 30, fixture['sha256'],
                              dict(LOAD_GATES, p50_below_seconds=math.inf, p99_below_seconds=math.inf, recent_fraction=(0, 1)))
        require(process.returncode == 0 and query['status'] == 'passed', 'canonical encrypted query probe failed')
        recovery = self.spec['routing']['recovery']['v11']
        wallet = R.run(artifact('transparent-loadtest'), recovery['binary_sha256'], recovery['sample'], recovery['sample_sha256'],
                       SCHEMA_V11, SHARD_ORIGIN, FILTER_ORIGIN, directory/'wallet', self.request['source_sha'])
        return {'query':query, 'wallet':{k:wallet.get(k) for k in ('status', 'native_report_sha256', 'sample_sha256')},
                'stores':len(wallet.get('observations', []))}

    def recover(self, raw, started, allowed):
        attempts = 0
        with (raw/'health.jsonl').open('a') as health:
            while True:
                attempts += 1
                try:
                    self.health(health, allowed)
                    result = self.probe(raw/('recovery-%02d' % attempts))
                    ready = self.readiness()
                    for worker, identity in ready.items():
                        old = self.baseline['ready'][worker]
                        require(identity['binary_sha256'] == old['binary_sha256'] and identity['map_sha256'] == old['map_sha256'],
                                'original baseline identity not preserved: '+worker)
                    if 'node_height' in self.record.get('effect', {}):
                        require(tail(SHARD_ORIGIN+'/v1/shards')['end_height'] >= self.record['effect']['node_height'],
                                'publication has not caught up to the pre-fault tip')
                    return {'recovered_seconds':time.time()-started, 'attempts':attempts, 'probe':result, 'ready':ready}
                except Exception as error:
                    with (raw/('recovery-%02d.error.json' % attempts)).open('x') as stream:
                        json.dump({'error_type':type(error).__name__, 'error':str(error)[:500], 'unix':time.time()}, stream)
                    require(time.time()-started < RECOVERY_SECONDS, 'recovery not established within 900 seconds')
                    time.sleep(15)

    def fault(self):
        request = self.request
        raw = self.directory/'raw'
        raw.mkdir(mode=0o700)
        with (raw/'health.jsonl').open('x') as health:
            self.health(health)
        before = self.probe(raw/'before')
        fault = request['fault']
        effect = {'fault':fault, 'status':'starting', 'started_unix':time.time()}
        self.record['effect'] = effect
        self.save(self.record)  # durable intent before the effect
        allowed = ()
        if fault == 'client-reopen':
            effect.update(self.client_reopen(raw))
        elif fault == 'publication-interruption':
            allowed = (PUBLISHER,)
            effect['node_height'] = node('getblockcount', [])
            self.save(self.record)
            self.lock.verify()
            H.Commands().unit('restart', PUBLISHER)
        elif fault == 'rollback-redeploy':
            # Root performed both through schema-rollback/schema-deploy; this owner
            # only binds their journals and never synthesizes either outcome.
            effect.update(self.rollback_evidence, source='retained schema journals')
            require(effect['rollback_seconds'] <= RECOVERY_SECONDS, 'rollback restore exceeded 900 seconds')
        else:
            role, operation = FAULT_ACTION[fault]
            host = request.get('target') or next(h['host'] for h in self.hosts if h['role'] == role)
            allowed = (host,)
            action = {'host':host, 'operation':operation, 'status':'starting'}
            self.record.setdefault('remote_actions', []).append(action)
            self.save(self.record)
            try:
                _, reply = self.remote(host, operation, 'act', 240)
                action['status'] = reply['status']
            finally:
                self.save(self.record)
            effect['remote'] = action
        effect['status'] = 'applied'
        self.save(self.record)
        recovered = self.recover(raw, time.time() if fault == 'rollback-redeploy' else effect['started_unix'], allowed)
        if fault == 'rollback-redeploy':
            # Restore time is the reviewed rollback itself; redeploy then needs exact recovery.
            recovered['restore_seconds'] = self.rollback_evidence['rollback_seconds']
        # Faulted units have new processes; quiescence is re-based after recovery.
        self.baseline = dict(self.baseline, local=local_resources(), remote=self.probe_all())
        with (raw/'health.jsonl').open('a') as health:
            self.health(health)
        return {'status':'passed', 'failures':[], 'before':before, 'effect':effect, 'recovery':recovered}

    def client_reopen(self, raw):
        """Terminate this owner's own wallet client mid-sync, then reopen its store."""
        recovery = self.spec['routing']['recovery']['v11']
        directory = raw/'interrupted-client'
        directory.mkdir(mode=0o700)
        stores = directory/'stores'
        with (directory/'native.log').open('xb') as log:
            process = self.spawn('interrupted-client', [artifact('transparent-loadtest'), '--shard-url', SHARD_ORIGIN,
                '--filter-url', FILTER_ORIGIN, '--sample', recovery['sample'], '--steps', 1, '--step-duration', '300s',
                '--http-attempts', 1, '--timeout', '60s', '--store-dir', stores, '--retain-stores',
                '--json-out', directory/'native.json', '--run-id', 'interrupted-client', '--source-sha', self.request['source_sha']], log, log)
            deadline = time.monotonic()+120
            while not any(stores.rglob('*.sqlite')) and process.poll() is None:
                require(time.monotonic() < deadline, 'owned client did not open a wallet store')
                time.sleep(.5)
            terminated = process.poll() is None
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
    raise InterruptedError('qualification owner interrupted')
