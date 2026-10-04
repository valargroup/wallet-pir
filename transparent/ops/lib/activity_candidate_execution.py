"""Closed, locked execution of the candidate's two offline native gate programs.

Root runs exactly two modes against the immutable initial v11 publication on
the pinned coordinator, with the candidate executables already retained by
CandidatePreparation:

- `artifact-verification`: one `shard-verify` with every explicit coverage,
  anchor, tier and map-file expectation and `--source-sha` of the candidate.
  No journal, `--data-dir` or rebuild.
- `native-certificates`: one `examples/native_certificate segment` per table
  segment of every pinned manifest, all 180, with fixed table paths.

The executable, argv and paths derive from the closed request; nothing is
caller-chosen. Stage and reconcile hold the global production lock in the
receiving process. Under it, before any mutation, the receiver surveys its own
host and requires a fresh survey of every other pinned inventory host, bound to
a nonce it issued after acquiring the lock. Each survey enumerates every
retained record of the schema, host-action and input-staging owner namespaces,
latest or not and of any kind, within fixed bounds, and associates every
recorded process with live processes by PID and start identity, session,
process group, cgroup and orphan start window. Any overflow, unreadable record
or process, unfinished candidate owner, lock holder, candidate process or live
association on any host refuses, and every survey reply is retained.

Children run one at a time under a closed guardian. The guardian is the session
leader and a child subreaper; it forks the native process, which applies the
hard address-space, CPU and core limits, dies with the guardian, and waits on a
gate pipe. The native program is executed only after its PID, start ticks and
launch token are durable. The guardian kills the native process and every
descendant at its own finite deadline even when the receiver is gone, so an
IO-blocked child cannot hold the lock past it. Each child inherits the lock
descriptor and is sampled for the 20% memory/disk floors with no gap over 10
seconds, as is all other work under the lock. A wall deadline per child and one
finite aggregate deadline per stage bound the run. Any failure stops it and
fences further mutation until reconciliation. Reconciliation first acquires the
production lock, so no recovery effect or owner write happens while any own
child, its guardian or an unknown process holds it; until then it retains its
observation and refuses. Under the lock it signals only token-bound processes
it proves its own through pidfds, and leaves ambiguous or escaped processes
untouched and the owner fenced.

Every raw stdout, stderr, owner, result and health sample is retained privately
under the candidate namespace with a `references.json` usable by
`activity_candidate_reports`. Native exit zero is not a gate pass; only the
offline producer evaluates the retained bytes. Nothing here touches a live
service, cache, unit, route or writer.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import secrets
import selectors
import shlex
import signal
import stat
import subprocess
import sys
import time

from wallet_pir_ops import durable, inherited_lock, owner_survey as S, schema_fence
from wallet_pir_ops.deploy.remote import ProductionLock, SSHExecutor

HERE = Path(__file__).parent


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


I = module('candidate_execution_input_stage', HERE/'activity_input_stage.py')
C, P = I.C, I.P
R = module('candidate_execution_reports', HERE/'activity_candidate_reports.py')
# One candidate identity instance for preparation, execution and the capture check.
R.C = C
VERSION = 2
KIND = 'candidate-native-execution'
SURVEY_KIND = 'candidate-execution-survey'
MODES = {'artifact-verification': 'shard-verify', 'native-certificates': 'examples/native_certificate'}
OWNERS = I.OWNERS
# The retained production owner namespaces every survey enumerates in full.
SCHEMA = schema_fence.SCHEMA_STATE
SOURCE = I.SOURCE
MACHINE_ID = Path('/etc/machine-id')
BOOT_ID = Path('/proc/sys/kernel/random/boot_id')
# The immutable initial publication, identified by its retained terminal record
# (transparent/evidence/activity-metadata-2026-09-30/full-publication-terminal.json).
PUBLICATION = P.OUTPUT
MAP_SHA256 = '34e3ebe3510206617460cebc87528e56f01949b971797e6f17cc8ca958f23f5d'
START = 0
THROUGH = P.THROUGH
ANCHOR_HASH = '00000000007b54884a0fdfd1c741e38ae1c538431126285a0ff410c288068665'
RECENT_FROM = 3289805
SHARDS = 90
SEGMENTS = 180
TABLES = (('directory', 'directory_segments'), ('pages', 'page_segments'))
ROW_BYTES = 4096
GiB = 1 << 30
# Hard per-child limits, enforced by the kernel (RLIMIT_AS, RLIMIT_CPU) and by
# the receiver's wall deadline. Sampled RSS is an observation, never a limit.
BUDGETS = {
    'native-certificates': {
        'wall_seconds': 600, 'cpu_seconds': 600, 'address_space_bytes': 14*GiB,
        'basis': 'root decision on the attempt-1 review: conservative closed certificate limits of 600 s and '
                 '14 GiB per segment, matching the prepared certificate driver RLIMIT_AS and per-segment CPU/wall'},
    'artifact-verification': {
        'wall_seconds': 1800, 'cpu_seconds': 1800, 'address_space_bytes': 14*GiB,
        'basis': 'root decision on the attempt-2 review: a conservative initial ceiling of the unchanged 1800 s '
                 'report-contract wall, equal CPU and 14 GiB, supported by streaming SegmentSource::verify in the '
                 'pinned source; an explicit ceiling, not a shard-verify measurement or qualification evidence'},
}
# Wrapper work under the lock outside native children: surveys, publication and
# bundle checks, table hashing, input retention, sampling between children.
# Root accepted it and the aggregates as finite operational maxima, not estimates.
NON_CHILD_SECONDS = 3600
CPU_GRACE_SECONDS = 5
FLOOR = .2
SAMPLE_SECONDS = 2
MAX_GAP_SECONDS = 10
STOP_SECONDS = 15
KILL_SECONDS = 5
MAX_STDOUT = R.MAX_JSON
MAX_STDERR = 1 << 20
MAX_HEADER = 8192
MAX_REPLY = 4 << 20
MAX_HOSTS = 32
MAX_SURVEY = 1 << 20
MAX_LISTED = 64
# Owner namespace enumeration bounds; reaching any refuses the survey.
MAX_OWNER_ENTRIES = 100000
MAX_OWNER_FILE = 16 << 20
MAX_OWNER_BYTES = 512 << 20
MAX_REFERENCES = 100000
MAX_SELECTED = 10000
# Operational classes: a live process whose executable or argv basename is one of
# these, or any tool name of the candidate, or whose executable or argv lies under
# these roots, refuses unless systemd placed it in a baseline unit's cgroup.
OPERATIONAL_NAMES = ('wallet-pir-deploy.py', 'transparent-block-server')
OPERATIONAL_ROOTS = ('/srv/transparent-activity/', '/srv/transparent-pir/')
# The real services that may run them, bound by exact unit cgroup and recorded
# with PID, start ticks, executable and its SHA-256.
BASELINE_UNITS = ('transparent-shard-server.service', 'transparent-filter-server.service',
                  'transparent-publish-controller.service', 'transparent-control-sessions.service',
                  'transparent-fleet-scaler.service', 'transparent-replica-reconciler.service',
                  'transparent-quality-rollout.service')
SURVEY_HOST_SECONDS = 120
SURVEY_WAIT_SECONDS = MAX_HOSTS*SURVEY_HOST_SECONDS + 120
HANDSHAKE_SECONDS = 300
TRANSPORT_MARGIN_SECONDS = 900
LOCK_WAIT_SECONDS = 30
# The pinned hosts' read-only ABI report (glibc 2.39) and the candidate's
# x86-64-v3 + pclmulqdq build flags.
GLIBC = 'glibc %d.%d' % C.GLIBC_CEILING
CPU_FLAGS = ('avx', 'avx2', 'bmi1', 'bmi2', 'fma', 'f16c', 'movbe', 'abm', 'xsave', 'sse4_2', 'popcnt', 'pclmulqdq')
STATUSES = ('absent', 'planned', 'preflight-passed', 'running', 'staged', 'failed', 'interrupted', 'reconciled')
UNFINISHED = ('running', 'failed', 'interrupted')
ENVIRONMENT = {'PATH': '/usr/sbin:/usr/bin:/sbin:/bin', 'LC_ALL': 'C', 'PYTHONDONTWRITEBYTECODE': '1'}
# Every launched process and its descendants carry the launch token here.
MARKER = 'WALLET_PIR_CANDIDATE_EXECUTION'
RECEIVE = b'schema-candidate-execute-receive'
NAME = re.compile('[A-Za-z0-9._-]{1,64}')
MACHINE = re.compile('[0-9a-f]{32}')
NONCE = re.compile('[0-9a-f]{64}')
TOKEN = re.compile('[0-9a-f]{64}:[0-9a-z-]{1,32}:[0-9a-f]{32}')
# Fixed guardian: session leader and child subreaper with its own deadline. It
# forks the native process, reports its PID on the notify pipe, and writes one
# terminal report. The native process dies with the guardian, applies the hard
# limits, and exits 125 without exec unless the receiver durably recorded it
# and sent `go`. At native exit, guardian deadline or SIGTERM, the guardian
# kills the native process and every descendant through pidfds after checking
# their start ticks, then reaps them. It never reads the gate itself.
GUARDIAN = r'''import ctypes, json, os, resource, select, signal, sys, time
gate, notify, report, address, cpu, grace = map(int, sys.argv[1:7])
wall = float(sys.argv[7])
argv = sys.argv[9:]
stop = []
signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
for name in ('SIGHUP', 'SIGINT', 'SIGPIPE'):
    signal.signal(getattr(signal, name), signal.SIG_IGN)
libc = ctypes.CDLL(None, use_errno=True)
guardian = os.getpid()
if libc.prctl(36, 1, 0, 0, 0) != 0:
    os._exit(126)
deadline = time.monotonic() + wall
native = os.fork()
if native == 0:
    try:
        if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != guardian:
            os._exit(125)
        for name in ('SIGTERM', 'SIGHUP', 'SIGINT', 'SIGPIPE'):
            signal.signal(getattr(signal, name), signal.SIG_DFL)
        os.close(notify)
        os.close(report)
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_AS, (address, address))
        resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu + grace))
        received = b''
        while len(received) < 3:
            data = os.read(gate, 3 - len(received))
            if not data:
                break
            received += data
        os.close(gate)
        if received == b'go\n':
            os.execve(argv[0], argv, os.environ)
    finally:
        os._exit(125)
os.close(gate)
pidfd = os.pidfd_open(native)
state = {'status': None, 'ended': None, 'ended_monotonic': None}
def stat(pid):
    try:
        with open('/proc/%d/stat' % pid) as stream:
            fields = stream.read().rsplit(')', 1)[1].split()
    except (OSError, IndexError):
        return None
    return fields[0], int(fields[1]), int(fields[19])
def reap():
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return
        if pid == native:
            state['status'], state['ended'] = status, time.time()
            state['ended_monotonic'] = time.monotonic()
def subtree():
    children = {}
    for name in os.listdir('/proc'):
        if name.isdigit():
            observed = stat(int(name))
            if observed and observed[0] != 'Z':
                children.setdefault(observed[1], []).append((int(name), observed[2]))
    found, pending = [], [guardian]
    while pending and len(found) < 4096:
        for item in children.get(pending.pop(), []):
            found.append(item)
            pending.append(item[0])
    return found
try:
    os.write(notify, b'%d\n' % native)
except OSError:
    pass
os.close(notify)
expired = stopped = False
while state['status'] is None:
    if (stop or time.monotonic() >= deadline) and not (expired or stopped):
        expired, stopped = not stop, bool(stop)
        try:
            signal.pidfd_send_signal(pidfd, signal.SIGKILL)
        except ProcessLookupError:
            pass
    select.select([pidfd], [], [], .2)
    reap()
os.close(pidfd)
killed, end = {}, time.monotonic() + 10
while time.monotonic() < end:
    members = subtree()
    if not members:
        break
    for pid, start in members:
        try:
            fd = os.pidfd_open(pid)
        except OSError:
            continue
        try:
            observed = stat(pid)
            if observed and observed[2] == start:
                signal.pidfd_send_signal(fd, signal.SIGKILL)
                killed[pid, start] = {'pid': pid, 'start_ticks': start}
        except OSError:
            pass
        finally:
            os.close(fd)
    time.sleep(.05)
    reap()
time.sleep(.05)
reap()
survivors = [{'pid': pid, 'start_ticks': start} for pid, start in subtree()]
value = {'native_pid': native, 'returncode': os.waitstatus_to_exitcode(state['status']),
         'ended_unix': state['ended'], 'ended_monotonic': state['ended_monotonic'],
         'expired': expired, 'stopped': stopped,
         'killed': list(killed.values())[:64], 'killed_count': len(killed), 'survivors': survivors[:64]}
os.write(report, json.dumps(value, sort_keys=True).encode() + b'\n')
os.fsync(report)
os.close(report)
os._exit(1 if survivors else 0)
'''
GUARDIAN_SHA256 = hashlib.sha256(GUARDIAN.encode()).hexdigest()
LAUNCH_REFUSED = 125
# The guardian's own deadline: the child wall budget plus a fixed launch window
# for the durable owner write and gate, measured from the guardian's start.
GUARDIAN_GRACE_SECONDS = 60
NOTIFY_SECONDS = 30
MAX_GUARDIAN_REPORT = 1 << 16
HEX = I.HEX
require = I.require
digest = I.digest
no_links = I.no_links


class Unknown(RuntimeError):
    """The remote outcome is not known locally; observe and reconcile its owner."""


class Interrupted(BaseException):
    """A signal stopped the receiver; its own child is stopped and retained."""


class Budget(ValueError):
    """A floor, limit, deadline, output bound or sampling gap stopped the run."""


class Blocked(ValueError):
    """Recovery cannot prove ownership; nothing foreign was signalled, the fence stays."""


def evidence_root():
    return C.ROOT/'executions'


def aggregate_seconds(mode):
    """The finite remote budget of one stage, lock acquisition to terminal owner."""
    children = 1 if mode == 'artifact-verification' else SEGMENTS
    return children*BUDGETS[mode]['wall_seconds'] + NON_CHILD_SECONDS


def validate(request):
    require(isinstance(request, dict) and set(request) == {'version', 'kind', 'mode', 'source_sha', 'candidate_sha',
            'candidate_identity', 'preparation_request_sha256', 'publication_sha256', 'coordinator', 'machine_id',
            'hosts', 'attempt'} and
            type(request['version']) is int and request['version'] == VERSION and request['kind'] == KIND and
            request['mode'] in MODES, 'invalid candidate execution request')
    require(isinstance(request['source_sha'], str) and re.fullmatch('[0-9a-f]{40}', request['source_sha']) and
            request['candidate_sha'] == C.SOURCE_SHA and request['candidate_identity'] == C.identity() and
            request['publication_sha256'] == MAP_SHA256 and
            isinstance(request['preparation_request_sha256'], str) and HEX.fullmatch(request['preparation_request_sha256']) and
            isinstance(request['machine_id'], str) and MACHINE.fullmatch(request['machine_id']) and
            type(request['attempt']) is int and 1 <= request['attempt'] <= 100,
            'candidate execution identity differs from the pinned candidate and publication')
    hosts = request['hosts']
    require(isinstance(hosts, list) and 1 <= len(hosts) <= MAX_HOSTS and
            all(isinstance(h, dict) and set(h) == {'host', 'machine_id'} and isinstance(h['host'], str) and
                NAME.fullmatch(h['host']) and isinstance(h['machine_id'], str) and MACHINE.fullmatch(h['machine_id'])
                for h in hosts) and
            [h['host'] for h in hosts] == sorted({h['host'] for h in hosts}) and
            len({h['machine_id'] for h in hosts}) == len(hosts) and
            {'host': request['coordinator'], 'machine_id': request['machine_id']} in hosts,
            'candidate execution host inventory is partial, duplicated or omits the coordinator')
    require(len(durable.canonical(request)) <= MAX_HEADER, 'candidate execution request exceeds bound')
    return request


def load(path, bound=1 << 20):
    path = Path(path)
    no_links(path)
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_size <= bound, 'candidate execution input is not a bounded regular file')
    return json.loads(path.read_bytes(), object_pairs_hook=I.unique)


def identity_of(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def immutable(path, kind):
    """Root-owned, not group/other writable, no links: publication bytes."""
    info = Path(path).lstat()
    require((stat.S_ISREG(info.st_mode) if kind == 'file' else stat.S_ISDIR(info.st_mode)) and
            info.st_uid == C.OWNER and not info.st_mode & 0o022,
            'publication namespace entry is not immutable root-owned '+kind)
    return info


def publication(root=None, tick=lambda: None):
    """The pinned initial publication: map, 90 manifests, 180 segments, exact files."""
    root = Path(root or PUBLICATION)
    no_links(root)
    immutable(root, 'directory')
    mapping_path = root/'shards.json'
    immutable(mapping_path, 'file')
    require(C.checksum(mapping_path) == MAP_SHA256, 'publication map file hash differs from the pinned publication')
    mapping = load(mapping_path, 1 << 24)
    rows = mapping.get('shards') if isinstance(mapping, dict) else None
    require(isinstance(rows, list) and len(rows) == SHARDS and
            [r.get('shard_id') if isinstance(r, dict) else None for r in rows] == list(range(SHARDS)),
            'publication shard inventory differs')
    require(mapping.get('start_height') == START and rows[0].get('start_height') == START and rows[-1].get('end_height') == THROUGH and
            rows[-1].get('terminal_block_hash') == ANCHOR_HASH and
            all(type(a.get('end_height')) is int and b.get('start_height') == a['end_height']+1 for a, b in zip(rows, rows[1:])),
            'publication coverage or anchor differs')
    require(all(r.get('geometry') == (P.GEOMETRIES['recent_geometry'] if r['start_height'] >= RECENT_FROM
                                      else P.GEOMETRIES['archive_geometry']) for r in rows) and
            any(r['start_height'] == RECENT_FROM for r in rows), 'publication tiers differ from the pinned cutoff')
    names = {r.get('manifest_digest') for r in rows}
    require(all(isinstance(n, str) and HEX.fullmatch(n) for n in names) and len(names) == SHARDS,
            'publication manifest digests are invalid or duplicated')
    require({e.name for e in os.scandir(root)} == names | {'shards.json'}, 'publication namespace has foreign or missing entries')
    manifests, segments = {}, []
    for row in rows:
        tick()
        directory = root/row['manifest_digest']
        immutable(directory, 'directory')
        manifest_path = directory/'manifest.json'
        immutable(manifest_path, 'file')
        require(C.checksum(manifest_path) == row['manifest_digest'], 'manifest bytes differ from the publication map')
        manifest = load(manifest_path)
        require(manifest.get('schema') == 'transparent-shard-v11' and manifest.get('shard_id') == row['shard_id'] and
                manifest.get('geometry') == row['geometry'], 'manifest identity differs')
        expected = {'manifest.json', 'filter.bin'}
        for table, key in TABLES:
            entries = manifest.get(key)
            require(isinstance(entries, list) and entries, 'manifest table segments missing')
            for index, item in enumerate(entries):
                require(isinstance(item, dict) and type(item.get('rows')) is int and item['rows'] > 0 and
                        item.get('row_bytes') == ROW_BYTES and isinstance(item.get('sha256'), str) and
                        HEX.fullmatch(item['sha256']), 'invalid manifest table segment')
                path = directory/('%s.%d.bin' % (table, index))
                info = immutable(path, 'file')
                require(info.st_size == item['rows']*ROW_BYTES, 'table segment file boundary differs from its manifest')
                expected.add(path.name)
                segments.append({'shard_id': row['shard_id'], 'table': table, 'segment': index,
                                 'geometry': row['geometry'], 'manifest_digest': row['manifest_digest'],
                                 'path': str(path), 'rows': item['rows'], 'size': info.st_size, 'sha256': item['sha256']})
        immutable(directory/'filter.bin', 'file')
        require({e.name for e in os.scandir(directory)} == expected, 'shard directory has foreign or missing files')
        manifests[row['manifest_digest']] = str(manifest_path)
    require(len(segments) == SEGMENTS and len({(s['shard_id'], s['table'], s['segment']) for s in segments}) == SEGMENTS,
            'publication does not have exactly 180 table segments')
    return {'root': str(root), 'mapping': str(mapping_path), 'manifests': manifests, 'segments': segments}


def dispatches(mode, binary, layout):
    """The closed child list; argv elements are fixed flags or pinned values."""
    if mode == 'artifact-verification':
        return [{'key': 'artifact', 'argv': [str(binary), '--shard-dir', layout['root'],
                 '--expect-start', str(START), '--expect-through', str(THROUGH),
                 '--expect-anchor-hash', ANCHOR_HASH, '--expect-recent-from', str(RECENT_FROM),
                 '--expect-recent-geometry', P.GEOMETRIES['recent_geometry'],
                 '--expect-archive-geometry', P.GEOMETRIES['archive_geometry'],
                 '--expect-map-sha256', MAP_SHA256, '--source-sha', C.SOURCE_SHA]}]
    return [{'key': '%03d-%s-%d' % (s['shard_id'], s['table'], s['segment']),
             'argv': [str(binary), 'segment', '--geometry', s['geometry'], '--table', s['table'], '--rows-bin', s['path']],
             'segment': {k: s[k] for k in ('shard_id', 'table', 'segment', 'geometry', 'manifest_digest',
                                           'path', 'rows', 'size', 'sha256')}}
            for s in layout['segments']]


def guardian_seconds(budget):
    return budget['wall_seconds']+GUARDIAN_GRACE_SECONDS


def guardian_argv(gate, notify, report, budget, argv):
    return [sys.executable, '-B', '-c', GUARDIAN, str(gate), str(notify), str(report),
            str(budget['address_space_bytes']), str(budget['cpu_seconds']), str(CPU_GRACE_SECONDS),
            str(guardian_seconds(budget)), '--', *argv]


def host_abi():
    """Runtime loader/CPU ABI of this host, compared with the pinned report."""
    flags = set()
    for line in Path('/proc/cpuinfo').read_text().splitlines():
        if line.startswith('flags'):
            flags = set(line.split(':', 1)[1].split())
            break
    return {'machine': platform.machine(), 'libc': os.confstr('CS_GNU_LIBC_VERSION'),
            'cpu_flags': sorted(set(CPU_FLAGS) & flags)}


def verify_host(observed):
    require(observed == {'machine': 'x86_64', 'libc': GLIBC, 'cpu_flags': sorted(CPU_FLAGS)},
            'runtime host ABI differs from the pinned candidate host ABI')
    return observed


# Process identity and the owner/process survey are the shared read-only component.
boot_id, process, booted_unix, started_unix, cgroup = S.boot_id, S.process, S.booted_unix, S.started_unix, S.cgroup
ancestors, recorded, associate = S.ancestors, S.recorded, S.associate


def me():
    current = process(os.getpid())
    return {'pid': os.getpid(), 'start_ticks': current['start_ticks'], 'boot_id': boot_id(),
            'session': current['session'], 'pgid': current['pgid'], 'cgroup': cgroup(os.getpid())}


def alive(recorded):
    """True only for the exact recorded process: same boot, PID and start ticks."""
    if recorded.get('boot_id') not in (None, boot_id()):
        return False
    current = process(recorded['pid'])
    return current is not None and current['state'] != 'Z' and current['start_ticks'] == recorded['start_ticks']


def scan(lock_path=None):
    """Every live process with its launch token, lock use and receiver role (see `owner_survey.scan`)."""
    return S.scan(lock_path, MARKER, RECEIVE)


def classify(processes, token, leader=None):
    """Split processes into this launch's own, escaped and ambiguous members.

    Own members carry the exact launch token and stay in the recorded
    guardian's session; without a recorded guardian (the launch window) the
    session of a token-bearing session leader, the guardian, counts. A token
    outside that session has escaped; a member of the session without the token
    is ambiguous. Either is preserved for inspection, never signalled.
    """
    own, escaped, ambiguous = [], [], []
    consistent = True
    if leader is not None:
        current = process(leader['pid'])
        consistent = (leader.get('boot_id') in (None, boot_id()) and
                      (current is None or current['state'] == 'Z' or current['start_ticks'] == leader['start_ticks']))
    else:
        leaders = {p['pid'] for p in processes if p['token'] == token and p['session'] == p['pid']}
    for item in processes:
        if item['pid'] == os.getpid():
            continue
        if leader is not None:
            member = consistent and item['session'] == leader['pid'] and item['start_ticks'] >= leader['start_ticks']
        else:
            member = item['session'] in leaders
        if item['token'] == token and not item['unreadable']:
            (own if member else escaped).append(item)
        elif member:
            ambiguous.append(item)
    return own, escaped, ambiguous


def summary(items):
    return S.summary(items, MAX_LISTED)


def signal_exact(pid, start_ticks, sig):
    """Signal one exact process through a pidfd; a reused PID is never touched."""
    try:
        fd = os.pidfd_open(pid)
    except ProcessLookupError:
        return False
    try:
        current = process(pid)
        if current is None or current['start_ticks'] != start_ticks:
            return False
        signal.pidfd_send_signal(fd, sig)
        return True
    except ProcessLookupError:
        return False
    finally:
        os.close(fd)


def stop_own(token, leader=None, wait=None):
    """TERM then KILL exactly this launch's own members; report what remains."""
    observed = {'signalled': [], 'escaped': [], 'ambiguous': [], 'survivors': []}
    for sig, seconds in ((signal.SIGTERM, STOP_SECONDS), (signal.SIGKILL, KILL_SECONDS)):
        own, escaped, ambiguous = classify(scan(), token, leader)
        observed['escaped'], observed['ambiguous'] = summary(escaped), summary(ambiguous)
        if not own:
            break
        for item in own:
            if signal_exact(item['pid'], item['start_ticks'], sig):
                observed['signalled'].append({'pid': item['pid'], 'start_ticks': item['start_ticks'],
                                              'signal': signal.Signals(sig).name})
        deadline = time.monotonic()+seconds
        while time.monotonic() < deadline:
            if wait is not None:
                wait()
            if not classify(scan(), token, leader)[0]:
                break
            time.sleep(.05)
    observed['survivors'] = summary(classify(scan(), token, leader)[0])
    return observed


def guardian_report(path, native):
    """The guardian's terminal report for this exact native process."""
    path = Path(path)
    no_links(path)
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= MAX_GUARDIAN_REPORT,
            'candidate guardian report is missing or exceeds bound')
    value = json.loads(path.read_bytes(), object_pairs_hook=I.unique)
    require(isinstance(value, dict) and set(value) == {'native_pid', 'returncode', 'ended_unix', 'ended_monotonic',
                                                       'expired', 'stopped',
                                                       'killed', 'killed_count', 'survivors'} and
            value['native_pid'] == native['pid'] and type(value['returncode']) is int and
            R.number(value['ended_unix']) and R.number(value['ended_monotonic']) and
            value['ended_monotonic'] > 0 and type(value['expired']) is bool and type(value['stopped']) is bool and
            type(value['killed_count']) is int and isinstance(value['killed'], list) and
            isinstance(value['survivors'], list), 'candidate guardian report is invalid')
    return value


def resources(paths):
    """One health sample: memory and every disk as available fractions."""
    memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    disks = {}
    for path in paths:
        path = Path(path)
        while not path.exists():
            path = path.parent
        disk = os.statvfs(path)
        disks[str(path)] = disk.f_bavail/disk.f_blocks
    return {'observed_unix': time.time(),
            'memory_available': int(memory['MemAvailable'].split()[0])/int(memory['MemTotal'].split()[0]),
            'disk_available': disks}


def rss(pids):
    total = 0
    for pid in pids:
        try:
            for line in Path('/proc/%d/status' % pid).read_text().splitlines():
                if line.startswith('VmRSS:'):
                    total += int(line.split()[1])*1024
        except (FileNotFoundError, ProcessLookupError):
            continue
    return total


class Sampler:
    """Resource samples across the whole locked interval, not only children.

    Every sample is kept. A gap over 10 seconds, a floor breach or the end of
    the aggregate budget fails the run; failure paths still sample but never
    raise from here.
    """

    def __init__(self, paths, deadline):
        self.paths, self.deadline = paths, deadline
        self.samples, self.stream, self.written = [], None, 0

    def attach(self, path):
        self.stream = open(path, 'x')
        os.chmod(path, 0o600)
        self.flush()

    def flush(self):
        if self.stream is not None:
            for sample in self.samples[self.written:]:
                self.stream.write(json.dumps(sample, sort_keys=True)+'\n')
            self.written = len(self.samples)
            self.stream.flush()
            os.fsync(self.stream.fileno())

    def close(self):
        if self.stream is not None:
            self.flush()
            os.fchmod(self.stream.fileno(), 0o400)
            self.stream.close()
            self.stream = None

    def sample(self, pids=(), strict=True):
        try:
            sample = resources(self.paths)
        except OSError:
            if strict:
                raise
            return None
        sample['sampled_rss_bytes'] = rss(pids)
        gap = bool(self.samples) and sample['observed_unix']-self.samples[-1]['observed_unix'] > MAX_GAP_SECONDS
        self.samples.append(sample)
        self.flush()
        if strict:
            if gap:
                raise Budget('resource sampling gap exceeded %d seconds' % MAX_GAP_SECONDS)
            floors(sample)
            if time.monotonic() > self.deadline:
                raise Budget('aggregate remote budget exhausted')
        return sample

    def tick(self, pids=()):
        if not self.samples or time.time()-self.samples[-1]['observed_unix'] >= SAMPLE_SECONDS:
            self.sample(pids)


def floors(sample):
    if not (sample['memory_available'] >= FLOOR and sample['disk_available'] and
            all(v >= FLOOR for v in sample['disk_available'].values())):
        raise Budget('memory or disk headroom below 20 percent')


def hashed(path, tick):
    sha = hashlib.sha256()
    with open(path, 'rb') as stream:
        while True:
            tick()
            data = stream.read(8 << 20)
            if not data:
                return sha.hexdigest()
            sha.update(data)


def staged_source():
    """The immutable reviewed source this wrapper runs from, by its receipt."""
    root = Path(__file__).resolve().parents[3]
    require(root.parent == SOURCE and re.fullmatch('[0-9a-f]{40}', root.name),
            'candidate execution requires immutable operation source')
    receipt = json.loads((SOURCE.parent/'staging'/(root.name+'.json')).read_text())
    I.S.verify_receipt(receipt, root, root.name, receipt['archive_sha256'])
    return root.name


def write_once(path, raw, mode=0o400):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fchmod(stream.fileno(), mode)
        os.fsync(stream.fileno())


def reference(path):
    return {'path': str(path), 'sha256': C.checksum(path)}


def lock_path(factory):
    return Path(factory().PATH)


def bounds():
    return dict(S.BOUNDS, entries=MAX_OWNER_ENTRIES, file_bytes=MAX_OWNER_FILE, json_bytes=MAX_OWNER_BYTES,
                references=MAX_REFERENCES, listed=MAX_LISTED, selected=MAX_SELECTED)


def classes():
    """The closed operational classes: every candidate and historical tool name, the wrapper, the roots."""
    return {'names': sorted({name.rsplit('/', 1)[-1] for name in C.ARTIFACTS} | set(OPERATIONAL_NAMES)),
            'roots': list(OPERATIONAL_ROOTS)}


def survey(request, identifier, nonce, host, *, skip=None, holder=None, path=None, owners=None, tick=lambda: None):
    """One host's owner/process reconciliation; read-only, bounded, raw JSON.

    `holder` is the receiver on the coordinator, the only process allowed to
    hold the production lock there. Other hosts must show no holder at all.
    """
    reasons = []
    entry = next((h for h in request['hosts'] if h['host'] == host), None)
    try:
        machine = MACHINE_ID.read_text().strip()
    except OSError as error:
        machine = None
        reasons.append('machine identity unreadable: %s' % type(error).__name__)
    if entry is None or machine != entry['machine_id'] or os.geteuid() != C.OWNER:
        reasons.append('host is not the pinned root machine')
    try:
        source = staged_source()
    except (OSError, ValueError, KeyError) as error:
        source = None
        reasons.append('operations source missing or unverified: %s' % str(error)[:120])
    if source not in (None, request['source_sha']):
        reasons.append('host runs another operations source')
    try:
        schema_fence.local_schema_fence(skip_input=skip)
        fence = 'clear'
    except (OSError, ValueError) as error:
        fence = str(error)[:200]
        reasons.append('fence: '+fence)
    path = path or lock_path(lambda: ProductionLock({'type': 'pinned_host', 'machine_id': request['machine_id']}))

    def select(label, name, record, sha):
        if label == 'input-staging' and HEX.fullmatch(name[:-5]) and isinstance(record, dict) and record.get('kind') == KIND:
            return {'name': name, 'sha256': sha, 'status': record.get('status')}

    def refuse(item):
        if item['status'] not in ('staged', 'reconciled') and item['name'] != '%s.json' % skip:
            return 'unfinished candidate execution owner: '+item['name']
    observed = S.observe((('schema', SCHEMA), ('host-actions', schema_fence.HOST_ACTIONS),
                          ('input-staging', Path(owners or OWNERS))),
                         classes=classes(), baseline=BASELINE_UNITS,
                         binding={'request_sha256': identifier, 'nonce': nonce, 'host': host, 'skip': skip},
                         lock_path=path, holder=holder, marker=MARKER, receiver=RECEIVE, tick=tick, bounds=bounds(),
                         select=select, refuse=refuse)
    reasons = observed['blocked']+reasons
    observed.update(kind=SURVEY_KIND, survey=S.KIND, request_sha256=identifier, nonce=nonce, host=host,
                    observed_unix=time.time(), observer=me(), skip=skip, machine_id=machine, source_sha=source,
                    fence=fence, owners=observed.pop('selected'), blocked=[r[:300] for r in reasons[:MAX_LISTED]],
                    blocked_count=observed['blocked_count']+len(reasons)-len(observed['blocked']),
                    status='blocked' if reasons else 'clear')
    return observed


def verify_survey(raw, request, identifier, nonce, host, skip, holder=None):
    """The receiver's own check of one retained host reply."""
    require(isinstance(raw, str) and len(raw.encode()) <= MAX_SURVEY, 'host survey reply missing or exceeds bound: '+host)
    try:
        value = json.loads(raw, object_pairs_hook=I.unique)
    except ValueError:
        raise ValueError('host survey reply is not JSON: '+host) from None
    entry = next(h for h in request['hosts'] if h['host'] == host)
    require(isinstance(value, dict) and value.get('kind') == SURVEY_KIND and value.get('request_sha256') == identifier and
            value.get('nonce') == nonce and value.get('host') == host and value.get('skip') == skip and
            value.get('machine_id') == entry['machine_id'] and value.get('source_sha') == request['source_sha'] and
            value.get('euid') == C.OWNER, 'host survey identity is foreign, stale or partial: '+host)
    require(value.get('status') == 'clear' and value.get('blocked') == [] and value.get('blocked_count') == 0 and
            value.get('fence') == 'clear' and value.get('processes') == [] and value.get('associated') == [] and
            value.get('associated_count') == 0 and value.get('unattributed') == [] and
            value.get('unattributed_count') == 0 and value.get('survey') == S.KIND and
            value.get('classes_sha256') == hashlib.sha256(S.canonical(classes())).hexdigest() and
            value.get('baseline') == list(BASELINE_UNITS) and value.get('bounds') == bounds() and
            value.get('binding') == {'request_sha256': identifier, 'nonce': nonce, 'host': host, 'skip': skip} and
            isinstance(value.get('inventory'), dict) and
            isinstance(value['inventory'].get('sha256'), str) and isinstance(value.get('lock'), dict) and
            isinstance(value['lock'].get('holders'), list) and
            [h.get('pid') for h in value['lock']['holders']] == ([holder['pid']] if holder else []) and
            isinstance(value.get('owners'), list) and
            all(o.get('status') in ('staged', 'reconciled') or skip and o.get('name') == skip+'.json'
                for o in value['owners']) and
            type(value.get('selected_count')) is int and len(value['owners']) == min(value['selected_count'], MAX_LISTED) and
            isinstance(value.get('selected_sha256'), str),
            'host survey shows a missing, unfinished or live owner: %s %s' % (host, value.get('blocked')))
    return value


class Lines:
    """Bounded newline-delimited reads from one descriptor with a deadline."""

    def __init__(self, fd):
        self.fd, self.buffer = fd, b''

    def read(self, timeout, bound=MAX_REPLY):
        deadline = time.monotonic()+timeout
        selector = selectors.DefaultSelector()
        selector.register(self.fd, selectors.EVENT_READ)
        try:
            while b'\n' not in self.buffer:
                require(len(self.buffer) <= bound, 'line exceeds bound')
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('no line before the deadline')
                if not selector.select(min(remaining, 1)):
                    continue
                data = os.read(self.fd, 65536)
                if not data:
                    raise EOFError('stream closed')
                self.buffer += data
        finally:
            selector.close()
        line, self.buffer = self.buffer.split(b'\n', 1)
        require(len(line) <= bound, 'line exceeds bound')
        return line


class Receiver:
    """Pinned root coordinator side; stdout carries the lock line and one bounded reply."""

    def __init__(self, request, *, owners=None, evidence=None, publication_root=None, lock_factory=None):
        self.request = validate(request)
        self.identifier = digest(request)
        self.mode = request['mode']
        self.executable = MODES[self.mode]
        self.budget = BUDGETS[self.mode]
        self.owners = Path(owners or OWNERS)
        self.owner = self.owners/(self.identifier+'.json')
        self.retained = self.owners/(self.identifier+'.request.json')
        self.evidence = Path(evidence or evidence_root())/self.identifier
        self.surveys = Path(evidence or evidence_root())/'surveys'/self.identifier
        self.root = Path(publication_root or PUBLICATION)
        self.lock_factory = lock_factory or (lambda: ProductionLock({'type': 'pinned_host', 'machine_id': request['machine_id']}))
        self.critical = self.pending = False

    def identity(self):
        require(os.geteuid() == C.OWNER and MACHINE_ID.read_text().strip() == self.request['machine_id'],
                'candidate execution machine differs from the coordinator pin')
        require(staged_source() == self.request['source_sha'], 'candidate execution names another operations source')

    def candidate(self):
        """The exact staged CandidatePreparation bundle of this candidate."""
        identifier = self.request['preparation_request_sha256']
        record = load(self.owners/(identifier+'.json'))
        retained = load(self.owners/(identifier+'.request.json'))
        files = {'provenance.json', *('artifacts/'+name for name in C.ARTIFACTS)}
        require(record.get('kind') == I.CandidatePreparation.KIND and record.get('status') == 'staged' and
                record.get('request_sha256') == identifier and digest(retained) == identifier and
                retained.get('candidate_sha') == C.SOURCE_SHA and record.get('target') == str(C.TARGET) and
                isinstance(record.get('files'), dict) and set(record['files']) == files and
                all(record['files']['artifacts/'+name] == sha for name, sha in C.ARTIFACTS.items()),
                'candidate execution requires the exact staged candidate preparation')
        require(C.verify_bundle(C.TARGET) == C.identity() and
                C.checksum(C.TARGET/'provenance.json') == record['files']['provenance.json'],
                'prepared candidate bundle changed')
        return C.binary(self.executable)

    def plan(self, tick=lambda: None):
        binary = self.candidate()
        tick()
        layout = publication(self.root, tick)
        host = verify_host(host_abi())
        budget = dict(self.budget, aggregate_seconds=aggregate_seconds(self.mode),
                      aggregate_scope='one stage on the coordinator, from production lock acquisition to the terminal '
                                      'owner record: host surveys, candidate and publication checks, table hashing, '
                                      'input retention, every native child and the sampling between them',
                      non_child_seconds=NON_CHILD_SECONDS, cpu_grace_seconds=CPU_GRACE_SECONDS,
                      guardian_seconds=guardian_seconds(self.budget),
                      transport_seconds=aggregate_seconds(self.mode)+TRANSPORT_MARGIN_SECONDS,
                      floor=FLOOR, sample_seconds=SAMPLE_SECONDS, max_gap_seconds=MAX_GAP_SECONDS,
                      max_stdout=MAX_STDOUT, max_stderr=MAX_STDERR, concurrency=1)
        return {'version': VERSION, 'kind': KIND, 'request': self.request, 'request_sha256': self.identifier,
                'mode': self.mode, 'executable': self.executable, 'binary': str(binary),
                'binary_sha256': C.ARTIFACTS[self.executable], 'candidate_identity': C.identity(),
                'guardian_sha256': GUARDIAN_SHA256,
                'publication': {'root': str(self.root), 'map_sha256': MAP_SHA256, 'start': START, 'through': THROUGH,
                                'anchor_hash': ANCHOR_HASH, 'recent_from': RECENT_FROM, 'shards': SHARDS,
                                'segments': SEGMENTS, 'manifests': sorted(layout['manifests'])},
                'host': host, 'hosts': self.request['hosts'], 'evidence': str(self.evidence), 'budgets': budget,
                'dispatches': dispatches(self.mode, binary, layout),
                'effects': 'runs the closed candidate children one at a time and writes private qualification '
                           'evidence only; no service, cache, unit, route or writer changes'}

    def status(self):
        no_links(self.owner)
        if not self.owner.exists():
            return {'status': 'absent', 'request_sha256': self.identifier}
        record = load(self.owner, 1 << 22)
        require(record.get('kind') == KIND and record.get('request_sha256') == self.identifier and
                load(self.retained) == self.request, 'retained candidate execution owner differs')
        if record['status'] == 'staged':
            references = record['references']
            R.blob(references)
            self.verify_references(R.value(references))
        return record

    def verify_references(self, references):
        if self.mode == 'artifact-verification':
            require(set(references) == {'mapping', 'execution'}, 'artifact references differ')
            executions = [references['execution']]
        else:
            require(set(references) == {'mapping', 'manifests', 'executions'} and
                    len(references['executions']) == SEGMENTS, 'certificate references differ')
            for item in references['manifests'].values():
                R.blob(item)
            executions = [item['execution'] for item in references['executions']]
        R.blob(references['mapping'])
        for execution in executions:
            R.capture(execution, self.executable, MAP_SHA256)

    def preflight(self):
        plan = self.checked()
        return {'status': 'preflight-passed', 'request_sha256': self.identifier, 'plan_sha256': digest(plan)}

    def checked(self, sampler=None):
        schema_fence.local_schema_fence()
        for path in (self.owners, self.owner, self.retained, self.evidence):
            no_links(path)
        require(self.status()['status'] == 'absent' and not self.evidence.exists() and not self.retained.exists(),
                'candidate execution already owned; inspect status or reconcile')
        plan = self.plan(sampler.tick if sampler else lambda: None)
        if sampler is None:
            floors(resources(self.paths()))
        return plan

    def paths(self):
        return (self.evidence.parent, self.root, Path('/'))

    def save(self, record):
        durable.atomic_json(self.owner, record, mode=0o600)

    def signalled(self, *_):
        if self.critical:
            self.pending = True
            return
        raise Interrupted('candidate execution receiver interrupted')

    def handlers(self):
        return {sig: signal.signal(sig, self.signalled) for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)}

    def handshake(self, channel, lock, tick, skip=None):
        """Fresh all-host reconciliation under the held lock, before mutation.

        Emits a nonce, waits for the root client's pinned surveys of every other
        inventory host, surveys this host itself, retains every raw reply,
        then refuses unless all are clear.
        """
        lock.verify()
        nonce = secrets.token_hex(32)
        holder = me()
        channel.emit({'phase': 'locked', 'request_sha256': self.identifier, 'nonce': nonce,
                      'coordinator': self.request['coordinator'], 'receiver': holder})
        deadline = time.monotonic()+SURVEY_WAIT_SECONDS
        while True:
            try:
                line = channel.read(min(SAMPLE_SECONDS, max(deadline-time.monotonic(), .01)))
                break
            except TimeoutError:
                require(time.monotonic() < deadline, 'host surveys did not arrive; nothing was mutated')
                tick()
        tick()
        try:
            remote = json.loads(line, object_pairs_hook=I.unique)
        except ValueError:
            raise ValueError('host survey bundle is not JSON; nothing was mutated') from None
        expected = sorted(h['host'] for h in self.request['hosts'] if h['host'] != self.request['coordinator'])
        surveys = remote.get('surveys') if isinstance(remote, dict) and set(remote) == {'surveys'} else None
        raw = dict(surveys) if isinstance(surveys, dict) else {}
        phase = 'reconcile' if skip else 'stage'
        try:
            local = survey(self.request, self.identifier, nonce, self.request['coordinator'], skip=skip, holder=holder,
                           path=lock_path(self.lock_factory), owners=self.owners, tick=tick)
            tick()
        except BaseException as error:
            # The remote replies already received are retained with the local failure.
            raw[self.request['coordinator']] = json.dumps({'host': self.request['coordinator'], 'nonce': nonce,
                                                           'error_type': type(error).__name__, 'error': str(error)[:300]})
            self.retain_surveys(nonce, raw, phase)
            raise
        raw[self.request['coordinator']] = json.dumps(local, sort_keys=True)
        retained = self.retain_surveys(nonce, raw, phase)
        require(isinstance(surveys, dict) and sorted(surveys) == expected,
                'host survey set is partial or foreign; retained at %s' % retained['path'])
        for host in sorted(raw):
            verify_survey(raw[host], self.request, self.identifier, nonce, host, skip,
                          holder if host == self.request['coordinator'] else None)
        lock.verify()
        return retained

    def retain_surveys(self, nonce, raw, phase):
        """Every survey attempt, clear or not, as raw immutable bytes."""
        no_links(self.surveys)
        self.surveys.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory = self.surveys/nonce
        directory.mkdir(mode=0o700)
        hosts = {}
        for host, text in raw.items():
            if not NAME.fullmatch(str(host)):
                continue
            data = text.encode() if isinstance(text, str) else durable.canonical(text)
            write_once(directory/(host+'.json'), data[:MAX_SURVEY+1])
            hosts[host] = reference(directory/(host+'.json'))
        index = {'version': 1, 'phase': phase, 'request_sha256': self.identifier, 'nonce': nonce,
                 'expected': [h['host'] for h in self.request['hosts']], 'hosts': hosts, 'retained_unix': time.time()}
        write_once(directory/'index.json', durable.canonical(index)+b'\n')
        I.sync_dir(directory)
        return reference(directory/'index.json')

    def stage(self, expected_plan, channel):
        require(isinstance(expected_plan, str) and HEX.fullmatch(expected_plan), 'pass the reviewed plan digest')
        handlers = self.handlers()
        try:
            with self.lock_factory() as lock:
                lock.verify()
                sampler = Sampler(self.paths(), time.monotonic()+aggregate_seconds(self.mode))
                sampler.sample()
                plan = self.checked(sampler)
                require(digest(plan) == expected_plan, 'candidate execution plan changed since review')
                surveyed = self.handshake(channel, lock, sampler.tick)
                self.owners.mkdir(parents=True, exist_ok=True, mode=0o700)
                self.evidence.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                durable.atomic_json(self.retained, self.request, mode=0o400)
                record = {'version': VERSION, 'kind': KIND, 'status': 'running', 'request_sha256': self.identifier,
                          'mode': self.mode, 'plan_sha256': expected_plan, 'evidence': str(self.evidence),
                          'receiver': me(), 'started_unix': time.time(), 'survey': surveyed,
                          'budgets': plan['budgets'], 'completed': 0, 'launch': None, 'child': None, 'recoveries': []}
                # Owner and the shared fence pointer exist before any child starts.
                self.save(record)
                durable.atomic_json(self.owners/'latest.json', {'request_sha256': self.identifier}, mode=0o600)
                try:
                    self.evidence.mkdir(mode=0o700)
                    sampler.attach(self.evidence/'health.ndjson')
                    write_once(self.evidence/'plan.json', durable.canonical(plan)+b'\n')
                    inputs = self.retain_inputs(plan, sampler)
                    executions = []
                    for item in plan['dispatches']:
                        executions.append(self.execute(lock, plan, item, record, sampler))
                        record['completed'] += 1
                        self.save(record)
                    references = self.references(inputs, plan, executions)
                    sampler.sample()
                    record.update(status='staged', references=references, launch=None, child=None,
                                  gate='unevaluated: run activity-candidate-report on the retained references')
                except BaseException as error:
                    record.update(status='interrupted' if isinstance(error, (Interrupted, KeyboardInterrupt, SystemExit))
                                  else 'failed', error_type=type(error).__name__, error=str(error)[:300])
                    raise
                finally:
                    self.critical = True
                    record['finished_unix'] = time.time()
                    sampler.sample(strict=False)
                    sampler.close()
                    record['samples'] = len(sampler.samples)
                    self.save(record)
                return record
        finally:
            self.critical = self.pending = False
            for sig, handler in handlers.items():
                signal.signal(sig, handler)

    def retain_inputs(self, plan, sampler):
        """Private single-link copies of the hash-checked map and manifests."""
        directory = self.evidence/'inputs'
        directory.mkdir(mode=0o700)
        raw = Path(self.root/'shards.json').read_bytes()
        require(hashlib.sha256(raw).hexdigest() == MAP_SHA256, 'publication map changed during execution')
        write_once(directory/'shards.json', raw)
        manifests = {}
        if self.mode == 'native-certificates':
            (directory/'manifests').mkdir(mode=0o700)
            for name in plan['publication']['manifests']:
                sampler.tick()
                raw = (self.root/name/'manifest.json').read_bytes()
                require(hashlib.sha256(raw).hexdigest() == name, 'manifest changed during execution')
                write_once(directory/'manifests'/(name+'.json'), raw)
                manifests[name] = reference(directory/'manifests'/(name+'.json'))
        return {'mapping': reference(directory/'shards.json'), 'manifests': manifests}

    def references(self, inputs, plan, executions):
        if self.mode == 'artifact-verification':
            value = {'mapping': inputs['mapping'], 'execution': executions[0]}
        else:
            value = {'mapping': inputs['mapping'], 'manifests': inputs['manifests'],
                     'executions': [{**{k: item['segment'][k] for k in ('shard_id', 'table', 'segment')},
                                     'execution': execution} for item, execution in zip(plan['dispatches'], executions)]}
        self.verify_references(value)
        path = self.evidence/'references.json'
        write_once(path, durable.canonical(value)+b'\n')
        return reference(path)

    def table(self, item, sampler):
        """Fixed table path: identity, boundary and hash before the child reads it."""
        segment = item['segment']
        path = Path(segment['path'])
        require(path == self.root/segment['manifest_digest']/('%s.%d.bin' % (segment['table'], segment['segment'])),
                'certificate table path is not the fixed publication path')
        no_links(path)
        info = immutable(path, 'file')
        require(info.st_size == segment['size'] and hashed(path, sampler.tick) == segment['sha256'] and
                identity_of(path.lstat()) == identity_of(info), 'certificate table bytes differ from the manifest')
        return identity_of(info)

    def execute(self, lock, plan, item, record, sampler):
        """One native child: verified inputs, gated launch, sampled run, raw capture."""
        lock.verify()
        budget = self.budget
        if time.monotonic()+budget['wall_seconds'] > sampler.deadline:
            raise Budget('aggregate remote budget cannot cover another child; nothing was started')
        directory = self.evidence/item['key']
        directory.mkdir(mode=0o700)
        binary = C.binary(self.executable)
        require(str(binary) == plan['binary'] == item['argv'][0], 'candidate executable path differs from plan')
        sampler.tick()
        C.abi(self.executable, binary.read_bytes())
        verify_host(host_abi())
        sampler.tick()
        before = self.table(item, sampler) if self.mode == 'native-certificates' else None
        files = {name: directory/name for name in ('owner.json', 'native.json', 'stderr.log', 'health.json', 'result.json',
                                                   'guardian.json')}
        child = failure = ended = started = started_monotonic = leader = native = report = None
        token = '%s:%s:%s' % (self.identifier, item['key'], secrets.token_hex(16))
        first = len(sampler.samples)
        gate = notify = None
        stdout = os.open(files['native.json'], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        stderr = os.open(files['stderr.log'], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            sampler.sample()
            first = len(sampler.samples)-1
            # Launch intent is durable before fork; the native process cannot
            # exec until its own and its guardian's identity are durable too.
            record['launch'] = {'key': item['key'], 'token': token, 'intent_unix': time.time()}
            self.save(record)
            self.critical = True
            try:
                started = time.time()
                started_monotonic = time.monotonic()
                descriptors = lock.descriptors()
                gate_read, gate = os.pipe()
                notify, notify_write = os.pipe()
                written = os.open(files['guardian.json'], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                try:
                    child = subprocess.Popen(
                        guardian_argv(gate_read, notify_write, written, budget, item['argv']), stdin=subprocess.DEVNULL,
                        stdout=stdout, stderr=stderr, cwd=directory, start_new_session=True, close_fds=True,
                        pass_fds=(*descriptors, gate_read, notify_write, written),
                        env=dict(ENVIRONMENT, **{inherited_lock.VARIABLE: ','.join(map(str, descriptors)), MARKER: token}))
                finally:
                    for fd in (gate_read, notify_write, written):
                        os.close(fd)
                current = process(child.pid)
                require(current is not None and current['state'] != 'Z', 'candidate guardian exited before launch')
                leader = {'pid': child.pid, 'start_ticks': current['start_ticks'], 'boot_id': boot_id()}
                native = self.native(child, notify)
                os.close(notify)
                notify = None
                deadline_unix = started+guardian_seconds(budget)
                owner = {'version': VERSION, 'kind': KIND, 'request_sha256': self.identifier, 'key': item['key'],
                         'mode': self.mode, 'executable': self.executable, 'binary': str(binary),
                         'binary_sha256': C.ARTIFACTS[self.executable], 'argv': item['argv'],
                         'guardian_sha256': GUARDIAN_SHA256, 'token': token,
                         'native_source_sha': C.SOURCE_SHA, 'candidate_sha256': C.identity(),
                         'publication_sha256': MAP_SHA256, 'operations_source_sha': self.request['source_sha'],
                         **native, 'boot_id': leader['boot_id'], 'session': child.pid, 'pgid': child.pid,
                         'guardian': leader, 'guardian_deadline_unix': deadline_unix, 'started_unix': started,
                         'started_monotonic': started_monotonic,
                         'timeout_seconds': budget['wall_seconds'], 'guardian_seconds': guardian_seconds(budget),
                         'limits': {k: budget[k] for k in ('cpu_seconds', 'address_space_bytes')},
                         'receiver': record['receiver'], 'host': plan['host'], 'segment': item.get('segment')}
                write_once(files['owner.json'], durable.canonical(owner)+b'\n')
                record['child'] = {'key': item['key'], 'token': token, **leader, 'native': native,
                                   'deadline_unix': deadline_unix}
                self.save(record)
                if not self.pending:
                    os.write(gate, b'go\n')
                os.close(gate)
                gate = None
            finally:
                self.critical = False
            if self.pending:
                raise Interrupted('candidate execution receiver interrupted')
            deadline = started_monotonic+budget['wall_seconds']
            while True:
                try:
                    child.wait(timeout=SAMPLE_SECONDS)
                    break
                except subprocess.TimeoutExpired:
                    pass
                lock.verify()
                sampler.sample([p['pid'] for p in classify(scan(), token, leader)[0]])
                if os.fstat(stdout).st_size > MAX_STDOUT or os.fstat(stderr).st_size > MAX_STDERR:
                    raise Budget('native output exceeded its bound')
                if time.monotonic() > deadline:
                    raise Budget('native child exceeded the wall deadline')
            sampler.sample()
            report = guardian_report(files['guardian.json'], native)
            ended = report['ended_unix']
            require(0 < report['ended_monotonic']-started_monotonic <= budget['wall_seconds'] and
                    report['ended_monotonic'] <= time.monotonic(),
                    'native child exceeded the observed monotonic execution bound')
            require(os.fstat(stdout).st_size <= MAX_STDOUT and os.fstat(stderr).st_size <= MAX_STDERR,
                    'native output exceeded its bound')
            require(not report['expired'], 'native child exceeded the guardian deadline')
            require(report['returncode'] != LAUNCH_REFUSED, 'native launch gate refused')
            require(report['returncode'] == 0, 'native child exited %s' % report['returncode'])
            require(not report['killed_count'] and child.returncode == 0 and not any(classify(scan(), token, leader)),
                    'native child left live descendants')
            if before is not None:
                require(identity_of(Path(item['segment']['path']).lstat()) == before, 'certificate table changed during execution')
            require(hashed(binary, sampler.tick) == C.ARTIFACTS[self.executable], 'candidate executable changed during execution')
        except BaseException as error:
            # Cleanup and retention finish before any later signal is honoured.
            self.critical, failure = True, error
            for fd in (gate, notify):
                if fd is not None:
                    os.close(fd)
            gate = notify = None
            if child is not None:
                # The receiver holds the production lock here: these are lock-owned effects.
                stopped = stop_own(token, leader, wait=lambda: sampler.sample(strict=False))
                if not (stopped['escaped'] or stopped['ambiguous'] or stopped['survivors']):
                    child.wait()
                else:
                    failure = Blocked('native child left processes it could not prove its own: %s' % stopped)
                if report is None and native is not None:
                    try:
                        report = guardian_report(files['guardian.json'], native)
                    except (OSError, ValueError):
                        pass
                sampler.sample(strict=False)
            if ended is None:
                ended = report['ended_unix'] if report and report['ended_unix'] else time.time()
            raise failure
        finally:
            self.critical = True
            for fd in (stdout, stderr):
                os.fsync(fd); os.fchmod(fd, 0o400); os.close(fd)
            if files['guardian.json'].exists():
                os.chmod(files['guardian.json'], 0o400)
            write_once(files['health.json'], json.dumps(sampler.samples[first:], sort_keys=True).encode()+b'\n')
            interrupted = isinstance(failure, (Interrupted, KeyboardInterrupt, SystemExit))
            code = report['returncode'] if report else None
            result = {'status': 'passed' if failure is None else 'interrupted' if interrupted else 'failed',
                      'exit_code': code, 'pid': native['pid'] if native else None,
                      'start_ticks': native['start_ticks'] if native else None,
                      'started_unix': started, 'ended_unix': ended, 'timeout_seconds': budget['wall_seconds'],
                      'ended_monotonic': report['ended_monotonic'] if report else time.monotonic(),
                      'limits': {k: budget[k] for k in ('cpu_seconds', 'address_space_bytes')},
                      'max_sampled_rss_bytes': max((s.get('sampled_rss_bytes', 0) for s in sampler.samples[first:]),
                                                   default=0),
                      'guardian': {'pid': child.pid if child else None, 'exit_code': child.returncode if child else None,
                                   'report': reference(files['guardian.json']) if report else None}}
            if type(code) is int and code < 0:
                result['signal'] = signal.Signals(-code).name
            if failure is not None:
                result.update(error_type=type(failure).__name__, error=str(failure)[:300])
            write_once(files['result.json'], durable.canonical(result)+b'\n')
            I.sync_dir(directory)
        refs = {'owner': reference(files['owner.json']), 'result': reference(files['result.json']),
                'native': reference(files['native.json']), 'stderr': reference(files['stderr.log']),
                'health': reference(files['health.json'])}
        # The shared raw capture contract; gate evaluation stays offline.
        R.capture(refs, self.executable, MAP_SHA256)
        record['launch'] = record['child'] = None
        self.critical = False
        if self.pending:
            raise Interrupted('candidate execution receiver interrupted')
        return refs

    def native(self, guardian, notify):
        """The guardian's forked native process, gated and not yet executed."""
        try:
            line = Lines(notify).read(NOTIFY_SECONDS, 32)
        except (TimeoutError, EOFError):
            raise ValueError('candidate guardian did not report its native process') from None
        require(re.fullmatch(b'[1-9][0-9]{0,9}', line), 'invalid candidate guardian report')
        current = process(int(line))
        require(current is not None and current['state'] != 'Z' and current['ppid'] == guardian.pid and
                current['session'] == guardian.pid, 'native process is not the guardian\'s gated child')
        return {'pid': current['pid'], 'start_ticks': current['start_ticks']}

    def reconcile(self, channel):
        """Acquire the production lock first; then stop only own processes and reconcile every host."""
        record = self.status()
        require(load(self.owners/'latest.json') == {'request_sha256': self.identifier},
                'reconcile the latest input owner first')
        require(record['status'] in UNFINISHED, 'candidate execution does not need reconciliation')
        handlers = self.handlers()
        evidence = {'version': 1, 'kind': KIND+'-reconciliation', 'request_sha256': self.identifier,
                    'observer': me(), 'started_unix': time.time(), 'receiver': record.get('receiver'),
                    'launch': record.get('launch'), 'child': record.get('child')}
        try:
            lock = self.acquire(record, evidence)
            try:
                lock.verify()
                require(self.status() == record, 'candidate execution owner changed before the lock was held')
                attempt = {'claimant': evidence['observer'], 'locked_unix': time.time(), 'outcome': 'locked'}
                record.setdefault('recoveries', []).append(attempt)
                # The first owner write of this recovery, made under the production lock.
                self.save(record)
                evidence['attempt'] = len(record['recoveries'])
                name = 'reconciliation-%d.json' % evidence['attempt']
                try:
                    outcome = self.recover(record, evidence, channel, lock)
                except BaseException as error:
                    attempt.update(outcome='blocked', error_type=type(error).__name__, error=str(error)[:300],
                                   ended_unix=time.time())
                    evidence['outcome'] = attempt
                    attempt['evidence'] = self.retain_reconciliation(evidence, name)
                    self.save(record)
                    raise
                attempt.update(outcome='reconciled', ended_unix=time.time())
                evidence['outcome'] = attempt
                attempt['evidence'] = self.retain_reconciliation(evidence, name)
                record.update(status='reconciled', reconciled_unix=time.time(), reconciliation=outcome,
                              launch=None, child=None)
                self.save(record)
                return record
            finally:
                lock.__exit__(None, None, None)
        finally:
            for sig, handler in handlers.items():
                signal.signal(sig, handler)

    def acquire(self, record, evidence):
        """The production lock, or a retained refusal that signalled and wrote nothing.

        While the receiver lives, or while any process holds the lock, recovery
        has no effect at all. Own children keep the lock until their guardian's
        deadline has killed them; the refusal says which case applies.
        """
        evidence['receiver_alive'] = alive(record['receiver'])
        message = 'candidate execution receiver is still active; observe it before reconciliation'
        if not evidence['receiver_alive']:
            deadline = time.monotonic()+LOCK_WAIT_SECONDS
            while True:
                try:
                    return self.lock_factory().__enter__()
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        break
                    time.sleep(.2)
        processes = scan(lock_path(self.lock_factory))
        holders = [p for p in processes if p['holds']]
        launch, child = record.get('launch'), record.get('child')
        own = []
        if launch:
            own = classify(processes, launch['token'], child if child and child.get('token') == launch['token'] else None)[0]
        evidence['holders'], evidence['own'] = summary(holders), summary(own)
        if not evidence['receiver_alive']:
            if holders and {p['pid'] for p in holders} <= {p['pid'] for p in own}:
                limit = child.get('deadline_unix') if child else None
                if limit is not None and time.time() < limit+STOP_SECONDS:
                    message = ('own guardian holds the production lock until its deadline %d; nothing was signalled, '
                               'reconcile after it' % limit)
                else:
                    message = 'own processes hold the production lock past their guardian deadline; preserved, nothing signalled'
            else:
                message = ('the production lock is held by a process this owner cannot prove its own; '
                           'preserved, nothing signalled')
        evidence['outcome'] = {'outcome': 'blocked-unlocked', 'error': message, 'ended_unix': time.time()}
        self.retain_reconciliation(evidence, 'reconciliation-unlocked-%d-%s.json' % (time.time_ns(), secrets.token_hex(4)))
        raise Blocked(message)

    def recover(self, record, evidence, channel, lock):
        """Under the held production lock: stop own processes that released it, then survey every host."""
        launch, child = record.get('launch'), record.get('child')
        processes = scan(lock_path(self.lock_factory))
        evidence['holders_locked'] = summary([p for p in processes if p['holds'] and p['pid'] != os.getpid()])
        if child:
            report = self.evidence/child['key']/'guardian.json'
            if report.exists() and report.stat().st_size:
                evidence['guardian'] = reference(report)
        own, escaped, ambiguous = [], [], []
        if launch:
            leader = child if child and child.get('token') == launch['token'] else None
            own, escaped, ambiguous = classify(processes, launch['token'], leader)
        evidence['before'] = {'own': summary(own), 'escaped': summary(escaped), 'ambiguous': summary(ambiguous)}
        if escaped or ambiguous:
            raise Blocked('processes of this launch escaped or cannot be attributed; preserved, nothing signalled')
        observed = {'action': 'no-live-child'}
        if own:
            lock.verify()
            stopped = stop_own(launch['token'], leader)
            evidence['stopped'] = stopped
            if stopped['escaped'] or stopped['ambiguous'] or stopped['survivors']:
                raise Blocked('own processes survived or became unattributable during recovery')
            observed = {'action': 'terminated-own-processes', 'signalled': stopped['signalled']}
        lock.verify()
        schema_fence.local_schema_fence(skip_input=self.identifier)
        current = self.status()
        require(current['status'] == record['status'] and current.get('recoveries') == record['recoveries'],
                'candidate execution owner changed during reconciliation')
        # Recovery itself runs no child; resource floors never block it.
        observed['survey'] = evidence['survey'] = self.handshake(channel, lock, lambda: None, skip=self.identifier)
        lock.verify()
        return observed

    def retain_reconciliation(self, evidence, name):
        no_links(self.evidence)
        self.evidence.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = self.evidence/name
        write_once(path, durable.canonical(evidence)+b'\n')
        I.sync_dir(self.evidence)
        return reference(path)


class Channel:
    """The receiver's stdout lines and stdin lines over one SSH session."""

    def __init__(self, lines, out):
        self.lines, self.out = lines, out

    def emit(self, value):
        self.out(json.dumps(value, sort_keys=True))
        sys.stdout.flush()

    def read(self, timeout):
        try:
            return self.lines.read(timeout, MAX_HOSTS*MAX_SURVEY*2).decode()
        except EOFError:
            raise ValueError('host surveys did not arrive; nothing was mutated') from None


def read_header(lines):
    try:
        raw = lines.read(30, MAX_HEADER*2)
    except (EOFError, TimeoutError):
        raise ValueError('invalid candidate execution request header') from None
    return raw


def receive(action, expected, stdin_fd, out, expect_plan=None):
    """`schema-candidate-execute-receive`: one fixed action on a pinned host."""
    def reply(value):
        try:
            out(json.dumps(value, sort_keys=True))
        except OSError:
            # Lost transport: the durable owner is the outcome.
            pass
    lines = Lines(stdin_fd)
    try:
        require(isinstance(expected, str) and HEX.fullmatch(expected), 'invalid candidate execution request digest')
        if action == 'survey':
            raw = read_header(lines)
            require(len(raw) <= MAX_HEADER*2, 'invalid survey envelope')
            envelope = json.loads(raw, object_pairs_hook=I.unique)
            require(isinstance(envelope, dict) and set(envelope) == {'request', 'nonce', 'host', 'skip'} and
                    isinstance(envelope['nonce'], str) and NONCE.fullmatch(envelope['nonce']) and
                    envelope['skip'] in (None, expected), 'invalid survey envelope')
            request = validate(envelope['request'])
            require(digest(request) == expected and envelope['host'] in [h['host'] for h in request['hosts']] and
                    envelope['host'] != request['coordinator'], 'survey names another request or host')
            reply(survey(request, expected, envelope['nonce'], envelope['host'], skip=envelope['skip']))
            return 0
        if action in ('plan', 'preflight', 'stage'):
            raw = read_header(lines)
            require(len(raw) <= MAX_HEADER, 'invalid candidate execution request header')
            request = validate(json.loads(raw, object_pairs_hook=I.unique))
            require(digest(request) == expected, 'candidate execution request differs from reviewed digest')
            receiver = Receiver(request)
        else:
            retained = OWNERS/(expected+'.request.json')
            no_links(retained)
            if not retained.exists():
                staged_source()
                reply({'request_sha256': expected, 'status': 'absent'})
                return 0
            request = validate(load(retained, MAX_HEADER))
            require(digest(request) == expected, 'retained candidate execution request differs')
            receiver = Receiver(request)
        receiver.identity()
        channel = Channel(lines, out)
        if action == 'plan':
            plan = receiver.plan()
            result = {'status': 'planned', 'request_sha256': expected, 'plan': plan, 'plan_sha256': digest(plan)}
        elif action == 'stage':
            result = receiver.stage(expect_plan, channel)
        elif action == 'reconcile':
            result = receiver.reconcile(channel)
        elif action == 'status':
            result = dict(receiver.status(), request=receiver.request)
        else:
            result = receiver.preflight()
    except BaseException as error:
        interrupted = isinstance(error, (Interrupted, KeyboardInterrupt, SystemExit))
        value = {'request_sha256': str(expected)[:64], 'status': 'interrupted' if interrupted else 'failed'}
        if isinstance(error, ValueError):
            value['error'] = str(error)[:600]
        reply(value)
        return 75 if interrupted else 1
    reply({k: v for k, v in result.items() if k in ('request_sha256', 'status', 'plan', 'plan_sha256', 'mode',
           'completed', 'references', 'evidence', 'child', 'launch', 'gate', 'error_type', 'error', 'reconciliation',
           'survey', 'recoveries', 'request')})
    return 0


class Execution:
    """Root workstation client against the inventory's pinned remote coordinator."""

    def __init__(self, inventory, source_sha, *, mode=None, attempt=None, preparation=None, request_sha256=None):
        require(inventory.lock.get('type') == 'remote', 'candidate execution requires a remote coordinator inventory')
        require(inventory.ssh.get('mode') == 'pinned', 'candidate execution requires pinned SSH host keys')
        self.host = inventory.lock['host']
        require(1 <= len(inventory.hosts) <= MAX_HOSTS, 'candidate execution inventory host count is out of bounds')
        for name, entry in inventory.hosts.items():
            require(NAME.fullmatch(name) and isinstance(entry.get('machine_id'), str) and
                    MACHINE.fullmatch(entry['machine_id']),
                    'candidate execution requires a machine_id pin for every inventory host')
            require(entry.get('user', inventory.ssh.get('user', 'root')) == 'root' or entry.get('sudo'),
                    'candidate execution requires root or the configured sudo identity on every host')
        require(isinstance(source_sha, str) and re.fullmatch('[0-9a-f]{40}', source_sha), 'invalid operations source')
        self.inventory, self.source = inventory, source_sha
        self.machine = inventory.hosts[self.host]['machine_id']
        self.hosts = [{'host': name, 'machine_id': inventory.hosts[name]['machine_id']} for name in sorted(inventory.hosts)]
        self.mode, self.attempt, self.preparation, self.expected = mode, attempt, preparation, request_sha256
        self.executor = SSHExecutor(inventory)

    def request(self):
        return validate({'version': VERSION, 'kind': KIND, 'mode': self.mode, 'source_sha': self.source,
                         'candidate_sha': C.SOURCE_SHA, 'candidate_identity': C.identity(),
                         'preparation_request_sha256': self.preparation, 'publication_sha256': MAP_SHA256,
                         'coordinator': self.host, 'machine_id': self.machine, 'hosts': self.hosts,
                         'attempt': self.attempt})

    def remote(self, host, action, identifier, expect_plan=None):
        entry = self.inventory.hosts[host]
        prefix = ['sudo', '-n', '--'] if entry.get('sudo') else []
        ssh = self.executor.transport(host)
        ssh = [*ssh[:-1], '-oControlMaster=no', '-oControlPath=none', ssh[-1]]
        remote = [*prefix, '/usr/bin/python3', '-B', str(SOURCE/self.source/'ops/scripts/wallet-pir-deploy.py'),
                  'schema-candidate-execute-receive', '--action', action, '--request-sha256', identifier]
        if expect_plan is not None:
            remote += ['--expect-plan-sha256', expect_plan]
        return [*ssh, shlex.join(remote)]

    def argv(self, action, identifier, expect_plan=None):
        return self.remote(self.host, action, identifier, expect_plan)

    def survey_argv(self, host, identifier):
        return self.remote(host, 'survey', identifier)

    def survey(self, host, request, identifier, nonce, skip):
        """One pinned host's raw survey reply; transport failures stay visible."""
        envelope = durable.canonical({'request': request, 'nonce': nonce, 'host': host, 'skip': skip})+b'\n'
        try:
            result = subprocess.run(self.survey_argv(host, identifier), input=envelope, capture_output=True,
                                    timeout=SURVEY_HOST_SECONDS)
        except subprocess.TimeoutExpired:
            return json.dumps({'host': host, 'transport': 'timeout'})
        raw = result.stdout[:MAX_SURVEY+1].decode(errors='replace').strip()
        if result.returncode or not raw:
            return json.dumps({'host': host, 'transport': 'exit %d' % result.returncode, 'reply': raw[:2000]})
        return raw

    def call(self, action, identifier, header=b'', timeout=600, expect_plan=None, request=None, skip=None):
        argv = self.argv(action, identifier, expect_plan)
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        lines = Lines(process.stdout.fileno())
        try:
            try:
                if header:
                    process.stdin.write(header)
                    process.stdin.flush()
                if action not in ('stage', 'reconcile'):
                    process.stdin.close()
                raw = lines.read(HANDSHAKE_SECONDS if action in ('stage', 'reconcile') else timeout)
                first = json.loads(raw, object_pairs_hook=I.unique)
                if isinstance(first, dict) and first.get('phase') == 'locked':
                    require(first.get('request_sha256') == identifier and isinstance(first.get('nonce'), str) and
                            NONCE.fullmatch(first['nonce']) and first.get('coordinator') == self.host and
                            action in ('stage', 'reconcile'), 'invalid candidate execution lock line')
                    surveys = {h['host']: self.survey(h['host'], request, identifier, first['nonce'], skip)
                               for h in request['hosts'] if h['host'] != self.host}
                    process.stdin.write(durable.canonical({'surveys': surveys})+b'\n')
                    process.stdin.close()
                    raw = lines.read(timeout)
            except (TimeoutError, EOFError, OSError, ValueError):
                # Killing local SSH proves nothing about the remote owner or its child.
                raise Unknown('candidate execution %s transport outcome unknown; run status, then reconcile' % action) from None
            code = process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            raise Unknown('candidate execution %s transport did not exit; run status, then reconcile' % action) from None
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            for stream in (process.stdin, process.stdout):
                try:
                    stream.close()
                except OSError:
                    pass
        try:
            require(len(raw) <= MAX_REPLY, 'candidate execution reply exceeds bound')
            reply = json.loads(raw, object_pairs_hook=I.unique)
            require(isinstance(reply, dict) and reply.get('request_sha256') == identifier and reply.get('status') in STATUSES,
                    'invalid candidate execution reply')
        except (ValueError, TypeError):
            raise Unknown('candidate execution reply unavailable; run status, never retry') from None
        if code in (75, 255) or action != 'status' and reply['status'] in ('running', 'interrupted'):
            raise Unknown('candidate execution outcome unfinished; observe status and reconcile explicitly')
        if code:
            raise ValueError('candidate execution %s refused: %s' % (action, reply.get('error', reply['status'])))
        return reply

    def run(self, action, expect_plan=None):
        if action in ('status', 'reconcile'):
            require(isinstance(self.expected, str) and HEX.fullmatch(self.expected), 'pass --request-sha256')
            if action == 'status':
                return self.call('status', self.expected)
            observed = self.call('status', self.expected)
            require(observed['status'] in UNFINISHED, 'candidate execution owner does not need reconciliation')
            request = validate(observed.get('request'))
            require(digest(request) == self.expected and request['hosts'] == self.hosts and
                    request['coordinator'] == self.host and request['machine_id'] == self.machine,
                    'retained request differs from this pinned inventory; reconcile with the original inventory')
            return self.call('reconcile', self.expected, timeout=SURVEY_WAIT_SECONDS+HANDSHAKE_SECONDS,
                             request=request, skip=self.expected)
        require(action in ('plan', 'preflight', 'stage'), 'unsupported candidate execution action')
        request = self.request()
        identifier, header = digest(request), durable.canonical(request)+b'\n'
        if action == 'stage':
            require(isinstance(expect_plan, str) and HEX.fullmatch(expect_plan), 'pass --expect-plan-sha256')
            remote = self.call('preflight', identifier, header)
            require(remote['status'] == 'preflight-passed' and remote.get('plan_sha256') == expect_plan,
                    'candidate execution plan changed since review')
            reply = self.call('stage', identifier, header, expect_plan=expect_plan, request=request,
                              timeout=aggregate_seconds(self.mode)+TRANSPORT_MARGIN_SECONDS)
            require(reply['status'] == 'staged', 'candidate execution did not retain complete evidence')
            return reply
        reply = self.call(action, identifier, header)
        if action == 'plan':
            plan = reply.get('plan')
            require(isinstance(plan, dict) and plan.get('request') == request and reply.get('plan_sha256') == digest(plan),
                    'candidate execution plan reply differs from the request')
        return reply
