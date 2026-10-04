"""Immutable snapshot of the full v3 event journal for candidate qualification.

The full journal `/srv/transparent-activity/full-v3/journal` has one writer, the
publication controller, whose native `EventStore::open` holds `writer.lock` with
Rust `File::try_lock` for its whole writable lifetime. On Linux that is an
exclusive `flock`, so this module proves the protocol on the existing lock file:
the reviewed writer process is the `FLOCK WRITE` holder in `/proc/locks`, holds
the file open, and a non-blocking `flock` from here is refused. It never creates
a substitute lock.

Only the deployment wrapper runs these actions, on the pinned root coordinator
from immutable staged operations source. Stage holds the production lock, runs
the cross-operation fences, records a durable owner before any effect and hands
the lock descriptor to a detached owner process, so a lost SSH session neither
releases the lock nor leaves the owner unrecorded. The owner:

1. rechecks the exact reviewed unit, binary, PID, kernel start time and the
   unit's lack of stop-propagating or restarting dependents;
2. records the committed prefix and independent node anchors;
3. pre-copies the immutable, self-authenticating display sidecars of every
   committed block while the writer still runs, recording their identities;
4. stops only that writer, then takes `writer.lock` itself, so no writer can
   reopen the journal while bytes are copied;
5. copies only the checkpoint-committed prefix of the four journal files and
   re-observes every committed block hash and sidecar, under the reviewed
   sidecar policy, into private regular files under
   `/srv/transparent-activity/snapshots/journal`, with time bounds and the 20%
   memory/disk floors;
6. restores the same writer on every handled exit and proves its identity,
   writer lock and resources before any terminal success.

Native `events_at` reads a missing sidecar as no oversized events, so sidecars
are part of a faithful copy, and their absence is recorded, never inferred.
Failure keeps the owner, partial bytes and health samples. Every non-staged
owner fences other mutation until explicit `reconcile`, which restores only the
exact owned writer and refuses foreign or unknown writer owners. A staged
snapshot is input for root's oracle and candidate gates; it qualifies nothing.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time
import urllib.request
import base64
import ctypes
import struct

from wallet_pir_ops import durable, inherited_lock, schema_fence, transparent_unit
from wallet_pir_ops.deploy import descriptors, host_helper
from wallet_pir_ops.deploy.remote import ProductionLock, RemoteError, SSHExecutor  # noqa: F401

HERE = Path(__file__).parent


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


I = module('journal_snapshot_input_stage', HERE/'activity_input_stage.py')
P, C = I.P, I.C
KIND = 'activity-journal-snapshot'
JOURNAL = P.JOURNAL
SNAPSHOTS = Path('/srv/transparent-activity/snapshots/journal')
OWNERS = I.OWNERS
SOURCE = I.SOURCE
MAP = P.OUTPUT/'shards.json'
RESULT = P.EVIDENCE/'result.json'
CUTOFF = P.EVIDENCE/'cutoff.json'
RPC = 'http://127.0.0.1:8232'
COOKIE = Path('/root/.cache/zakura/.cookie')
PROC = Path('/proc')
CGROUP = Path('/sys/fs/cgroup')
MEMINFO = Path('/proc/meminfo')
MACHINE_ID = Path('/etc/machine-id')
OWNER = 0
statvfs = os.statvfs

# transparent-filter-server/src/events.rs and display_journal.rs, version 3.
FORMAT_VERSION = 3
START_HEIGHT = 0
RECORD_BYTES = 48
CHECKPOINT_BYTES = 16
EVENT_BYTES = 87
MAX_EVENT_BYTES = EVENT_BYTES+1+5+10
MAX_SCRIPT_BYTES = 10_000
# The native reader's own lower bound per entry: script length plus event.
MIN_ENTRY = EVENT_BYTES+2
MAX_ENTRY = 2+MAX_SCRIPT_BYTES+2+MAX_EVENT_BYTES
MAX_SIDECAR = 8_000_076
SIDECAR_MAGIC = b'TPIRTX01'
MAX_META = 4096
FILES = ('meta.json', 'checkpoint.bin', 'blocks.bin', 'events.bin')
CHUNK = 1 << 20
SAMPLE_SECONDS = 1
MAX_REQUEST = 16384
HEX = re.compile('[0-9a-f]{64}')
UNIT = re.compile(r'[A-Za-z0-9][A-Za-z0-9@._-]{0,200}\.service')
PHASES = ('launching', 'adopted', 'precopying', 'quiescing', 'quiesced', 'copying', 'copied', 'restoring', 'restored', 'retained')
UNFINISHED = ('running', 'failed', 'interrupted', 'restore-failed')
BOUNDS = ('precopy_seconds', 'stop_seconds', 'copy_seconds', 'restart_seconds', 'restart_attempts', 'total_seconds')
POLICIES = ('every-committed-block', 'mirror-source')
# Reverse dependencies through which stopping the writer would stop another
# unit, or through which systemd could start it again while it is quiesced.
INTERPLAY = ('RequiredBy', 'RequisiteOf', 'BoundBy', 'ConsistsOf', 'UpheldBy', 'TriggeredBy', 'PropagatesStopTo')
NOT_QUALIFICATION = ('journal snapshot only; it is not an oracle, certificate, candidate, '
                     'serving or capacity qualification')
require = I.require
digest = I.digest


class Unknown(RuntimeError):
    """The detached owner's outcome is not yet known; observe status."""


class Interrupted(BaseException):
    """A handled termination signal; the owner restores the writer first."""


class Foreign(ValueError):
    """The writer changed outside this owner; only root may decide."""


def safe_absolute(value):
    require(isinstance(value, str) and value.startswith('/') and '\x00' not in value and
            '\\' not in value and not any(ord(c) < 32 for c in value), 'unsafe writer path')
    path = Path(value)
    require(str(path) == value and '..' not in path.parts and '.' not in path.parts, 'unsafe writer path')
    return path


def validate(request):
    """The closed, path-free snapshot request; root chooses every bound."""
    require(isinstance(request, dict) and set(request) == {'version', 'kind', 'source_sha', 'attempt', 'machine_id',
            'candidate', 'publication', 'journal', 'sidecars', 'writer', 'bounds'} and type(request['version']) is int and
            request['version'] == 1 and request['kind'] == KIND, 'invalid journal snapshot request')
    require(isinstance(request['source_sha'], str) and re.fullmatch('[0-9a-f]{40}', request['source_sha']) and
            type(request['attempt']) is int and 1 <= request['attempt'] <= 100 and
            isinstance(request['machine_id'], str) and re.fullmatch('[0-9a-f]{32}', request['machine_id']),
            'invalid journal snapshot identity')
    require(request['candidate'] == {'source_sha':C.SOURCE_SHA, 'identity':C.identity()},
            'journal snapshot names another candidate')
    publication = request['publication']
    require(isinstance(publication, dict) and set(publication) == {'map_sha256', 'anchor_height', 'anchor_hash'} and
            isinstance(publication['map_sha256'], str) and HEX.fullmatch(publication['map_sha256']) and
            type(publication['anchor_height']) is int and publication['anchor_height'] == P.THROUGH and
            isinstance(publication['anchor_hash'], str) and HEX.fullmatch(publication['anchor_hash']),
            'journal snapshot publication identity differs')
    journal = request['journal']
    require(isinstance(journal, dict) and set(journal) == {'version', 'genesis_hash', 'start_height'} and
            type(journal['version']) is int and journal['version'] == FORMAT_VERSION and
            type(journal['start_height']) is int and journal['start_height'] == START_HEIGHT and
            isinstance(journal['genesis_hash'], str) and HEX.fullmatch(journal['genesis_hash']),
            'journal snapshot format identity differs')
    sidecars = request['sidecars']
    require(isinstance(sidecars, dict) and set(sidecars) == {'policy', 'coverage_heights'} and
            sidecars['policy'] in POLICIES and isinstance(sidecars['coverage_heights'], list) and
            len(sidecars['coverage_heights']) <= 64 and
            all(type(h) is int and START_HEIGHT <= h < 1 << 32 for h in sidecars['coverage_heights']) and
            sidecars['coverage_heights'] == sorted(set(sidecars['coverage_heights'])),
            'invalid journal snapshot sidecar policy or coverage heights')
    writer = request['writer']
    require(isinstance(writer, dict) and set(writer) == {'unit', 'fragment_path', 'fragment_sha256', 'drop_ins',
            'binary_path', 'binary_sha256', 'main_pid', 'process_start'}, 'invalid journal writer identity')
    safe_absolute(writer['fragment_path']); safe_absolute(writer['binary_path'])
    require(isinstance(writer['unit'], str) and UNIT.fullmatch(writer['unit']) and
            all(isinstance(writer[k], str) and HEX.fullmatch(writer[k]) for k in ('fragment_sha256', 'binary_sha256')) and
            type(writer['main_pid']) is int and 1 < writer['main_pid'] < 1 << 32 and
            type(writer['process_start']) is int and 0 < writer['process_start'] < 1 << 63 and
            isinstance(writer['drop_ins'], list) and len(writer['drop_ins']) <= 32,
            'invalid journal writer identity')
    paths = []
    for item in writer['drop_ins']:
        require(isinstance(item, dict) and set(item) == {'path', 'sha256'} and
                isinstance(item['sha256'], str) and HEX.fullmatch(item['sha256']), 'invalid writer drop-in identity')
        paths.append(str(safe_absolute(item['path'])))
    require(len(set(paths)) == len(paths) and paths == sorted(paths, key=os.path.basename), 'invalid writer drop-in order')
    bounds = request['bounds']
    require(isinstance(bounds, dict) and set(bounds) == set(BOUNDS) and
            all(type(bounds[k]) is int and 0 < bounds[k] < 1 << 31 for k in BOUNDS),
            'journal snapshot bounds must all be reviewed positive integers')
    require(bounds['total_seconds'] >= bounds['precopy_seconds']+bounds['stop_seconds']+bounds['copy_seconds']+
            bounds['restart_seconds']*bounds['restart_attempts'], 'total bound is shorter than its phases')
    require(len(durable.canonical(request)) <= MAX_REQUEST, 'journal snapshot request exceeds bound')
    return request


def checksum(path, *, follow=False):
    fd = os.open(path, os.O_RDONLY | (0 if follow else os.O_NOFOLLOW))
    with os.fdopen(fd, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def running_executable(pid):
    """Bytes the kernel runs for `pid`; `/proc/PID/exe` is a kernel link."""
    return checksum(PROC/str(pid)/'exe', follow=True)


def process_start(pid):
    """Kernel start time, so a reused PID is never mistaken for the owner."""
    try:
        raw = (PROC/str(pid)/'stat').read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    return int(raw.rsplit(')', 1)[1].split()[19])


def process_active(pid, start):
    return type(pid) is int and process_start(pid) is not None and (start is None or process_start(pid) == start)


def no_links(path):
    for parent in [Path(path), *Path(path).parents]:
        require(not parent.is_symlink(), 'journal snapshot path contains a symlink')


def journal_root():
    no_links(JOURNAL)
    info = JOURNAL.lstat()
    require(stat.S_ISDIR(info.st_mode), 'journal is not a directory')
    return info


def open_existing(path, *, limit=None):
    """A source file as a single-link regular file, never through a symlink."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and (limit is None or info.st_size <= limit),
                'journal file is not a bounded single-link regular file: '+Path(path).name)
    except BaseException:
        os.close(fd)
        raise
    return fd, info


def read_small(path, limit):
    fd, _ = open_existing(path, limit=limit)
    with os.fdopen(fd, 'rb') as stream:
        data = stream.read(limit+1)
    require(len(data) <= limit, 'journal metadata exceeds bound')
    return data


# --- writer.lock: the native File::try_lock protocol on Linux ----------------

def open_writer_lock(root=None):
    """The existing writer.lock; never created, replaced or followed."""
    journal_root()
    fd, info = open_existing(Path(root or JOURNAL)/'writer.lock')
    if info.st_uid != OWNER:
        os.close(fd)
        raise ValueError('writer.lock is not root-owned')
    return fd, info


def flock_holders(info):
    """`/proc/locks` entries on this inode as (class, mode, access, pid)."""
    wanted = (os.major(info.st_dev), os.minor(info.st_dev), info.st_ino)
    holders = []
    for line in (PROC/'locks').read_text().splitlines():
        parts = line.split()
        if len(parts) < 6 or parts[1] == '->':
            continue  # Blocked waiters do not hold the lock.
        identity = parts[5].split(':')
        if len(identity) != 3:
            continue
        if (int(identity[0], 16), int(identity[1], 16), int(identity[2])) == wanted:
            holders.append((parts[1], parts[2], parts[3], int(parts[4])))
    return holders


def holds_open(pid, info):
    """Whether `pid` has this inode open; the descriptor carries its flock."""
    directory = PROC/str(pid)/'fd'
    entries = os.listdir(directory)
    require(len(entries) <= 1 << 16, 'writer descriptor table exceeds bound')
    for name in entries:
        try:
            target = os.stat(directory/name)
        except (FileNotFoundError, PermissionError):
            continue
        if (target.st_dev, target.st_ino) == (info.st_dev, info.st_ino):
            return True
    return False


def lock_proof(fd, info, pid, *, contend=True):
    """Prove `pid` holds the native exclusive flock on the existing writer.lock.

    Rust `File::try_lock` is `flock(LOCK_EX|LOCK_NB)` on Linux: the kernel lists
    it as `FLOCK ADVISORY WRITE` and refuses every other flock on the inode. The
    contention attempt runs only after the holder is listed, so it cannot win
    against a live writer and never blocks a starting one.
    """
    holders = flock_holders(info)
    require(holders == [('FLOCK', 'ADVISORY', 'WRITE', pid)],
            'writer.lock is not held exclusively by the reviewed writer: %d holder(s)' % len(holders))
    require(holds_open(pid, info), 'reviewed writer does not hold writer.lock open')
    if contend:
        try:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            fcntl.flock(fd, fcntl.LOCK_UN)
            raise ValueError('writer.lock was not contended; the native writer lock protocol differs')
    return {'class':'FLOCK', 'access':'WRITE', 'pid':pid, 'inode':info.st_ino, 'device':info.st_dev,
            'contended':bool(contend)}


def acquire_writer_lock(fd, info):
    """Hold writer.lock after quiescence; any surviving holder refuses."""
    import fcntl
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise Foreign('another process still holds writer.lock after quiescence')
    current = (JOURNAL/'writer.lock').lstat()
    require((current.st_dev, current.st_ino) == (info.st_dev, info.st_ino), 'writer.lock was replaced')
    return {'held':True, 'inode':info.st_ino, 'device':info.st_dev}


# --- committed journal format -------------------------------------------------

def display_hex(internal):
    return internal[::-1].hex()


def meta(root, request):
    raw = read_small(Path(root)/'meta.json', MAX_META)
    value = json.loads(raw, object_pairs_hook=I.unique)
    require(isinstance(value, dict) and set(value) == {'version', 'genesis_hash', 'start_height'} and
            value == request['journal'], 'journal format, genesis or start height differs')
    return raw


def checkpoint(root):
    raw = read_small(Path(root)/'checkpoint.bin', CHECKPOINT_BYTES)
    require(len(raw) == CHECKPOINT_BYTES, 'checkpoint is not 16 bytes')
    events, blocks = int.from_bytes(raw[:8], 'little'), int.from_bytes(raw[8:], 'little')
    require(blocks % RECORD_BYTES == 0 and blocks > 0, 'checkpoint does not name complete 48-byte block records')
    return raw, events, blocks


def committed(root, request, *, exact=False):
    """The checkpoint-committed prefix summary, read independently of any writer.

    A live journal may hold an uncommitted tail past the checkpoint; the
    snapshot itself must equal its checkpoint exactly.
    """
    root = Path(root)
    meta_raw = meta(root, request)
    raw, events, blocks = checkpoint(root)
    sizes = {}
    for name, length in (('events.bin', events), ('blocks.bin', blocks)):
        fd, info = open_existing(root/name)
        os.close(fd)
        require(info.st_size >= length, name+' is shorter than its checkpoint')
        require(not exact or info.st_size == length, name+' differs from its checkpoint')
        sizes[name] = info.st_size
    count = blocks//RECORD_BYTES
    last = record(root/'blocks.bin', count-1)
    require(last['offset'] <= events, 'last block record lies past the committed events')
    return {'version':request['journal']['version'], 'genesis_hash':request['journal']['genesis_hash'],
            'start_height':START_HEIGHT, 'events_bytes':events, 'blocks_bytes':blocks, 'blocks':count,
            'tip_height':START_HEIGHT+count-1, 'tip_hash':last['hash'],
            'meta_sha256':hashlib.sha256(meta_raw).hexdigest(), 'checkpoint_sha256':hashlib.sha256(raw).hexdigest(),
            'file_bytes':sizes}


def record(path, index):
    fd, info = open_existing(path)
    try:
        require(0 <= index and (index+1)*RECORD_BYTES <= info.st_size, 'block record outside the journal')
        raw = os.pread(fd, RECORD_BYTES, index*RECORD_BYTES)
    finally:
        os.close(fd)
    require(len(raw) == RECORD_BYTES, 'incomplete block record')
    return {'hash':display_hex(raw[:32]), 'internal':raw[:32], 'offset':int.from_bytes(raw[32:40], 'little'),
            'count':int.from_bytes(raw[40:48], 'little')}


class Records:
    """Streaming record-boundary checks over complete 48-byte block records.

    Each block's events span from its offset to the next block's offset (the
    last to the committed events length). Offsets begin at zero and never
    decrease; an empty span holds exactly zero events, and a span's count fits
    the native entry bounds. Event bytes themselves are decoded by the native
    reader that consumes the snapshot.
    """

    def __init__(self, events_bytes):
        self.events_bytes = events_bytes
        self.index = 0
        self.previous = None
        self.wanted = {}
        self.found = {}
        self.last = None
        self.events = 0

    def want(self, height, expected):
        self.wanted[height-START_HEIGHT] = expected

    def close_span(self, end):
        offset, count = self.previous
        span = end-offset
        require(span >= 0 and (span == 0) == (count == 0) and count*MIN_ENTRY <= span <= count*MAX_ENTRY,
                'block record boundary does not match its event span')
        self.events += count

    def feed(self, raw):
        require(len(raw) == RECORD_BYTES, 'incomplete block record')
        offset, count = int.from_bytes(raw[32:40], 'little'), int.from_bytes(raw[40:48], 'little')
        require(offset <= self.events_bytes, 'block record lies past the committed events')
        if self.previous is None:
            require(offset == 0, 'first block record does not start the event journal')
        else:
            require(offset >= self.previous[0], 'block record offsets decrease')
            self.close_span(offset)
        if self.index in self.wanted:
            self.found[self.index] = display_hex(raw[:32])
        self.previous = (offset, count)
        self.last = display_hex(raw[:32])
        self.index += 1

    def finish(self):
        require(self.previous is not None, 'journal has no committed block')
        self.close_span(self.events_bytes)
        for index, expected in self.wanted.items():
            require(self.found.get(index) == expected,
                    'journal reorganized or differs at height %d' % (index+START_HEIGHT))
        return {'blocks':self.index, 'events':self.events, 'tip_height':START_HEIGHT+self.index-1, 'tip_hash':self.last}


def sidecar_path(root, internal):
    return Path(root)/'display-v1'/(display_hex(internal)+'.bin')


def check_sidecar(data, internal):
    require(76 <= len(data) <= MAX_SIDECAR and data[:8] == SIDECAR_MAGIC and data[8:40] == internal and
            hashlib.sha256(data[:-32]).digest() == data[-32:], 'invalid committed display sidecar')


def iterate_records(path, length, budget):
    fd, info = open_existing(path)
    require(info.st_size >= length, 'block records shorter than checkpoint')
    with os.fdopen(fd, 'rb') as stream:
        remaining = length
        while remaining:
            budget.check()
            data = stream.read(min(remaining, RECORD_BYTES*4096))
            require(data and len(data) % RECORD_BYTES == 0, 'truncated block records')
            remaining -= len(data)
            for at in range(0, len(data), RECORD_BYTES):
                yield data[at:at+RECORD_BYTES]


def sidecar_estimate(root, length, sidecars, budget):
    """Committed sidecar presence before quiescence, against policy and coverage."""
    count = size = absent = 0
    missing = []
    coverage = set(sidecars['coverage_heights'])
    for height, raw in enumerate(iterate_records(Path(root)/'blocks.bin', length, budget), START_HEIGHT):
        try:
            info = sidecar_path(root, raw[:32]).lstat()
        except FileNotFoundError:
            absent += 1
            if height in coverage or len(missing) < 16:
                missing.append(height)
            continue
        count += 1; size += info.st_size
    return {'sidecars':count, 'sidecar_bytes':size, 'sidecars_absent':absent, 'absent_heights_sample':missing[:16],
            'coverage_absent':sorted(coverage & set(missing))}


def sidecar_policy(estimate, sidecars, tip_height):
    require(all(h <= tip_height for h in sidecars['coverage_heights']), 'coverage heights lie beyond the committed journal')
    require(not estimate['coverage_absent'], 'coverage heights have no display sidecar')
    require(sidecars['policy'] != 'every-committed-block' or not estimate['sidecars_absent'],
            'committed blocks have no display sidecar: %d' % estimate['sidecars_absent'])


# --- host observations -------------------------------------------------------

def resources(root, remaining=0):
    """Memory and disk at or above 20% after reserving `remaining` bytes."""
    memory = {}
    for line in MEMINFO.read_text().splitlines():
        key, _, value = line.partition(':')
        if key in ('MemTotal', 'MemAvailable'):
            memory[key] = int(value.split()[0])*1024
    require(set(memory) == {'MemTotal', 'MemAvailable'} and memory['MemTotal'] > 0, 'memory observation unavailable')
    require(memory['MemAvailable']*5 >= memory['MemTotal'], 'journal snapshot memory headroom below 20 percent')
    parent = Path(root)
    while not parent.exists():
        parent = parent.parent
    disks = {}
    for path in dict.fromkeys((parent, JOURNAL)):
        disk = statvfs(path)
        require(disk.f_blocks > 0, 'disk observation unavailable')
        require(disk.f_bavail*disk.f_frsize-remaining >= .2*disk.f_blocks*disk.f_frsize,
                'journal snapshot disk headroom below 20 percent')
        disks[str(path)] = {'available':disk.f_bavail*disk.f_frsize, 'total':disk.f_blocks*disk.f_frsize}
    return {'unix':time.time(), 'memory_available':memory['MemAvailable'], 'memory_total':memory['MemTotal'],
            'disk':disks, 'reserved_bytes':remaining}


class Budget:
    """One monotonic deadline and sampled 20% headroom for every long step.

    `check` runs before each bounded chunk, record batch, file and remote call.
    It refuses past the deadline and, at most once a second, re-observes the
    floors after reserving the bytes still to be written, runs `guard` (for
    example quiescence and the inherited lock) and appends the sample to the
    private health log when there is one. Bounds are never widened.
    """

    def __init__(self, deadline, label, *, reserve=0, health=None, guard=None):
        self.deadline, self.label, self.reserve = deadline, label, reserve
        self.health, self.guard = health, guard
        self.copied, self.last = 0, float('-inf')

    def remaining(self):
        return self.deadline-time.monotonic()

    def phase(self, seconds, label, **options):
        """A phase bound inside this budget: whichever deadline comes first."""
        return Budget(min(self.deadline, time.monotonic()+seconds), label,
                      **{'health':self.health, **options})

    def check(self):
        if time.monotonic() >= self.deadline:
            raise subprocess.TimeoutExpired(self.label, 0)
        if time.monotonic()-self.last >= SAMPLE_SECONDS:
            try:
                if self.guard is not None:
                    self.guard()
                observed = resources(SNAPSHOTS, max(0, self.reserve-self.copied))
            except BaseException as error:
                self.log({'unix':time.time(), 'refused':type(error).__name__, 'error':str(error)[:300]})
                raise
            self.log(observed)
            self.last = time.monotonic()

    def log(self, sample):
        """Append one sample, including a refusal, to the private health log."""
        if self.health is not None:
            with open(self.health, 'a') as stream:
                stream.write(json.dumps(dict(sample, phase=self.label, copied_bytes=self.copied), sort_keys=True)+'\n')
                stream.flush(); os.fsync(stream.fileno())

    def seconds(self):
        """A positive timeout for one call; refuses when nothing remains."""
        self.check()
        return max(1, int(self.remaining()))


# --- every pinned host's owners -------------------------------------------------

PID_FIELDS = ('pid', 'child_pid', 'ssh_pid', 'relay_pid', 'parent_pid', 'launcher_pid')
SOURCE_STATES = ('staged', 'failed')


class LocalHost:
    """The coordinator's own owner records, read in-process."""

    def call(self, op, budget, **arguments):
        budget.check()
        return host_helper.OPERATIONS[op](**arguments)


class RemoteHost:
    """One pinned host through the deploy helper over pinned SSH."""

    def __init__(self, executor, host):
        self.executor, self.host = executor, host

    def call(self, op, budget, **arguments):
        return self.executor.call(self.host, op, deadline=budget.seconds(), **arguments)


def owner_processes(record):
    """(pid, recorded start, latest record time) for the record's own processes.

    Only owner fields count: top-level PID fields and the `launcher` and `owner`
    objects. Processes a record merely observed, such as a restored writer in
    a proof, are not its owners and are excluded.
    """
    if not isinstance(record, dict):
        return []
    times = [v for k, v in record.items() if k.endswith('_unix') and type(v) in (int, float)]
    latest = max(times) if times else None
    found = []
    for holder in (record, record.get('launcher'), record.get('owner')):
        if not isinstance(holder, dict):
            continue
        for key in PID_FIELDS:
            pid = holder.get(key)
            if type(pid) is int and pid > 0:
                start = holder.get('process_start') if key == 'pid' else None
                found.append((pid, start if type(start) is int else None, latest))
    return found


def live(stat_text, boot, ticks, start, latest):
    """Whether a live PID can be the recorded owner or one of its descendants.

    A recorded kernel start time must match exactly. Without one, a process that
    started no later than the record's last update could be its owner; only a
    PID reused afterwards is excluded. A missing record time fails closed.
    """
    if stat_text is None:
        return False
    fields = stat_text.rsplit(')', 1)[1].split()
    if fields[0] in ('Z', 'X'):
        return False
    started = int(fields[19])
    if start is not None:
        return started == start
    return latest is None or boot+started/ticks <= latest+1


def fleet_host(name, machine_id, reader, budget, *, skip_input=None):
    """Fresh exact machine, fence, source-staging and owner-process reads of one host."""
    pointers = [str(schema_fence.SCHEMA_STATE/schema_fence.SCHEMA_POINTER),
                str(schema_fence.HOST_ACTIONS/'latest.json'), str(schema_fence.INPUT_STAGING/'latest.json')]
    staging = SOURCE.parent/'staging'
    receipt = {'host':name, 'machine_id':machine_id, 'reads':{}, 'source_staging':None, 'processes':None}
    first = reader.call('read_many', budget, paths=[str(MACHINE_ID), *pointers])
    receipt['reads'].update(first)
    require((first[str(MACHINE_ID)] or '').strip() == machine_id, 'host %s machine identity differs from its pin' % name)
    names = reader.call('list_json', budget, path=str(staging))
    receipt['source_staging'] = names
    records = []

    def named(path, build):
        try:
            pointer = json.loads(first[path]) if first[path] is not None else None
            return build(pointer) if isinstance(pointer, dict) else None
        except (ValueError, TypeError, KeyError):
            return None  # The fence below refuses the malformed pointer.
    for path in filter(None, (
            named(pointers[0], lambda p: str(schema_fence.SCHEMA_STATE/(str(p['id'])+'.json'))),
            named(pointers[1], lambda p: str(schema_fence.HOST_ACTIONS/str(p['transaction'])/(str(p['request_id'])+'.json'))),
            named(pointers[2], lambda p: str(schema_fence.INPUT_STAGING/(str(p['request_sha256'])+'.json'))))):
        if '..' not in Path(path).parts:
            records.append(path)
    sources = [str(staging/n) for n in names or [] if re.fullmatch(r'[0-9a-f]{40}\.json', n)]
    if records or sources:
        receipt['reads'].update(reader.call('read_many', budget, paths=records+sources))
    reads = receipt['reads']

    def read(path):
        require(path in reads, 'host %s owner record was not read: %s' % (name, path))
        return reads[path]
    skip = skip_input[0] if skip_input and machine_id == skip_input[1] else None
    schema_fence.schema_mutation_fence(read, schema_fence.SCHEMA_STATE, skip_input=skip)
    owned = str(schema_fence.INPUT_STAGING/(skip+'.json')) if skip else None
    processes = []
    for path in records+sources:
        text = reads[path]
        require(text is not None, 'host %s owner record vanished: %s' % (name, path))
        value = json.loads(text)
        if path in sources:
            require(isinstance(value, dict) and value.get('status') in SOURCE_STATES,
                    'host %s has unfinished source staging' % name)
        if path != owned:
            processes += owner_processes(value)
    if processes:
        observed = reader.call('proc_stats', budget, pids=sorted({p for p, _, _ in processes})[:512])
        receipt['processes'] = observed
        for pid, start, latest in processes:
            require(not live(observed['stats'].get(str(pid)), observed['boot_unix'], observed['ticks'], start, latest),
                    'host %s has a live recorded owner or descendant: PID %d' % (name, pid))
    return receipt


def fleet(inventory, budget, executor_factory, *, skip_input=None, retain=None):
    """Every pinned host, coordinator first; each raw receipt kept when `retain`.

    Refuses missing pins, unreadable or unfinished owners, unfinished source
    staging and live recorded owners or descendants. `skip_input` is
    `(request SHA, coordinator machine ID)`: only this snapshot's own owner.
    """
    coordinator = inventory.lock['machine_id']
    hosts = [('coordinator', coordinator, LocalHost())]
    executor = None
    for name in sorted(inventory.hosts):
        machine = inventory.hosts[name].get('machine_id')
        require(isinstance(machine, str) and re.fullmatch('[0-9a-f]{32}', machine), 'host %s has no machine pin' % name)
        if machine == coordinator:
            continue
        executor = executor or executor_factory(inventory)
        hosts.append((name, machine, RemoteHost(executor, name)))
    receipts = []
    for name, machine, reader in hosts:
        try:
            receipt = dict(fleet_host(name, machine, reader, budget, skip_input=skip_input), result='passed')
        except BaseException as error:
            receipt = {'host':name, 'machine_id':machine, 'result':'refused', 'error_type':type(error).__name__,
                       'error':str(error)[:300]}
            if retain is not None:
                keep(retain, name, receipt)
            raise
        receipt['checked_unix'] = time.time()
        if retain is not None:
            keep(retain, name, receipt)
        receipts.append({'host':name, 'machine_id':machine, 'sha256':durable.digest(receipt)})
    return receipts


def keep(directory, name, receipt):
    Path(directory).mkdir(parents=True, exist_ok=True, mode=0o700)
    durable.atomic_json(Path(directory)/(name+'.json'), receipt, mode=0o400)


class Systemd:
    PROPERTIES = ','.join(('ActiveState', 'SubState', 'MainPID', 'NRestarts', 'FragmentPath', 'DropInPaths',
                           'ControlGroup', 'NeedDaemonReload', *INTERPLAY))

    def show(self, unit):
        data = subprocess.run(['systemctl', 'show', unit, '--property='+self.PROPERTIES], capture_output=True,
                              check=True, timeout=30, **inherited_lock.options()).stdout.decode()
        return dict(line.split('=', 1) for line in data.splitlines() if '=' in line)

    def stop(self, unit, timeout):
        subprocess.run(['systemctl', 'stop', unit], capture_output=True, check=True, timeout=timeout,
                       **inherited_lock.options())

    def start(self, unit, timeout):
        subprocess.run(['systemctl', 'start', unit], capture_output=True, check=True, timeout=timeout,
                       **inherited_lock.options())

    def pids(self, group):
        if not group:
            return set()
        require(group.startswith('/') and '..' not in Path(group).parts, 'unsafe writer cgroup')
        root = CGROUP/group.lstrip('/')
        if not root.exists():
            return set()
        paths = list(root.rglob('cgroup.procs'))
        require(len(paths) <= 1024, 'writer cgroup exceeds bound')
        return {int(pid) for path in paths for pid in path.read_text().split()}


class Node:
    """Independent canonical block hashes from the coordinator's own node."""

    def block_hash(self, height, timeout):
        cookie = COOKIE.read_text().strip()
        body = json.dumps({'jsonrpc':'2.0', 'id':height, 'method':'getblockhash', 'params':[height]}).encode()
        request = urllib.request.Request(RPC, body, {'Content-Type':'application/json',
            'Authorization':'Basic '+base64.b64encode(cookie.encode()).decode()})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=timeout) as response:
            raw = response.read(65537)
        require(len(raw) <= 65536, 'anchor RPC response exceeds bound')
        result = json.loads(raw)
        require(result.get('id') == height and result.get('error') is None and
                isinstance(result.get('result'), str) and HEX.fullmatch(result['result']), 'invalid anchor RPC response')
        return result['result']


# --- the operation ------------------------------------------------------------

class Snapshot:
    def __init__(self, inventory, request, expected, *, systemd=None, node=None, lock_factory=None):
        self.request = validate(request)
        require(digest(request) == expected, 'journal snapshot request differs from its reviewed digest')
        self.inventory = inventory
        self.identifier = expected
        self.writer = request['writer']
        self.bounds = request['bounds']
        self.owner = OWNERS/(expected+'.json')
        self.retained = OWNERS/(expected+'.request.json')
        self.log = OWNERS/(expected+'.snapshot.log')
        self.health = OWNERS/(expected+'.snapshot-health.ndjson')
        self.sidecar_inventory = OWNERS/(expected+'.snapshot-sidecars.bin')
        self.retained_inventory = OWNERS/(expected+'.snapshot-inventory.json')
        self.fleet_receipts = OWNERS/(expected+'.snapshot-fleet')
        self.target = SNAPSHOTS/expected
        self.partial = SNAPSHOTS/(expected+'.copying')
        self.systemd = systemd or Systemd()
        self.node = node or Node()
        self.lock_factory = lock_factory or (lambda: ProductionLock({'type':'pinned_host', 'machine_id':request['machine_id']}))
        self.lock_path = ProductionLock.PATH
        self.shielded = False
        self.pending = None

    def executor(self, inventory):
        return SSHExecutor(inventory)

    def budget(self, label):
        return Budget(time.monotonic()+self.bounds['total_seconds'], label)

    def pinned(self, inventory):
        require(inventory is not None and inventory.lock == {'type':'pinned_host', 'machine_id':self.request['machine_id']},
                'journal snapshot requires the pinned coordinator inventory')
        return {'hosts':inventory.hosts, 'ssh':inventory.ssh, 'lock':inventory.lock, 'services':inventory.services}

    def receipts(self, label):
        return self.fleet_receipts/('%s-%d' % (label, time.time_ns()))

    # Identity of the operator, source, publication and candidate.

    def identity(self):
        source = self.request['source_sha']
        require(os.geteuid() == 0 and MACHINE_ID.read_text().strip() == self.request['machine_id'],
                'journal snapshot requires the pinned root coordinator')
        if self.inventory is not None:
            self.pinned(self.inventory)
        require(Path(__file__).resolve().parents[3] == SOURCE/source, 'journal snapshot requires immutable operation source')
        receipt = json.loads((SOURCE.parent/'staging'/(source+'.json')).read_text())
        I.S.verify_receipt(receipt, SOURCE/source, source, receipt['archive_sha256'])

    def publication(self):
        expected = self.request['publication']
        result = json.loads(read_small(RESULT, 1 << 20), object_pairs_hook=I.unique)
        cutoff = json.loads(read_small(CUTOFF, 1 << 20), object_pairs_hook=I.unique)
        require(checksum(MAP) == expected['map_sha256'] and result.get('status') == 'passed' and
                result.get('map_sha256') == expected['map_sha256'] and
                cutoff.get('anchor') == {'height':expected['anchor_height'], 'hash':expected['anchor_hash']},
                'journal snapshot publication identity differs from the retained publication')
        require(self.request['candidate'] == {'source_sha':C.SOURCE_SHA, 'identity':C.identity()},
                'journal snapshot candidate identity differs')
        return expected

    # The reviewed writer.

    def unit_files(self, state):
        """Unit fragment, drop-ins and binary bytes, all exactly as reviewed."""
        writer = self.writer
        require(state.get('FragmentPath') == writer['fragment_path'] and state.get('NeedDaemonReload') == 'no',
                'writer unit drift: fragment path or pending reload')
        no_links(writer['fragment_path'])
        require(checksum(writer['fragment_path']) == writer['fragment_sha256'], 'writer unit drift: fragment bytes')
        text = read_small(writer['fragment_path'], 1 << 20).decode()
        require(transparent_unit.exec_args(text)[0] == writer['binary_path'], 'writer unit drift: ExecStart binary')
        for name in INTERPLAY:
            require(not state.get(name, '').split(),
                    'writer unit drift: %s would propagate its stop or restart it' % name)
        drop_ins = sorted(state.get('DropInPaths', '').split(), key=os.path.basename)
        require(drop_ins == [d['path'] for d in writer['drop_ins']], 'writer unit drift: drop-in set')
        for item in writer['drop_ins']:
            no_links(item['path'])
            require(checksum(item['path']) == item['sha256'], 'writer unit drift: drop-in bytes')
        no_links(writer['binary_path'])
        require(checksum(writer['binary_path']) == writer['binary_sha256'], 'writer binary drift: installed bytes')

    def running(self, pid, start, *, contend=True):
        """Prove `pid`/`start` is the exact reviewed writer holding writer.lock."""
        state = self.systemd.show(self.writer['unit'])
        require(state.get('ActiveState') == 'active' and state.get('SubState') == 'running',
                'reviewed writer unit is not running')
        require(state.get('MainPID') == str(pid) and process_start(pid) == start,
                'writer PID/start drift: the unit runs another process')
        self.unit_files(state)
        require(running_executable(pid) == self.writer['binary_sha256'], 'writer binary drift: running executable')
        require(pid in self.systemd.pids(state.get('ControlGroup', '')), 'writer PID is outside the unit cgroup')
        fd, info = open_writer_lock()
        try:
            proof = lock_proof(fd, info, pid, contend=contend)
        finally:
            os.close(fd)
        return {'pid':pid, 'process_start':start, 'n_restarts':state.get('NRestarts'),
                'control_group':state.get('ControlGroup'), 'binary_sha256':self.writer['binary_sha256'], 'lock':proof}

    def observed(self):
        """Read-only writer observation for plan; drift is reported, not refused."""
        try:
            return {'writer':self.running(self.writer['main_pid'], self.writer['process_start'], contend=False), 'drift':None}
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            return {'writer':None, 'drift':str(error)[:300]}

    def stopped(self, state):
        return (state.get('ActiveState') in ('inactive', 'failed') and state.get('MainPID') == '0' and
                not self.systemd.pids(state.get('ControlGroup', '')))

    # Plan, preflight and status.

    def stable_plan(self):
        return {'version':1, 'kind':KIND, 'request_sha256':self.identifier, 'journal':str(JOURNAL),
                'target':str(self.target), 'partial':str(self.partial), 'owner':str(self.owner),
                'writer':self.writer, 'bounds':self.bounds, 'publication':self.request['publication'],
                'sidecars':self.request['sidecars'],
                'candidate':self.request['candidate'],
                'effects':'stop and restore only the reviewed journal writer; copy its committed prefix into the fixed private snapshot namespace',
                'qualification':NOT_QUALIFICATION}

    def plan(self):
        plan = self.stable_plan()
        budget = self.budget('journal snapshot plan')
        self.publication()
        journal = committed(JOURNAL, self.request)
        estimate = sidecar_estimate(JOURNAL, journal['blocks_bytes'], self.request['sidecars'], budget)
        observation = {'journal':journal, **estimate, **self.observed(),
                       'quiesced_copy_bytes':journal['events_bytes']+journal['blocks_bytes']}
        return {'plan':plan, 'plan_sha256':digest(plan), 'observation':observation}

    def namespace(self):
        no_links(SNAPSHOTS)
        for path in (self.owner, self.retained, self.target, self.partial, self.log, self.health,
                     self.sidecar_inventory, self.retained_inventory):
            no_links(path)
            require(not path.exists(), 'journal snapshot already owned; inspect status or reconcile')
        if SNAPSHOTS.exists():
            info = SNAPSHOTS.lstat()
            require(stat.S_ISDIR(info.st_mode) and info.st_uid == OWNER and not info.st_mode & 0o077,
                    'snapshot namespace is not a private root-owned directory')

    def preflight(self, budget=None, retain=None):
        budget = budget or self.budget('journal snapshot preflight')
        self.pinned(self.inventory)
        schema_fence.local_schema_fence()
        # Every pinned host, under the caller's production lock for stage.
        hosts = fleet(self.inventory, budget, self.executor, retain=retain)
        self.namespace()
        self.publication()
        journal_root()
        writer = self.running(self.writer['main_pid'], self.writer['process_start'])
        journal = committed(JOURNAL, self.request)
        estimate = sidecar_estimate(JOURNAL, journal['blocks_bytes'], self.request['sidecars'], budget)
        sidecar_policy(estimate, self.request['sidecars'], journal['tip_height'])
        anchors = self.anchors(budget, journal['tip_height'], journal['tip_hash'])
        sample = resources(SNAPSHOTS, journal['events_bytes']+journal['blocks_bytes']+estimate['sidecar_bytes'])
        return {'status':'preflight-passed', 'request_sha256':self.identifier, 'writer':writer, 'hosts':hosts,
                'journal':journal, **estimate, 'anchors':anchors, 'resources':sample}

    def load(self):
        no_links(self.owner)
        require(self.owner.lstat().st_size <= 1 << 20, 'journal snapshot owner exceeds bound')
        record = json.loads(self.owner.read_text(), object_pairs_hook=I.unique)
        no_links(self.retained)
        require(record.get('kind') == KIND and record.get('request_sha256') == self.identifier and
                json.loads(self.retained.read_text(), object_pairs_hook=I.unique) == self.request,
                'retained journal snapshot owner differs')
        return record

    def status(self):
        no_links(self.owner)
        if not self.owner.exists():
            return {'status':'absent', 'request_sha256':self.identifier}
        record = self.load()
        if record['status'] == 'staged':
            verify_tree(self.target, record['manifest'], full=False)
        return record

    def save(self, record):
        durable.atomic_json(self.owner, record, mode=0o600)

    def phase(self, record, name, **values):
        record.update(phase=name, **values)
        record.setdefault('events', []).append({'phase':name, 'unix':time.time()})
        self.save(record)

    def anchors(self, budget, *pairs):
        """Independent node agreement with genesis, the anchor and given tips."""
        checks = [(0, self.request['journal']['genesis_hash']),
                  (self.request['publication']['anchor_height'], self.request['publication']['anchor_hash'])]
        checks += [(pairs[i], pairs[i+1]) for i in range(0, len(pairs), 2)]
        observed = []
        for height, expected in checks:
            actual = self.node.block_hash(height, timeout=min(10, budget.seconds()))
            require(actual == expected, 'canonical anchor differs at height %d; the chain or journal reorganized' % height)
            observed.append({'height':height, 'hash':actual})
        return {'unix':time.time(), 'anchors':observed}

    # Stage: durable intent, then a detached owner that inherits the lock.

    def owner_command(self):
        return ['/usr/bin/python3', '-B', str(SOURCE/self.request['source_sha']/'ops/scripts/wallet-pir-deploy.py'),
                'schema-snapshot-owner', '--request-sha256', self.identifier]

    def stage(self, expected_plan):
        budget = self.budget('journal snapshot stage')
        with self.lock_factory() as lock:
            lock.verify()
            require(digest(self.stable_plan()) == expected_plan, 'journal snapshot plan changed')
            OWNERS.mkdir(parents=True, exist_ok=True, mode=0o700)
            proof = self.preflight(budget, retain=self.receipts('stage'))
            inventory = self.pinned(self.inventory)
            durable.atomic_json(self.retained, self.request, mode=0o400)
            durable.atomic_json(self.retained_inventory, inventory, mode=0o400)
            # One total bound from here, through the owner's verification.
            record = {'version':1, 'kind':KIND, 'request_sha256':self.identifier, 'plan_sha256':expected_plan,
                      'status':'running', 'phase':'launching', 'launcher':{'pid':os.getpid(), 'process_start':process_start(os.getpid())},
                      'inventory_sha256':durable.digest(inventory), 'deadline_unix':time.time()+budget.remaining(),
                      'preflight':proof, 'started_unix':time.time(), 'events':[{'phase':'launching', 'unix':time.time()}]}
            # The owner and the shared fence pointer exist before any effect.
            self.save(record)
            durable.atomic_json(OWNERS/'latest.json', {'request_sha256':self.identifier}, mode=0o600)
            try:
                with self.log.open('xb') as output:
                    os.fchmod(output.fileno(), 0o600)
                    descriptors = lock.descriptors()
                    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
                    env[inherited_lock.VARIABLE] = ','.join(map(str, descriptors))
                    # A new session survives the SSH session's hangup; the owner
                    # keeps this production lock until it exits.
                    child = subprocess.Popen(self.owner_command(), stdin=subprocess.DEVNULL, stdout=output,
                                             stderr=subprocess.STDOUT, pass_fds=descriptors, env=env,
                                             start_new_session=True)
            except BaseException as error:
                record.update(status='failed', error_type=type(error).__name__, finished_unix=time.time())
                self.save(record)
                raise
        try:
            child.wait(timeout=max(0, budget.remaining()))
        except subprocess.TimeoutExpired:
            raise Unknown('journal snapshot owner %d is still running; observe status' % child.pid)
        record = self.load()
        require(record['status'] == 'staged', 'journal snapshot owner ended %s; inspect status and reconcile' % record['status'])
        return record

    # The detached owner.

    def interrupt(self, signum, _frame):
        if self.shielded:
            self.pending = signum
            return
        raise Interrupted(signum)

    def adopt(self):
        inherited_lock.descriptors(required=True, path=self.lock_path)
        record = self.load()
        require(record['status'] == 'running' and record['phase'] == 'launching' and 'owner' not in record,
                'journal snapshot owner was already adopted; never replay')
        self.phase(record, 'adopted', owner={'pid':os.getpid(), 'process_start':process_start(os.getpid())})
        return record

    def owned_inventory(self, record):
        """The exact inventory stage retained; a changed one refuses."""
        no_links(self.retained_inventory)
        inventory = descriptors.load_inventory(self.retained_inventory)
        require(durable.digest(self.pinned(inventory)) == record['inventory_sha256'], 'retained snapshot inventory differs')
        return inventory

    def run_owner(self):
        record = self.adopt()
        handlers = {s:signal.signal(s, self.interrupt) for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}
        total = Budget(time.monotonic()+record['deadline_unix']-time.time(), 'journal snapshot', health=self.health)
        failure, writer_lock, stop_issued = None, [None], False
        try:
            try:
                self.health.touch(mode=0o600, exist_ok=False)
                inherited_lock.descriptors(required=True, path=self.lock_path)
                schema_fence.local_schema_fence(skip_input=self.identifier)
                inventory = self.owned_inventory(record)
                record['hosts'] = fleet(inventory, total, self.executor, retain=self.receipts('owner'),
                                        skip_input=(self.identifier, self.request['machine_id']))
                original = self.running(self.writer['main_pid'], self.writer['process_start'])
                before = committed(JOURNAL, self.request)
                estimate = sidecar_estimate(JOURNAL, before['blocks_bytes'], self.request['sidecars'], total)
                sidecar_policy(estimate, self.request['sidecars'], before['tip_height'])
                anchors = self.anchors(total, before['tip_height'], before['tip_hash'])
                resources(SNAPSHOTS, before['events_bytes']+before['blocks_bytes']+estimate['sidecar_bytes'])
                self.phase(record, 'precopying', writer_before=original, before=before, estimate=estimate,
                           anchors_before=anchors)
                precopy = self.precopy(before, estimate, total)
                self.running(self.writer['main_pid'], self.writer['process_start'])
                # Durable before the stop: reconciliation may then restart it.
                self.phase(record, 'quiescing', precopy=precopy)
                stop_issued = True
                self.quiesce(record, writer_lock)
                self.phase(record, 'copying')
                self.phase(record, 'copied', snapshot=self.copy(before, total))
            except BaseException as error:
                failure = error
            # Every handled exit restores the writer; later signals wait for it.
            self.shielded = True
            try:
                record['restoration'] = self.restore(record, writer_lock[0], stop_issued)
            except BaseException as error:
                record.update(status='restore-failed', restore_error_type=type(error).__name__,
                              restore_error=str(error)[:300])
                failure = failure or error
            if failure is None:
                try:
                    record['anchors_after'] = self.anchors(
                        total, record['snapshot']['tip_height'], record['snapshot']['tip_hash'],
                        record['before']['tip_height'], record['before']['tip_hash'])
                    self.retain(record, total)
                    record.update(status='staged', qualification=NOT_QUALIFICATION)
                except BaseException as error:
                    failure = error
        finally:
            if failure is not None:
                if record['status'] == 'running':
                    interrupted = isinstance(failure, (Interrupted, subprocess.TimeoutExpired, KeyboardInterrupt, SystemExit))
                    record['status'] = 'interrupted' if interrupted else 'failed'
                record.update(error_type=type(failure).__name__, error=str(failure)[:300])
            if self.pending is not None:
                record['deferred_signal'] = self.pending
            record['finished_unix'] = time.time()
            self.save(record)
            for number, handler in handlers.items():
                signal.signal(number, handler)
        if failure is not None:
            raise failure
        return record

    def quiesce(self, record, holder):
        """Stop only the reviewed writer and take its native lock ourselves."""
        deadline = time.monotonic()+self.bounds['stop_seconds']
        self.systemd.stop(self.writer['unit'], self.bounds['stop_seconds'])
        while True:
            state = self.systemd.show(self.writer['unit'])
            if self.stopped(state) and not process_active(self.writer['main_pid'], self.writer['process_start']):
                break
            if time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired('journal writer quiescence', self.bounds['stop_seconds'])
            time.sleep(.05)
        fd, info = open_writer_lock()
        try:
            proof = acquire_writer_lock(fd, info)
        except BaseException:
            os.close(fd)
            raise
        holder[0] = fd  # Released by restore, whatever happens next.
        self.phase(record, 'quiesced', stopped_unix=time.time(), writer_lock=proof, writer_stopped_state=state)

    def check_quiet(self):
        state = self.systemd.show(self.writer['unit'])
        if not self.stopped(state):
            raise Foreign('journal writer restarted during the snapshot')
        inherited_lock.descriptors(required=True, path=self.lock_path)

    def precopy(self, before, estimate, total):
        """While the writer runs, copy the immutable committed sidecars."""
        budget = total.phase(self.bounds['precopy_seconds'], 'journal snapshot sidecar pre-copy',
                             reserve=before['events_bytes']+before['blocks_bytes']+estimate['sidecar_bytes'])
        no_links(SNAPSHOTS)
        SNAPSHOTS.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.namespace_dir(SNAPSHOTS)
        require(not self.target.exists(), 'journal snapshot target appeared')
        self.partial.mkdir(mode=0o700)
        (self.partial/'journal').mkdir(mode=0o700)
        result = precopy_sidecars(self.partial/'journal', self.sidecar_inventory, before['blocks_bytes'], budget)
        budget.check()
        return result

    def copy(self, before, total):
        """Independent private copies of exactly the committed prefix."""
        def guard():
            self.check_quiet()
        budget = total.phase(self.bounds['copy_seconds'], 'journal snapshot copy', guard=guard)
        journal = self.partial/'journal'
        root = journal_root()
        quiet = committed(JOURNAL, self.request)
        require(quiet['blocks'] >= before['blocks'], 'journal reorganized below the pre-quiesce prefix')
        # Observe first, then reserve exactly what remains to be written.
        display, late, blocks_sha = observe_sidecars(self.sidecar_inventory, quiet['blocks_bytes'],
                                                     self.request['sidecars'], budget)
        budget.reserve = quiet['events_bytes']+quiet['blocks_bytes']+display['quiesced_sidecar_bytes']
        reserved = resources(SNAPSHOTS, budget.reserve)
        lengths = {'meta.json':None, 'checkpoint.bin':CHECKPOINT_BYTES,
                   'blocks.bin':quiet['blocks_bytes'], 'events.bin':quiet['events_bytes']}
        files = {name:copy_file(JOURNAL/name, journal/name, lengths[name], budget) for name in FILES}
        require(files['meta.json']['sha256'] == quiet['meta_sha256'] and
                files['checkpoint.bin']['sha256'] == quiet['checkpoint_sha256'] and
                files['blocks.bin']['sha256'] == blocks_sha, 'journal changed while quiesced')
        copy_late(late, journal, budget)
        current = JOURNAL.lstat()
        require((current.st_dev, current.st_ino) == (root.st_dev, root.st_ino), 'journal directory was replaced')
        self.check_quiet()
        budget.check()
        sync_filesystem(self.partial)
        # Format and boundaries now; the byte re-read waits until the writer runs.
        summary = verify_journal(journal, self.request, before, files, budget)
        budget.check()
        return {**summary, 'display':display, 'reserved':reserved}

    def namespace_dir(self, path):
        info = Path(path).lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == OWNER and not info.st_mode & 0o077,
                'snapshot namespace is not a private root-owned directory')

    def retain(self, record, total):
        """Re-derive every digest within the total bound, write the manifest, then publish."""
        snapshot = record['snapshot']
        manifest = {'version':1, 'kind':KIND, 'request_sha256':self.identifier, 'source_sha':self.request['source_sha'],
                    'candidate':self.request['candidate'], 'publication':self.request['publication'],
                    'journal_source':str(JOURNAL), 'journal':snapshot, 'before':record['before'],
                    'writer':{'before':record['writer_before'], 'restored':record['restoration']},
                    'anchors':{'before':record['anchors_before'], 'after':record['anchors_after']},
                    'hosts':record['hosts'], 'qualification':NOT_QUALIFICATION}
        verification = Budget(total.deadline, 'journal snapshot verification', health=self.health)
        contents = inspect(self.partial, self.request, expected_hashes(self.request, manifest), verification)
        require(contents['files'] == snapshot['files'] and contents['sidecars'] == snapshot['display']['sidecars'] and
                contents['records']['tip_hash'] == snapshot['tip_hash'] and
                contents['records']['events'] == snapshot['events'], 'snapshot bytes differ from the bytes copied')
        manifest['contents'] = contents
        raw = durable.canonical(manifest)+b'\n'
        fd = os.open(self.partial/'manifest.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as output:
            output.write(raw); output.flush(); os.fchmod(output.fileno(), 0o400); os.fsync(output.fileno())
        sync_dir(self.partial)
        manifest_sha = hashlib.sha256(raw).hexdigest()
        verify_tree(self.partial, manifest_sha, full=False)
        total.check()
        inherited_lock.descriptors(required=True, path=self.lock_path)
        require(not self.target.exists(), 'journal snapshot target appeared')
        os.rename(self.partial, self.target)
        sync_dir(SNAPSHOTS)
        verify_tree(self.target, manifest_sha, full=False)
        self.phase(record, 'retained', manifest=manifest_sha, target=str(self.target))

    # Restoration of the exact owned writer.

    def settle(self, deadline):
        while True:
            state = self.systemd.show(self.writer['unit'])
            if state.get('ActiveState') not in ('activating', 'deactivating', 'reloading'):
                return state
            if time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired('journal writer state settlement', self.bounds['stop_seconds'])
            time.sleep(.05)

    def restore(self, record, writer_lock, stop_issued, *, owned=()):
        """Restart the reviewed writer; refuse any writer this owner did not run."""
        if writer_lock is not None:
            os.close(writer_lock)  # Release our flock before the writer reopens.
        original = (self.writer['main_pid'], self.writer['process_start'])
        state = self.settle(time.monotonic()+self.bounds['stop_seconds'])
        if state.get('ActiveState') == 'active':
            pid = int(state.get('MainPID') or 0)
            current = (pid, process_start(pid))
            if current == original or current in owned:
                proof = self.running(*current)
                return {'status':'proven', 'started':False, 'writer':proof, 'resources':resources(SNAPSHOTS)}
            raise Foreign('journal writer is running under an unknown owner or restart')
        if not stop_issued:
            raise Foreign('journal writer stopped outside this owner')
        require(self.stopped(state), 'journal writer did not quiesce cleanly')
        self.unit_files(state)
        attempts = list(record.get('restore_attempts', []))
        for number in range(1, self.bounds['restart_attempts']+1):
            attempt = {'attempt':number, 'unix':time.time()}
            attempts.append(attempt)
            # Intent is durable before each start; any PID it yields is owned.
            self.phase(record, 'restoring', restore_attempts=attempts)
            deadline = time.monotonic()+self.bounds['restart_seconds']
            proof = None
            try:
                self.systemd.start(self.writer['unit'], self.bounds['restart_seconds'])
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                attempt['start_error'] = type(error).__name__
            while True:
                state = self.systemd.show(self.writer['unit'])
                pid = int(state.get('MainPID') or 0)
                if state.get('ActiveState') == 'active' and pid:
                    if attempt.get('pid') != pid:
                        attempt.update(pid=pid, process_start=process_start(pid))
                        self.save(record)
                    try:
                        proof = self.running(pid, attempt['process_start'])
                        break
                    except ValueError as error:
                        attempt['unproven'] = str(error)[:200]
                elif self.stopped(state):
                    break
                if time.monotonic() >= deadline:
                    break
                time.sleep(.05)
            if proof is not None:
                # No automatic restart or PID change between start and proof.
                again = self.systemd.show(self.writer['unit'])
                require(again.get('MainPID') == str(proof['pid']) and again.get('NRestarts') == proof['n_restarts'] and
                        process_start(proof['pid']) == proof['process_start'], 'restored writer restarted during proof')
                attempt.pop('unproven', None)
                self.phase(record, 'restored', restore_attempts=attempts)
                return {'status':'proven', 'started':True, 'attempts':attempts, 'writer':proof,
                        'resources':resources(SNAPSHOTS), 'unix':time.time()}
            state = self.settle(time.monotonic()+self.bounds['stop_seconds'])
            self.save(record)
            if not self.stopped(state):
                raise Foreign('restarted writer is running but unproven within its bound; root must inspect it')
        raise ValueError('journal writer was not restored within its reviewed attempts')

    # Explicit reconciliation of an unfinished owner.

    def reconcile(self):
        with self.lock_factory() as lock:
            lock.verify()
            schema_fence.local_schema_fence(skip_input=self.identifier)
            record = self.load()
            require(json.loads((OWNERS/'latest.json').read_text()) == {'request_sha256':self.identifier},
                    'reconcile the latest input owner first')
            require(record['status'] in UNFINISHED, 'journal snapshot does not need reconciliation')
            for key in ('launcher', 'owner'):
                value = record.get(key) or {}
                require(not process_active(value.get('pid'), value.get('process_start')),
                        'journal snapshot %s process is still active; observe it first' % key)
            # Fresh exact owner and process reads of every pinned host first.
            inventory = self.owned_inventory(record)
            require(self.pinned(self.inventory) == self.pinned(inventory),
                    'reconcile inventory differs from the retained snapshot inventory')
            hosts = fleet(inventory, self.budget('journal snapshot reconcile'), self.executor,
                          retain=self.receipts('reconcile'), skip_input=(self.identifier, self.request['machine_id']))
            previous = record['phase']
            stop_issued = PHASES.index(previous) >= PHASES.index('quiescing')
            owned = {(a['pid'], a['process_start']) for a in record.get('restore_attempts', []) if 'pid' in a}
            restoration = record.get('restoration')
            if restoration and restoration.get('status') == 'proven':
                owned.add((restoration['writer']['pid'], restoration['writer']['process_start']))
            proof = self.restore(record, None, stop_issued, owned=owned)
            moves = []
            for path in (self.partial, self.target):
                no_links(path)
                if path.exists():
                    retained = path.with_name(path.name+'.abandoned-'+self.identifier)
                    require(not retained.exists() and retained.parent == path.parent, 'journal snapshot displacement exists')
                    moves.append((path, retained))
            for path, retained in moves:
                os.rename(path, retained); sync_dir(path.parent)
            record.update(status='reconciled', reconciled_unix=time.time(), reconciliation={
                'writer':proof, 'hosts':hosts, 'retained':[str(r) for _, r in moves], 'previous_phase':previous,
                'previous_status':record['status']})
            self.save(record)
            return record

    def run(self, action, expect_plan=None):
        self.identity()
        if action == 'plan':
            return self.plan()
        if action == 'preflight':
            return {**self.preflight(), 'plan_sha256':digest(self.stable_plan())}
        if action == 'status':
            return self.status()
        if action == 'stage':
            return self.stage(expect_plan)
        if action == 'reconcile':
            return self.reconcile()
        raise ValueError('unsupported journal snapshot action')


# --- copies and verification --------------------------------------------------

def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def sync_tree(root):
    for directory, _, _ in os.walk(root):
        sync_dir(directory)


def write_new(path):
    return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)


def copy_file(source, destination, length, budget):
    """Copy `length` bytes (the whole bounded file when None) and hash them."""
    fd, info = open_existing(source, limit=MAX_META if length is None else None)
    try:
        length = info.st_size if length is None else length
        require(info.st_size >= length, Path(source).name+' is shorter than its checkpoint')
        sha, remaining, at = hashlib.sha256(), length, 0
        out = write_new(destination)
        try:
            while remaining:
                budget.check()
                data = os.pread(fd, min(CHUNK, remaining), at)
                require(data, 'journal shortened while taking snapshot')
                written = os.write(out, data)
                require(written == len(data), 'short snapshot write')
                sha.update(data); remaining -= len(data); at += len(data); budget.copied += len(data)
            os.fsync(out); os.fchmod(out, 0o400)
        finally:
            os.close(out)
        after = os.fstat(fd)
        current = Path(source).lstat()
        require(after.st_size >= length and (current.st_dev, current.st_ino) == (info.st_dev, info.st_ino),
                'journal file was truncated or replaced while copying: '+Path(source).name)
    finally:
        os.close(fd)
    return {'size':length, 'sha256':sha.hexdigest()}


def hash_file(path, budget):
    """SHA-256 of a private snapshot file in bounded chunks."""
    fd, _ = open_existing(path)
    sha = hashlib.sha256()
    with os.fdopen(fd, 'rb') as stream:
        while True:
            budget.check()
            data = stream.read(CHUNK)
            if not data:
                return sha.hexdigest()
            sha.update(data)


def sidecar_digest():
    return hashlib.sha256(b'wallet-pir-journal-snapshot-sidecars-v1\n')


INVENTORY = struct.Struct('<32sQQQQ')  # block hash, inode, size, mtime_ns, ctime_ns; inode 0 is absent.


def sidecar_identity(info):
    return (info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def sidecar_source():
    """The live sidecar directory identity, or None when it does not exist."""
    path = JOURNAL/'display-v1'
    if not os.path.lexists(path):
        return None
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode), 'display sidecar directory is not a directory')
    return (info.st_dev, info.st_ino)


def copy_sidecar(internal, journal, budget):
    """Copy one immutable, self-authenticating sidecar; None when absent.

    The writer renames each sidecar into place before the block that names it
    is committed and never rewrites it, so a copy taken while it runs is the
    committed sidecar. Each copy is checked against its block hash and digest.
    The whole snapshot is flushed once by `syncfs`, not per small file.
    """
    budget.check()
    try:
        fd, info = open_existing(sidecar_path(JOURNAL, internal), limit=MAX_SIDECAR)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, 'rb') as stream:
        data = stream.read(MAX_SIDECAR+1)
    check_sidecar(data, internal)
    out = write_new(sidecar_path(journal, internal))
    try:
        require(os.write(out, data) == len(data), 'short sidecar write')
        os.fchmod(out, 0o400)
    finally:
        os.close(out)
    budget.copied += len(data)
    return info


def precopy_sidecars(journal, inventory, blocks_bytes, budget):
    """While the writer runs: copy each committed block's sidecar, recording its identity."""
    directory = sidecar_source()
    (journal/'display-v1').mkdir(mode=0o700)
    count = absent = size = 0
    fd = write_new(inventory)
    with os.fdopen(fd, 'wb') as output:
        for raw in iterate_records(JOURNAL/'blocks.bin', blocks_bytes, budget):
            internal = raw[:32]
            info = copy_sidecar(internal, journal, budget) if directory else None
            if info is None:
                output.write(INVENTORY.pack(internal, 0, 0, 0, 0)); absent += 1
            else:
                output.write(INVENTORY.pack(internal, *sidecar_identity(info))); count += 1; size += info.st_size
        output.flush(); os.fsync(output.fileno())
    require(sidecar_source() == directory, 'display sidecar directory was replaced')
    return {'directory':directory is not None, 'sidecars':count, 'absent':absent, 'bytes':size}


def observe_sidecars(inventory, blocks_bytes, sidecars, budget):
    """Under quiescence, before writing: the source's committed blocks and sidecars.

    Every committed block's source sidecar is observed again while the writer is
    stopped and this owner holds writer.lock. Every block hash read while the
    writer ran must be unchanged, and pre-copied sidecars must keep the same
    inode, size and times. Sidecars still to copy are returned with their exact
    sizes, so disk is reserved for what will actually be written. A block with no
    source sidecar refuses under `every-committed-block`; under `mirror-source`
    it is recorded as absent, which is what the native reader would see, never
    as evidence that the block has no oversized events.
    """
    directory = sidecar_source()
    policy, coverage = sidecars['policy'], set(sidecars['coverage_heights'])
    count = absent = late_bytes = 0
    covered, late, blocks = [], [], hashlib.sha256()
    with open(inventory, 'rb') as stream:
        for height, raw in enumerate(iterate_records(JOURNAL/'blocks.bin', blocks_bytes, budget), START_HEIGHT):
            blocks.update(raw)
            internal = raw[:32]
            entry = stream.read(INVENTORY.size)
            entry = INVENTORY.unpack(entry) if entry else None
            try:
                info = sidecar_path(JOURNAL, internal).lstat() if directory else None
            except FileNotFoundError:
                info = None
            require(info is None or stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
                    'display sidecar is not a single-link regular file at height %d' % height)
            if entry is not None:
                # Independent prefixes: every block read while the writer ran is unchanged.
                require(entry[0] == internal, 'journal differs from its pre-quiesce prefix at height %d' % height)
                if entry[1]:
                    require(info is not None and sidecar_identity(info) == entry[1:],
                            'committed display sidecar changed or vanished at height %d' % height)
                elif info is not None:
                    late.append((internal, sidecar_identity(info))); late_bytes += info.st_size
            elif info is not None:
                late.append((internal, sidecar_identity(info))); late_bytes += info.st_size
            if info is None:
                require(policy != 'every-committed-block', 'committed block %d has no display sidecar' % height)
                absent += 1
            else:
                count += 1
            if height in coverage:
                require(info is not None, 'coverage height %d has no display sidecar' % height)
                covered.append({'height':height, 'hash':display_hex(internal),
                                'event_count':int.from_bytes(raw[40:48], 'little'), 'sidecar_bytes':info.st_size})
    require(len(covered) == len(coverage), 'coverage heights lie beyond the committed snapshot')
    require(sidecar_source() == directory, 'display sidecar directory was replaced')
    summary = {'policy':policy, 'sidecars':count, 'sidecars_absent_in_source':absent,
               'copied_while_quiesced':len(late), 'quiesced_sidecar_bytes':late_bytes, 'coverage':covered}
    return summary, late, blocks.hexdigest()


def copy_late(late, journal, budget):
    """Copy the sidecars observed under quiescence, exactly as observed."""
    for internal, identity in late:
        info = copy_sidecar(internal, journal, budget)
        require(info is not None and sidecar_identity(info) == identity, 'display sidecar changed while quiesced')


def sync_filesystem(path):
    """One `syncfs` for the snapshot's filesystem, then its directories."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.syncfs(fd) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
    finally:
        os.close(fd)
    sync_tree(path)


def verify_journal(journal, request, before, files, budget):
    """Re-read the private copy: format, exact checkpoint, records and anchors."""
    summary = committed(journal, request, exact=True)
    require(summary['meta_sha256'] == files['meta.json']['sha256'] and
            summary['checkpoint_sha256'] == files['checkpoint.bin']['sha256'], 'snapshot metadata differs from copied bytes')
    records = Records(summary['events_bytes'])
    records.want(START_HEIGHT, request['journal']['genesis_hash'])
    records.want(before['tip_height'], before['tip_hash'])
    records.want(request['publication']['anchor_height'], request['publication']['anchor_hash'])
    for raw in iterate_records(journal/'blocks.bin', summary['blocks_bytes'], budget):
        records.feed(raw)
    result = records.finish()
    require(result['tip_height'] == summary['tip_height'] and result['tip_hash'] == summary['tip_hash'],
            'snapshot block records disagree with the checkpoint')
    return {**summary, 'events':result['events'], 'files':files}


def expected_hashes(request, manifest):
    """Every height whose hash was independently observed or reviewed."""
    pairs = [(START_HEIGHT, request['journal']['genesis_hash']),
             (request['publication']['anchor_height'], request['publication']['anchor_hash']),
             (manifest['before']['tip_height'], manifest['before']['tip_hash'])]
    for observation in (manifest['anchors']['before'], manifest['anchors']['after']):
        pairs += [(a['height'], a['hash']) for a in observation['anchors']]
    pairs += [(c['height'], c['hash']) for c in manifest['journal']['display']['coverage']]
    expected = {}
    for height, value in pairs:
        require(expected.setdefault(height, value) == value, 'observed anchors disagree at height %d' % height)
    return expected


def structure(root):
    """Private directories and single-link 0400 regular files, nothing else."""
    root = Path(root)
    no_links(root)
    for directory in (root, root/'journal'):
        info = directory.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == OWNER and stat.S_IMODE(info.st_mode) == 0o700,
                'snapshot directory is not private')
    require(set(os.listdir(root)) <= {'journal', 'manifest.json'}, 'snapshot top-level set differs')
    require(set(os.listdir(root/'journal')) == set(FILES) | {'display-v1'}, 'snapshot journal file set differs')
    for name in FILES:
        private_file((root/'journal'/name).lstat())
    info = (root/'journal'/'display-v1').lstat()
    require(stat.S_ISDIR(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o700 and info.st_uid == OWNER,
            'snapshot sidecar directory is not private')


def private_file(info):
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid == OWNER and
            stat.S_IMODE(info.st_mode) == 0o400, 'snapshot file is not a private single-link regular file')


def inspect(root, request, expected, budget):
    """Re-derive every digest, boundary and anchor from the private copy alone."""
    root = Path(root)
    structure(root)
    journal = root/'journal'
    summary = committed(journal, request, exact=True)
    files = {name:{'size':(journal/name).lstat().st_size, 'sha256':hash_file(journal/name, budget)} for name in FILES}
    count = 0
    with os.scandir(journal/'display-v1') as entries:
        for entry in entries:
            if count % 4096 == 0:
                budget.check()
            private_file(entry.stat(follow_symlinks=False))
            count += 1
    records = Records(summary['events_bytes'])
    for height, value in expected.items():
        records.want(height, value)
    aggregate, present, size = sidecar_digest(), 0, 0
    for height, raw in enumerate(iterate_records(journal/'blocks.bin', summary['blocks_bytes'], budget), START_HEIGHT):
        records.feed(raw)
        path = sidecar_path(journal, raw[:32])
        if os.path.lexists(path):
            budget.check()
            data = read_small(path, MAX_SIDECAR)
            check_sidecar(data, raw[:32])
            aggregate.update(height.to_bytes(8, 'little')+raw[:32]+len(data).to_bytes(8, 'little')+
                             hashlib.sha256(data).digest())
            present += 1; size += len(data)
    result = records.finish()
    require(present == count, 'snapshot holds display sidecars of no committed block')
    require(result['tip_hash'] == summary['tip_hash'], 'snapshot block records disagree with the checkpoint')
    return {'files':files, 'sidecars':present, 'sidecar_bytes':size, 'sidecar_sha256':aggregate.hexdigest(),
            'records':result}


def verify_tree(root, manifest_sha256, *, full, request=None, budget=None):
    """The retained snapshot; the full form re-derives everything within `budget`."""
    root = Path(root)
    structure(root)
    require(set(os.listdir(root)) == {'journal', 'manifest.json'}, 'snapshot top-level set differs')
    manifest_path = root/'manifest.json'
    private_file(manifest_path.lstat())
    raw = read_small(manifest_path, 1 << 20)
    require(hashlib.sha256(raw).hexdigest() == manifest_sha256, 'snapshot manifest differs')
    manifest = json.loads(raw, object_pairs_hook=I.unique)
    for name in FILES:
        require((root/'journal'/name).lstat().st_size == manifest['contents']['files'][name]['size'],
                'snapshot file size differs')
    if full:
        require(budget is not None and request is not None, 'full snapshot verification needs its request and budget')
        expected = expected_hashes(request, manifest)
        require(inspect(root, request, expected, budget) == manifest['contents'], 'snapshot contents differ from the manifest')
    return manifest


def owner_main(expected, *, systemd=None, node=None):
    """`schema-snapshot-owner`: the detached owner, from the retained request."""
    require(isinstance(expected, str) and HEX.fullmatch(expected), 'invalid journal snapshot request digest')
    retained = OWNERS/(expected+'.request.json')
    no_links(retained)
    require(retained.lstat().st_size <= MAX_REQUEST, 'retained journal snapshot request exceeds bound')
    request = validate(json.loads(retained.read_text(), object_pairs_hook=I.unique))
    snapshot = Snapshot(None, request, expected, systemd=systemd, node=node)
    snapshot.identity()
    return snapshot.run_owner()


def read_request(path, expected):
    with Path(path).open('rb') as stream:
        raw = stream.read(MAX_REQUEST+1)
    require(len(raw) <= MAX_REQUEST, 'journal snapshot request exceeds bound')
    request = validate(json.loads(raw, object_pairs_hook=I.unique))
    require(isinstance(expected, str) and HEX.fullmatch(expected) and digest(request) == expected,
            'journal snapshot request differs from its reviewed digest')
    return request


if __name__ == '__main__':
    sys.exit('run through ops/scripts/wallet-pir-deploy.py')
