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
caller-chosen. The receiver holds the production lock in the executing process
and records its durable owner in the shared input-staging namespace before any
child starts. Children run one at a time in their own session, inherit the lock
descriptor, and are sampled for the 20% memory/disk floors, a fixed memory and
time budget and bounded output. Any failure stops the run and fences further
mutation until explicit reconciliation, which acts only on the recorded child
verified by PID and kernel start time, and holds the lock afresh before
accepting that no descendant survives.

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
import shlex
import signal
import stat
import subprocess
import time

from wallet_pir_ops import durable, inherited_lock, schema_fence
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
KIND = 'candidate-native-execution'
MODES = {'artifact-verification': 'shard-verify', 'native-certificates': 'examples/native_certificate'}
OWNERS = I.OWNERS
SOURCE = I.SOURCE
MACHINE_ID = Path('/etc/machine-id')
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
# Pre-reviewed bounds, reused rather than widened: the report contract's
# 1800 s per native execution and the publication job's 16 GiB MemoryMax for
# native children on this coordinator. Retained 12ce runs took 149 s to load
# the set and 1275 s for all 180 certificates. Changing either is root's call.
TIMEOUT_SECONDS = 1800
MEMORY_BYTES = 16 << 30
FLOOR = .2
SAMPLE_SECONDS = 2
MAX_GAP_SECONDS = 10
STOP_SECONDS = 15
MAX_STDOUT = R.MAX_JSON
MAX_STDERR = 1 << 20
MAX_HEADER = 8192
MAX_REPLY = 4 << 20
STAGE_SECONDS = 4*3600
LOCK_WAIT_SECONDS = 30
# The pinned hosts' read-only ABI report (glibc 2.39) and the candidate's
# x86-64-v3 + pclmulqdq build flags.
GLIBC = 'glibc %d.%d' % C.GLIBC_CEILING
CPU_FLAGS = ('avx', 'avx2', 'bmi1', 'bmi2', 'fma', 'f16c', 'movbe', 'abm', 'xsave', 'sse4_2', 'popcnt', 'pclmulqdq')
STATUSES = ('absent', 'planned', 'preflight-passed', 'running', 'staged', 'failed', 'interrupted', 'reconciled')
ENVIRONMENT = {'PATH': '/usr/sbin:/usr/bin:/sbin:/bin', 'LC_ALL': 'C', 'PYTHONDONTWRITEBYTECODE': '1'}
HEX = I.HEX
require = I.require
digest = I.digest
no_links = I.no_links


class Unknown(RuntimeError):
    """The remote outcome is not known locally; observe and reconcile its owner."""


class Interrupted(BaseException):
    """A signal stopped the receiver; its own child is stopped and retained."""


class Budget(ValueError):
    """A floor, budget, output bound or sampling gap stopped the child."""


def evidence_root():
    return C.ROOT/'executions'


def validate(request):
    require(isinstance(request, dict) and set(request) == {'version', 'kind', 'mode', 'source_sha', 'candidate_sha',
            'candidate_identity', 'preparation_request_sha256', 'publication_sha256', 'machine_id', 'attempt'} and
            type(request['version']) is int and request['version'] == 1 and request['kind'] == KIND and
            request['mode'] in MODES, 'invalid candidate execution request')
    require(isinstance(request['source_sha'], str) and re.fullmatch('[0-9a-f]{40}', request['source_sha']) and
            request['candidate_sha'] == C.SOURCE_SHA and request['candidate_identity'] == C.identity() and
            request['publication_sha256'] == MAP_SHA256 and
            isinstance(request['preparation_request_sha256'], str) and HEX.fullmatch(request['preparation_request_sha256']) and
            isinstance(request['machine_id'], str) and re.fullmatch('[0-9a-f]{32}', request['machine_id']) and
            type(request['attempt']) is int and 1 <= request['attempt'] <= 100,
            'candidate execution identity differs from the pinned candidate and publication')
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


def publication(root=None):
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


def process(pid):
    """Kernel state, process group, session and start ticks; None when absent."""
    try:
        raw = Path('/proc/%d/stat' % pid).read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    fields = raw.rsplit(')', 1)[1].split()
    return {'state': fields[0], 'pgid': int(fields[2]), 'session': int(fields[3]), 'start_ticks': int(fields[19])}


def alive(pid, start_ticks):
    current = process(pid)
    return current is not None and current['state'] != 'Z' and current['start_ticks'] == start_ticks


def members(session, start_ticks):
    """Live processes of a recorded child's own session.

    The kernel never reallocates a PID still naming a live session, so a
    different process at that PID proves the recorded session has ended.
    """
    leader = process(session)
    if leader is not None and leader['start_ticks'] != start_ticks:
        return []
    found = []
    for entry in os.scandir('/proc'):
        if entry.name.isdigit():
            current = process(int(entry.name))
            if (current and current['session'] == session and current['state'] != 'Z' and
                    current['start_ticks'] >= start_ticks):
                found.append(int(entry.name))
    return sorted(found)


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


def terminate(pid, start_ticks):
    """Stop only a verified recorded child's own session, TERM then KILL."""
    if alive(pid, start_ticks):
        current = process(pid)
        require(current['session'] == pid and current['pgid'] == pid,
                'recorded child is alive outside its own session; inspect it, nothing was signalled')
    elif not members(pid, start_ticks):
        return False
    for sig, wait in ((signal.SIGTERM, STOP_SECONDS), (signal.SIGKILL, 5)):
        for target in members(pid, start_ticks):
            try:
                os.kill(target, sig)
            except ProcessLookupError:
                pass
        deadline = time.monotonic()+wait
        while time.monotonic() < deadline and members(pid, start_ticks):
            time.sleep(.05)
        if not members(pid, start_ticks):
            return True
    raise ValueError('recorded child process group survived SIGKILL; inspect it')


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


class Receiver:
    """Pinned root coordinator side; stdout carries only a bounded reply."""

    def __init__(self, request, *, owners=None, evidence=None, publication_root=None, lock_factory=None):
        self.request = validate(request)
        self.identifier = digest(request)
        self.mode = request['mode']
        self.executable = MODES[self.mode]
        self.owners = Path(owners or OWNERS)
        self.owner = self.owners/(self.identifier+'.json')
        self.retained = self.owners/(self.identifier+'.request.json')
        self.evidence = Path(evidence or evidence_root())/self.identifier
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

    def plan(self):
        binary = self.candidate()
        layout = publication(self.root)
        host = verify_host(host_abi())
        return {'version': 1, 'kind': KIND, 'request': self.request, 'request_sha256': self.identifier,
                'mode': self.mode, 'executable': self.executable, 'binary': str(binary),
                'binary_sha256': C.ARTIFACTS[self.executable], 'candidate_identity': C.identity(),
                'publication': {'root': str(self.root), 'map_sha256': MAP_SHA256, 'start': START, 'through': THROUGH,
                                'anchor_hash': ANCHOR_HASH, 'recent_from': RECENT_FROM, 'shards': SHARDS,
                                'segments': SEGMENTS, 'manifests': sorted(layout['manifests'])},
                'host': host, 'evidence': str(self.evidence),
                'budgets': {'timeout_seconds': TIMEOUT_SECONDS, 'memory_bytes': MEMORY_BYTES, 'floor': FLOOR,
                            'sample_seconds': SAMPLE_SECONDS, 'max_gap_seconds': MAX_GAP_SECONDS,
                            'max_stdout': MAX_STDOUT, 'max_stderr': MAX_STDERR, 'concurrency': 1},
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

    def checked(self):
        schema_fence.local_schema_fence()
        for path in (self.owners, self.owner, self.retained, self.evidence):
            no_links(path)
        require(self.status()['status'] == 'absent' and not self.evidence.exists() and not self.retained.exists(),
                'candidate execution already owned; inspect status or reconcile')
        plan = self.plan()
        self.floors(resources(self.paths()))
        return plan

    def paths(self):
        return (self.evidence.parent, self.root, Path('/'))

    @staticmethod
    def floors(sample):
        if not (sample['memory_available'] >= FLOOR and sample['disk_available'] and
                all(v >= FLOOR for v in sample['disk_available'].values())):
            raise Budget('memory or disk headroom below 20 percent')

    def save(self, record):
        durable.atomic_json(self.owner, record, mode=0o600)

    def signalled(self, *_):
        if self.critical:
            self.pending = True
            return
        raise Interrupted('candidate execution receiver interrupted')

    def stage(self, expected_plan):
        require(isinstance(expected_plan, str) and HEX.fullmatch(expected_plan), 'pass the reviewed plan digest')
        handlers = {sig: signal.signal(sig, self.signalled) for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)}
        try:
            with self.lock_factory() as lock:
                lock.verify()
                plan = self.checked()
                require(digest(plan) == expected_plan, 'candidate execution plan changed since review')
                self.owners.mkdir(parents=True, exist_ok=True, mode=0o700)
                self.evidence.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                durable.atomic_json(self.retained, self.request, mode=0o400)
                record = {'version': 1, 'kind': KIND, 'status': 'running', 'request_sha256': self.identifier,
                          'mode': self.mode, 'plan_sha256': expected_plan, 'evidence': str(self.evidence),
                          'receiver': {'pid': os.getpid(), 'start_ticks': process(os.getpid())['start_ticks']},
                          'started_unix': time.time(), 'completed': 0, 'child': None}
                # Owner and the shared fence pointer exist before any child starts.
                self.save(record)
                durable.atomic_json(self.owners/'latest.json', {'request_sha256': self.identifier}, mode=0o600)
                try:
                    self.evidence.mkdir(mode=0o700)
                    write_once(self.evidence/'plan.json', durable.canonical(plan)+b'\n')
                    inputs = self.retain_inputs(plan)
                    executions = []
                    for item in plan['dispatches']:
                        executions.append(self.execute(lock, plan, item, record))
                        record['completed'] += 1
                        self.save(record)
                    references = self.references(inputs, plan, executions)
                    record.update(status='staged', references=references, child=None,
                                  gate='unevaluated: run activity-candidate-report on the retained references')
                except BaseException as error:
                    record.update(status='interrupted' if isinstance(error, (Interrupted, KeyboardInterrupt, SystemExit))
                                  else 'failed', error_type=type(error).__name__, error=str(error)[:300])
                    raise
                finally:
                    record['finished_unix'] = time.time()
                    self.save(record)
                return record
        finally:
            self.critical = self.pending = False
            for sig, handler in handlers.items():
                signal.signal(sig, handler)

    def retain_inputs(self, plan):
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

    def table(self, item):
        """Fixed table path: identity, boundary and hash before the child reads it."""
        segment = item['segment']
        path = Path(segment['path'])
        require(path == self.root/segment['manifest_digest']/('%s.%d.bin' % (segment['table'], segment['segment'])),
                'certificate table path is not the fixed publication path')
        no_links(path)
        info = immutable(path, 'file')
        require(info.st_size == segment['size'] and C.checksum(path) == segment['sha256'] and
                identity_of(path.lstat()) == identity_of(info), 'certificate table bytes differ from the manifest')
        return identity_of(info)

    def execute(self, lock, plan, item, record):
        """One native child: verified inputs, sampled run, retained raw capture."""
        lock.verify()
        directory = self.evidence/item['key']
        directory.mkdir(mode=0o700)
        binary = C.binary(self.executable)
        require(str(binary) == plan['binary'] == item['argv'][0], 'candidate executable path differs from plan')
        C.abi(self.executable, binary.read_bytes())
        verify_host(host_abi())
        before = self.table(item) if self.mode == 'native-certificates' else None
        files = {name: directory/name for name in ('owner.json', 'native.json', 'stderr.log', 'health.ndjson',
                                                   'health.json', 'result.json')}
        samples, child, failure, code, ended = [], None, None, None, None
        health = files['health.ndjson'].open('x')

        def observe(pids=()):
            sample = resources(self.paths())
            sample['child_rss_bytes'] = rss(pids)
            gap = bool(samples) and sample['observed_unix']-samples[-1]['observed_unix'] > MAX_GAP_SECONDS
            samples.append(sample)
            health.write(json.dumps(sample, sort_keys=True)+'\n')
            health.flush(); os.fsync(health.fileno())
            if gap:
                raise Budget('resource sampling gap exceeded %d seconds' % MAX_GAP_SECONDS)
            self.floors(sample)
            if sample['child_rss_bytes'] > MEMORY_BYTES:
                raise Budget('native child exceeded the memory budget')
            return sample

        started = start_ticks = None
        stdout = os.open(files['native.json'], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        stderr = os.open(files['stderr.log'], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            observe()
            self.critical = True
            try:
                started = time.time()
                descriptors = lock.descriptors()
                child = subprocess.Popen(item['argv'], stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                                         cwd=directory, start_new_session=True, close_fds=True, pass_fds=descriptors,
                                         env=dict(ENVIRONMENT, **{inherited_lock.VARIABLE: ','.join(map(str, descriptors))}))
                start_ticks = process(child.pid)['start_ticks']
                owner = {'version': 1, 'kind': KIND, 'request_sha256': self.identifier, 'key': item['key'],
                         'mode': self.mode, 'executable': self.executable, 'binary': str(binary),
                         'binary_sha256': C.ARTIFACTS[self.executable], 'argv': item['argv'],
                         'native_source_sha': C.SOURCE_SHA, 'candidate_sha256': C.identity(),
                         'publication_sha256': MAP_SHA256, 'operations_source_sha': self.request['source_sha'],
                         'pid': child.pid, 'start_ticks': start_ticks, 'pgid': child.pid, 'started_unix': started,
                         'timeout_seconds': TIMEOUT_SECONDS, 'memory_budget_bytes': MEMORY_BYTES,
                         'receiver': record['receiver'], 'host': plan['host'], 'segment': item.get('segment')}
                write_once(files['owner.json'], durable.canonical(owner)+b'\n')
                record['child'] = {'key': item['key'], 'pid': child.pid, 'start_ticks': start_ticks}
                self.save(record)
            finally:
                self.critical = False
            if self.pending:
                raise Interrupted('candidate execution receiver interrupted')
            deadline = time.monotonic()+TIMEOUT_SECONDS
            while True:
                try:
                    code = child.wait(timeout=SAMPLE_SECONDS)
                    ended = time.time()
                    break
                except subprocess.TimeoutExpired:
                    pass
                lock.verify()
                observe(members(child.pid, start_ticks))
                if os.fstat(stdout).st_size > MAX_STDOUT or os.fstat(stderr).st_size > MAX_STDERR:
                    raise Budget('native output exceeded its bound')
                if time.monotonic() > deadline:
                    raise Budget('native child exceeded the time budget')
            observe()
            require(os.fstat(stdout).st_size <= MAX_STDOUT and os.fstat(stderr).st_size <= MAX_STDERR,
                    'native output exceeded its bound')
            require(code == 0, 'native child exited %s' % code)
            require(not members(child.pid, start_ticks), 'native child left live descendants')
            if before is not None:
                require(identity_of(Path(item['segment']['path']).lstat()) == before, 'certificate table changed during execution')
            require(C.checksum(binary) == C.ARTIFACTS[self.executable], 'candidate executable changed during execution')
        except BaseException as error:
            # Cleanup and retention finish before any later signal is honoured.
            self.critical, failure = True, error
            if child is not None:
                if child.poll() is None:
                    terminate(child.pid, start_ticks)
                    child.wait()
                elif start_ticks is not None:
                    terminate(child.pid, start_ticks)
            if child is not None and ended is None:
                ended = time.time()
            if child is not None:
                try:
                    samples.append(resources(self.paths()))
                except OSError:
                    pass
            raise
        finally:
            self.critical = True
            for fd in (stdout, stderr):
                os.fsync(fd); os.fchmod(fd, 0o400); os.close(fd)
            health.close(); os.chmod(files['health.ndjson'], 0o400)
            write_once(files['health.json'], json.dumps(samples, sort_keys=True).encode()+b'\n')
            interrupted = isinstance(failure, (Interrupted, KeyboardInterrupt, SystemExit))
            result = {'status': 'passed' if failure is None else 'interrupted' if interrupted else 'failed',
                      'exit_code': child.returncode if child is not None else None,
                      'pid': child.pid if child is not None else None, 'started_unix': started, 'ended_unix': ended,
                      'timeout_seconds': TIMEOUT_SECONDS, 'memory_budget_bytes': MEMORY_BYTES,
                      'max_child_rss_bytes': max((s.get('child_rss_bytes', 0) for s in samples), default=0)}
            if failure is not None:
                result.update(error_type=type(failure).__name__, error=str(failure)[:300])
            write_once(files['result.json'], durable.canonical(result)+b'\n')
            I.sync_dir(directory)
        refs = {'owner': reference(files['owner.json']), 'result': reference(files['result.json']),
                'native': reference(files['native.json']), 'stderr': reference(files['stderr.log']),
                'health': reference(files['health.json'])}
        # The shared raw capture contract; gate evaluation stays offline.
        R.capture(refs, self.executable, MAP_SHA256)
        record['child'] = None
        self.critical = False
        if self.pending:
            raise Interrupted('candidate execution receiver interrupted')
        return refs

    def reconcile(self):
        """Stop only the verified recorded child, then prove the lock is free."""
        record = self.status()
        require(load(self.owners/'latest.json') == {'request_sha256': self.identifier},
                'reconcile the latest input owner first')
        require(record['status'] in ('running', 'failed', 'interrupted'), 'candidate execution does not need reconciliation')
        receiver = record['receiver']
        require(not alive(receiver['pid'], receiver['start_ticks']),
                'candidate execution receiver is still active; observe it before reconciliation')
        observed = {'child': record.get('child'), 'action': 'no-live-child'}
        child = record.get('child')
        if child:
            current = process(child['pid'])
            if current is not None and current['state'] != 'Z' and current['start_ticks'] != child['start_ticks']:
                observed['action'] = 'pid-reused-untouched'
            elif terminate(child['pid'], child['start_ticks']):
                observed['action'] = 'terminated-own-process-group'
            require(not members(child['pid'], child['start_ticks']), 'recorded child session still has live members')
        # The child inherited the lock; acquiring it proves no descendant holds it.
        deadline = time.monotonic()+LOCK_WAIT_SECONDS
        while True:
            try:
                lock = self.lock_factory().__enter__()
                break
            except BlockingIOError:
                require(time.monotonic() < deadline, 'production lock is still held; an unknown descendant or '
                        'another owner remains, inspect before any mutation')
                time.sleep(.2)
        try:
            lock.verify()
            schema_fence.local_schema_fence(skip_input=self.identifier)
            require(self.status() == record, 'candidate execution owner changed during reconciliation')
            record.update(status='reconciled', reconciled_unix=time.time(), reconciliation=observed)
            self.save(record)
            return record
        finally:
            lock.__exit__(None, None, None)


def read_header(fd, expected):
    raw = b''
    while not raw.endswith(b'\n'):
        data = os.read(fd, MAX_HEADER+1-len(raw))
        require(data and len(raw)+len(data) <= MAX_HEADER, 'invalid candidate execution request header')
        raw += data
    request = validate(json.loads(raw, object_pairs_hook=I.unique))
    require(digest(request) == expected, 'candidate execution request differs from reviewed digest')
    return request


def receive(action, expected, stdin_fd, out, expect_plan=None):
    """`schema-candidate-execute-receive`: one fixed action on the pinned coordinator."""
    def reply(value):
        try:
            out(json.dumps(value, sort_keys=True))
        except OSError:
            # Lost transport: the durable owner is the outcome.
            pass
    try:
        require(isinstance(expected, str) and HEX.fullmatch(expected), 'invalid candidate execution request digest')
        if action in ('plan', 'preflight', 'stage'):
            receiver = Receiver(read_header(stdin_fd, expected))
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
        if action == 'plan':
            plan = receiver.plan()
            result = {'status': 'planned', 'request_sha256': expected, 'plan': plan, 'plan_sha256': digest(plan)}
        elif action == 'stage':
            result = receiver.stage(expect_plan)
        else:
            result = getattr(receiver, action)()
    except BaseException as error:
        interrupted = isinstance(error, (Interrupted, KeyboardInterrupt, SystemExit))
        value = {'request_sha256': str(expected)[:64], 'status': 'interrupted' if interrupted else 'failed'}
        if isinstance(error, ValueError):
            value['error'] = str(error)[:300]
        reply(value)
        return 75 if interrupted else 1
    reply({k: v for k, v in result.items() if k in ('request_sha256', 'status', 'plan', 'plan_sha256', 'mode',
           'completed', 'references', 'evidence', 'child', 'gate', 'error_type', 'error', 'reconciliation')})
    return 0


class Execution:
    """Root workstation client against the inventory's pinned remote coordinator."""

    def __init__(self, inventory, source_sha, *, mode=None, attempt=None, preparation=None, request_sha256=None):
        require(inventory.lock.get('type') == 'remote', 'candidate execution requires a remote coordinator inventory')
        require(inventory.ssh.get('mode') == 'pinned', 'candidate execution requires pinned SSH host keys')
        self.host = inventory.lock['host']
        entry = inventory.hosts[self.host]
        require(isinstance(entry.get('machine_id'), str) and re.fullmatch('[0-9a-f]{32}', entry['machine_id']),
                'candidate execution requires the coordinator machine_id pin')
        require(entry.get('user', inventory.ssh.get('user', 'root')) == 'root' or entry.get('sudo'),
                'candidate execution requires root or the configured sudo identity')
        require(isinstance(source_sha, str) and re.fullmatch('[0-9a-f]{40}', source_sha), 'invalid operations source')
        self.inventory, self.source, self.machine = inventory, source_sha, entry['machine_id']
        self.mode, self.attempt, self.preparation, self.expected = mode, attempt, preparation, request_sha256
        self.executor = SSHExecutor(inventory)

    def request(self):
        return validate({'version': 1, 'kind': KIND, 'mode': self.mode, 'source_sha': self.source,
                         'candidate_sha': C.SOURCE_SHA, 'candidate_identity': C.identity(),
                         'preparation_request_sha256': self.preparation, 'publication_sha256': MAP_SHA256,
                         'machine_id': self.machine, 'attempt': self.attempt})

    def argv(self, action, identifier, expect_plan=None):
        entry = self.inventory.hosts[self.host]
        prefix = ['sudo', '-n', '--'] if entry.get('sudo') else []
        ssh = self.executor.transport(self.host)
        ssh = [*ssh[:-1], '-oControlMaster=no', '-oControlPath=none', ssh[-1]]
        remote = [*prefix, '/usr/bin/python3', '-B', str(SOURCE/self.source/'ops/scripts/wallet-pir-deploy.py'),
                  'schema-candidate-execute-receive', '--action', action, '--request-sha256', identifier]
        if expect_plan is not None:
            remote += ['--expect-plan-sha256', expect_plan]
        return [*ssh, shlex.join(remote)]

    def call(self, action, identifier, header=b'', timeout=600, expect_plan=None):
        try:
            result = subprocess.run(self.argv(action, identifier, expect_plan), input=header, capture_output=True,
                                    timeout=timeout)
        except subprocess.TimeoutExpired:
            # Killing local SSH proves nothing about the remote owner or its child.
            raise Unknown('candidate execution %s transport outcome unknown; run status, then reconcile' % action) from None
        try:
            require(len(result.stdout) <= MAX_REPLY, 'candidate execution reply exceeds bound')
            reply = json.loads(result.stdout, object_pairs_hook=I.unique)
            require(isinstance(reply, dict) and reply.get('request_sha256') == identifier and reply.get('status') in STATUSES,
                    'invalid candidate execution reply')
        except (ValueError, TypeError):
            raise Unknown('candidate execution reply unavailable; run status, never retry') from None
        if result.returncode in (75, 255) or action != 'status' and reply['status'] in ('running', 'interrupted'):
            raise Unknown('candidate execution outcome unfinished; observe status and reconcile explicitly')
        if result.returncode:
            raise ValueError('candidate execution %s refused: %s' % (action, reply.get('error', reply['status'])))
        return reply

    def run(self, action, expect_plan=None):
        if action in ('status', 'reconcile'):
            require(isinstance(self.expected, str) and HEX.fullmatch(self.expected), 'pass --request-sha256')
            if action == 'reconcile':
                observed = self.call('status', self.expected)
                require(observed['status'] in ('running', 'failed', 'interrupted'),
                        'candidate execution owner does not need reconciliation')
            return self.call(action, self.expected, timeout=600 if action == 'status' else 120)
        require(action in ('plan', 'preflight', 'stage'), 'unsupported candidate execution action')
        request = self.request()
        identifier, header = digest(request), durable.canonical(request)+b'\n'
        if action == 'stage':
            require(isinstance(expect_plan, str) and HEX.fullmatch(expect_plan), 'pass --expect-plan-sha256')
            remote = self.call('preflight', identifier, header)
            require(remote['status'] == 'preflight-passed' and remote.get('plan_sha256') == expect_plan,
                    'candidate execution plan changed since review')
            reply = self.call('stage', identifier, header, timeout=STAGE_SECONDS, expect_plan=expect_plan)
            require(reply['status'] == 'staged', 'candidate execution did not retain complete evidence')
            return reply
        reply = self.call(action, identifier, header)
        if action == 'plan':
            plan = reply.get('plan')
            require(isinstance(plan, dict) and plan.get('request') == request and reply.get('plan_sha256') == digest(plan),
                    'candidate execution plan reply differs from the request')
        return reply
