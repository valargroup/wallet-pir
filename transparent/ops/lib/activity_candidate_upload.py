"""Guarded transfer of the three candidate archives to the pinned coordinator.

Root retains the reviewed `transparent-filter` and `transparent-publisher` CI
bundles and the supplemental archive locally. This path streams exactly those
three pinned archives, after a closed request header, to the coordinator's
staged operations source over pinned SSH. The receiver holds the global
production lock in the same process that receives, records its durable owner in
the shared input-staging namespace before any byte, and renames one complete
private namespace only after all 18 artifacts pass `activity_candidate.collect`.
It installs and runs nothing; CandidatePreparation then reads the retained
archives through its existing plan/preflight/stage path.

There is no caller target path, generic upload or remote command: the remote
argv is the immutable staged wrapper with a fixed action. Local transport loss is
UNKNOWN; root observes the remote owner and reconciles explicitly, never retries.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import select
import shlex
import stat
import subprocess
import tempfile
import time

from wallet_pir_ops import durable, schema_fence
from wallet_pir_ops.deploy.remote import ProductionLock, SSHExecutor

HERE = Path(__file__).parent


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


I = module('candidate_upload_input_stage', HERE/'activity_input_stage.py')
C = I.C
B = module('candidate_upload_bootstrap', HERE/'activity_source_stage.py')
G = B.FLEET
KIND = 'candidate-archive-upload'
# Root-reviewed archive bytes. The two CI bundles are run 37173250956's exact-head
# release artifacts; the supplemental digest is the manifest's.
ARCHIVES = {
    'transparent-filter': '1a3dcc5805dbb4b0cfbf82fe32c0be72b501d350d3f7ae2385b7e3f378506ea1',
    'transparent-publisher': '092fe69ce76f8003714524f77741754448913efb7ca483d3fe36e056a50708fa',
    'supplemental': C.SUPPLEMENTAL_ARCHIVE_SHA256,
}
ORDER = ('transparent-filter', 'transparent-publisher', 'supplemental')
UPLOADS = C.ROOT/'archives'
OWNERS = I.OWNERS
SOURCE = I.SOURCE
PREPARATION = 'preparation-request.json'
MAX_HEADER = 8192
CHUNK = 1 << 20
IDLE_SECONDS = 300
STAGE_SECONDS = 7200
ARCHIVE_MODE = 0o400
STATUSES = ('absent', 'preflight-passed', 'receiving', 'staged', 'failed', 'interrupted', 'reconciled')
require = I.require
digest = I.digest


class Unknown(RuntimeError):
    """The remote outcome is not known locally; observe and reconcile its owner."""


def name(kind):
    return kind+'.tar.gz'


def validate(request):
    require(isinstance(request, dict) and set(request) == {'version', 'kind', 'source_sha', 'candidate_sha', 'ci_run',
            'attempt', 'machine_id', 'archives'} and type(request['version']) is int and request['version'] == 1 and
            request['kind'] == KIND, 'invalid candidate upload request')
    require(isinstance(request['source_sha'], str) and re.fullmatch('[0-9a-f]{40}', request['source_sha']) and
            request['candidate_sha'] == C.SOURCE_SHA and type(request['ci_run']) is int and request['ci_run'] == C.CI_RUN and
            type(request['attempt']) is int and 1 <= request['attempt'] <= 100 and
            isinstance(request['machine_id'], str) and re.fullmatch('[0-9a-f]{32}', request['machine_id']),
            'candidate upload identity differs')
    archives = request['archives']
    require(isinstance(archives, dict) and set(archives) == set(ORDER) and
            all(isinstance(item, dict) and set(item) == {'sha256', 'size'} and item['sha256'] == ARCHIVES[kind] and
                type(item['size']) is int and 0 < item['size'] <= C.MAX_ARCHIVE for kind, item in archives.items()),
            'candidate upload archives differ from the reviewed pins')
    require(len(durable.canonical(request)) <= MAX_HEADER, 'candidate upload request exceeds bound')
    return request


def target(request):
    return UPLOADS/digest(request)


def preparation(request):
    """The closed CandidatePreparation request for the retained archives."""
    root = target(request)
    return {'version':1, 'source_sha':request['source_sha'], 'candidate_sha':C.SOURCE_SHA, 'ci_run':C.CI_RUN,
            'attempt':request['attempt'], 'archives':{kind:str(root/name(kind)) for kind in ORDER}}


def verify_artifacts(archives, scratch):
    """All 18 artifacts, ABI, CI revision and fat-LTO build provenance."""
    payload, digests = C.collect(archives, scratch=scratch)
    require(digests == {kind:ARCHIVES[kind] for kind in ORDER} and
            {k:hashlib.sha256(v).hexdigest() for k, v in payload.items()} == C.ARTIFACTS,
            'candidate archive or artifact digests differ from reviewed pins')
    return {'identity':C.identity(), 'artifacts':len(payload)}


def staged_source():
    """The immutable reviewed source this wrapper runs from, by its receipt."""
    root = Path(__file__).resolve().parents[3]
    source = root.name
    require(root.parent == SOURCE and re.fullmatch('[0-9a-f]{40}', source), 'candidate receiver requires immutable operation source')
    receipt = json.loads((SOURCE.parent/'staging'/(source+'.json')).read_text())
    I.S.verify_receipt(receipt, root, source, receipt['archive_sha256'])
    I.require_release_tool(source)
    return source


def process_start(pid):
    """Kernel start time, so a reused PID is not mistaken for the owner."""
    try:
        raw = Path('/proc/%d/stat' % pid).read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    return int(raw.rsplit(')', 1)[1].split()[19])


def process_active(pid, start):
    if Path('/proc').is_dir():
        current = process_start(pid)
        return current is not None and (start is None or current == start)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True


class Incoming:
    """Bounded reads from a raw pipe with total and idle deadlines."""

    def __init__(self, fd, total=STAGE_SECONDS, idle=IDLE_SECONDS):
        self.fd, self.buffer = fd, b''
        self.deadline, self.idle = time.monotonic()+total, idle

    def fill(self, limit):
        remaining = self.deadline-time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired('candidate upload stream', STAGE_SECONDS)
        ready, _, _ = select.select([self.fd], [], [], min(self.idle, remaining))
        if not ready:
            raise subprocess.TimeoutExpired('candidate upload stream', self.idle)
        data = os.read(self.fd, limit)
        self.buffer += data
        return bool(data)

    def readline(self, limit):
        while b'\n' not in self.buffer and len(self.buffer) <= limit and self.fill(1):
            pass
        line, newline, rest = self.buffer.partition(b'\n')
        require(newline and len(line) < limit, 'invalid candidate upload header')
        self.buffer = rest
        return line+newline

    def read(self, size):
        if not self.buffer:
            self.fill(size)
        data, self.buffer = self.buffer[:size], self.buffer[size:]
        return data


def read_header(incoming, expected):
    request = validate(json.loads(incoming.readline(MAX_HEADER+1), object_pairs_hook=I.unique))
    require(digest(request) == expected, 'candidate upload request differs from reviewed plan')
    return request


class Receiver:
    """Pinned root coordinator side; stdout carries only a bounded status reply."""

    def __init__(self, request, *, owners=None, uploads=None, lock_factory=None, scratch='/dev/shm'):
        self.request = validate(request)
        self.identifier = digest(request)
        self.owners = Path(owners or OWNERS)
        self.uploads = Path(uploads or UPLOADS)
        self.owner = self.owners/(self.identifier+'.json')
        self.retained = self.owners/(self.identifier+'.request.json')
        self.target = self.uploads/self.identifier
        self.partial = self.uploads/(self.identifier+'.receiving')
        self.scratch = scratch
        self.lock_factory = lock_factory or (lambda: ProductionLock({'type':'pinned_host', 'machine_id':request['machine_id']}))

    def identity(self):
        require(os.geteuid() == 0 and Path('/etc/machine-id').read_text().strip() == self.request['machine_id'],
                'candidate receiver machine differs from the coordinator pin')
        require(staged_source() == self.request['source_sha'], 'candidate upload names another operations source')

    def status(self):
        I.no_links(self.owner)
        if not self.owner.exists():
            return {'status':'absent', 'request_sha256':self.identifier}
        require(self.owner.lstat().st_size <= 1 << 20, 'candidate upload owner exceeds bound')
        record = json.loads(self.owner.read_text(), object_pairs_hook=I.unique)
        I.no_links(self.retained)
        require(record.get('kind') == KIND and record.get('request_sha256') == self.identifier and
                json.loads(self.retained.read_text(), object_pairs_hook=I.unique) == self.request,
                'retained candidate upload owner differs')
        if record['status'] == 'staged':
            self.verify(self.target, complete=True)
        return record

    def preflight(self, *, fleet=True):
        schema_fence.local_schema_fence()
        for path in (self.owners, self.uploads, self.owner, self.target, self.partial):
            I.no_links(path)
        require(self.status()['status'] == 'absent' and not self.target.exists() and not self.partial.exists(),
                'candidate upload already owned; inspect status or reconcile')
        I.resources(self.uploads, sum(item['size'] for item in self.request['archives'].values()))
        result={'status':'preflight-passed', 'request_sha256':self.identifier}
        if fleet:result['fleet']=G.fleet(self.request,None,I.S.verify_receipt,B.SURVEY_HELPER)
        return result

    def verify(self, root, complete):
        """Exact private regular files: no links, hard links, drift or extras."""
        I.no_links(root)
        expected = {name(kind) for kind in ORDER} | ({PREPARATION} if complete else set())
        info = root.lstat()
        require(stat.S_ISDIR(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o700 and info.st_uid == C.OWNER,
                'candidate upload directory is not private')
        found = {}
        for entry in os.scandir(root):
            found[entry.name] = entry.stat(follow_symlinks=False)
        require(set(found) == expected, 'candidate upload file set differs')
        for entry, info in found.items():
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid == C.OWNER and
                    stat.S_IMODE(info.st_mode) == ARCHIVE_MODE, 'candidate upload file is not a private regular file')
        for kind in ORDER:
            item = self.request['archives'][kind]
            require(found[name(kind)].st_size == item['size'] and C.checksum(root/name(kind)) == item['sha256'],
                    'candidate upload archive bytes differ')
        if complete:
            require((root/PREPARATION).read_bytes() == durable.canonical(self.preparation())+b'\n',
                    'retained preparation request differs')

    def preparation(self):
        result = preparation(self.request)
        result['archives'] = {kind:str(self.target/name(kind)) for kind in ORDER}
        return result

    def save(self, record):
        durable.atomic_json(self.owner, record, mode=0o600)

    def receive(self, incoming, record, lock):
        health = self.owners/(self.identifier+'.health.ndjson')
        last = 0
        for kind in ORDER:
            item = self.request['archives'][kind]
            sha, remaining = hashlib.sha256(), item['size']
            fd = os.open(self.partial/name(kind), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as output:
                while remaining:
                    lock.verify()
                    if time.monotonic()-last >= 5:
                        sample = I.resources(self.uploads)
                        with health.open('a') as stream:
                            stream.write(json.dumps(dict(sample, received_bytes=record['received_bytes']), sort_keys=True)+'\n')
                            stream.flush(); os.fsync(stream.fileno())
                        last = time.monotonic()
                    data = incoming.read(min(CHUNK, remaining))
                    require(data, 'truncated candidate archive stream')
                    remaining -= len(data); record['received_bytes'] += len(data)
                    output.write(data); sha.update(data)
                output.flush(); os.fchmod(output.fileno(), ARCHIVE_MODE); os.fsync(output.fileno())
            require(sha.hexdigest() == item['sha256'], 'received candidate archive checksum differs')
        require(not incoming.read(1), 'extra candidate upload stream bytes')

    def stage(self, incoming):
        with self.lock_factory() as lock:
            lock.verify(); self.preflight(fleet=False)
            self.owners.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.uploads.mkdir(parents=True, exist_ok=True, mode=0o700)
            durable.atomic_json(self.retained, self.request, mode=0o400)
            record = {'version':1, 'kind':KIND, 'status':'receiving', 'request_sha256':self.identifier,
                      'pid':os.getpid(), 'process_start':process_start(os.getpid()), 'boot_id':G.S.boot_id(), 'started_unix':time.time(),
                      'received_bytes':0, 'target':str(self.target),'machine_id':self.request['machine_id']}
            # Intent and the shared fence pointer exist before any byte.
            self.save(record)
            durable.atomic_json(self.owners/'latest.json', {'request_sha256':self.identifier}, mode=0o600)
            try:
                record['fleet']=G.fleet(self.request,lock,I.S.verify_receipt,B.SURVEY_HELPER,skip=self.identifier)
                self.save(record)
                self.partial.mkdir(mode=0o700)
                (self.owners/(self.identifier+'.health.ndjson')).touch(mode=0o600, exist_ok=False)
                self.receive(incoming, record, lock)
                self.verify(self.partial, complete=False)
                lock.verify(); I.resources(self.uploads)
                checked = verify_artifacts({kind:str(self.partial/name(kind)) for kind in ORDER}, self.scratch)
                self.verify(self.partial, complete=False)
                prepared = self.partial/PREPARATION
                with prepared.open('xb') as output:
                    output.write(durable.canonical(self.preparation())+b'\n')
                    output.flush(); os.fchmod(output.fileno(), ARCHIVE_MODE); os.fsync(output.fileno())
                I.sync_dir(self.partial)
                lock.verify()
                require(not self.target.exists(), 'candidate upload target appeared during transfer')
                os.rename(self.partial, self.target); I.sync_dir(self.uploads)
                self.verify(self.target, complete=True)
                record.update(status='staged', candidate_identity=checked['identity'], artifacts=checked['artifacts'],
                              preparation_request_sha256=digest(self.preparation()),
                              preparation_request=str(self.target/PREPARATION))
            except BaseException as error:
                # Received bytes stay in the partial namespace for reconciliation.
                record.update(status='interrupted' if isinstance(error, (subprocess.TimeoutExpired, KeyboardInterrupt, SystemExit))
                              else 'failed', error_type=type(error).__name__)
                raise
            finally:
                record['finished_unix'] = time.time(); self.save(record)
            return record

    def reconcile(self):
        with self.lock_factory() as lock:
            lock.verify()
            schema_fence.local_schema_fence(skip_input=self.identifier)
            record = self.status()
            require(json.loads((self.owners/'latest.json').read_text()) == {'request_sha256':self.identifier},
                    'reconcile the latest input owner first')
            require(record['status'] in ('receiving', 'failed', 'interrupted'), 'candidate upload does not need reconciliation')
            # The receiver starts no descendants. Holding the lock proves no process
            # retains its descriptor; the recorded owner must also have exited.
            require(not process_active(record['pid'], record.get('process_start')),
                    'candidate upload owner process is still active; observe it before reconciliation')
            record['reconciliation_fleet']=G.fleet(self.request,lock,I.S.verify_receipt,B.SURVEY_HELPER,skip=self.identifier)
            moves = []
            for path in (self.partial, self.target):
                I.no_links(path)
                if path.exists():
                    retained = path.with_name(path.name+'.abandoned-'+self.identifier)
                    require(not retained.exists() and retained.parent == path.parent,
                            'candidate upload displacement already exists')
                    moves.append((path, retained))
            for path, retained in moves:
                # Same directory, so rename never copies or crosses filesystems.
                os.rename(path, retained); I.sync_dir(path.parent)
            record.update(status='reconciled', reconciled_unix=time.time(),
                          retained=[str(retained) for _, retained in moves])
            self.save(record)
            return record


def receive(action, expected, stdin_fd, out):
    """`schema-candidate-receive`: one fixed action on the pinned coordinator."""
    try:
        require(isinstance(expected, str) and I.HEX.fullmatch(expected), 'invalid candidate upload request digest')
        if action in ('preflight', 'stage'):
            incoming = Incoming(stdin_fd)
            receiver = Receiver(read_header(incoming, expected))
        else:
            retained = OWNERS/(expected+'.request.json')
            I.no_links(retained)
            if not retained.exists():
                staged_source()
                out(json.dumps({'request_sha256':expected, 'status':'absent'}, sort_keys=True))
                return 0
            require(retained.lstat().st_size <= MAX_HEADER, 'retained candidate upload request exceeds bound')
            request = validate(json.loads(retained.read_text(), object_pairs_hook=I.unique))
            require(digest(request) == expected, 'retained candidate upload request differs')
            receiver = Receiver(request)
        receiver.identity()
        if action == 'preflight':
            require(not incoming.read(1), 'extra candidate upload preflight bytes')
            result = receiver.preflight()
        elif action == 'stage':
            result = receiver.stage(incoming)
        else:
            result = getattr(receiver, action)()
    except BaseException as error:
        interrupted = isinstance(error, (subprocess.TimeoutExpired, KeyboardInterrupt, SystemExit))
        reply = {'request_sha256':str(expected)[:64], 'status':'interrupted' if interrupted else 'failed'}
        if isinstance(error, ValueError):
            reply['error'] = str(error)[:300]
        out(json.dumps(reply, sort_keys=True))
        return 75 if interrupted else 1
    out(json.dumps({k:v for k, v in result.items() if k in ('request_sha256', 'status', 'received_bytes', 'target',
                    'candidate_identity', 'artifacts', 'preparation_request', 'preparation_request_sha256',
                    'error_type', 'retained','fleet')}, sort_keys=True))
    return 0


class Local:
    """One archive on root's workstation, bound to the identity verified at plan."""

    def __init__(self, kind, path):
        self.kind, self.path = kind, Path(path)
        require(self.path.is_absolute(), 'candidate archive path must be absolute')
        I.no_links(self.path)
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and 0 < info.st_size <= C.MAX_ARCHIVE,
                    'candidate archive is not a bounded regular file without links')
            with os.fdopen(os.dup(fd), 'rb') as stream:
                self.sha256 = hashlib.file_digest(stream, 'sha256').hexdigest()
            after = os.fstat(fd)
        finally:
            os.close(fd)
        self.identity = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        require(self.identity == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                'candidate archive changed while hashing')
        require(self.sha256 == ARCHIVES[kind], 'candidate archive digest differs from reviewed pin: '+kind)
        self.size = info.st_size

    def chunks(self):
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) == self.identity,
                    'candidate archive changed after verification: '+self.kind)
            total, sha = 0, hashlib.sha256()
            for data in iter(lambda: stream.read(CHUNK), b''):
                total += len(data); sha.update(data)
                require(total <= self.size, 'candidate archive grew during streaming')
                yield data
            after = os.fstat(stream.fileno())
        require(total == self.size and sha.hexdigest() == self.sha256 and
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) == self.identity,
                'candidate archive changed during SSH streaming: '+self.kind)


class Upload:
    """Root workstation client against the inventory's pinned remote coordinator."""

    def __init__(self, inventory, source_sha, *, attempt=None, archives=None, request_sha256=None, out=print):
        require(inventory.lock.get('type') == 'remote', 'candidate upload requires a remote coordinator inventory')
        require(inventory.ssh.get('mode') == 'pinned', 'candidate upload requires pinned SSH host keys')
        self.host = inventory.lock['host']
        entry = inventory.hosts[self.host]
        require(isinstance(entry.get('machine_id'), str) and re.fullmatch('[0-9a-f]{32}', entry['machine_id']),
                'candidate upload requires the coordinator machine_id pin')
        require(entry.get('user', inventory.ssh.get('user', 'root')) == 'root' or entry.get('sudo'),
                'candidate upload requires root or the configured sudo identity')
        require(isinstance(source_sha, str) and re.fullmatch('[0-9a-f]{40}', source_sha), 'invalid operations source')
        self.inventory, self.source, self.machine, self.out = inventory, source_sha, entry['machine_id'], out
        self.attempt, self.paths, self.expected = attempt, archives, request_sha256
        self.executor = SSHExecutor(inventory)

    def local(self):
        require(isinstance(self.paths, dict) and set(self.paths) == set(ORDER), 'pass exactly the three candidate archives')
        archives = {kind:Local(kind, self.paths[kind]) for kind in ORDER}
        require(len({a.identity[:2] for a in archives.values()}) == len(ORDER), 'candidate archives must be distinct files')
        request = validate({'version':1, 'kind':KIND, 'source_sha':self.source, 'candidate_sha':C.SOURCE_SHA,
                            'ci_run':C.CI_RUN, 'attempt':self.attempt, 'machine_id':self.machine,
                            'archives':{kind:{'sha256':a.sha256, 'size':a.size} for kind, a in archives.items()}})
        # Private scratch on either workstation OS; the coordinator keeps tmpfs.
        with tempfile.TemporaryDirectory(prefix='wallet-pir-candidate-upload-') as scratch:
            os.chmod(scratch, 0o700)
            checked = verify_artifacts({kind:str(a.path) for kind, a in archives.items()}, scratch)
        for archive in archives.values():
            again = os.lstat(archive.path)
            require((again.st_dev, again.st_ino, again.st_size, again.st_mtime_ns, again.st_ctime_ns) == archive.identity,
                    'candidate archive changed during verification: '+archive.kind)
        return request, archives, checked

    def plan(self, request, checked):
        identifier = digest(request)
        prepared = preparation(request)
        wrapper = ['/usr/bin/python3', '-B', str(SOURCE/self.source/'ops/scripts/wallet-pir-deploy.py'),
                   '--inventory', '<coordinator-inventory>']
        files = ['--request', prepared_path(request), '--request-sha256', digest(prepared)]
        return {'request':request, 'request_sha256':identifier, 'coordinator':self.host, 'target':str(target(request)),
                'candidate_identity':checked['identity'], 'artifacts':checked['artifacts'],
                'preparation_request':prepared, 'preparation_request_path':prepared_path(request),
                'preparation_request_sha256':digest(prepared),
                'bootstrap_fleet':{'inventory_sha256':G.INVENTORY_SHA,'runtime_source_sha':G.RUNTIME_SHA,
                                   'hosts':sorted(G.PINS),'survey_wall_seconds':G.SECONDS},
                'effects':'retains three verified archives under the candidate upload namespace; installs and runs nothing',
                'coordinator_next':[wrapper+['schema-candidate-plan', *files], wrapper+['schema-candidate-preflight', *files],
                                    wrapper+['schema-candidate-stage', *files, '--expect-plan-sha256', '<candidate plan digest>'],
                                    wrapper+['schema-candidate-status', *files]]}

    def argv(self, action, identifier):
        entry = self.inventory.hosts[self.host]
        prefix = ['sudo', '-n', '--'] if entry.get('sudo') else []
        ssh = self.executor.transport(self.host)
        ssh = [*ssh[:-1], '-oControlMaster=no', '-oControlPath=none', ssh[-1]]
        return [*ssh, shlex.join([*prefix, '/usr/bin/python3', '-B', str(SOURCE/self.source/'ops/scripts/wallet-pir-deploy.py'),
                                  'schema-candidate-receive', '--action', action, '--request-sha256', identifier])]

    def reply(self, identifier, code, raw, action):
        try:
            reply = json.loads(raw, object_pairs_hook=I.unique)
            require(isinstance(reply, dict) and reply.get('request_sha256') == identifier and reply.get('status') in STATUSES,
                    'invalid candidate receiver reply')
        except (ValueError, TypeError):
            raise Unknown('candidate receiver reply unavailable; run schema-candidate-upload-status, never retry') from None
        if code in (75, 255) or action != 'status' and reply['status'] in ('receiving', 'interrupted'):
            raise Unknown('candidate receiver outcome unfinished; observe status and reconcile explicitly')
        if code:
            # A failed stage keeps its owner and raw bytes: status, then reconcile.
            raise ValueError('candidate receiver %s refused: %s' % (action, reply.get('error', reply['status'])))
        return reply

    def call(self, action, identifier, header=None, timeout=60):
        try:
            result = subprocess.run(self.argv(action, identifier), input=header or b'', capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Unknown('candidate receiver %s timed out; observe status before any action' % action) from None
        return self.reply(identifier, result.returncode, result.stdout, action)

    def run(self, action, expected=None):
        if action in ('status', 'reconcile'):
            require(isinstance(self.expected, str) and I.HEX.fullmatch(self.expected), 'pass --request-sha256')
            if action == 'reconcile':
                observed = self.call('status', self.expected, timeout=600)
                require(observed['status'] in ('failed', 'receiving', 'interrupted'),
                        'candidate upload owner does not need reconciliation')
            return self.call(action, self.expected, timeout=600 if action == 'status' else 120)
        require(action in ('plan', 'preflight', 'stage'), 'unsupported candidate upload action')
        request, archives, checked = self.local()
        plan = self.plan(request, checked)
        if action == 'plan':
            return plan
        identifier, header = plan['request_sha256'], durable.canonical(request)+b'\n'
        if action == 'stage':
            require(expected == digest(plan), 'candidate upload plan changed')
        remote = self.call('preflight', identifier, header)
        require(remote['status'] == 'preflight-passed', 'candidate receiver preflight did not pass')
        if action == 'preflight':
            return dict(plan, remote=remote)

        def body():
            yield header
            for kind in ORDER:
                yield from archives[kind].chunks()
        try:
            code, raw = I.pump(self.argv('stage', identifier), None, STAGE_SECONDS, body=body())
        except subprocess.TimeoutExpired:
            # Killing local SSH proves nothing about the remote owner's exit.
            raise Unknown('candidate upload transport outcome unknown; observe status and reconcile explicitly') from None
        reply = self.reply(identifier, code, raw, 'stage')
        require(reply['status'] == 'staged' and reply.get('preparation_request_sha256') == plan['preparation_request_sha256'] and
                reply.get('candidate_identity') == C.identity(), 'candidate receiver did not stage the reviewed plan')
        return reply


def prepared_path(request):
    return str(target(request)/PREPARATION)
