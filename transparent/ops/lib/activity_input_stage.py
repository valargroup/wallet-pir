"""Checksum-bound streaming of immutable v11 worker inputs through the wrapper.

The coordinator owns the global production lock until SSH exits. A pinned root
receiver owns its own host lock, records intent before bytes, and only renames a
complete private candidate after native verification. No live unit, executable,
active record, cache or route is changed. Partial copies stay fenced until an
explicit locked reconciliation retains them in an abandoned namespace.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import shlex
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

P = module('input_publication', HERE/'activity_publication_job.py')
S = module('input_source', HERE/'activity_source_stage_host.py')
ROOT = Path('/srv/transparent-pir/v11/publications')
OWNERS = Path('/srv/transparent-activity/ops/input-staging')
SOURCE = Path('/srv/transparent-activity/ops/sources')
MAX_REQUEST = 8 << 20
MAX_FILES = 65536
MAX_BYTES = 192 << 30
CHUNK = 1 << 20
HEX = re.compile('[0-9a-f]{64}')
ID = re.compile('[a-zA-Z0-9-]{1,64}')
INPUTS = {'.inputs/transparent-shard-server':0o755, '.inputs/shard-control':0o755,
          '.inputs/transparent-shard-server.service':0o644}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def digest(value):
    return durable.digest(value)


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate input-staging field')
        result[key] = value
    return result


def safe(path):
    require(isinstance(path, str) and path and '\\' not in path and
            not any(ord(c) < 32 for c in path), 'unsafe input path')
    p = PurePosixPath(path)
    require(str(p) == path and '..' not in p.parts and '.' not in p.parts, 'unsafe input path')
    return p


def validate(request):
    require(isinstance(request, dict) and set(request) == {'version','source_sha','machine_id','worker_id',
            'map_sha256','assignment_sha256','release_result_sha256','attempt','cache_bytes','files'}, 'invalid input-staging request')
    require(type(request['version']) is int and request['version'] == 1 and
            isinstance(request['source_sha'], str) and re.fullmatch('[0-9a-f]{40}', request['source_sha']) and
            isinstance(request['machine_id'], str) and re.fullmatch('[0-9a-f]{32}', request['machine_id']) and
            isinstance(request['worker_id'], str) and ID.fullmatch(request['worker_id']), 'invalid staging identity')
    for key in ('map_sha256','assignment_sha256','release_result_sha256'):
        require(isinstance(request[key], str) and HEX.fullmatch(request[key]), 'invalid staging digest')
    require(type(request['attempt']) is int and 1 <= request['attempt'] <= 100 and
            type(request['cache_bytes']) is int and 0 < request['cache_bytes'] <= 32 << 30, 'invalid staging bounds')
    files = request['files']
    require(isinstance(files, list) and 5 <= len(files) <= MAX_FILES, 'invalid input file count')
    names, total = set(), 0
    for item in files:
        require(isinstance(item, dict) and set(item) == {'source','path','size','sha256','mode'}, 'invalid input file')
        path = safe(item['path']); source = safe(item['source'])
        require(not path.is_absolute() and source.is_absolute() and item['path'] not in names, 'duplicate/absolute input target')
        # Only pinned publication artifacts, assignment and three preparation
        # inputs can be transmitted. All target paths are relative to v11.
        if item['path'] in INPUTS:
            mode = INPUTS[item['path']]
        else:
            require(item['path'] in ('shards.json','assignment.json') or
                    (len(path.parts) >= 2 and HEX.fullmatch(path.parts[0])), 'unsupported candidate path')
            mode = 0o600
        require(type(item['mode']) is int and item['mode'] == mode and
                type(item['size']) is int and 0 <= item['size'] <= MAX_BYTES and
                isinstance(item['sha256'], str) and HEX.fullmatch(item['sha256']), 'invalid candidate file identity')
        total += item['size']; names.add(item['path'])
    require(total <= MAX_BYTES and {'shards.json','assignment.json',*INPUTS} <= names, 'incomplete/oversized candidate')
    require(not any(any(parent.as_posix() in names for parent in PurePosixPath(name).parents if str(parent) != '.')
                    for name in names), 'file/directory collision')
    by_name = {f['path']:f for f in files}
    require(by_name['shards.json']['sha256'] == request['map_sha256'] and
            by_name['assignment.json']['sha256'] == request['assignment_sha256'], 'candidate identity disagrees')
    require(len(durable.canonical(request)) <= MAX_REQUEST, 'input request exceeds bound')
    return request


def read_request(stream, expected):
    raw = stream.readline(MAX_REQUEST+1)
    require(raw.endswith(b'\n') and len(raw) <= MAX_REQUEST, 'invalid input stream header')
    request = validate(json.loads(raw, object_pairs_hook=unique))
    require(digest(request) == expected, 'input request differs from reviewed plan')
    return request


def no_links(path):
    for parent in [Path(path), *Path(path).parents]:
        require(not parent.is_symlink(), 'candidate path contains a symlink')


def resources(root, remaining=0):
    memory = dict(line.split(':',1) for line in Path('/proc/meminfo').read_text().splitlines())
    require(int(memory['MemAvailable'].split()[0])*5 >= int(memory['MemTotal'].split()[0]), 'input staging memory below 20 percent')
    parent = root
    while not parent.exists(): parent = parent.parent
    samples = {}
    for path,reserve in ((parent,remaining),(Path('/'),0)):
        no_links(path)
        disk = os.statvfs(path)
        require(disk.f_bavail*disk.f_frsize-reserve >= .2*disk.f_blocks*disk.f_frsize,
                'input staging disk reserve below 20 percent')
        samples[str(path)] = disk.f_bavail/disk.f_blocks
    return {'unix':time.time(),'memory_available':int(memory['MemAvailable'].split()[0])/int(memory['MemTotal'].split()[0]),
            'disk_available':samples}


def verify_files(root, request):
    no_links(root)
    paths = list(root.rglob('*'))
    require(len(paths) <= MAX_FILES*4 and not any(p.is_symlink() for p in paths), 'invalid candidate file tree')
    require({str(p.relative_to(root)) for p in paths if not p.is_dir()} == {i['path'] for i in request['files']},
            'candidate file set differs')
    for item in request['files']:
        path = root/item['path']; info = path.stat()
        require(stat.S_ISREG(info.st_mode) and info.st_size == item['size'] and stat.S_IMODE(info.st_mode) == item['mode'] and
                P.checksum(path) == item['sha256'], 'candidate checksum/size/mode differs')


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


class Receiver:
    def __init__(self, request, *, root=ROOT, owners=OWNERS, lock_factory=None):
        self.request = validate(request)
        self.root, self.owners = Path(root), Path(owners)
        self.identifier = digest(request)
        self.path = self.owners/(self.identifier+'.json')
        self.target = self.root/request['map_sha256']
        self.partial = self.root/(request['map_sha256']+'.receiving-'+self.identifier)
        self.lock_factory = lock_factory or (lambda: ProductionLock({'type':'pinned_host','machine_id':request['machine_id']}))

    def identity(self):
        require(os.geteuid() == 0 and Path('/etc/machine-id').read_text().strip() == self.request['machine_id'], 'input receiver machine differs')
        source = self.request['source_sha']
        require(Path(__file__).resolve().parents[3] == SOURCE/source, 'input receiver requires pinned immutable operation source')
        receipt = json.loads((SOURCE.parent/'staging'/(source+'.json')).read_text())
        S.verify_receipt(receipt, SOURCE/source, source, receipt['archive_sha256'])

    def status(self):
        no_links(self.path)
        if not self.path.exists(): return {'status':'absent','request_sha256':self.identifier}
        require(self.path.stat().st_size <= MAX_REQUEST+4096, 'input owner exceeds bound')
        result = json.loads(self.path.read_text(), object_pairs_hook=unique)
        require(result['request_sha256'] == self.identifier and
                json.loads((self.owners/(self.identifier+'.request.json')).read_text(), object_pairs_hook=unique) == self.request,
                'retained input request differs')
        if result['status'] == 'staged': verify_files(self.target, self.request)
        return result

    def preflight(self):
        schema_fence.local_schema_fence()
        no_links(self.root); no_links(self.owners)
        require(self.status()['status'] == 'absent' and not self.target.exists() and not self.partial.exists(),
                'candidate already exists; inspect/reconcile its owner')
        resources(self.root, sum(i['size'] for i in self.request['files']))
        return {'status':'preflight-passed','request_sha256':self.identifier}

    def native(self, lock):
        argv = [str(self.partial/'.inputs/transparent-shard-server'), '--shard-dir', str(self.partial),
                '--assignment', str(self.partial/'assignment.json'), '--worker-id', self.request['worker_id'],
                '--cache-bytes', str(self.request['cache_bytes']), '--verify-only']
        with (self.owners/(self.identifier+'.native.log')).open('xb') as log:
            os.fchmod(log.fileno(),0o600)
            process = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, **inherited_lock.options())
            durable.atomic_json(self.owners/(self.identifier+'.native.owner.json'), {'pid':process.pid,'started_unix':time.time(),
                'binary_sha256':P.checksum(self.partial/'.inputs/transparent-shard-server'),'request_sha256':self.identifier}, mode=0o600)
            try:
                deadline = time.monotonic()+300
                while process.poll() is None:
                    lock.verify()
                    sample=resources(self.root)
                    with (self.owners/(self.identifier+'.health.ndjson')).open('a') as health:
                        health.write(json.dumps(dict(sample,stage='native-verification'),sort_keys=True)+'\n');health.flush();os.fsync(health.fileno())
                    require(time.monotonic() < deadline, 'native candidate verification timed out')
                    time.sleep(1)
                require(process.returncode == 0, 'native candidate verification failed; inspect private log')
            except BaseException as error:
                if process.poll() is None: process.kill()
                process.wait()
                durable.atomic_json(self.owners/(self.identifier+'.native.result.json'),
                    {'status':'failed','pid':process.pid,'exit_code':process.returncode,'ended_unix':time.time(),
                     'error_type':type(error).__name__,'log_sha256':P.checksum(log.name)},mode=0o600)
                raise
            durable.atomic_json(self.owners/(self.identifier+'.native.result.json'),
                {'status':'passed','pid':process.pid,'exit_code':process.returncode,'ended_unix':time.time(),
                 'log_sha256':P.checksum(log.name)},mode=0o600)

    def stage(self, stream):
        with self.lock_factory() as lock:
            lock.verify(); self.preflight()
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.owners.mkdir(parents=True, exist_ok=True, mode=0o700)
            result = {'version':1,'status':'receiving','request_sha256':self.identifier,
                      'pid':os.getpid(),'started_unix':time.time(),'received_bytes':0}
            durable.atomic_json(self.owners/(self.identifier+'.request.json'),self.request,mode=0o400)
            durable.atomic_json(self.path,result,mode=0o600)
            durable.atomic_json(self.owners/'latest.json',{'request_sha256':self.identifier},mode=0o600)
            old = os.environ.get(inherited_lock.VARIABLE)
            os.environ[inherited_lock.VARIABLE] = ','.join(map(str,lock.descriptors()))
            os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
            try:
                self.partial.mkdir(mode=0o700)
                last = 0
                health_path=self.owners/(self.identifier+'.health.ndjson')
                health_path.touch(mode=0o600,exist_ok=False)
                for item in self.request['files']:
                    output = self.partial/item['path']; output.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
                    sha = hashlib.sha256(); remaining = item['size']
                    with output.open('xb') as handle:
                        while remaining:
                            lock.verify()
                            if time.monotonic()-last >= 5:
                                sample=resources(self.root)
                                with health_path.open('a') as health:
                                    health.write(json.dumps(dict(sample,received_bytes=result['received_bytes']),sort_keys=True)+'\n'); health.flush(); os.fsync(health.fileno())
                                last=time.monotonic()
                            data = stream.read(min(CHUNK,remaining))
                            require(data, 'truncated input file stream')
                            remaining -= len(data); result['received_bytes'] += len(data)
                            handle.write(data); sha.update(data)
                        handle.flush(); os.fchmod(handle.fileno(),item['mode']); os.fsync(handle.fileno())
                    require(sha.hexdigest() == item['sha256'], 'received candidate checksum differs')
                require(not stream.read(1), 'extra input stream bytes')
                verify_files(self.partial,self.request)
                self.native(lock)
                for directory in sorted([self.partial,*(p for p in self.partial.rglob('*') if p.is_dir())],
                                        key=lambda p:len(p.parts),reverse=True): sync_dir(directory)
                lock.verify(); resources(self.root)
                require(not self.target.exists(), 'candidate target appeared during transfer')
                os.rename(self.partial,self.target); sync_dir(self.root)
                result.update(status='staged',allocated_bytes=sum(p.stat().st_blocks*512 for p in self.target.rglob('*') if p.is_file()))
            except BaseException as error:
                result.update(status='interrupted' if isinstance(error,(subprocess.TimeoutExpired,KeyboardInterrupt,SystemExit)) else 'failed',
                              error_type=type(error).__name__)
                raise
            finally:
                if old is None: os.environ.pop(inherited_lock.VARIABLE,None)
                else: os.environ[inherited_lock.VARIABLE]=old
                result['finished_unix']=time.time(); durable.atomic_json(self.path,result,mode=0o600)
            return result

    def reconcile(self):
        with self.lock_factory() as lock:
            lock.verify()
            schema_fence.local_schema_fence(skip_input=self.identifier)
            result=self.status()
            require(json.loads((self.owners/'latest.json').read_text()) == {'request_sha256':self.identifier},
                    'reconcile the latest input owner first')
            require(result['status'] in ('receiving','failed','interrupted'), 'input owner does not need reconciliation')
            # After a rename/crash, never accept the target without its complete
            # receipt. Retain both possible locations on their original filesystem.
            displacements=[]
            for path in (self.partial,self.target):
                no_links(path)
                if path.exists():
                    retained=path.with_name(path.name+'.abandoned-'+self.identifier)
                    require(not retained.exists(), 'input reconciliation displacement already exists')
                    displacements.append((path,retained))
            for path,retained in displacements:
                os.rename(path,retained); sync_dir(path.parent)
            result.update(status='reconciled',reconciled_unix=time.time())
            durable.atomic_json(self.path,result,mode=0o600)
            return result


def source_files(request):
    for item in request['files']:
        source=Path(item['source']); no_links(source)
        require(source.is_file() and source.stat().st_size == item['size'] and P.checksum(source) == item['sha256'], 'coordinator input changed')


def chunks(request):
    yield durable.canonical(request)+b'\n'
    for item in request['files']:
        with Path(item['source']).open('rb') as stream:
            before=os.fstat(stream.fileno()); total=0; sha=hashlib.sha256()
            for data in iter(lambda:stream.read(CHUNK),b''):
                total+=len(data); sha.update(data); yield data
            after=os.fstat(stream.fileno())
            require(total == item['size'] and sha.hexdigest() == item['sha256'] and
                    (before.st_size,before.st_mtime_ns,before.st_ctime_ns) == (after.st_size,after.st_mtime_ns,after.st_ctime_ns),
                    'input source changed during SSH streaming')


def pump(argv, request, timeout):
    """Bounded bidirectional pipe pump: no archive or whole-file buffering."""
    process=subprocess.Popen(argv,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,**inherited_lock.options())
    output=bytearray(); deadline=time.monotonic()+timeout
    selector=selectors.DefaultSelector(); iterator=iter(chunks(request)); pending=b''
    for stream,events in ((process.stdin,selectors.EVENT_WRITE),(process.stdout,selectors.EVENT_READ),(process.stderr,selectors.EVENT_READ)):
        os.set_blocking(stream.fileno(),False); selector.register(stream,events)
    try:
        while selector.get_map():
            require(time.monotonic() < deadline, 'input SSH deadline exceeded')
            for key,_ in selector.select(1):
                stream=key.fileobj
                if stream is process.stdin:
                    if not pending:
                        pending=next(iterator,b'')
                        if not pending: selector.unregister(stream); stream.close(); continue
                    try: count=os.write(stream.fileno(),pending)
                    except BlockingIOError: continue
                    pending=pending[count:]
                else:
                    data=os.read(stream.fileno(),65536)
                    if not data: selector.unregister(stream); continue
                    if stream is process.stdout:
                        output.extend(data); require(len(output) <= 65536,'input receiver reply exceeds bound')
                    # Raw SSH/private errors are drained, never exported.
        return process.wait(timeout=max(.1,deadline-time.monotonic())),bytes(output)
    except BaseException:
        # Killing this local SSH cannot prove the remote owner stopped. Keep
        # its durable fence and force reconciliation instead of an automatic retry.
        process.kill(); process.wait()
        raise subprocess.TimeoutExpired('input SSH outcome unknown; inspect/reconcile remote owner',timeout) from None
    finally:
        selector.close()
        for stream in (process.stdin,process.stdout,process.stderr):
            if not stream.closed: stream.close()


class Client:
    def __init__(self, inventory, host, request):
        self.inventory,self.host,self.request=inventory,host,validate(request)
        require(inventory.lock.get('type') == 'pinned_host', 'input staging must run on the pinned coordinator')
        require(Path('/etc/machine-id').read_text().strip() == inventory.lock['machine_id'] and os.geteuid() == 0,
                'input staging must run as root on the pinned coordinator')
        require(inventory.hosts[host]['machine_id'] == request['machine_id'] and inventory.hosts[host].get('user','root') == 'root',
                'input inventory differs from pinned root worker')
        require(Path(__file__).resolve().parents[3] == SOURCE/request['source_sha'], 'input client requires immutable coordinator operation source')
        receipt=json.loads((SOURCE.parent/'staging'/(request['source_sha']+'.json')).read_text())
        S.verify_receipt(receipt,SOURCE/request['source_sha'],request['source_sha'],receipt['archive_sha256'])
        self.executor=SSHExecutor(inventory)

    def call(self, action):
        ssh=self.executor.transport(self.host)
        ssh=[*ssh[:-1],'-oControlMaster=no','-oControlPath=none',ssh[-1]]
        argv=[*ssh,shlex.join(['/usr/bin/python3','-B',str(SOURCE/self.request['source_sha']/'ops/scripts/wallet-pir-deploy.py'),
              'schema-input-receive','--action',action,'--request-sha256',digest(self.request)])]
        if action == 'stage': code,raw=pump(argv,self.request,7200)
        else:
            result=subprocess.run(argv,input=durable.canonical(self.request)+b'\n',capture_output=True,timeout=600 if action == 'status' else 60,**inherited_lock.options())
            code,raw=result.returncode,result.stdout
        try:
            reply=json.loads(raw,object_pairs_hook=unique)
            require(reply['request_sha256'] == digest(self.request) and reply['status'] in
                    ('staged','preflight-passed','absent','failed','interrupted','receiving','reconciled'), 'invalid input receiver reply')
        except (ValueError,KeyError,TypeError):
            raise subprocess.TimeoutExpired('input receiver reply unavailable; inspect pinned owner',7200) from None
        if code in (75,255) or reply['status'] in ('receiving','interrupted'):
            raise subprocess.TimeoutExpired('input receiver unfinished; reconciliation required',7200)
        require(code == 0, 'input staging failed; inspect private receiver owner')
        return reply

    def checked_sources(self):
        by_name={i['path']:i for i in self.request['files']}
        rendered=build(self.inventory,self.host,self.request['source_sha'],by_name['assignment.json']['source'],
                       by_name['.inputs/transparent-shard-server.service']['source'],self.request['release_result_sha256'],
                       self.request['cache_bytes'],self.request['attempt'],worker_id=self.request['worker_id'])
        require(rendered == self.request, 'input request is not the native-assigned complete publication inventory')

    def run(self, action):
        if action == 'plan':
            self.checked_sources()
            return {'request_sha256':digest(self.request),'bytes':sum(i['size'] for i in self.request['files']),
                    'files':len(self.request['files']),'worker_id':self.request['worker_id'],'target':str(ROOT/self.request['map_sha256'])}
        if action in ('stage','reconcile'):
            with ProductionLock(self.inventory.lock) as lock:
                lock.verify()
                identifier=digest(self.request)
                schema_fence.local_schema_fence(skip_input=identifier if action == 'reconcile' else None)
                no_links(OWNERS)
                record_path=OWNERS/(identifier+'.json')
                if action == 'stage':
                    require(not record_path.exists(), 'coordinator input owner already exists; inspect retained result')
                    self.checked_sources()
                    for name in ('transparent-shard-server','shard-control'):
                        item=next(i for i in self.request['files'] if i['path'] == '.inputs/'+name)
                        require(item['sha256'] == P.checksum(P.RELEASE/'artifacts'/name), 'input binary is not frozen release')
                    OWNERS.mkdir(parents=True,exist_ok=True,mode=0o700)
                    durable.atomic_json(OWNERS/(identifier+'.request.json'),self.request,mode=0o400)
                    record={'version':1,'request_sha256':identifier,'status':'running','pid':os.getpid(),'started_unix':time.time()}
                    durable.atomic_json(record_path,record,mode=0o600)
                    durable.atomic_json(OWNERS/'latest.json',{'request_sha256':identifier},mode=0o600)
                else:
                    require(record_path.exists() and json.loads((OWNERS/'latest.json').read_text()) == {'request_sha256':identifier},
                            'reconcile the latest coordinator input owner first')
                    record=json.loads(record_path.read_text())
                    require(record['request_sha256'] == identifier and record['status'] in ('running','failed','interrupted') and
                            json.loads((OWNERS/(identifier+'.request.json')).read_text(),object_pairs_hook=unique) == self.request,
                            'coordinator input owner does not need reconciliation')
                old=os.environ.get(inherited_lock.VARIABLE)
                os.environ[inherited_lock.VARIABLE]=','.join(map(str,lock.descriptors()))
                try:
                    if action == 'stage':
                        self.call('preflight')
                        reply=self.call('stage')
                    else:
                        reply=self.call('status')
                        if reply['status'] in ('failed','receiving','interrupted'): reply=self.call('reconcile')
                        require(reply['status'] in ('staged','reconciled','absent'), 'remote input owner is not reconciled')
                    lock.verify()
                    record.update(status='staged' if reply['status'] == 'staged' else 'reconciled',remote=reply)
                    return reply
                except BaseException as error:
                    record.update(status='interrupted' if isinstance(error,(subprocess.TimeoutExpired,KeyboardInterrupt,SystemExit)) else 'failed',
                                  error_type=type(error).__name__)
                    raise
                finally:
                    record['finished_unix']=time.time(); durable.atomic_json(record_path,record,mode=0o600)
                    if old is None: os.environ.pop(inherited_lock.VARIABLE,None)
                    else: os.environ[inherited_lock.VARIABLE]=old
        if action == 'preflight': self.checked_sources()
        return self.call(action)


def build(inventory, host, source_sha, assignment, unit, release_sha256, cache_bytes, attempt, *, worker_id):
    """Render exact native-assigned files after the owned full publication passes."""
    result=json.loads((P.EVIDENCE/'result.json').read_text())
    require(result['status'] == 'passed' and result['map_sha256'] == P.checksum(P.OUTPUT/'shards.json'), 'full publication is not verified complete')
    release=P.verify_release(release_sha256)
    assignment=Path(assignment); unit=Path(unit)
    require(unit.stat().st_size <= 65536, 'worker unit exceeds bound')
    value=json.loads(assignment.read_text(),object_pairs_hook=unique)
    require(value['set']['map_sha256'] == result['map_sha256'] and value['set']['shard_schema'] == 'transparent-shard-v11' and
            value['generated_by']['source_sha'] == P.RELEASE_SHA and not value['unassigned'], 'assignment is not the complete frozen v11 publication')
    require(isinstance(worker_id,str) and any(w['id'] == worker_id for w in value['workers']), 'worker is absent from native assignment')
    raw=subprocess.run([str(P.RELEASE/'artifacts/shard-assign'),'files','--shard-dir',str(P.OUTPUT),
        '--assignment',str(assignment),'--worker-id',worker_id],capture_output=True,check=True,timeout=60,**inherited_lock.options()).stdout
    names=raw.decode().splitlines()
    require(names and len(set(names)) == len(names), 'native file inventory is empty or duplicate')
    files=[]
    sources={}
    for name in names:
        # Native files_for emits a digest directory with a trailing slash for
        # every assigned shard, and manifest/filter files for other shards.
        if name.endswith('/'):
            directory=name[:-1]
            require(HEX.fullmatch(directory), 'native directory is not a manifest digest')
            root=P.OUTPUT/directory; no_links(root)
            require(root.is_dir(), 'native assigned directory is absent')
            paths=list(root.rglob('*'))
            require(len(paths) <= MAX_FILES and not any(p.is_symlink() for p in paths), 'native assigned directory is unsafe')
            for path in paths:
                if path.is_file(): sources[str(path.relative_to(P.OUTPUT))]=path
        else:
            safe(name); sources[name]=P.OUTPUT/name
        require(len(sources) <= MAX_FILES, 'native file inventory exceeds bound')
    require('assignment.json' not in sources and not any(name.startswith('.inputs/') for name in sources),'native inventory collides with staging inputs')
    sources.update({'assignment.json':assignment,'.inputs/transparent-shard-server':P.RELEASE/'artifacts/transparent-shard-server',
        '.inputs/shard-control':P.RELEASE/'artifacts/shard-control','.inputs/transparent-shard-server.service':unit})
    for name,path in sorted(sources.items()):
        safe(name); no_links(path)
        files.append({'source':str(path),'path':name,'size':path.stat().st_size,'sha256':P.checksum(path),'mode':INPUTS.get(name,0o600)})
    # The full retained release was verified above, including the native file
    # lister. Receiver native verification independently checks assignment scope.
    require(release['binaries']['transparent-shard-server']['sha256'] == next(f['sha256'] for f in files if f['path'] == '.inputs/transparent-shard-server'),
            'native worker changed during inventory')
    return validate({'version':1,'source_sha':source_sha,'machine_id':inventory.hosts[host]['machine_id'],'worker_id':worker_id,
        'map_sha256':result['map_sha256'],'assignment_sha256':P.checksum(assignment),'release_result_sha256':release_sha256,
        'attempt':attempt,'cache_bytes':cache_bytes,'files':files})
