"""Prepare the approved genesis-to-anchor v11 publication, without a cutover.

Only the deployment wrapper may launch this fixed coordinator job. The detached
systemd owner reacquires the production lock and rechecks all inputs before
writing. Its native children inherit that lock; failed/partial output is retained
and blocks a second launch. Publication success is not deployment qualification.
"""
import base64
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import tomllib
import urllib.request

from wallet_pir_ops import durable, schema_fence
from wallet_pir_ops.deploy.remote import ProductionLock

ROOT = Path('/srv/transparent-activity')
RELEASE_SHA = '12ce12918446eaa56e2d766ec2f43d82c531abb9'
RELEASE = ROOT/'build/evidence'/('release-'+RELEASE_SHA)
JOURNAL = ROOT/'full-v3/journal'
INGEST_UNIT = 'transparent-activity-full-ingest-release-a1c4b809'
GUARD_RESULT = ROOT/'full-v3/evidence/health-release-a1c4b809/result.json'
THROUGH = 3500738
OUTPUT = ROOT/'full-v11/publications/initial'
EVIDENCE = ROOT/'full-v11/preparation'
UNIT = 'transparent-activity-full-publication-v11'
CONTROLLER = Path('/opt/transparent-publisher/controller.json')
GEOMETRIES = {'recent_geometry':'recent-4k-8k', 'archive_geometry':'archive-wide',
              'directory_choice':'all', 'range_profile':'zcash-transparent-range-v2'}
PROPERTIES = ('CPUQuota=400%', 'MemoryHigh=14G', 'MemoryMax=16G',
              'MemorySwapMax=0', 'Nice=10', 'IOWeight=20', 'Restart=no',
              'KillMode=control-group', 'TimeoutStopSec=20', 'RemainAfterExit=yes')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def checksum(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def state(unit):
    raw = subprocess.check_output(['systemctl','show',unit,
        '--property=ActiveState,MainPID,NRestarts,Result,ExecMainStatus'], text=True)
    return dict(line.split('=', 1) for line in raw.splitlines())


def resources():
    memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    available = int(memory['MemAvailable'].split()[0])/int(memory['MemTotal'].split()[0])
    disks = {str(p):os.statvfs(p).f_bavail/os.statvfs(p).f_blocks for p in (ROOT, Path('/srv/zakura'), Path('/'))}
    return {'unix':time.time(), 'memory_available':available, 'disk_available':disks}


def healthy(value):
    require(value['memory_available'] >= .2 and all(n >= .2 for n in value['disk_available'].values()),
            'publication preparation stopped: memory or disk headroom below 20 percent')


def interrupted(_signal, _frame):
    raise InterruptedError('publication owner interrupted')


def verify_release(expected):
    require(re.fullmatch('[0-9a-f]{64}', expected or '') is not None, 'invalid release receipt checksum')
    require(checksum(RELEASE/'result.json') == expected, 'release receipt identity changed')
    result = json.loads((RELEASE/'result.json').read_text())
    require(result['status'] == 'passed' and result['source_sha'] == RELEASE_SHA
            and result['profile'] == 'release', 'publication requires the frozen successful fat-LTO release')
    source = ROOT/'build/sources'/RELEASE_SHA
    require(result['source'] == str(source) and result['cargo_lock_sha256'] == checksum(source/'Cargo.lock'),
            'release source/dependency identity changed')
    profile = tomllib.loads((source/'Cargo.toml').read_text())['profile']['release']
    require(profile.get('lto') == 'fat' and profile.get('codegen-units') == 1, 'release profile changed')
    require(result['compiler'].startswith('rustc 1.97.1 ') and len(result['binaries']) == 18,
            'unexpected compiler or incomplete retained release')
    for name, entry in result['binaries'].items():
        path = RELEASE/'artifacts'/name
        require(not path.is_symlink() and path.is_file() and entry['retained_path'] == str(path)
                and checksum(path) == entry['sha256'], 'retained executable identity changed')
    return result


def verify_journal():
    require(not JOURNAL.is_symlink(), 'journal must retain its fixed namespace')
    metadata = json.loads((JOURNAL/'meta.json').read_text())
    checkpoint = (JOURNAL/'checkpoint.bin').read_bytes()
    require(metadata['version'] == 3 and metadata['start_height'] == 0 and len(checkpoint) == 16
            and int.from_bytes(checkpoint[8:], 'little') == (THROUGH+1)*48,
            'genesis-to-anchor v3 ingestion is incomplete')
    terminal = state(INGEST_UNIT)
    require(terminal['ActiveState'] in ('inactive','active') and terminal['MainPID'] == '0'
            and terminal['Result'] == 'success' and terminal['ExecMainStatus'] == '0'
            and terminal['NRestarts'] == '0', 'ingestion is still owned or did not complete successfully')
    guard = json.loads(GUARD_RESULT.read_text())
    require(guard['status'] == 'passed' and guard['guarded_unit'] == INGEST_UNIT
            and guard['through'] == THROUGH and guard['source_sha'] == 'a1c4b809f62035d5e8509b3b43807fb7e3dab7e1'
            and guard['binary_sha256'] == 'd4a8cc161cc9c6b938963a81d12dd9b13df5cfad595d6fe4a5309d3a2032a972',
            'independent ingestion health guard has not passed against the frozen ingester')
    return {'meta_sha256':checksum(JOURNAL/'meta.json'), 'checkpoint_sha256':checksum(JOURNAL/'checkpoint.bin'),
            'guard_sha256':checksum(GUARD_RESULT)}


def verify_anchor(cutoff):
    # Block heights are public publisher inputs; no wallet txid/prevout request.
    cookie = Path('/root/.cache/zakura/.cookie').read_text().strip()
    for height, expected in ((0,cutoff['genesis_hash']),(THROUGH,cutoff['anchor']['hash'])):
        body = json.dumps({'jsonrpc':'2.0','id':height,'method':'getblockhash','params':[height]}).encode()
        request = urllib.request.Request('http://127.0.0.1:8232',body,{'Content-Type':'application/json',
            'Authorization':'Basic '+base64.b64encode(cookie.encode()).decode()})
        with urllib.request.urlopen(request,timeout=10) as response:
            raw = response.read(65537)
        require(len(raw) <= 65536, 'anchor RPC response exceeds bound')
        result = json.loads(raw)
        require(result.get('id') == height and result.get('error') is None and result.get('result') == expected,
                'publication anchor no longer accepted independently by the node')


def allocation():
    files = [p for p in OUTPUT.rglob('*') if p.is_file()]
    require(not any(p.is_symlink() for p in OUTPUT.rglob('*')), 'initial publication contains unexpected links')
    manifests = [json.loads(p.read_text()) for p in OUTPUT.rglob('manifest.json')]
    mapping = json.loads((OUTPUT/'shards.json').read_text())
    require(len(manifests) == len(mapping['shards']) and len(manifests) > 0, 'initial manifest inventory differs')
    require(all(m['schema'] == 'transparent-shard-v11' and not m.get('txid_display') for m in manifests)
            and all(not s.get('txid_segments') for s in mapping['shards']), 'publication schema or capability differs')
    tables = [table for m in manifests for name in ('directory_segments','page_segments') for table in m[name]]
    require(all(t['row_bytes'] == 4096 for t in tables), 'publication row width changed')
    return {'files':len(files), 'logical_bytes':sum(p.stat().st_size for p in files),
            'allocated_bytes':sum(p.stat().st_blocks*512 for p in files), 'shards':len(manifests),
            'directory_segments':sum(len(m['directory_segments']) for m in manifests),
            'page_segments':sum(len(m['page_segments']) for m in manifests),
            'directory_rows':sum(t['rows'] for m in manifests for t in m['directory_segments']),
            'page_rows':sum(t['rows'] for m in manifests for t in m['page_segments']),
            'occupancy':[{k:m[k] for k in ('shard_id','geometry','occupancy')} for m in manifests]}


class PublicationJob:
    def __init__(self, inventory, source_sha, release_sha256, out=print):
        self.inventory, self.source_sha, self.release_sha256, self.out = inventory, source_sha, release_sha256, out
        require(re.fullmatch('[0-9a-f]{40}', source_sha or '') is not None, 'invalid operation source SHA')
        self.source = ROOT/'ops/sources'/source_sha

    def plan(self):
        plan = {'version':1, 'source_sha':self.source_sha, 'release_source_sha':RELEASE_SHA,
                'release_result_sha256':self.release_sha256, 'journal':str(JOURNAL), 'through':THROUGH,
                'output':str(OUTPUT), 'geometries':GEOMETRIES, 'properties':PROPERTIES, 'unit':UNIT}
        self.out(json.dumps({'plan_sha256':identity(plan), **plan}, sort_keys=True))
        return plan

    def preflight(self, *, running=False):
        require(self.inventory.lock.get('type') == 'pinned_host' and os.geteuid() == 0
                and ProductionLock.MACHINE_ID.read_text().strip() == self.inventory.lock['machine_id'],
                'publication preparation must run on the pinned root coordinator')
        require(Path(__file__).resolve() == self.source/'transparent/ops/lib/activity_publication_job.py',
                'publication preparation requires the immutable staged operation source')
        receipt = json.loads((ROOT/'ops/staging'/(self.source_sha+'.json')).read_text())
        spec = importlib.util.spec_from_file_location('publication_source_check',
            self.source/'transparent/ops/lib/activity_source_stage_host.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.verify_receipt(receipt, self.source, self.source_sha, receipt['archive_sha256'])
        verify_release(self.release_sha256)
        journal = verify_journal()
        require(not OUTPUT.exists(), 'publication output already exists; retain it and explicitly reconcile')
        if not running:
            require(not (EVIDENCE/'owner.json').exists() and not EVIDENCE.is_symlink(),
                    'publication owner already exists; do not duplicate or overwrite it')
        controller = json.loads(CONTROLLER.read_text())
        require(all(controller.get(k) == v for k,v in GEOMETRIES.items()), 'live approved geometry/profile changed')
        healthy(resources())
        require(JOURNAL.stat().st_dev == ROOT.stat().st_dev, 'journal moved off the publication filesystem')
        for path in (ROOT/'full-v11', OUTPUT.parent, OUTPUT, EVIDENCE):
            require(not path.is_symlink(), 'candidate publication namespace contains a symlink')
            if path.exists():
                require(path.stat().st_dev == JOURNAL.stat().st_dev, 'publication moved off the journal filesystem')
        # Never hard-link mutable journal inputs or bypass the writer lock.
        with (JOURNAL/'writer.lock').open('rb') as lock:
            fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
            require(journal['checkpoint_sha256'] == checksum(JOURNAL/'checkpoint.bin'), 'journal changed during preflight')
        return journal

    def start(self, expected_plan):
        plan = self.plan()
        require(identity(plan) == expected_plan, 'publication plan identity differs')
        with ProductionLock(self.inventory.lock) as lock:
            schema_fence.local_schema_fence()
            journal = self.preflight()
            EVIDENCE.mkdir(parents=True, mode=0o700, exist_ok=True)
            require(EVIDENCE.stat().st_uid == 0 and EVIDENCE.stat().st_mode & 0o077 == 0,
                    'publication evidence must be private root-owned state')
            owner = {'version':1, 'status':'launching', 'plan':plan, 'plan_sha256':identity(plan),
                     'machine_id':self.inventory.lock['machine_id'], 'journal':journal,
                     'started_unix':time.time(), 'launcher_pid':os.getpid(), 'unit':UNIT}
            durable.atomic_json(EVIDENCE/'owner.json', owner, mode=0o600)
            durable.atomic_json(EVIDENCE/'inventory.json', {'hosts':{'coordinator':{}}, 'ssh':{'mode':'config'},
                'lock':self.inventory.lock, 'services':{}}, mode=0o600)
            command = ['/usr/bin/systemd-run','--quiet','--unit='+UNIT]
            command += ['--property='+p for p in PROPERTIES]
            command += ['--setenv=PYTHONDONTWRITEBYTECODE=1','/usr/bin/python3','-B',
                str(self.source/'ops/scripts/wallet-pir-deploy.py'), '--inventory',str(EVIDENCE/'inventory.json'),
                'schema-publication-run','--source-sha',self.source_sha,'--release-result-sha256',self.release_sha256]
            # This owner starts only the fixed product job, never a service switch.
            # systemd does not inherit FDs; the child obtains its own lock before effects.
            lock.verify()
            subprocess.run(command, check=True, capture_output=True, timeout=30,
                           pass_fds=lock.descriptors(), env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'))
        self.status()

    def status(self):
        if not (EVIDENCE/'owner.json').exists():
            self.out(json.dumps({'status':'not-started', 'unit':UNIT}))
            return
        owner = json.loads((EVIDENCE/'owner.json').read_text())
        result = json.loads((EVIDENCE/'result.json').read_text()) if (EVIDENCE/'result.json').exists() else None
        self.out(json.dumps({'unit':UNIT, 'plan_sha256':owner['plan_sha256'], 'owner_status':owner['status'],
            'state':state(UNIT), 'result':result}, sort_keys=True))

    def run(self):
        os.umask(0o077)
        owner = json.loads((EVIDENCE/'owner.json').read_text())
        plan = self.plan()
        require(owner['status'] == 'launching' and owner['plan'] == json.loads(json.dumps(plan))
                and owner['plan_sha256'] == identity(plan) and owner['machine_id'] == self.inventory.lock['machine_id'],
                'publication launch identity changed or job already ran')
        # Bounded handoff after systemd-run; do not run while any other owner
        # holds the production lock. Preflight is repeated after acquiring it.
        deadline = time.monotonic()+15
        lock = ProductionLock(self.inventory.lock)
        while True:
            try:
                lock.__enter__()
                break
            except BlockingIOError:
                require(time.monotonic() < deadline, 'publication lock handoff timed out; reconcile launch intent')
                time.sleep(.1)
        previous_handler = signal.signal(signal.SIGTERM, interrupted)
        try:
            journal = self.preflight(running=True)
            require(journal == owner['journal'], 'journal changed after launch preflight')
            owner.update(status='running', pid=os.getpid())
            durable.atomic_json(EVIDENCE/'owner.json', owner, mode=0o600)
            with (JOURNAL/'writer.lock').open('rb') as journal_lock:
                fcntl.flock(journal_lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
                self.journal_fd = journal_lock.fileno()
                self.build(lock)
            result = {'status':'passed', 'plan_sha256':identity(plan), 'ended_unix':time.time(),
                      'map_sha256':checksum(OUTPUT/'shards.json'), 'pid':os.getpid(),
                      'limitation':'publication preparation only; certificates, independent oracle, cutover and qualification remain gates'}
            durable.atomic_json(EVIDENCE/'result.json', result, mode=0o600)
        except BaseException as error:
            durable.atomic_json(EVIDENCE/'result.json', {'status':'failed', 'error_type':type(error).__name__,
                'plan_sha256':identity(plan), 'ended_unix':time.time(), 'pid':os.getpid()}, mode=0o600)
            raise
        finally:
            signal.signal(signal.SIGTERM, previous_handler)
            lock.__exit__(None, None, None)

    def native(self, name, arguments, lock):
        lock.verify()
        verify_release(self.release_sha256)
        with (EVIDENCE/(name+'.log')).open('xb') as log, (EVIDENCE/'health.jsonl').open('a') as health:
            started = time.time()
            failure = None
            proc = subprocess.Popen([str(RELEASE/'artifacts'/name), *map(str,arguments)], stdout=log,
                stderr=subprocess.STDOUT, pass_fds=(*lock.descriptors(), self.journal_fd), start_new_session=True,
                env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1',
                         WALLET_PIR_PRODUCTION_LOCK_FDS=','.join(map(str,lock.descriptors()))))
            try:
                durable.atomic_json(EVIDENCE/(name+'.owner.json'), {'pid':proc.pid, 'binary_sha256':checksum(RELEASE/'artifacts'/name),
                    'unit':UNIT, 'source_sha':RELEASE_SHA, 'started_unix':time.time()}, mode=0o600)
                while True:
                    sample = resources()
                    health.write(json.dumps(dict(stage=name, **sample))+'\n')
                    health.flush()
                    lock.verify()
                    healthy(sample)
                    try:
                        code = proc.wait(timeout=5)
                        break
                    except subprocess.TimeoutExpired:
                        continue
                require(code == 0, 'native publication stage failed: '+name)
            except BaseException as error:
                failure = type(error).__name__
                if proc.poll() is None:
                    # Stop only this job's own process group; preserve canonical owners.
                    os.killpg(proc.pid, signal.SIGTERM)
                    try:
                        proc.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid, signal.SIGKILL)
                        proc.wait(timeout=5)
                raise
            finally:
                durable.atomic_json(EVIDENCE/(name+'.result.json'), {'pid':proc.pid, 'exit_code':proc.poll(),
                    'status':'failed' if failure else 'passed', 'error_type':failure,
                    'started_unix':started,'ended_unix':time.time()}, mode=0o600)

    def build(self, lock):
        self.native('shard-cutoff', ['--data-dir',JOURNAL,'--anchor-height',THROUGH,'--months','6',
            '--zakura-cookie','/root/.cache/zakura/.cookie','--out',EVIDENCE/'cutoff.json','--source-sha',RELEASE_SHA], lock)
        cutoff = json.loads((EVIDENCE/'cutoff.json').read_text())
        recent = cutoff['cutoff']['height']
        require(cutoff['anchor']['height'] == THROUGH and cutoff['journal']['start_height'] == 0
                and 0 < recent <= THROUGH, 'cutoff does not bind the full journal anchor')
        self.native('shard-publish', ['--data-dir',JOURNAL,'--output',OUTPUT,'--through',THROUGH,
            '--recent-from',recent,'--recent-geometry',GEOMETRIES['recent_geometry'],
            '--archive-geometry',GEOMETRIES['archive_geometry'],'--directory-choice',GEOMETRIES['directory_choice'],
            '--range-profile',GEOMETRIES['range_profile'],'--zakura-cookie','/root/.cache/zakura/.cookie',
            '--record',EVIDENCE/'publication.json','--source-sha',RELEASE_SHA], lock)
        self.native('shard-verify', ['--shard-dir',OUTPUT,'--publication',EVIDENCE/'publication.json',
            '--expect-start','0','--expect-through',THROUGH,'--expect-anchor-hash',cutoff['anchor']['hash'],
            '--expect-recent-from',recent,'--expect-recent-geometry',GEOMETRIES['recent_geometry'],
            '--expect-archive-geometry',GEOMETRIES['archive_geometry'],'--data-dir',JOURNAL,'--rebuild','4',
            '--out',EVIDENCE/'verified.json','--source-sha',RELEASE_SHA], lock)
        verify_anchor(cutoff)
        durable.atomic_json(EVIDENCE/'allocation.json', allocation(), mode=0o600)
