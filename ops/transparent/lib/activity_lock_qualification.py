"""Bounded SSH descendant qualification through the production wrapper.

Only private input-owner receipts and the existing production lock are touched.
The relay exits while SSH retains the coordinator FD; the remote parent exits
while its bounded child retains the independently acquired host FD. Both owners
remain fenced until their descendants release the locks and reconcile succeeds.
"""
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time

from wallet_pir_ops import durable, inherited_lock, schema_fence
from wallet_pir_ops.deploy.remote import ProductionLock, SSHExecutor

SOURCE = Path('/srv/transparent-activity/ops/sources')
OWNERS = schema_fence.INPUT_STAGING
HOLD_SECONDS = 15
spec = importlib.util.spec_from_file_location('lock_source_receipt', Path(__file__).with_name('activity_source_stage_host.py'))
S = importlib.util.module_from_spec(spec)
spec.loader.exec_module(S)


def require(ok, message):
    if not ok:
        raise ValueError(message)


def validate(request):
    require(isinstance(request, dict) and set(request) == {'version','source_sha','host','machine_id','coordinator_machine_id','attempt'},
            'invalid lock qualification request')
    require(type(request['version']) is int and request['version'] == 1 and
            isinstance(request['source_sha'], str) and re.fullmatch('[0-9a-f]{40}', request['source_sha']) and
            isinstance(request['host'], str) and re.fullmatch('[a-zA-Z0-9-]{1,64}', request['host']), 'invalid qualification source/host')
    for key in ('machine_id','coordinator_machine_id'):
        require(isinstance(request[key], str) and re.fullmatch('[0-9a-f]{32}', request[key]), 'invalid qualification machine')
    require(request['machine_id'] != request['coordinator_machine_id'] and type(request['attempt']) is int and
            1 <= request['attempt'] <= 100, 'qualification requires a distinct remote host and bounded attempt')
    return request


def identity(request, remote=False):
    validate(request)
    key = 'machine_id' if remote else 'coordinator_machine_id'
    require(os.geteuid() == 0 and Path('/etc/machine-id').read_text().strip() == request[key], 'qualification requires the pinned root host')
    root = SOURCE/request['source_sha']
    require(Path(__file__).resolve().parents[3] == root, 'qualification requires immutable reviewed source')
    receipt = json.loads((SOURCE.parent/'staging'/(request['source_sha']+'.json')).read_bytes())
    S.verify_receipt(receipt, root, request['source_sha'], receipt['archive_sha256'])


def load(path):
    require(not path.is_symlink() and path.stat().st_size <= 1 << 20, 'invalid qualification owner/log bound')
    return json.loads(path.read_bytes())


class Owner:
    def __init__(self, request, *, remote=False):
        self.request = validate(request)
        self.sha = durable.digest(request)
        self.remote = remote
        self.path = OWNERS/(self.sha+'.json')
        self.directory = OWNERS/(self.sha+'.qualification')

    def lock(self):
        key = 'machine_id' if self.remote else 'coordinator_machine_id'
        return ProductionLock({'type':'pinned_host','machine_id':self.request[key]})

    def status(self):
        require(not self.path.is_symlink(), 'qualification owner cannot be a symlink')
        if not self.path.exists():
            return {'status':'absent','request_sha256':self.sha}
        result = load(self.path)
        require(result.get('request') == self.request and result.get('request_sha256') == self.sha and
                result.get('kind') == 'lock-qualification', 'qualification owner identity differs')
        return result

    def save(self, record):
        durable.atomic_json(self.path, record, mode=0o600)

    def begin(self):
        schema_fence.local_schema_fence()
        require(self.status()['status'] == 'absent', 'qualification already owned; inspect/reconcile, never replay')
        OWNERS.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.directory.mkdir(mode=0o700)
        record = {'kind':'lock-qualification','request':self.request,'request_sha256':self.sha,
                  'status':'interrupted','pid':os.getpid(),'started_unix':time.time()}
        self.save(record)
        durable.atomic_json(OWNERS/'latest.json', {'request_sha256':self.sha}, mode=0o600)
        return record

    def reconcile(self):
        with self.lock() as lock:
            lock.verify()
            schema_fence.local_schema_fence(skip_input=self.sha)
            require(load(OWNERS/'latest.json') == {'request_sha256':self.sha}, 'reconcile latest qualification first')
            record = self.status()
            require(record['status'] == 'interrupted', 'qualification does not require reconciliation')
            record.update(status='reconciled', reconciled_unix=time.time())
            self.save(record)
            return record

    def remote_parent(self):
        with self.lock() as lock:
            record = self.begin()
            env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
            env[inherited_lock.VARIABLE] = ','.join(map(str, lock.descriptors()))
            # Inherit stdout: real SSH cannot finish until this child's pipe
            # closes, even though the remote wrapper parent exits immediately.
            child = subprocess.Popen(command(self.request, 'child'), stdin=subprocess.PIPE,
                                     env=env, pass_fds=lock.descriptors())
            child.stdin.write(durable.canonical(self.request)); child.stdin.close()
            record.update(child_pid=child.pid)
            # Child waits for parent exit before updating this same receipt.
            self.save(record)
            return {'parent_pid':os.getpid(), 'child_pid':child.pid, 'request_sha256':self.sha}

    def child(self):
        inherited_lock.descriptors(required=True, path=ProductionLock.PATH)
        record = self.status()
        deadline = time.monotonic()+5
        while record.get('child_pid') != os.getpid() and time.monotonic() < deadline:
            require(record['status'] == 'interrupted', 'child owner changed before readiness')
            time.sleep(.025)
            record = self.status()
        require(record['status'] == 'interrupted' and record.get('child_pid') == os.getpid(), 'child requires its recorded owner')
        parent = record['pid']
        deadline = time.monotonic()+5
        while os.getppid() == parent and time.monotonic() < deadline:
            time.sleep(.05)
        require(os.getppid() != parent, 'remote parent did not exit within bound')
        record.update(child_pid=os.getpid(), parent_exited=True, child_ready_unix=time.time())
        self.save(record)
        time.sleep(HOLD_SECONDS)
        record.update(child_finished_unix=time.time())
        self.save(record)
        return {'child_pid':os.getpid(), 'parent_exited':True, 'request_sha256':self.sha}

    def probe(self):
        record = self.status()
        require(record['status'] == 'interrupted' and record.get('parent_exited') is True, 'surviving remote child is not ready')
        blocked = False
        try:
            with self.lock():
                pass
        except BlockingIOError:
            blocked = True
        require(blocked, 'remote child failed to retain the host lock')
        fenced = False
        try:
            schema_fence.local_schema_fence()
        except ValueError:
            fenced = True
        require(fenced, 'interrupted qualification failed to fence other operations')
        return {'host_lock_blocked':True,'unfinished_owner_fenced':True,'parent_exited':True}


def command(request, action):
    return ['/usr/bin/python3','-B',str(SOURCE/request['source_sha']/'ops/scripts/wallet-pir-deploy.py'),
            'schema-lock-remote','--action',action,'--request-sha256',durable.digest(request)]


class Client:
    def __init__(self, inventory, source, host, attempt, inventory_path):
        require(inventory.lock['type'] == 'pinned_host' and host in inventory.hosts and
                inventory.hosts[host].get('user',inventory.ssh.get('user','root')) == 'root' and
                not inventory.hosts[host].get('sudo'), 'qualification requires pinned root coordinator and remote root')
        self.inventory, self.inventory_path = inventory, inventory_path
        require(Path(inventory_path).is_file() and not Path(inventory_path).is_symlink() and
                str(Path(inventory_path)).startswith('/srv/transparent-activity/full-v11/inputs/'),
                'qualification relay requires retained immutable coordinator inventory')
        self.request = validate({'version':1,'source_sha':source,'host':host,'attempt':attempt,
            'machine_id':inventory.hosts[host]['machine_id'],'coordinator_machine_id':inventory.lock['machine_id']})
        self.owner = Owner(self.request)
        self.executor = SSHExecutor(inventory)

    def remote(self, action, **options):
        argv = self.executor.transport(self.request['host'])+[shlex.join(command(self.request,action))]
        return subprocess.run(argv, input=durable.canonical(self.request), capture_output=True, timeout=30,
                              **inherited_lock.options(), **options)

    def checked_remote(self, action):
        result = self.remote(action)
        require(result.returncode == 0, 'remote qualification '+action+' failed; inspect retained owner')
        return json.loads(result.stdout)

    def relay(self):
        inherited_lock.descriptors(required=True, path=ProductionLock.PATH)
        record = self.owner.status()
        require(record['status'] == 'interrupted', 'relay requires current interrupted probe owner')
        log = self.owner.directory/'ssh.log'
        with log.open('xb') as output:
            os.chmod(log,0o600)
            argv = self.executor.transport(self.request['host'])+[shlex.join(command(self.request,'parent'))]
            child = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=output, stderr=output, **inherited_lock.options())
            child.stdin.write(durable.canonical(self.request)); child.stdin.close()
        record.update(ssh_pid=child.pid, relay_pid=os.getpid())
        self.owner.save(record)
        return {'ssh_pid':child.pid,'relay_pid':os.getpid()}

    def run(self, action, expected=None):
        identity(self.request)
        plan = {'request':self.request,'request_sha256':self.owner.sha,'hold_seconds':HOLD_SECONDS,
                'effects':'private qualification receipts and existing host locks only'}
        if action == 'plan':
            return plan
        if action == 'status':
            return self.owner.status()
        if action == 'reconcile':
            remote = self.checked_remote('reconcile')
            return {'remote':remote,'coordinator':self.owner.reconcile()}
        if action == 'relay':
            return self.relay()
        require(action in ('preflight','qualify'), 'unsupported lock qualification action')
        require(self.owner.status()['status'] == 'absent', 'qualification already owned')
        with self.owner.lock() as lock:
            schema_fence.local_schema_fence()
            self.checked_remote('preflight')
            if action == 'preflight':
                return plan
            require(expected == durable.digest(plan), 'qualification plan checksum differs')
            self.owner.begin()
            argv = ['/usr/bin/python3','-B',str(SOURCE/self.request['source_sha']/'ops/scripts/wallet-pir-deploy.py'),
                    '--inventory',self.inventory_path,'schema-lock-relay','--source-sha',self.request['source_sha'],
                    '--host',self.request['host'],'--attempt',str(self.request['attempt'])]
            env = dict(os.environ,PYTHONDONTWRITEBYTECODE='1')
            env[inherited_lock.VARIABLE] = ','.join(map(str,lock.descriptors()))
            relay = subprocess.run(argv,capture_output=True,timeout=10,env=env,pass_fds=lock.descriptors())
            require(relay.returncode == 0, 'qualification relay failed; reconcile owned descendants')
            receipt = json.loads(relay.stdout)
        # Original coordinator and relay have exited/closed their descriptors.
        # Only actual SSH may now prevent a new coordinator writer.
        blocked = False
        try:
            with self.owner.lock():
                pass
        except BlockingIOError:
            blocked = True
        require(blocked, 'surviving SSH failed to retain coordinator lock')
        deadline = time.monotonic()+8
        while True:
            ready = self.checked_remote('status')
            if ready.get('parent_exited'):
                break
            require(time.monotonic() < deadline, 'remote child readiness timed out; reconcile')
            time.sleep(.2)
        proof = self.checked_remote('probe')
        deadline = time.monotonic()+30
        while True:
            try:
                with self.owner.lock():
                    break
            except BlockingIOError:
                require(time.monotonic() < deadline, 'surviving SSH exceeded qualification bound; explicit reconcile required')
                time.sleep(.2)
        remote = self.checked_remote('status')
        require(remote.get('child_finished_unix') and remote.get('parent_exited'), 'remote child completion absent')
        self.checked_remote('reconcile')
        record = self.owner.reconcile()
        record.update(status='staged', result={'coordinator_lock_blocked_after_relay_exit':True,
                      'relay':receipt,'remote':proof,'remote_owner':remote,'finished_unix':time.time()})
        # Take lock again: a failed qualification keeps its interruption fence;
        # a complete one is publishable only after both reconciliations.
        with self.owner.lock():
            schema_fence.local_schema_fence(skip_input=self.owner.sha)
            self.owner.save(record)
        return record


def remote_run(request, action):
    identity(request, remote=True)
    owner = Owner(request,remote=True)
    if action == 'preflight':
        schema_fence.local_schema_fence()
        require(owner.status()['status'] == 'absent', 'remote qualification already owned')
        return {'status':'ready'}
    if action == 'status':
        return owner.status()
    return getattr(owner, {'parent':'remote_parent','child':'child','probe':'probe','reconcile':'reconcile'}[action])()
