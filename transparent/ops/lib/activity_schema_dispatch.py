"""Pinned remote root owners for concrete schema host transitions.

SSH inherits the coordinator FD locally. The remote wrapper acquires its own
host lock and journals intent before service effects. Lost replies interrupt the
coordinator transaction; they never imply that a remote action failed safely.
Status/reconciliation use the same request identity and retained private result.
"""
import hashlib
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

SPEC = importlib.util.spec_from_file_location('dispatch_host', Path(__file__).with_name('activity_schema_host.py'))
H = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(H)
SPEC = importlib.util.spec_from_file_location('dispatch_source_receipt', Path(__file__).with_name('activity_source_stage_host.py'))
S = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(S)
ROOT = Path('/srv/transparent-activity/ops/host-actions')
ACTIONS = ('preflight', 'protect-publications', 'capture', 'stage', 'activate', 'restore', 'restore-routing', 'verify-worker', 'verify-rollback-worker', 'repair-restore','repair-capture','attest-captured-router-guard')
READ_ONLY = ('preflight', 'verify-worker', 'verify-rollback-worker','attest-captured-router-guard')
MAX_REQUEST = 256*1024


def digest(value):
    return hashlib.sha256(H.encode(value)).hexdigest()


def validate(request):
    fields = {'version', 'request_id', 'action', 'plan', 'plan_sha256'}
    H.require(isinstance(request, dict) and set(request) in (fields, fields|{'recovery_source_sha'},fields|{'recovery_source_sha','guard_sha256'}), 'invalid host request')
    H.require(('guard_sha256' in request)==(request.get('action')=='attest-captured-router-guard'), 'guard digest is capture-attestation only')
    if 'guard_sha256' in request:
        H.require(request['plan'].get('role')=='router' and isinstance(request['guard_sha256'],str) and H.HEX.fullmatch(request['guard_sha256']), 'invalid captured router guard binding')
    if 'recovery_source_sha' in request:
        H.require(request.get('action') in ('repair-restore','repair-capture','verify-rollback-worker','restore-routing','attest-captured-router-guard') and
                  isinstance(request['recovery_source_sha'],str) and re.fullmatch('[0-9a-f]{40}',request['recovery_source_sha']),
                  'repair source cannot authorize a forward host phase')
    H.require(request.get('action') not in ('repair-restore','repair-capture') or 'recovery_source_sha' in request, 'repair restore/capture needs a bound repair program')
    H.require(type(request['version']) is int and request['version'] == 1 and request['action'] in ACTIONS, 'unsupported host action')
    H.require(isinstance(request['request_id'], str) and re.fullmatch('[a-z0-9-]{1,64}', request['request_id']), 'invalid host request identifier')
    H.validate(request['plan'])
    H.require(request['plan_sha256'] == digest(request['plan']), 'host plan digest differs')
    H.require(len(H.encode(request)) <= MAX_REQUEST, 'host request exceeds bound')
    return request


def read_request(stream, expected):
    data = stream.read(MAX_REQUEST+1)
    H.require(len(data) <= MAX_REQUEST, 'host request exceeds bound')
    request = validate(json.loads(data, object_pairs_hook=H.unique))
    H.require(digest(request) == expected, 'host request differs from reviewed identity')
    return request


class Actor:
    def __init__(self, request, *, root=ROOT, host_factory=H.Host, lock_factory=None):
        self.request = validate(request)
        self.plan = request['plan']
        self.root = Path(root)
        self.host = host_factory(self.plan)
        if 'recovery_source_sha' in request:
            self.host.repair_retained = True
        self.lock_factory = lock_factory or (lambda: ProductionLock({'type':'pinned_host', 'machine_id':self.plan['machine_id']}))
        self.path = self.root/self.plan['transaction']/(request['request_id']+'.json')
        self.pointer = self.root/'latest.json'

    def identity(self):
        self.host.identity()
        H.require(self.plan['role'] != 'coordinator', 'coordinator phases must run in the owning schema transaction')
        source = self.request.get('recovery_source_sha',self.plan['source_sha'])
        H.require(Path(__file__).resolve().parents[3] == Path('/srv/transparent-activity/ops/sources')/source,
                  'remote action requires its immutable reviewed operation source')
        receipt = H.load(Path('/srv/transparent-activity/ops/staging')/(source+'.json'))
        S.verify_receipt(receipt, Path('/srv/transparent-activity/ops/sources')/source, source, receipt['archive_sha256'])
        if source != self.plan['source_sha']:
            original = self.plan['source_sha']
            old = H.load(Path('/srv/transparent-activity/ops/staging')/(original+'.json'))
            S.verify_receipt(old,Path('/srv/transparent-activity/ops/sources')/original,original,old['archive_sha256'])

    def status(self):
        if not self.path.exists():
            return {'status':'absent', 'request_sha256':digest(self.request)}
        H.require(not self.path.is_symlink() and self.path.stat().st_size <= 1024*1024, 'invalid remote owner record')
        record = H.load(self.path)
        H.require(record.get('request_sha256') == digest(self.request) and record.get('request') == self.request,
                  'remote request identity changed')
        return record

    def latest(self):
        if not self.pointer.exists():
            return None
        H.require(not self.pointer.is_symlink(), 'remote owner pointer cannot be a symlink')
        pointer = H.load(self.pointer)
        H.require(set(pointer) == {'transaction', 'request_id'} and H.TXN.fullmatch(pointer['transaction']) and
                  re.fullmatch('[a-z0-9-]{1,64}', pointer['request_id']), 'invalid remote owner pointer')
        return H.load(self.root/pointer['transaction']/(pointer['request_id']+'.json'))

    def fence(self):
        schema_fence.local_schema_fence()
        previous = self.latest()
        H.require(previous is None or previous.get('status') in ('passed', 'failed', 'reconciled'),
                  'unfinished remote host owner; inspect/reconcile before another action')

    def save(self, record):
        durable.atomic_json(self.path, record, mode=0o600)

    def reconcile(self):
        # Acquiring the same lock proves that no correctly inherited local
        # descendant still owns it. It does not establish service correctness.
        with self.lock_factory() as lock:
            lock.verify()
            record = self.status()
            H.require(record['status'] in ('running', 'interrupted'), 'remote owner does not need reconciliation')
            previous = self.latest()
            H.require(previous == record, 'reconcile the latest remote owner first')
            record.update(status='reconciled', reconciled_unix=time.time())
            self.save(record)
            return record

    def run(self):
        action = self.request['action']
        if action in READ_ONLY:
            self.fence()
            # Read-only preflight neither creates a lock nor a writer record.
            return self.execute()
        with self.lock_factory() as lock:
            lock.verify()
            self.fence()
            existing = self.status()
            H.require(existing['status'] == 'absent', 'remote request already exists; inspect its retained result')
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            record = {'version':1, 'request':self.request, 'request_sha256':digest(self.request),
                      'status':'running', 'pid':os.getpid(), 'started_unix':time.time(), 'result':None}
            self.save(record)
            durable.atomic_json(self.pointer, {'transaction':self.plan['transaction'], 'request_id':self.request['request_id']}, mode=0o600)
            old = os.environ.get(inherited_lock.VARIABLE)
            os.environ[inherited_lock.VARIABLE] = ','.join(map(str, lock.descriptors()))
            os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
            try:
                self.host.identity(mutation=True)
                record['result'] = self.execute()
                lock.verify()
                record['status'] = 'passed'
            except BaseException as error:
                record.update(status='interrupted' if isinstance(error, (subprocess.TimeoutExpired, KeyboardInterrupt, SystemExit)) else 'failed',
                              error_type=type(error).__name__)
                raise
            finally:
                if old is None:
                    os.environ.pop(inherited_lock.VARIABLE, None)
                else:
                    os.environ[inherited_lock.VARIABLE] = old
                record['finished_unix'] = time.time()
                self.save(record)
            return record

    def execute(self):
        action = self.request['action']
        if action == 'repair-restore':
            return self.host.restore(repair_token=self.request['request_id'])
        if action == 'repair-capture':
            return self.host.capture(repair_token=self.request['request_id'])
        if action == 'attest-captured-router-guard':
            return self.host.attest_captured_router_guard(self.request['guard_sha256'])
        if action.startswith('verify-'):
            deadline = time.monotonic()+(300 if action == 'verify-worker' else 60)
            while True:
                try:
                    status = self.host.commands.control()
                except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
                    status = {}
                if status:
                    H.require(status.get('invalidated') is False and status.get('candidate') is None and status.get('preparing') is None,
                              'worker advertised invalidation or unauthorized preparation')
                if status.get('warm') is True:
                    break
                H.require(time.monotonic() < deadline, 'worker warm deadline exceeded')
                time.sleep(2)
            return self.host.verify_worker(rollback=action == 'verify-rollback-worker')
        return getattr(self.host, action.replace('-', '_'))()


class Dispatch:
    def __init__(self, inventory, *, run=subprocess.run):
        self.inventory = inventory
        self.executor = SSHExecutor(inventory)
        self.run = run

    def call(self, host, request, *, mode='run', timeout=360):
        validate(request)
        entry = self.inventory.hosts[host]
        H.require(entry.get('machine_id') == request['plan']['machine_id'], 'remote machine differs from reviewed host plan')
        H.require(entry.get('user', self.inventory.ssh.get('user', 'root')) == 'root' or entry.get('sudo'), 'host actor needs root identity')
        inherited_lock.descriptors(required=mode in ('run', 'reconcile') and request['action'] not in READ_ONLY, path=H.LOCK)
        source = Path('/srv/transparent-activity/ops/sources')/request.get('recovery_source_sha',request['plan']['source_sha'])
        argv = (['sudo', '-n', '--'] if entry.get('sudo') else []) + ['/usr/bin/python3', '-B', str(source/'ops/scripts/wallet-pir-deploy.py'),
                'schema-host-'+mode, '--request-sha256', digest(request)]
        # No persistent SSH masters: their lifetime would retain global FDs.
        ssh = self.executor.transport(host)
        ssh = [*ssh[:-1], '-oControlMaster=no', '-oControlPath=none', ssh[-1]]
        try:
            result = self.run([*ssh, shlex.join(argv)], input=H.encode(request), capture_output=True, timeout=timeout,
                              **inherited_lock.options())
        except subprocess.TimeoutExpired:
            raise
        # SSH loss or any unstructured reply leaves remote outcome unknown.
        # Raising TimeoutExpired makes SchemaRunner pause instead of racing it.
        try:
            reply = json.loads(result.stdout, object_pairs_hook=H.unique)
            H.require(reply['request_sha256'] == digest(request), 'remote reply identity differs')
            H.require(reply['status'] in ('passed', 'failed', 'interrupted', 'reconciled', 'absent'), 'invalid remote reply status')
        except (ValueError, KeyError, TypeError):
            raise subprocess.TimeoutExpired('schema remote reply unavailable; inspect pinned host owner', timeout) from None
        if reply['status'] == 'interrupted' or result.returncode in (75, 255):
            raise subprocess.TimeoutExpired('schema remote owner requires reconciliation', timeout)
        H.require(result.returncode == 0 and reply['status'] in ('passed', 'reconciled', 'absent'), 'remote host phase failed; inspect private owner result')
        return reply
