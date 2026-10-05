"""Bootstrap immutable reviewed operation sources through the deployment wrapper.

The root helper owns the production lock in the same process that receives and
extracts the archive. No service, config, binary or public route is activated.
"""
import hashlib
import base64
import zlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

from wallet_pir_ops import hostlock, inherited_lock, schema_fence, owner_survey, ancillary_baseline
from wallet_pir_ops.deploy.remote import ProductionLock, SSHExecutor

HERE = Path(__file__).parent
HELPER_PATH = HERE/'activity_source_stage_host.py'
FLEET_PATH = HERE/'activity_bootstrap_fleet.py'
_spec = importlib.util.spec_from_file_location('source_bootstrap_fleet', FLEET_PATH)
FLEET = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(FLEET)
FLEET.FENCE = schema_fence.local_schema_fence
_host_spec = importlib.util.spec_from_file_location('source_bootstrap_host', HELPER_PATH)
HOST = importlib.util.module_from_spec(_host_spec); _host_spec.loader.exec_module(HOST)
# Each prefixed component keeps its own globals; their similarly named helpers
# and bounds must never overwrite those of the source receiver.
EMBEDDED_FLEET = "import types\n"
for name, path in (('_ANCILLARY',Path(ancillary_baseline.__file__)),('_SURVEY', Path(owner_survey.__file__)), ('_FENCE', Path(schema_fence.__file__)),
                   ('_BOOTSTRAP_FLEET', FLEET_PATH)):
    EMBEDDED_FLEET += name+"=types.ModuleType("+repr(name)+")\n"
    if name == '_BOOTSTRAP_FLEET':
        EMBEDDED_FLEET += "_BOOTSTRAP_FLEET.S=_SURVEY\n_BOOTSTRAP_FLEET.A=_ANCILLARY\n"
    EMBEDDED_FLEET += "exec("+repr(path.read_text())+", "+name+".__dict__)\n"
EMBEDDED_FLEET += "_BOOTSTRAP_FLEET.FENCE=_FENCE.local_schema_fence\n"
SURVEY_HELPER = EMBEDDED_FLEET+r"""
import json, sys, time
try:
    raw=sys.stdin.buffer.readline(8193)
    _BOOTSTRAP_FLEET.require(len(raw)<=8192 and raw.endswith(b'\n'), 'invalid bootstrap survey header')
    value=json.loads(raw,object_pairs_hook=_SURVEY.unique)
    _BOOTSTRAP_FLEET.require(set(value)=={'binding','host','skip','source_skip'}, 'invalid bootstrap survey envelope')
    result=_BOOTSTRAP_FLEET.local(value['binding'],value['host'],skip=value['skip'],
            source_skip=value['source_skip'],deadline=time.monotonic()+_BOOTSTRAP_FLEET.HOST_SECONDS)
    print(json.dumps(result,sort_keys=True))
except BaseException as error:
    print(json.dumps({'error_type':type(error).__name__}))
    sys.exit(1)
"""
HELPER = Path(hostlock.__file__).read_text()+'\n'+Path(schema_fence.__file__).read_text()+'\n'+EMBEDDED_FLEET+\
         '_BOOTSTRAP_REMOTE_CODE='+repr(SURVEY_HELPER)+'\n'+HELPER_PATH.read_text()
# Linux bounds each exec argument, including the complete remote shell command.
# The immutable reviewed program is compressed only for transport, then bounded
# and hash checked before compilation. The request remains sys.argv[1].
MAX_HELPER = 512 << 10
MAX_COMMAND = 120 << 10
MAX_REQUEST = 8192
if len(HELPER.encode()) > MAX_HELPER:
    raise ValueError('reviewed source helper exceeds decompressed bound')
HELPER_SHA256 = hashlib.sha256(HELPER.encode()).hexdigest()
HELPER_PAYLOAD = base64.b64encode(zlib.compress(HELPER.encode(), 9)).decode('ascii')
LAUNCHER = "import base64,hashlib,zlib\n" + \
    "d=zlib.decompressobj()\nraw=d.decompress(base64.b64decode("+repr(HELPER_PAYLOAD)+",validate=True),"+str(MAX_HELPER+1)+")\n" + \
    "assert len(raw)<="+str(MAX_HELPER)+" and d.eof and not d.unconsumed_tail and not d.unused_data\n" + \
    "assert hashlib.sha256(raw).hexdigest()=="+repr(HELPER_SHA256)+"\n" + \
    "exec(compile(raw,'<reviewed-source-bootstrap>','exec'))\n"

class Unknown(RuntimeError):
    """Transport outcome requires retained owner observation and reconciliation."""


def command_for(prefix, request):
    raw = json.dumps(request,sort_keys=True,separators=(',',':'))
    if len(raw.encode()) > MAX_REQUEST:
        raise ValueError('source staging request exceeds bounded header')
    command = shlex.join([*prefix, '/usr/bin/python3', '-B', '-c', LAUNCHER, raw])
    if len(command.encode()) > MAX_COMMAND:
        raise ValueError('source staging command exceeds bounded transport argument')
    return command

MAX_COMPRESSED = 64 << 20  # v1 bound shared with the transmitted root helper.


class SourceStage:
    def __init__(self, inventory, out=print, target=None, recovery=None, attempt=1, private_evidence_file=None):
        self.inventory, self.out = inventory, out
        self.private_evidence_file = private_evidence_file
        self.recovery = recovery
        if type(attempt) is not int or not 1 <= attempt <= 100:raise ValueError("invalid source fleet attempt")
        self.attempt = attempt
        lock = inventory.lock
        self.target = target
        if target is None:
            if lock.get('type') != 'remote':
                raise ValueError('source bootstrap requires a remote coordinator inventory')
            self.host = lock['host']
        else:
            if lock.get('type') != 'pinned_host' or os.geteuid() != 0 or Path('/etc/machine-id').read_text().strip() != lock['machine_id']:
                raise ValueError('remote source staging requires the pinned root coordinator')
            self.host = target
            if inventory.hosts[target].get('machine_id') == lock['machine_id']:
                raise ValueError('remote source staging cannot target its coordinator')
        entry = inventory.hosts[self.host]
        self.machine = entry.get('machine_id')
        if not isinstance(self.machine, str) or not re.fullmatch('[0-9a-f]{32}', self.machine):
            raise ValueError('source bootstrap requires the coordinator machine_id pin')
        if entry.get('user', inventory.ssh.get('user', 'root')) != 'root' and not entry.get('sudo'):
            raise ValueError('source bootstrap requires root or the configured sudo identity')
        self.executor = SSHExecutor(inventory)

    def request(self, mode, source, checksum):
        if not re.fullmatch('[0-9a-f]{40}', source) or not re.fullmatch('[0-9a-f]{64}', checksum):
            raise ValueError('source bootstrap requires exact source SHA and archive SHA-256')
        request = {'mode': mode, 'source_sha': source, 'sha256': checksum, 'machine_id': self.machine, 'guard_attempt': self.attempt}
        if self.recovery is not None and self.target is None:
            request['recovery'] = self.recovery
        return request

    def call(self, request, archive=None):
        entry = self.inventory.hosts[self.host]
        prefix = ['sudo', '-n', '--'] if entry.get('sudo') else []
        command = command_for(prefix, request)
        handle = Path(archive).open('rb') if archive else subprocess.DEVNULL
        evidence = None
        try:
            path = getattr(self, 'private_evidence_file', None)
            if path is not None:
                if request.get('mode') != 'preflight':
                    raise ValueError('private source survey evidence is preflight-only')
                path = Path(path).absolute()
                if any(p.is_symlink() for p in (path, *path.parents)):
                    raise ValueError('private evidence path contains a link')
                evidence = os.fdopen(os.open(path, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600), 'wb')
                os.fchmod(evidence.fileno(), 0o600)
            transport = self.executor.transport(self.host)
            transport = [*transport[:-1], '-oControlMaster=no', '-oControlPath=none', transport[-1]]
            try:
                result = subprocess.run(transport+[command], stdin=handle,
                                        capture_output=True, timeout=1800, **inherited_lock.options())
            except (subprocess.TimeoutExpired, OSError):
                raise Unknown('source staging transport outcome unknown; observe coordinator owner and reconcile') from None
            if evidence is not None:
                # Bounded exact wire reply stays private even when parsing/guards refuse.
                evidence.write(result.stdout[:12 << 20]); evidence.flush(); os.fsync(evidence.fileno())
                if len(result.stdout) > 12 << 20:
                    raise Unknown('source reply exceeds private evidence bound; retained prefix cannot clear guards')
        finally:
            if evidence is not None:
                os.fchmod(evidence.fileno(), 0o400); evidence.close()
            if archive:
                handle.close()
        # Keep arbitrary remote stderr/argv out of user-facing errors.
        if result.returncode:
            raise Unknown('source staging transport outcome unknown; observe coordinator owner and reconcile')
        try:
            reply = json.loads(result.stdout)
            if not isinstance(reply,dict) or type(reply.get('ok')) is not bool:
                raise ValueError()
        except (ValueError, UnicodeError):
            raise Unknown('source staging reply invalid; observe coordinator owner and reconcile') from None
        if not reply.get('ok'):
            site = reply.get('refusal_site')
            suffix = ''
            if (isinstance(site, dict) and set(site) == {'component','function','line'} and
                    site['component'] in ('bootstrap','survey','fence','ancillary') and
                    isinstance(site['function'],str) and re.fullmatch('[A-Za-z_][A-Za-z_0-9]{0,63}',site['function']) and
                    type(site['line']) is int and 1 <= site['line'] <= 5000):
                suffix = ' (reviewed guard %s.%s:%d)' % (site['component'],site['function'],site['line'])
            raise ValueError('source staging refused: '+reply['error']+suffix)
        self.out(json.dumps(reply['result'], sort_keys=True))
        return reply['result']

    def run(self, mode, source, checksum, archive=None):
        if getattr(self, 'private_evidence_file', None) is not None and (mode != 'preflight' or self.target is not None):
            raise ValueError('private survey evidence requires a coordinator preflight')
        request = self.request(mode, source, checksum)
        if mode in ('plan', 'preflight', 'stage'):
            if archive is None:
                raise ValueError('pass the git archive for source staging')
            if not Path(archive).is_file() or Path(archive).is_symlink():
                raise ValueError('source archive must be a regular file, not a symlink')
            if Path(archive).stat().st_size > MAX_COMPRESSED:
                raise ValueError('source archive exceeds the received-archive bound; export only operation sources')
            # Stream without buffering or exporting file contents to logs.
            with Path(archive).open('rb') as stream:
                actual = hashlib.file_digest(stream, 'sha256').hexdigest()
            if actual != checksum:
                raise ValueError('local source archive checksum differs from reviewed plan')
        if mode == 'plan':
            self.out('source '+source+' archive '+checksum+' coordinator '+self.host)
            self.out('immutable staging only; no service activation; preflight required before stage')
            self.out(json.dumps({'bootstrap_inventory_sha256':FLEET.INVENTORY_SHA,
                'bootstrap_runtime_source_sha':FLEET.RUNTIME_SHA,'hosts':sorted(FLEET.PINS),
                'survey_wall_seconds':FLEET.SECONDS,'guard_attempt':self.attempt,
                'lease_request_sha256':FLEET.lease_id(self.request('stage',source,checksum))},sort_keys=True))
            return
        if self.target is not None and mode == 'preflight':
            proof=FLEET.fleet(request,None,HOST.verify_receipt,SURVEY_HELPER)
            request['coordinator_fleet']={k:v for k,v in proof.items() if k!='surveys'}
            result=self.call(request)
            result['coordinator_fleet']=proof
            self.out(json.dumps(result,sort_keys=True))
            return result
        if mode in ('stage','reconcile') and self.target is not None:
            with ProductionLock(self.inventory.lock) as lock:
                lock.verify(); schema_fence.local_schema_fence(recovery=self.recovery)
                old = os.environ.get(inherited_lock.VARIABLE)
                os.environ[inherited_lock.VARIABLE] = ','.join(map(str,lock.descriptors()))
                try:
                    def operation(proof):
                        effect=dict(request,coordinator_fleet=proof)
                        if mode=='stage':self.call(dict(effect,mode='preflight'))
                        return self.call(effect,archive if mode=='stage' else None)
                    runner=FLEET.leased if mode=='stage' else FLEET.reconciled
                    return runner(request,lock,HOST.verify_receipt,SURVEY_HELPER,operation,HOST.atomic_json)
                finally:
                    if old is None: os.environ.pop(inherited_lock.VARIABLE,None)
                    else: os.environ[inherited_lock.VARIABLE] = old
        if mode=='status' and self.target is not None:
            result=self.call(request);result['coordinator_owner']=FLEET.status(request)
            return result
        if mode == 'stage':
            self.call(self.request('preflight', source, checksum))
        return self.call(request, archive if mode == 'stage' else None)
