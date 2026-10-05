"""Closed bootstrap owner survey using the retained production inventory.

Stdlib only when embedded by source staging. The existing reviewed runtime is
used solely for pinned SSH transport after its source receipt is verified. No
credential values or inventory contents are retained. No baseline exception is
created here: a live unclassified prototype or incomplete owner refuses.

A worker `shard-control` in an SSH session is pending, not admitted: `fleet`
admits it only when `control_attribution` binds its exact connection to a
verified direct client of the running replica reconciler, observed on the
coordinator immediately before or after that worker's survey. A host checking
only itself (`local` plus `verify` without attributions) refuses it.
"""
import hashlib
import importlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import time
try:
    S
except NameError:
    from wallet_pir_ops import owner_survey as S

try:
    A
except NameError:
    from wallet_pir_ops import ancillary_baseline as A

try:
    CA
except NameError:
    from wallet_pir_ops import control_attribution as CA

ROOT = Path('/srv/transparent-activity/ops')
INPUTS = ROOT/'input-staging'
LOCK = Path('/run/lock/wallet-pir-production.lock')
RUNTIME_SHA = '4c85b6c20ced1e2077245491e77d3afc98bfd644'
INVENTORY = Path('/srv/transparent-activity/full-v11/inputs/0604bc95c50fa084919cfc5d0fcf3310398761f721404c78f604c98d78ffb060/inventory.json')
INVENTORY_SHA = '39609723d74fa60852dc86be763bd892bc085a21e05fb6526ff6fb73a08f1b9f'
PINS = {'coordinator':'6a16bce88fb2493681d327344090293a',
        'router':'6a384cb26a354c4285198bab6a1c9713',
        'worker-1':'74abf01381c74848b9b1a848d391240c',
        'worker-2':'8fa779c5e6ef4cbc923d73c92d1a8328',
        'worker-3':'0c1420311d8a47a0a6fce44ebf91cfb6'}
BASELINE = ('transparent-shard-server.service','transparent-filter-server.service',
            'transparent-publish-controller.service','transparent-control-sessions.service',
            'transparent-fleet-scaler.service','transparent-replica-reconciler.service',
            'transparent-quality-rollout.service')
NAMES = ('wallet-pir-deploy.py','transparent-block-server','event-ingest','event-spotcheck',
         'filter-server','shard-build','shard-control','shard-index','shard-verify','shard-assign',
         'shard-render-router','shard-query','transparent-shard-server','transparent-filter-server',
         'transparent-publish-controller','native_certificate','transparent-event-ingest','shard-publish',
         'script-sample','shard-cutoff','journal-inventory','shard-census','transparent-loadtest',
         'transparent-measure','rate-query','activity-reopen')
CLASSES = {'names':sorted(NAMES),'roots':['/srv/transparent-activity/','/srv/transparent-pir/']}
SECONDS = 300
HOST_SECONDS = 60
MAX_REPLY = 1 << 20
KIND = 'bootstrap-fleet-survey-v1'
FENCE = None


def require(ok,message):
    if not ok:raise ValueError(message)


def no_links(path):
    for p in [Path(path), *Path(path).parents]:
        require(not p.is_symlink(),'bootstrap path contains a link')


def immutable_json(path,value):
    no_links(path)
    raw=S.canonical(value)+b'\n'
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as f:
        f.write(raw);f.flush();os.fchmod(f.fileno(),0o400);os.fsync(f.fileno())
    fd=os.open(Path(path).parent,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def pinned(inventory):
    require(sorted(inventory.hosts)==sorted(PINS) and
            all(inventory.hosts[h].get('machine_id')==pin for h,pin in PINS.items()),
            'bootstrap requires the retained complete five-host production fleet')
    require(inventory.ssh.get('mode')=='pinned', 'bootstrap requires pinned SSH')
    return inventory


def runtime(verify_receipt):
    """Verify installed transport source before importing it; no guessed keys."""
    root=ROOT/'sources'/RUNTIME_SHA
    no_links(root)
    receipt_path=ROOT/'staging'/(RUNTIME_SHA+'.json')
    no_links(receipt_path);require(receipt_path.stat().st_size<=1<<20,'bootstrap runtime receipt exceeds bound')
    receipt=json.loads(receipt_path.read_text(),object_pairs_hook=S.unique)
    verify_receipt(receipt,root,RUNTIME_SHA,receipt['archive_sha256'])
    no_links(INVENTORY)
    require(INVENTORY.stat().st_size<=1<<20 and hashlib.sha256(INVENTORY.read_bytes()).hexdigest()==INVENTORY_SHA,
            'bootstrap retained fleet inventory differs')
    # The imported runtime is exact reviewed code, not the source being staged.
    alias='_wallet_pir_bootstrap_runtime'
    if alias not in sys.modules:
        spec=importlib.util.spec_from_file_location(alias,root/'ops/lib/wallet_pir_ops/__init__.py',
                submodule_search_locations=[str(root/'ops/lib/wallet_pir_ops')])
        package=importlib.util.module_from_spec(spec);sys.modules[alias]=package;spec.loader.exec_module(package)
    else:
        require(Path(sys.modules[alias].__file__).resolve()==root/'ops/lib/wallet_pir_ops/__init__.py',
                'bootstrap runtime package identity differs')
    D=importlib.import_module(alias+'.deploy.descriptors')
    T=importlib.import_module(alias+'.deploy.remote')
    inventory=pinned(D.load_inventory(INVENTORY))
    return inventory,T.SSHExecutor(inventory)


def local(binding,host,*,holder=None,skip=None,recovery=None,deadline=None,source_skip=None):
    """All retained source, schema, host and input owners plus live descendants."""
    from_namespace = globals().get('FENCE')
    require(from_namespace is not None,'bootstrap fence provider missing')
    def tick():
        require(deadline is not None and time.monotonic()<deadline,'bootstrap survey deadline exceeded')
    tick()
    machine=Path('/etc/machine-id').read_text().strip()
    require(host in PINS and os.geteuid()==0 and machine==PINS[host], 'bootstrap survey machine differs')
    from_namespace(skip_input=skip,recovery=recovery)
    def select(label,name,record,sha):
        leaf=Path(name).name
        if label=='input-staging' and re.fullmatch(r'[0-9a-f]{64}\.json',leaf) and isinstance(record,dict) and \
                record.get('request_sha256')==leaf[:-5] and 'status' in record:
            return {'name':name,'sha256':sha,'status':record['status']}
        if label=='source-staging' and re.fullmatch(r'[0-9a-f]{40}\.json',leaf) and isinstance(record,dict):
            return {'name':name,'sha256':sha,'status':record.get('status'),'source':record.get('source_sha'),
                    'archive':record.get('archive_sha256')}
    def refuse(item):
        if item['status'] in ('staged','reconciled'):return None
        if skip and item['name']==skip+'.json':return None
        if source_skip and item.get('source')==source_skip.get('source_sha') and \
                item.get('archive')==source_skip.get('sha256') and item['name']==source_skip['source_sha']+'.json':return None
        return 'unfinished retained bootstrap owner: '+item['name']
    ancillary=A.observe(machine,tick=tick)
    authorities=A.authorities(ancillary,machine)
    # Only workers receive reconciler controls; the coordinator binds its own
    # clients to the baseline unit cgroup.
    found,rejected=CA.controls(tick=tick) if host.startswith('worker-') else ([],[])
    result=S.observe((('schema',ROOT/'schema'),('host-actions',ROOT/'host-actions'),
                      ('input-staging',INPUTS),('source-staging',ROOT/'staging')),
                     classes=CLASSES,baseline=BASELINE,binding=binding,lock_path=LOCK,holder=holder,
                     tick=tick,select=select,refuse=refuse,ancillary=authorities,
                     pending={c['pid']:c for c in found})
    tick()
    result.update(bootstrap=KIND,machine_id=machine,observed_unix=time.time(),
                  inventory_sha256=INVENTORY_SHA,ancillary=ancillary,controls=found,
                  control_rejections=rejected[:S.BOUNDS['listed']])
    return result


def verify(value,binding,host,holder=None,attributed=None):
    """One host's survey; pending worker controls pass only with `attributed` covering each exactly."""
    require(isinstance(value,dict) and value.get('bootstrap')==KIND and value.get('kind')==S.KIND and
            value.get('binding')==binding and value.get('machine_id')==PINS[host] and value.get('euid')==0 and
            value.get('inventory_sha256')==INVENTORY_SHA and S.number(value.get('observed_unix')) and
            -1<=time.time()-value['observed_unix']<=SECONDS and value.get('bounds')==S.BOUNDS and
            value.get('classes')==CLASSES and
            value.get('classes_sha256')==hashlib.sha256(S.canonical(CLASSES)).hexdigest() and
            value.get('namespaces')==[[n,str(path)] for n,path in
                (('schema',ROOT/'schema'),('host-actions',ROOT/'host-actions'),
                 ('input-staging',INPUTS),('source-staging',ROOT/'staging'))] and
            value.get('baseline')==list(BASELINE) and isinstance(value.get('inventory'),dict) and
            isinstance(value['inventory'].get('sha256'),str) and re.fullmatch('[0-9a-f]{64}',value['inventory']['sha256']) and
            type(value.get('selected_count')) is int and 0<=value['selected_count']<=S.BOUNDS['selected'] and
            isinstance(value.get('selected'),list) and
            len(value['selected'])==min(value['selected_count'],S.BOUNDS['listed']) and
            isinstance(value.get('selected_sha256'),str) and re.fullmatch('[0-9a-f]{64}',value['selected_sha256']),
            'bootstrap survey binding is partial, stale or foreign')
    A.verify(value.get('ancillary'),PINS[host])
    controls=CA.verify_controls(value.get('controls'))
    identities=sorted((c['pid'],c['start_ticks']) for c in controls)
    require((host.startswith('worker-') or not controls) and isinstance(value.get('control_rejections'),list) and
            len(value['control_rejections'])<=S.BOUNDS['listed'] and value.get('pending_count')==len(controls) and
            isinstance(value.get('pending'),list) and
            sorted((p.get('pid'),p.get('start_ticks')) for p in value['pending'])==identities,
            'bootstrap survey pending controls are partial or foreign')
    require(not controls or isinstance(attributed,list) and
            sorted((a['control']['pid'],a['control']['start_ticks']) for a in attributed)==identities,
            'bootstrap worker control is not attributed to the verified replica reconciler')
    require(value.get('blocked_count')==0 and value.get('blocked')==[] and
            value.get('associated_count')==0 and value.get('associated')==[] and
            value.get('unattributed_count')==0 and value.get('unattributed')==[] and value.get('processes')==[] and
            [p.get('pid') for p in value.get('lock',{}).get('holders',[])]==([holder['pid']] if holder else []),
            'bootstrap fleet has unfinished, live or unattributed owners')
    return value


def fleet(request,lock,verify_receipt,remote_code,*,skip=None,recovery=None,retain=None,source_skip=None):
    """Fresh nonce issued after lock ownership, complete actual retained fleet.

    `remote_code` is the exact embedded survey helper, not a caller CLI value.
    Local SSH timeout proves no remote exit: these probes perform only bounded
    reads and never launch mutation work. Raw replies are retained before refusal.
    """
    inventory,transport=runtime(verify_receipt)
    nonce=secrets.token_hex(24); identifier=hashlib.sha256(S.canonical(request)).hexdigest()
    deadline=time.monotonic()+SECONDS
    current=S.process(os.getpid())
    require(current is not None,'bootstrap owner kernel identity missing')
    holder={'pid':current['pid'],'start_ticks':current['start_ticks']} if lock else None
    results={};directory=None
    if retain is None and lock:
        directory=INPUTS/'fleet-surveys'/identifier/nonce
        no_links(directory);directory.mkdir(parents=True,mode=0o700)
        def retain(host,raw):
            path=directory/(host+'.json')
            fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            with os.fdopen(fd,'wb') as f:
                f.write(raw[:MAX_REPLY]);f.flush();os.fchmod(f.fileno(),0o400);os.fsync(f.fileno())
            fd=os.open(directory,os.O_RDONLY)
            try:os.fsync(fd)
            finally:os.close(fd)
    for host in sorted(PINS):
        if lock:lock.verify()
        attributed=None
        # Taken before the worker scans, so a control that ends before the
        # reply is still bound; a later one is bound by the snapshot after it.
        before=CA.authority() if host.startswith('worker-') else None
        host_skip=skip if host=='coordinator' else None
        binding={'request_sha256':identifier,'nonce':nonce,'host':host,'skip':host_skip}
        host_source_skip=source_skip if source_skip and PINS[host]==source_skip.get('machine_id') else None
        code=0
        if host=='coordinator':
            value=local(binding,host,holder=holder,skip=host_skip,recovery=recovery,deadline=deadline,source_skip=host_source_skip)
            raw=S.canonical(value)
        else:
            envelope={'binding':binding,'host':host,'skip':host_skip,'source_skip':host_source_skip}
            entry=inventory.hosts[host]
            prefix=['sudo','-n','--'] if entry.get('sudo') else []
            argv=[*prefix,'/usr/bin/python3','-B','-c',remote_code]
            ssh=transport.transport(host)
            ssh=[*ssh[:-1],'-oControlMaster=no','-oControlPath=none',ssh[-1]]
            try:
                process=subprocess.Popen([*ssh,shlex.join(argv)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                                         stderr=subprocess.DEVNULL)
                raw,_=process.communicate(S.canonical(envelope)+b'\n',timeout=min(HOST_SECONDS,max(.01,deadline-time.monotonic())))
                code=process.returncode
            except subprocess.TimeoutExpired:
                if retain:retain(host,S.canonical({'host':host,'transport':'timeout','binding':binding}))
                process.kill();process.wait()
                raise ValueError('bootstrap read-only survey transport timeout') from None

        if retain:retain(host,raw)
        require(code==0 and len(raw)<=MAX_REPLY,'bootstrap survey transport failed or response exceeds bound')
        if host!='coordinator':value=json.loads(raw,object_pairs_hook=S.unique)
        if before is not None and isinstance(value,dict) and value.get('controls'):
            snapshots=[before,CA.authority()]
            if retain:retain(host+'.attribution',S.canonical({'host':host,'binding':binding,'snapshots':snapshots}))
            attributed=CA.attribute(CA.verify_controls(value['controls']),snapshots)
            value['control_attribution']={'attributed':attributed,'snapshots':snapshots}
        results[host]=verify(value,binding,host,holder if host=='coordinator' else None,attributed)
        require(time.monotonic()<deadline,'bootstrap complete-fleet survey exceeded bound')
    require(set(results)==set(PINS),'bootstrap fleet survey is incomplete')
    if lock:lock.verify()
    proof={'schema':KIND,'request_sha256':identifier,'nonce':nonce,'inventory_sha256':INVENTORY_SHA,
            'hosts':sorted(results),'surveys_sha256':hashlib.sha256(S.canonical(results)).hexdigest(),
            'observed_unix':time.time(),'owner':dict(holder,boot_id=S.boot_id()) if holder else None,
           'evidence':str(directory) if directory else None}
    if lock is None:proof['surveys']=results
    return proof


def lease_id(request):
    normalized=dict(request,mode='stage')
    normalized.pop('coordinator_fleet',None)
    return hashlib.sha256(S.canonical({'kind':'source-bootstrap-lease-v1','request':normalized})).hexdigest()


def leased(request,lock,verify_receipt,remote_code,operation,save):
    """Durable global fence before surveys and any source receiver effect."""
    FENCE(recovery=request.get('recovery'));lock.verify()
    require(Path('/etc/machine-id').read_text().strip()==PINS['coordinator'],
            'source fleet lease requires the pinned coordinator')
    identifier=lease_id(request);owner=INPUTS/(identifier+'.json')
    no_links(owner);require(not owner.exists(),'source fleet lease exists; inspect or reconcile before retry')
    INPUTS.mkdir(parents=True,exist_ok=True,mode=0o700)
    immutable_json(INPUTS/(identifier+'.request.json'),dict(request,mode='stage'))
    current=S.process(os.getpid());require(current is not None,'source fleet owner identity missing')
    record={'kind':'source-bootstrap-lease-v1','request_sha256':identifier,'status':'running','machine_id':PINS['coordinator'],
            'pid':current['pid'],'start_ticks':current['start_ticks'],'boot_id':S.boot_id(),
            'started_unix':time.time(),'request':dict(request,mode='stage')}
    save(owner,record);save(INPUTS/'latest.json',{'request_sha256':identifier})
    try:
        proof=fleet(request,lock,verify_receipt,remote_code,skip=identifier,recovery=request.get('recovery'))
        proof['lease_request_sha256']=identifier;record['fleet']=proof;save(owner,record)
        result=operation(proof)
        lock.verify();record.update(status='staged',result=result)
        return result
    except BaseException as error:
        record.update(status='failed',error_type=type(error).__name__)
        raise
    finally:
        record['finished_unix']=time.time();save(owner,record)


def reconciled(request,lock,verify_receipt,remote_code,operation,save):
    """No signals: prove every source owner exited, retain failures, clear fence."""
    identifier=lease_id(request);owner=INPUTS/(identifier+'.json')
    no_links(owner);require(owner.is_file() and owner.stat().st_size<=1<<20,'source lease owner missing or oversized')
    record=json.loads(owner.read_text(),object_pairs_hook=S.unique)
    require(record.get('kind')=='source-bootstrap-lease-v1' and record.get('request_sha256')==identifier and
            record.get('request')==dict(request,mode='stage') and record.get('status') in ('running','failed'),
            'source lease is not the matching unfinished owner')
    require(type(record.get('pid')) is int and record['pid']>0 and
            type(record.get('start_ticks')) is int and record['start_ticks']>0 and
            isinstance(record.get('boot_id'),str) and
            re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',record['boot_id']),
            'source lease kernel identity is incomplete')
    current=S.process(record['pid'])
    require(current is None or current['state']=='Z' or current['start_ticks']!=record.get('start_ticks') or record['boot_id']!=S.boot_id(),
            'source lease owner remains alive; nothing was signalled')
    no_links(INPUTS/'latest.json')
    require(json.loads((INPUTS/'latest.json').read_text())=={'request_sha256':identifier},
            'reconcile the latest input owner first')
    FENCE(skip_input=identifier,recovery=request.get('recovery'));lock.verify()
    proof=fleet(dict(request,mode='stage'),lock,verify_receipt,remote_code,skip=identifier,recovery=request.get('recovery'),source_skip=request)
    proof['lease_request_sha256']=identifier
    result=operation(proof)
    # Retain the original owner and every failed field before terminal update.
    immutable_json(INPUTS/(identifier+'.failure.json'),record)
    record.update(status='reconciled',reconciled_unix=time.time(),reconciliation_fleet=proof,result=result)
    save(owner,record)
    return result


def status(request):
    identifier=lease_id(request);path=INPUTS/(identifier+'.json')
    no_links(path)
    if not path.exists():return {'status':'absent','request_sha256':identifier}
    require(path.is_file() and path.stat().st_size<=1<<20,'source lease owner invalid')
    record=json.loads(path.read_text(),object_pairs_hook=S.unique)
    require(record.get('request_sha256')==identifier and record.get('request')==dict(request,mode='stage'),
            'source lease owner identity changed')
    return {k:v for k,v in record.items() if k in
            ('kind','status','request_sha256','pid','start_ticks','boot_id','started_unix','finished_unix',
             'fleet','reconciliation_fleet','error_type')}
