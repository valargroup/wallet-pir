"""Stdlib-only source-staging helper, prefixed with the shared hostlock source.

Only the deployment wrapper transmits/runs this helper. It has no standalone
entry point without that prefix. Root controls all paths and is trusted.
"""
import hashlib
import importlib.util
import json
import marshal
import os
import re
import stat
from pathlib import Path, PurePosixPath
import shutil
import sys
import tarfile
import tempfile
import time

STAGE_ROOT = Path('/srv/transparent-activity/ops')
LOCK_PATH = Path('/run/lock/wallet-pir-production.lock')
MEMINFO = Path('/proc/meminfo')
MAX_COMPRESSED = 64 << 20
MAX_EXPANDED = 512 << 20
MAX_ENTRIES = 16384


class SourceStageError(ValueError):
    pass


def refusal_site(error):
    """Identify reviewed guard code without serializing exception data or locals."""
    codes = {}
    for label, symbol in (('bootstrap', '_BOOTSTRAP_FLEET'), ('survey', '_SURVEY'),
                          ('fence', '_FENCE'), ('ancillary', '_ANCILLARY')):
        module = globals().get(symbol)
        namespace = getattr(module, '__dict__', {})
        for name, value in namespace.items():
            code = getattr(value, '__code__', None)
            if (code is not None and getattr(value, '__globals__', None) is namespace
                    and name != 'require' and re.fullmatch('[A-Za-z_][A-Za-z_0-9]{0,63}', name)):
                codes[code] = (label, name)
    result = None
    trace = error.__traceback__
    while trace is not None:
        identity = codes.get(trace.tb_frame.f_code)
        if identity is not None and 1 <= trace.tb_lineno <= 5000:
            result = {'component': identity[0], 'function': identity[1], 'line': trace.tb_lineno}
        trace = trace.tb_next
    return result


def require(ok, message):
    if not ok:
        raise SourceStageError(message)


def sha256(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def atomic_json(path, value):
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix='.receipt-')
    try:
        with os.fdopen(descriptor, 'w') as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def headroom(root):
    fields = {}
    with MEMINFO.open() as stream:
        for line in stream:
            key, _, value = line.partition(':')
            if key in ('MemTotal', 'MemAvailable'):
                fields[key] = int(value.split()[0])
    require(fields.get('MemTotal', 0) > 0 and fields.get('MemAvailable', 0)*5 >= fields['MemTotal'],
            'source staging memory below 20 percent available')
    parent = root
    while not parent.exists():
        parent = parent.parent
    disk = os.statvfs(parent)
    require(disk.f_bavail * disk.f_frsize >= MAX_COMPRESSED+MAX_EXPANDED+(1 << 30),
            'insufficient staging disk reserve')
    require(disk.f_bavail / disk.f_blocks >= .20, 'staging disk below 20 percent headroom')


def unpack(archive, target, source, lock):
    files, seen, expanded = {}, set(), 0
    with tarfile.open(archive, 'r:gz') as tar:
        require(tar.pax_headers.get('comment') == source, 'archive is not pinned to the requested git commit')
        for count, entry in enumerate(tar, 1):
            require(count <= MAX_ENTRIES, 'source archive has too many entries')
            path = PurePosixPath(entry.name)
            require(entry.name and not path.is_absolute() and '..' not in path.parts
                    and str(path) not in ('.', '') and '\\' not in entry.name
                    and not any(ord(c) < 32 for c in entry.name), 'unsafe source archive path')
            name = str(path)
            require(name not in seen, 'duplicate source archive path')
            seen.add(name)
            require(entry.isdir() or entry.isfile(), 'source archive links and special files are forbidden')
            expanded += entry.size
            require(entry.size >= 0 and expanded <= MAX_EXPANDED, 'source archive expansion exceeds bound')
            lock.verify()
            output = target/name
            if entry.isdir():
                output.mkdir(parents=True, exist_ok=False, mode=0o700)
                continue
            output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            # No links exist in the fresh, private extraction directory.
            with tar.extractfile(entry) as incoming, output.open('xb') as stream:
                shutil.copyfileobj(incoming, stream, 1 << 20)
                stream.flush()
                os.fchmod(stream.fileno(), 0o700 if entry.mode & 0o111 else 0o600)
                os.fsync(stream.fileno())
            files[name] = sha256(output)
    require('ops/scripts/wallet-pir-deploy.py' in files and
            'transparent/ops/lib/activity_schema_operation.py' in files, 'source archive lacks schema deployment wrapper')
    return files


def verify_receipt(receipt, target, source, checksum):
    require(receipt.get('version') == 1, 'unsupported source staging receipt version')
    require(receipt.get('source_sha') == source and receipt.get('archive_sha256') == checksum,
            'retained source identity differs')
    require(receipt.get('status') == 'staged', 'unfinished source staging; reconcile its private receipt first')
    expected = receipt['files']
    require(isinstance(expected, dict) and 1 <= len(expected) <= MAX_ENTRIES, 'invalid retained source file list')
    for name, checksum in expected.items():
        path = PurePosixPath(name)
        require(not path.is_absolute() and '..' not in path.parts and str(path) == name
                and isinstance(checksum, str) and len(checksum) == 64, 'invalid retained source file identity')
    paths = list(target.rglob('*'))
    require(not any(p.is_symlink() for p in paths), 'retained source contains a symlink')
    actual = {str(p.relative_to(target)) for p in paths if not p.is_dir()}
    require(actual == set(expected), 'retained source file set changed ('+
            str(len(actual-set(expected)))+' unexpected, '+str(len(set(expected)-actual))+' missing)')
    for name, checksum in expected.items():
        path = target/name
        require(not path.is_symlink() and path.is_file() and sha256(path) == checksum,
                'retained source file checksum changed')


def retain_diagnostic_bytecode(receipt, target, source, checksum, recovery, verify_lock):
    """Retain compiler-proved import caches outside a failed recipe's source.

    The caller must first prove its current, checksum-bound rollback repair
    intent. Payload files and the original source receipt are never rewritten.
    Unfinished retention refuses; ordinary source verification remains strict.
    """
    verify_lock()
    require(set(recovery) == {'transaction', 'recipe_sha256', 'repair_source_sha'} and
            re.fullmatch(r'transparent-schema-[A-Za-z0-9-]+', recovery['transaction']) and
            re.fullmatch('[0-9a-f]{64}', recovery['recipe_sha256']) and
            re.fullmatch('[0-9a-f]{40}', recovery['repair_source_sha']) and
            recovery['repair_source_sha'] != source, 'invalid bytecode retention repair binding')
    target = Path(target)
    require(target.name == source and re.fullmatch('[0-9a-f]{40}', source) and
            not target.is_symlink() and target.resolve() == target, 'invalid original source namespace')
    retained = target.parent.parent/'diagnostic-bytecode'/(recovery['transaction']+'-'+source)
    context = {**recovery, 'source_sha': source, 'archive_sha256': checksum}
    if retained.exists():
        require(not retained.is_symlink() and (retained/'complete.json').is_file(),
                'unfinished bytecode retention requires reconciliation')
        done = json.loads((retained/'complete.json').read_bytes())
        require(done['context'] == context, 'bytecode retention owner differs')
        for name, item in done['files'].items():
            require(sha256(retained/'files'/name) == item['sha256'], 'retained bytecode changed')
        verify_receipt(receipt, target, source, checksum)
        return done
    expected = receipt['files']
    paths = list(target.rglob('*'))
    require(not any(p.is_symlink() for p in paths), 'retained source contains a symlink')
    actual = {str(p.relative_to(target)) for p in paths if not p.is_dir()}
    extra = sorted(actual-set(expected))
    if not extra:
        verify_receipt(receipt, target, source, checksum)
        return None
    require(1 <= len(extra) <= 16 and set(expected) <= actual, 'source drift is not bounded import bytecode')
    files = {}
    for name in extra:
        p = target/name
        info = p.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= 1 << 20,
                'unexpected source entry is not bounded regular bytecode')
        relative = PurePosixPath(name)
        require(relative.parent.name == '__pycache__', 'unexpected source entry is not import bytecode')
        match = re.fullmatch(r'([A-Za-z_][A-Za-z0-9_]*)\.'+re.escape(sys.implementation.cache_tag)+r'\.pyc', relative.name)
        require(match is not None, 'unexpected bytecode interpreter or optimization')
        original = str(relative.parent.parent/(match.group(1)+'.py'))
        require(original in expected, 'bytecode has no reviewed source payload')
        raw = p.read_bytes()
        require(len(raw) >= 16 and raw[:4] == importlib.util.MAGIC_NUMBER and
                marshal.loads(raw[16:]) == compile((target/original).read_bytes(), str(target/original),
                                                   'exec', dont_inherit=True, optimize=0),
                'bytecode does not compile the reviewed source')
        files[name] = {'sha256': hashlib.sha256(raw).hexdigest(), 'size': info.st_size,
                       'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid,
                       'device': info.st_dev, 'inode': info.st_ino, 'mtime_ns': info.st_mtime_ns}
    # Verify every payload and reject all other entries before any relocation.
    verify_receipt({**receipt, 'files': {**expected, **{n:i['sha256'] for n,i in files.items()}}},
                   target, source, checksum)
    retained.parent.mkdir(mode=0o700, exist_ok=True)
    require(not retained.parent.is_symlink() and retained.parent.stat().st_dev == target.stat().st_dev,
            'bytecode retention must use the original filesystem')
    retained.mkdir(mode=0o700)
    intent = {'context': context, 'files': files}
    atomic_json(retained/'intent.json', intent)
    for name, item in files.items():
        verify_lock()
        p = target/name
        require(sha256(p) == item['sha256'] and p.stat().st_ino == item['inode'],
                'bytecode changed during retention')
        destination = retained/'files'/name
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.rename(p, destination)
    verify_receipt(receipt, target, source, checksum)
    verify_lock()
    atomic_json(retained/'complete.json', intent)
    return intent


def stage(request, lock, incoming, root=STAGE_ROOT, *, fleet_proof=None):
    source, checksum = request['source_sha'], request['sha256']
    target = root/'sources'/source
    receipt_path = root/'staging'/(source+'.json')
    lock.verify()
    if request['mode'] == 'status':
        if not receipt_path.exists():
            return {'status': 'absent', 'source_sha': source}
        receipt = json.loads(receipt_path.read_text())
        if receipt.get('status') == 'staged':
            verify_receipt(receipt, target, source, checksum)
        return {'status': receipt['status'], 'source_sha': receipt['source_sha'],
                'archive_sha256': receipt['archive_sha256'], 'path': str(target),
                'verified_files': len(receipt.get('files', {})) if receipt.get('status') == 'staged' else 0}
    headroom(root)
    if target.exists() or receipt_path.exists():
        require(target.is_dir() and not target.is_symlink() and receipt_path.is_file(), 'incomplete retained source staging')
        receipt = json.loads(receipt_path.read_text())
        verify_receipt(receipt, target, source, checksum)
        return {'status': 'staged', 'source_sha': source, 'archive_sha256': checksum, 'path': str(target)}
    if request['mode'] == 'preflight':
        return {'status': 'preflight-passed', 'source_sha': source, 'archive_sha256': checksum, 'path': str(target)}
    require(request['mode'] == 'stage', 'unsupported source stage mode')
    lock.verify()
    (root/'sources').mkdir(parents=True, exist_ok=True, mode=0o700)
    (root/'staging').mkdir(parents=True, exist_ok=True, mode=0o700)
    receipt = {'version': 1, 'status': 'receiving', 'source_sha': source,
               'archive_sha256': checksum, 'started_unix': time.time(), 'pid': os.getpid(),'machine_id':request['machine_id']}
    # Kernel identity and cross-host guard are durable before archive bytes.
    if fleet_proof is not None:
        current=_BOOTSTRAP_FLEET.S.process(os.getpid())
        require(current is not None,'source receiver kernel identity missing')
        receipt.update(process_start=current['start_ticks'],boot_id=_BOOTSTRAP_FLEET.S.boot_id(),
                       fleet=fleet_proof)
    atomic_json(receipt_path, receipt)  # Intent before receiving or extracting.
    temporary = Path(tempfile.mkdtemp(dir=root, prefix='.source-'))
    receipt['temporary']=str(temporary);atomic_json(receipt_path,receipt)
    try:
        archive, extraction = temporary/'source.tar.gz', temporary/'extract'
        with archive.open('xb') as stream:
            size, digest = 0, hashlib.sha256()
            for chunk in iter(lambda: incoming.read(1 << 20), b''):
                size += len(chunk)
                require(size <= MAX_COMPRESSED, 'compressed source archive exceeds bound')
                lock.verify()
                stream.write(chunk)
                digest.update(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        require(digest.hexdigest() == checksum, 'received source archive checksum differs')
        extraction.mkdir(mode=0o700)
        files = unpack(archive, extraction, source, lock)
        # Persist directory entries as well as file contents before publishing
        # the immutable source directory and its completed receipt.
        directories = [extraction, *(p for p in extraction.rglob('*') if p.is_dir())]
        for directory in sorted(directories, key=lambda p: len(p.parts), reverse=True):
            lock.verify()
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        lock.verify()
        headroom(root)
        require(not target.exists(), 'source target appeared during extraction')
        # Retain the independently hashed file set before either promotion rename.
        receipt.update(files=files,compressed_bytes=size)
        atomic_json(receipt_path,receipt)
        os.rename(extraction, target)
        fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        # Retain the exact reviewed export for coordinator-to-worker bootstrap;
        # no separately uploaded archive or reconstruction is needed.
        retained_archive=root/'staging'/(source+'.tar.gz')
        require(not retained_archive.exists(), 'retained source archive already exists')
        os.chmod(archive,0o400)
        os.rename(archive,retained_archive)
        fd=os.open(retained_archive.parent,os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)
        receipt.update(status='staged', files=files, compressed_bytes=size, completed_unix=time.time(),
                       retained_archive=str(retained_archive))
        atomic_json(receipt_path, receipt)
        return {'status': 'staged', 'source_sha': source, 'archive_sha256': checksum, 'path': str(target)}
    except BaseException as error:
        # If a crash happened after rename, retain that source and the unfinished
        # receipt; a retry cannot silently accept or overwrite it.
        receipt.update(status='failed', error_type=type(error).__name__)
        atomic_json(receipt_path, receipt)
        raise
    finally:
        if receipt.get('status')=='staged':shutil.rmtree(temporary)


def reconcile_source(request,lock,proof,root=STAGE_ROOT):
    """Retain an exact failed receipt; never remove partial archive bytes."""
    source=request['source_sha'];path=root/'staging'/(source+'.json');target=root/'sources'/source
    lock.verify()
    require(all(not p.is_symlink() for p in (root,*root.parents)), 'source recovery root contains a link')
    if not path.exists():
        require(not target.exists(),'source target has no owner receipt')
        return {'status':'reconciled','source_sha':source,'retained':[]}
    require(not path.is_symlink() and path.stat().st_size<=1<<20,'source receipt invalid')
    receipt=json.loads(path.read_text())
    require(receipt.get('source_sha')==source and receipt.get('archive_sha256')==request['sha256'],
            'source reconciliation identity differs')
    if receipt.get('status')=='staged':
        verify_receipt(receipt,target,source,request['sha256'])
        return {'status':'reconciled','source_sha':source,'retained':[str(path)]}
    require(receipt.get('status') in ('receiving','failed'),'source receipt does not need reconciliation')
    require(type(receipt.get('pid')) is int and receipt['pid']>0 and
            type(receipt.get('process_start')) is int and receipt['process_start']>0 and
            isinstance(receipt.get('boot_id'),str) and
            re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',receipt['boot_id']),
            'source receiver kernel identity is incomplete')
    current=_BOOTSTRAP_FLEET.S.process(receipt['pid'])
    require(current is None or current['state']=='Z' or current['start_ticks']!=receipt['process_start'] or
            receipt['boot_id']!=_BOOTSTRAP_FLEET.S.boot_id(),'source receiver remains alive; nothing was signalled')
    lease=proof['lease_request_sha256']
    require(isinstance(lease,str) and re.fullmatch('[0-9a-f]{64}',lease),'invalid source recovery lease')
    abandoned=target.with_name(source+'.abandoned-'+lease)
    archive=root/'staging'/(source+'.tar.gz')
    saved_archive=archive.with_name(source+'.abandoned-'+lease+'.tar.gz')
    intent=path.with_name(source+'.recovery-'+lease+'.json')
    # A promoted directory is still inert source. Verify its exact committed file
    # set before quarantining it; never adopt a failed promotion as staged.
    for candidate in (target,abandoned):
        require(not candidate.is_symlink(),'source recovery namespace contains a link')
        if candidate.exists():
            require(candidate.is_dir() and candidate.resolve()==candidate,'invalid source recovery directory')
            require(isinstance(receipt.get('files'),dict),'promoted source lacks pre-promotion file evidence')
            verify_receipt(dict(receipt,status='staged'),candidate,source,request['sha256'])
    require(not (target.exists() and abandoned.exists()),'both source recovery directories exist')
    for candidate in (archive,saved_archive):
        require(not candidate.is_symlink(),'source recovery archive contains a link')
        if candidate.exists():
            info=candidate.stat()
            require(candidate.is_file() and info.st_nlink==1 and info.st_size<=MAX_COMPRESSED and
                    sha256(candidate)==request['sha256'],'source recovery archive differs')
    require(not (archive.exists() and saved_archive.exists()),'both source recovery archives exist')
    binding={'source_sha':source,'archive_sha256':request['sha256'],'lease_request_sha256':lease,
             'receipt_sha256':sha256(path),'directory_present':target.exists() or abandoned.exists(),
             'archive_present':archive.exists() or saved_archive.exists()}
    if intent.exists():
        require(not intent.is_symlink() and intent.stat().st_size<=1<<20 and
                json.loads(intent.read_text())==binding,'source recovery intent differs')
    else:
        lock.verify();atomic_json(intent,binding)
    for original,saved in ((target,abandoned),(archive,saved_archive)):
        if original.exists():
            require(original.parent.stat().st_dev==original.stat().st_dev,'source recovery filesystem differs')
            lock.verify();os.rename(original,saved)
            fd=os.open(original.parent,os.O_RDONLY)
            try:os.fsync(fd)
            finally:os.close(fd)
    destination=path.with_name(source+'.abandoned-'+proof['lease_request_sha256']+'.json')
    require(not destination.exists(),'source failure already retained')
    lock.verify();os.rename(path,destination)
    fd=os.open(path.parent,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)
    return {'status':'reconciled','source_sha':source,'retained':[str(destination)],
            'partial':receipt.get('temporary'),'recovery_intent':str(intent),
            'quarantined_source':str(abandoned) if abandoned.exists() else None,
            'quarantined_archive':str(saved_archive) if saved_archive.exists() else None}


def main():
    request = json.loads(sys.argv[1])
    preflight_surveys = {}
    try:
        require(set(request)-{'recovery','coordinator_fleet','guard_attempt'} ==
                {'mode','source_sha','sha256','machine_id'} and
                not ('recovery' in request and 'coordinator_fleet' in request), 'invalid source stage request')
        require(type(request.get('guard_attempt',1)) is int and 1<=request.get('guard_attempt',1)<=100,
                'invalid source fleet attempt')
        require(request['mode'] in ('preflight', 'stage', 'status','reconcile'), 'invalid source stage mode')
        for field, size in [('source_sha', 40), ('sha256', 64), ('machine_id', 32)]:
            require(isinstance(request[field], str) and re.fullmatch('[0-9a-f]{'+str(size)+'}', request[field]),
                    'invalid source stage identity')
        config = {'type': 'pinned_host', 'machine_id': request['machine_id']}
        # PinnedHostLock comes from the exact shared hostlock source prefix.
        lock = PinnedHostLock(config, path=LOCK_PATH)
        if request['mode'] in ('stage','reconcile'):
            with lock:
                if request['mode']=='stage':local_schema_fence(recovery=request.get('recovery'))  # Only checksum-bound failed-transaction source repair.
                if 'coordinator_fleet' in request:
                    proof=request['coordinator_fleet']
                    _BOOTSTRAP_FLEET.require(isinstance(proof,dict) and proof.get('schema')==_BOOTSTRAP_FLEET.KIND and
                        proof.get('inventory_sha256')==_BOOTSTRAP_FLEET.INVENTORY_SHA and
                        proof.get('hosts')==sorted(_BOOTSTRAP_FLEET.PINS) and
                        isinstance(proof.get('nonce'),str) and re.fullmatch('[0-9a-f]{48}',proof['nonce']) and
                        proof.get('lease_request_sha256')==_BOOTSTRAP_FLEET.lease_id(request) and
                        proof.get('request_sha256')==hashlib.sha256(_BOOTSTRAP_FLEET.S.canonical(
                            {k:v for k,v in dict(request,mode='stage').items() if k!='coordinator_fleet'})).hexdigest() and
                        isinstance(proof.get('owner'),dict) and
                        type(proof['owner'].get('pid')) is int and proof['owner']['pid']>0 and
                        type(proof['owner'].get('start_ticks')) is int and proof['owner']['start_ticks']>0 and
                        isinstance(proof['owner'].get('boot_id'),str) and
                        re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',proof['owner']['boot_id']) and
                        _BOOTSTRAP_FLEET.S.number(proof.get('observed_unix')) and
                        0<=time.time()-proof['observed_unix']<=_BOOTSTRAP_FLEET.SECONDS,
                        'source target lacks fresh coordinator fleet ownership')
                    host=next((h for h,pin in _BOOTSTRAP_FLEET.PINS.items() if pin==request['machine_id']),None)
                    binding={'request_sha256':proof['request_sha256'],'nonce':proof['nonce'],'host':host,'skip':None}
                    current=_BOOTSTRAP_FLEET.S.process(os.getpid())
                    holder={'pid':current['pid'],'start_ticks':current['start_ticks']}
                    local=_BOOTSTRAP_FLEET.local(binding,host,holder=holder,deadline=time.monotonic()+60,
                            source_skip=request if request['mode']=='reconcile' else None)
                    _BOOTSTRAP_FLEET.verify(local,binding,host,holder)
                else:
                    require(request['machine_id']==_BOOTSTRAP_FLEET.PINS['coordinator'],
                            'source bootstrap requires retained coordinator identity')
                    def operation(proof):
                        return reconcile_source(request,lock,proof) if request['mode']=='reconcile' else \
                               stage(request,lock,sys.stdin.buffer,fleet_proof=proof)
                    runner=_BOOTSTRAP_FLEET.reconciled if request['mode']=='reconcile' else _BOOTSTRAP_FLEET.leased
                    result=runner(request,lock,verify_receipt,_BOOTSTRAP_REMOTE_CODE,operation,atomic_json)
                if 'coordinator_fleet' in request:
                    result=reconcile_source(request,lock,proof) if request['mode']=='reconcile' else \
                           stage(request,lock,sys.stdin.buffer,fleet_proof=proof)
        else:
            require(os.geteuid() == 0 and lock.MACHINE_ID.read_text().strip() == request['machine_id'],
                    'source staging is not on the pinned root coordinator')
            # Read-only preflight/status must not even create a lock file.
            if request.get('recovery') is not None:
                local_schema_fence(recovery=request['recovery'])
            if request['mode']=='preflight' and 'coordinator_fleet' not in request:
                def retain_preflight(host, raw):
                    # Read-only preflight keeps replies in memory, never on a host.
                    # Hex preserves exact bytes, including invalid or truncated JSON.
                    preflight_surveys[host] = {'bytes':len(raw), 'truncated':len(raw)>_BOOTSTRAP_FLEET.MAX_REPLY,
                        'hex':raw[:_BOOTSTRAP_FLEET.MAX_REPLY].hex()}
                proof=_BOOTSTRAP_FLEET.fleet(request,None,verify_receipt,_BOOTSTRAP_REMOTE_CODE,
                                       recovery=request.get('recovery'), retain=retain_preflight)
            else:proof=request.get('coordinator_fleet')
            result = stage(request, PinnedHostLock(None), sys.stdin.buffer)
            if request['mode']=='preflight':result['fleet']=proof
        if request['mode']=='status' and request['machine_id']==_BOOTSTRAP_FLEET.PINS['coordinator']:
            result['coordinator_owner']=_BOOTSTRAP_FLEET.status(request)
        print(json.dumps({'ok': True, 'result': result, 'preflight_surveys':preflight_surveys}))
    except Exception as error:
        # No raw paths, request arguments, stderr or archive contents in errors.
        message = str(error) if isinstance(error, SourceStageError) else type(error).__name__
        print(json.dumps({'ok': False, 'error': message, 'refusal_site': refusal_site(error),
                          'preflight_surveys':preflight_surveys}))


if __name__ == '__main__':
    main()
