"""Independent, read-only reinspection of retained successful v10 rollback.

This is rollback evidence only. The historical reader cannot qualify c3.
All paths derive from the validated product spec and original journal, never
from caller-selected filenames. Complete immutable stores are opened without
SQLite writes and rechecked against the retained sample and raw native report.
"""
import hashlib
import json
from pathlib import Path
import re
import stat
import time
import activity_recovery_proof as P
from wallet_pir_ops import ancillary_baseline as A

ROOT=Path('/srv/transparent-activity/ops/schema')
ORIGINS=('https://transparent-pir.valargroup.dev','https://enhance-pir.valargroup.dev')
READER='/srv/transparent-activity/build/evidence/release-12ce12918446eaa56e2d766ec2f43d82c531abb9/artifacts/transparent-loadtest'
SCHEMA='transparent-shard-v10'
SECONDS=60
FILES=1024
BYTES=2<<30
STORE_BYTES=128<<20


def require(ok,message):
    if not ok:raise ValueError(message)


def no_links(path):
    require(all(not p.is_symlink() for p in (path,*path.parents)),'rollback evidence contains a link')


def unique(pairs):
    value={}
    for k,v in pairs:
        require(k not in value,'duplicate rollback evidence field');value[k]=v
    return value


def command(binary,sample,query,filter_,directory,source):
    return [binary,'--shard-url',query,'--filter-url',filter_,'--sample',sample,
            '--steps','1','--step-duration','30s','--min-completed-per-class','1',
            '--max-error-rate','0','--max-503-rate','0','--http-attempts','1','--timeout','60s',
            '--store-dir',str(directory/'stores'),'--retain-stores','--json-out',str(directory/'native.json'),
            '--run-id',directory.name,'--source-sha',source]


def verify(original,spec):
    end=time.monotonic()+SECONDS;files={};used=0
    def tick():require(time.monotonic()<end,'rollback evidence inspection deadline exceeded')
    def file(path,limit):
        nonlocal used
        tick();path=Path(path);no_links(path)
        info=path.stat();require(stat.S_ISREG(info.st_mode) and info.st_size<=limit,'rollback evidence file exceeds bound')
        if str(path) not in files:
            used+=info.st_size;require(len(files)<FILES and used<=BYTES,'rollback evidence inventory exceeds bound')
        digest=A.hashed(path,limit,tick);files[str(path)]={'bytes':info.st_size,'sha256':digest}
        return digest
    def load(path):
        path=Path(path);file(path,8<<20)
        return json.loads(path.read_bytes(),object_pairs_hook=unique)
    require(original.get('status')=='rolled-back' and re.fullmatch('transparent-schema-[A-Za-z0-9-]{1,128}',original['id']),
            'rollback evidence requires a terminal original transaction')
    root=ROOT/original['id']/'routing'
    input_=spec['routing']['recovery']['v10'];source=spec['source_sha']
    require(input_['binary']==READER,'rollback must retain the historical reader')
    sample_path=Path(input_['sample'])
    require(file(input_['binary'],A.MAX_EXE)==input_['binary_sha256'] and
            file(sample_path,8<<20)==input_['sample_sha256'],'rollback reader or sample changed')
    sample=load(sample_path)
    require(sample.get('clients') and all(c['journal_events']>0 for c in sample['clients']),
            'rollback sample must be nonempty')
    phases={e['name']:e for e in original['events'] if e['group']=='rollback'}
    for name in ('verify-rollback','verify-service'):
        require(phases[name]['status']=='passed' and phases[name]['exit_code']==0,'rollback verification phase did not pass')
    def evidence(path,sha,query,filter_,phase):
        path=Path(path)
        prefix='v10-recovery-' if query=='http://127.0.0.1:18193' else 'v10-public-recovery-'+str(ORIGINS.index(query))+'-'
        require(path.name=='result.json' and path.parent.parent==root and
                re.fullmatch(re.escape(prefix)+'[0-9]+',path.parent.name),'rollback result escaped its original routing namespace')
        require(file(path,8<<20)==sha,'retained rollback result changed')
        result=load(path);directory=path.parent
        owner=load(directory/'owner.json');native=load(directory/'native.json')
        require(result.get('status')=='passed' and result.get('exit_code')==0 and result.get('schema')==SCHEMA and
                all(result.get(k)==v for k,v in (('source_sha',source),('binary_sha256',input_['binary_sha256']),
                                                ('sample_sha256',input_['sample_sha256']))),
                'rollback result identity or outcome differs')
        require(owner.get('source_sha')==source and owner.get('schema')==SCHEMA and
                owner.get('binary_sha256')==input_['binary_sha256'] and owner.get('sample_sha256')==input_['sample_sha256'] and
                owner.get('command')==command(input_['binary'],input_['sample'],query,filter_,directory,source),
                'rollback native owner or canonical command differs')
        require(phase['started']<=owner['started']<=result['finished']<=phase['started']+phase['seconds']+2,
                'rollback proof is outside its original phase')
        require(result.get('native_report_sha256')==files[str(directory/'native.json')]['sha256'] and
                native.get('in_progress') is False and native.get('stop_reason') is None and native.get('source_sha')==source and
                native.get('set',{}).get('schema')==SCHEMA and native['set'].get('genesis_hash')==sample['genesis_hash'] and
                native['set'].get('target_height')==sample['anchor_height'] and native['set'].get('target_hash')==sample['anchor_hash'] and
                native.get('sample',{}).get('sha256')==input_['sample_sha256'],'rollback raw native identity or completion differs')
        steps=native.get('steps',[])
        require(len(steps)==1,'rollback needs one complete native step');step=steps[0]
        require(type(step.get('syncs')) is int and step['syncs']>0 and
                step['syncs']==step.get('completed')==step.get('exact') and step.get('failed')==0,
                'rollback native step has incomplete or inexact syncs')
        classes=step.get('classes',{})
        require(set(classes)=={c['class'] for c in sample['clients']} and
                all(type(c.get('n')) is int and c['n']>0 and c['n']==c.get('completed')==c.get('exact') and
                    c.get('failed')==0 and c.get('incomplete_by_reason')=={} for c in classes.values()),
                'rollback native classes are incomplete')
        stores=directory/'stores';no_links(stores)
        found=[];nodes=0
        for p in stores.rglob('*'):
            nodes+=1
            require(nodes<=FILES and len(p.relative_to(stores).parts)<=8,'rollback store walk exceeds bound')
            tick();no_links(p)
            require(len(found)<256 and (p.is_dir() or stat.S_ISREG(p.stat().st_mode)), 'rollback store inventory exceeds bound')
            if p.is_file():
                require(p.suffix=='.sqlite','unexpected retained rollback store sidecar or file')
                file(p,STORE_BYTES);found.append(p)
        require(found and len(found)==len(result.get('observations',[])),'rollback retained store observations differ')
        checked=[P.inspect_store(p,sample,SCHEMA,deadline=end) for p in sorted(found)]
        require(checked==result['observations'],'independent rollback SQLite reinspection differs')
        return {'result':str(path),'result_sha256':sha,'query_origin':query,'filter_origin':filter_,
                'observations':checked,'started_unix':owner['started'],'finished_unix':result['finished']}
    private=load(root/'verified-v10.json');public=load(root/'verified-public-v10.json')
    for value,name in ((private,'verify-rollback'),(public,'verify-service')):
        e=phases[name]
        require(value.get('transaction')==original['id'] and value.get('source_sha')==source and value.get('kind')=='v10' and
                value.get('sample_sha256')==input_['sample_sha256'] and e['started']<=value['verified_unix']<=e['started']+e['seconds']+2,
                'rollback routing proof is stale or foreign')
    proofs=[evidence(private['recovery_result'],private['recovery_sha256'],'http://127.0.0.1:18193',
                     'http://127.0.0.1:18193',phases['verify-rollback'])]
    refs=public.get('recoveries',[])
    require(len(refs)==2 and [v.get('query_origin') for v in refs]==list(ORIGINS) and
            [v.get('filter_origin') for v in refs]==list(reversed(ORIGINS)),'rollback did not prove both canonical origins')
    proofs.extend(evidence(v['result'],v['sha256'],v['query_origin'],v['filter_origin'],phases['verify-service']) for v in refs)
    # The report, inputs and independent committed SQLite bytes must remain exact.
    for path,item in list(files.items()):
        require(file(path,max(8<<20,item['bytes']))==item['sha256'],'rollback evidence changed during inspection')
    tick()
    return {'kind':'independent-retained-rollback-v1','transaction':original['id'],'schema':SCHEMA,
            'proofs':proofs,'files':files,'observed_unix':time.time()}
