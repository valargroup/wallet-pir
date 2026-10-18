"""Actual SQLite/raw-report fixtures, never production rollback qualification."""
import copy
from contextlib import closing
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'lib'),str(Path(__file__).resolve().parents[3]/'ops/lib')]
import activity_rollback_evidence as R
import test_activity_recovery_proof as F

class RawRollback(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);self.root=Path(t.name).resolve()
        f=F.RecoveryTests();f.setUp();self.addCleanup(f.doCleanups)
        f.change('UPDATE receives SET event=?',(F.receive(False),))
        f.change("UPDATE wallet_meta SET value=? WHERE key='set_identity'",(json.dumps({'shard_schema':R.SCHEMA,'genesis_hash':'b'*64}),))
        self.sample=copy.deepcopy(f.sample);self.sample['clients'][0]['expected_digest']=hashlib.sha256(F.receive(False)).hexdigest()
        self.binary=self.root/'reader';self.binary.write_bytes(b'fictional historical code')
        self.sample_path=self.root/'sample.json';self.write(self.sample_path,self.sample)
        self.source='a'*40;self.txn='transparent-schema-0';self.routing=self.root/self.txn/'routing';self.routing.mkdir(parents=True)
        self.input={'binary':str(self.binary),'binary_sha256':self.sha(self.binary),
                    'sample':str(self.sample_path),'sample_sha256':self.sha(self.sample_path)}
        self.spec={'source_sha':self.source,'routing':{'recovery':{'v10':self.input}}}
        self.original={'id':self.txn,'status':'rolled-back','events':[{'group':'rollback','name':name,'status':'passed',
            'exit_code':0,'started':start,'seconds':100} for name,start in (('verify-rollback',1000),('verify-service',2000))]}
        self.paths=[]
        def proof(prefix,query,filter_,start):
            directory=self.routing/(prefix+'123');(directory/'stores').mkdir(parents=True)
            store=directory/'stores/wallet.sqlite';shutil.copy2(f.path,store)
            checked=R.P.inspect_store(store,self.sample,R.SCHEMA)
            owner={'source_sha':self.source,'schema':R.SCHEMA,'binary_sha256':self.input['binary_sha256'],
                   'sample_sha256':self.input['sample_sha256'],'started':start+1,
                   'command':R.command(str(self.binary),str(self.sample_path),query,filter_,directory,self.source)}
            native={'source_sha':self.source,'in_progress':False,'stop_reason':None,'sample':{'sha256':self.input['sample_sha256']},
                    'set':{'schema':R.SCHEMA,'genesis_hash':self.sample['genesis_hash'],'target_height':self.sample['anchor_height'],
                           'target_hash':self.sample['anchor_hash']},
                    'steps':[{'syncs':1,'completed':1,'exact':1,'failed':0,
                              'classes':{'cutover-active':{'n':1,'completed':1,'exact':1,'failed':0,'incomplete_by_reason':{}}}}]}
            self.write(directory/'owner.json',owner);self.write(directory/'native.json',native)
            result={'status':'passed','exit_code':0,'schema':R.SCHEMA,'source_sha':self.source,
                    'binary_sha256':self.input['binary_sha256'],'sample_sha256':self.input['sample_sha256'],
                    'finished':start+4,'observations':[checked],'native_report_sha256':self.sha(directory/'native.json')}
            self.write(directory/'result.json',result);self.paths.append(directory)
            return {'result':str(directory/'result.json'),'sha256':self.sha(directory/'result.json'),
                    'query_origin':query,'filter_origin':filter_}
        loop='http://127.0.0.1:18193'
        private=proof('v10-recovery-',loop,loop,1000)
        public=[proof('v10-public-recovery-'+str(i)+'-',origin,R.ORIGINS[1-i],2000) for i,origin in enumerate(R.ORIGINS)]
        common={'source_sha':self.source,'transaction':self.txn,'kind':'v10','sample_sha256':self.input['sample_sha256']}
        self.write(self.routing/'verified-v10.json',dict(common,verified_unix=1005,
                    recovery_result=private['result'],recovery_sha256=private['sha256']))
        self.write(self.routing/'verified-public-v10.json',dict(common,verified_unix=2005,recoveries=public))
        for p in (patch.object(R,'ROOT',self.root),patch.object(R,'READER',str(self.binary))):
            p.start();self.addCleanup(p.stop)

    def sha(self,path):return hashlib.sha256(path.read_bytes()).hexdigest()
    def write(self,path,value):path.write_text(json.dumps(value)+'\n')
    def verify(self):return R.verify(self.original,self.spec)
    def mutate(self,path,change):
        value=json.loads(path.read_text());change(value);self.write(path,value)
        if path.name=='result.json':
            index=self.paths.index(path.parent)
            record=self.routing/('verified-v10.json' if index==0 else 'verified-public-v10.json')
            if index==0:self.mutate(record,lambda v:v.update(recovery_sha256=self.sha(path)))
            else:self.mutate(record,lambda v:v['recoveries'][index-1].update(sha256=self.sha(path)))

    def test_private_both_canonical_reports_and_independent_legacy_sqlite_pass(self):
        value=self.verify();self.assertEqual(value['transaction'],self.txn);self.assertEqual(len(value['proofs']),3)
        self.assertTrue(all(p['observations'][0]['events']==1 and not p['observations'][0]['metadata_available'] for p in value['proofs']))
        self.assertEqual([p['query_origin'] for p in value['proofs'][1:]],list(R.ORIGINS))

    def test_missing_or_duplicate_canonical_origin_refuses(self):
        p=self.routing/'verified-public-v10.json';before=p.read_bytes()
        for change in (lambda v:v['recoveries'].pop(),lambda v:v['recoveries'][1].update(query_origin=R.ORIGINS[0]),
                       lambda v:v.update(transaction='transparent-schema-foreign'),lambda v:v.update(verified_unix=9999)):
            self.mutate(p,change)
            with self.assertRaises(ValueError):self.verify()
            p.write_bytes(before)

    def test_retained_raw_native_or_owner_cannot_be_replaced_or_foreign(self):
        for name,change in (('native.json',lambda v:v.update(source_sha='f'*40)),
                            ('owner.json',lambda v:v['command'].append('--foreign')),
                            ('result.json',lambda v:v.update(finished=5000))):
            p=self.paths[0]/name;before=p.read_bytes();self.mutate(p,change)
            with self.assertRaises(ValueError):self.verify()
            p.write_bytes(before)
            if name=='result.json':self.mutate(p,lambda v:None)

    def test_sqlite_changes_sidecars_and_links_refuse_without_writes(self):
        p=self.paths[0]/'stores/wallet.sqlite';before=p.read_bytes()
        with closing(sqlite3.connect(p)) as db,db:db.execute('DELETE FROM coverage')
        with self.assertRaises(ValueError):self.verify()
        p.write_bytes(before)
        sidecar=Path(str(p)+'-wal');sidecar.write_bytes(b'fixture')
        with self.assertRaises(ValueError):self.verify()
        self.assertEqual(sidecar.read_bytes(),b'fixture');sidecar.unlink()
        saved=p.with_suffix('.saved');p.rename(saved);p.symlink_to(saved)
        with self.assertRaises(ValueError):self.verify()

    def test_binary_sample_path_escape_budget_and_deadline_refuse(self):
        before=self.binary.read_bytes();self.binary.write_bytes(b'changed')
        with self.assertRaises(ValueError):self.verify()
        self.binary.write_bytes(before)
        with patch.object(R,'BYTES',1):
            with self.assertRaises(ValueError):self.verify()
        with patch.object(R,'SECONDS',0):
            with self.assertRaises(ValueError):self.verify()
        self.mutate(self.routing/'verified-v10.json',lambda v:v.update(recovery_result=str(self.root/'elsewhere/result.json')))
        with self.assertRaises(ValueError):self.verify()

if __name__=='__main__':unittest.main()
