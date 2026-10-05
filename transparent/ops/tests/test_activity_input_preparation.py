"""Concrete assignment/unit preparation, distinct map identities and fencing."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[3]/'ops/lib'))
sys.path.insert(0,str(Path(__file__).parent))
from test_activity_input_stage import Lock
SPEC=importlib.util.spec_from_file_location('input_preparation_test',Path(__file__).parents[1]/'lib/activity_input_stage.py')
M=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(M)


def mapping():
    return {'genesis_hash':'a'*64,'network':'main','profile':'zcash-transparent-range-v2','range_envelope_version':2,
            'start_height':0,'seal':{'recent':{'max_scripts':1,'max_page_rows':1,'max_txids':0}},'shards':[
            {'shard_id':0,'geometry':'recent-4k-8k','start_height':0,'end_height':3500738,'parent_block_hash':'0'*64,
             'terminal_block_hash':'b'*64,'filter_hash':'c'*64,'scripts':1,'page_rows':1,'txids':0,
             'directory_segments':1,'page_segments':1,'manifest_digest':'d'*64,'revision':0,'sealed':False}]}


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
        hosts={};workers=[]
        for n,role in enumerate(('recent-replica','recent-replica','archive-owner'),1):
            id='worker-'+str(n);host='host-'+str(n)
            hosts[host]={'machine_id':str(n)*32,'address':'10.142.0.'+str(n),'user':'root'}
            workers.append({'host':host,'id':id,'role':role,'replica_group':'recent' if n<3 else None,
                'upstream':'10.142.0.'+str(n)+':8093','cache_bytes':(5 if n<3 else 48)<<30,
                'unit':'[Service]\nExecStart=/usr/local/bin/transparent-shard-server --worker-id '+id+
                ' --shard-dir /srv/old --assignment /srv/old/assignment.json --cache-bytes 1\nMemoryMax=64G\n'})
        self.inventory=SimpleNamespace(hosts=hosts,ssh={'mode':'config'},lock={'type':'pinned_host','machine_id':'9'*32},services={})
        self.request={'version':1,'source_sha':'e'*40,'release_result_sha256':'f'*64,'created_unix':1790833000,'attempt':1,'workers':workers}
        self.identifier=M.digest(self.request)
        self.prepared=self.root/'prepared';self.owners=self.root/'owners'
        self.lock=Lock(self.root/'lock')
        for thing,name,value in [(M,'PREPARED',self.prepared),(M,'OWNERS',self.owners),
             (M,'ProductionLock',lambda _:self.lock),(M.schema_fence,'local_schema_fence',lambda **_:None),
             (M,'resources',lambda *_:{})]:
            p=patch.object(thing,name,value);p.start();self.addCleanup(p.stop)
        self.job=M.Preparation(self.inventory,self.request,self.identifier)
        p=patch.object(self.job,'identity');p.start();self.addCleanup(p.stop)
        self.files={'assignment.json':b'assignment','inventory.json':b'inventory','worker-1.service':b'unit'}
        p=patch.object(self.job,'render',return_value=self.files);p.start();self.addCleanup(p.stop)
        self.job.executor=SimpleNamespace(probe_unit=lambda host,_:{'fragment_text':next(w['unit'] for w in workers if w['host']==host),
            'drop_ins':[],'need_daemon_reload':False,'active_state':'active','main_pid':1})

    def stage(self):return self.job.run('stage',M.digest(self.job.plan()))

    def test_plan_preflight_have_no_retained_writes_stage_is_immutable_and_checked(self):
        self.job.run('preflight');self.assertFalse(self.owners.exists());self.assertFalse(self.prepared.exists())
        record=self.stage();self.assertEqual(record['status'],'staged');self.assertEqual(record['pid'],os.getpid())
        self.assertEqual(self.job.status()['files'],record['files'])
        self.assertEqual((self.job.target/'assignment.json').stat().st_mode&0o777,0o400)
        with self.assertRaisesRegex(ValueError,'already exists'):self.stage()
        (self.job.target/'assignment.json').chmod(0o600);(self.job.target/'assignment.json').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'changed'):self.job.status()

    def test_unknown_failed_render_retains_owner_then_explicit_same_fs_reconcile(self):
        plan=self.job.plan()
        with patch.object(self.job,'render',side_effect=[self.files,subprocess.TimeoutExpired('planner',60)]),self.assertRaises(subprocess.TimeoutExpired):
            self.job.run('stage',M.digest(plan))
        self.assertEqual(self.job.status()['status'],'interrupted')
        self.assertEqual(json.loads((self.owners/'latest.json').read_text()),{'request_sha256':self.identifier})
        self.job.partial.mkdir(parents=True);(self.job.partial/'partial').write_bytes(b'preserve')
        self.job.run('reconcile')
        self.assertEqual((self.prepared/(self.identifier+'.preparing.abandoned')/'partial').read_bytes(),b'preserve')
        self.assertEqual(self.job.status()['status'],'reconciled')
        with self.assertRaises(ValueError):self.stage()

    def test_changed_plan_or_peer_unit_drop_ins_refuse_before_owner(self):
        with self.assertRaisesRegex(ValueError,'plan changed'):self.job.run('stage','0'*64)
        self.assertFalse(self.owners.exists())
        self.job.executor.probe_unit=lambda *_:{'fragment_text':'changed','drop_ins':['override'],'need_daemon_reload':False,'active_state':'active','main_pid':1}
        with self.assertRaisesRegex(ValueError,'drift'):self.stage()
        self.assertFalse(self.owners.exists())

    def test_request_shape_role_host_budget_and_public_upstream_refuse(self):
        for mutate in (lambda r:r.update(extra=1),lambda r:r['workers'][0].update(upstream='8.8.8.8:8093'),
                       lambda r:r['workers'][1].update(host='host-1'),lambda r:r['workers'][0].update(cache_bytes=True),
                       lambda r:r['workers'][2].update(role='recent-replica'),lambda r:r['workers'][0].update(unit='[Service]\nExecStart=/bin/sh\n')):
            bad=copy.deepcopy(self.request);mutate(bad)
            with self.assertRaises((ValueError,KeyError)):M.Preparation(self.inventory,bad,M.digest(bad))
        with self.assertRaises(ValueError):M.Preparation(self.inventory,self.request,'0'*64)

    def test_retained_native_failure_keeps_original_pid_exit_log_and_lock(self):
        self.job.retained_native=self.root/'native'
        with self.lock,M.lock_environment(self.lock):
            code='import os; os.fstat('+str(self.lock.fd)+'); print("retained native failure"); raise SystemExit(7)'
            with self.assertRaises(subprocess.CalledProcessError):self.job.native('plan',[sys.executable,'-c',code])
        result=json.loads((self.job.retained_native/'plan.result.json').read_text())
        owner=json.loads((self.job.retained_native/'plan.owner.json').read_text())
        self.assertEqual(result['pid'],owner['pid']);self.assertEqual(result['exit_code'],7)
        self.assertEqual(result['status'],'failed')
        self.assertIn('retained native failure',(self.job.retained_native/'plan.log').read_text())
        self.assertEqual((self.job.retained_native/'plan.owner.json').stat().st_mode&0o777,0o400)
        with self.assertRaisesRegex(ValueError,'already owned'):self.job.native('plan',[sys.executable,'-c','pass'])

    def test_native_renderer_keeps_file_and_protocol_identity_separate_and_48_gib_owner(self):
        self.job.render= M.Preparation.render.__get__(self.job)
        output=self.root/'output';output.mkdir();raw=json.dumps(mapping(),indent=2).encode();(output/'shards.json').write_bytes(raw)
        evidence=self.root/'evidence';evidence.mkdir()
        (evidence/'result.json').write_text(json.dumps({'status':'passed','map_sha256':hashlib.sha256(raw).hexdigest()}))
        (evidence/'publication.json').write_text(json.dumps({'published':{'recent_from':0}}))
        assignment={'set':{'map_sha256':M.transparent_map.served_sha256(mapping()),'shard_schema':'transparent-shard-v11'},
                    'generated_by':{'generated_at':'native now'},'workers':[],'unassigned':[]}
        calls=[]
        def native(argv,**kwargs):
            calls.append((argv,kwargs));self.assertEqual(kwargs['env']['PYTHONDONTWRITEBYTECODE'],'1')
            if 'plan' in argv:Path(argv[argv.index('--out-assignment')+1]).write_text(json.dumps(assignment))
            return SimpleNamespace(stdout=b'',returncode=0)
        tmpdir=tempfile.TemporaryDirectory
        with patch.object(M.P,'OUTPUT',output),patch.object(M.P,'EVIDENCE',evidence),patch.object(M.P,'verify_release'), \
             patch.object(M.subprocess,'run',side_effect=native), \
             patch.object(M.tempfile,'TemporaryDirectory',side_effect=lambda **_:tmpdir(dir=self.root)),self.lock,M.lock_environment(self.lock):
            files=self.job.render()
        kept=json.loads(files['assignment.json']);self.assertEqual(kept['set']['map_sha256'],M.transparent_map.served_sha256(mapping()))
        self.assertNotEqual(kept['set']['map_sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(kept['generated_by']['generated_at'],'2026-10-01T05:36:40Z')
        args=M.transparent_unit.exec_args(files['worker-3.service'].decode())
        self.assertIn('/srv/transparent-pir/v11/publications/'+hashlib.sha256(raw).hexdigest(),args)
        self.assertEqual(args[args.index('--cache-bytes')+1],str(48<<30))
        self.assertIn('/opt/transparent-publisher/v11/active.json',args)
        self.assertEqual(len(calls),2)


class MapIdentity(unittest.TestCase):
    def test_native_order_optional_null_and_sorted_seal_policies(self):
        value=mapping();expected=M.transparent_map.served_sha256(value)
        shuffled=json.loads(json.dumps(value,sort_keys=True,indent=2))
        shuffled['shards'][0]['txid_segments']=None
        self.assertEqual(M.transparent_map.served_sha256(shuffled),expected)
        shuffled['shards'][0]['txid_segments']=[1,2]
        self.assertNotEqual(M.transparent_map.served_sha256(shuffled),expected)
        shuffled['shards'][0]['txid_segments']=[True,2]
        with self.assertRaises(ValueError):M.transparent_map.served_bytes(shuffled)
        value['shards'][0]['future_field']=1
        with self.assertRaises(ValueError):M.transparent_map.served_bytes(value)


if __name__=='__main__':unittest.main()
