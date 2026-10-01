"""Streaming, file identity, interruption fences and native scope preparation."""
import copy
import hashlib
import importlib.util
import io
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
SPEC=importlib.util.spec_from_file_location('input_stage_test',Path(__file__).parents[1]/'lib/activity_input_stage.py')
M=importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(M)


class Lock:
    def __init__(self,path): self.path=path; self.fd=None
    def __enter__(self): self.fd=os.open(self.path,os.O_CREAT|os.O_RDWR,0o600); return self
    def __exit__(self,*_): os.close(self.fd); self.fd=None
    def verify(self): assert self.fd is not None
    def descriptors(self): self.verify(); return (self.fd,)


class Inputs(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve(); self.sources=self.root/'source';self.sources.mkdir()
        self.mapping=b'{}'; self.assignment=b'assignment'
        self.names={'shards.json':self.mapping,'assignment.json':self.assignment,
                    '.input-transparent-shard-server':b'bin','.input-shard-control':b'control',
                    '.input-transparent-shard-server.service':b'unit','a'*64+'/manifest.json':b'manifest',
                    'a'*64+'/pages-0.bin':b'page\x00data'}
        files=[]
        for name,data in sorted(self.names.items()):
            path=self.sources/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
            files.append({'path':name,'source':str(path),'size':len(data),'sha256':hashlib.sha256(data).hexdigest(),
                          'mode':M.INPUTS.get(name,0o600)})
        self.request={'version':1,'source_sha':'b'*40,'machine_id':'c'*32,'worker_id':'worker-1',
            'map_sha256':hashlib.sha256(self.mapping).hexdigest(),'assignment_sha256':hashlib.sha256(self.assignment).hexdigest(),
            'release_result_sha256':'d'*64,'attempt':1,'cache_bytes':1<<30,'files':files}
        self.lock=Lock(self.root/'lock')
        self.receiver=M.Receiver(self.request,root=self.root/'publication',owners=self.root/'owners',lock_factory=lambda:self.lock)
        for name,thing,value in [('resources',M,lambda *_: {}),('local_schema_fence',M.schema_fence,lambda **_:None)]:
            p=patch.object(thing,name,value);p.start();self.addCleanup(p.stop)
        self.native_calls=[]
        p=patch.object(self.receiver,'native',lambda lock:self.native_calls.append(lock.descriptors()))
        p.start();self.addCleanup(p.stop)

    def body(self): return b''.join(self.names[f['path']] for f in self.request['files'])

    def test_complete_stream_atomic_target_private_modes_and_receipt(self):
        result=self.receiver.stage(io.BytesIO(self.body()))
        self.assertEqual(result['status'],'staged');self.assertEqual(result['received_bytes'],len(self.body()))
        self.assertFalse(self.receiver.partial.exists());self.assertEqual(len(self.native_calls),1)
        M.verify_files(self.receiver.target,self.request)
        self.assertEqual(self.receiver.path.stat().st_mode&0o777,0o600)
        self.assertEqual((self.receiver.owners/(self.receiver.identifier+'.request.json')).stat().st_mode&0o777,0o400)
        self.assertEqual(self.receiver.status()['pid'],os.getpid())
        with self.assertRaisesRegex(ValueError,'already exists'):self.receiver.stage(io.BytesIO(self.body()))

    def test_truncated_extra_and_corrupt_stream_keep_failed_partial_and_no_target(self):
        for data in (self.body()[:-1],self.body()+b'extra',b'x'+self.body()[1:]):
            with self.subTest(data=data[:4]):
                root=self.root/hashlib.sha256(data).hexdigest()
                receiver=M.Receiver(self.request,root=root/'pub',owners=root/'owners',lock_factory=lambda:self.lock)
                with patch.object(receiver,'native') as native,self.assertRaises(ValueError):receiver.stage(io.BytesIO(data))
                native.assert_not_called();self.assertFalse(receiver.target.exists());self.assertTrue(receiver.partial.exists())
                self.assertEqual(receiver.status()['status'],'failed')
                with self.assertRaisesRegex(ValueError,'already exists'):receiver.stage(io.BytesIO(self.body()))

    def test_interrupted_native_is_retained_and_requires_explicit_reconciliation(self):
        with patch.object(self.receiver,'native',side_effect=subprocess.TimeoutExpired('native',1)),self.assertRaises(subprocess.TimeoutExpired):
            self.receiver.stage(io.BytesIO(self.body()))
        self.assertEqual(self.receiver.status()['status'],'interrupted')
        result=self.receiver.reconcile();self.assertEqual(result['status'],'reconciled')
        self.assertFalse(self.receiver.partial.exists());self.assertEqual(len(list(self.receiver.root.glob('*.abandoned-*'))),1)
        with self.assertRaises(ValueError):self.receiver.stage(io.BytesIO(self.body()))
        retry=copy.deepcopy(self.request);retry['attempt']=2
        second=M.Receiver(retry,root=self.receiver.root,owners=self.receiver.owners,lock_factory=lambda:self.lock)
        with patch.object(second,'native'):self.assertEqual(second.stage(io.BytesIO(self.body()))['status'],'staged')

    def test_crash_after_rename_retains_candidate_before_reconcile(self):
        self.receiver.stage(io.BytesIO(self.body()))
        record=json.loads(self.receiver.path.read_text());record['status']='receiving';M.durable.atomic_json(self.receiver.path,record,mode=0o600)
        self.receiver.reconcile();self.assertFalse(self.receiver.target.exists())
        self.assertEqual(len(list(self.receiver.root.glob('*.abandoned-*'))),1)

    def test_tampered_modes_bytes_extra_file_and_symlink_refuse_completed_status(self):
        self.receiver.stage(io.BytesIO(self.body()))
        path=self.receiver.target/'shards.json'
        path.chmod(0o644)
        with self.assertRaisesRegex(ValueError,'mode differs'):self.receiver.status()
        path.chmod(0o600);path.write_bytes(b'bad')
        with self.assertRaisesRegex(ValueError,'differs'):self.receiver.status()
        path.write_bytes(self.mapping);extra=self.receiver.target/'extra';extra.touch()
        with self.assertRaisesRegex(ValueError,'file set'):self.receiver.status()
        extra.unlink();extra.symlink_to(self.sources)
        with self.assertRaisesRegex(ValueError,'tree'):self.receiver.status()

    def test_parent_symlink_refuses_before_owner_or_writes(self):
        self.receiver.root.symlink_to(self.sources,target_is_directory=True)
        with self.assertRaisesRegex(ValueError,'symlink'):self.receiver.preflight()
        self.assertFalse(self.receiver.owners.exists())

    def test_request_boundaries_paths_modes_duplicates_and_file_directory_collision(self):
        for key,value in [('attempt',0),('cache_bytes',True),('map_sha256','f'*64),('version',2)]:
            req=copy.deepcopy(self.request);req[key]=value
            with self.assertRaises(ValueError):M.validate(req)
        for path in ('../escape','/etc/systemd/system/live.service','shards.json/child','a'*64+'/../escape','a'*64+'//pages.bin','.inputs/unknown'):
            req=copy.deepcopy(self.request);req['files'][-1]['path']=path
            with self.assertRaises(ValueError):M.validate(req)
        for field,value in [('size',True),('size',M.MAX_BYTES+1),('mode',0o777),('source','relative')]:
            req=copy.deepcopy(self.request);req['files'][-1][field]=value
            with self.assertRaises(ValueError):M.validate(req)
        req=copy.deepcopy(self.request);req['files'].append(req['files'][0])
        with self.assertRaises(ValueError):M.validate(req)

    def test_framing_binds_reviewed_request_and_rejects_duplicate_or_oversize_header(self):
        raw=M.durable.canonical(self.request)+b'\n'
        stream=io.BytesIO(raw+self.body());self.assertEqual(M.read_request(stream,M.digest(self.request)),self.request)
        self.assertEqual(stream.read(),self.body())
        for data in (raw[:-1],b'{"version":1,"version":1}\n',b'x'*(M.MAX_REQUEST+1)):
            with self.assertRaises(ValueError):M.read_request(io.BytesIO(data),M.digest(self.request))
        with self.assertRaises(ValueError):M.read_request(io.BytesIO(raw),'f'*64)

    def test_stream_rechecks_source_and_detects_change_without_archive_buffer(self):
        output=b''.join(M.chunks(self.request));self.assertEqual(output,M.durable.canonical(self.request)+b'\n'+self.body())
        first=Path(self.request['files'][0]['source']);first.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'changed during'):b''.join(M.chunks(self.request))

    def test_pipe_pump_joins_process_and_drains_errors_with_bounded_reply(self):
        program='import sys,json; h=json.loads(sys.stdin.buffer.readline()); n=sum(f["size"] for f in h["files"]); assert len(sys.stdin.buffer.read())==n; sys.stderr.write("private"); print("{}")'
        code,raw=M.pump([sys.executable,'-c',program],self.request,5)
        self.assertEqual(code,0);self.assertEqual(raw,b'{}\n')
        with self.assertRaises(subprocess.TimeoutExpired):M.pump([sys.executable,'-c','import time;time.sleep(5)'],self.request,.01)

    def test_coordinator_and_remote_input_fence_has_no_schema_pointer_shortcut(self):
        identity=M.digest(self.request);paths={str(M.schema_fence.INPUT_STAGING/'latest.json'):json.dumps({'request_sha256':identity}),
            str(M.schema_fence.INPUT_STAGING/(identity+'.json')):json.dumps({'request_sha256':identity,'status':'receiving'})}
        with self.assertRaisesRegex(ValueError,'unfinished input'):M.schema_fence.schema_mutation_fence(paths.get)
        M.schema_fence.schema_mutation_fence(paths.get,skip_input=identity)
        with self.assertRaises(ValueError):M.schema_fence.schema_mutation_fence(paths.get,skip_input='f'*64)
        for status in ('staged','reconciled'):
            paths[str(M.schema_fence.INPUT_STAGING/(identity+'.json'))]=json.dumps({'request_sha256':identity,'status':status})
            M.schema_fence.schema_mutation_fence(paths.get)

    def test_renderer_expands_native_digest_directories_and_keeps_unassigned_metadata(self):
        output=self.root/'full';output.mkdir()
        digest='a'*64
        folder=output/digest;folder.mkdir()
        for name,data in [('manifest.json',b'm'),('filter.bin',b'f'),('page-0.bin',b'pages')]: (folder/name).write_bytes(data)
        (output/'shards.json').write_bytes(self.mapping)
        assignment=self.root/'assignment.json'
        assignment.write_text(json.dumps({'set':{'map_sha256':'e'*64,'shard_schema':'transparent-shard-v11'},
             'generated_by':{'source_sha':M.P.RELEASE_SHA},'unassigned':[],'workers':[{'id':'native-worker'}]}))
        release=self.root/'release';(release/'artifacts').mkdir(parents=True)
        for name in ('transparent-shard-server','shard-control'): (release/'artifacts'/name).write_bytes(name.encode())
        evidence=self.root/'evidence';evidence.mkdir();(evidence/'result.json').write_text(json.dumps({'status':'passed','map_sha256':hashlib.sha256(self.mapping).hexdigest()}))
        unit=self.root/'unit';unit.write_bytes(b'unit')
        inventory=SimpleNamespace(hosts={'ssh-alias':{'machine_id':'c'*32}})
        result=SimpleNamespace(stdout=(digest+'/\n'+digest+'/manifest.json\nshards.json\n').encode())
        pins={'binaries':{'transparent-shard-server':{'sha256':M.P.checksum(release/'artifacts/transparent-shard-server')}}}
        with patch.object(M.P,'OUTPUT',output),patch.object(M.P,'RELEASE',release),patch.object(M.P,'EVIDENCE',evidence), \
             patch.object(M.P,'verify_release',return_value=pins),patch.object(M,'worker_binary',side_effect=lambda name: release/'artifacts'/name), \
             patch.dict(M.WORKER_HASHES,{name:M.P.checksum(release/'artifacts'/name) for name in ('transparent-shard-server','shard-control')},clear=True),patch.object(M.transparent_map,'served_sha256',return_value='e'*64),patch.object(M.subprocess,'run',return_value=result):
            request=M.build(inventory,'ssh-alias','b'*40,assignment,unit,'d'*64,1<<30,1,worker_id='native-worker')
        self.assertEqual(len(request['files']),8)
        self.assertIn(digest+'/page-0.bin',{f['path'] for f in request['files']})
        self.assertEqual(request['worker_id'],'native-worker')

    def test_portable_worker_refuses_native_or_changed_artifacts_and_modes(self):
        name='transparent-shard-server'; data=b'portable'; expected=hashlib.sha256(data).hexdigest()
        with patch.object(M,'WORKER_ROOT',self.root/'portable'),patch.dict(M.WORKER_HASHES,{name:expected},clear=True):
            path=M.WORKER_ROOT/expected/name; path.parent.mkdir(parents=True); path.write_bytes(data); path.chmod(0o755)
            self.assertEqual(M.worker_binary(name),path)
            path.write_bytes(b'native-avx512')
            with self.assertRaisesRegex(ValueError,'identity/mode'):M.worker_binary(name)
            path.write_bytes(data); path.chmod(0o700)
            with self.assertRaisesRegex(ValueError,'identity/mode'):M.worker_binary(name)
            path.chmod(0o755);path.unlink();path.symlink_to(self.sources/'shards.json')
            with self.assertRaisesRegex(ValueError,'symlink'):M.worker_binary(name)
            with self.assertRaisesRegex(ValueError,'unsupported'):M.worker_binary('publisher')

    def client(self):
        client=object.__new__(M.Client)
        client.inventory=SimpleNamespace(lock={'type':'pinned_host','machine_id':'c'*32})
        client.host='worker';client.request=self.request
        return client

    def test_uncertain_remote_reply_keeps_coordinator_fence_until_verified_reconcile(self):
        client=self.client();owners=self.root/'client-owners'
        with patch.object(M,'OWNERS',owners),patch.object(M,'ProductionLock',return_value=self.lock), \
             patch.object(client,'checked_sources'),patch.object(M,'worker_binary',side_effect=lambda name:self.sources/('.input-'+name)), \
             patch.dict(M.WORKER_HASHES,{i['path'].removeprefix('.input-'):i['sha256'] for i in self.request['files'] if i['path'] in M.INPUTS},clear=True), \
             patch.object(client,'call',side_effect=[{'status':'preflight-passed'},subprocess.TimeoutExpired('ssh',1)]):
            with self.assertRaises(subprocess.TimeoutExpired):client.run('stage')
        identifier=M.digest(self.request);path=owners/(identifier+'.json')
        self.assertEqual(json.loads(path.read_text())['status'],'interrupted')
        replies=[]
        def call(action):
            M.inherited_lock.descriptors(required=True,path=self.lock.path);replies.append(action)
            return {'status':'staged','request_sha256':identifier}
        with patch.object(M,'OWNERS',owners),patch.object(M,'ProductionLock',return_value=self.lock),patch.object(client,'call',side_effect=call):
            result=client.run('reconcile')
        self.assertEqual(result['status'],'staged');self.assertEqual(replies,['status'])
        self.assertEqual(json.loads(path.read_text())['status'],'staged')

    def test_failed_remote_receive_is_displaced_only_by_explicit_locked_reconciliation(self):
        client=self.client();owners=self.root/'client-owners';owners.mkdir();identifier=M.digest(self.request)
        M.durable.atomic_json(owners/'latest.json',{'request_sha256':identifier})
        M.durable.atomic_json(owners/(identifier+'.request.json'),self.request)
        M.durable.atomic_json(owners/(identifier+'.json'),{'request_sha256':identifier,'status':'failed'})
        with patch.object(M,'OWNERS',owners),patch.object(M,'ProductionLock',return_value=self.lock), \
             patch.object(client,'call',side_effect=[{'status':'failed'},{'status':'reconciled'}]) as call:
            self.assertEqual(client.run('reconcile')['status'],'reconciled')
        self.assertEqual([c.args[0] for c in call.call_args_list],['status','reconcile'])

    def test_remote_source_stage_joins_under_coordinator_fd_without_persistent_master(self):
        spec=importlib.util.spec_from_file_location('input_source_target',M.HERE/'activity_source_stage.py')
        source=importlib.util.module_from_spec(spec);spec.loader.exec_module(source)
        client=object.__new__(source.SourceStage)
        client.recovery=None
        client.target='worker';client.host='worker';client.machine='c'*32;client.out=lambda _:None
        client.inventory=SimpleNamespace(lock={'type':'pinned_host','machine_id':'c'*32},hosts={'worker':{}},ssh={'mode':'config'})
        client.executor=SimpleNamespace(transport=lambda host:['ssh','worker'])
        archive=self.root/'ops.tar.gz';archive.write_bytes(b'archive')
        calls=[]
        def run(argv,**options):
            self.assertIn('-oControlMaster=no',argv);self.assertIn('-oControlPath=none',argv)
            self.assertEqual(options['pass_fds'],self.lock.descriptors())
            self.assertEqual(options['env']['PYTHONDONTWRITEBYTECODE'],'1')
            calls.append(argv)
            return SimpleNamespace(returncode=0,stdout=b'{"ok":true,"result":{"status":"staged"}}')
        with patch.object(source,'ProductionLock',return_value=self.lock),patch.object(source.subprocess,'run',side_effect=run):
            client.run('stage','b'*40,M.P.checksum(archive),archive)
        self.assertEqual(len(calls),2)

    def test_streamed_native_discovery_sees_only_shard_directories(self):
        # ShardSet opens manifest.json in every direct child directory. Run an
        # independent child through the real stream/verify path to reproduce
        # that boundary, rather than mocking native verification away.
        name='.input-transparent-shard-server'
        self.names[name]=('#!'+sys.executable+'\nimport pathlib,sys\nroot=pathlib.Path(sys.argv[sys.argv.index("--shard-dir")+1])\nfor child in root.iterdir():\n if child.is_dir(): assert (child/"manifest.json").is_file(), str(child)\nprint("native directory discovery passed")\n').encode()
        for item in self.request['files']:
            if item['path']==name:
                item['size']=len(self.names[name]);item['sha256']=hashlib.sha256(self.names[name]).hexdigest()
        receiver=M.Receiver(self.request,root=self.root/'flat-pub',owners=self.root/'flat-owner',lock_factory=lambda:self.lock)
        reply=receiver.stage(io.BytesIO(self.body()))
        self.assertEqual(reply['status'],'staged')
        self.assertEqual({p.name for p in receiver.target.iterdir() if p.is_dir()},{'a'*64})
        native=json.loads((receiver.owners/(receiver.identifier+'.native.result.json')).read_text())
        self.assertEqual(native['exit_code'],0)

    def test_native_child_keeps_remote_lock_and_retains_exact_verification_result(self):
        # Exercise the actual subprocess boundary using an isolated executable.
        self.receiver.partial.mkdir(parents=True);binary=self.receiver.partial/'.input-transparent-shard-server'
        binary.parent.mkdir(exist_ok=True);binary.write_text('#!'+sys.executable+'\nimport os,sys\nassert os.environ["WALLET_PIR_PRODUCTION_LOCK_FDS"]\nprint("verified")\n')
        binary.chmod(0o755);self.receiver.owners.mkdir()
        with self.lock,patch.dict(os.environ,{M.inherited_lock.VARIABLE:str(self.lock.fd),'PYTHONDONTWRITEBYTECODE':'1'}):
            M.Receiver.native(self.receiver,self.lock)
        result=json.loads((self.receiver.owners/(self.receiver.identifier+'.native.result.json')).read_text())
        self.assertEqual(result['exit_code'],0);self.assertEqual(result['status'],'passed')
        self.assertEqual(result['log_sha256'],M.P.checksum(self.receiver.owners/(self.receiver.identifier+'.native.log')))

    def test_failed_native_verification_retains_exit_and_log_in_denominator(self):
        self.receiver.partial.mkdir(parents=True);binary=self.receiver.partial/'.input-transparent-shard-server'
        binary.parent.mkdir(exist_ok=True);binary.write_text('#!'+sys.executable+'\nimport sys\nprint("failure")\nsys.exit(3)\n');binary.chmod(0o755)
        self.receiver.owners.mkdir()
        with self.lock,patch.dict(os.environ,{M.inherited_lock.VARIABLE:str(self.lock.fd)}),self.assertRaisesRegex(ValueError,'native candidate'):
            M.Receiver.native(self.receiver,self.lock)
        result=json.loads((self.receiver.owners/(self.receiver.identifier+'.native.result.json')).read_text())
        self.assertEqual(result['status'],'failed');self.assertEqual(result['exit_code'],3)
        self.assertEqual(result['log_sha256'],M.P.checksum(self.receiver.owners/(self.receiver.identifier+'.native.log')))

    def test_source_bootstrap_target_requires_pinned_coordinator(self):
        spec=importlib.util.spec_from_file_location('input_source_client',M.HERE/'activity_source_stage.py')
        source=importlib.util.module_from_spec(spec);spec.loader.exec_module(source)
        inventory=SimpleNamespace(lock={'type':'remote','host':'coordinator'},hosts={'worker':{'machine_id':'a'*32}},ssh={'mode':'config'})
        with self.assertRaisesRegex(ValueError,'pinned root'):source.SourceStage(inventory,target='worker')


class CutoverInputs(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        root=Path(self.temp.name).resolve();output=root/'initial';output.mkdir();evidence=root/'evidence';evidence.mkdir()
        (output/'shards.json').write_text('{}')
        self.publication=M.P.checksum(output/'shards.json')
        (evidence/'result.json').write_text(json.dumps({'status':'passed','map_sha256':self.publication}))
        for name,value in (('OUTPUT',output),('EVIDENCE',evidence),('verify_release',lambda _:None)):
            p=patch.object(M.P,name,value);p.start();self.addCleanup(p.stop)
        inv={'hosts':{'coordinator':{'machine_id':'a'*32}},'ssh':{'mode':'config'},
             'lock':{'type':'pinned_host','machine_id':'a'*32},'services':{}}
        self.inventory=SimpleNamespace(**inv)
        files={'inventory.json':json.dumps(inv)}
        for gate in M.CutoverPreparation.GATES:
            files[gate+'.json']=json.dumps({'gate':gate,'status':'passed','native_source_sha':M.P.RELEASE_SHA,
                                          'publication_sha256':self.publication})
        sample={'schema':'transparent-script-sample-v1','anchor_height':3500738,'tool_sha':M.P.RELEASE_SHA,
                'clients':[{'scripts':['aa'],'journal_events':2,'expected_digest':'f'*64}]}
        for schema in ('v10','v11'):files[schema+'-sample.json']=json.dumps(sample)
        self.request={'version':1,'source_sha':'b'*40,'release_result_sha256':'c'*64,'attempt':1,'files':files}

    def preparation(self):return M.CutoverPreparation(self.inventory,self.request,M.digest(self.request))

    def test_closed_proofs_bind_real_inputs_without_writing_or_executing(self):
        prep=self.preparation();self.assertEqual(set(prep.render()),M.CutoverPreparation.PROOFS)
        self.assertFalse(prep.target.exists());self.assertFalse(prep.owner.exists())
        self.assertIs(M.CutoverPreparation.run,M.Preparation.run)

    def test_pending_or_other_publication_gate_cannot_be_staged(self):
        name='comprehensive-ci.json';original=self.request['files'][name]
        for field,value in [('status','pending'),('publication_sha256','0'*64),('native_source_sha','0'*40)]:
            v=json.loads(original);v[field]=value;self.request['files'][name]=json.dumps(v)
            with self.assertRaisesRegex(ValueError,'gate'):self.preparation().render()

    def test_inventory_drift_empty_sample_and_arbitrary_files_refuse(self):
        v=json.loads(self.request['files']['inventory.json']);v['lock']['machine_id']='0'*32
        self.request['files']['inventory.json']=json.dumps(v)
        with self.assertRaisesRegex(ValueError,'inventory'):self.preparation().render()
        self.request['files']['inventory.json']=json.dumps(vars(self.inventory))
        sample=json.loads(self.request['files']['v11-sample.json']);sample['clients']=[]
        self.request['files']['v11-sample.json']=json.dumps(sample)
        with self.assertRaisesRegex(ValueError,'sample'):self.preparation().render()
        self.request['files']['run.py']='print(1)'
        with self.assertRaisesRegex(ValueError,'file set'):self.preparation()

    def test_specification_cannot_stage_arbitrary_commands_or_missing_product_fields(self):
        self.request['files']={'product.json':json.dumps({'shell':'execute unreviewed command'})}
        with self.assertRaisesRegex(ValueError,'specification'):self.preparation().render()


class ServiceInputs(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.output = self.root/'publications/initial'; self.output.mkdir(parents=True)
        self.evidence = self.root/'preparation'; self.evidence.mkdir()
        mapping = {'shards': [{'shard_id':i, 'manifest_digest':str(i)*64, 'geometry':g, 'start_height':i}
                    for i,g in enumerate(('archive-wide','recent-4k-8k'))]}
        (self.output/'shards.json').write_text(json.dumps(mapping))
        (self.evidence/'result.json').write_text(json.dumps({'status':'passed','map_sha256':M.P.checksum(self.output/'shards.json')}))
        for name,value in (('OUTPUT',self.output),('EVIDENCE',self.evidence),('JOURNAL',self.root/'journal'),('verify_release',lambda _:None)):
            p=patch.object(M.P,name,value);p.start();self.addCleanup(p.stop)
        self.inventory=SimpleNamespace(hosts={},ssh={'mode':'config'})
        source='a'*40
        files={
            'controller.json':json.dumps({'data_dir':str(M.P.JOURNAL),'publication_root':str(self.output.parent),
                'initial_publication':str(self.output),'fleet_config':'/opt/transparent-publisher/v11/fleet.json',
                'source_sha':M.P.RELEASE_SHA,'shadow':False,'recent_from':1,'recent_geometry':'recent-4k-8k',
                'archive_geometry':'archive-wide','directory_choice':'all','range_profile':'v2',
                'fleet_command':str(M.SOURCE/source/'transparent/ops/scripts/transparent-live-fleet.py')}),
            'fleet.json':json.dumps({'state_dir':'/opt/transparent-publisher/v11/state','roster':'/opt/transparent-publisher/v11/roster.json',
                'worker_schema':'transparent-shard-v11','worker_active_record':'/opt/transparent-publisher/v11/active.json',
                'worker_runtime_cache_dir':'/srv/transparent-pir/v11/runtime-cache','worker_root':'/srv/transparent-pir/v11/publications'}),
            'roster.json':json.dumps([{'id':'worker-'+str(i)} for i in range(3)]),
            'pins.json':json.dumps({'worker-'+str(i):M.WORKER_HASHES['transparent-shard-server'] for i in range(3)}),
            'policy.json':json.dumps({'mode':'observe'}),
            'fixture.json':json.dumps({'schema':'transparent-shard-v11','tables':[{'shard_id':s['shard_id'],
                'geometry':s['geometry'],'revision':s['manifest_digest'],'table':t} for s in mapping['shards'] for t in ('directory','pages')]})}
        for unit in M.ServicePreparation.UNITS:
            if unit=='transparent-publish-controller':args=['/usr/local/bin/transparent-publish-controller','--config','/opt/transparent-publisher/v11/controller.json']
            elif unit=='transparent-filter-server':args=['/usr/local/bin/transparent-filter-server','--shard-dir',str(self.output)]
            else:
                script='transparent-quality-load.py' if unit=='transparent-5qps-continuous' else 'transparent-fleet-scaler.py' if unit=='transparent-fleet-scaler' else 'transparent-live-fleet.py'
                args=['/usr/bin/python3','-B',str(M.SOURCE/source/'transparent/ops/scripts'/script),'/opt/transparent-publisher/v11/fleet.json']
            files[unit+'.service']='[Service]\nExecStart='+M.shlex.join(args)+'\n'
        self.request={'version':1,'source_sha':source,'release_result_sha256':'b'*64,'attempt':1,'files':files}

    def preparation(self,request=None):
        request=request or self.request
        return M.ServicePreparation(self.inventory,request,M.digest(request))

    def test_plan_binds_every_reviewed_byte_without_output_or_service_effects(self):
        prep=self.preparation();files=prep.render();plan=prep.plan()
        self.assertEqual(set(files),M.ServicePreparation.FILES)
        self.assertEqual(plan['files'],{n:hashlib.sha256(b).hexdigest() for n,b in files.items()})
        self.assertFalse(prep.target.exists());self.assertFalse(prep.owner.exists())
        self.assertIs(M.ServicePreparation.run,M.Preparation.run)

    def test_namespace_scaler_pin_and_traffic_group_rejections(self):
        cases=[('controller.json','shadow',True),('controller.json','data_dir','/old/journal'),
               ('controller.json','recent_from',999),('controller.json','range_profile','v1'),
               ('controller.json','fleet_command','/old/live-fleet.py'),
               ('fleet.json','state_dir','/opt/transparent-publisher/state'),('policy.json','mode','active'),
               ('pins.json','worker-0','0'*64)]
        for name,key,value in cases:
            with self.subTest(name=name,key=key):
                r=copy.deepcopy(self.request);v=json.loads(r['files'][name]);v[key]=value;r['files'][name]=json.dumps(v)
                with self.assertRaises(ValueError):self.preparation(r).render()
        r=copy.deepcopy(self.request);v=json.loads(r['files']['fixture.json']);v['tables'].pop();r['files']['fixture.json']=json.dumps(v)
        with self.assertRaisesRegex(ValueError,'traffic group'):self.preparation(r).render()

    def test_closed_files_digest_bounds_and_duplicate_configuration_keys(self):
        for name in ('../../Caddyfile','credentials','unknown.json'):
            r=copy.deepcopy(self.request);r['files'][name]='x'
            with self.assertRaises(ValueError):self.preparation(r)
        r=copy.deepcopy(self.request);r['files']['policy.json']='x'*(256*1024+1)
        with self.assertRaises(ValueError):self.preparation(r)
        with self.assertRaises(ValueError):M.ServicePreparation(self.inventory,self.request,'0'*64)
        r=copy.deepcopy(self.request);r['files']['policy.json']='{"mode":"observe","mode":"active"}'
        with self.assertRaisesRegex(ValueError,'duplicate'):self.preparation(r).render()

    def test_units_cannot_execute_other_source_or_use_predecessor_filter(self):
        for unit in M.ServicePreparation.UNITS:
            r=copy.deepcopy(self.request)
            r['files'][unit+'.service']='[Service]\nExecStart=/bin/true\n'
            with self.subTest(unit=unit),self.assertRaises(ValueError):self.preparation(r).render()

    def test_real_fixture_stdout_does_not_create_output_file(self):
        table=b'\x01'+b'\x00'*4095
        shard=self.output/('c'*64);shard.mkdir()
        for name in ('directory','pages'):(shard/(name+'.0.bin')).write_bytes(table)
        segment={'rows':1,'row_bytes':4096,'sha256':hashlib.sha256(table).hexdigest()}
        (shard/'manifest.json').write_text(json.dumps({'schema':'transparent-shard-v11',
            'directory_segments':[segment],'page_segments':[segment]}))
        (self.output/'shards.json').write_text(json.dumps({'shards':[{'shard_id':0,'manifest_digest':'c'*64,
            'geometry':'archive-wide','sealed':True}]}))
        script=Path(__file__).parents[1]/'scripts/activity-query-fixture.py'
        result=subprocess.run([sys.executable,str(script),'--shard-dir',str(self.output),'--out','-'],
                              cwd=self.root,capture_output=True,check=True)
        fixture=json.loads(result.stdout);summary=json.loads(result.stderr)
        self.assertEqual(len(fixture['tables']),2)
        self.assertEqual(summary['sha256'],hashlib.sha256(result.stdout).hexdigest())
        self.assertFalse((self.root/'-').exists())

    def test_service_configuration_agrees_with_actual_host_namespace_validator(self):
        spec=importlib.util.spec_from_file_location('service_host_contract',M.HERE/'activity_schema_host.py')
        host=importlib.util.module_from_spec(spec);spec.loader.exec_module(host)
        files=copy.deepcopy(self.request['files'])
        controller=json.loads(files['controller.json']);controller.update(
            data_dir='/srv/transparent-activity/full-v3/journal',publication_root='/srv/transparent-activity/full-v11/publications',
            initial_publication='/srv/transparent-activity/full-v11/publications/initial')
        files['controller.json']=json.dumps(controller)
        files['transparent-publish-controller.service']='[Service]\nExecStart=/usr/local/bin/transparent-publish-controller --config "/opt/transparent-publisher/v11/controller.json"\n'
        actor=host.Host.__new__(host.Host);actor.role='coordinator'
        installs=[]
        for name in ('controller.json','fleet.json',*host.AUTHORITY):
            path=self.root/name;path.write_text(files[name])
            target='/etc/systemd/system/'+name if name.endswith('.service') else '/opt/transparent-publisher/v11/'+name
            installs.append({'target':target,'source':str(path)})
        actor.plan={'installs':installs}
        actor.validate_units()


if __name__ == '__main__': unittest.main()
