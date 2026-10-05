"""Fictional bootstrap fleet contracts; kernel lease fixtures run on Linux."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
HERE=Path(__file__).resolve()
sys.path[:0]=[str(HERE.parents[3]/'ops/lib'),str(HERE.parents[1]/'lib')]
import activity_bootstrap_fleet as G
import activity_source_stage as B
from wallet_pir_ops import durable
REAL_SCAN=G.S.scan


class Fleet(unittest.TestCase):
    def response(self,binding,host,holder=None):
        return dict(bootstrap=G.KIND,kind=G.S.KIND,binding=binding,machine_id=G.PINS[host],euid=0,
            observed_unix=time.time(),inventory_sha256=G.INVENTORY_SHA,bounds=G.S.BOUNDS,classes=G.CLASSES,
            classes_sha256=hashlib.sha256(G.S.canonical(G.CLASSES)).hexdigest(),baseline=list(G.BASELINE),
            namespaces=[[n,str(p)] for n,p in (('schema',G.ROOT/'schema'),('host-actions',G.ROOT/'host-actions'),
                        ('input-staging',G.INPUTS),('source-staging',G.ROOT/'staging'))],
            inventory={'sha256':'1'*64},selected=[],selected_count=0,selected_sha256='2'*64,
            blocked=[],blocked_count=0,associated=[],associated_count=0,unattributed=[],unattributed_count=0,
            processes=[],lock={'holders':[holder] if holder else []},controls=[],control_rejections=[],
            pending=[],pending_count=0,ancillary={
                'kind':G.A.KIND,'pins_sha256':G.A.PINS_SHA256,'machine_id':G.PINS[host],
                'boot_id':'00000000-0000-0000-0000-000000000001' if host in ('coordinator','router') else None,
                'observed_unix':time.time(),'units':{u:{'status':'absent'} for u in G.A.UNITS} if host=='coordinator' else {},
                'route':{'sha256':'1'*64,'bytes':10,'prototype_port_excluded':True} if host in ('coordinator','router') else None})

    def test_partial_duplicate_or_substituted_inventory_refuses(self):
        inventory=SimpleNamespace(hosts={h:{'machine_id':p} for h,p in G.PINS.items()},ssh={'mode':'pinned'})
        G.pinned(inventory)
        for hosts in ({'coordinator':inventory.hosts['coordinator']},
                      {**inventory.hosts,'worker-3':inventory.hosts['worker-2']},
                      {**inventory.hosts,'worker-3':{'machine_id':'f'*32}}):
            with self.assertRaisesRegex(ValueError,'complete five-host'):G.pinned(SimpleNamespace(hosts=hosts,ssh=inventory.ssh))

    def test_missing_stale_foreign_or_blocked_survey_is_not_a_pass(self):
        binding={'request_sha256':'a'*64,'nonce':'b'*48,'host':'coordinator','skip':None}
        value=self.response(binding,'coordinator')
        G.verify(value,binding,'coordinator')
        changes=({'binding':{**binding,'nonce':'c'*48}}, {'machine_id':'f'*32},{'blocked_count':1},
                 {'unattributed_count':1},{'associated_count':1},{'processes':[{}]}, {'namespaces':[]},
                 {'inventory':{}},{'observed_unix':time.time()-301},{'selected_count':1},
                 {'lock':{'holders':[{'pid':100}]}})
        for change in changes:
            with self.assertRaises(ValueError):G.verify({**value,**change},binding,'coordinator')

    def test_complete_fleet_is_nonce_bound_and_failed_raw_reply_is_retained(self):
        request={'source_sha':'a'*40};raws=[];commands=[]
        inventory=SimpleNamespace(hosts={h:{} for h in G.PINS})
        transport=SimpleNamespace(transport=lambda h:['ssh',h])
        fixture=self
        class Child:
            def __init__(self,argv,**kw):
                commands.append(argv);self.returncode=0
            def communicate(self,raw,timeout):
                envelope=json.loads(raw);host=envelope['host']
                binding=envelope['binding']
                if fixture.foreign and host=='worker-2':binding={**binding,'nonce':'f'*48}
                return json.dumps(fixture.response(binding,host)).encode(),None
        self.foreign=False
        def local(binding,host,**kw):return self.response(binding,host,kw['holder'])
        with patch.object(G,'runtime',return_value=(inventory,transport)),patch.object(G,'local',side_effect=local), \
             patch.object(G.subprocess,'Popen',Child),patch.object(G.S,'process',return_value={'pid':123,'start_ticks':456}), \
             patch.object(G.S,'boot_id',return_value='00000000-0000-0000-0000-000000000001'), \
             patch.object(G.CA,'authority',return_value={'status':'refused'}):
            proof=G.fleet(request,SimpleNamespace(verify=lambda:None),None,'fixed readonly code',retain=lambda h,r:raws.append((h,r)))
            self.assertEqual(proof['hosts'],sorted(G.PINS))
            self.assertEqual([h for h,_ in raws],sorted(G.PINS))
            self.assertTrue(all('-oControlPath=none' in command for command in commands))
            self.assertTrue(all(' -B ' in command[-1] for command in commands))
            self.foreign=True;raws.clear()
            with self.assertRaisesRegex(ValueError,'stale or foreign'):
                G.fleet(request,SimpleNamespace(verify=lambda:None),None,'fixed readonly code',retain=lambda h,r:raws.append((h,r)))
            self.assertEqual(raws[-1][0],'worker-2')
            self.assertEqual(json.loads(raws[-1][1])['binding']['nonce'],'f'*48)

    CONTROL={'pid':4242,'start_ticks':1002,'exe':G.CA.CONTROL[0],'command_sha256':G.CA.CONTROL_SHA256,
             'cgroup':'0::/user.slice/user-0.slice/session-5.scope','shell':{'pid':4241,'start_ticks':1001},
             'sshd':{'pid':4240,'start_ticks':1000},
             'connection':{'local':['10.142.0.5',22],'remote':['10.142.0.3',40000]}}

    def snapshot(self,port=40000,status='verified'):
        return {'kind':G.CA.KIND,'status':status,'machine_id':G.CA.COORDINATOR,'monotonic':time.monotonic(),
                'main':{'pid':4015133,'start_ticks':270361979},
                'clients':[{'pid':5000,'start_ticks':270400000,'destination':'10.142.0.5',
                            'connection':{'local':['10.142.0.3',port],'remote':['10.142.0.5',22]}}]}

    def run_fleet(self,snapshots,raws):
        inventory=SimpleNamespace(hosts={h:{} for h in G.PINS})
        transport=SimpleNamespace(transport=lambda h:['ssh',h])
        fixture=self
        class Child:
            def __init__(self,argv,**kw):self.returncode=0
            def communicate(self,raw,timeout):
                envelope=json.loads(raw);value=fixture.response(envelope['binding'],envelope['host'])
                if envelope['host']=='worker-2':
                    value.update(controls=[fixture.CONTROL],pending_count=1,
                                 pending=[{k:fixture.CONTROL[k] for k in ('pid','start_ticks','exe','command_sha256','cgroup')}])
                return json.dumps(value).encode(),None
        def local(binding,host,**kw):return self.response(binding,host,kw['holder'])
        with patch.object(G,'runtime',return_value=(inventory,transport)),patch.object(G,'local',side_effect=local), \
             patch.object(G.subprocess,'Popen',Child),patch.object(G.S,'process',return_value={'pid':123,'start_ticks':456}), \
             patch.object(G.S,'boot_id',return_value='00000000-0000-0000-0000-000000000001'), \
             patch.object(G.CA,'authority',side_effect=snapshots):
            return G.fleet({'source_sha':'a'*40},None,None,'fixed readonly code',retain=lambda h,r:raws.append((h,r)))

    def test_service_issued_worker_control_is_attributed_through_a_fresh_coordinator_snapshot(self):
        raws=[]
        # Before every worker survey, and after the one reporting a control:
        # here only the earlier snapshot still shows the client.
        snapshots=iter([self.snapshot(),self.snapshot(),self.snapshot(port=40001),self.snapshot()])
        proof=self.run_fleet(lambda:next(snapshots),raws)
        self.assertIsNone(next(snapshots,None))
        attribution=proof['surveys']['worker-2']['control_attribution']
        self.assertEqual(attribution['attributed'][0]['client'],{'pid':5000,'start_ticks':270400000})
        self.assertEqual(attribution['attributed'][0]['reconciler'],{'pid':4015133,'start_ticks':270361979})
        self.assertIn('worker-2.attribution',[h for h,_ in raws])
        self.assertNotIn('control_attribution',proof['surveys']['worker-1'])

    def test_unattributed_worker_control_refuses_after_retaining_raw_evidence(self):
        for snapshots in ([self.snapshot(port=40001)]*4,[self.snapshot(status='refused')]*4):
            raws=[];snapshots=iter(snapshots)
            with self.assertRaisesRegex(ValueError,'not attributable'):
                self.run_fleet(lambda:next(snapshots),raws)
            self.assertEqual([h for h,_ in raws][-2:],['worker-2','worker-2.attribution'])
            self.assertEqual(json.loads(raws[-1][1])['binding']['host'],'worker-2')

    def test_host_local_check_never_admits_a_pending_control(self):
        binding={'request_sha256':'a'*64,'nonce':'b'*48,'host':'worker-1','skip':None}
        value=self.response(binding,'worker-1')
        pending=[{k:self.CONTROL[k] for k in ('pid','start_ticks','exe','command_sha256','cgroup')}]
        value.update(controls=[self.CONTROL],pending=pending,pending_count=1)
        with self.assertRaisesRegex(ValueError,'not attributed'):G.verify(value,binding,'worker-1')
        attributed=[{'control':{'pid':4242,'start_ticks':1002}}]
        G.verify(value,binding,'worker-1',attributed=attributed)
        for change in ({'pending':[]},{'pending_count':0},{'controls':[dict(self.CONTROL,start_ticks=1)]},
                       {'controls':[dict(self.CONTROL,exe='/tmp/shard-control')]},{'control_rejections':None}):
            with self.assertRaises(ValueError):G.verify({**value,**change},binding,'worker-1',attributed=attributed)
        with self.assertRaises(ValueError):
            G.verify(value,binding,'worker-1',attributed=[{'control':{'pid':4242,'start_ticks':1}}])
        coordinator={**self.response({**binding,'host':'coordinator'},'coordinator'),'controls':[self.CONTROL],
                     'pending':pending,'pending_count':1}
        with self.assertRaisesRegex(ValueError,'partial or foreign'):
            G.verify(coordinator,{**binding,'host':'coordinator'},'coordinator',attributed=attributed)

    def test_every_candidate_tool_is_an_operational_class_and_prototype_is_never_baseline(self):
        import activity_candidate as C
        self.assertTrue({n.rsplit('/',1)[-1] for n in C.ARTIFACTS}<=set(G.NAMES))
        self.assertFalse(any('prototype' in name for name in G.BASELINE))

    def test_embedded_components_keep_distinct_globals_and_fixed_survey_has_no_mutation_entry(self):
        namespace={};exec(B.EMBEDDED_FLEET,namespace)
        self.assertIsNot(namespace['_SURVEY'].__dict__,namespace['_BOOTSTRAP_FLEET'].__dict__)
        self.assertIs(namespace['_BOOTSTRAP_FLEET'].S,namespace['_SURVEY'])
        self.assertIs(namespace['_BOOTSTRAP_FLEET'].CA,namespace['_CONTROL'])
        self.assertEqual(namespace['_CONTROL'].CONTROL_SHA256,G.CA.CONTROL_SHA256)
        self.assertEqual(namespace['_SURVEY'].KIND,G.S.KIND)
        self.assertEqual(namespace['_BOOTSTRAP_FLEET'].KIND,G.KIND)
        compile(B.SURVEY_HELPER,'fictional-readonly-survey','exec')
        compile(B.HELPER,'fictional-source-receiver','exec')


class Transport(unittest.TestCase):
    def client(self):
        client=object.__new__(B.SourceStage)
        client.host='worker-1';client.machine=G.PINS['worker-1'];client.target='worker-1'
        client.attempt=1;client.recovery=None;client.out=lambda value:None
        client.inventory=SimpleNamespace(hosts={'worker-1':{}})
        client.executor=SimpleNamespace(transport=lambda host:['ssh',host])
        return client

    def test_launcher_executes_exact_reviewed_bytes_with_bounded_full_argument(self):
        compiled=[]
        def compiler(raw,name,mode):
            compiled.append(raw);return compile('',name,mode)
        exec(B.LAUNCHER,{'compile':compiler})
        self.assertEqual(compiled,[B.HELPER.encode()])
        command=B.command_for(['sudo','-n','--'],{'mode':'stage','source_sha':'a'*40})
        self.assertLess(len(command.encode()),131071)
        self.assertLess(len(command.encode()),B.MAX_COMMAND)
        with self.assertRaisesRegex(ValueError,'bounded header'):
            B.command_for([],{'surveys':'x'*(B.MAX_REQUEST+1)})
        # A substituted digest must refuse before compilation/execution.
        with self.assertRaises(AssertionError):
            exec(B.LAUNCHER.replace(B.HELPER_SHA256,'0'*64),{'compile':compiler})
        self.assertEqual(len(compiled),1)

    def test_transport_loss_is_unknown_without_exposing_command_or_stderr(self):
        import subprocess
        client=self.client()
        cases=[subprocess.TimeoutExpired('fictional-sensitive-command',1800),OSError('fictional-sensitive-path')]
        for error in cases:
            with patch.object(B.subprocess,'run',side_effect=error):
                with self.assertRaises(B.Unknown) as caught:client.call({'mode':'stage'})
            self.assertNotIn('fictional-sensitive',str(caught.exception))
        for result in (SimpleNamespace(returncode=255,stdout=b'',stderr=b'fictional-sensitive'),
                       SimpleNamespace(returncode=0,stdout=b'[]',stderr=b''),
                       SimpleNamespace(returncode=0,stdout=b'bad json',stderr=b'')):
            with patch.object(B.subprocess,'run',return_value=result):
                with self.assertRaises(B.Unknown):client.call({'mode':'stage'})

    def test_target_preflight_retains_raw_surveys_but_forwards_only_bounded_proof(self):
        client=self.client();seen=[];output=[];client.out=output.append
        proof={'schema':G.KIND,'surveys':{'worker-1':'x'*200000},'nonce':'b'*48}
        def call(request):
            seen.append(request);B.command_for([],request);return {'ok':True}
        with tempfile.TemporaryDirectory() as directory:
            archive=Path(directory)/'fixture.tar';archive.write_bytes(b'fictional archive')
            checksum=hashlib.sha256(archive.read_bytes()).hexdigest()
            with patch.object(B.FLEET,'fleet',return_value=proof),patch.object(client,'call',side_effect=call):
                result=client.run('preflight','a'*40,checksum,archive)
        self.assertNotIn('surveys',seen[0]['coordinator_fleet'])
        self.assertEqual(result['coordinator_fleet'],proof)
        self.assertEqual(json.loads(output[-1])['coordinator_fleet'],proof)


@unittest.skipUnless(Path('/proc/sys/kernel/random/boot_id').is_file(),'Linux kernel identity fixture')
class Lease(unittest.TestCase):
    def setUp(self):
        scratch=tempfile.TemporaryDirectory();self.addCleanup(scratch.cleanup)
        self.root=Path(scratch.name);self.inputs=self.root/'input';self.calls=[]
        for name,value in (('INPUTS',self.inputs),('PINS',{'coordinator':Path('/etc/machine-id').read_text().strip()}),
                           ('FENCE',lambda **kw:self.calls.append(('fence',kw)))):
            p=patch.object(G,name,value);p.start();self.addCleanup(p.stop)
        self.request={'mode':'stage','machine_id':G.PINS['coordinator'],'source_sha':'a'*40,'sha256':'b'*64,'guard_attempt':1}
        self.lock=SimpleNamespace(verify=lambda:self.calls.append('lock'))
        self.proof={'schema':G.KIND,'nonce':'c'*48}
        p=patch.object(G,'fleet',return_value=self.proof);p.start();self.addCleanup(p.stop)

    def leased(self,operation):return G.leased(self.request,self.lock,None,'fixed-readonly-code',operation,durable.atomic_json)

    def test_fence_and_real_kernel_owner_are_durable_before_effect(self):
        def effect(proof):
            identifier=G.lease_id(self.request)
            record=json.loads((self.inputs/(identifier+'.json')).read_text())
            self.assertEqual(record['status'],'running');self.assertEqual(record['pid'],os.getpid())
            self.assertEqual(record['start_ticks'],G.S.process(os.getpid())['start_ticks'])
            self.assertEqual(record['boot_id'],G.S.boot_id())
            self.assertEqual(json.loads((self.inputs/'latest.json').read_text()),{'request_sha256':identifier})
            self.assertEqual(proof['lease_request_sha256'],identifier)
            return {'status':'staged'}
        self.leased(effect)
        self.assertEqual(G.status(self.request)['status'],'staged')
        with self.assertRaisesRegex(ValueError,'exists'):self.leased(effect)

    def test_fleet_failure_keeps_failed_owner_and_never_dispatches(self):
        G.fleet.side_effect=ValueError('fictional stale fleet')
        with self.assertRaisesRegex(ValueError,'stale fleet'):
            self.leased(lambda p:(_ for _ in ()).throw(AssertionError('no effects')))
        self.assertEqual(G.status(self.request)['status'],'failed')
        self.assertTrue((self.inputs/(G.lease_id(self.request)+'.request.json')).exists())

    def test_reconcile_refuses_live_owner_and_invalid_boot_then_preserves_failure(self):
        with self.assertRaises(ValueError):self.leased(lambda p:(_ for _ in ()).throw(ValueError('fictional transfer failure')))
        callback=lambda p:{'status':'reconciled'}
        with self.assertRaisesRegex(ValueError,'remains alive'):
            G.reconciled(self.request,self.lock,None,'code',callback,durable.atomic_json)
        path=self.inputs/(G.lease_id(self.request)+'.json');record=json.loads(path.read_text())
        record['boot_id']=None;durable.atomic_json(path,record)
        with self.assertRaisesRegex(ValueError,'identity is incomplete'):
            G.reconciled(self.request,self.lock,None,'code',callback,durable.atomic_json)
        record['boot_id']=G.S.boot_id();record['start_ticks']-=1;durable.atomic_json(path,record)
        G.reconciled(self.request,self.lock,None,'code',callback,durable.atomic_json)
        self.assertEqual(G.status(self.request)['status'],'reconciled')
        failed=json.loads((self.inputs/(G.lease_id(self.request)+'.failure.json')).read_text())
        self.assertEqual(failed['status'],'failed');self.assertEqual(failed['error_type'],'ValueError')
        with self.assertRaisesRegex(ValueError,'exists'):self.leased(callback)
        self.request['guard_attempt']=2;self.leased(callback)
        self.assertEqual(G.status(self.request)['status'],'staged')


@unittest.skipUnless(Path('/proc/sys/kernel/random/boot_id').is_file(),'Linux kernel identity fixture')
class RetainedSurvey(unittest.TestCase):
    """A completed locked survey's retained replies are evidence for the next survey, never its owners.

    Real processes stand in for the reconciler service main, its SSH client
    and a genuine owner. The fixture host is this test's own process tree:
    other hub processes are filtered from the scan, and the service fixture's
    cgroup reads as a baseline unit's.
    """
    SERVICE='0::/system.slice/transparent-shard-server.service'

    def spawn(self,argv,executable=None,cgroup=None):
        child=subprocess.Popen(argv,executable=executable,start_new_session=True,stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        def stop():
            try:os.killpg(child.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            child.wait()
        self.addCleanup(stop)
        found=[child.pid]
        if argv[-1].endswith('& wait'):
            deadline=time.monotonic()+10
            while not (Path('/proc/%d/task/%d/children'%(child.pid,child.pid)).read_text().split()):
                self.assertLess(time.monotonic(),deadline);time.sleep(.01)
            found+=[int(x) for x in Path('/proc/%d/task/%d/children'%(child.pid,child.pid)).read_text().split()]
        self.tracked.update(found)
        if cgroup:self.service.update(dict.fromkeys(found,cgroup))
        return [G.S.process(pid) for pid in found]

    def setUp(self):
        scratch=tempfile.TemporaryDirectory();self.addCleanup(scratch.cleanup)
        root=Path(scratch.name);os.chmod(root,0o700)
        self.tracked,self.service=set(),{}
        machine=Path('/etc/machine-id').read_text().strip()
        for name,value in (('ROOT',root/'ops'),('INPUTS',root/'ops/input-staging'),('LOCK',root/'production.lock'),
                           ('PINS',{**G.PINS,'coordinator':machine}),('FENCE',lambda **kw:None)):
            p=patch.object(G,name,value);p.start();self.addCleanup(p.stop)
        own=G.S.ancestors()
        def scan(*a,**k):
            return [dict(p,cgroup=self.service[p['pid']]) if p['pid'] in self.service else p
                    for p in REAL_SCAN(*a,**k) if p['pid'] in self.tracked or p['pid'] in own]
        for target,name,value in ((G.S,'scan',scan),(G.os,'geteuid',lambda:0)):
            p=patch.object(target,name,value);p.start();self.addCleanup(p.stop)
        # The reconciler service main (a baseline-class process) and its SSH client child.
        self.main,self.client=self.spawn(['transparent-shard-server','-c','sleep 600 & wait'],'/bin/sh',self.SERVICE)
        self.boot=G.S.boot_id()

    def snapshot(self):
        return {'kind':G.CA.KIND,'status':'verified','machine_id':G.CA.COORDINATOR,'boot_id':self.boot,
                'unit':G.CA.UNIT,'fragment_sha256':G.CA.FRAGMENT_SHA256,'script_sha256':G.CA.SCRIPT_SHA256,
                'script_source':G.CA.SCRIPT_SOURCE,
                'main':{'pid':self.main['pid'],'start_ticks':self.main['start_ticks'],'command_sha256':'3'*64},
                'clients':[{'pid':self.client['pid'],'start_ticks':self.client['start_ticks'],'destination':'10.142.0.5',
                            'connection':{'local':['10.142.0.3',40000],'remote':['10.142.0.5',22]}}],
                'observed_unix':time.time(),'monotonic':time.monotonic()}

    def reply(self,binding,host):
        value=dict(Fleet.response(None,binding,host),version=G.S.VERSION,boot_id='00000000-0000-0000-0000-00000000000a',
                   booted_unix=1,scanned=10,associated_records={},baseline_bound=[],baseline_bound_count=0,
                   lock={'path':str(G.LOCK),'holders':[]})
        if host=='worker-2':
            value.update(controls=[Fleet.CONTROL],pending_count=1,
                         pending=[{k:Fleet.CONTROL[k] for k in ('pid','start_ticks','exe','command_sha256','cgroup')}])
        return value

    def fleet(self,lock):
        fixture=self
        class Child:
            def __init__(self,argv,**kw):self.returncode=0
            def communicate(self,raw,timeout):
                envelope=json.loads(raw)
                return json.dumps(fixture.reply(envelope['binding'],envelope['host'])).encode(),None
        inventory=SimpleNamespace(hosts={h:{} for h in G.PINS})
        with patch.object(G,'runtime',return_value=(inventory,SimpleNamespace(transport=lambda h:['ssh',h]))), \
             patch.object(G.subprocess,'Popen',Child),patch.object(G.CA,'authority',side_effect=lambda:self.snapshot()):
            if lock is None:return G.fleet({'source_sha':'a'*40},None,None,'fixed readonly code')
            request={'mode':'stage','machine_id':G.PINS['coordinator'],'source_sha':'a'*40,'sha256':'b'*64,'guard_attempt':1}
            G.INPUTS.mkdir(parents=True,mode=0o700)
            fd=os.open(G.LOCK,os.O_RDWR|os.O_CREAT,0o600)
            try:
                G.leased(request,lock,None,'fixed readonly code',lambda proof:{'status':'staged'},durable.atomic_json)
            finally:os.close(fd)
            return G.lease_id(request)

    def survey(self):
        binding={'request_sha256':'c'*64,'nonce':'d'*48,'host':'coordinator','skip':None}
        value=G.local(binding,'coordinator',deadline=time.monotonic()+60)
        return value,binding

    def associated(self,value):
        return {(p['pid'],p['association']) for p in value['associated']}

    def write(self,relative,raw):
        path=G.INPUTS/relative;path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o400)
        with os.fdopen(fd,'wb') as f:f.write(raw)
        return path

    def refused(self,relative,raw):
        """The next survey with one more retained file: it must refuse; the file is then removed."""
        path=self.write(relative,raw)
        try:
            value,binding=self.survey()
            with self.assertRaisesRegex(ValueError,'unfinished, live or unattributed'):G.verify(value,binding,'coordinator')
            return value
        finally:path.unlink()

    def retained(self):
        """A completed locked source survey: its owner, and its directory relative to input-staging."""
        lease=self.fleet(SimpleNamespace(verify=lambda:None))
        self.assertEqual(json.loads((G.INPUTS/(lease+'.json')).read_text())['status'],'staged')
        [directory]=(G.INPUTS/'fleet-surveys').glob('*/*')
        return lease,directory.relative_to(G.INPUTS)

    def test_completed_locked_survey_does_not_make_baseline_services_owners_of_the_next_survey(self):
        lease,relative=self.retained()
        directory=G.INPUTS/relative
        names=sorted(p.name for p in directory.iterdir())
        self.assertEqual(names,sorted([h+'.json' for h in G.PINS]+['worker-2.attribution.json']))
        coordinator=json.loads((directory/'coordinator.json').read_text())
        self.assertIn(self.main['pid'],[p['pid'] for p in coordinator['baseline_bound']])
        self.assertEqual([h['pid'] for h in coordinator['lock']['holders']],[os.getpid()])
        for name in names:
            self.assertIsNotNone(G.retained('input-staging',str(relative/name),json.loads((directory/name).read_text())),name)

        # The next (candidate upload) survey: every file read and hashed, nothing adopted.
        value,binding=self.survey()
        G.verify(value,binding,'coordinator')
        self.assertEqual(value['associated_count'],0)
        self.assertEqual(value['inventory']['evidence'],len(names))
        self.assertEqual(value['inventory']['json_files'],len(names)+3)
        # The durable owner keeps its top-level and fleet proof owner identity; the reply keeps its holder.
        _,references,_,_,_=G.S.owner_inventory(G.namespaces(),evidence=G.retained)
        me=G.S.process(os.getpid())['start_ticks']
        self.assertEqual(sorted((r['record'].rsplit('/',1)[-1],r['key'],r['pid'],r['start_ticks']) for r in references),
                         sorted([('coordinator.json','survey-holder',os.getpid(),me),
                                 (lease+'.json','pid',os.getpid(),me),(lease+'.json','pid',os.getpid(),me)]))
        # Before the fix, and for any caller without `evidence`, the replies name owners.
        with patch.object(G,'retained',lambda *a:None):
            value,binding=self.survey()
        self.assertLessEqual({self.main['pid'],self.client['pid']},{pid for pid,_ in self.associated(value)})
        self.assertEqual({p['record'].split('/')[1] for p in value['associated']},{'fleet-surveys'})
        with self.assertRaisesRegex(ValueError,'unfinished, live or unattributed'):G.verify(value,binding,'coordinator')
        _,references,_,_,_=G.S.owner_inventory(G.namespaces())
        self.assertIn(self.main['pid'],[r['pid'] for r in references])

    def test_true_owners_and_misplaced_tampered_or_nested_evidence_still_refuse(self):
        lease,relative=self.retained()
        request,nonce=relative.parts[1:]
        coordinator=json.loads((G.INPUTS/relative/'coordinator.json').read_text())
        attribution=json.loads((G.INPUTS/relative/'worker-2.attribution.json').read_text())
        owner=json.loads((G.INPUTS/(lease+'.json')).read_text())
        live,descendant=self.spawn(['sh','-c','sleep 600 & wait'])
        # A genuine owner, completed or not, with its fleet proof and result, and its descendants.
        found=self.associated(self.refused('f'*64+'.json',G.S.canonical(
            dict(owner,request_sha256='f'*64,pid=live['pid'],start_ticks=live['start_ticks']))))
        self.assertIn((live['pid'],'recorded-process'),found)
        self.assertEqual({pid for pid,_ in found},{live['pid'],descendant['pid']})
        # A proof embedding replies (as a read-only preflight returns it) is not exempt inside a durable owner.
        nested=self.fleet(None)
        dead=self.spawn(['sleep','600'])[0];os.killpg(dead['pid'],signal.SIGKILL)
        while G.S.process(dead['pid'])['state']!='Z':time.sleep(.01)  # a zombie until cleanup: no PID reuse
        found=self.associated(self.refused('e'*64+'.json',G.S.canonical(
            dict(owner,request_sha256='e'*64,pid=dead['pid'],start_ticks=dead['start_ticks'],fleet=nested))))
        self.assertIn(self.main['pid'],{pid for pid,_ in found})
        # At its exact place, a reply's lock holder remains an owner identity.
        other='0'*48
        moved=lambda value,**binding:dict(value,binding=dict(value['binding'],**binding))
        holder={k:v for k,v in self.main.items() if k in G.S.SUMMARY}
        smuggled=dict(moved(coordinator,nonce=other),lock={'path':coordinator['lock']['path'],
                      'holders':[dict(holder,pid=live['pid'],start_ticks=live['start_ticks'])]})
        self.assertIn((live['pid'],'recorded-process'),self.associated(
            self.refused('fleet-surveys/%s/%s/coordinator.json'%(request,other),G.S.canonical(smuggled))))
        # Tampered content at an exactly bound place, or exact content at another place, is an owner record.
        bound=coordinator['baseline_bound'];place='fleet-surveys/%s/%s/'%(request,other)
        cases=[(place+'coordinator.json',dict(moved(coordinator,nonce=other),note={'pid':live['pid']})),
               (place+'coordinator.json',dict(moved(coordinator,nonce=other),machine_id=G.PINS['worker-1'])),
               (place+'coordinator.json',dict(moved(coordinator,nonce=other),
                                               baseline_bound=[dict(bound[0],child_pid=live['pid'])])),
               (place+'coordinator.json',dict(moved(coordinator,nonce=other),baseline_bound_count=len(bound)+1)),
               (place+'coordinator.json',dict(moved(coordinator,nonce=other),classes={'names':[],'roots':[]})),
               (place+'coordinator.json',coordinator),
               ('fleet-surveys/%s/%s/coordinator.json'%('1'*64,nonce),coordinator),
               (place+'router.json',moved(coordinator,nonce=other)),
               (place+'worker-1.json',moved(coordinator,nonce=other,host='worker-1')),
               (place+'sub/coordinator.json',moved(coordinator,nonce=other)),
               ('x/'+place+'coordinator.json',moved(coordinator,nonce=other)),
               (place+'coordinator.json',dict(moved(coordinator,nonce=other),machine_id=G.PINS['worker-1'],
                                               ancillary=dict(coordinator['ancillary'],machine_id=G.PINS['worker-1']))),
               (place+'worker-2.attribution.json',dict(moved(attribution,nonce=other),host='worker-1')),
               ('d'*64+'.json',coordinator),
               (place+'worker-2.attribution.json',dict(moved(attribution,nonce=other),snapshots=[
                   dict(v,clients=[dict(c,extra=1) for c in v['clients']]) for v in attribution['snapshots']])),
               (place+'worker-2.attribution.json',attribution),
               (place+'coordinator.attribution.json',moved(attribution,nonce=other,host='coordinator'))]
        for name,changed in cases:
            self.assertIsNone(G.retained('input-staging',name,changed),name)
            found=self.associated(self.refused(name,G.S.canonical(changed)))
            self.assertTrue({self.main['pid'],self.client['pid']}&{pid for pid,_ in found},name)
        # Another namespace never holds retained replies.
        path=G.ROOT/'schema'/place/'coordinator.json';path.parent.mkdir(parents=True)
        path.write_bytes(G.S.canonical(moved(coordinator,nonce=other)))
        self.assertIsNone(G.retained('schema',place+'coordinator.json',moved(coordinator,nonce=other)))
        value,binding=self.survey();path.unlink()
        self.assertIn(self.main['pid'],{pid for pid,_ in self.associated(value)})
        # Malformed bytes or a link refuse outright.
        value=self.refused(place+'coordinator.json',G.S.canonical(moved(coordinator,nonce=other))[:-1])
        self.assertTrue(any(r.startswith('owner record unreadable') for r in value['blocked']))
        link=G.INPUTS/place/'router.json';link.symlink_to(G.INPUTS/relative/'router.json')
        value,binding=self.survey();link.unlink()
        self.assertTrue(any('link or special file' in r for r in value['blocked']))
        # A foreign operational process outside every baseline unit still refuses.
        self.spawn(['shard-build','600'],'/bin/sleep')
        value,binding=self.survey()
        self.assertTrue(any('not bound to a baseline service' in r for r in value['blocked']))
        with self.assertRaises(ValueError):G.verify(value,binding,'coordinator')

    def test_production_shaped_coordinator_reply_with_ancillary_units_is_evidence(self):
        """The pinned coordinator's reply lists the prototype, canonical load and its native child."""
        _,relative=self.retained()
        coordinator=json.loads((G.INPUTS/relative/'coordinator.json').read_text())
        (G.INPUTS/relative/'coordinator.json').unlink()
        A=G.A;unit='/system.slice/'+A.LOAD;boot=A.PINS[A.PROTOTYPE]['boot_id']
        load,child=self.spawn(['sh','-c','sleep 600 & wait'],cgroup='0::'+unit)
        pin=A.PINS[A.LOAD];prototype=A.PINS[A.PROTOTYPE]
        units={A.PROTOTYPE:{'pid':prototype['pid'],'start_ticks':prototype['start_ticks'],'state':'S','status':'verified',
                            'unit':A.PROTOTYPE,'boot_id':boot,'cgroup':'/system.slice/'+A.PROTOTYPE,'exe':'/usr/bin/python3',
                            'listeners':[['tcp','0100007F',8192]],
                            **{k:prototype[k] for k in ('exe_sha256','command_sha256','fragment_sha256')}},
               A.LOAD:{'pid':load['pid'],'start_ticks':load['start_ticks'],'state':'S','status':'verified','unit':A.LOAD,
                       'boot_id':boot,'cgroup':unit,'exe':'/bin/sh',
                       **{k:pin[k] for k in ('exe_sha256','command_sha256','fragment_sha256')}}}
        children=[{'pid':child['pid'],'start_ticks':child['start_ticks'],'state':'S','unit':A.LOAD,'boot_id':boot,
                   'cgroup':unit,**pin['child'],'parent_pid':load['pid'],'parent_start_ticks':load['start_ticks'],
                   'pgid':load['pid'],'session':load['pid']}]
        ancillary={'kind':A.KIND,'pins_sha256':A.PINS_SHA256,'machine_id':A.COORDINATOR,'boot_id':boot,'units':units,
                   'load_children':children,'route':{'sha256':'1'*64,'bytes':10,'prototype_port_excluded':True},
                   'observed_unix':coordinator['observed_unix']}
        listed={k:v for k,v in self.main.items() if k in G.S.SUMMARY}
        bound=[dict(listed,pid=load['pid'],start_ticks=load['start_ticks'],cgroup='0::'+unit,unit=A.LOAD,
                    exe_sha256=pin['exe_sha256'],**{'class':['transparent-loadtest']})]
        value=dict(coordinator,machine_id=A.COORDINATOR,boot_id=boot,ancillary=ancillary,
                   baseline_bound=coordinator['baseline_bound']+bound,
                   baseline_bound_count=coordinator['baseline_bound_count']+1)
        with patch.dict(G.PINS,coordinator=A.COORDINATOR),patch.object(G.S,'boot_id',lambda:boot):
            name=str(relative/'coordinator.json')
            self.assertEqual(G.retained('input-staging',name,value),[{'pid':os.getpid(),
                             'start_ticks':G.S.process(os.getpid())['start_ticks'],'boot_id':boot}])
            self.write(name,G.S.canonical(value))
            def observe(evidence):
                return G.S.observe(G.namespaces(),classes=G.CLASSES,baseline=G.BASELINE,binding={},evidence=evidence)
            clear=observe(G.retained)
            self.assertEqual((clear['associated_count'],clear['blocked']),(0,[]))
            self.assertEqual(clear['inventory']['evidence'],6)
            # As root saw it: service mains, the load and its child become owners of the historical survey.
            poisoned={(p['pid'],p['association']) for p in observe(None)['associated']}
            self.assertLessEqual({(load['pid'],'recorded-process'),(self.main['pid'],'recorded-process')},poisoned)
            self.assertIn(child['pid'],{pid for pid,_ in poisoned})
            for changed in (dict(ancillary,units=dict(units,**{A.LOAD:dict(units[A.LOAD],note=1)})),
                            dict(ancillary,load_children=[dict(children[0],extra_pid=1)]),
                            dict(ancillary,pins_sha256='0'*64)):
                self.assertIsNone(G.retained('input-staging',name,dict(value,ancillary=changed)))


if __name__=='__main__':unittest.main()
