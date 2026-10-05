"""Fictional assembly contracts. Snapshot provider/selection are mocked here.

These tests prove refusal/composition, not safe snapshot construction, process
ownership, production execution or any actual candidate qualification.
"""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[3]/'ops/lib'), str(HERE.parents[1]/'lib')]
import activity_oracle_report as M


class Assembly(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(); self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name).resolve(); self.count = 0
        self.budget = SimpleNamespace(deadline=time.monotonic()+30, check=lambda:None)

    def ref(self, value, *, path=None, lines=False):
        self.count += 1
        path = path or self.root/str(self.count)
        raw = (''.join(json.dumps(row)+'\n' for row in value) if lines else json.dumps(value)).encode()
        path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
        return dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest())

    def fixture(self):
        S,R=M.S,M.R
        heights=sorted(S.fixed_heights() | {1,2,3})
        hashes={str(h):format(h+1,'064x') for h in heights}
        sample={'recipe':{'explicit':list(S.EXPLICIT), 'top':3, 'random':5, 'last':3,
                         'seed':20261001, 'through':S.THROUGH, 'heights':heights},
                'canonical_hashes':hashes, 'blocks_bytes':48*(S.THROUGH+2),'blocks_sha256':'b'*64}
        request={'source_sha':'f'*40,'candidate':{'source_sha':R.C.SOURCE_SHA,'identity':R.C.identity()},
                 'journal':{'genesis_hash':hashes['0']},
                 'publication':{'anchor_hash':hashes[str(S.THROUGH)],'map_sha256':None}}
        mapping=self.ref({'genesis_hash':hashes['0'],'start_height':0,
                         'shards':[{'end_height':S.THROUGH,'terminal_block_hash':hashes[str(S.THROUGH)]}]})
        request['publication']['map_sha256']=mapping['sha256']
        request_sha=R.C.durable.digest(request)
        root=self.root/'snapshots'/request_sha; owners=self.root/'owners'
        restoration={'status':'proven','writer':{'pid':900}}
        manifest={'source_sha':request['source_sha'],'request_sha256':request_sha,
                  'candidate':request['candidate'],'publication':request['publication'],
                  'writer':{'restored':restoration},'journal':{'tip_height':S.THROUGH+1},
                  'contents':{'files':{'blocks.bin':{'size':sample['blocks_bytes'],'sha256':'b'*64}}}}
        manifest_ref=self.ref(manifest,path=root/'manifest.json')
        owner={'status':'staged','phase':'retained','request_sha256':request_sha,'target':str(root),
               'manifest':manifest_ref['sha256'],'restoration':restoration}
        native={'schema':'transparent-event-spotcheck-v2','tool_sha':R.C.SOURCE_SHA,
                'data_dir':str(root/'journal'),'genesis_hash':hashes['0'],
                'journal':{'start_height':0,'covered_through':S.THROUGH+1},
                'sample':copy.deepcopy(sample['recipe']),'blocks_compared':17,'blocks_disagreeing':0,'blocks':[]}
        attempts,timings=[],[]
        def append(calls,replies,start):
            i=len(attempts)
            attempts.append(dict(status=200,request=self.ref(calls),response=self.ref(replies)))
            timings.append(dict(attempt_index=i,started_unix=start,ended_unix=start+.1,
                                started_monotonic=start,ended_monotonic=start+.1))
        def anchors(at):
            append([dict(id=h,method='getblockhash',params=[h]) for h in heights],
                   [dict(id=h,result=hashes[str(h)],error=None) for h in heights],at)
        anchors(10)
        for i,h in enumerate(heights):
            txid=format(h+100,'064x'); identity=hashes[str(h)]
            append(dict(id=1,method='getblock',params=[str(h),1]),
                   dict(id=1,result={'height':h,'hash':identity,'tx':[txid]},error=None),60+i)
            append(dict(id=1,method='getrawtransaction',params=[txid,1]),
                   dict(id=1,result={'txid':txid,'vin':[{'coinbase':'00'}],
                                     'vout':[{'n':0,'scriptPubKey':{'hex':'00'}}]},error=None),60+i+.5)
            native['blocks'].append({'height':h,'journal_hash':identity,'node_hash':identity,'agrees':True,
                'node_receives':1,'node_spends':0,'node_events':1,'journal_events':1,
                'missing_count':0,'extra_count':0,'missing_from_journal':[],'extra_in_journal':[]})
        anchors(200)
        execution={'owner':self.ref(dict(pid=42,start_ticks=123,started_unix=50,started_monotonic=50,
                        native_source_sha=R.C.SOURCE_SHA,candidate_sha256=R.C.identity(),
                        binary_sha256=R.C.ARTIFACTS['event-spotcheck'],publication_sha256=mapping['sha256'])),
                   'result':self.ref(dict(status='passed',pid=42,start_ticks=123,exit_code=0,
                        started_unix=50,ended_unix=150,ended_monotonic=150,timeout_seconds=180)),
                   'native':self.ref(native),'stderr':self.ref('fictional stderr'),
                   'health':self.ref([dict(observed_unix=t,memory_available=.5,disk_available={'/':.5})
                                      for t in range(49,155,5)])}
        evidence=dict(mapping=mapping,execution=execution,snapshot_request=self.ref(request,path=owners/(request_sha+'.request.json')),
                      snapshot_owner=self.ref(owner,path=owners/(request_sha+'.json')),snapshot_manifest=manifest_ref,
                      rpc_attempts=self.ref(attempts,lines=True),rpc_timings=self.ref(timings,lines=True))
        evidence['capture_result']=self.ref(dict(schema='transparent-oracle-capture-v1',status='passed',failures=[],
                          attempt_count=len(attempts),timing_count=len(timings),attempts=evidence['rpc_attempts'],
                          timings=evidence['rpc_timings'],
                          interval=dict(started_unix=5,ended_unix=250,started_monotonic=5,ended_monotonic=250)))
        seen=[]
        def verify(root_,checksum,*,request,budget):
            seen.append((root_,checksum,budget)); budget.check()
            return manifest
        def nested_budget(deadline,label,*,guard):
            return SimpleNamespace(deadline=deadline,label=label,guard=guard,check=guard)
        provider=SimpleNamespace(SNAPSHOTS=self.root/'snapshots',OWNERS=owners,validate=lambda r:r,
                                 digest=R.C.durable.digest,verify_tree=verify,Budget=nested_budget)
        return evidence,sample,provider,seen

    def produce(self,evidence,sample,provider):
        with patch.object(M.importlib,'import_module',return_value=provider), patch.object(M.S,'derive',return_value=sample):
            return M.produce(evidence,self.budget)

    def test_pre_reader_verifier_requires_no_native_assertion_and_full_provider(self):
        evidence,sample,provider,seen=self.fixture()
        inputs={k:evidence[k] for k in ('mapping','snapshot_request','snapshot_owner','snapshot_manifest')}
        with patch.object(M.importlib,'import_module',return_value=provider), patch.object(M.S,'derive',return_value=sample):
            prepared=M.prepare_snapshot(inputs,self.budget)
        self.assertEqual(len(seen),1)
        self.assertEqual(prepared['sample'],sample)
        self.assertEqual(prepared['root'],Path(inputs['snapshot_manifest']['path']).parent)
        self.assertEqual(seen[0][2].deadline,self.budget.deadline)

    def test_requires_full_provider_then_composes_real_raw_and_temporal_validators(self):
        evidence,sample,provider,seen=self.fixture()
        report=self.produce(evidence,sample,provider)
        self.assertEqual(report['gate'],'independent-chain-oracle')
        self.assertEqual(report['events_compared'],17)
        self.assertEqual(seen[0][2].deadline,self.budget.deadline)
        self.assertIs(seen[0][2].guard,self.budget.check)
        self.assertEqual(report['raw_pool_category_coverage']['orchard_component_transactions'],0)
        self.assertEqual(report['raw_evidence']['snapshot_manifest'],evidence['snapshot_manifest'])

    def test_failed_full_snapshot_verification_cannot_emit_a_gate(self):
        evidence,sample,provider,_=self.fixture()
        def refuse(*args,**kwargs): raise ValueError('fictional snapshot checksum differs')
        provider.verify_tree=refuse
        with self.assertRaisesRegex(ValueError,'fictional snapshot checksum'):
            self.produce(evidence,sample,provider)

    def test_missing_provider_refuses_without_manifest_only_fallback(self):
        evidence,_,_,_=self.fixture()
        with patch.object(M.importlib,'import_module',side_effect=ModuleNotFoundError('fictional missing provider')):
            with self.assertRaises(ModuleNotFoundError): M.produce(evidence,self.budget)

    def test_missing_restoration_historical_child_pid_reuse_and_late_anchors_refuse(self):
        for kind in ('restoration','historical','pid-reuse','late-anchors'):
            evidence,sample,provider,_=self.fixture()
            if kind=='restoration':
                owner=M.R.value(evidence['snapshot_owner']); owner['restoration']['status']='unknown'
                evidence['snapshot_owner']=self.ref(owner,path=Path(evidence['snapshot_owner']['path']))
            elif kind in ('historical','pid-reuse'):
                name='owner' if kind=='historical' else 'result'
                value=M.R.value(evidence['execution'][name])
                value.update(native_source_sha=M.R.C.HISTORICAL_SHA) if kind=='historical' else value.update(start_ticks=124)
                evidence['execution'][name]=self.ref(value)
            else:
                timings=M.json_lines(evidence['rpc_timings'],lambda:None)
                timings[-1].update(started_unix=140,ended_unix=141,started_monotonic=140,ended_monotonic=141)
                evidence['rpc_timings']=self.ref(timings,lines=True)
            with self.subTest(kind=kind),self.assertRaises(ValueError):
                self.produce(evidence,sample,provider)

    def test_budget_refusal_and_partial_raw_index_cannot_emit_a_gate(self):
        evidence,sample,provider,_=self.fixture()
        evidence['rpc_attempts']=self.ref([],lines=True)
        with self.assertRaises(ValueError): self.produce(evidence,sample,provider)
        def refuse(): raise ValueError('fictional aggregate budget')
        self.budget.check=refuse
        with self.assertRaisesRegex(ValueError,'fictional aggregate budget'):
            self.produce(evidence,sample,provider)

    def test_failed_capture_or_unbound_index_cannot_emit_a_gate(self):
        for kind in ('failed','partial','unbound'):
            evidence,sample,provider,_=self.fixture()
            closure=M.R.value(evidence['capture_result'])
            if kind=='failed': closure['failures']=[{'error_type':'HTTPBoundaryRefusal'}]
            if kind=='partial': closure['attempt_count']-=1
            if kind=='unbound': closure['attempts']['sha256']='f'*64
            evidence['capture_result']=self.ref(closure)
            with self.subTest(kind=kind),self.assertRaises(ValueError):
                self.produce(evidence,sample,provider)

    def test_valid_json_without_committed_line_terminator_refuses(self):
        reference = self.ref([{'attempt_index':0}], lines=True)
        path = Path(reference['path'])
        raw = path.read_bytes().rstrip(b'\n')
        path.write_bytes(raw)
        reference['sha256'] = hashlib.sha256(raw).hexdigest()
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            M.json_lines(reference, lambda:None)

    def test_wall_clock_change_cannot_hide_native_monotonic_overrun(self):
        evidence,sample,provider,_=self.fixture()
        result=M.R.value(evidence['execution']['result'])
        # Unix elapsed still passes the generic capture check. The monotonic
        # elapsed proves this child exceeded its immutable timeout.
        result['ended_monotonic']=250
        evidence['execution']['result']=self.ref(result)
        with self.assertRaisesRegex(ValueError, 'monotonic execution bound'):
            self.produce(evidence,sample,provider)

    def test_output_uses_same_budget_and_expiry_cannot_emit_a_report(self):
        out = self.root/'report.json'
        report = {'raw_evidence': {'fictional': True}, 'gate': 'fictional'}
        checked = []
        self.budget.check = lambda: checked.append(True)
        with patch.object(M, 'produce', return_value=report):
            self.assertIs(M.produce_and_write({}, self.budget, out), report)
        self.assertGreater(len(checked), 10)
        self.assertEqual(M.R.json_bytes(out.read_bytes())['gate'], 'fictional')
        self.budget.deadline = time.monotonic()-1
        refused = self.root/'expired.json'
        with patch.object(M, 'produce', return_value=report), self.assertRaisesRegex(ValueError, 'deadline'):
            M.produce_and_write({}, self.budget, refused)
        self.assertFalse(refused.exists())
        self.assertFalse(Path(str(refused)+'.raw-evidence.json').exists())


if __name__=='__main__': unittest.main()
