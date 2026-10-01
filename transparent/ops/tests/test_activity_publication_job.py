"""Full publication refuses incomplete sources and retains failed preparation."""
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3]/'ops/lib'))
SPEC = importlib.util.spec_from_file_location('publication_job', Path(__file__).parents[1]/'lib/activity_publication_job.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.journal = self.root/'journal'
        self.journal.mkdir()
        (self.journal/'writer.lock').touch()
        (self.journal/'meta.json').write_text(json.dumps({'version':3,'start_height':0}))
        (self.journal/'checkpoint.bin').write_bytes((123).to_bytes(8,'little')+((M.THROUGH+1)*48).to_bytes(8,'little'))
        self.guard = self.root/'guard.json'
        self.guard.write_text(json.dumps({'status':'passed','guarded_unit':M.INGEST_UNIT,'through':M.THROUGH,
            'source_sha':'a1c4b809f62035d5e8509b3b43807fb7e3dab7e1',
            'binary_sha256':'d4a8cc161cc9c6b938963a81d12dd9b13df5cfad595d6fe4a5309d3a2032a972'}))
        self.terminal = {'ActiveState':'inactive','MainPID':'0','Result':'success','ExecMainStatus':'0','NRestarts':'0'}
        for name,value in [('JOURNAL',self.journal),('GUARD_RESULT',self.guard),('EVIDENCE',self.root/'evidence'),
                           ('OUTPUT',self.root/'output'),('ROOT',self.root)]:
            p = patch.object(M,name,value)
            p.start()
            self.addCleanup(p.stop)
        self.inventory = SimpleNamespace(lock={'type':'pinned_host','machine_id':'a'*32})
        self.job = M.PublicationJob(self.inventory,'b'*40,'c'*64,lambda _:None)

    def test_completed_checkpoint_is_not_enough_until_ingest_and_guard_pass(self):
        with patch.object(M,'state',return_value=self.terminal):
            M.verify_journal()
        for key,value in [('MainPID','123'),('NRestarts','1'),('Result','oom-kill'),('ExecMainStatus','1')]:
            with patch.object(M,'state',return_value=dict(self.terminal,**{key:value})), self.assertRaises(ValueError):
                M.verify_journal()
        self.guard.write_text(json.dumps({'status':'failed'}))
        with patch.object(M,'state',return_value=self.terminal), self.assertRaises(ValueError):
            M.verify_journal()

    def test_truncated_or_short_checkpoint_refuses(self):
        for data in [b'',b'\x00'*15,(123).to_bytes(8,'little')+(48).to_bytes(8,'little')]:
            (self.journal/'checkpoint.bin').write_bytes(data)
            with patch.object(M,'state',return_value=self.terminal), self.assertRaises(ValueError):
                M.verify_journal()

    def test_memory_and_disk_boundaries_do_not_lower_floors(self):
        M.healthy({'memory_available':.2,'disk_available':{'disk':.2}})
        for sample in [{'memory_available':.199,'disk_available':{'disk':.9}},
                       {'memory_available':.9,'disk_available':{'disk':.199}}]:
            with self.assertRaises(ValueError):
                M.healthy(sample)

    def test_plan_is_bound_to_release_receipt_and_resources(self):
        plan = self.job.plan()
        changed = M.PublicationJob(self.inventory,'b'*40,'d'*64,lambda _:None).plan()
        self.assertNotEqual(M.identity(plan),M.identity(changed))
        self.assertIn('MemoryMax=16G',plan['properties'])
        self.assertEqual(plan['through'],3500738)
        self.assertNotIn('txid',json.dumps(plan))
        with self.assertRaisesRegex(ValueError,'plan identity'):
            self.job.start('0'*64)
        self.assertFalse(M.EVIDENCE.exists())

    def test_build_arguments_cover_genesis_anchor_and_six_month_boundary(self):
        M.EVIDENCE.mkdir()
        calls = []
        def native(name,arguments,_lock):
            calls.append((name,list(map(str,arguments))))
            if name == 'shard-cutoff':
                (M.EVIDENCE/'cutoff.json').write_text(json.dumps({'anchor':{'height':M.THROUGH,'hash':'a'*64},
                    'journal':{'start_height':0},'cutoff':{'height':3262749}}))
            if name == 'shard-publish':
                M.OUTPUT.mkdir()
                (M.OUTPUT/'shards.json').write_text('{"shards":[{}]}')
                (M.OUTPUT/'manifest.json').write_text(json.dumps({'schema':'transparent-shard-v11','shard_id':0,
                    'geometry':'archive-wide','occupancy':{'page_rows':1},
                    'directory_segments':[{'rows':65536,'row_bytes':4096}],
                    'page_segments':[{'rows':65536,'row_bytes':4096}]}))
        with patch.object(self.job,'native',side_effect=native), patch.object(M,'verify_anchor'):
            self.job.build(object())
        self.assertEqual([name for name,_ in calls],['shard-cutoff','shard-publish','shard-verify'])
        self.assertIn('--months',calls[0][1])
        self.assertIn('--expect-start',calls[2][1])
        self.assertIn('--expect-anchor-hash',calls[2][1])
        self.assertFalse(any('--txid-display' in args for _,args in calls))
        allocation = json.loads((M.EVIDENCE/'allocation.json').read_text())
        self.assertGreater(allocation['allocated_bytes'],0)
        self.assertEqual(allocation['page_rows'],65536)

    def test_v10_manifest_cannot_be_accepted_by_compatibility_loader(self):
        M.OUTPUT.mkdir()
        (M.OUTPUT/'shards.json').write_text('{"shards":[{}]}')
        (M.OUTPUT/'manifest.json').write_text('{"schema":"transparent-shard-v10"}')
        with self.assertRaisesRegex(ValueError,'schema or capability'):
            M.allocation()

    @unittest.skipUnless(os.name == 'posix','requires flock and process groups')
    def test_native_child_inherits_both_locks_and_records_real_attempt(self):
        M.EVIDENCE.mkdir()
        release = self.root/'release'
        (release/'artifacts').mkdir(parents=True)
        executable = release/'artifacts'/'probe'
        executable.write_text('#!'+sys.executable+'\n'+
            'import os,json,fcntl\n'+
            'fds=list(map(int,os.environ["WALLET_PIR_PRODUCTION_LOCK_FDS"].split(",")))\n'+
            'assert len(fds)==1\n'+
            'assert os.environ["PYTHONDONTWRITEBYTECODE"]=="1"\n'+
            'os.fstat(fds[0])\n'+
            'print("lock-inherited")\n')
        executable.chmod(0o700)
        with (self.root/'production.lock').open('w') as production, (self.journal/'writer.lock').open('rb') as journal:
            fcntl.flock(production,fcntl.LOCK_EX|fcntl.LOCK_NB)
            fcntl.flock(journal,fcntl.LOCK_SH|fcntl.LOCK_NB)
            self.job.journal_fd = journal.fileno()
            lock = SimpleNamespace(verify=lambda:None,descriptors=lambda:(production.fileno(),))
            with patch.object(M,'RELEASE',release), patch.object(M,'verify_release'), patch.object(M,'resources',
                    return_value={'unix':1,'memory_available':.8,'disk_available':{'disk':.8}}):
                self.job.native('probe',[],lock)
        self.assertIn('lock-inherited',(M.EVIDENCE/'probe.log').read_text())
        self.assertGreater(json.loads((M.EVIDENCE/'probe.owner.json').read_text())['pid'],0)
        self.assertEqual(json.loads((M.EVIDENCE/'probe.result.json').read_text())['exit_code'],0)
        self.assertTrue((M.EVIDENCE/'health.jsonl').exists())

    def test_failed_preflight_under_acquired_lock_retains_terminal_result(self):
        M.EVIDENCE.mkdir()
        plan = self.job.plan()
        (M.EVIDENCE/'owner.json').write_text(json.dumps({'status':'launching','plan':plan,
            'plan_sha256':M.identity(plan),'machine_id':'a'*32,'journal':{}}))
        lock = SimpleNamespace(__enter__=lambda:None,__exit__=lambda *_:None)
        with patch.object(M,'ProductionLock',return_value=lock), patch.object(self.job,'preflight',side_effect=ValueError('incomplete')):
            with self.assertRaises(ValueError):
                self.job.run()
        result = json.loads((M.EVIDENCE/'result.json').read_text())
        self.assertEqual(result['status'],'failed')
        self.assertFalse(M.OUTPUT.exists())


if __name__ == '__main__':
    unittest.main()
