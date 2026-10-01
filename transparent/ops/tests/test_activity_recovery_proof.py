"""Real reopened SQLite gates, including a compatible legacy reader rollback."""
from contextlib import closing
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3]/'ops/lib'))
SPEC = importlib.util.spec_from_file_location('recovery_proof', Path(__file__).parents[1]/'lib/activity_recovery_proof.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


def receive(metadata=True):
    raw = bytearray(87)
    raw[3:7] = (10).to_bytes(4, 'little')
    raw[7:15] = (123).to_bytes(8, 'little')
    raw[15:47] = bytes.fromhex('a'*64)
    return bytes(raw)+(b'\x28\x01\x05' if metadata else b'')


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root/'client.sqlite'
        self.sample = {'genesis_hash':'b'*64,'anchor_height':10,'anchor_hash':'c'*64,
            'clients':[{'scripts':['51'],'required_from':0,'class':'cutover-active','journal_events':1,
                        'expected_digest':hashlib.sha256(receive()).hexdigest()}]}
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''CREATE TABLE wallet_meta(key TEXT,value TEXT);
              CREATE TABLE scripts(script BLOB,required_from INTEGER);
              CREATE TABLE coverage(script BLOB,start_height INTEGER,end_height INTEGER);
              CREATE TABLE pending_work(id INTEGER);
              CREATE TABLE receives(event BLOB,script BLOB,height INTEGER,revision_digest TEXT);
              CREATE TABLE spends(event BLOB,script BLOB,height INTEGER,revision_digest TEXT);''')
            db.executemany('INSERT INTO wallet_meta VALUES(?,?)', [('schema_version','4'),
                ('set_identity',json.dumps({'shard_schema':'transparent-shard-v11','genesis_hash':'b'*64})),
                ('anchor',json.dumps({'height':10,'hash':'c'*64}))])
            db.execute('INSERT INTO scripts VALUES(?,0)', (b'\x51',))
            db.execute('INSERT INTO coverage VALUES(?,0,10)', (b'\x51',))
            db.execute('INSERT INTO receives VALUES(?,?,10,?)',(receive(),b'\x51','d'*64))

    def change(self, sql, args=()):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute(sql,args)

    def check(self, schema='transparent-shard-v11'):
        return M.inspect_store(self.path,self.sample,schema)

    def test_nonempty_persisted_metadata_and_trusted_anchor_survive_reopen(self):
        result = self.check()
        self.assertEqual(result['events'],1)
        self.assertTrue(result['metadata_available'])
        self.assertEqual(M.event(receive(),'transparent-shard-v11')['fee'],{'state':'exact','value':5})

    def test_legacy_reader_retains_unavailable_metadata_without_zero_fee(self):
        self.change('UPDATE receives SET event=?',(receive(False),))
        self.change("UPDATE wallet_meta SET value=? WHERE key='set_identity'",(json.dumps({'shard_schema':'transparent-shard-v10','genesis_hash':'b'*64}),))
        self.sample['clients'][0]['expected_digest']=hashlib.sha256(receive(False)).hexdigest()
        result=self.check('transparent-shard-v10')
        self.assertFalse(result['metadata_available'])
        legacy=M.event(receive(False),'transparent-shard-v10')
        self.assertIsNone(legacy['fee'])
        self.assertIsNone(legacy['input_count'])
        with self.assertRaises(ValueError):
            M.event(receive(),'transparent-shard-v10')

    def test_pending_unresolved_anchor_or_coverage_cannot_pass(self):
        for sql,args in [('INSERT INTO pending_work VALUES(1)',()),
                         ("UPDATE wallet_meta SET value='null' WHERE key='anchor'",()),
                         ('UPDATE coverage SET end_height=9',())]:
            with self.subTest(sql=sql):
                saved=self.path.read_bytes()
                self.change(sql,args)
                with self.assertRaises(ValueError):self.check()
                self.path.write_bytes(saved)

    def test_foreign_lineage_reader_or_source_attribution_refuses(self):
        for sql,args in [("UPDATE wallet_meta SET value='3' WHERE key='schema_version'",()),
                         ("UPDATE wallet_meta SET value='{}' WHERE key='set_identity'",()),
                         ('UPDATE receives SET script=?',(b'\x52',)),
                         ("UPDATE receives SET revision_digest=?",('z'*64,))]:
            with self.subTest(sql=sql):
                saved=self.path.read_bytes();self.change(sql,args)
                with self.assertRaises(ValueError):self.check()
                self.path.write_bytes(saved)

    def test_inexact_event_and_cross_page_transaction_metadata_refuse(self):
        raw=bytearray(receive());raw[7]=124
        self.change('UPDATE receives SET event=?',(bytes(raw),))
        with self.assertRaisesRegex(ValueError,'differs'):self.check()
        self.change('UPDATE receives SET event=?',(receive(),))
        other=bytearray(receive());other[47:51]=(1).to_bytes(4,'little');other[-1]=6
        self.change('INSERT INTO receives VALUES(?,?,10,?)',(bytes(other),b'\x51','d'*64))
        self.sample['clients'][0].update(journal_events=2,expected_digest=hashlib.sha256(receive()+bytes(other)).hexdigest())
        with self.assertRaisesRegex(ValueError,'contradiction'):self.check()

    def test_failed_native_attempt_and_unresolved_completed_shortcut_are_retained(self):
        binary=self.root/'binary';binary.write_bytes(b'fixture executable')
        sample=self.root/'sample.json';sample.write_text(json.dumps(self.sample))
        def execute(command,**_):
            out=Path(command[command.index('--json-out')+1])
            report={'in_progress':False,'stop_reason':None,'source_sha':'e'*40,
                'set':{'schema':'transparent-shard-v11','genesis_hash':'b'*64,'target_height':10,'target_hash':'c'*64},
                'sample':{'sha256':M.checksum(sample)},
                'steps':[{'syncs':1,'completed':1,'exact':1,'failed':0,
                          'classes':{'cutover-active':{'incomplete_by_reason':{'unresolved-spends':1}}}}]}
            out.write_text(json.dumps(report));return type('Result',(),{'returncode':0})()
        output=self.root/'proof'
        with patch.object(M.subprocess,'run',side_effect=execute),self.assertRaisesRegex(ValueError,'unresolved'):
            M.run(binary,M.checksum(binary),sample,M.checksum(sample),'transparent-shard-v11','http://localhost','http://localhost',output,'e'*40)
        self.assertEqual(json.loads((output/'result.json').read_text())['status'],'failed')
        self.assertTrue((output/'native.json').exists())
        self.assertTrue((output/'owner.json').exists())

    def test_native_success_requires_every_reviewed_class_and_reopened_stores(self):
        binary=self.root/'binary';binary.write_bytes(b'fixture executable')
        sample=self.root/'sample.json';sample.write_text(json.dumps(self.sample))
        missing=False
        def execute(command,**_):
            report=Path(command[command.index('--json-out')+1])
            stores=Path(command[command.index('--store-dir')+1]);stores.mkdir()
            (stores/'client.sqlite').write_bytes(self.path.read_bytes())
            data={'in_progress':False,'stop_reason':None,'source_sha':'e'*40,
                'set':{'schema':'transparent-shard-v11','genesis_hash':'b'*64,'target_height':10,'target_hash':'c'*64},
                'sample':{'sha256':M.checksum(sample)},
                'steps':[{'syncs':1,'completed':1,'exact':1,'failed':0,'classes':{} if missing else
                    {'cutover-active':{'n':1,'completed':1,'exact':1,'failed':0,'incomplete_by_reason':{}}}}]}
            report.write_text(json.dumps(data));return type('Result',(),{'returncode':0})()
        args=(binary,M.checksum(binary),sample,M.checksum(sample),'transparent-shard-v11','http://localhost','http://localhost')
        with patch.object(M.subprocess,'run',side_effect=execute):
            result=M.run(*args,self.root/'pass','e'*40)
            self.assertEqual(result['status'],'passed')
            self.assertEqual(result['observations'][0]['events'],1)
            for name in ('owner.json','result.json','native.log'):
                self.assertEqual((self.root/'pass'/name).stat().st_mode & 0o777,0o600)
            missing=True
            with self.assertRaisesRegex(ValueError,'class'):
                M.run(*args,self.root/'missing','e'*40)
        self.assertEqual(json.loads((self.root/'missing/result.json').read_text())['error_type'],'ValueError')


if __name__=='__main__':unittest.main()
