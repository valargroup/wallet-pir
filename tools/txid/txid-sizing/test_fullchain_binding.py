"""Path replacement during acquisition must never give the wrong parser a writer."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import fullchain
import source_info
from test_source_info import identity


class ExtractionBindingTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.binary=self.root/'export'
        self.cache=self.root/'cache';self.cache.mkdir()
        self.evidence=self.root/'evidence';self.evidence.mkdir()
        self.write_binary('A')

    def write_binary(self, parser):
        value=identity()
        revision=('a' if parser=='A' else 'f')*40
        value['node_parser']['git_revision']=revision
        value['node_parser']['git_source']='git+https://example.invalid/node#'+revision
        self.binary.write_text(f'#!{sys.executable}\n'+f'IDENTITY={value!r}\nPARSER={parser!r}\n'+'''import hashlib,json,sqlite3,sys
from pathlib import Path
if sys.argv[1:]==['--source-info']:
    IDENTITY['binary_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    print(json.dumps(IDENTITY))
else:
    assert sys.argv[1]=='--census'
    with sqlite3.connect(sys.argv[2]) as db:
        db.executescript('CREATE TABLE IF NOT EXISTS blocks(height INTEGER PRIMARY KEY,hash TEXT,parser TEXT); CREATE TABLE IF NOT EXISTS txids(id TEXT PRIMARY KEY,parser TEXT);')
        for line in sys.stdin:
            frame=json.loads(line)
            db.execute('INSERT OR REPLACE INTO blocks VALUES(?,?,?)',(frame['height'],frame.get('hash','00'*32),PARSER))
            db.execute('INSERT OR REPLACE INTO txids VALUES(?,?)',(str(frame['height']),PARSER))
            db.commit()
            print(json.dumps(dict(height=frame['height'],raw_sha256=hashlib.sha256(bytes.fromhex(frame['raw_block'])).hexdigest(),eligible=1,shielded_only=0,transactions=1)),flush=True)
''')
        self.binary.chmod(0o700)

    def database(self):
        database=self.cache/'census.sqlite'
        with closing(sqlite3.connect(database)) as db:
            db.executescript('CREATE TABLE blocks(height INTEGER PRIMARY KEY,hash TEXT,parser TEXT); CREATE TABLE txids(id TEXT PRIMARY KEY,parser TEXT);')
            db.execute('INSERT INTO blocks VALUES(0,?,?)',('00'*32,'A'))
            db.execute('INSERT INTO txids VALUES(?,?)',('0','A'));db.commit()
        return database

    def test_live_acquisition_replacement_is_rejected_before_database_writes(self):
        compiled=source_info.read(self.binary)
        database=self.database();before=database.read_bytes()
        anchor='11'*32
        checkpoint=dict(database_sha256=hashlib.sha256(before).hexdigest(),anchor_hash=anchor,anchor_height=1,
                        binary_sha256=compiled['binary_sha256'],extractor_sha256='d'*64,census_source_sha256='e'*64)
        (self.cache/'checkpoint.json').write_text(json.dumps(checkpoint))
        owner=self
        class Pipeline:
            hashes={1:anchor}
            def __init__(self,*args):pass
            def blocks(self,start,stop):
                self.assert_range=(start,stop)
                owner.write_binary('B')  # A rebuild while the network was delivering this batch.
                yield 1,b'new public block'
        with patch.object(fullchain,'ANCHOR_HEIGHT',1), patch.object(fullchain,'ANCHOR_HASH',anchor), \
                patch.object(fullchain,'bundles',return_value=[]), patch.object(fullchain,'Pipeline',Pipeline), \
                patch.object(fullchain,'Gateway') as gateway:
            gateway.return_value.__enter__.return_value.return_value={'result':anchor}
            with self.assertRaisesRegex(ValueError,'binary.*changed'):
                fullchain.scan(self.cache,self.binary,self.root/'status.json',self.evidence)
        self.assertEqual(database.read_bytes(),before)
        with closing(sqlite3.connect(database)) as db:
            self.assertEqual(db.execute('SELECT height,parser FROM blocks').fetchall(),[(0,'A')])
        self.assertEqual(json.loads((self.cache/'checkpoint.json').read_text())['binary_sha256'],compiled['binary_sha256'])

    def test_replacement_after_validation_executes_the_verified_snapshot_not_the_new_path(self):
        compiled=source_info.read(self.binary)
        database=self.database()
        raw=b'public'
        bundle=self.root/'batch.raw';bundle.write_bytes(raw)
        rows=[dict(height=1,offset=0,raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest(),hash='11'*32)]
        original_popen=subprocess.Popen
        writers=[]
        def launch(args,*extra,**kwargs):
            if '--census' in args:
                writers.append(Path(args[0]))
                self.assertNotEqual(Path(args[0]),self.binary)
                self.write_binary('B')  # Swap after the snapshot's verification, just before exec.
            return original_popen(args,*extra,**kwargs)
        with patch.object(fullchain.subprocess,'Popen',side_effect=launch):
            receipt=fullchain.extract_bundle(self.cache,self.binary,bundle,rows,self.root/'status.json',compiled)
        self.assertEqual(receipt['blocks'],1)
        self.assertEqual(len(writers),1)
        self.assertFalse(writers[0].exists())  # Private execution bytes are cleaned after the child exits.
        with closing(sqlite3.connect(database)) as db:
            self.assertEqual(db.execute('SELECT height,parser FROM blocks ORDER BY height').fetchall(),[(0,'A'),(1,'A')])
            self.assertEqual(db.execute("SELECT count(*) FROM txids WHERE parser='B'").fetchone()[0],0)

    def test_live_writer_replacement_preserves_a_checkpoint_that_can_be_reopened(self):
        compiled=source_info.read(self.binary);original_bytes=self.binary.read_bytes()
        database=self.database();anchor='11'*32
        checkpoint=dict(database_sha256=hashlib.sha256(database.read_bytes()).hexdigest(),
                        anchor_hash=anchor,anchor_height=1,binary_sha256=compiled['binary_sha256'],
                        extractor_sha256='d'*64,census_source_sha256='e'*64)
        (self.cache/'checkpoint.json').write_text(json.dumps(checkpoint))
        class Pipeline:
            hashes={1:anchor}
            def __init__(self,*args):pass
            def blocks(self,start,stop):yield 1,b'public block'
        original_popen=subprocess.Popen;original_run=subprocess.run
        writers=[]
        publications=[]
        def launch(args,*extra,**kwargs):
            if '--census' in args:
                writers.append(Path(args[0]));self.write_binary('B')
            return original_popen(args,*extra,**kwargs)
        def command(args,*extra,**kwargs):
            if args[0]=='git':
                publications.append(args)
                return subprocess.CompletedProcess(args,0)
            return original_run(args,*extra,**kwargs)
        with patch.object(fullchain,'ANCHOR_HEIGHT',1),patch.object(fullchain,'ANCHOR_HASH',anchor), \
                patch.object(fullchain,'bundles',return_value=[]),patch.object(fullchain,'Pipeline',Pipeline), \
                patch.object(fullchain,'Gateway') as gateway, \
                patch.object(fullchain.subprocess,'Popen',side_effect=launch), \
                patch.object(fullchain.subprocess,'run',side_effect=command):
            gateway.return_value.__enter__.return_value.return_value={'result':anchor}
            report=fullchain.scan(self.cache,self.binary,self.root/'status.json',self.evidence)
        self.assertEqual(publications, [], "a completed census saves local progress without publishing it to Git")
        self.assertEqual(len(writers),1)
        self.assertNotEqual(writers[0],self.binary)
        with closing(sqlite3.connect(database)) as db:
            self.assertEqual(db.execute('SELECT height,parser FROM blocks ORDER BY height').fetchall(),[(0,'A'),(1,'A')])
        saved=json.loads((self.cache/'checkpoint.json').read_text())
        self.assertEqual(saved,report)
        self.assertEqual(saved['database_sha256'],hashlib.sha256(database.read_bytes()).hexdigest())
        self.assertEqual(saved['binary_sha256'],compiled['binary_sha256'])
        self.assertEqual(saved['compiled_reader_identity'],compiled)
        # The exact verified reader can reopen this committed checkpoint; a
        # mutable-path rejection must never leave a checksum-stranded database.
        self.binary.write_bytes(original_bytes)
        with patch.object(fullchain,'ANCHOR_HEIGHT',1),patch.object(fullchain,'ANCHOR_HASH',anchor), \
                patch.object(fullchain,'bundles',return_value=[]),patch.object(fullchain,'Gateway') as gateway:
            gateway.return_value.__enter__.return_value.return_value={'result':anchor}
            resumed=fullchain.scan(self.cache,self.binary,self.root/'resume.json',self.evidence)
        self.assertEqual(resumed['database_sha256'],saved['database_sha256'])
        self.assertEqual(resumed['last_height'],1)

    def test_retained_batch_is_checkpointed_before_rejecting_the_next_rebuilt_reader(self):
        compiled=source_info.read(self.binary)
        database=self.database();anchor='22'*32
        checkpoint=dict(database_sha256=hashlib.sha256(database.read_bytes()).hexdigest(),
                        anchor_hash=anchor,anchor_height=2,binary_sha256=compiled['binary_sha256'],
                        extractor_sha256='d'*64,census_source_sha256='e'*64)
        (self.cache/'checkpoint.json').write_text(json.dumps(checkpoint))
        batches=[]
        for height in [1,2]:
            raw=b'public'+bytes([height]);bundle=self.root/(str(height)+'.raw');bundle.write_bytes(raw)
            batches.append((bundle,[dict(height=height,offset=0,raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest(),hash=f'{height:02x}'*32)]))
        original_popen=subprocess.Popen
        def launch(args,*extra,**kwargs):
            if '--census' in args:self.write_binary('B')
            return original_popen(args,*extra,**kwargs)
        with patch.object(fullchain,'ANCHOR_HEIGHT',2),patch.object(fullchain,'ANCHOR_HASH',anchor), \
                patch.object(fullchain,'bundles',return_value=batches), \
                patch.object(fullchain.subprocess,'Popen',side_effect=launch):
            with self.assertRaisesRegex(ValueError,'binary identity changed'):
                fullchain.scan(self.cache,self.binary,self.root/'status.json',self.evidence)
        with closing(sqlite3.connect(database)) as db:
            self.assertEqual(db.execute('SELECT height,parser FROM blocks ORDER BY height').fetchall(),[(0,'A'),(1,'A')])
        saved=json.loads((self.cache/'checkpoint.json').read_text())
        self.assertEqual(saved['last_height'],1)
        self.assertEqual(saved['database_sha256'],hashlib.sha256(database.read_bytes()).hexdigest())
        self.assertEqual(saved['binary_sha256'],compiled['binary_sha256'])

    def test_replacement_while_copying_is_rejected_before_starting_a_writer(self):
        compiled=source_info.read(self.binary)
        database=self.database();before=database.read_bytes()
        raw=b'public';bundle=self.root/'batch.raw';bundle.write_bytes(raw)
        rows=[dict(height=1,offset=0,raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest())]
        original=source_info.require_same
        def check_then_replace(binary,expected):
            original(binary,expected)
            self.write_binary('B')
        with patch.object(source_info,'require_same',side_effect=check_then_replace):
            with self.assertRaisesRegex(ValueError,'binary identity changed'):
                fullchain.extract_bundle(self.cache,self.binary,bundle,rows,self.root/'status.json',compiled)
        self.assertEqual(database.read_bytes(),before)


if __name__=='__main__':unittest.main()
