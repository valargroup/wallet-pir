"""Guard location diagnostics never serialize exception messages or frame locals."""
import json
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from contextlib import redirect_stdout
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[3]/'ops/lib'), str(HERE.parents[1]/'lib')]
import activity_source_stage as B


class RefusalSite(unittest.TestCase):
    def client(self):
        stage = object.__new__(B.SourceStage)
        stage.inventory = SimpleNamespace(hosts={'coordinator':{}})
        stage.host = 'coordinator'
        stage.executor = SimpleNamespace(transport=lambda _: ['ssh','pinned-host'])
        stage.out = lambda _: self.fail('refusal cannot be a successful result')
        return stage

    def test_refusal_exact_wire_is_exclusive_private_and_not_emitted(self):
        raw = b'{"ok":false,"error":"ValueError","preflight_surveys":{"coordinator":{"hex":"SECRET_SENTINEL"}}}'
        with tempfile.TemporaryDirectory() as directory:
            stage = self.client(); path = Path(directory).resolve()/'reply.json'
            stage.private_evidence_file = path
            with patch.object(B.subprocess,'run',return_value=subprocess.CompletedProcess([],0,raw,b'')) as run:
                with self.assertRaises(ValueError) as caught:stage.call({'mode':'preflight'})
                self.assertEqual(path.read_bytes(),raw)
                self.assertEqual(path.stat().st_mode & 0o777,0o400)
                self.assertNotIn('SECRET_SENTINEL',str(caught.exception))
                with self.assertRaises(FileExistsError):stage.call({'mode':'preflight'})
                self.assertEqual(run.call_count,1)
                with self.assertRaisesRegex(ValueError,'preflight-only'):stage.call({'mode':'stage'})
                self.assertEqual(run.call_count,1)
                link = path.parent/'link';link.symlink_to(path)
                stage.private_evidence_file = link
                with self.assertRaisesRegex(ValueError,'link'):stage.call({'mode':'preflight'})
                self.assertEqual(run.call_count,1)

    def test_invalid_reply_is_preserved_before_parsing(self):
        with tempfile.TemporaryDirectory() as directory:
            stage=self.client();stage.private_evidence_file=Path(directory).resolve()/'reply'
            with patch.object(B.subprocess,'run',return_value=subprocess.CompletedProcess([],0,b'broken wire',b'')):
                with self.assertRaises(B.Unknown):stage.call({'mode':'preflight'})
            self.assertEqual(stage.private_evidence_file.read_bytes(),b'broken wire')

    def test_host_preserves_reply_before_guard_refusal_without_host_writes(self):
        request={'mode':'preflight','source_sha':'a'*40,'sha256':'b'*64,'machine_id':'c'*32}
        raw=b'{"fictional":"owner evidence"}'
        def fleet(*args, **kwargs):
            kwargs['retain']('coordinator',raw)
            raise ValueError('SECRET_SENTINEL')
        machine=SimpleNamespace(read_text=lambda:'c'*32)
        module=SimpleNamespace(fleet=fleet,MAX_REPLY=1<<20)
        output=io.StringIO()
        with patch.object(sys,'argv',['helper',json.dumps(request)]),patch.object(os,'geteuid',return_value=0), \
             patch.object(B.HOST,'PinnedHostLock',return_value=SimpleNamespace(MACHINE_ID=machine),create=True), \
             patch.object(B.HOST,'_BOOTSTRAP_FLEET',module,create=True), \
             patch.object(B.HOST,'_BOOTSTRAP_REMOTE_CODE','fixed',create=True),redirect_stdout(output):
            B.HOST.main()
        result=json.loads(output.getvalue())
        self.assertFalse(result['ok']);self.assertEqual(result['error'],'ValueError')
        self.assertEqual(bytes.fromhex(result['preflight_surveys']['coordinator']['hex']),raw)
        self.assertNotIn('SECRET_SENTINEL',output.getvalue())

    def test_only_reviewed_code_identity_is_reported(self):
        module = ModuleType('fictional_reviewed_component')
        exec("def require():\n raise ValueError('SECRET_SENTINEL')\ndef runtime():\n require()\n", module.__dict__)
        with patch.object(B.HOST, '_BOOTSTRAP_FLEET', module, create=True):
            try:
                module.runtime()
            except ValueError as error:
                site = B.HOST.refusal_site(error)
        self.assertEqual(site, {'component':'bootstrap','function':'runtime','line':4})
        self.assertNotIn('SECRET_SENTINEL', json.dumps(site))
        # A function with the same name and source filename has no authority.
        other = ModuleType('unreviewed')
        exec("def runtime():\n raise ValueError('SECRET_SENTINEL')\n", other.__dict__)
        with patch.object(B.HOST, '_BOOTSTRAP_FLEET', module, create=True):
            try:
                other.runtime()
            except ValueError as error:
                self.assertIsNone(B.HOST.refusal_site(error))

    def test_client_accepts_only_bounded_known_site_fields(self):
        stage = object.__new__(B.SourceStage)
        stage.inventory = SimpleNamespace(hosts={'coordinator':{}})
        stage.host = 'coordinator'
        stage.executor = SimpleNamespace(transport=lambda _: ['ssh','pinned-host'])
        stage.out = lambda _: self.fail('refusal cannot be a successful result')
        good = {'component':'bootstrap','function':'runtime','line':96}
        for site, wanted in ((good,True), ({**good,'line':True},False),
                             ({**good,'function':'/SECRET_SENTINEL'},False),
                             ({**good,'component':'unreviewed'},False),
                             ({**good,'line':5001},False),
                             ({**good,'extra':'SECRET_SENTINEL'},False), (None,False)):
            reply = {'ok':False,'error':'ValueError','refusal_site':site}
            with patch.object(B.subprocess,'run',return_value=subprocess.CompletedProcess([],0,json.dumps(reply).encode(),b'')):
                with self.assertRaises(ValueError) as caught:
                    stage.call({})
            self.assertEqual('reviewed guard bootstrap.runtime:96' in str(caught.exception),wanted)
            self.assertNotIn('SECRET_SENTINEL',str(caught.exception))


if __name__ == '__main__':
    unittest.main()
