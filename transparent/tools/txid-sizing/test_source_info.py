"""Compiled identity comes from the selected executable, never adjacent source."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import fullchain
import prepare_evidence
import source_info


def identity():
    def package(name, revision):
        return dict(package=name, version='1.0', git_revision=revision,
                    git_source='git+https://example.invalid/source#'+revision)
    return dict(schema='txid-sizing-export-source-v1',
                node_parser=package('zakura-chain', 'a'*40),
                server_adapter=package('zakura-server-chain', 'b'*40),
                event_codec=package('zakura-transparent-events', 'b'*40),
                cargo_lock_sha256='c'*64,
                compiled_source_file_sha256=dict(extractor='d'*64,census='e'*64))


class SourceInfoTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.binary=self.root/'export'

    def executable(self, value, wrong_hash=False):
        self.binary.write_text(f'#!{sys.executable}\nimport hashlib,json,sys\nfrom pathlib import Path\n'
                               'assert sys.argv[1:]==["--source-info"]\n'
                               f'value=json.loads({json.dumps(value)!r})\n'
                               'value["binary_sha256"]=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()\n'
                               + ('value["binary_sha256"]="0"*64\n' if wrong_hash else '')
                               + 'print(json.dumps(value))\n')
        self.binary.chmod(0o700)

    def test_live_binary_identifies_node_separately_from_mutated_checkout_adapter(self):
        self.executable(identity())
        (self.root/'Cargo.toml').write_text('[dependencies]\nzakura-server-chain = { rev="'+'f'*40+'" }\n')
        (self.root/'Cargo.lock').write_text('not the compilation lock')
        actual=source_info.read(self.binary)
        self.assertEqual(actual['node_parser']['git_revision'],'a'*40)
        self.assertEqual(actual['server_adapter']['git_revision'],'b'*40)
        self.assertEqual(actual['binary_sha256'],hashlib.sha256(self.binary.read_bytes()).hexdigest())
        self.assertEqual(fullchain.parser_revision(self.binary),'a'*40)

    def test_forged_binary_hash_is_rejected(self):
        self.executable(identity(),wrong_hash=True)
        with self.assertRaisesRegex(ValueError,'executing binary'):
            source_info.read(self.binary)

    def test_unknown_missing_or_ambiguous_identity_is_rejected(self):
        malformed=[]
        value=identity();value['schema']='unknown';malformed.append(value)
        value=identity();value.pop('node_parser');malformed.append(value)
        value=identity();value['node_parser']['package']='zakura-server-chain';malformed.append(value)
        value=identity();value['node_parser']['git_revision']='branch';malformed.append(value)
        value=identity();value['node_parser']['git_source']='registry+https://example.invalid';malformed.append(value)
        value=identity();value['node_parser']['git_source']='git+https://example.invalid#'+'f'*40;malformed.append(value)
        value=identity();value['node_parser']=[value['node_parser']];malformed.append(value)
        value=identity();value['cargo_lock_sha256']='not a digest';malformed.append(value)
        value=identity();value['compiled_source_file_sha256'].pop('extractor');malformed.append(value)
        for value in malformed:
            with self.subTest(value=value):
                self.executable(value)
                with self.assertRaises(ValueError):source_info.read(self.binary)

    def test_preparing_evidence_fails_before_writing_when_binary_identity_is_wrong(self):
        self.executable(identity(),wrong_hash=True)
        output=self.root/'evidence'
        with self.assertRaisesRegex(ValueError,'executing binary'):
            prepare_evidence.publish(self.root/'checkpoint',self.binary,output)
        self.assertFalse(output.exists())

    def test_prepared_sources_record_the_same_executable_identity_without_changing_inputs(self):
        self.executable(identity())
        checkpoint=self.root/'checkpoint';checkpoint.mkdir()
        receipt=dict(source='captured archive',blocks=[],design='fixed sample',canonical_recheck={},
                     node_info_sha256='f'*64,acquisition_seconds=1,new_rpc_calls=0)
        for name,value in [('receipt.json',receipt),('final-replay.json',{}),('node-info.json',{})]:
            (checkpoint/name).write_text(json.dumps(value))
        before={p.name:p.read_bytes() for p in checkpoint.iterdir()}
        output=self.root/'output';output.mkdir()
        (output/'sources.json').write_text(json.dumps(dict(source_files={})))
        with patch.object(prepare_evidence.survey,'sufficient',return_value={}), \
                patch.object(prepare_evidence.survey,'report',return_value={}), \
                patch.object(prepare_evidence.geometry,'survey_projection',return_value={}), \
                patch.object(prepare_evidence.verify_stream,'verify',return_value={}), \
                patch.object(prepare_evidence,'sample_packing',return_value={}):
            prepare_evidence.publish(checkpoint,self.binary,output)
        actual=json.loads((output/'archive-survey-sources.json').read_text())
        self.assertEqual(actual['parser_revision'],'a'*40)
        self.assertEqual(actual['metadata_dependency'],'b'*40)
        self.assertEqual(actual['compiled_reader_identity'],source_info.read(self.binary))
        self.assertEqual({p.name:p.read_bytes() for p in checkpoint.iterdir()},before)

    def test_fullchain_resume_refuses_relabeling_previous_binary_records(self):
        self.executable(identity())
        cache=self.root/'cache';cache.mkdir()
        (cache/'census.sqlite').write_bytes(b'captured database')
        saved=dict(database_sha256=hashlib.sha256(b'captured database').hexdigest(),
                   anchor_hash=fullchain.ANCHOR_HASH,anchor_height=fullchain.ANCHOR_HEIGHT,
                   binary_sha256='0'*64)
        (cache/'checkpoint.json').write_text(json.dumps(saved))
        with patch.object(fullchain,'extract_bundle') as extract:
            with self.assertRaisesRegex(ValueError,'checkpoint export binary differs'):
                fullchain.scan(cache,self.binary,self.root/'status',self.root/'evidence')
            extract.assert_not_called()
        self.assertEqual(json.loads((cache/'checkpoint.json').read_text()),saved)


if __name__=='__main__':unittest.main()
