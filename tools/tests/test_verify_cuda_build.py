"""A CUDA label must match compiler features and the packaged executable."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('verify_cuda_build', ROOT / 'tools/ci/verify_cuda_build.py')
verification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verification)
SHA = 'a' * 40


class CudaBuildTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.target = Path(self.directory.name)
        self.binary = self.target / 'release/enhance-pir-server'
        self.binary.parent.mkdir()
        self.binary.write_bytes(b'executable')
        self.messages = self.target / 'messages.jsonl'
        self.artifacts = [
            {'reason': 'compiler-artifact', 'target': {'name': 'enhance-pir-server'},
             'features': ['cuda', 'native-reinspiring'], 'executable': str(self.binary)},
            {'reason': 'compiler-artifact', 'target': {'name': 'enhance_query_server'},
             'features': ['cuda', 'native-reinspiring']},
        ]
        self.info = {'cuda': True, 'native_reinspiring': True, 'source_revision': SHA}

    def verify(self):
        self.messages.write_text('\n'.join(json.dumps(a) for a in self.artifacts))
        with patch.object(verification.subprocess, 'check_output', return_value=json.dumps(self.info).encode()):
            return verification.verify(self.messages, self.target, SHA)

    def test_matching_compiler_and_executable_pass(self):
        self.assertTrue(self.verify()['cuda'])

    def test_either_package_missing_cuda_refuses(self):
        for index in (0, 1):
            self.artifacts[index]['features'].remove('cuda')
            with self.assertRaisesRegex(ValueError, 'Cargo did not produce exactly one CUDA artifact'):
                self.verify()
            self.artifacts[index]['features'].append('cuda')

    def test_wrong_output_path_refuses(self):
        self.artifacts[0]['executable'] = str(self.target / 'other-server')
        with self.assertRaisesRegex(ValueError, 'packaging path differs'):
            self.verify()

    def test_cpu_binary_wrong_protocol_or_stale_revision_refuses(self):
        for key, value in [('cuda', False), ('native_reinspiring', False), ('source_revision', 'b' * 40)]:
            original = self.info[key]
            self.info[key] = value
            with self.assertRaisesRegex(ValueError, 'compiled server capabilities differ'):
                self.verify()
            self.info[key] = original


if __name__ == '__main__':
    unittest.main()
