"""A CUDA label must match compiler features and the packaged query server."""
import importlib.util
import json
from pathlib import Path
import subprocess
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
        self.binary = self.target / 'release/pir-query-server'
        self.binary.parent.mkdir()
        self.binary.write_bytes(b'executable')
        self.messages = self.target / 'messages.jsonl'
        self.artifacts = [
            {'reason': 'compiler-artifact', 'target': {'name': 'pir-query-server'},
             'features': ['enhance', 'cuda', 'native-reinspiring'], 'executable': str(self.binary)},
            {'reason': 'compiler-artifact', 'target': {'name': 'enhance_query_server'},
             'features': ['cuda', 'native-reinspiring']},
            {'reason': 'compiler-artifact', 'target': {'name': 'pir_cuda'},
             'features': ['cuda']},
        ]

    def verify(self, revision=SHA):
        self.messages.write_text('\n'.join(json.dumps(a) for a in self.artifacts))
        with patch.object(verification.subprocess, 'check_output', return_value=b'') as run:
            result = verification.verify(self.messages, self.target, revision)
        self.command = run.call_args.args[0]
        self.env = run.call_args.kwargs['env']
        return result

    def test_matching_compiler_and_executable_pass(self):
        result = self.verify()
        self.assertTrue(result['cuda'])
        self.assertTrue(result['native_reinspiring'])
        self.assertEqual(result['source_revision'], SHA)
        self.assertEqual(self.command[1:], ['check-backend'])
        self.assertTrue(self.command[0].endswith('pir-query-server'))
        self.assertNotIn('ENHANCE_MATVEC_BACKEND', self.env)
        self.assertNotIn('ENHANCE_CUDA_DEVICE', self.env)


    def test_either_package_missing_cuda_refuses(self):
        for index in (0, 1, 2):
            self.artifacts[index]['features'].remove('cuda')
            with self.assertRaisesRegex(ValueError, 'Cargo did not produce exactly one CUDA artifact'):
                self.verify()
            self.artifacts[index]['features'].append('cuda')

    def test_missing_native_protocol_refuses(self):
        self.artifacts[0]['features'].remove('native-reinspiring')
        with self.assertRaisesRegex(ValueError, 'Cargo did not produce exactly one CUDA artifact'):
            self.verify()

    def test_wrong_output_path_refuses(self):
        self.artifacts[0]['executable'] = str(self.target / 'other-server')
        with self.assertRaisesRegex(ValueError, 'packaging path differs'):
            self.verify()


    def test_evaluator_check_failure_refuses(self):
        self.messages.write_text('\n'.join(json.dumps(a) for a in self.artifacts))
        error = subprocess.CalledProcessError(1, 'pir-query-server', output=b'driver missing')
        with patch.object(verification.subprocess, 'check_output', side_effect=error):
            with self.assertRaisesRegex(ValueError, 'evaluator check failed: driver missing'):
                verification.verify(self.messages, self.target, SHA)


if __name__ == '__main__':
    unittest.main()
