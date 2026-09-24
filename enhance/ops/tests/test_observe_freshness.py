"""Freshness samples retain node and publication identities without RPC credentials."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/observe-freshness.py'
spec = importlib.util.spec_from_file_location('freshness', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class FreshnessTests(unittest.TestCase):
    def test_node_tip_and_published_anchor_are_recorded(self):
        with tempfile.TemporaryDirectory() as temp:
            cookie = Path(temp) / 'cookie'
            cookie.write_text('__cookie__:secret')
            with patch.object(module, 'read_json', side_effect=[
                {'result': 123, 'error': None},
                {'anchor_height': 122, 'generation': 9,
                 'ingestion_error': 'node lag', 'blocked_reason': None},
            ]) as read:
                result = module.sample(cookie)
            self.assertEqual((result['node_tip'], result['published_anchor'], result['generation']),
                             (123, 122, 9))
            self.assertTrue(result['ingestion_failed'])
            self.assertFalse(result['publication_blocked'])
            self.assertNotIn('secret', str(result))
            self.assertEqual(read.call_args_list[0].args[:2], (8232, '/'))
            self.assertEqual(read.call_args_list[1].args, (8080, '/v1/health'))

    def test_collection_failure_is_explicit_without_details(self):
        with tempfile.TemporaryDirectory() as temp:
            cookie = Path(temp) / 'cookie'
            cookie.write_text('__cookie__:secret')
            with patch.object(module, 'read_json', side_effect=OSError('private path')):
                result = module.sample(cookie)
        self.assertEqual(result['error'], 'OSError')
        self.assertIsNone(result['node_tip'])
        self.assertNotIn('private path', str(result))

    def test_missing_coordinator_failure_fields_are_collection_errors(self):
        with tempfile.TemporaryDirectory() as temp:
            cookie = Path(temp) / 'cookie'
            cookie.write_text('__cookie__:secret')
            with patch.object(module, 'read_json', side_effect=[
                {'result': 123, 'error': None},
                {'anchor_height': 122, 'generation': 9, 'blocked_reason': None},
            ]):
                result = module.sample(cookie)
        self.assertEqual(result['error'], 'KeyError')

    def test_termination_finalizes_the_trace_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'observation'
            process = subprocess.Popen([
                sys.executable, str(SCRIPT), '--output', str(output),
                '--seconds', '30', '--interval', '1',
                '--cookie', str(Path(temp) / 'missing-cookie'),
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 5
                trace = output / 'samples.jsonl'
                while (not trace.exists() or trace.stat().st_size == 0) and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(trace.exists() and trace.stat().st_size > 0)
                process.terminate()
                _stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 0, stderr.decode())
                manifest = json.loads((output / 'manifest.json').read_text())
                self.assertEqual(manifest['status'], 'interrupted')
                self.assertGreaterEqual(manifest['samples'], 1)
                self.assertEqual(manifest['samples_sha256'], hashlib.sha256(trace.read_bytes()).hexdigest())
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()


if __name__ == '__main__':
    unittest.main()
