"""Freshness samples retain node and publication identities without RPC credentials."""
import importlib.util
from pathlib import Path
import tempfile
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


if __name__ == '__main__':
    unittest.main()
