"""Transient transport retries must be bounded and never conceal API errors."""
import io
import json
import os
from pathlib import Path
import ssl
import sys
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools/lib'))
from wallet_pir_ops import github_api as github


class GithubTests(unittest.TestCase):
    def call(self, replies):
        return patch.object(github.urllib.request.OpenerDirector, 'open', side_effect=replies)

    def test_transient_failure_recovers_and_exhaustion_fails(self):
        with patch.dict(os.environ, GH_TOKEN='fixture-not-a-secret'), patch.object(github.time, 'sleep'):
            with self.call([TimeoutError('timed out'), io.StringIO('{"value":1}')]) as opened:
                self.assertEqual(github.api('fixture'), {'value': 1})
                self.assertEqual(opened.call_count, 2)
            with self.call([urllib.error.URLError('timed out')] * 3) as opened:
                with self.assertRaises(urllib.error.URLError):
                    github.api('fixture')
                self.assertEqual(opened.call_count, 3)

    def test_authentication_not_found_and_invalid_json_fail_without_retry(self):
        errors = [urllib.error.HTTPError('fixture', code, 'error', {}, None) for code in [401, 403, 404]]
        errors += [urllib.error.URLError(ssl.SSLCertVerificationError('bad certificate'))]
        with patch.dict(os.environ, GH_TOKEN='fixture-not-a-secret'), patch.object(github.time, 'sleep') as sleep:
            for error in errors:
                with self.subTest(error=type(error).__name__), self.call([error]) as opened:
                    with self.assertRaises(type(error)):
                        github.api('fixture')
                    self.assertEqual(opened.call_count, 1)
            with self.call([io.StringIO('{invalid')]) as opened:
                with self.assertRaises(json.JSONDecodeError):
                    github.api('fixture')
                self.assertEqual(opened.call_count, 1)
            sleep.assert_not_called()

    def test_rate_limit_and_server_errors_retry_but_remain_failures(self):
        with patch.dict(os.environ, GH_TOKEN='fixture-not-a-secret'), patch.object(github.time, 'sleep'):
            for code in [429, 500, 502, 503, 504]:
                replies = [urllib.error.HTTPError('fixture', code, 'error', {}, None) for _ in range(3)]
                with self.subTest(code=code), self.call(replies) as opened:
                    with self.assertRaises(urllib.error.HTTPError):
                        github.api('fixture')
                    self.assertEqual(opened.call_count, 3)

    def test_storage_redirect_does_not_forward_authorization(self):
        request = urllib.request.Request('https://api.github.com/artifact', headers={'Authorization': 'fixture'})
        redirected = github.DropAuthOnRedirect().redirect_request(request, None, 302, '', {}, 'https://storage.example/artifact')
        self.assertIsNone(redirected.get_header('Authorization'))


if __name__ == '__main__':
    unittest.main()
