"""Wallet routes reach query ingress without exposing coordinator listeners."""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = ROOT / 'tools/deploy/coordinator/Caddyfile'


class CoordinatorTemplate(unittest.TestCase):
    def test_wallet_routes_reach_query_ingress_and_never_the_coordinator(self):
        text = TEMPLATE.read_text()
        self.assertNotIn('127.0.0.1:8080', text)
        self.assertNotIn('127.0.0.1:8489', text)
        self.assertIn('handle /v1/status/* {\n\t\treverse_proxy 127.0.0.1:8492', text)
        self.assertIn('handle /v1/enhance/query {\n\t\treverse_proxy 127.0.0.1:8082', text)
        self.assertIn('handle /v1/enhance/* {\n\t\treverse_proxy 127.0.0.1:8082', text)
        self.assertIn('handle {\n\t\trespond 404', text)

    def test_canonical_public_routes_sit_beside_their_legacy_aliases(self):
        text = TEMPLATE.read_text()
        self.assertIn('handle /status/v1/* {\n\t\treverse_proxy 127.0.0.1:8492', text)
        self.assertIn('handle /enhance/v1/query {\n\t\treverse_proxy 127.0.0.1:8082 {\n'
                      '\t\t\ttransport http {\n\t\t\t\tkeepalive off', text)
        self.assertIn('handle /enhance/v1/* {\n\t\treverse_proxy 127.0.0.1:8082', text)
        self.assertIn('@legacy_transparent_filters path /v1/filters/* /transparent/v1/filters/*\n', text)
        self.assertIn('handle_path /transparent/v1/filters/parents/* {\n'
                      '\t\troot * /srv/transparent-parent-filters/public', text)
        # Canonical prefixes are matched per product: no bare `/transparent/v1/*`,
        # no transparent health, and no control or worker surface under them.
        handled = re.findall(r'(?m)^\t(?:handle(?:_path)?|@\w+ path) (.+?)(?: \{)?$', text)
        canonical = [p for line in handled for p in line.split() if p.startswith(('/enhance/', '/status/', '/transparent/'))]
        self.assertEqual(sorted(canonical), ['/enhance/v1/*', '/enhance/v1/query', '/status/v1/*',
                                             '/transparent/v1/filters/*', '/transparent/v1/filters/parents/*'])
        for private in ('/control', '/internal', '/evaluate', '/metrics', '/v1/ready', '/v1/health', '/v1/provider-'):
            self.assertNotIn('handle ' + private, text)


if __name__ == '__main__':
    unittest.main()
