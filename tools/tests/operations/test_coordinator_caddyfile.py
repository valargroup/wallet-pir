"""Wallet routes reach query ingress without exposing coordinator listeners."""
from pathlib import Path
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


if __name__ == '__main__':
    unittest.main()
