"""The coordinator Caddyfile template no longer carries transparent wallet metadata.

The v11 publish controller on 127.0.0.1:8094 served `/v1/shards*` metadata
through this file until `/v1` was retired on 2026-10-13. The v12 publisher is
reached directly by its own router, so the template must not route wallet
metadata to the retired controller, and the publisher deploy must not add it back.
"""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = ROOT / 'tools/deploy/coordinator/Caddyfile'
PUBLISHER = ROOT / 'tools/transparent/scripts/deploy-transparent-publisher.py'


class CoordinatorTemplate(unittest.TestCase):
    def test_template_routes_nothing_to_the_retired_publisher(self):
        text = TEMPLATE.read_text()
        self.assertNotIn('127.0.0.1:8094', text)
        self.assertNotIn('/v1/shards', text)

    def test_publisher_deploy_does_not_edit_the_coordinator_caddyfile(self):
        text = PUBLISHER.read_text()
        self.assertNotIn('Continuous transparent publication', text)
        self.assertNotIn('@transparent_publication', text)


if __name__ == '__main__':
    unittest.main()
