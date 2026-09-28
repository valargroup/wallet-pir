"""The coordinator Caddyfile template must keep the transparent publication route.

The Enhance deploy renders the coordinator's Caddyfile from this template. On
2026-09-27 the template lacked the continuous-publication route, and an Enhance
deploy withdrew public transparent metadata for about four and a half hours.
The transparent publisher's activation recognises the block by its marker
comment and leaves it alone, so the marker and routes must match it.
"""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / 'ops/deploy/coordinator/Caddyfile'
PUBLISHER = ROOT / 'transparent/ops/scripts/deploy-transparent-publisher.py'
MARKER = '# Continuous transparent publication'


class CoordinatorTemplate(unittest.TestCase):
    def test_template_routes_transparent_metadata_to_the_publisher(self):
        text = TEMPLATE.read_text()
        self.assertIn(MARKER, text)
        matcher = re.search(r'@transparent_publication path ([^\n]+)', text)
        self.assertIsNotNone(matcher)
        paths = set(matcher.group(1).split())
        self.assertEqual(
            paths,
            {'/v1/shards', '/v1/shards/init', '/v1/shards/*/revisions/*/manifest',
             '/v1/filters/shards', '/v1/filters/shards/*'},
        )
        block = text[matcher.end():text.index('}', matcher.end())]
        self.assertIn('reverse_proxy 127.0.0.1:8094', block)

    def test_publication_route_precedes_the_legacy_filter_handler(self):
        text = TEMPLATE.read_text()
        self.assertLess(text.index('@transparent_publication'), text.index('@legacy_transparent_filters'))
        self.assertNotIn('handle /v1/filters/* {', text)

    def test_publisher_activation_recognises_the_template_marker(self):
        self.assertIn(repr(MARKER)[1:-1], PUBLISHER.read_text())


if __name__ == '__main__':
    unittest.main()
