#!/usr/bin/env python3
"""The history router's opt-in import hook for display routes.

Without `route_imports` the rendered router must be byte-identical to the
renderer before the hook existed: the digests below were recorded from that
renderer for the same fleet, assignment and site variants. With the key, the
only difference is one `import` line per site, and the withdrawn router never
changes.
"""
import asyncio
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('live_fleet_router', ROOT / 'transparent/ops/scripts/transparent-live-fleet.py')
LIVE = importlib.util.module_from_spec(spec)
spec.loader.exec_module(LIVE)

GLOB = '/etc/caddy/txid-display/*.caddy'
# (internal listener, maintenance guard) -> sha256 of (live render, withdrawn render)
# from transparent-live-fleet.py at b18a21de, before route_imports existed.
GOLDEN = {
    (False, False): ('db0acffd2cd85d7cc0b2a6b68a65cd276e14ebf8db35585a48a57518a06c7059',
                     '5f246c39cbee07d214e81c52127db0f13a2812701ff7f57e31cdd661ea284d04'),
    (False, True): ('7e4e77767b5220b1be8d178d7f6411cc37b9e20701002d81ecfc57abc48bb8ea',
                    '7e4e77767b5220b1be8d178d7f6411cc37b9e20701002d81ecfc57abc48bb8ea'),
    (True, False): ('d7a48b2c3635d42df0a9b1afee617f0e98eae0602be2008563ca58361121eb1e',
                    'df6cb8cbb4630d289d87fa81fac17b35d0ad4d67d3b4f7ee0700dd2942c659c2'),
    (True, True): ('21b23e070a0f6b963f24cf2b1ae521fa97f02b74033a8edb0636ffe86c74064f',
                   '2c2475b54a445f40e8c7242e0831d226f43ef1a56ad4316dd380432c136699d9'),
}


def render(internal, guarded, live=True, **extra):
    """The live and withdrawn routers the adapter would send for a fixed fleet."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        roster = [dict(id=f'a{i}', role='archive-owner', ssh_host=f'10.0.0.{i}', upstream=f'10.0.0.{i}:8093')
                  for i in (1, 2)]
        roster += [dict(id=f'r{i}', role='recent-replica', ssh_host=f'10.0.1.{i}', upstream=f'10.0.1.{i}:8093')
                   for i in (1, 2)]
        (tmp / 'roster.json').write_text(json.dumps(roster))
        config = dict(roster=str(tmp / 'roster.json'), state_dir=str(tmp), known_hosts='known', ssh_key='key',
                      public_host='pir.example', authority_upstream='https://filters.example',
                      router_host='10.0.0.9', **extra)
        if internal:
            config['internal_listen'] = '10.0.0.9:8080'
        fleet = LIVE.Fleet(config)
        if guarded:
            (tmp / 'maintenance.json').write_text(json.dumps({'enabled': True}))
        captured = []

        async def ssh(host, command, data=None, timeout=25):
            captured.append(data)
            return b''
        fleet.ssh = ssh
        assignment = {'workers': [dict(id=w['id'], shards=[0] if w['role'] == 'archive-owner' else [1, 2])
                                  for w in roster]}
        assignment['workers'][1]['shards'] = [3]

        async def both():
            if live:
                await fleet.route(roster, assignment)
            await fleet.route([])
        asyncio.run(both())
        return captured


class RouterHookTests(unittest.TestCase):
    def test_absent_or_empty_key_renders_the_previous_bytes(self):
        for (internal, guarded), digests in GOLDEN.items():
            for extra in ({}, {'route_imports': []}):
                with self.subTest(internal=internal, guarded=guarded, extra=extra):
                    live, withdrawn = render(internal, guarded, **extra)
                    self.assertEqual((hashlib.sha256(live).hexdigest(), hashlib.sha256(withdrawn).hexdigest()),
                                     digests)

    def test_key_adds_only_an_import_per_serving_site(self):
        for (internal, guarded), digests in GOLDEN.items():
            with self.subTest(internal=internal, guarded=guarded):
                live, withdrawn = render(internal, guarded, route_imports=[GLOB])
                before, _ = render(internal, guarded)
                lines = live.decode().split('\n')
                imports = [i for i, line in enumerate(lines) if line == '\timport ' + GLOB]
                serving = 1 + internal - guarded
                self.assertEqual(len(imports), serving)
                for index in imports:
                    self.assertTrue(lines[index + 1].startswith('\t@metadata path '))
                    self.assertLess(index, lines.index('\thandle {', index))
                self.assertEqual('\n'.join(line for line in lines if line != '\timport ' + GLOB).encode(), before)
                # The withdrawn router attests rollback captures and never imports.
                self.assertEqual(hashlib.sha256(withdrawn).hexdigest(), digests[1])
                self.assertNotIn(b'import', withdrawn)

    def test_invalid_imports_refuse_before_any_router_change(self):
        for value in ('/etc/caddy/x/*.caddy', ['/etc/caddy/x/*.conf'], ['relative/*.caddy'],
                      ['/etc/caddy/x/*.caddy\n\trespond 200'], ['/etc/caddy/../Caddyfile.caddy'], [7]):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'invalid route imports'):
                render(False, False, route_imports=value)

    def test_an_invalid_key_never_blocks_withdrawal(self):
        for (internal, guarded), digests in GOLDEN.items():
            with self.subTest(internal=internal, guarded=guarded):
                withdrawn, = render(internal, guarded, live=False, route_imports=['/etc/caddy/../x.caddy'])
                self.assertEqual(hashlib.sha256(withdrawn).hexdigest(), digests[1])

    def test_publisher_redeploys_carry_the_key(self):
        self.assertIn('route_imports', LIVE.OPERATIONAL_KEYS)
        merged = LIVE.carry_operational({'roster': 'new'}, {'roster': 'old', 'route_imports': [GLOB]})
        self.assertEqual(merged, {'roster': 'new', 'route_imports': [GLOB]})

    def test_composed_router_adapts_with_caddy_when_available(self):
        caddy = shutil.which('caddy')
        if not caddy:
            self.skipTest('caddy is not installed')
        with tempfile.TemporaryDirectory() as tmp:
            snippet = (ROOT / 'transparent/ops/deploy/txid-display-routes.caddy.in').read_text()
            snippet = snippet.replace('@ARCHIVE_UPSTREAM@', '10.0.0.5:8095').replace('@RECENT_UPSTREAM@', '10.0.0.6:8095')
            (Path(tmp) / 'routes.caddy').write_text(snippet)
            live, _ = render(True, False, route_imports=[tmp + '/*.caddy'])
            (Path(tmp) / 'Caddyfile').write_text(live.decode())
            result = subprocess.run([caddy, 'adapt', '--adapter', 'caddyfile', '--config', tmp + '/Caddyfile'],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('/v1/txid/archive/*', result.stdout)


if __name__ == '__main__':
    unittest.main()
