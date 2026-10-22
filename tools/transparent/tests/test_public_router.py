#!/usr/bin/env python3
"""The public router shell: one route file per wallet API version.

The shell holds no version. Each committed `api/v<N>.caddy` is inlined in both
sites, a file may only handle its own prefix, a withdrawn version answers 503
on its prefix alone, and a retired version answers 410.
"""
import importlib.util
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('public_router', ROOT / 'tools/transparent/scripts/transparent-public-router.py')
ROUTER = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ROUTER)

CONFIG = dict(public_host='pir.example', internal_listen='10.0.0.9:8080', router_host='10.0.0.9',
              ssh_key='key', known_hosts='known')


def routes(**files):
    tmp = tempfile.TemporaryDirectory()
    for name, text in files.items():
        (Path(tmp.name) / name).write_text(text)
    return tmp


class PublicRouterTests(unittest.TestCase):
    def test_committed_routes_retire_v1_and_proxy_v2(self):
        text = ROUTER.render(CONFIG)
        for site in ('pir.example {', 'http://10.0.0.9:8080 {'):
            self.assertIn(site, text)
        self.assertEqual(text.count('respond `{"error":"api_version_retired","retired":"/v1","use":"/v2"}` 410'), 2)
        # Each upstream is reached by the legacy `/v2/` and canonical
        # `/transparent/v1/` prefix in both sites, and by nothing else.
        self.assertEqual(text.count('reverse_proxy 10.142.0.18:8095'), 4)
        self.assertEqual(text.count('reverse_proxy 10.142.0.9:8080 {'), 4)
        self.assertEqual(text.count('handle /transparent/v1/txid/* {\n\t\treverse_proxy 10.142.0.18:8095'), 2)
        self.assertEqual(text.count('handle /transparent/v1/* {\n\t\treverse_proxy 10.142.0.9:8080 {'), 2)
        handled = sorted({line.strip() for line in text.splitlines() if line.strip().startswith('handle /')})
        self.assertEqual(handled, ['handle /transparent/v1/* {', 'handle /transparent/v1/txid/* {',
                                   'handle /v1/* {', 'handle /v2/* {', 'handle /v2/txid/* {'])
        # Versions come in numeric order and before the catch-all.
        self.assertLess(text.index('handle /v1/*'), text.index('handle /v2/txid/*'))
        self.assertLess(text.index('handle /v2/*'), text.index('\thandle {\n\t\trespond 404'))

    def test_a_new_version_is_one_more_file(self):
        v2 = (ROOT / 'tools/transparent/deploy/api/v2.caddy').read_text()
        with routes(**{'v2.caddy': v2, 'v10.caddy': 'handle /v10/* {\n\treverse_proxy 10.0.0.7:8080\n}\n',
                       'v3.caddy': 'handle /v3/* {\n\treverse_proxy 10.0.0.8:8080\n}\n'}) as tmp:
            text = ROUTER.render(CONFIG, tmp)
        self.assertLess(text.index('handle /v2/'), text.index('handle /v3/'))
        self.assertLess(text.index('handle /v3/'), text.index('handle /v10/'))

    def test_withdrawal_touches_one_prefix(self):
        text = ROUTER.render(CONFIG, withdrawn=['v2'])
        self.assertNotIn('10.142.0.9:8080', text)
        self.assertEqual(text.count('respond "transparent v2 maintenance" 503'), 4)
        self.assertEqual(text.count('handle /transparent/v1/* {\n\t\theader Retry-After 1'), 2)
        self.assertIn('api_version_retired', text)
        with self.assertRaisesRegex(ValueError, 'no route file to withdraw'):
            ROUTER.render(CONFIG, withdrawn=['v3'])

    def test_a_file_cannot_route_another_prefix(self):
        for name, text in (('v3.caddy', 'handle /v2/* {\n\trespond 200\n}\n'),
                           ('v3.caddy', 'handle /transparent/v1/* {\n\trespond 200\n}\n'),
                           ('v1.caddy', 'handle /transparent/v1/* {\n\trespond 200\n}\n'),
                           ('v2.caddy', 'handle /transparent/* {\n\trespond 200\n}\n'),
                           ('v3.caddy', 'handle {\n\trespond 200\n}\n'),
                           ('v3.conf', ''), ('routes.caddy', '')):
            with self.subTest(name=name, text=text), routes(**{name: text}) as tmp, self.assertRaises(ValueError):
                ROUTER.render(CONFIG, tmp)

    def test_invalid_addresses_refuse(self):
        for key in ('public_host', 'internal_listen'):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'invalid router addresses'):
                ROUTER.render({**CONFIG, key: 'pir.example {\n\trespond 200'})

    def test_shell_adapts_with_caddy_when_available(self):
        caddy = shutil.which('caddy')
        if not caddy:
            self.skipTest('caddy is not installed')
        with tempfile.TemporaryDirectory() as tmp:
            for withdrawn in ([], ['v2']):
                with self.subTest(withdrawn=withdrawn):
                    path = Path(tmp) / 'Caddyfile'
                    path.write_text(ROUTER.render(CONFIG, withdrawn=withdrawn))
                    result = subprocess.run([caddy, 'adapt', '--adapter', 'caddyfile', '--config', str(path)],
                                            capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn('/v1/*', result.stdout)


if __name__ == '__main__':
    unittest.main()
