"""The coordinator edge serves the unified wallet host beside the Enhance host.

The renderer and the per-site helpers are tested directly. The integration test
runs the rendered Caddyfile in a real Caddy (skipped when `caddy` is not on
PATH) against recording upstreams: loopback HTTP services for Enhance and
Status, and HTTPS origins for Transparent and Receiver whose
certificates chain to a test CA. The only edits to the rendered file are test
plumbing: global ports and storage, internal TLS instead of ACME, the
upstream dial addresses, and trust in the test CA. Routes, Host and SNI
settings are exercised exactly as rendered.
"""
from datetime import datetime, timedelta, timezone
import http.client
import http.server
import importlib.util
import json
from pathlib import Path
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest

from wallet_pir_ops.deploy import coordinator_edge
from wallet_pir_ops.deploy.coordinator_edge import EdgeError, UNIFIED_HOST, render, sites
from wallet_pir_ops.deploy.publisher_serving import DISCOVERY, candidate as discovery_candidate

ROOT = Path(__file__).resolve().parents[3]
ENHANCE_HOST = 'enhance-pir.valargroup.dev'
TRANSPARENT_HOST = 'transparent-pir.valargroup.dev'
RECEIVER_HOST = 'receiver-pir.valargroup.dev'


def rendered():
    return render(coordinator_edge.TEMPLATE.read_text(), 'https://' + ENHANCE_HOST)


def site(text, host):
    start, end = sites(text)[host]
    return text[start:end]


def load_schema_routing():
    path = ROOT / 'tools/transparent/lib/activity_schema_routing.py'
    spec = importlib.util.spec_from_file_location('activity_schema_routing', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Rendering(unittest.TestCase):
    def test_render_keeps_both_sites_and_refuses_collapsing_them(self):
        text = rendered()
        self.assertEqual(set(sites(text)), {ENHANCE_HOST, UNIFIED_HOST})
        self.assertNotIn(coordinator_edge.TOKEN, text)
        with self.assertRaises(EdgeError):
            render(coordinator_edge.TEMPLATE.read_text(), 'https://' + UNIFIED_HOST)
        without_unified = coordinator_edge.TEMPLATE.read_text().split('\nwallet-pir.valargroup.dev {', 1)[0]
        with self.assertRaises(EdgeError):
            render(without_unified, ENHANCE_HOST)

    def test_unified_site_serves_only_canonical_prefixes_and_never_itself(self):
        unified = site(rendered(), UNIFIED_HOST)
        handled = re.findall(r'(?m)^\thandle (\S+) \{$', unified)
        self.assertEqual(sorted(handled), sorted(['/enhance/v1/*', '/enhance/v1/query', '/install', '/receiver/v1/*', '/status/v1/*',
                                           '/transparent/v1/*', '/transparent/v1/filters/shards*',
                                           '/transparent/v1/filters/parents/*']))
        self.assertNotIn(UNIFIED_HOST, unified.split('{', 1)[1])
        for legacy in ('/v1/enhance', '/v1/status', '/v1/filters', '/v2/', '/v1/receiver', '/apm', '/provider/'):
            self.assertFalse([path for path in handled if path.startswith(legacy)])

    def test_publisher_discovery_route_lands_in_the_enhance_site_only(self):
        text = rendered()
        after = discovery_candidate(text)
        self.assertIn(DISCOVERY, site(after, ENHANCE_HOST))
        self.assertEqual(site(after, UNIFIED_HOST), site(text, UNIFIED_HOST))

    def test_status_routing_moves_every_alias_with_the_legacy_route(self):
        from tools.status.lib.routing import candidate
        text = rendered().replace('127.0.0.1:8492', '127.0.0.1:8489')
        after = candidate(text)
        self.assertNotIn('8489', after)
        self.assertEqual(after, rendered())

    def test_maintenance_closes_filter_routes_on_both_sites(self):
        guard = load_schema_routing().guard_coordinator
        text = rendered().replace('\thandle /v1/status/* {', '\t@transparent_publication path /v1/shards\n'
                                  '\thandle @transparent_publication {\n\t\treverse_proxy 127.0.0.1:8094\n\t}\n'
                                  '\thandle /v1/status/* {', 1)
        guarded = guard(text)
        unified = site(guarded, UNIFIED_HOST)
        self.assertEqual(unified.count('transparent fleet maintenance'), 1)
        self.assertIn('reverse_proxy https://' + TRANSPARENT_HOST, unified)
        self.assertNotIn('127.0.0.1:8090', unified)


def certificate(name, issuer=None):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    signer_name, signer_key = (subject, key) if issuer is None else issuer[:2]
    now = datetime.now(timezone.utc)
    builder = (x509.CertificateBuilder().subject_name(subject).issuer_name(signer_name)
               .public_key(key.public_key()).serial_number(x509.random_serial_number())
               .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=1)))
    if issuer is None:
        builder = builder.add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
    else:
        builder = builder.add_extension(x509.SubjectAlternativeName([x509.DNSName(name)]), critical=False)
    cert = builder.sign(signer_key, hashes.SHA256())
    pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption())
    return subject, key, pem, key_pem


class Recorder(http.server.ThreadingHTTPServer):
    """An upstream that records each request and answers with its own name."""

    def __init__(self, name, tls=None):
        self.name, self.seen, self.sni = name, [], []
        super().__init__(('127.0.0.1', 0), RecordingHandler)
        if tls:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(*tls)
            context.sni_callback = lambda sock, server_name, ctx: self.sni.append(server_name)
            self.socket = context.wrap_socket(self.socket, server_side=True)
        threading.Thread(target=self.serve_forever, daemon=True).start()

    @property
    def port(self):
        return self.server_address[1]


class RecordingHandler(http.server.BaseHTTPRequestHandler):
    def handle_one(self):
        length = int(self.headers.get('Content-Length') or 0)
        body = self.rfile.read(length)
        self.server.seen.append({'method': self.command, 'path': self.path, 'body': body,
                                 'host': self.headers.get('Host'),
                                 'forwarded_for': self.headers.get('X-Forwarded-For')})
        reply = self.server.name.encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)

    do_GET = do_POST = handle_one

    def log_message(self, *_args):
        pass


def free_port():
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]


@unittest.skipUnless(shutil.which('caddy'), 'caddy is not installed')
class LiveEdge(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        root = Path(cls.directory.name)
        ca = certificate('wallet-pir test CA')
        (root / 'ca.pem').write_bytes(ca[2])
        origins = {}
        for host in (TRANSPARENT_HOST, RECEIVER_HOST):
            _, _, pem, key = certificate(host, issuer=ca)
            (root / (host + '.pem')).write_bytes(pem)
            (root / (host + '.key')).write_bytes(key)
            origins[host] = Recorder(host, (root / (host + '.pem'), root / (host + '.key')))
        cls.upstreams = {'enhance': Recorder('enhance'), 'status': Recorder('status'),
                         'transparent': origins[TRANSPARENT_HOST],
                         'receiver': origins[RECEIVER_HOST]}
        parents = root / 'parents'
        parents.mkdir()
        (parents / 'parent.bin').write_bytes(b'parent filter')
        cls.https_port = free_port()
        cls.caddyfile = root / 'Caddyfile'
        cls.caddyfile.write_text(cls.plumb(rendered(), root, parents))
        cls.log = (root / 'caddy.log').open('wb')
        cls.caddy = subprocess.Popen([shutil.which('caddy'), 'run', '--config', str(cls.caddyfile), '--adapter', 'caddyfile'],
                                     stdout=subprocess.DEVNULL, stderr=cls.log,
                                     env={'HOME': str(root), 'XDG_DATA_HOME': str(root), 'PATH': '/usr/bin:/bin'})
        deadline = time.monotonic() + 30
        while True:
            try:
                if cls.request(ENHANCE_HOST, 'GET', '/v1/enhance/ready')[0] == 200:
                    break
            except OSError:
                pass
            if time.monotonic() > deadline or cls.caddy.poll() is not None:
                cls.tearDownClass()
                raise RuntimeError('caddy did not start: ' + (root / 'caddy.log').read_text()[-2000:])
            time.sleep(0.2)
        for upstream in cls.upstreams.values():
            upstream.seen.clear()

    @classmethod
    def plumb(cls, text, root, parents):
        ports = {'127.0.0.1:8082': 'enhance', '127.0.0.1:8492': 'status'}
        for address, name in ports.items():
            text = text.replace('reverse_proxy ' + address, f'reverse_proxy 127.0.0.1:{cls.upstreams[name].port}')
        for host, name in ((TRANSPARENT_HOST, 'transparent'), (RECEIVER_HOST, 'receiver')):
            text = text.replace('reverse_proxy https://' + host,
                                f'reverse_proxy https://127.0.0.1:{cls.upstreams[name].port}')
            text = text.replace('tls_server_name ' + host,
                                f'tls_server_name {host}\n\t\t\t\ttls_trust_pool file {root / "ca.pem"}')
        text = re.sub(r'\ttls \{\n\t\tissuer acme \{\n[^}]*\}\n\t\}\n', '\ttls internal\n', text)
        text = text.replace('/srv/transparent-parent-filters/public', str(parents))
        return ('{\n\tadmin off\n\tskip_install_trust\n\tauto_https disable_redirects\n'
                f'\thttp_port {free_port()}\n\thttps_port {cls.https_port}\n'
                f'\tstorage file_system {root / "storage"}\n}}\n\n' + text)

    @classmethod
    def tearDownClass(cls):
        cls.caddy.terminate()
        cls.caddy.wait(10)
        cls.log.close()
        for upstream in cls.upstreams.values():
            upstream.shutdown()
        cls.directory.cleanup()

    @classmethod
    def request(cls, host, method, path, body=None):
        context = ssl.create_default_context()
        context.check_hostname, context.verify_mode = False, ssl.CERT_NONE  # test client only
        raw = socket.create_connection(('127.0.0.1', cls.https_port), timeout=10)
        connection = http.client.HTTPConnection(host, timeout=10)
        connection.sock = context.wrap_socket(raw, server_hostname=host)
        connection.request(method, path, body=body, headers={'Content-Type': 'application/octet-stream'})
        response = connection.getresponse()
        result = response.status, response.read()
        connection.close()
        return result

    def only(self, name):
        hits = {key: upstream.seen[:] for key, upstream in self.upstreams.items() if upstream.seen}
        for upstream in self.upstreams.values():
            upstream.seen.clear()
        self.assertEqual(list(hits), [name])
        self.assertEqual(len(hits[name]), 1)
        return hits[name][0]

    def nothing_reached(self):
        self.assertEqual({k: u.seen for k, u in self.upstreams.items() if u.seen}, {})

    def test_unified_host_dispatches_each_product_and_preserves_queries(self):
        query = bytes(range(256)) * 64
        for path, name in (('/enhance/v1/query', 'enhance'), ('/status/v1/query', 'status'),
                           ('/transparent/v1/query', 'transparent'), ('/receiver/v1/query', 'receiver')):
            with self.subTest(path=path):
                status, reply = self.request(UNIFIED_HOST, 'POST', path, query)
                self.assertEqual((status, reply), (200, name.encode() if name in ('enhance', 'status') else
                                                   {'transparent': TRANSPARENT_HOST, 'receiver': RECEIVER_HOST}[name].encode()))
                seen = self.only(name)
                backend_path = path.replace('/enhance/v1', '/v1/enhance').replace('/transparent/v1', '/v2').replace('/receiver/v1', '/v1/receiver')
                self.assertEqual((seen['method'], seen['path'], seen['body']), ('POST', backend_path, query))
                self.assertEqual(seen['forwarded_for'], '127.0.0.1')
        for path, name in (('/enhance/v1/init', 'enhance'), ('/status/v1/session/00', 'status'),
                           ('/transparent/v1/filters/shards', 'transparent'),
                           ('/transparent/v1/filters/shards/3/filter', 'transparent'),
                           ('/receiver/v1/inventory', 'receiver')):
            with self.subTest(path=path):
                self.assertEqual(self.request(UNIFIED_HOST, 'GET', path)[0], 200)
                self.assertEqual(self.only(name)['method'], 'GET')
        self.assertEqual(self.request(UNIFIED_HOST, 'GET', '/transparent/v1/filters/parents/parent.bin'),
                         (200, b'parent filter'))
        self.nothing_reached()

    def test_recovery_metadata_and_queries_use_the_same_active_publication(self):
        for path in ('/transparent/v1/shards', '/transparent/v1/filters/shards',
                     '/transparent/v1/filters/shards/3/filter'):
            self.assertEqual(self.request(UNIFIED_HOST, 'GET', path), (200, TRANSPARENT_HOST.encode()))
            self.assertEqual(self.only('transparent')['path'], path.replace('/transparent/v1', '/v2'))

    def test_cross_origin_hops_name_the_origin_as_host_and_sni(self):
        for host, name in ((TRANSPARENT_HOST, 'transparent'), (RECEIVER_HOST, 'receiver')):
            self.upstreams[name].sni.clear()
            prefix = '/transparent/v1/' if name == 'transparent' else '/receiver/v1/'
            self.assertEqual(self.request(UNIFIED_HOST, 'GET', prefix + 'inventory')[0], 200)
            self.assertEqual(self.only(name)['host'], host)
            self.assertIn(host, self.upstreams[name].sni)

    def test_unified_host_refuses_legacy_and_operator_paths(self):
        for path in ('/v1/enhance/query', '/v1/status/init', '/v1/filters/shards', '/v2/query',
                     '/v1/receiver/query', '/metrics', '/v1/health', '/v1/ready', '/apm', '/control/health',
                     '/internal/x', '/provider/enhance/v1/x', '/', '/wallet-pir/v1/x'):
            with self.subTest(path=path):
                self.assertEqual(self.request(UNIFIED_HOST, 'GET', path)[0], 404)
        self.nothing_reached()

    def test_enhance_host_keeps_its_legacy_and_canonical_routes(self):
        query = b'\x00\xffencrypted' * 100
        for path, name in (('/v1/enhance/query', 'enhance'), ('/enhance/v1/query', 'enhance'),
                           ('/v1/status/query', 'status'), ('/status/v1/query', 'status')):
            with self.subTest(path=path):
                self.assertEqual(self.request(ENHANCE_HOST, 'POST', path, query)[0], 200)
                seen = self.only(name)
                self.assertEqual((seen['method'], seen['path'], seen['body']), ('POST', path, query))
        for path in ('/metrics', '/receiver/v1/query', '/transparent/v1/query', '/v1/filters/info',
                     '/transparent/v1/filters/range'):
            self.assertEqual(self.request(ENHANCE_HOST, 'GET', path)[0], 404)
        self.nothing_reached()


@unittest.skipUnless(shutil.which('caddy'), 'caddy is not installed')
class ReceiverTrustBoundary(unittest.TestCase):
    """Receiver's edge keeps the wallet's address only from the coordinator.

    The test peer is loopback, so trusting it stands in for the coordinator;
    with the shipped coordinator address it is an untrusted peer.
    """

    def forwarded_for(self, trust_loopback):
        text = (ROOT / 'tools/receiver/digitalocean/Caddyfile').read_text()
        self.assertIn('trusted_proxies static 167.99.42.60/32', text)
        if trust_loopback:
            text = text.replace('167.99.42.60/32', '127.0.0.1/32')
        upstream = Recorder('receiver')
        self.addCleanup(upstream.shutdown)
        port = free_port()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        text = text.replace('{$RECEIVER_QUERY_UPSTREAM}', f'127.0.0.1:{upstream.port}')
        text = text.replace('{\n\tservers {', '{\n\tadmin off\n\tskip_install_trust\n\tauto_https disable_redirects\n'
                            f'\thttp_port {free_port()}\n\thttps_port {port}\n'
                            f'\tstorage file_system {root / "storage"}\n\tservers {{', 1)
        text = text.replace(RECEIVER_HOST + ' {', RECEIVER_HOST + ' {\n\ttls internal', 1)
        (root / 'Caddyfile').write_text(text)
        log = (root / 'caddy.log').open('wb')
        self.addCleanup(log.close)
        caddy = subprocess.Popen([shutil.which('caddy'), 'run', '--config', str(root / 'Caddyfile'),
                                  '--adapter', 'caddyfile'], stdout=subprocess.DEVNULL, stderr=log,
                                 env={'HOME': str(root), 'XDG_DATA_HOME': str(root), 'PATH': '/usr/bin:/bin'})
        self.addCleanup(caddy.wait, 10)
        self.addCleanup(caddy.terminate)
        context = ssl.create_default_context()
        context.check_hostname, context.verify_mode = False, ssl.CERT_NONE  # test client only
        deadline = time.monotonic() + 30
        while True:
            try:
                raw = socket.create_connection(('127.0.0.1', port), timeout=10)
                connection = http.client.HTTPConnection(RECEIVER_HOST, timeout=10)
                connection.sock = context.wrap_socket(raw, server_hostname=RECEIVER_HOST)
                # The deployed route; the canonical one arrives with the Receiver change.
                connection.request('POST', '/v1/receiver/query', body=b'q' * 32,
                                   headers={'X-Forwarded-For': '203.0.113.9'})
                status = connection.getresponse().status
                connection.close()
                self.assertEqual(status, 200)
                break
            except (OSError, ssl.SSLError):
                self.assertLess(time.monotonic(), deadline, (root / 'caddy.log').read_text()[-2000:])
                time.sleep(0.2)
        return upstream.seen[-1]['forwarded_for']

    def test_coordinator_forwarded_wallet_address_reaches_admission(self):
        self.assertEqual(self.forwarded_for(trust_loopback=True).split(',')[0].strip(), '203.0.113.9')

    def test_other_peers_cannot_choose_their_admission_key(self):
        self.assertEqual(self.forwarded_for(trust_loopback=False), '127.0.0.1')


if __name__ == '__main__':
    unittest.main()
