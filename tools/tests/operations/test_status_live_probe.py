"""The load transport preserves encrypted bytes and refusals across real HTTP."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import unittest
import urllib.error
import urllib.request

from tools.status.lib.live_probe import simulated_clients


class LiveProbe(unittest.TestCase):
    def test_query_bytes_and_overload_refusals_survive_with_distinct_client_addresses(self):
        observations = []

        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                observations.append((self.headers['X-Forwarded-For'],
                                     self.rfile.read(int(self.headers['Content-Length']))))
                self.send_response(429)
                self.send_header('Retry-After', '1')
                self.end_headers()
                self.wfile.write(b'admission refused')

        server = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with simulated_clients('http://127.0.0.1:' + str(server.server_port), 2) as origin:
                for _ in range(4):
                    request = urllib.request.Request(origin + '/v1/status/query', data=b'\x00\xffencrypted')
                    with self.assertRaises(urllib.error.HTTPError) as failure:
                        urllib.request.urlopen(request, timeout=2)
                    with failure.exception as response:
                        self.assertEqual(response.code, 429)
                        self.assertEqual(response.headers['Retry-After'], '1')
                        self.assertEqual(response.read(), b'admission refused')
                with self.assertRaises(urllib.error.HTTPError) as failure:
                    urllib.request.urlopen(origin + '/control/health', timeout=2)
                failure.exception.close()
                self.assertEqual(failure.exception.code, 404)
            self.assertEqual(observations, [('192.0.2.' + str(i % 2 + 1), b'\x00\xffencrypted') for i in range(4)])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_public_or_authenticated_origins_and_unbounded_clients_are_refused(self):
        for origin, clients in [('http://10.0.0.1:8484', 8), ('https://127.0.0.1:8484', 8),
                                ('http://user:pass@127.0.0.1:8484', 8), ('http://127.0.0.1:8484/x', 8),
                                ('http://127.0.0.1:8484', 0), ('http://127.0.0.1:8484', 65)]:
            with self.subTest(origin=origin, clients=clients), self.assertRaises(ValueError):
                with simulated_clients(origin, clients):
                    self.fail('invalid transport started')


if __name__ == '__main__':
    unittest.main()
